#!/usr/bin/env python3
"""
Run several IBM Storage Protect dsmc incremental processes against one mounted
filesystem without recursively crossing into nested directories/filesystems.

Each directory is submitted with a trailing slash and -subdir=no. A shared,
bounded work queue gives the next batch to whichever worker finishes first.
"""

from __future__ import annotations

import argparse
import dataclasses
import os
import queue
import re
import shutil
import subprocess
import sys
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class Counters:
    discovered: int = 0
    completed: int = 0
    failed: int = 0
    skipped_mounts: int = 0
    scan_errors: int = 0
    lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    def add(self, name: str, value: int = 1) -> None:
        with self.lock:
            setattr(self, name, getattr(self, name) + value)

    def snapshot(self) -> tuple[int, int, int, int, int]:
        with self.lock:
            return (
                self.discovered,
                self.completed,
                self.failed,
                self.skipped_mounts,
                self.scan_errors,
            )


@dataclass
class WorkerState:
    worker_number: int
    status: str = "idle"
    batch_index: int = 0
    batch_total: int = 0
    current_directory: str = ""
    last_return_code: int | None = None


class WorkerStates:
    def __init__(self, streams: int) -> None:
        self._lock = threading.Lock()
        self._states = {
            number: WorkerState(worker_number=number)
            for number in range(1, streams + 1)
        }

    def start_batch(self, worker_number: int, total: int) -> None:
        with self._lock:
            state = self._states[worker_number]
            state.status = "running"
            state.batch_index = 0
            state.batch_total = total
            state.current_directory = ""
            state.last_return_code = None

    def set_directory(self, worker_number: int, index: int, path: str) -> None:
        with self._lock:
            state = self._states[worker_number]
            state.status = "running"
            state.batch_index = index
            state.current_directory = path

    def set_result(self, worker_number: int, return_code: int) -> None:
        with self._lock:
            self._states[worker_number].last_return_code = return_code

    def idle(self, worker_number: int) -> None:
        with self._lock:
            state = self._states[worker_number]
            state.status = "idle"
            state.batch_index = 0
            state.batch_total = 0
            state.current_directory = ""

    def stopped(self, worker_number: int) -> None:
        with self._lock:
            state = self._states[worker_number]
            state.status = "stopped"
            state.batch_index = 0
            state.batch_total = 0
            state.current_directory = ""

    def snapshot(self) -> list[WorkerState]:
        with self._lock:
            return [
                dataclasses.replace(state)
                for _, state in sorted(self._states.items(), key=lambda item: item[0])
            ]


class Dashboard:
    def __init__(
        self,
        counters: Counters,
        worker_states: WorkerStates,
        producer_done: threading.Event,
        stop_event: threading.Event,
        refresh_seconds: float,
    ) -> None:
        self.counters = counters
        self.worker_states = worker_states
        self.producer_done = producer_done
        self.stop_event = stop_event
        self.refresh_seconds = refresh_seconds
        self._rendered_lines = 0

    @staticmethod
    def _truncate(value: str, max_length: int) -> str:
        if max_length < 1:
            return ""
        if len(value) <= max_length:
            return value
        if max_length == 1:
            return "…"
        return value[: max_length - 1] + "…"

    @staticmethod
    def _bar(index: int, total: int, width: int) -> str:
        if width <= 0:
            return ""
        if total <= 0:
            filled = 0
        else:
            filled = min(width, max(0, int((index / total) * width)))
        return "#" * filled + "-" * (width - filled)

    def _render(self) -> None:
        terminal_width = shutil.get_terminal_size(fallback=(120, 20)).columns
        discovered, completed, failed, skipped, errors = self.counters.snapshot()
        states = self.worker_states.snapshot()

        header = (
            f"PROGRESS discovered={discovered} completed={completed} "
            f"failed={failed} skipped_mounts={skipped} scan_errors={errors}"
        )

        bar_width = 20
        worker_label_width = 4  # e.g. "W01 "
        bar_padding_width = 3  # "[" + "] "
        position_width = 9  # "X/Y" plus padding
        status_width = 9  # status text plus padding
        static_width = (
            worker_label_width + bar_padding_width + bar_width + position_width + status_width
        )
        path_width = max(10, terminal_width - static_width)

        lines = [header]
        for state in states:
            position = (
                f"{state.batch_index}/{state.batch_total}"
                if state.batch_total > 0
                else "0/0"
            )
            rc = ""
            if state.last_return_code is not None:
                rc = f" rc={state.last_return_code}"
            line = (
                f"W{state.worker_number:02d} "
                f"[{self._bar(state.batch_index, state.batch_total, bar_width)}] "
                f"{position:<8} {state.status:<8} "
                f"{self._truncate(state.current_directory, path_width)}{rc}"
            )
            lines.append(line)

        if self._rendered_lines:
            sys.stdout.write(f"\x1b[{self._rendered_lines}F")
        for line in lines:
            sys.stdout.write("\x1b[2K" + line + "\n")
        sys.stdout.flush()
        self._rendered_lines = len(lines)

    def run(self) -> None:
        while not self.stop_event.wait(self.refresh_seconds):
            self._render()
            discovered, completed, failed, _, _ = self.counters.snapshot()
            if self.producer_done.is_set() and completed + failed >= discovered:
                return

    def finish(self) -> None:
        self._render()


class SafeLogger:
    def __init__(self, path: Path, echo: bool = True) -> None:
        self.path = path
        self.echo = echo
        self.lock = threading.Lock()

    def write(self, message: str) -> None:
        stamp = time.strftime("%Y-%m-%d %H:%M:%S")
        line = f"{stamp} {message}\n"
        with self.lock:
            with self.path.open("a", encoding="utf-8", errors="backslashreplace") as handle:
                handle.write(line)
        if self.echo:
            print(line, end="", flush=True)


class SafeAppender:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.lock = threading.Lock()

    def write(self, line: str) -> None:
        with self.lock:
            with self.path.open("a", encoding="utf-8", errors="backslashreplace") as handle:
                handle.write(line.rstrip("\n") + "\n")


_MOUNT_ESCAPE = re.compile(r"\\([0-7]{3})")
MAX_DSMC_SUCCESS_RC = 4


def decode_mountinfo_path(value: str) -> str:
    """Decode octal escapes in /proc/self/mountinfo paths (such as \\040 for space)."""
    return _MOUNT_ESCAPE.sub(lambda match: chr(int(match.group(1), 8)), value)


def mounted_paths() -> set[str]:
    paths: set[str] = set()
    try:
        with open("/proc/self/mountinfo", "r", encoding="utf-8") as handle:
            for line in handle:
                fields = line.split()
                if len(fields) >= 5:
                    mountpoint = decode_mountinfo_path(fields[4])
                    paths.add(os.path.normpath(os.path.realpath(mountpoint)))
    except OSError:
        # st_dev checks still provide the normal filesystem-boundary protection.
        pass
    return paths


def is_within(root: str, candidate: str) -> bool:
    try:
        return os.path.commonpath((root, candidate)) == root
    except ValueError:
        return False


def nested_mounts(root: str) -> set[str]:
    return {path for path in mounted_paths() if path != root and is_within(root, path)}


def put_with_stop(
    work_queue: queue.Queue[str], path: str, stop_event: threading.Event
) -> bool:
    while not stop_event.is_set():
        try:
            work_queue.put(path, timeout=0.5)
            return True
        except queue.Full:
            continue
    return False


def scan_directories(
    root: str,
    work_queue: queue.Queue[str],
    producer_done: threading.Event,
    stop_event: threading.Event,
    counters: Counters,
    logger: SafeLogger,
) -> None:
    """Iteratively scan directories while remaining on the root filesystem."""
    try:
        root_device = os.stat(root, follow_symlinks=False).st_dev
    except OSError as exc:
        logger.write(f"FATAL: cannot stat {root!r}: {exc}")
        counters.add("scan_errors")
        producer_done.set()
        return

    mount_boundaries = nested_mounts(root)
    stack = [root]

    try:
        while stack and not stop_event.is_set():
            current = stack.pop()

            if not put_with_stop(work_queue, current, stop_event):
                break
            counters.add("discovered")

            try:
                with os.scandir(current) as entries:
                    for entry in entries:
                        if stop_event.is_set():
                            break
                        try:
                            if not entry.is_dir(follow_symlinks=False):
                                continue

                            child = os.path.normpath(entry.path)

                            # Explicit mountpoint detection catches bind mounts too.
                            if child in mount_boundaries:
                                counters.add("skipped_mounts")
                                logger.write(f"SKIP nested mount: {child}")
                                continue

                            # The device check is cheap and catches ordinary mounts.
                            if entry.stat(follow_symlinks=False).st_dev != root_device:
                                counters.add("skipped_mounts")
                                logger.write(f"SKIP different filesystem: {child}")
                                continue

                            stack.append(child)
                        except OSError as exc:
                            counters.add("scan_errors")
                            logger.write(f"SCAN ERROR: {entry.path!r}: {exc}")
            except OSError as exc:
                counters.add("scan_errors")
                logger.write(f"SCAN ERROR: {current!r}: {exc}")
    finally:
        producer_done.set()


def dsm_directory_operand(path: str) -> str:
    """A trailing slash tells dsmc to process the directory's immediate contents."""
    if path == os.sep:
        return path
    return path.rstrip(os.sep) + os.sep


def get_batch(
    work_queue: queue.Queue[str],
    batch_size: int,
    producer_done: threading.Event,
    stop_event: threading.Event,
) -> list[str]:
    batch: list[str] = []

    while not batch:
        if (producer_done.is_set() or stop_event.is_set()) and work_queue.empty():
            return batch
        try:
            batch.append(work_queue.get(timeout=0.5))
        except queue.Empty:
            continue

    while len(batch) < batch_size and not stop_event.is_set():
        try:
            batch.append(work_queue.get_nowait())
        except queue.Empty:
            break

    return batch


def wait_for_queue_drain(work_queue: queue.Queue[str], timeout_seconds: int) -> bool:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        if work_queue.unfinished_tasks == 0:
            return True
        time.sleep(0.2)
    return work_queue.unfinished_tasks == 0


def worker(
    worker_number: int,
    args: argparse.Namespace,
    work_queue: queue.Queue[str],
    producer_done: threading.Event,
    stop_event: threading.Event,
    counters: Counters,
    worker_states: WorkerStates,
    logger: SafeLogger,
    failed_logger: SafeAppender,
) -> None:
    worker_name = f"worker-{worker_number:02d}"
    worker_log = Path(args.log_dir) / f"{worker_name}.log"
    dsm_log_dir = Path(args.log_dir) / f"{worker_name}-dsm"
    dsm_log_dir.mkdir(parents=True, exist_ok=True)

    environment = os.environ.copy()
    environment["DSM_LOG"] = str(dsm_log_dir)

    try:
        while True:
            batch = get_batch(
                work_queue,
                args.batch_size,
                producer_done,
                stop_event,
            )
            if not batch:
                worker_states.stopped(worker_number)
                return

            worker_states.start_batch(worker_number, len(batch))
            logger.write(
                f"{worker_name}: reserved {len(batch)} "
                f"{'directory' if len(batch) == 1 else 'directories'}"
            )

            try:
                with worker_log.open("ab", buffering=0) as output:
                    output.write(
                        (
                            f"\n===== {time.strftime('%Y-%m-%d %H:%M:%S')} =====\n"
                            f"Directories: {len(batch)}\n"
                            + "\n".join(dsm_directory_operand(path) for path in batch)
                            + "\n"
                        ).encode("utf-8", errors="backslashreplace")
                    )

                    for index, path in enumerate(batch, start=1):
                        worker_states.set_directory(worker_number, index, path)
                        operand = dsm_directory_operand(path)
                        # One directory per invocation keeps worker X/Y progress accurate.
                        command = [
                            args.dsmc,
                            "incremental",
                            "-subdir=no",
                            f"-resourceutilization={args.resourceutilization}",
                            *args.dsmc_option,
                            operand,
                        ]

                        output.write(
                            (
                                f"\n--- {time.strftime('%Y-%m-%d %H:%M:%S')} ---\n"
                                f"Directory {index}/{len(batch)}: {operand}\n"
                            ).encode("utf-8", errors="backslashreplace")
                        )

                        return_code = 0
                        try:
                            if args.dry_run:
                                output.write(
                                    ("DRY RUN: " + repr(command) + "\n").encode(
                                        "utf-8", errors="backslashreplace"
                                    )
                                )
                            else:
                                completed = subprocess.run(
                                    command,
                                    stdout=output,
                                    stderr=subprocess.STDOUT,
                                    env=environment,
                                    check=False,
                                )
                                return_code = completed.returncode
                        except OSError as exc:
                            return_code = 127
                            logger.write(f"{worker_name}: failed to start dsmc: {exc}")

                        worker_states.set_result(worker_number, return_code)
                        if return_code <= MAX_DSMC_SUCCESS_RC:
                            counters.add("completed")
                            logger.write(
                                f"{worker_name}: completed {index}/{len(batch)} "
                                f"rc={return_code} path={path}"
                            )
                        else:
                            counters.add("failed")
                            logger.write(
                                f"{worker_name}: FAILED {index}/{len(batch)} "
                                f"rc={return_code} path={path}"
                            )
                            failed_logger.write(f"{return_code}\t{path}")
            finally:
                worker_states.idle(worker_number)
                for _ in batch:
                    work_queue.task_done()
    finally:
        worker_states.stopped(worker_number)


def progress_reporter(
    counters: Counters,
    producer_done: threading.Event,
    stop_event: threading.Event,
    interval: int,
) -> None:
    while not stop_event.wait(interval):
        discovered, completed, failed, skipped, errors = counters.snapshot()
        print(
            f"PROGRESS discovered={discovered} completed={completed} "
            f"failed={failed} skipped_mounts={skipped} scan_errors={errors}",
            flush=True,
        )
        if producer_done.is_set() and completed + failed >= discovered:
            return


def should_enable_dashboard(dashboard_disabled: bool) -> bool:
    return not dashboard_disabled and sys.stdout.isatty()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Back up every directory in one mounted filesystem using a dynamic "
            "pool of parallel dsmc processes."
        )
    )
    parser.add_argument("mountpoint", help="Mounted filesystem to process")
    parser.add_argument(
        "streams_positional",
        nargs="?",
        type=int,
        default=None,
        metavar="WORKERS",
        help=(
            "Number of parallel dsmc workers (positional shortcut). "
            "Ignored when --streams is also supplied."
        ),
    )
    parser.add_argument(
        "-n",
        "--streams",
        type=int,
        default=None,
        help=(
            "Parallel dsmc workers (default: 3). "
            "Takes precedence over the positional WORKERS argument."
        ),
    )
    parser.add_argument(
        "-b",
        "--batch-size",
        type=int,
        default=20,
        help="Directories reserved by each worker at a time (default: 20)",
    )
    parser.add_argument(
        "--queue-size",
        type=int,
        default=1000,
        help="Maximum queued directories; bounds memory usage (default: 1000)",
    )
    parser.add_argument(
        "--dsmc", default="dsmc", help="Path to dsmc executable (default: dsmc in PATH)"
    )
    parser.add_argument(
        "--dsmc-option",
        action="append",
        default=[],
        help=(
            "Additional dsmc option; repeat as needed. For options beginning '-' use "
            "--dsmc-option=-servername=NAME"
        ),
    )
    parser.add_argument(
        "--resourceutilization",
        type=int,
        default=2,
        help=(
            "dsmc resourceutilization value (default: 2, avoids internal multisession "
            "backup; valid range: 1-100)"
        ),
    )
    parser.add_argument(
        "--log-dir",
        default="./sp-parallel-logs",
        help="Directory for controller and dsmc logs",
    )
    parser.add_argument(
        "--progress-seconds",
        type=int,
        default=30,
        help="Plain progress reporting interval when dashboard is disabled (default: 30)",
    )
    parser.add_argument(
        "--dashboard-refresh-seconds",
        type=float,
        default=1.0,
        help="Dashboard refresh interval in seconds (default: 1.0)",
    )
    parser.add_argument(
        "--shutdown-wait-seconds",
        type=int,
        default=30,
        help="Seconds to wait for producer/workers to exit during shutdown (default: 30)",
    )
    parser.add_argument(
        "--no-dashboard",
        action="store_true",
        help="Disable live dashboard and use plain progress output",
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="Scan and schedule without running dsmc"
    )
    args = parser.parse_args()

    # Precedence: explicit --streams > positional WORKERS > built-in default (3)
    if args.streams is not None:
        pass  # explicit --streams wins; positional WORKERS is ignored
    elif args.streams_positional is not None:
        args.streams = args.streams_positional
    else:
        args.streams = 3

    if args.streams < 1:
        parser.error("--streams / WORKERS must be at least 1")
    if args.batch_size < 1:
        parser.error("--batch-size must be at least 1")
    if args.queue_size < args.streams:
        parser.error("--queue-size must be at least the number of streams")
    if not 1 <= args.resourceutilization <= 100:
        parser.error("--resourceutilization must be between 1 and 100 (inclusive)")
    if args.progress_seconds < 1:
        parser.error("--progress-seconds must be at least 1")
    if args.dashboard_refresh_seconds <= 0:
        parser.error("--dashboard-refresh-seconds must be greater than 0")
    if args.shutdown_wait_seconds < 1:
        parser.error("--shutdown-wait-seconds must be at least 1")

    return args


def main() -> int:
    args = parse_args()
    root = os.path.normpath(os.path.realpath(args.mountpoint))
    args.mountpoint = root
    args.log_dir = os.path.abspath(args.log_dir)

    if not os.path.isdir(root):
        print(f"ERROR: not a directory: {root}", file=sys.stderr)
        return 2

    if not args.dry_run:
        resolved_dsmc = shutil.which(args.dsmc)
        if resolved_dsmc is None:
            print(f"ERROR: dsmc executable not found: {args.dsmc}", file=sys.stderr)
            return 2
        args.dsmc = resolved_dsmc

    dashboard_enabled = should_enable_dashboard(args.no_dashboard)

    log_dir = Path(args.log_dir)
    log_dir.mkdir(parents=True, exist_ok=True)

    logger = SafeLogger(log_dir / "controller.log", echo=not dashboard_enabled)
    failed_path = log_dir / "failed-directories.tsv"
    if not failed_path.exists():
        failed_path.write_text("return_code\tdirectory\n", encoding="utf-8")
    failed_logger = SafeAppender(failed_path)
    counters = Counters()
    worker_states = WorkerStates(args.streams)
    work_queue: queue.Queue[str] = queue.Queue(maxsize=args.queue_size)
    producer_done = threading.Event()
    stop_event = threading.Event()

    logger.write(
        f"START root={root} streams={args.streams} batch_size={args.batch_size} "
        f"dry_run={args.dry_run} dashboard={dashboard_enabled}"
    )

    if root not in mounted_paths():
        logger.write(
            "WARNING: supplied path is not listed as a mountpoint; processing remains "
            "restricted to its current filesystem"
        )

    producer = threading.Thread(
        name="directory-scanner",
        target=scan_directories,
        args=(
            root,
            work_queue,
            producer_done,
            stop_event,
            counters,
            logger,
        ),
        daemon=True,
    )

    workers = [
        threading.Thread(
            name=f"worker-{number:02d}",
            target=worker,
            args=(
                number,
                args,
                work_queue,
                producer_done,
                stop_event,
                counters,
                worker_states,
                logger,
                failed_logger,
            ),
            daemon=True,
        )
        for number in range(1, args.streams + 1)
    ]

    reporter: threading.Thread | None = None
    dashboard: Dashboard | None = None
    if dashboard_enabled:
        dashboard = Dashboard(
            counters=counters,
            worker_states=worker_states,
            producer_done=producer_done,
            stop_event=stop_event,
            refresh_seconds=args.dashboard_refresh_seconds,
        )
        reporter = threading.Thread(
            name="dashboard",
            target=dashboard.run,
            daemon=True,
        )
    else:
        reporter = threading.Thread(
            name="progress-reporter",
            target=progress_reporter,
            args=(
                counters,
                producer_done,
                stop_event,
                args.progress_seconds,
            ),
            daemon=True,
        )

    try:
        for thread in workers:
            thread.start()
        reporter.start()
        producer.start()

        producer.join(timeout=args.shutdown_wait_seconds)
        if producer.is_alive():
            logger.write("WARNING: producer thread did not exit in time; requesting stop")
            stop_event.set()
            producer_done.set()
        if not wait_for_queue_drain(work_queue, args.shutdown_wait_seconds):
            logger.write("WARNING: work queue did not drain in time")
        for thread in workers:
            thread.join(timeout=args.shutdown_wait_seconds)
            if thread.is_alive():
                logger.write(f"WARNING: {thread.name} did not exit in time")
    except KeyboardInterrupt:
        logger.write("INTERRUPTED: verify whether any active dsmc child processes remain")
        stop_event.set()
        producer_done.set()
        return 130
    finally:
        stop_event.set()
        reporter.join(timeout=1)
        if dashboard is not None:
            dashboard.finish()

    discovered, completed, failed, skipped, errors = counters.snapshot()
    logger.write(
        f"END discovered={discovered} completed={completed} failed={failed} "
        f"skipped_mounts={skipped} scan_errors={errors}"
    )

    if not dashboard_enabled:
        print(
            f"PROGRESS discovered={discovered} completed={completed} "
            f"failed={failed} skipped_mounts={skipped} scan_errors={errors}",
            flush=True,
        )

    return 0 if failed == 0 and errors == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
