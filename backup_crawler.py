#!/usr/bin/env python3
"""
Run several IBM Storage Protect dsmc incremental processes against one mounted
filesystem without recursively crossing into nested directories/filesystems.

Each directory is submitted with a trailing slash and -subdir=no.  A shared,
bounded work queue gives the next batch to whichever worker finishes first.

Design notes
------------
* dsmc session start-up can take 10–20 s even for a single object; this is
  normal and does NOT indicate a hang.  Begin with a modest worker count (4 is
  a reasonable starting point) and measure throughput before increasing it.
* Very high worker counts can overload or throttle the SP client/server session
  pool and may reduce overall throughput.
* Per-worker dsmc processes are started with stdin=DEVNULL so they cannot block
  waiting for interactive input.
* Configurable hard (--dsmc-timeout) and idle (--dsmc-idle-timeout) timeouts
  allow safe termination of genuinely stalled processes.  Both default to 0
  (disabled) to avoid interrupting legitimate long-running backups.
* No SQLite database is used; all state is held in-memory and written to log
  files under --log-dir.

Special handling of the filesystem root (/)
-------------------------------------------
When the crawl entry point is exactly /, the script treats / as a traversal
anchor rather than an ordinary directory job:

1. / is never submitted to dsmc as a directory operand.  Passing ``/`` to
   ``dsmc incremental`` can be interpreted as a full filesystem/volume backup
   and may take orders of magnitude longer than a per-directory job.

2. A dedicated ROOT_FILES job (worker slot 0 in the dashboard) is started
   alongside the regular workers.  It collects every non-directory entry
   directly under / (plain files, device nodes, sockets, FIFOs, and symlinks)
   and invokes ``dsmc incremental`` with those entries listed as explicit file
   operands — for example::

       dsmc incremental -resourceutilization=2 /etc.conf /initrd.img /vmlinuz

   This avoids the ambiguous "/"  directory operand entirely.

3. Symlinks under / are included as link objects (backed up as symlinks rather
   than their targets) because ``entry.is_dir(follow_symlinks=False)`` returns
   False for all symlinks, even those pointing at directories.  This is
   consistent with how dsmc handles symlinks by default.

4. If there are no eligible non-directory entries directly under /, the
   ROOT_FILES job logs a skip reason and exits without invoking dsmc.

5. If the combined length of the file-path arguments would exceed
   MAX_ROOT_FILES_ARG_BYTES (128 KB), the list is split into multiple chunks,
   each invoked as a separate dsmc command.

6. All immediate child directories of / continue through the normal dynamic
   worker/batch queue as if they had been the entry point themselves.

7. The ROOT_FILES row in the dashboard shows: state, number of chunks,
   current chunk, child PID, runtime, last-output age, per-invocation return
   code, and aggregated dsmc statistics.
"""

from __future__ import annotations

import argparse
import dataclasses
import os
import queue
import re
import shutil
import signal
import subprocess
import sys
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

MAX_DSMC_SUCCESS_RC = 4
TIMEOUT_RC = 124        # Synthetic RC: hard wall-clock timeout (GNU timeout convention)
IDLE_TIMEOUT_RC = 125   # Synthetic RC: no-output (idle) timeout

# Dashboard: annotate a running worker as "quiet" after this many idle seconds.
# This means no dsmc output has been received, not that the process is hung.
QUIET_DISPLAY_THRESHOLD_SECS = 60.0

# Maximum combined byte length of explicit file-path arguments passed to dsmc
# in a single root-files chunk.  Stays well below the Linux ARG_MAX of 2 MB.
MAX_ROOT_FILES_ARG_BYTES = 128 * 1024

_UNIT_MULTIPLIERS: dict[str, int] = {
    "B": 1,
    "KB": 1024,
    "MB": 1024 ** 2,
    "GB": 1024 ** 3,
    "TB": 1024 ** 4,
}


# ---------------------------------------------------------------------------
# dsmc summary-line parsing
# ---------------------------------------------------------------------------

@dataclass
class DsmcInvocationStats:
    """Statistics parsed from one dsmc incremental invocation's output."""

    objects_inspected: int = 0
    objects_backed_up: int = 0
    objects_updated: int = 0
    objects_rebound: int = 0
    objects_deleted: int = 0
    objects_expired: int = 0
    objects_failed: int = 0
    objects_encrypted: int = 0
    objects_grew: int = 0
    retries: int = 0
    bytes_inspected: float = 0.0     # bytes
    bytes_transferred: float = 0.0   # bytes
    transfer_time_secs: float = 0.0
    network_rate_bps: float = 0.0    # bytes/sec
    aggregate_rate_bps: float = 0.0  # bytes/sec
    objects_compressed_pct: float = 0.0
    data_reduction_pct: float = 0.0
    elapsed_secs: float = 0.0

    def has_data(self) -> bool:
        return self.elapsed_secs > 0 or self.objects_inspected > 0 or self.bytes_inspected > 0


def _parse_int_field(s: str) -> int:
    return int(s.replace(",", "").replace(" ", ""))


def _parse_float_field(s: str) -> float:
    return float(s.replace(",", ""))


def _bytes_to_si(value_str: str, unit_str: str) -> float:
    """Convert a value + IBM unit string to raw bytes."""
    return _parse_float_field(value_str) * _UNIT_MULTIPLIERS.get(unit_str.strip().upper(), 1)


_RE_OBJ_COUNT = re.compile(
    r"total\s+number\s+of\s+objects\s+"
    r"(inspected|backed\s+up|updated|rebound|deleted|expired|failed|encrypted|grew)"
    r"\s*:\s*([\d,]+)",
    re.I,
)
_RE_RETRIES = re.compile(r"total\s+number\s+of\s+retries\s*:\s*([\d,]+)", re.I)
_RE_BYTES = re.compile(
    r"total\s+number\s+of\s+bytes\s+(inspected|transferred)"
    r"\s*:\s*([\d,.]+)\s*(B|KB|MB|GB|TB)\b",
    re.I,
)
_RE_TRANSFER_TIME = re.compile(r"data\s+transfer\s+time\s*:\s*([\d.]+)\s*sec", re.I)
_RE_NETWORK_RATE = re.compile(
    r"network\s+data\s+transfer\s+rate\s*:\s*([\d.]+)\s*(B|KB|MB|GB|TB)/sec", re.I
)
_RE_AGG_RATE = re.compile(
    r"aggregate\s+data\s+transfer\s+rate\s*:\s*([\d.]+)\s*(B|KB|MB|GB|TB)/sec", re.I
)
_RE_COMPRESSED = re.compile(r"objects\s+compressed\s+by\s*:\s*([\d.]+)\s*%", re.I)
_RE_REDUCTION = re.compile(r"total\s+data\s+reduction\s+ratio\s*:\s*([\d.]+)\s*%", re.I)
_RE_ELAPSED = re.compile(r"elapsed\s+processing\s+time\s*:\s*(\d+):(\d+):(\d+)", re.I)

_OBJ_FIELD_MAP = {
    "inspected": "objects_inspected",
    "backed up": "objects_backed_up",
    "updated": "objects_updated",
    "rebound": "objects_rebound",
    "deleted": "objects_deleted",
    "expired": "objects_expired",
    "failed": "objects_failed",
    "encrypted": "objects_encrypted",
    "grew": "objects_grew",
}


def parse_dsmc_summary_line(line: str, stats: DsmcInvocationStats) -> None:
    """Update *stats* in-place from a single dsmc output line.  No-op on non-matching lines."""
    m = _RE_OBJ_COUNT.search(line)
    if m:
        key = re.sub(r"\s+", " ", m.group(1).lower().strip())
        fname = _OBJ_FIELD_MAP.get(key)
        if fname:
            try:
                setattr(stats, fname, _parse_int_field(m.group(2)))
            except (ValueError, OverflowError):
                pass
        return

    m = _RE_RETRIES.search(line)
    if m:
        try:
            stats.retries = _parse_int_field(m.group(1))
        except (ValueError, OverflowError):
            pass
        return

    m = _RE_BYTES.search(line)
    if m:
        kind = m.group(1).lower().strip()
        try:
            val = _bytes_to_si(m.group(2), m.group(3))
        except (ValueError, KeyError):
            val = 0.0
        if kind == "inspected":
            stats.bytes_inspected = val
        elif kind == "transferred":
            stats.bytes_transferred = val
        return

    m = _RE_TRANSFER_TIME.search(line)
    if m:
        try:
            stats.transfer_time_secs = float(m.group(1))
        except ValueError:
            pass
        return

    m = _RE_NETWORK_RATE.search(line)
    if m:
        try:
            stats.network_rate_bps = _bytes_to_si(m.group(1), m.group(2))
        except (ValueError, KeyError):
            pass
        return

    m = _RE_AGG_RATE.search(line)
    if m:
        try:
            stats.aggregate_rate_bps = _bytes_to_si(m.group(1), m.group(2))
        except (ValueError, KeyError):
            pass
        return

    m = _RE_COMPRESSED.search(line)
    if m:
        try:
            stats.objects_compressed_pct = float(m.group(1))
        except ValueError:
            pass
        return

    m = _RE_REDUCTION.search(line)
    if m:
        try:
            stats.data_reduction_pct = float(m.group(1))
        except ValueError:
            pass
        return

    m = _RE_ELAPSED.search(line)
    if m:
        try:
            h, mi, s = int(m.group(1)), int(m.group(2)), int(m.group(3))
            stats.elapsed_secs = h * 3600 + mi * 60 + s
        except (ValueError, OverflowError):
            pass


def format_bytes(value: float) -> str:
    """Format a byte count as a concise human-readable string."""
    for unit in ("B", "KB", "MB", "GB"):
        if abs(value) < 1024.0:
            return f"{value:.2f} {unit}"
        value /= 1024.0
    return f"{value:.2f} TB"


def format_rate(bps: float) -> str:
    """Format a bytes/sec value as a human-readable rate string."""
    return format_bytes(bps) + "/s"


# ---------------------------------------------------------------------------
# Global aggregated dsmc statistics (thread-safe)
# ---------------------------------------------------------------------------

class GlobalDsmcStats:
    """Accumulates dsmc statistics across all invocations.  Thread-safe."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.objects_inspected: int = 0
        self.objects_backed_up: int = 0
        self.objects_updated: int = 0
        self.objects_failed: int = 0
        self.retries: int = 0
        self.bytes_inspected: float = 0.0
        self.bytes_transferred: float = 0.0
        # Sum of all per-invocation elapsed times; used for effective throughput.
        self.total_elapsed_secs: float = 0.0
        self.active_children: int = 0

    def add_invocation(self, stats: DsmcInvocationStats) -> None:
        with self._lock:
            self.objects_inspected += stats.objects_inspected
            self.objects_backed_up += stats.objects_backed_up
            self.objects_updated += stats.objects_updated
            self.objects_failed += stats.objects_failed
            self.retries += stats.retries
            self.bytes_inspected += stats.bytes_inspected
            self.bytes_transferred += stats.bytes_transferred
            self.total_elapsed_secs += stats.elapsed_secs

    def child_started(self) -> None:
        with self._lock:
            self.active_children += 1

    def child_finished(self) -> None:
        with self._lock:
            self.active_children = max(0, self.active_children - 1)

    def snapshot(self) -> dict:
        with self._lock:
            return {
                "objects_inspected": self.objects_inspected,
                "objects_backed_up": self.objects_backed_up,
                "objects_failed": self.objects_failed,
                "retries": self.retries,
                "bytes_inspected": self.bytes_inspected,
                "bytes_transferred": self.bytes_transferred,
                "total_elapsed_secs": self.total_elapsed_secs,
                "active_children": self.active_children,
            }


# ---------------------------------------------------------------------------
# Counters (discovered directories, outcomes, skip reasons)
# ---------------------------------------------------------------------------

@dataclass
class Counters:
    discovered: int = 0
    completed: int = 0
    failed: int = 0
    skipped_mounts: int = 0
    excluded_paths: int = 0
    scan_errors: int = 0
    lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    def add(self, name: str, value: int = 1) -> None:
        with self.lock:
            setattr(self, name, getattr(self, name) + value)

    def snapshot(self) -> tuple[int, int, int, int, int, int]:
        with self.lock:
            return (
                self.discovered,
                self.completed,
                self.failed,
                self.skipped_mounts,
                self.excluded_paths,
                self.scan_errors,
            )


# ---------------------------------------------------------------------------
# Per-worker state (dashboard-visible)
# ---------------------------------------------------------------------------

@dataclass
class WorkerState:
    worker_number: int
    # Lifecycle state; see WorkerStates transition methods for valid values.
    status: str = "idle"
    batch_id: int = 0
    batch_index: int = 0
    batch_total: int = 0
    current_directory: str = ""
    last_return_code: int | None = None
    batches_completed: int = 0
    dirs_completed: int = 0
    dirs_failed: int = 0
    dirs_timed_out: int = 0
    batch_start_time: float | None = None
    dir_start_time: float | None = None
    # Child process tracking
    child_pid: int | None = None
    child_start_time: float | None = None
    child_last_output_time: float | None = None
    # Latest per-invocation parsed stats (may be None)
    invocation_stats: DsmcInvocationStats | None = None


class WorkerStates:
    def __init__(self, streams: int, has_root_files_job: bool = False) -> None:
        self._lock = threading.Lock()
        self._next_batch_id = 0
        self._states: dict[int, WorkerState] = {}
        if has_root_files_job:
            # Slot 0 is reserved for the ROOT_FILES special job.
            self._states[0] = WorkerState(worker_number=0)
        for number in range(1, streams + 1):
            self._states[number] = WorkerState(worker_number=number)

    def start_batch(self, worker_number: int, total: int) -> None:
        with self._lock:
            self._next_batch_id += 1
            state = self._states[worker_number]
            state.status = "running"
            state.batch_id = self._next_batch_id
            state.batch_index = 0
            state.batch_total = total
            state.current_directory = ""
            state.last_return_code = None
            state.batch_start_time = time.monotonic()
            state.dir_start_time = None
            state.child_pid = None
            state.child_start_time = None
            state.child_last_output_time = None
            state.invocation_stats = None

    def set_directory(self, worker_number: int, index: int, path: str) -> None:
        """Called just before launching dsmc for a directory."""
        with self._lock:
            state = self._states[worker_number]
            state.status = "starting_dsmc"
            state.batch_index = index
            state.current_directory = path
            state.dir_start_time = time.monotonic()
            state.child_pid = None
            state.child_start_time = None
            state.child_last_output_time = None
            state.invocation_stats = None

    def set_child(self, worker_number: int, pid: int, start_time: float) -> None:
        """Called after Popen succeeds; records PID and transitions to running."""
        with self._lock:
            state = self._states[worker_number]
            state.status = "running"
            state.child_pid = pid
            state.child_start_time = start_time
            state.child_last_output_time = start_time

    def update_child_output_time(self, worker_number: int, ts: float) -> None:
        """Called by the reader thread each time dsmc produces output."""
        with self._lock:
            state = self._states[worker_number]
            state.child_last_output_time = ts
            # Reset quiet -> running when output is received
            if state.status == "quiet":
                state.status = "running"

    def mark_quiet(self, worker_number: int) -> None:
        """Called by the supervision loop when the child has been silent too long."""
        with self._lock:
            state = self._states[worker_number]
            if state.status == "running":
                state.status = "quiet"

    def set_result(
        self,
        worker_number: int,
        return_code: int,
        stats: DsmcInvocationStats | None = None,
    ) -> None:
        with self._lock:
            state = self._states[worker_number]
            state.last_return_code = return_code
            state.child_pid = None
            state.invocation_stats = stats
            if return_code in (TIMEOUT_RC, IDLE_TIMEOUT_RC):
                state.dirs_timed_out += 1
            elif return_code <= MAX_DSMC_SUCCESS_RC:
                state.dirs_completed += 1
            else:
                state.dirs_failed += 1

    def waiting(self, worker_number: int) -> None:
        with self._lock:
            state = self._states[worker_number]
            state.status = "waiting_for_work"
            state.batch_index = 0
            state.batch_total = 0
            state.current_directory = ""
            state.batch_start_time = None
            state.dir_start_time = None
            state.child_pid = None
            state.child_start_time = None
            state.child_last_output_time = None

    def idle(self, worker_number: int) -> None:
        """Called after all directories in a batch are processed."""
        with self._lock:
            state = self._states[worker_number]
            state.batches_completed += 1
            state.status = "finished_batch"
            state.batch_index = 0
            state.batch_total = 0
            state.current_directory = ""
            state.batch_start_time = None
            state.dir_start_time = None
            state.child_pid = None

    def stopped(self, worker_number: int) -> None:
        with self._lock:
            state = self._states[worker_number]
            state.status = "done"
            state.batch_index = 0
            state.batch_total = 0
            state.current_directory = ""
            state.batch_start_time = None
            state.dir_start_time = None
            state.child_pid = None

    def set_custom_status(
        self, worker_number: int, status: str, directory: str = ""
    ) -> None:
        """Set status and current_directory directly (for special jobs, e.g. ROOT_FILES)."""
        with self._lock:
            state = self._states[worker_number]
            state.status = status
            state.current_directory = directory

    def get_batch_id(self, worker_number: int) -> int:
        """Return the current batch ID for the given worker (thread-safe)."""
        with self._lock:
            return self._states[worker_number].batch_id

    def snapshot(self) -> list[WorkerState]:
        with self._lock:
            return [
                dataclasses.replace(state)
                for _, state in sorted(self._states.items(), key=lambda item: item[0])
            ]


# ---------------------------------------------------------------------------
# Live ASCII dashboard
# ---------------------------------------------------------------------------

class Dashboard:
    _STATUS_WIDTH = 16

    def __init__(
        self,
        counters: Counters,
        worker_states: WorkerStates,
        global_stats: GlobalDsmcStats,
        work_queue: "queue.Queue[str]",
        producer_done: threading.Event,
        stop_event: threading.Event,
        refresh_seconds: float,
    ) -> None:
        self.counters = counters
        self.worker_states = worker_states
        self.global_stats = global_stats
        self.work_queue = work_queue
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
        now = time.monotonic()
        terminal_width = shutil.get_terminal_size(fallback=(120, 20)).columns
        discovered, completed, failed, skipped, excluded, errors = self.counters.snapshot()
        states = self.worker_states.snapshot()
        gs = self.global_stats.snapshot()
        q_size = self.work_queue.qsize()
        q_max = self.work_queue.maxsize
        scanner_done = self.producer_done.is_set()

        in_progress = max(0, discovered - completed - failed - q_size)
        scanner_label = "DONE   " if scanner_done else "RUNNING"

        # Queue saturation indicator
        queue_full = q_max > 0 and q_size >= q_max
        q_str = f"q={q_size}/{q_max}"
        if queue_full:
            q_str += "[FULL]"

        # Percentage complete
        if scanner_done and discovered > 0:
            pct = min(100.0, (completed + failed) / discovered * 100)
            pct_str = f"  ({pct:.1f}%)"
        else:
            pct_str = "  (scanning, total growing)" if not scanner_done else ""

        # Effective aggregate throughput
        total_elapsed = gs["total_elapsed_secs"]
        if total_elapsed > 0 and gs["bytes_transferred"] > 0:
            rate_str = f"  rate={format_rate(gs['bytes_transferred'] / total_elapsed)}"
        else:
            rate_str = ""

        sep = "-" * min(terminal_width, 80)

        lines = [
            (
                f"Scanner: {scanner_label}  found={discovered}  {q_str}  "
                f"in-prog={in_progress}  excl={excluded}  "
                f"skipped={skipped}  errors={errors}"
            ),
            f"Overall: completed={completed}  failed={failed}{pct_str}",
            (
                f"dsmc:  insp={gs['objects_inspected']}  bkup={gs['objects_backed_up']}  "
                f"fail={gs['objects_failed']}  retries={gs['retries']}  "
                f"bytes_i={format_bytes(gs['bytes_inspected'])}  "
                f"bytes_x={format_bytes(gs['bytes_transferred'])}"
                f"  children={gs['active_children']}{rate_str}"
            ),
            sep,
        ]

        # Per-worker rows (worker 0 = ROOT_FILES special job, shown first when present)
        bar_width = 10
        # Layout (fixed-width prefix before path):
        # label + sp = 11 (ROOT_FILES=10 chars; regular workers such as W01 are padded)
        # [bar] + sp = bar_width+3
        # pos + sp = 7+1 = 8
        # status + sp = STATUS_WIDTH+1
        # b#NNN + sp = 6
        # pid=NNNNN + sp = 10
        # rt:NNN.Ns + sp = 10
        # idle:NNN.Ns + sp = 12
        # ok:NN to:NN fl:NN + sp = 18
        # rc=NNN + sp = 7
        # path (remaining)
        static_width = 11 + (bar_width + 3) + 8 + (self._STATUS_WIDTH + 1) + 6 + 10 + 10 + 12 + 18 + 7
        path_width = max(8, terminal_width - static_width)

        for state in states:
            # Worker slot 0 is the ROOT_FILES special job; all others are regular workers.
            label = "ROOT_FILES" if state.worker_number == 0 else f"W{state.worker_number:02d}"

            pos = (
                f"{state.batch_index}/{state.batch_total}"
                if state.batch_total > 0
                else "--/--"
            )

            # Elapsed time for current directory, or batch elapsed while between dirs
            if state.dir_start_time is not None:
                time_str = f"{now - state.dir_start_time:.1f}s"
            elif state.batch_start_time is not None:
                time_str = f"~{now - state.batch_start_time:.1f}s"
            else:
                time_str = ""

            # Age since last child output (idle time)
            if state.child_last_output_time is not None:
                idle_secs = now - state.child_last_output_time
                idle_str = f"{idle_secs:.1f}s"
            else:
                idle_str = ""

            rc_str = (
                f"rc={state.last_return_code}"
                if state.last_return_code is not None
                else ""
            )
            b_str = f"b#{state.batch_id}" if state.batch_id > 0 else ""
            pid_str = f"pid={state.child_pid}" if state.child_pid is not None else ""
            stats_str = (
                f"ok:{state.dirs_completed} to:{state.dirs_timed_out} fl:{state.dirs_failed}"
            )

            # Display status; annotate running workers that have been quiet
            display_status = state.status
            if state.status in ("running", "quiet"):
                if (
                    state.child_last_output_time is not None
                    and (now - state.child_last_output_time) >= QUIET_DISPLAY_THRESHOLD_SECS
                ):
                    display_status = "quiet"
                else:
                    display_status = "running"

            bar = (
                f"[{self._bar(state.batch_index, state.batch_total, bar_width)}]"
                if state.status in ("running", "quiet", "starting_dsmc")
                else " " * (bar_width + 2)
            )

            prefix = (
                f"{label:<10} "
                f"{bar} "
                f"{pos:>7} "
                f"{display_status:<{self._STATUS_WIDTH}} "
                f"{b_str:<5} "
                f"{pid_str:<9} "
                f"rt:{time_str:<7} "
                f"idle:{idle_str:<7} "
                f"{stats_str:<17} "
                f"{rc_str:<6} "
            )
            path = self._truncate(state.current_directory, path_width)
            lines.append(prefix + path)

        if self._rendered_lines:
            sys.stdout.write(f"\x1b[{self._rendered_lines}F")
        for line in lines:
            sys.stdout.write("\x1b[2K" + line + "\n")
        sys.stdout.flush()
        self._rendered_lines = len(lines)

    def run(self) -> None:
        while not self.stop_event.wait(self.refresh_seconds):
            self._render()
            discovered, completed, failed, _, _, _ = self.counters.snapshot()
            if self.producer_done.is_set() and completed + failed >= discovered:
                return

    def finish(self) -> None:
        self._render()


# ---------------------------------------------------------------------------
# Logging helpers
# ---------------------------------------------------------------------------

class SafeLogger:
    """Append-only timestamped log; optionally echos to stdout."""

    def __init__(self, path: Path, echo: bool = True) -> None:
        self.path = path
        self.echo = echo
        self.lock = threading.Lock()

    def write(self, message: str) -> None:
        stamp = time.strftime("%Y-%m-%d %H:%M:%S")
        line = f"{stamp} {message}\n"
        with self.lock:
            with self.path.open("a", encoding="utf-8", errors="backslashreplace") as fh:
                fh.write(line)
        if self.echo:
            print(line, end="", flush=True)


class SafeAppender:
    """Append-only file writer; safe for concurrent use."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self.lock = threading.Lock()

    def write(self, line: str) -> None:
        with self.lock:
            with self.path.open("a", encoding="utf-8", errors="backslashreplace") as fh:
                fh.write(line.rstrip("\n") + "\n")


# ---------------------------------------------------------------------------
# Filesystem / mount utilities
# ---------------------------------------------------------------------------

_MOUNT_ESCAPE = re.compile(r"\\([0-7]{3})")
def decode_mountinfo_path(value: str) -> str:
    """Decode octal escapes in /proc/self/mountinfo paths (e.g. \\040 for space)."""
    return _MOUNT_ESCAPE.sub(lambda m: chr(int(m.group(1), 8)), value)


def mounted_paths() -> set[str]:
    paths: set[str] = set()
    try:
        with open("/proc/self/mountinfo", "r", encoding="utf-8") as fh:
            for line in fh:
                fields = line.split()
                if len(fields) >= 5:
                    mountpoint = decode_mountinfo_path(fields[4])
                    paths.add(os.path.normpath(os.path.realpath(mountpoint)))
    except OSError:
        pass
    return paths


def is_within(root: str, candidate: str) -> bool:
    try:
        return os.path.commonpath((root, candidate)) == root
    except ValueError:
        return False


def nested_mounts(root: str) -> set[str]:
    return {p for p in mounted_paths() if p != root and is_within(root, p)}


# ---------------------------------------------------------------------------
# Path exclusion utilities
# ---------------------------------------------------------------------------

def is_path_excluded(path: str, excluded_paths: frozenset[str]) -> bool:
    """
    Return True if *path* equals or is a descendant of any path in *excluded_paths*.
    Uses Path.relative_to for correct boundary-aware containment (not string prefix).
    """
    p = Path(path)
    for excl in excluded_paths:
        try:
            p.relative_to(excl)
            return True
        except ValueError:
            continue
    return False


# ---------------------------------------------------------------------------
# Scanner (producer thread)
# ---------------------------------------------------------------------------

def put_with_stop(
    work_queue: "queue.Queue[str]", path: str, stop_event: threading.Event
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
    work_queue: "queue.Queue[str]",
    producer_done: threading.Event,
    stop_event: threading.Event,
    counters: Counters,
    excluded_paths: frozenset[str],
    logger: SafeLogger,
) -> None:
    """Iteratively scan directories, staying on the root filesystem, honouring exclusions."""
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

            # When the crawl root is the filesystem root (/), do not enqueue it
            # as an ordinary directory job.  The dedicated root-files job handles
            # non-directory entries directly under / using explicit file operands.
            # All child directories of / continue through the normal queue.
            if current != os.sep:
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

                            # Exclusion check (path-aware, catches log dir and user paths)
                            if is_path_excluded(child, excluded_paths):
                                counters.add("excluded_paths")
                                logger.write(f"SKIP excluded path: {child}")
                                continue

                            # Explicit mountpoint detection catches bind mounts too.
                            if child in mount_boundaries:
                                counters.add("skipped_mounts")
                                logger.write(f"SKIP nested mount: {child}")
                                continue

                            # Device check is cheap and catches ordinary mounts.
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


# ---------------------------------------------------------------------------
# Worker queue helpers
# ---------------------------------------------------------------------------

def dsm_directory_operand(path: str) -> str:
    """A trailing slash tells dsmc to process the directory's immediate contents."""
    if path == os.sep:
        return path
    return path.rstrip(os.sep) + os.sep


# ---------------------------------------------------------------------------
# Root-files job helpers
# ---------------------------------------------------------------------------

def collect_root_files(root: str, excluded_paths: frozenset) -> list[str]:
    """
    Return a sorted list of non-directory entries directly under *root*.

    Symlinks pointing at directories are treated as non-directories here
    (follow_symlinks=False) and are included — dsmc backs them up as link
    objects rather than descending into their targets.  Symlinks to files
    are included in the same way.

    Entries that match *excluded_paths* are omitted.
    """
    files: list[str] = []
    try:
        with os.scandir(root) as it:
            for entry in it:
                try:
                    if entry.is_dir(follow_symlinks=False):
                        continue  # directories are handled by the normal work queue
                    path = os.path.normpath(entry.path)
                    if is_path_excluded(path, excluded_paths):
                        continue
                    files.append(path)
                except OSError:
                    pass
    except OSError:
        pass
    return sorted(files)


def chunk_root_files(
    files: list[str],
    max_bytes: int = MAX_ROOT_FILES_ARG_BYTES,
) -> list[list[str]]:
    """
    Split *files* into argv-safe chunks so that the total UTF-8 byte length
    of each chunk stays within *max_bytes*.  Every file path appears in
    exactly one chunk.  An empty *files* list produces an empty result.
    """
    if not files:
        return []
    chunks: list[list[str]] = []
    current: list[str] = []
    current_bytes = 0
    for path in files:
        # +1 for the null-byte separator in the kernel's execve argv accounting
        cost = len(path.encode("utf-8", errors="surrogateescape")) + 1
        if current and current_bytes + cost > max_bytes:
            chunks.append(current)
            current = [path]
            current_bytes = cost
        else:
            current.append(path)
            current_bytes += cost
    if current:
        chunks.append(current)
    return chunks


def run_root_files_job(
    args: argparse.Namespace,
    root: str,
    excluded_paths: frozenset,
    worker_states: "WorkerStates",
    global_stats: "GlobalDsmcStats",
    logger: "SafeLogger",
    failed_logger: "SafeAppender",
    stop_event: threading.Event,
) -> None:
    """
    Dedicated thread that backs up non-directory entries directly under the
    filesystem root (/).

    Strategy
    --------
    * Collect every non-directory entry under ``root`` (symlinks included as
      link objects, not descended into).
    * Split the list into argv-safe chunks (≤ MAX_ROOT_FILES_ARG_BYTES each).
    * For each chunk invoke::

          dsmc incremental -resourceutilization=N [extra-opts] /file1 /file2 …

      Explicit file operands avoid passing the ambiguous ``/`` directory
      operand to dsmc, which IBM Storage Protect may interpret as a full
      filesystem/volume backup and which can take orders of magnitude longer
      than an ordinary directory-content job.
    * If there are no eligible files the job is marked *skipped* and dsmc is
      never invoked.

    Dashboard visibility
    --------------------
    Uses worker slot 0 in *worker_states* so the dashboard renders it as the
    ``ROOT_FILES`` row above the regular worker rows.
    """
    JOB_NAME = "ROOT_FILES"
    WORKER_SLOT = 0

    worker_log = Path(args.log_dir) / "root-files-job.log"
    dsm_log_dir = Path(args.log_dir) / "root-files-dsm"
    dsm_log_dir.mkdir(parents=True, exist_ok=True)

    environment = os.environ.copy()
    environment["DSM_LOG"] = str(dsm_log_dir)

    # ---- Step 1: collect eligible files ----
    worker_states.set_custom_status(WORKER_SLOT, "scanning", f"scanning {root}")
    files = collect_root_files(root, excluded_paths)
    logger.write(
        f"{JOB_NAME}: found {len(files)} eligible non-directory "
        f"{'entry' if len(files) == 1 else 'entries'} under {root!r}"
    )

    if not files:
        logger.write(f"{JOB_NAME}: no eligible files directly under {root!r}; job skipped")
        worker_states.set_custom_status(WORKER_SLOT, "skipped", "no files under /")
        return

    chunks = chunk_root_files(files)
    total_files = len(files)
    total_chunks = len(chunks)
    logger.write(f"{JOB_NAME}: {total_files} file(s) → {total_chunks} chunk(s)")

    # ---- Step 2: initialise dashboard slot ----
    worker_states.start_batch(WORKER_SLOT, total_chunks)
    worker_states.set_custom_status(
        WORKER_SLOT,
        "running",
        f"{total_files} file(s), {total_chunks} chunk(s)",
    )

    try:
        with worker_log.open("ab", buffering=0) as output:
            output.write(
                (
                    f"\n===== {time.strftime('%Y-%m-%d %H:%M:%S')} =====\n"
                    f"ROOT_FILES job: {total_files} file(s), {total_chunks} chunk(s)\n"
                ).encode("utf-8", errors="backslashreplace")
            )

            for chunk_idx, chunk_files in enumerate(chunks, start=1):
                if stop_event.is_set():
                    break

                label = f"chunk {chunk_idx}/{total_chunks}"
                worker_states.set_directory(WORKER_SLOT, chunk_idx, label)

                # Build explicit-file command.  -subdir=no is not needed because
                # we are passing individual file paths, not a directory operand.
                command = [
                    args.dsmc,
                    "incremental",
                    f"-resourceutilization={args.resourceutilization}",
                    *args.dsmc_option,
                    *chunk_files,
                ]

                output.write(
                    (
                        f"\n--- {time.strftime('%Y-%m-%d %H:%M:%S')} ---\n"
                        f"{JOB_NAME} {label}: {len(chunk_files)} file(s)\n"
                    ).encode("utf-8", errors="backslashreplace")
                )

                return_code = 0
                inv_stats = DsmcInvocationStats()
                try:
                    if args.dry_run:
                        output.write(
                            ("DRY RUN: " + repr(command) + "\n").encode(
                                "utf-8", errors="backslashreplace"
                            )
                        )
                    else:
                        return_code, inv_stats = run_dsmc_supervised(
                            command=command,
                            env=environment,
                            output_file=output,
                            worker_number=WORKER_SLOT,
                            worker_states=worker_states,
                            global_stats=global_stats,
                            logger=logger,
                            worker_name=JOB_NAME,
                            dsmc_timeout=args.dsmc_timeout,
                            dsmc_idle_timeout=args.dsmc_idle_timeout,
                            stop_event=stop_event,
                        )
                        if inv_stats.has_data():
                            global_stats.add_invocation(inv_stats)
                except OSError as exc:
                    return_code = 127
                    logger.write(f"{JOB_NAME}: OS error running dsmc: {exc}")

                worker_states.set_result(WORKER_SLOT, return_code, inv_stats)

                if return_code == TIMEOUT_RC:
                    logger.write(f"{JOB_NAME}: HARD TIMEOUT on {label}")
                    failed_logger.write(
                        f"{return_code}\t{root} [{label}]\t# hard timeout"
                    )
                elif return_code == IDLE_TIMEOUT_RC:
                    logger.write(f"{JOB_NAME}: IDLE TIMEOUT on {label}")
                    failed_logger.write(
                        f"{return_code}\t{root} [{label}]\t# idle timeout"
                    )
                elif return_code <= MAX_DSMC_SUCCESS_RC:
                    logger.write(
                        f"{JOB_NAME}: completed {label} rc={return_code}"
                    )
                else:
                    logger.write(
                        f"{JOB_NAME}: FAILED {label} rc={return_code}"
                    )
                    failed_logger.write(f"{return_code}\t{root} [{label}]")

    finally:
        worker_states.idle(WORKER_SLOT)
        worker_states.stopped(WORKER_SLOT)


def get_batch(
    work_queue: "queue.Queue[str]",
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


# ---------------------------------------------------------------------------
# Process group management (POSIX / Windows)
# ---------------------------------------------------------------------------

def _kill_process_group(
    proc: subprocess.Popen,
    logger: SafeLogger,
    worker_name: str,
    reason: str,
) -> None:
    """
    Send SIGTERM to the process group (POSIX) or terminate the process (Windows),
    wait up to 2 s, then SIGKILL / kill if still alive.
    """
    _posix = hasattr(os, "killpg") and hasattr(signal, "SIGTERM")
    try:
        if _posix:
            try:
                pgid = os.getpgid(proc.pid)
            except ProcessLookupError:
                return
            try:
                os.killpg(pgid, signal.SIGTERM)
                logger.write(f"{worker_name}: SIGTERM → process group {pgid} ({reason})")
            except ProcessLookupError:
                return
            except OSError:
                pass
            # Wait up to 2 s for graceful exit
            deadline = time.monotonic() + 2.0
            while time.monotonic() < deadline:
                if proc.poll() is not None:
                    return
                time.sleep(0.1)
            # Force-kill
            try:
                pgid = os.getpgid(proc.pid)
                os.killpg(pgid, signal.SIGKILL)
                logger.write(f"{worker_name}: SIGKILL → process group {pgid}")
            except (ProcessLookupError, OSError):
                pass
        else:
            proc.terminate()
            deadline = time.monotonic() + 2.0
            while time.monotonic() < deadline:
                if proc.poll() is not None:
                    return
                time.sleep(0.1)
            proc.kill()
    except OSError:
        pass
    finally:
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            pass


# ---------------------------------------------------------------------------
# Supervised dsmc runner
# ---------------------------------------------------------------------------

def run_dsmc_supervised(
    command: list[str],
    env: dict,
    output_file,
    worker_number: int,
    worker_states: WorkerStates,
    global_stats: GlobalDsmcStats,
    logger: SafeLogger,
    worker_name: str,
    dsmc_timeout: float,
    dsmc_idle_timeout: float,
    stop_event: threading.Event,
) -> tuple[int, DsmcInvocationStats]:
    """
    Launch dsmc with stdin=DEVNULL, stream combined output, supervise timeouts.

    Returns (return_code, DsmcInvocationStats).
    On timeout the child process group is killed and TIMEOUT_RC / IDLE_TIMEOUT_RC
    is returned so the worker can continue with subsequent directories.
    """
    stats = DsmcInvocationStats()
    start_time = time.monotonic()
    # Mutable holder so the reader thread can update it
    last_output_ts: list[float] = [start_time]

    popen_kwargs: dict = {
        "stdout": subprocess.PIPE,
        "stderr": subprocess.STDOUT,
        "stdin": subprocess.DEVNULL,
        "env": env,
    }
    if hasattr(os, "setsid"):
        popen_kwargs["preexec_fn"] = os.setsid

    try:
        proc = subprocess.Popen(command, **popen_kwargs)
    except OSError as exc:
        logger.write(f"{worker_name}: failed to start dsmc: {exc}")
        return 127, stats

    worker_states.set_child(worker_number, proc.pid, start_time)
    global_stats.child_started()

    reader_done = threading.Event()

    def _reader() -> None:
        buf = b""
        try:
            assert proc.stdout is not None
            while True:
                chunk = proc.stdout.read(4096)
                if not chunk:
                    break
                now = time.monotonic()
                last_output_ts[0] = now
                worker_states.update_child_output_time(worker_number, now)
                output_file.write(chunk)
                buf += chunk
                while b"\n" in buf:
                    raw_line, buf = buf.split(b"\n", 1)
                    parse_dsmc_summary_line(
                        raw_line.decode("utf-8", errors="replace"), stats
                    )
            if buf:
                parse_dsmc_summary_line(buf.decode("utf-8", errors="replace"), stats)
        finally:
            reader_done.set()

    reader_thread = threading.Thread(target=_reader, daemon=True)
    reader_thread.start()

    timed_out_reason: str | None = None

    try:
        while True:
            now = time.monotonic()
            elapsed = now - start_time
            idle = now - last_output_ts[0]

            # Ctrl-C / explicit stop
            if stop_event.is_set():
                timed_out_reason = "stop requested"
                break

            # Hard wall-clock timeout
            if dsmc_timeout > 0 and elapsed >= dsmc_timeout:
                timed_out_reason = (
                    f"hard timeout ({elapsed:.0f}s >= {dsmc_timeout:.0f}s)"
                )
                break

            # Idle (no-output) timeout
            if dsmc_idle_timeout > 0 and idle >= dsmc_idle_timeout:
                timed_out_reason = (
                    f"idle timeout ({idle:.0f}s >= {dsmc_idle_timeout:.0f}s)"
                )
                break

            # Update quiet display state
            if idle >= QUIET_DISPLAY_THRESHOLD_SECS:
                worker_states.mark_quiet(worker_number)

            # Process exited normally
            rc = proc.poll()
            if rc is not None:
                reader_thread.join(timeout=10)
                return rc, stats

            # Compute next sleep; bound by nearest timeout
            sleep_time = 0.2
            if dsmc_timeout > 0:
                sleep_time = min(sleep_time, max(0.01, dsmc_timeout - elapsed))
            if dsmc_idle_timeout > 0:
                sleep_time = min(sleep_time, max(0.01, dsmc_idle_timeout - idle))
            time.sleep(sleep_time)

    finally:
        global_stats.child_finished()

    # Reached only on timeout or stop
    is_idle_timeout = timed_out_reason is not None and "idle" in timed_out_reason
    synthetic_rc = IDLE_TIMEOUT_RC if is_idle_timeout else TIMEOUT_RC
    logger.write(
        f"{worker_name}: killing dsmc pid={proc.pid} ({timed_out_reason})"
    )
    _kill_process_group(proc, logger, worker_name, timed_out_reason or "timeout")
    reader_thread.join(timeout=5)
    return synthetic_rc, stats


# ---------------------------------------------------------------------------
# Worker thread
# ---------------------------------------------------------------------------

def worker(
    worker_number: int,
    args: argparse.Namespace,
    work_queue: "queue.Queue[str]",
    producer_done: threading.Event,
    stop_event: threading.Event,
    counters: Counters,
    worker_states: WorkerStates,
    global_stats: GlobalDsmcStats,
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
            worker_states.waiting(worker_number)
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
                f"{'directory' if len(batch) == 1 else 'directories'} "
                f"(batch #{worker_states.get_batch_id(worker_number)})"
            )

            try:
                with worker_log.open("ab", buffering=0) as output:
                    output.write(
                        (
                            f"\n===== {time.strftime('%Y-%m-%d %H:%M:%S')} =====\n"
                            f"Batch size: {len(batch)}\n"
                            + "\n".join(
                                dsm_directory_operand(p) for p in batch
                            )
                            + "\n"
                        ).encode("utf-8", errors="backslashreplace")
                    )

                    for index, path in enumerate(batch, start=1):
                        if stop_event.is_set():
                            break

                        worker_states.set_directory(worker_number, index, path)
                        operand = dsm_directory_operand(path)
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
                        inv_stats = DsmcInvocationStats()
                        try:
                            if args.dry_run:
                                output.write(
                                    ("DRY RUN: " + repr(command) + "\n").encode(
                                        "utf-8", errors="backslashreplace"
                                    )
                                )
                            else:
                                return_code, inv_stats = run_dsmc_supervised(
                                    command=command,
                                    env=environment,
                                    output_file=output,
                                    worker_number=worker_number,
                                    worker_states=worker_states,
                                    global_stats=global_stats,
                                    logger=logger,
                                    worker_name=worker_name,
                                    dsmc_timeout=args.dsmc_timeout,
                                    dsmc_idle_timeout=args.dsmc_idle_timeout,
                                    stop_event=stop_event,
                                )
                                if inv_stats.has_data():
                                    global_stats.add_invocation(inv_stats)
                        except OSError as exc:
                            return_code = 127
                            logger.write(f"{worker_name}: OS error running dsmc: {exc}")

                        worker_states.set_result(worker_number, return_code, inv_stats)

                        if return_code == TIMEOUT_RC:
                            counters.add("failed")
                            logger.write(
                                f"{worker_name}: HARD TIMEOUT {index}/{len(batch)} path={path}"
                            )
                            failed_logger.write(
                                f"{return_code}\t{path}\t# hard timeout"
                            )
                        elif return_code == IDLE_TIMEOUT_RC:
                            counters.add("failed")
                            logger.write(
                                f"{worker_name}: IDLE TIMEOUT {index}/{len(batch)} path={path}"
                            )
                            failed_logger.write(
                                f"{return_code}\t{path}\t# idle timeout"
                            )
                        elif return_code <= MAX_DSMC_SUCCESS_RC:
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


# ---------------------------------------------------------------------------
# Progress reporter (non-TTY fallback)
# ---------------------------------------------------------------------------

def progress_reporter(
    counters: Counters,
    global_stats: GlobalDsmcStats,
    work_queue: "queue.Queue[str]",
    producer_done: threading.Event,
    stop_event: threading.Event,
    interval: int,
) -> None:
    while not stop_event.wait(interval):
        discovered, completed, failed, skipped, excluded, errors = counters.snapshot()
        q_size = work_queue.qsize()
        in_progress = max(0, discovered - completed - failed - q_size)
        gs = global_stats.snapshot()
        print(
            f"PROGRESS discovered={discovered} q={q_size} in-prog={in_progress} "
            f"excl={excluded} completed={completed} failed={failed} "
            f"skipped_mounts={skipped} scan_errors={errors} "
            f"children={gs['active_children']} "
            f"insp={gs['objects_inspected']} bkup={gs['objects_backed_up']} "
            f"bytes_i={format_bytes(gs['bytes_inspected'])} "
            f"bytes_x={format_bytes(gs['bytes_transferred'])}",
            flush=True,
        )
        if producer_done.is_set() and completed + failed >= discovered:
            return


def should_enable_dashboard(dashboard_disabled: bool) -> bool:
    return not dashboard_disabled and sys.stdout.isatty()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Back up every directory in one mounted filesystem using a dynamic "
            "pool of parallel dsmc processes.\n\n"
            "NOTE: Individual dsmc invocations have substantial session start-up "
            "latency (often 10–20 s even for a single object). Begin with a modest "
            "worker count (4 is a good starting point) and measure throughput "
            "before increasing it."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
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
            "Additional dsmc option; repeat as needed. "
            "For options beginning '-' use --dsmc-option=-servername=NAME"
        ),
    )
    parser.add_argument(
        "--dsmc-timeout",
        type=float,
        default=0,
        metavar="SECONDS",
        help=(
            "Hard wall-clock timeout for one dsmc directory invocation in seconds "
            "(0 = disabled, default: 0). "
            "Conservative recommendation: leave disabled unless you observe genuine hangs."
        ),
    )
    parser.add_argument(
        "--dsmc-idle-timeout",
        type=float,
        default=0,
        metavar="SECONDS",
        help=(
            "Timeout when dsmc produces no output for this many seconds "
            "(0 = disabled, default: 0). "
            "dsmc can legitimately stay quiet while processing large files."
        ),
    )
    parser.add_argument(
        "--resourceutilization",
        type=int,
        default=2,
        help=(
            "dsmc resourceutilization value (default: 2, avoids internal "
            "multisession backup; valid range: 1-100)"
        ),
    )
    parser.add_argument(
        "--log-dir",
        default="./sp-parallel-logs",
        help="Directory for controller and dsmc logs (default: ./sp-parallel-logs)",
    )
    parser.add_argument(
        "--exclude-path",
        action="append",
        default=[],
        dest="exclude_path",
        metavar="PATH",
        help=(
            "Exclude PATH and all its subdirectories from crawling. "
            "Repeatable. The log directory is excluded automatically when it "
            "falls within the crawl root."
        ),
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
        help=(
            "Seconds to wait for workers to finish after a Ctrl-C interrupt "
            "(default: 30). Not used during normal (non-interrupted) runs."
        ),
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
    if args.streams is None:
        if args.streams_positional is not None:
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
    if args.dsmc_timeout < 0:
        parser.error("--dsmc-timeout must be >= 0 (0 = disabled)")
    if args.dsmc_idle_timeout < 0:
        parser.error("--dsmc-idle-timeout must be >= 0 (0 = disabled)")

    return args


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

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

    # ---- Build exclusion set ----
    excluded_paths_set: set[str] = set()

    log_dir_norm = os.path.normpath(args.log_dir)
    # Auto-exclude log directory if it falls within the crawl root
    if is_path_excluded(log_dir_norm, frozenset([root])):
        excluded_paths_set.add(log_dir_norm)
        # Will be logged after SafeLogger is created

    for raw_excl in args.exclude_path:
        norm = os.path.normpath(os.path.abspath(raw_excl))
        if norm == root:
            print(
                f"WARNING: --exclude-path {raw_excl!r} resolves to the crawl root; "
                "ignoring to avoid excluding all work",
                file=sys.stderr,
            )
            continue
        excluded_paths_set.add(norm)

    excluded_paths = frozenset(excluded_paths_set)

    # ---- Set up log directory and shared objects ----
    dashboard_enabled = should_enable_dashboard(args.no_dashboard)

    log_dir = Path(args.log_dir)
    log_dir.mkdir(parents=True, exist_ok=True)

    logger = SafeLogger(log_dir / "controller.log", echo=not dashboard_enabled)
    failed_path = log_dir / "failed-directories.tsv"
    if not failed_path.exists():
        failed_path.write_text(
            "return_code\tdirectory\tnotes\n", encoding="utf-8"
        )
    failed_logger = SafeAppender(failed_path)
    counters = Counters()
    global_stats = GlobalDsmcStats()
    # When the crawl root is /, allocate slot 0 for the ROOT_FILES special job.
    is_root_crawl = (root == os.sep)
    worker_states = WorkerStates(args.streams, has_root_files_job=is_root_crawl)
    work_queue: queue.Queue[str] = queue.Queue(maxsize=args.queue_size)
    producer_done = threading.Event()
    stop_event = threading.Event()

    logger.write(
        f"START root={root} streams={args.streams} batch_size={args.batch_size} "
        f"dry_run={args.dry_run} dashboard={dashboard_enabled} "
        f"dsmc_timeout={args.dsmc_timeout} dsmc_idle_timeout={args.dsmc_idle_timeout} "
        f"root_files_job={is_root_crawl}"
    )
    if is_root_crawl:
        logger.write(
            "INFO: crawl root is /; / will not be submitted as an ordinary directory job. "
            "Non-directory entries under / are handled by the dedicated ROOT_FILES job."
        )

    if log_dir_norm in excluded_paths_set:
        logger.write(
            f"INFO: log directory {log_dir_norm!r} is inside crawl root; "
            "automatically excluded from scanning"
        )

    for excl in sorted(excluded_paths_set - {log_dir_norm}):
        logger.write(f"INFO: user-specified exclusion: {excl!r}")

    if root not in mounted_paths():
        logger.write(
            "WARNING: supplied path is not listed as a mountpoint; "
            "processing remains restricted to its current filesystem"
        )

    # ---- Thread construction ----
    producer = threading.Thread(
        name="directory-scanner",
        target=scan_directories,
        args=(
            root,
            work_queue,
            producer_done,
            stop_event,
            counters,
            excluded_paths,
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
                global_stats,
                logger,
                failed_logger,
            ),
            daemon=True,
        )
        for number in range(1, args.streams + 1)
    ]

    # When crawl root is /, start the dedicated ROOT_FILES job thread.
    root_files_thread: threading.Thread | None = None
    if is_root_crawl:
        root_files_thread = threading.Thread(
            name="root-files-job",
            target=run_root_files_job,
            args=(
                args,
                root,
                excluded_paths,
                worker_states,
                global_stats,
                logger,
                failed_logger,
                stop_event,
            ),
            daemon=False,  # not a daemon: we explicitly join it to ensure proper accounting
        )

    reporter: threading.Thread | None = None
    dashboard: Dashboard | None = None
    if dashboard_enabled:
        dashboard = Dashboard(
            counters=counters,
            worker_states=worker_states,
            global_stats=global_stats,
            work_queue=work_queue,
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
                global_stats,
                work_queue,
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
        if root_files_thread is not None:
            root_files_thread.start()

        # Normal flow: wait for the scanner to finish (no timeout — scans can take
        # arbitrarily long and a fixed timeout would prematurely stop large runs).
        producer.join()

        # Wait for every enqueued item to be processed (task_done called by workers).
        work_queue.join()

        # Workers should exit quickly once the queue is drained and producer_done is set.
        for thread in workers:
            thread.join()

        # The root-files job runs in parallel with the scanner and workers and may
        # finish at any point; wait for it here to ensure complete accounting.
        if root_files_thread is not None:
            root_files_thread.join()

    except KeyboardInterrupt:
        logger.write("INTERRUPTED: requesting stop; terminating active dsmc children")
        stop_event.set()
        producer_done.set()
        # Workers check stop_event; run_dsmc_supervised will kill child processes.
        for thread in [*workers, root_files_thread] if root_files_thread else workers:
            thread.join(timeout=args.shutdown_wait_seconds)
            if thread.is_alive():
                logger.write(
                    f"WARNING: {thread.name} did not exit within "
                    f"{args.shutdown_wait_seconds}s after interrupt"
                )
        return 130
    finally:
        stop_event.set()
        reporter.join(timeout=2)
        if dashboard is not None:
            dashboard.finish()

    # ---- Post-run summary ----
    discovered, completed, failed, skipped, excluded, errors = counters.snapshot()
    gs = global_stats.snapshot()
    logger.write(
        f"END discovered={discovered} completed={completed} failed={failed} "
        f"skipped_mounts={skipped} excluded_paths={excluded} scan_errors={errors} "
        f"dsmc_insp={gs['objects_inspected']} dsmc_bkup={gs['objects_backed_up']} "
        f"bytes_i={format_bytes(gs['bytes_inspected'])} "
        f"bytes_x={format_bytes(gs['bytes_transferred'])}"
    )

    # Reconciliation: every discovered directory should be accounted for.
    # Note: when root is /, it is never enqueued as an ordinary directory, so
    # the counters correctly exclude it.
    if completed + failed != discovered:
        logger.write(
            f"WARNING: counter mismatch — discovered={discovered} but "
            f"completed+failed={completed + failed} "
            f"(unaccounted: {discovered - completed - failed})"
        )

    # Root-files job summary (only when crawl root is /)
    if is_root_crawl:
        all_states = worker_states.snapshot()
        root_state = next((s for s in all_states if s.worker_number == 0), None)
        if root_state is not None:
            rf_status = root_state.status
            rf_ok = root_state.dirs_completed
            rf_fail = root_state.dirs_failed
            rf_timeout = root_state.dirs_timed_out
            rf_total = root_state.batch_total
            logger.write(
                f"ROOT_FILES summary: status={rf_status} "
                f"chunks_ok={rf_ok} chunks_failed={rf_fail} "
                f"chunks_timed_out={rf_timeout} total_chunks={rf_total}"
            )

    if not dashboard_enabled:
        print(
            f"DONE discovered={discovered} completed={completed} failed={failed} "
            f"skipped_mounts={skipped} excluded={excluded} scan_errors={errors}",
            flush=True,
        )

    return 0 if failed == 0 and errors == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
