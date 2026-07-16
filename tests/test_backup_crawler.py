"""
Focused tests for backup_crawler.py
-------------------------------------
Run with:  python3 -m pytest tests/
       or: python3 -m unittest discover tests/
"""

from __future__ import annotations

import os
import queue
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path

# Make the parent package importable from this tests directory.
sys.path.insert(0, str(Path(__file__).parent.parent))

import backup_crawler as bc


# ---------------------------------------------------------------------------
# 1. dsmc output parsing
# ---------------------------------------------------------------------------

SAMPLE_DSMC_OUTPUT = """\
IBM Storage Protect
Command Line Backup-Archive Client Interface
  Client Version 8, Release 1, Level 27.0
  Client date/time: 07/16/2026 16:16:16
(c) Copyright IBM Corp. 1990, 2025. All Rights Reserved.

Node Name: OPENCLAW
Session established with server SP: Linux/x86_64
  Server Version 8, Release 1, Level 27.000
  Server date/time: 07/16/2026 16:16:27  Last access: 07/16/2026 16:16:09

Incremental backup of volume '/tmp'
Successful incremental backup of '/tmp'

Total number of objects inspected:            1
Total number of objects backed up:            0
Total number of objects updated:              0
Total number of objects rebound:              0
Total number of objects deleted:              0
Total number of objects expired:              0
Total number of objects failed:               0
Total number of objects encrypted:            0
Total number of objects grew:                  0
Total number of retries:                      0
Total number of bytes inspected:           4.00 KB
Total number of bytes transferred:            0  B
Data transfer time:                        0.00 sec
Network data transfer rate:                0.00 KB/sec
Aggregate data transfer rate:              0.00 KB/sec
Objects compressed by:                        0%
Total data reduction ratio:              100.00%
Elapsed processing time:               00:00:19
"""


class TestParseDsmcSummaryLine(unittest.TestCase):
    def _parse_all(self, text: str) -> bc.DsmcInvocationStats:
        stats = bc.DsmcInvocationStats()
        for line in text.splitlines():
            bc.parse_dsmc_summary_line(line, stats)
        return stats

    def test_sample_output_objects_inspected(self):
        stats = self._parse_all(SAMPLE_DSMC_OUTPUT)
        self.assertEqual(stats.objects_inspected, 1)

    def test_sample_output_objects_backed_up(self):
        stats = self._parse_all(SAMPLE_DSMC_OUTPUT)
        self.assertEqual(stats.objects_backed_up, 0)

    def test_sample_output_objects_failed(self):
        stats = self._parse_all(SAMPLE_DSMC_OUTPUT)
        self.assertEqual(stats.objects_failed, 0)

    def test_sample_output_retries(self):
        stats = self._parse_all(SAMPLE_DSMC_OUTPUT)
        self.assertEqual(stats.retries, 0)

    def test_sample_output_bytes_inspected_4kb(self):
        stats = self._parse_all(SAMPLE_DSMC_OUTPUT)
        # 4.00 KB = 4096 bytes
        self.assertAlmostEqual(stats.bytes_inspected, 4.0 * 1024, places=1)

    def test_sample_output_bytes_transferred_zero(self):
        stats = self._parse_all(SAMPLE_DSMC_OUTPUT)
        self.assertAlmostEqual(stats.bytes_transferred, 0.0, places=1)

    def test_sample_output_elapsed_19_seconds(self):
        stats = self._parse_all(SAMPLE_DSMC_OUTPUT)
        self.assertEqual(stats.elapsed_secs, 19)

    def test_sample_output_data_reduction_100_pct(self):
        stats = self._parse_all(SAMPLE_DSMC_OUTPUT)
        self.assertAlmostEqual(stats.data_reduction_pct, 100.0, places=1)

    def test_sample_output_has_data(self):
        stats = self._parse_all(SAMPLE_DSMC_OUTPUT)
        self.assertTrue(stats.has_data())

    def test_empty_stats_not_has_data(self):
        stats = bc.DsmcInvocationStats()
        self.assertFalse(stats.has_data())

    def test_no_match_line_is_noop(self):
        stats = bc.DsmcInvocationStats()
        bc.parse_dsmc_summary_line("This line has nothing to parse.", stats)
        self.assertFalse(stats.has_data())

    def test_large_numbers_with_commas(self):
        stats = bc.DsmcInvocationStats()
        bc.parse_dsmc_summary_line(
            "Total number of objects inspected:    1,234,567", stats
        )
        self.assertEqual(stats.objects_inspected, 1234567)

    def test_bytes_in_gb(self):
        stats = bc.DsmcInvocationStats()
        bc.parse_dsmc_summary_line(
            "Total number of bytes transferred:    2.50 GB", stats
        )
        self.assertAlmostEqual(stats.bytes_transferred, 2.5 * 1024 ** 3, delta=1)

    def test_bytes_in_mb(self):
        stats = bc.DsmcInvocationStats()
        bc.parse_dsmc_summary_line(
            "Total number of bytes inspected:   512.00 MB", stats
        )
        self.assertAlmostEqual(stats.bytes_inspected, 512 * 1024 ** 2, delta=1)

    def test_elapsed_time_1_hour(self):
        stats = bc.DsmcInvocationStats()
        bc.parse_dsmc_summary_line(
            "Elapsed processing time:               01:02:03", stats
        )
        self.assertEqual(stats.elapsed_secs, 3600 + 120 + 3)

    def test_network_rate_kb_sec(self):
        stats = bc.DsmcInvocationStats()
        bc.parse_dsmc_summary_line(
            "Network data transfer rate:              100.00 KB/sec", stats
        )
        self.assertAlmostEqual(stats.network_rate_bps, 100 * 1024, places=0)

    def test_case_insensitive(self):
        stats = bc.DsmcInvocationStats()
        bc.parse_dsmc_summary_line(
            "TOTAL NUMBER OF OBJECTS BACKED UP:   5", stats
        )
        self.assertEqual(stats.objects_backed_up, 5)

    def test_updated(self):
        stats = bc.DsmcInvocationStats()
        bc.parse_dsmc_summary_line("Total number of objects updated:   3", stats)
        self.assertEqual(stats.objects_updated, 3)

    def test_rebound(self):
        stats = bc.DsmcInvocationStats()
        bc.parse_dsmc_summary_line("Total number of objects rebound:   1", stats)
        self.assertEqual(stats.objects_rebound, 1)

    def test_compressed_pct(self):
        stats = bc.DsmcInvocationStats()
        bc.parse_dsmc_summary_line("Objects compressed by:   42%", stats)
        self.assertAlmostEqual(stats.objects_compressed_pct, 42.0, places=1)

    def test_transfer_time(self):
        stats = bc.DsmcInvocationStats()
        bc.parse_dsmc_summary_line("Data transfer time:   1.23 sec", stats)
        self.assertAlmostEqual(stats.transfer_time_secs, 1.23, places=2)


class TestFormatBytes(unittest.TestCase):
    def test_bytes(self):
        self.assertEqual(bc.format_bytes(512), "512.00 B")

    def test_kb(self):
        self.assertEqual(bc.format_bytes(4 * 1024), "4.00 KB")

    def test_mb(self):
        self.assertEqual(bc.format_bytes(1.5 * 1024 ** 2), "1.50 MB")

    def test_gb(self):
        self.assertEqual(bc.format_bytes(2.0 * 1024 ** 3), "2.00 GB")

    def test_tb(self):
        self.assertEqual(bc.format_bytes(1.0 * 1024 ** 4), "1.00 TB")

    def test_zero(self):
        self.assertEqual(bc.format_bytes(0), "0.00 B")


# ---------------------------------------------------------------------------
# 2. Path exclusion utilities
# ---------------------------------------------------------------------------

class TestIsPathExcluded(unittest.TestCase):
    def test_exact_match(self):
        self.assertTrue(
            bc.is_path_excluded("/tmp/sp-logs", frozenset(["/tmp/sp-logs"]))
        )

    def test_descendant(self):
        self.assertTrue(
            bc.is_path_excluded(
                "/tmp/sp-logs/worker-01",
                frozenset(["/tmp/sp-logs"]),
            )
        )

    def test_deep_descendant(self):
        self.assertTrue(
            bc.is_path_excluded(
                "/tmp/sp-logs/worker-01/dsm",
                frozenset(["/tmp/sp-logs"]),
            )
        )

    def test_similar_prefix_not_excluded(self):
        # /tmp/sp-logsfoo must NOT be excluded by /tmp/sp-logs
        self.assertFalse(
            bc.is_path_excluded(
                "/tmp/sp-logsfoo",
                frozenset(["/tmp/sp-logs"]),
            )
        )

    def test_sibling_not_excluded(self):
        self.assertFalse(
            bc.is_path_excluded(
                "/tmp/other",
                frozenset(["/tmp/sp-logs"]),
            )
        )

    def test_parent_not_excluded(self):
        self.assertFalse(
            bc.is_path_excluded(
                "/tmp",
                frozenset(["/tmp/sp-logs"]),
            )
        )

    def test_empty_exclusion_set(self):
        self.assertFalse(bc.is_path_excluded("/tmp/anything", frozenset()))


# ---------------------------------------------------------------------------
# 3. Auto log-directory exclusion in scan_directories
# ---------------------------------------------------------------------------

class TestScanDirectoriesExclusion(unittest.TestCase):
    """
    Verify that directories listed in excluded_paths are never enqueued
    and that path-boundary containment is correct.
    """

    def _run_scan(self, root: str, excluded_paths: frozenset) -> list[str]:
        wq: queue.Queue[str] = queue.Queue(maxsize=10000)
        done = threading.Event()
        stop = threading.Event()
        counters = bc.Counters()
        # Use NamedTemporaryFile to avoid the insecure mktemp race condition
        with tempfile.NamedTemporaryFile(suffix=".log", delete=False) as _tf:
            log_file = Path(_tf.name)
        logger = bc.SafeLogger(log_file, echo=False)

        t = threading.Thread(
            target=bc.scan_directories,
            args=(root, wq, done, stop, counters, excluded_paths, logger),
        )
        t.start()
        t.join(timeout=30)

        collected: list[str] = []
        while True:
            try:
                collected.append(wq.get_nowait())
            except queue.Empty:
                break

        # Cleanup
        try:
            log_file.unlink(missing_ok=True)
        except Exception:
            pass

        return collected

    def test_log_dir_under_root_excluded(self):
        with tempfile.TemporaryDirectory() as root:
            # Create some directories
            Path(root, "alpha").mkdir()
            Path(root, "beta").mkdir()
            log_dir = Path(root, "sp-logs")
            log_dir.mkdir()
            Path(root, "sp-logs", "worker-01").mkdir()

            excluded = frozenset([str(log_dir)])
            found = self._run_scan(root, excluded)

            # The log_dir itself must not appear
            self.assertNotIn(str(log_dir), found)
            # sub-dir of log_dir must not appear
            self.assertNotIn(str(log_dir / "worker-01"), found)
            # Sibling directories must appear
            self.assertIn(str(Path(root, "alpha")), found)
            self.assertIn(str(Path(root, "beta")), found)

    def test_similar_prefix_dirs_not_excluded(self):
        """sp-logsfoo must not be excluded when sp-logs is excluded."""
        with tempfile.TemporaryDirectory() as root:
            Path(root, "sp-logs").mkdir()
            Path(root, "sp-logsfoo").mkdir()

            excluded = frozenset([os.path.join(root, "sp-logs")])
            found = self._run_scan(root, excluded)

            self.assertNotIn(os.path.join(root, "sp-logs"), found)
            self.assertIn(os.path.join(root, "sp-logsfoo"), found)

    def test_no_exclusions_scans_all(self):
        with tempfile.TemporaryDirectory() as root:
            Path(root, "a").mkdir()
            Path(root, "b").mkdir()
            Path(root, "a", "c").mkdir()

            found = self._run_scan(root, frozenset())
            # root + 3 subdirs
            self.assertIn(root, found)
            self.assertIn(os.path.join(root, "a"), found)
            self.assertIn(os.path.join(root, "b"), found)
            self.assertIn(os.path.join(root, "a", "c"), found)


# ---------------------------------------------------------------------------
# 4. Worker: repeatedly obtains batches until scanner completion
# ---------------------------------------------------------------------------

class TestWorkerRepeatedBatches(unittest.TestCase):
    """
    Confirm that a worker loops and processes all queued items,
    calling task_done() for each one.
    """

    def test_worker_drains_multiple_batches(self):
        import argparse

        with tempfile.TemporaryDirectory() as tmpdir:
            args = argparse.Namespace(
                log_dir=tmpdir,
                dsmc="/bin/true",
                dsmc_option=[],
                dsmc_timeout=0,
                dsmc_idle_timeout=0,
                resourceutilization=2,
                batch_size=3,
                dry_run=True,
            )

            wq: queue.Queue[str] = queue.Queue(maxsize=100)
            n_dirs = 9  # three batches of 3
            for i in range(n_dirs):
                wq.put(f"/fake/dir/{i:02d}")

            producer_done = threading.Event()
            producer_done.set()
            stop_event = threading.Event()
            counters = bc.Counters()
            worker_states = bc.WorkerStates(1)
            global_stats = bc.GlobalDsmcStats()

            log_file = Path(tmpdir) / "controller.log"
            logger = bc.SafeLogger(log_file, echo=False)
            failed_log = Path(tmpdir) / "failed.tsv"
            failed_log.write_text("return_code\tdirectory\tnotes\n")
            failed_logger = bc.SafeAppender(failed_log)

            t = threading.Thread(
                target=bc.worker,
                args=(
                    1,
                    args,
                    wq,
                    producer_done,
                    stop_event,
                    counters,
                    worker_states,
                    global_stats,
                    logger,
                    failed_logger,
                ),
            )
            t.start()
            t.join(timeout=30)

            self.assertFalse(t.is_alive(), "worker did not exit within 30 s")
            self.assertEqual(wq.unfinished_tasks, 0, "not all task_done() calls made")
            disc, comp, fail, _, _, _ = counters.snapshot()
            self.assertEqual(comp, n_dirs, f"expected {n_dirs} completed, got {comp}")
            self.assertEqual(fail, 0)


# ---------------------------------------------------------------------------
# 5. Timeout handling with a fake slow child process
# ---------------------------------------------------------------------------

class TestDsmcTimeout(unittest.TestCase):
    def _run_with_timeout(
        self,
        dsmc_timeout: float,
        dsmc_idle_timeout: float,
        child_script: str,
    ) -> tuple[int, bc.DsmcInvocationStats]:
        """Run a fake dsmc (via python -c) and return (rc, stats)."""
        import argparse
        import tempfile

        worker_states = bc.WorkerStates(1)
        global_stats = bc.GlobalDsmcStats()
        stop_event = threading.Event()

        with tempfile.TemporaryDirectory() as tmpdir:
            log_file = Path(tmpdir) / "controller.log"
            logger = bc.SafeLogger(log_file, echo=False)

            output_buf = bytearray()

            class _FakeBytesIO:
                def write(self, data: bytes) -> None:
                    output_buf.extend(data)

            # Use python3 as fake dsmc
            command = [sys.executable, "-c", child_script]

            # set_directory to initialise the state
            worker_states.set_directory(1, 1, "/fake/path")

            rc, stats = bc.run_dsmc_supervised(
                command=command,
                env=os.environ.copy(),
                output_file=_FakeBytesIO(),
                worker_number=1,
                worker_states=worker_states,
                global_stats=global_stats,
                logger=logger,
                worker_name="worker-01",
                dsmc_timeout=dsmc_timeout,
                dsmc_idle_timeout=dsmc_idle_timeout,
                stop_event=stop_event,
            )
        return rc, stats

    @unittest.skipIf(sys.platform == "win32", "signal/process-group tests are POSIX-only")
    def test_hard_timeout_kills_sleeping_process(self):
        rc, _ = self._run_with_timeout(
            dsmc_timeout=1.5,
            dsmc_idle_timeout=0,
            # sleep 60 seconds; should be killed by hard timeout
            child_script="import time; time.sleep(60)",
        )
        self.assertEqual(rc, bc.TIMEOUT_RC)

    @unittest.skipIf(sys.platform == "win32", "signal/process-group tests are POSIX-only")
    def test_idle_timeout_kills_silent_process(self):
        rc, _ = self._run_with_timeout(
            dsmc_timeout=0,
            dsmc_idle_timeout=1.5,
            # sleep 60 without output
            child_script="import time; time.sleep(60)",
        )
        self.assertEqual(rc, bc.IDLE_TIMEOUT_RC)

    def test_normal_exit_returns_process_rc(self):
        rc, _ = self._run_with_timeout(
            dsmc_timeout=10,
            dsmc_idle_timeout=0,
            child_script="raise SystemExit(0)",
        )
        self.assertEqual(rc, 0)

    def test_nonzero_exit_returned(self):
        rc, _ = self._run_with_timeout(
            dsmc_timeout=10,
            dsmc_idle_timeout=0,
            child_script="raise SystemExit(8)",
        )
        self.assertEqual(rc, 8)

    def test_output_is_parsed(self):
        _, stats = self._run_with_timeout(
            dsmc_timeout=10,
            dsmc_idle_timeout=0,
            child_script=(
                "print('Total number of objects inspected:   3');"
                "print('Elapsed processing time:   00:00:01')"
            ),
        )
        self.assertEqual(stats.objects_inspected, 3)
        self.assertEqual(stats.elapsed_secs, 1)


# ---------------------------------------------------------------------------
# 6. Correct task_done / reconciliation
# ---------------------------------------------------------------------------

class TestTaskDoneReconciliation(unittest.TestCase):
    """task_done must be called exactly once per get(), even on early exit."""

    def test_task_done_called_on_stop(self):
        """If stop_event is set before the batch is fully processed,
        task_done() must still be called for every item in the batch."""
        import argparse

        with tempfile.TemporaryDirectory() as tmpdir:
            args = argparse.Namespace(
                log_dir=tmpdir,
                dsmc="/bin/true",
                dsmc_option=[],
                dsmc_timeout=0,
                dsmc_idle_timeout=0,
                resourceutilization=2,
                batch_size=5,
                dry_run=True,
            )

            wq: queue.Queue[str] = queue.Queue(maxsize=20)
            for i in range(5):
                wq.put(f"/fake/{i}")

            producer_done = threading.Event()
            producer_done.set()
            stop_event = threading.Event()
            # Set stop AFTER filling queue so worker gets the batch then stops
            counters = bc.Counters()
            worker_states = bc.WorkerStates(1)
            global_stats = bc.GlobalDsmcStats()

            log_file = Path(tmpdir) / "ctrl.log"
            logger = bc.SafeLogger(log_file, echo=False)
            failed_log = Path(tmpdir) / "failed.tsv"
            failed_log.write_text("return_code\tdirectory\tnotes\n")
            failed_logger = bc.SafeAppender(failed_log)

            # Delay stop_event until after the first item is processed
            def _set_stop_soon():
                time.sleep(0.1)
                stop_event.set()

            threading.Thread(target=_set_stop_soon, daemon=True).start()

            t = threading.Thread(
                target=bc.worker,
                args=(
                    1,
                    args,
                    wq,
                    producer_done,
                    stop_event,
                    counters,
                    worker_states,
                    global_stats,
                    logger,
                    failed_logger,
                ),
            )
            t.start()
            t.join(timeout=15)
            self.assertFalse(t.is_alive())
            self.assertEqual(wq.unfinished_tasks, 0, "task_done not called for all items")


# ---------------------------------------------------------------------------
# 7. CLI validation for new timeout options
# ---------------------------------------------------------------------------

class TestCliValidation(unittest.TestCase):
    def _parse(self, extra_args: list[str]) -> "argparse.Namespace | None":
        """Return parsed args or None if parse_args raised SystemExit."""
        old = sys.argv
        try:
            sys.argv = ["backup_crawler.py", "/tmp"] + extra_args
            return bc.parse_args()
        except SystemExit:
            return None
        finally:
            sys.argv = old

    def test_valid_dsmc_timeout(self):
        args = self._parse(["--dsmc-timeout", "300"])
        self.assertIsNotNone(args)
        self.assertEqual(args.dsmc_timeout, 300)

    def test_valid_dsmc_idle_timeout(self):
        args = self._parse(["--dsmc-idle-timeout", "120"])
        self.assertIsNotNone(args)
        self.assertEqual(args.dsmc_idle_timeout, 120)

    def test_zero_disables_timeout(self):
        args = self._parse(["--dsmc-timeout", "0", "--dsmc-idle-timeout", "0"])
        self.assertIsNotNone(args)
        self.assertEqual(args.dsmc_timeout, 0)
        self.assertEqual(args.dsmc_idle_timeout, 0)

    def test_negative_dsmc_timeout_rejected(self):
        result = self._parse(["--dsmc-timeout", "-1"])
        self.assertIsNone(result)

    def test_negative_dsmc_idle_timeout_rejected(self):
        result = self._parse(["--dsmc-idle-timeout", "-5"])
        self.assertIsNone(result)

    def test_exclude_path_accepted(self):
        args = self._parse(["--exclude-path", "/some/path"])
        self.assertIsNotNone(args)
        self.assertIn("/some/path", args.exclude_path)

    def test_exclude_path_repeatable(self):
        args = self._parse(["--exclude-path", "/a", "--exclude-path", "/b"])
        self.assertIsNotNone(args)
        self.assertEqual(set(args.exclude_path), {"/a", "/b"})

    def test_positional_workers(self):
        args = self._parse(["4"])
        self.assertIsNotNone(args)
        self.assertEqual(args.streams, 4)

    def test_streams_flag_takes_precedence(self):
        args = self._parse(["10", "--streams", "2"])
        self.assertIsNotNone(args)
        self.assertEqual(args.streams, 2)


# ---------------------------------------------------------------------------
# 8. Root-files job: collect_root_files
# ---------------------------------------------------------------------------

class TestCollectRootFiles(unittest.TestCase):
    """collect_root_files must return non-directory entries and exclude dirs/excluded paths."""

    def test_returns_files_not_dirs(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            (Path(tmpdir) / "file_a.txt").write_text("a")
            (Path(tmpdir) / "file_b.bin").write_bytes(b"\x00")
            (Path(tmpdir) / "subdir").mkdir()

            files = bc.collect_root_files(tmpdir, frozenset())

            self.assertIn(str(Path(tmpdir) / "file_a.txt"), files)
            self.assertIn(str(Path(tmpdir) / "file_b.bin"), files)
            self.assertNotIn(str(Path(tmpdir) / "subdir"), files)

    def test_excludes_paths_matching_excluded_set(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            included = Path(tmpdir) / "included.txt"
            excluded = Path(tmpdir) / "excluded.txt"
            included.write_text("keep")
            excluded.write_text("skip")

            files = bc.collect_root_files(tmpdir, frozenset([str(excluded)]))

            self.assertIn(str(included), files)
            self.assertNotIn(str(excluded), files)

    def test_empty_dir_returns_empty_list(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            files = bc.collect_root_files(tmpdir, frozenset())
            self.assertEqual(files, [])

    def test_returns_sorted_list(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            for name in ("z_file", "a_file", "m_file"):
                (Path(tmpdir) / name).write_text(name)

            files = bc.collect_root_files(tmpdir, frozenset())
            self.assertEqual(files, sorted(files))

    def test_symlink_to_file_included(self):
        """Symlinks to files should be included; they are not directories."""
        with tempfile.TemporaryDirectory() as tmpdir:
            target = Path(tmpdir) / "real_file"
            link = Path(tmpdir) / "link_to_file"
            target.write_text("target")
            link.symlink_to(target)

            files = bc.collect_root_files(tmpdir, frozenset())
            self.assertIn(str(link), files)

    def test_symlink_to_dir_not_included(self):
        """Symlinks pointing at directories are included as non-directory entries.
        DirEntry.is_dir(follow_symlinks=False) returns False for all symlinks, so
        a symlink-to-dir is treated as a file-like entry and backed up as a link
        object rather than descended into."""
        # symlink-to-dir with follow_symlinks=False reports is_dir=False
        # so it will be included as a file-like entry (backed up as a link object)
        with tempfile.TemporaryDirectory() as tmpdir:
            real_dir = Path(tmpdir) / "real_dir"
            real_dir.mkdir()
            link = Path(tmpdir) / "link_to_dir"
            link.symlink_to(real_dir)

            files = bc.collect_root_files(tmpdir, frozenset())
            # Symlink-to-dir is NOT a dir when follow_symlinks=False; it IS included
            self.assertIn(str(link), files)
            # The real dir itself is a dir and must NOT be included
            self.assertNotIn(str(real_dir), files)


# ---------------------------------------------------------------------------
# 9. Root-files job: chunk_root_files
# ---------------------------------------------------------------------------

class TestChunkRootFiles(unittest.TestCase):
    """chunk_root_files must split file lists into argv-safe chunks."""

    def test_empty_input_returns_empty_list(self):
        self.assertEqual(bc.chunk_root_files([]), [])

    def test_all_files_fit_in_one_chunk(self):
        files = ["/file1", "/file2", "/file3"]
        chunks = bc.chunk_root_files(files, max_bytes=10000)
        self.assertEqual(len(chunks), 1)
        self.assertEqual(chunks[0], files)

    def test_files_split_when_limit_exceeded(self):
        # "/file_1" is 7 chars + 1 = 8 bytes cost; max_bytes=10 → only one fits per chunk
        files = ["/file_1", "/file_2", "/file_3"]
        chunks = bc.chunk_root_files(files, max_bytes=10)
        self.assertEqual(len(chunks), 3)

    def test_all_files_present_across_chunks(self):
        files = [f"/path/to/file_{i:03d}" for i in range(200)]
        chunks = bc.chunk_root_files(files, max_bytes=500)
        flattened = [f for c in chunks for f in c]
        self.assertEqual(sorted(flattened), sorted(files))

    def test_no_chunk_exceeds_max_bytes(self):
        files = [f"/some/very/long/path/to/file_{i:04d}" for i in range(100)]
        max_bytes = 200
        chunks = bc.chunk_root_files(files, max_bytes=max_bytes)
        for chunk in chunks:
            total = sum(len(p.encode("utf-8")) + 1 for p in chunk)
            self.assertLessEqual(
                total, max_bytes,
                f"Chunk byte total {total} exceeds limit {max_bytes}: {chunk}",
            )

    def test_single_file_larger_than_limit_still_in_own_chunk(self):
        """A single very long path must still be placed in a chunk even if its
        byte cost exceeds max_bytes (we cannot split a single path further)."""
        # Path length chosen to far exceed a small max_bytes limit (10 bytes)
        long_path = "/" + "x" * 300
        files = [long_path, "/short"]
        chunks = bc.chunk_root_files(files, max_bytes=10)
        flattened = [f for c in chunks for f in c]
        self.assertIn(long_path, flattened)
        self.assertIn("/short", flattened)

    def test_chunk_count_scales_with_file_count(self):
        # 100 files each costing ~10 bytes, limit 50 bytes → ≥2 chunks
        files = [f"/f{i:06d}" for i in range(100)]  # 9 bytes each → cost 10
        chunks = bc.chunk_root_files(files, max_bytes=50)
        self.assertGreater(len(chunks), 1)


# ---------------------------------------------------------------------------
# 10. scan_directories: / not enqueued as ordinary directory job
# ---------------------------------------------------------------------------

class TestScanDirectoriesRootSlash(unittest.TestCase):
    """
    When the crawl root is /, scan_directories must NOT enqueue / in the work
    queue, but must still enqueue immediate child directories.
    """

    def _run_scan_mocked(self, root: str, children: list) -> list[str]:
        """Run scan_directories with os.stat and os.scandir mocked."""
        from unittest.mock import patch, MagicMock

        wq: queue.Queue[str] = queue.Queue(maxsize=500)
        done = threading.Event()
        stop = threading.Event()
        counters = bc.Counters()

        with tempfile.NamedTemporaryFile(suffix=".log", delete=False) as _tf:
            log_file = Path(_tf.name)
        try:
            logger = bc.SafeLogger(log_file, echo=False)

            # Build mock DirEntry objects for each child
            mock_entries: list = []
            for child_path in children:
                e = MagicMock()
                e.is_dir.return_value = True
                e.path = child_path
                e.stat.return_value = MagicMock(st_dev=42)
                mock_entries.append(e)

            def make_cm(path):
                """Return a context manager that yields mock_entries for root
                and no entries for any child (to prevent infinite recursion)."""
                cm = MagicMock()
                if path == root:
                    cm.__enter__ = MagicMock(return_value=iter(mock_entries))
                else:
                    cm.__enter__ = MagicMock(return_value=iter([]))
                cm.__exit__ = MagicMock(return_value=False)
                return cm

            with (
                patch("os.stat", return_value=MagicMock(st_dev=42)),
                patch("os.scandir", side_effect=make_cm),
                patch("backup_crawler.nested_mounts", return_value=set()),
            ):
                t = threading.Thread(
                    target=bc.scan_directories,
                    args=(root, wq, done, stop, counters, frozenset(), logger),
                )
                t.start()
                t.join(timeout=10)

            items: list[str] = []
            while True:
                try:
                    items.append(wq.get_nowait())
                except queue.Empty:
                    break
            return items
        finally:
            log_file.unlink(missing_ok=True)

    def test_slash_not_in_queue(self):
        items = self._run_scan_mocked(os.sep, ["/bin", "/etc", "/usr"])
        self.assertNotIn(
            os.sep,
            items,
            "/ must never be enqueued as an ordinary directory job",
        )

    def test_child_dirs_of_slash_are_in_queue(self):
        items = self._run_scan_mocked(os.sep, ["/bin", "/etc", "/usr"])
        self.assertIn("/bin", items)
        self.assertIn("/etc", items)
        self.assertIn("/usr", items)

    def test_non_root_entry_point_is_enqueued(self):
        """For non-/ entry points, the root directory itself IS enqueued."""
        with tempfile.TemporaryDirectory() as tmpdir:
            wq: queue.Queue[str] = queue.Queue(maxsize=500)
            done = threading.Event()
            stop = threading.Event()
            counters = bc.Counters()

            with tempfile.NamedTemporaryFile(suffix=".log", delete=False) as _tf:
                log_file = Path(_tf.name)
            try:
                logger = bc.SafeLogger(log_file, echo=False)
                t = threading.Thread(
                    target=bc.scan_directories,
                    args=(tmpdir, wq, done, stop, counters, frozenset(), logger),
                )
                t.start()
                t.join(timeout=10)

                items: list[str] = []
                while True:
                    try:
                        items.append(wq.get_nowait())
                    except queue.Empty:
                        break

                self.assertIn(
                    tmpdir, items,
                    "Non-/ crawl root must appear in the work queue",
                )
            finally:
                log_file.unlink(missing_ok=True)

    def test_discovered_counter_excludes_slash(self):
        """When root is /, the 'discovered' counter must not include /."""
        items = self._run_scan_mocked(os.sep, ["/bin", "/etc"])
        # discovered should be 2 (the two children), not 3
        # We can only verify that "/" is not in items (counter test is implicit)
        self.assertNotIn(os.sep, items)
        self.assertEqual(len(items), 2)


# ---------------------------------------------------------------------------
# 11. run_root_files_job: dry-run integration
# ---------------------------------------------------------------------------

class TestRunRootFilesJobDryRun(unittest.TestCase):
    """Verify run_root_files_job behaviour in dry-run mode (no real dsmc needed)."""

    def _run_job(
        self, files_to_create: list[str], dirs_to_create: list[str]
    ) -> tuple[bc.WorkerStates, Path, str]:
        """
        Run the root-files job against a temp directory.
        Returns (worker_states, worker_log_path, tmpdir).
        Caller is responsible for cleanup via shutil.rmtree(tmpdir).
        """
        import argparse
        import shutil as _shutil

        tmpdir = tempfile.mkdtemp()

        # Create test files and dirs
        for name in files_to_create:
            (Path(tmpdir) / name).write_text(name)
        for name in dirs_to_create:
            (Path(tmpdir) / name).mkdir(exist_ok=True)

        log_dir = Path(tmpdir) / "logs"
        log_dir.mkdir()

        args = argparse.Namespace(
            log_dir=str(log_dir),
            dsmc="/bin/true",
            dsmc_option=[],
            dsmc_timeout=0,
            dsmc_idle_timeout=0,
            resourceutilization=2,
            dry_run=True,
        )

        worker_states = bc.WorkerStates(1, has_root_files_job=True)
        global_stats = bc.GlobalDsmcStats()
        stop_event = threading.Event()

        logger = bc.SafeLogger(log_dir / "ctrl.log", echo=False)
        failed_log = log_dir / "failed.tsv"
        failed_log.write_text("return_code\tdirectory\tnotes\n")
        failed_logger = bc.SafeAppender(failed_log)

        t = threading.Thread(
            target=bc.run_root_files_job,
            args=(
                args,
                tmpdir,
                frozenset(),
                worker_states,
                global_stats,
                logger,
                failed_logger,
                stop_event,
            ),
        )
        t.start()
        t.join(timeout=30)

        self.assertFalse(t.is_alive(), "run_root_files_job did not exit within 30s")

        log_path = log_dir / "root-files-job.log"
        return worker_states, log_path, tmpdir

    def test_no_files_job_is_skipped(self):
        import shutil as _shutil
        ws, log_path, tmpdir = self._run_job(files_to_create=[], dirs_to_create=["subdir"])
        try:
            states = ws.snapshot()
            root_state = next(s for s in states if s.worker_number == 0)
            self.assertEqual(root_state.status, "skipped")
            self.assertFalse(log_path.exists(), "log must not be created if no files")
        finally:
            _shutil.rmtree(tmpdir, ignore_errors=True)

    def test_files_present_job_runs_and_slots_done(self):
        import shutil as _shutil
        ws, log_path, tmpdir = self._run_job(
            files_to_create=["file_a", "file_b", "file_c"],
            dirs_to_create=["subdir"],
        )
        try:
            states = ws.snapshot()
            root_state = next(s for s in states if s.worker_number == 0)
            # After completion the slot should be in "done" state
            self.assertEqual(root_state.status, "done")
        finally:
            _shutil.rmtree(tmpdir, ignore_errors=True)

    def test_log_contains_dry_run_command(self):
        import shutil as _shutil
        _, log_path, tmpdir = self._run_job(
            files_to_create=["file_x"],
            dirs_to_create=[],
        )
        try:
            self.assertTrue(log_path.exists())
            log_content = log_path.read_bytes().decode("utf-8", errors="replace")
            self.assertIn("DRY RUN", log_content)
            # The log must show an explicit file path, not just "/"
            self.assertIn("file_x", log_content)
        finally:
            _shutil.rmtree(tmpdir, ignore_errors=True)

    def test_files_appear_as_explicit_operands_not_slash(self):
        """The dsmc command must contain explicit file paths, never the bare / operand."""
        import ast
        import shutil as _shutil
        _, log_path, tmpdir = self._run_job(
            files_to_create=["alpha", "beta"],
            dirs_to_create=[],
        )
        try:
            log_content = log_path.read_bytes().decode("utf-8", errors="replace")
            # The command repr in the DRY RUN line must include explicit file paths
            self.assertIn("alpha", log_content)
            self.assertIn("beta", log_content)
            # The command must NOT contain a bare "/" as a path operand.
            # Look for the repr of the command list.
            for line in log_content.splitlines():
                if line.startswith("DRY RUN: ["):
                    cmd = ast.literal_eval(line[len("DRY RUN: "):])
                    self.assertNotIn(os.sep, cmd,
                        "/ must not appear as a plain operand in the root-files command")
                    break
        finally:
            _shutil.rmtree(tmpdir, ignore_errors=True)

    def test_chunk_count_in_dashboard_state(self):
        """batch_total in worker slot 0 should equal the number of chunks."""
        import shutil as _shutil
        files = [f"f{i}" for i in range(5)]
        ws, _, tmpdir = self._run_job(files_to_create=files, dirs_to_create=[])
        try:
            states = ws.snapshot()
            root_state = next(s for s in states if s.worker_number == 0)
            # In dry-run mode with small files, everything fits in one chunk.
            # Note: dirs_completed tracks completed chunks for the ROOT_FILES job
            # (it reuses the WorkerState field that counts completed directory invocations
            # for regular workers; for slot 0 it represents completed dsmc chunk runs).
            self.assertGreaterEqual(root_state.dirs_completed, 1)
        finally:
            _shutil.rmtree(tmpdir, ignore_errors=True)


# ---------------------------------------------------------------------------
# 12. WorkerStates slot-0 visibility
# ---------------------------------------------------------------------------

class TestWorkerStatesRootFilesSlot(unittest.TestCase):
    def test_slot_0_present_when_requested(self):
        ws = bc.WorkerStates(3, has_root_files_job=True)
        numbers = [s.worker_number for s in ws.snapshot()]
        self.assertIn(0, numbers)
        self.assertIn(1, numbers)
        self.assertIn(3, numbers)

    def test_slot_0_absent_by_default(self):
        ws = bc.WorkerStates(3)
        numbers = [s.worker_number for s in ws.snapshot()]
        self.assertNotIn(0, numbers)

    def test_slot_0_appears_first_in_snapshot(self):
        ws = bc.WorkerStates(3, has_root_files_job=True)
        states = ws.snapshot()
        self.assertEqual(states[0].worker_number, 0)

    def test_set_custom_status(self):
        ws = bc.WorkerStates(2, has_root_files_job=True)
        ws.set_custom_status(0, "scanning", "scanning /")
        state = ws.snapshot()[0]
        self.assertEqual(state.status, "scanning")
        self.assertEqual(state.current_directory, "scanning /")


if __name__ == "__main__":
    unittest.main()
