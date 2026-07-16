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


if __name__ == "__main__":
    unittest.main()
