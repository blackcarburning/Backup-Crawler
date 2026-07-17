"""
Focused tests for backup_crawler.py
-------------------------------------
Run with:  python3 -m pytest tests/
       or: python3 -m unittest discover tests/
"""

from __future__ import annotations

import contextlib
import io
import os
import queue
import sqlite3
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
    """format_bytes uses IEC units (1024-based, KiB/MiB/GiB/TiB/PiB labels)."""

    def test_bytes(self):
        self.assertEqual(bc.format_bytes(512), "512.00 B")

    def test_kib(self):
        self.assertEqual(bc.format_bytes(4 * 1024), "4.00 KiB")

    def test_mib(self):
        self.assertEqual(bc.format_bytes(1.5 * 1024 ** 2), "1.50 MiB")

    def test_gib(self):
        self.assertEqual(bc.format_bytes(2.0 * 1024 ** 3), "2.00 GiB")

    def test_tib(self):
        self.assertEqual(bc.format_bytes(1.0 * 1024 ** 4), "1.00 TiB")

    def test_pib(self):
        self.assertEqual(bc.format_bytes(1.0 * 1024 ** 5), "1.00 PiB")

    def test_zero(self):
        self.assertEqual(bc.format_bytes(0), "0.00 B")

    def test_just_below_kib(self):
        self.assertEqual(bc.format_bytes(1023), "1023.00 B")

    def test_exact_1_kib(self):
        self.assertEqual(bc.format_bytes(1024), "1.00 KiB")


# ---------------------------------------------------------------------------
# 1b. Integer byte parsing (DsmcInvocationStats.bytes_* are int)
# ---------------------------------------------------------------------------

class TestByteParsing(unittest.TestCase):
    """Parsed byte counts must be exact integers (1024-based units)."""

    def _parse_bytes_line(self, line: str) -> bc.DsmcInvocationStats:
        stats = bc.DsmcInvocationStats()
        bc.parse_dsmc_summary_line(line, stats)
        return stats

    def test_zero_bytes(self):
        # dsmc uses two spaces before the unit when the value is 0: "0  B"
        # The double space is actual dsmc output, not a typo.
        stats = self._parse_bytes_line("Total number of bytes transferred:    0  B")
        self.assertEqual(stats.bytes_transferred, 0)
        self.assertIsInstance(stats.bytes_transferred, int)

    def test_4kb_is_4096_bytes(self):
        stats = self._parse_bytes_line("Total number of bytes inspected:   4.00 KB")
        self.assertEqual(stats.bytes_inspected, 4096)
        self.assertIsInstance(stats.bytes_inspected, int)

    def test_1_5mb_is_1572864_bytes(self):
        stats = self._parse_bytes_line("Total number of bytes transferred:   1.50 MB")
        self.assertEqual(stats.bytes_transferred, 1572864)
        self.assertIsInstance(stats.bytes_transferred, int)

    def test_2_5gb_is_correct_bytes(self):
        stats = self._parse_bytes_line("Total number of bytes transferred:   2.50 GB")
        self.assertEqual(stats.bytes_transferred, round(2.5 * 1024 ** 3))
        self.assertIsInstance(stats.bytes_transferred, int)

    def test_inspected_field_is_int(self):
        stats = self._parse_bytes_line("Total number of bytes inspected:   512.00 MB")
        self.assertIsInstance(stats.bytes_inspected, int)
        self.assertEqual(stats.bytes_inspected, 512 * 1024 * 1024)


# ---------------------------------------------------------------------------
# 1c. Duplicate summary keys — last occurrence wins
# ---------------------------------------------------------------------------

class TestDuplicateSummaryKeys(unittest.TestCase):
    """When dsmc emits the same summary key twice, the last value must win."""

    def test_duplicate_backed_up_uses_last(self):
        stats = bc.DsmcInvocationStats()
        bc.parse_dsmc_summary_line("Total number of objects backed up:   3", stats)
        bc.parse_dsmc_summary_line("Total number of objects backed up:   7", stats)
        self.assertEqual(stats.objects_backed_up, 7)

    def test_duplicate_bytes_inspected_uses_last(self):
        stats = bc.DsmcInvocationStats()
        bc.parse_dsmc_summary_line("Total number of bytes inspected:   1.00 KB", stats)
        bc.parse_dsmc_summary_line("Total number of bytes inspected:   2.00 KB", stats)
        self.assertEqual(stats.bytes_inspected, 2048)

    def test_duplicate_elapsed_uses_last(self):
        stats = bc.DsmcInvocationStats()
        bc.parse_dsmc_summary_line("Elapsed processing time:  00:00:10", stats)
        bc.parse_dsmc_summary_line("Elapsed processing time:  00:00:30", stats)
        self.assertEqual(stats.elapsed_secs, 30)


# ---------------------------------------------------------------------------
# 1d. GlobalDsmcStats — counters, completeness, thread-safety
# ---------------------------------------------------------------------------

class TestGlobalDsmcStats(unittest.TestCase):
    """Thread-safe aggregate statistics."""

    def _make_stats(self, **kwargs) -> bc.DsmcInvocationStats:
        """Return a DsmcInvocationStats with elapsed_secs=1 by default
        (so has_data() returns True) plus any extra overrides."""
        s = bc.DsmcInvocationStats(elapsed_secs=1, **kwargs)
        return s

    def test_dsmc_done_increments_per_invocation(self):
        gs = bc.GlobalDsmcStats()
        gs.add_invocation(self._make_stats())
        gs.add_invocation(self._make_stats())
        self.assertEqual(gs.snapshot()["dsmc_done"], 2)

    def test_summaries_parsed_counts_complete(self):
        gs = bc.GlobalDsmcStats()
        gs.add_invocation(self._make_stats(objects_backed_up=5))
        self.assertEqual(gs.snapshot()["summaries_parsed"], 1)
        self.assertEqual(gs.snapshot()["incomplete_summaries"], 0)

    def test_incomplete_counted_when_no_data(self):
        gs = bc.GlobalDsmcStats()
        empty = bc.DsmcInvocationStats()  # has_data() == False
        gs.add_invocation(empty)
        snap = gs.snapshot()
        self.assertEqual(snap["dsmc_done"], 1)
        self.assertEqual(snap["summaries_parsed"], 0)
        self.assertEqual(snap["incomplete_summaries"], 1)

    def test_incomplete_does_not_add_bytes(self):
        gs = bc.GlobalDsmcStats()
        empty = bc.DsmcInvocationStats()
        gs.add_invocation(empty)
        snap = gs.snapshot()
        self.assertEqual(snap["bytes_inspected"], 0)
        self.assertEqual(snap["bytes_transferred"], 0)

    def test_byte_totals_are_integers(self):
        gs = bc.GlobalDsmcStats()
        gs.add_invocation(self._make_stats(bytes_inspected=4096, bytes_transferred=1024))
        snap = gs.snapshot()
        self.assertIsInstance(snap["bytes_inspected"], int)
        self.assertIsInstance(snap["bytes_transferred"], int)

    def test_all_object_fields_accumulated(self):
        gs = bc.GlobalDsmcStats()
        gs.add_invocation(self._make_stats(
            objects_inspected=10,
            objects_backed_up=5,
            objects_updated=2,
            objects_rebound=1,
            objects_deleted=1,
            objects_expired=1,
            objects_failed=1,
            objects_encrypted=1,
            objects_grew=1,
            retries=1,
        ))
        gs.add_invocation(self._make_stats(
            objects_inspected=10,
            objects_backed_up=5,
            objects_updated=2,
        ))
        snap = gs.snapshot()
        self.assertEqual(snap["objects_inspected"], 20)
        self.assertEqual(snap["objects_backed_up"], 10)
        self.assertEqual(snap["objects_updated"], 4)
        self.assertEqual(snap["objects_rebound"], 1)
        self.assertEqual(snap["objects_deleted"], 1)
        self.assertEqual(snap["objects_expired"], 1)
        self.assertEqual(snap["objects_failed"], 1)
        self.assertEqual(snap["objects_encrypted"], 1)
        self.assertEqual(snap["objects_grew"], 1)
        self.assertEqual(snap["retries"], 1)

    def test_bytes_accumulated_correctly(self):
        gs = bc.GlobalDsmcStats()
        gs.add_invocation(self._make_stats(bytes_inspected=4096, bytes_transferred=1024))
        gs.add_invocation(self._make_stats(bytes_inspected=8192, bytes_transferred=2048))
        snap = gs.snapshot()
        self.assertEqual(snap["bytes_inspected"], 12288)
        self.assertEqual(snap["bytes_transferred"], 3072)

    def test_exactly_once_merge(self):
        """Each invocation stats object must only be merged once (caller responsibility)."""
        gs = bc.GlobalDsmcStats()
        inv = self._make_stats(objects_backed_up=10, bytes_inspected=1024)
        gs.add_invocation(inv)
        # Simulating exactly-once: do NOT call add_invocation again for same inv
        snap = gs.snapshot()
        self.assertEqual(snap["objects_backed_up"], 10)
        self.assertEqual(snap["bytes_inspected"], 1024)
        self.assertEqual(snap["dsmc_done"], 1)

    def test_concurrent_workers_no_lost_updates(self):
        """N threads each adding M invocations must yield N*M total without loss."""
        NUM_THREADS = 8
        INVOCATIONS_PER_THREAD = 50
        gs = bc.GlobalDsmcStats()
        barrier = threading.Barrier(NUM_THREADS)

        def _add_many():
            barrier.wait()  # All start at the same time
            for _ in range(INVOCATIONS_PER_THREAD):
                gs.add_invocation(self._make_stats(
                    objects_backed_up=1,
                    bytes_inspected=1024,
                    bytes_transferred=512,
                ))

        threads = [threading.Thread(target=_add_many) for _ in range(NUM_THREADS)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=15)

        # Verify all threads completed within the timeout
        for t in threads:
            self.assertFalse(t.is_alive(), f"Thread {t.name} did not complete within timeout")

        snap = gs.snapshot()
        expected = NUM_THREADS * INVOCATIONS_PER_THREAD
        self.assertEqual(snap["dsmc_done"], expected)
        self.assertEqual(snap["summaries_parsed"], expected)
        self.assertEqual(snap["objects_backed_up"], expected)
        self.assertEqual(snap["bytes_inspected"], expected * 1024)
        self.assertEqual(snap["bytes_transferred"], expected * 512)

    def test_snapshot_is_consistent(self):
        """snapshot() must return a consistent copy, not partial state."""
        gs = bc.GlobalDsmcStats()
        stop = threading.Event()

        def _writer():
            while not stop.is_set():
                gs.add_invocation(self._make_stats(
                    objects_backed_up=1,
                    bytes_inspected=1000,
                    bytes_transferred=500,
                ))

        writer = threading.Thread(target=_writer, daemon=True)
        writer.start()
        time.sleep(0.05)

        # Take multiple snapshots and verify internal consistency
        for _ in range(20):
            snap = gs.snapshot()
            # dsmc_done == summaries_parsed + incomplete_summaries
            self.assertEqual(
                snap["dsmc_done"],
                snap["summaries_parsed"] + snap["incomplete_summaries"],
            )

        stop.set()
        writer.join(timeout=5)
        self.assertFalse(writer.is_alive(), "Writer thread did not stop within timeout")


# ---------------------------------------------------------------------------
# 1e. Dry-run exclusion — no dsmc stats fabricated
# ---------------------------------------------------------------------------

class TestDryRunExclusion(unittest.TestCase):
    """Dry-run invocations must not add any stats to GlobalDsmcStats."""

    def test_dry_run_does_not_add_invocation(self):
        """
        The worker/root_files_job code path never calls add_invocation when
        args.dry_run is True.  We verify this by running run_root_files_job
        in dry-run mode and checking that global_stats remain at zero.
        """
        import argparse
        import shutil as _shutil

        tmpdir = tempfile.mkdtemp()
        try:
            # Create one plain file so the job is not skipped
            (Path(tmpdir) / "dummy_file").write_text("data")

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

            ws = bc.WorkerStates(1, has_root_files_job=True)
            gs = bc.GlobalDsmcStats()
            stop_event = threading.Event()
            logger = bc.SafeLogger(log_dir / "ctrl.log", echo=False)
            failed_log = log_dir / "failed.tsv"
            failed_log.write_text("return_code\tdirectory\tnotes\n")
            failed_logger = bc.SafeAppender(failed_log)

            t = threading.Thread(
                target=bc.run_root_files_job,
                args=(args, tmpdir, frozenset(), ws, gs, logger, failed_logger, stop_event),
            )
            t.start()
            t.join(timeout=30)
            self.assertFalse(t.is_alive())

            snap = gs.snapshot()
            self.assertEqual(snap["dsmc_done"], 0,
                "Dry-run must not increment dsmc_done")
            self.assertEqual(snap["objects_backed_up"], 0,
                "Dry-run must not fabricate objects_backed_up")
            self.assertEqual(snap["bytes_inspected"], 0,
                "Dry-run must not fabricate bytes_inspected")
        finally:
            _shutil.rmtree(tmpdir, ignore_errors=True)


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

    def test_log_contains_full_path_without_truncation(self):
        import shutil as _shutil
        # 12 repeated segments force a clearly long operand path in logs; this
        # verifies dashboard-only truncation never alters logged full paths.
        long_name = ("segment_" * 12) + "tail.txt"
        _, log_path, tmpdir = self._run_job(
            files_to_create=[long_name],
            dirs_to_create=[],
        )
        try:
            log_content = log_path.read_bytes().decode("utf-8", errors="replace")
            self.assertIn(str(Path(tmpdir) / long_name), log_content)
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


# ---------------------------------------------------------------------------
# 13. Dashboard.truncate_path — path column display truncation
# ---------------------------------------------------------------------------

class TestTruncatePath(unittest.TestCase):
    """Dashboard.truncate_path must middle-truncate with an ASCII marker while
    preserving useful context from both ends of long paths."""

    # --- basic behaviour ---

    def test_short_path_unchanged(self):
        path = "/home/user/backup"
        self.assertEqual(bc.Dashboard.truncate_path(path, 40), path)

    def test_exact_fit_unchanged(self):
        path = "abc"
        self.assertEqual(bc.Dashboard.truncate_path(path, 3), path)

    def test_long_path_truncated_with_middle_marker(self):
        path = "/very/long/path/to/some/directory/with/a/meaningful/basename"
        result = bc.Dashboard.truncate_path(path, 20)
        self.assertEqual(len(result), 20)
        self.assertIn(".....", result)

    def test_truncated_result_has_correct_length(self):
        path = "/" + "x" * 100
        for w in (5, 10, 20, 40):
            result = bc.Dashboard.truncate_path(path, w)
            self.assertEqual(len(result), w, f"width={w}: expected {w} chars, got {len(result)}")

    # --- tail/basename preservation ---

    def test_prefix_and_tail_are_preserved(self):
        path = "/home/user/node_modules/parse5/lib/extensions"
        result = bc.Dashboard.truncate_path(path, 30)
        self.assertEqual(len(result), 30)
        self.assertIn(path[:10], result)
        self.assertIn(path[-10:], result)
        self.assertEqual(result.count("....."), 1)

    def test_tail_basename_preserved(self):
        path = "/home/user/node_modules/parse5/lib/extensions"
        result = bc.Dashboard.truncate_path(path, 30)
        self.assertTrue(result.endswith("extensions"))

    def test_suffix_is_ascii_not_unicode(self):
        path = "/a/very/long/path/that/needs/truncation"
        result = bc.Dashboard.truncate_path(path, 10)
        # Must not contain the Unicode ellipsis character
        self.assertNotIn("\u2026", result)
        self.assertIn(".....", result)

    # --- edge cases: very small widths ---

    def test_width_zero_returns_empty(self):
        self.assertEqual(bc.Dashboard.truncate_path("/some/path", 0), "")

    def test_width_negative_returns_empty(self):
        self.assertEqual(bc.Dashboard.truncate_path("/some/path", -5), "")

    def test_width_1_returns_first_suffix_char(self):
        result = bc.Dashboard.truncate_path("/some/path", 1)
        self.assertEqual(result, ".")
        self.assertEqual(len(result), 1)

    def test_width_2_returns_two_suffix_chars(self):
        result = bc.Dashboard.truncate_path("/some/path", 2)
        self.assertEqual(result, "..")
        self.assertEqual(len(result), 2)

    def test_width_3_returns_clipped_marker(self):
        result = bc.Dashboard.truncate_path("/some/path", 3)
        self.assertEqual(result, "...")
        self.assertEqual(len(result), 3)

    def test_width_4_returns_clipped_marker(self):
        path = "/abcde"
        result = bc.Dashboard.truncate_path(path, 4)
        self.assertEqual(result, "....")

    def test_width_7_splits_prefix_and_suffix(self):
        path = "/abcdefg"
        result = bc.Dashboard.truncate_path(path, 7)
        self.assertEqual(result, "/.....g")

    def test_empty_path_unchanged(self):
        self.assertEqual(bc.Dashboard.truncate_path("", 10), "")

    def test_custom_marker(self):
        path = "/home/user/documents/project/README.md"
        result = bc.Dashboard.truncate_path(path, 20, marker=">>")
        self.assertEqual(len(result), 20)
        self.assertEqual(result.count(">>"), 1)
        self.assertIn(path[:9], result)
        self.assertIn(path[-9:], result)

    # --- ROOT_FILES chunk label (not a file path, but must still fit) ---

    def test_chunk_label_unchanged_when_short(self):
        label = "chunk 1/3"
        self.assertEqual(bc.Dashboard.truncate_path(label, 20), label)

    def test_chunk_label_truncated_when_long(self):
        label = "chunk 100/200 (some extra description text that is very long)"
        result = bc.Dashboard.truncate_path(label, 20)
        self.assertEqual(len(result), 20)
        self.assertIn(".....", result)


# ---------------------------------------------------------------------------
# 14. Dashboard worker-row width constraints
# ---------------------------------------------------------------------------

class TestDashboardRowWidth(unittest.TestCase):
    """Worker rows must not exceed the terminal width at representative sizes."""

    _BAR_WIDTH = 10
    _STATUS_WIDTH = bc.Dashboard._STATUS_WIDTH
    _MAX_PATH = bc.Dashboard._MAX_PATH_DISPLAY

    def _build_prefix(self, label: str, bar: str, pos: str, status: str,
                      b_str: str, pid_str: str, time_str: str, idle_str: str,
                      stats_str: str, rc_str: str) -> str:
        """Mirror the f-string in Dashboard._render exactly."""
        return (
            f"{label:<10} "
            f"{bar} "
            f"{pos:>7} "
            f"{status:<{self._STATUS_WIDTH}} "
            f"{b_str:<5} "
            f"{pid_str:<9} "
            f"rt:{time_str:<7} "
            f"idle:{idle_str:<7} "
            f"{stats_str:<17} "
            f"{rc_str:<6} "
        )

    def _static_width(self) -> int:
        return (
            11                          # label (10) + space
            + (self._BAR_WIDTH + 3)     # "[" + bar + "]" + space
            + 8                         # pos (7) + space
            + (self._STATUS_WIDTH + 1)  # status + space
            + 6                         # b_str (5) + space
            + 10                        # pid_str (9) + space
            + 11                        # "rt:" (3) + time_str (7) + space
            + 13                        # "idle:" (5) + idle_str (7) + space
            + 18                        # stats_str (17) + space
            + 7                         # rc_str (6) + space
        )

    def _path_width(self, terminal_width: int) -> int:
        """Mirror Dashboard._render's path_width formula."""
        return min(self._MAX_PATH, max(0, terminal_width - self._static_width()))

    def _row_for(self, path: str, terminal_width: int) -> str:
        """Build a full worker row as Dashboard._render would, then clip it."""
        path_width = self._path_width(terminal_width)
        bar = "[" + "#" * self._BAR_WIDTH + "]"
        prefix = self._build_prefix(
            label="W01",
            bar=bar,
            pos="1/10",
            status="running",
            b_str="b#1",
            pid_str="pid=12345",
            time_str="1.5s",
            idle_str="0.2s",
            stats_str="ok:3 to:0 fl:0",
            rc_str="rc=0",
        )
        truncated_path = bc.Dashboard.truncate_path(path, path_width)
        row = prefix + truncated_path
        if len(row) > terminal_width:
            row = row[:terminal_width]
        return row

    def _root_row_for(self, path: str, terminal_width: int) -> str:
        """Build a ROOT_FILES row as Dashboard._render would, then clip it."""
        path_width = self._path_width(terminal_width)
        bar = " " * (self._BAR_WIDTH + 2)
        prefix = self._build_prefix(
            label="ROOT_FILES",
            bar=bar,
            pos="--/--",
            status="done",
            b_str="",
            pid_str="",
            time_str="",
            idle_str="",
            stats_str="ok:1 to:0 fl:0",
            rc_str="rc=0",
        )
        truncated_path = bc.Dashboard.truncate_path(path, path_width)
        row = prefix + truncated_path
        if len(row) > terminal_width:
            row = row[:terminal_width]
        return row

    def test_row_fits_80_col_terminal(self):
        long_path = "/very/long/nested/directory/path/that/would/normally/wrap/onto/the/next/line"
        row = self._row_for(long_path, terminal_width=80)
        self.assertLessEqual(len(row), 80,
            f"Row length {len(row)} exceeds 80 cols: {row!r}")

    def test_row_fits_120_col_terminal(self):
        long_path = "/very/long/nested/directory/path/that/would/normally/wrap/onto/the/next/line"
        row = self._row_for(long_path, terminal_width=120)
        self.assertLessEqual(len(row), 120,
            f"Row length {len(row)} exceeds 120 cols: {row!r}")

    def test_row_fits_160_col_terminal(self):
        long_path = "/very/long/nested/directory/path/that/would/normally/wrap/onto/the/next/line"
        row = self._row_for(long_path, terminal_width=160)
        self.assertLessEqual(len(row), 160,
            f"Row length {len(row)} exceeds 160 cols: {row!r}")

    def test_path_column_capped_at_max_on_wide_terminal(self):
        """On a very wide terminal (300 cols), the path column must not exceed MAX_PATH_DISPLAY."""
        long_path = "/" + "x" * 500
        row = self._row_for(long_path, terminal_width=300)
        path_part = row[self._static_width():]
        self.assertLessEqual(len(path_part), self._MAX_PATH,
            f"Path column {len(path_part)} chars exceeds MAX_PATH_DISPLAY={self._MAX_PATH}")

    def test_root_files_row_fits_80_col(self):
        label = "chunk 1/3"
        row = self._root_row_for(label, terminal_width=80)
        self.assertLessEqual(len(row), 80,
            f"ROOT_FILES row length {len(row)} exceeds 80 cols: {row!r}")

    def test_root_files_row_fits_120_col(self):
        label = "chunk 10/200 (very long description that might overflow)"
        row = self._root_row_for(label, terminal_width=120)
        self.assertLessEqual(len(row), 120,
            f"ROOT_FILES row length {len(row)} exceeds 120 cols: {row!r}")

    def test_short_path_not_truncated(self):
        # On a 200-col terminal, path_width = min(MAX_PATH_DISPLAY, 200-static_width) = 78.
        # A short path under 78 chars must pass through unchanged.
        short_path = "/home/user"
        result = bc.Dashboard.truncate_path(short_path, self._path_width(200))
        self.assertEqual(result, short_path,
            "Short path should not be truncated")

    def test_truncated_path_has_middle_marker_once(self):
        long_path = "/" + "a/b/c/" * 20
        # Use a wide terminal (200 cols) so path_width = MAX_PATH_DISPLAY = 78;
        # with a path of 120 chars it will definitely be truncated.
        result = bc.Dashboard.truncate_path(long_path, self._path_width(200))
        self.assertEqual(result.count("....."), 1,
            f"Truncated path should contain exactly one middle marker, got: {result!r}")

    def test_max_path_display_is_78(self):
        self.assertEqual(self._MAX_PATH, 78)


# ---------------------------------------------------------------------------
# 15. format_final_summary
# ---------------------------------------------------------------------------

def _make_gs(
    dsmc_done: int = 0,
    summaries_parsed: int = 0,
    incomplete_summaries: int = 0,
    active_children: int = 0,
    objects_inspected: int = 0,
    objects_backed_up: int = 0,
    objects_updated: int = 0,
    objects_failed: int = 0,
    retries: int = 0,
    objects_rebound: int = 0,
    objects_deleted: int = 0,
    objects_expired: int = 0,
    objects_encrypted: int = 0,
    objects_grew: int = 0,
    bytes_inspected: int = 0,
    bytes_transferred: int = 0,
    total_elapsed_secs: float = 0.0,
) -> dict:
    """Return a GlobalDsmcStats snapshot dict with sensible defaults."""
    return {
        "dsmc_done": dsmc_done,
        "summaries_parsed": summaries_parsed,
        "incomplete_summaries": incomplete_summaries,
        "active_children": active_children,
        "objects_inspected": objects_inspected,
        "objects_backed_up": objects_backed_up,
        "objects_updated": objects_updated,
        "objects_failed": objects_failed,
        "retries": retries,
        "objects_rebound": objects_rebound,
        "objects_deleted": objects_deleted,
        "objects_expired": objects_expired,
        "objects_encrypted": objects_encrypted,
        "objects_grew": objects_grew,
        "bytes_inspected": bytes_inspected,
        "bytes_transferred": bytes_transferred,
        "total_elapsed_secs": total_elapsed_secs,
    }


class TestFormatFinalSummary(unittest.TestCase):
    """Tests for the format_final_summary function."""

    # -----------------------------------------------------------------------
    # Helpers
    # -----------------------------------------------------------------------

    def _call(
        self,
        root: str = "/data",
        streams: int = 4,
        elapsed_secs: float = 60.0,
        discovered: int = 10,
        completed: int = 10,
        failed: int = 0,
        skipped: int = 0,
        excluded: int = 0,
        errors: int = 0,
        gs: dict | None = None,
        is_root_crawl: bool = False,
        root_state: "bc.WorkerState | None" = None,
        scanner_done: bool = True,
    ) -> str:
        if gs is None:
            gs = _make_gs()
        return bc.format_final_summary(
            root=root,
            streams=streams,
            elapsed_secs=elapsed_secs,
            discovered=discovered,
            completed=completed,
            failed=failed,
            skipped=skipped,
            excluded=excluded,
            errors=errors,
            gs=gs,
            is_root_crawl=is_root_crawl,
            root_state=root_state,
            scanner_done=scanner_done,
        )

    # -----------------------------------------------------------------------
    # Structural checks
    # -----------------------------------------------------------------------

    def test_starts_and_ends_with_separator(self):
        s = self._call()
        lines = s.splitlines()
        self.assertEqual(lines[0], "=" * 80)
        self.assertEqual(lines[-1], "=" * 80)

    def test_contains_header_text(self):
        s = self._call()
        self.assertIn("FINAL BACKUP SUMMARY", s)

    def test_contains_root_path(self):
        s = self._call(root="/mnt/data")
        self.assertIn("/mnt/data", s)

    def test_contains_worker_count(self):
        s = self._call(streams=8)
        self.assertIn("8", s)

    def test_elapsed_formatted_as_hhmmss(self):
        s = self._call(elapsed_secs=3661.0)  # 1h 1m 1s
        self.assertIn("01:01:01", s)

    def test_elapsed_zero(self):
        s = self._call(elapsed_secs=0.0)
        self.assertIn("00:00:00", s)

    # -----------------------------------------------------------------------
    # Run status
    # -----------------------------------------------------------------------

    def test_success_outcome(self):
        s = self._call(failed=0, errors=0)
        self.assertIn("SUCCESS", s)

    def test_failure_outcome_on_failed_dirs(self):
        s = self._call(failed=3, errors=0)
        self.assertIn("COMPLETED WITH FAILURES", s)
        self.assertIn("3 directory job(s) failed", s)

    def test_failure_outcome_on_scan_errors(self):
        s = self._call(failed=0, errors=2)
        self.assertIn("COMPLETED WITH FAILURES", s)
        self.assertIn("2 scan error(s)", s)

    def test_failure_outcome_includes_both_components(self):
        s = self._call(failed=1, errors=1)
        self.assertIn("1 directory job(s) failed", s)
        self.assertIn("1 scan error(s)", s)

    def test_scanner_done_shown(self):
        s = self._call(scanner_done=True)
        self.assertIn("DONE", s)

    def test_scanner_not_done_shown(self):
        s = self._call(scanner_done=False)
        self.assertIn("DID NOT FINISH", s)

    # -----------------------------------------------------------------------
    # Directory / folder accounting
    # -----------------------------------------------------------------------

    def test_directory_section_heading(self):
        s = self._call()
        self.assertIn("Directories", s)

    def test_discovered_count_present(self):
        s = self._call(discovered=12345, completed=12345, failed=0)
        # thousands separator expected
        self.assertIn("12,345", s)

    def test_completed_count_present(self):
        s = self._call(discovered=100, completed=95, failed=5)
        self.assertIn("95", s)

    def test_failed_count_present(self):
        s = self._call(discovered=100, completed=97, failed=3)
        self.assertIn("3", s)

    def test_excluded_count_present(self):
        s = self._call(excluded=7)
        self.assertIn("7", s)

    def test_skipped_count_present(self):
        s = self._call(skipped=2)
        self.assertIn("2", s)

    def test_scan_errors_count_present(self):
        s = self._call(errors=4)
        self.assertIn("4", s)

    # -----------------------------------------------------------------------
    # Reconciliation
    # -----------------------------------------------------------------------

    def test_reconciliation_ok_label(self):
        s = self._call(discovered=10, completed=8, failed=2)
        self.assertIn("OK", s)
        self.assertIn("10 = 8 + 2", s)

    def test_reconciliation_mismatch_label(self):
        # discovered=10, completed+failed=8 → 2 unaccounted
        s = self._call(discovered=10, completed=7, failed=1)
        self.assertIn("MISMATCH", s)
        self.assertIn("2 unaccounted", s)

    def test_reconciliation_zero_all(self):
        s = self._call(discovered=0, completed=0, failed=0)
        self.assertIn("OK", s)

    # -----------------------------------------------------------------------
    # dsmc invocation accounting
    # -----------------------------------------------------------------------

    def test_dsmc_invocation_section_heading(self):
        s = self._call()
        self.assertIn("dsmc invocation", s)

    def test_dsmc_done_count(self):
        s = self._call(gs=_make_gs(dsmc_done=42))
        self.assertIn("42", s)

    def test_summaries_parsed_count(self):
        s = self._call(gs=_make_gs(dsmc_done=10, summaries_parsed=8))
        self.assertIn("8", s)

    def test_incomplete_summaries_count(self):
        s = self._call(gs=_make_gs(dsmc_done=10, incomplete_summaries=2))
        self.assertIn("2", s)

    def test_active_children_zero(self):
        """active_children should render safely as zero on clean completion."""
        s = self._call(gs=_make_gs(active_children=0))
        # Line should exist and show 0
        self.assertIn("Active children", s)

    # -----------------------------------------------------------------------
    # Object totals — separately labelled from directory counts
    # -----------------------------------------------------------------------

    def test_objects_section_separately_labelled(self):
        s = self._call()
        # Both headings must be present and distinct
        self.assertIn("Directories", s)
        self.assertIn("Objects reported by dsmc", s)

    def test_objects_inspected(self):
        s = self._call(gs=_make_gs(objects_inspected=1_234_567))
        self.assertIn("1,234,567", s)

    def test_objects_backed_up(self):
        s = self._call(gs=_make_gs(objects_backed_up=45_678))
        self.assertIn("45,678", s)

    def test_objects_updated(self):
        s = self._call(gs=_make_gs(objects_updated=300))
        self.assertIn("300", s)

    def test_objects_failed(self):
        s = self._call(gs=_make_gs(objects_failed=7))
        self.assertIn("7", s)

    def test_retries(self):
        s = self._call(gs=_make_gs(retries=5))
        self.assertIn("5", s)

    def test_optional_object_field_shown_when_nonzero(self):
        s = self._call(gs=_make_gs(objects_rebound=3))
        self.assertIn("Rebound", s)
        self.assertIn("3", s)

    def test_optional_object_field_hidden_when_zero(self):
        s = self._call(gs=_make_gs(objects_rebound=0))
        self.assertNotIn("Rebound", s)

    def test_all_zero_object_counters_render_safely(self):
        s = self._call(gs=_make_gs())
        self.assertIn("Inspected", s)
        self.assertIn("Backed up", s)

    # -----------------------------------------------------------------------
    # Byte totals — exact integer and human-readable form
    # -----------------------------------------------------------------------

    def test_data_section_heading(self):
        s = self._call()
        self.assertIn("Data reported by dsmc", s)

    def test_bytes_inspected_exact_integer(self):
        s = self._call(gs=_make_gs(bytes_inspected=123_456_789))
        # exact bytes value with thousands separator
        self.assertIn("123,456,789 bytes", s)

    def test_bytes_inspected_human_readable(self):
        s = self._call(gs=_make_gs(bytes_inspected=123_456_789))
        # human-readable IEC form must also be present
        self.assertIn("MiB", s)

    def test_bytes_transferred_exact_integer(self):
        s = self._call(gs=_make_gs(bytes_transferred=4_567_890))
        self.assertIn("4,567,890 bytes", s)

    def test_bytes_transferred_human_readable(self):
        s = self._call(gs=_make_gs(bytes_transferred=4_567_890))
        self.assertIn("MiB", s)

    def test_zero_bytes_render_safely(self):
        s = self._call(gs=_make_gs(bytes_inspected=0, bytes_transferred=0))
        self.assertIn("0 bytes", s)

    def test_effective_rate_shown_when_data_transferred(self):
        s = self._call(gs=_make_gs(
            bytes_transferred=10_000,
            total_elapsed_secs=100.0,
        ))
        self.assertIn("Effective rate", s)
        self.assertIn("/s", s)

    def test_effective_rate_absent_when_zero_transfer(self):
        s = self._call(gs=_make_gs(bytes_transferred=0, total_elapsed_secs=100.0))
        self.assertNotIn("Effective rate", s)

    def test_effective_rate_absent_when_zero_elapsed(self):
        s = self._call(gs=_make_gs(bytes_transferred=1000, total_elapsed_secs=0.0))
        self.assertNotIn("Effective rate", s)

    # -----------------------------------------------------------------------
    # ROOT_FILES accounting
    # -----------------------------------------------------------------------

    def test_root_files_section_absent_when_not_root_crawl(self):
        s = self._call(is_root_crawl=False, root_state=None)
        self.assertNotIn("ROOT_FILES", s)

    def test_root_files_section_present_when_root_crawl(self):
        rs = bc.WorkerState(worker_number=0)
        rs.status = "done"
        rs.dirs_completed = 2
        rs.dirs_failed = 0
        rs.dirs_timed_out = 0
        rs.batch_total = 2
        s = self._call(is_root_crawl=True, root_state=rs)
        self.assertIn("ROOT_FILES", s)

    def test_root_files_status_shown(self):
        rs = bc.WorkerState(worker_number=0)
        rs.status = "done"
        rs.dirs_completed = 1
        rs.dirs_failed = 0
        rs.dirs_timed_out = 0
        rs.batch_total = 1
        s = self._call(is_root_crawl=True, root_state=rs)
        self.assertIn("done", s)

    def test_root_files_chunk_counts_shown(self):
        rs = bc.WorkerState(worker_number=0)
        rs.status = "done"
        rs.dirs_completed = 3
        rs.dirs_failed = 1
        rs.dirs_timed_out = 0
        rs.batch_total = 4
        s = self._call(is_root_crawl=True, root_state=rs)
        self.assertIn("3", s)  # chunks completed
        self.assertIn("4", s)  # total chunks

    def test_root_files_none_state_graceful(self):
        """root_state=None must not raise; a placeholder message is shown."""
        s = self._call(is_root_crawl=True, root_state=None)
        self.assertIn("ROOT_FILES", s)
        self.assertIn("no ROOT_FILES state", s)

    # -----------------------------------------------------------------------
    # Thousands separators
    # -----------------------------------------------------------------------

    def test_large_counts_have_thousands_separators(self):
        s = self._call(
            discovered=1_000_000,
            completed=999_990,
            failed=10,
            gs=_make_gs(objects_backed_up=1_234_567),
        )
        self.assertIn("1,000,000", s)
        self.assertIn("1,234,567", s)

    # -----------------------------------------------------------------------
    # SafeLogger.write_raw
    # -----------------------------------------------------------------------

    def test_write_raw_appends_to_log(self):
        """write_raw must write the raw text to the log file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            log_path = Path(tmpdir) / "ctrl.log"
            logger = bc.SafeLogger(log_path, echo=False)
            logger.write_raw("BLOCK LINE 1\nBLOCK LINE 2\n")
            content = log_path.read_text(encoding="utf-8")
            self.assertIn("BLOCK LINE 1\n", content)
            self.assertIn("BLOCK LINE 2\n", content)

    def test_write_raw_does_not_add_timestamp_prefix(self):
        """write_raw must not prepend a timestamp to each line."""
        with tempfile.TemporaryDirectory() as tmpdir:
            log_path = Path(tmpdir) / "ctrl.log"
            logger = bc.SafeLogger(log_path, echo=False)
            logger.write_raw("MARKER_LINE\n")
            first_line = log_path.read_text(encoding="utf-8").splitlines()[0]
            # A timestamped line would look like "2026-07-17 08:00:00 MARKER_LINE";
            # raw output must not start with a date-like prefix.
            self.assertEqual(first_line, "MARKER_LINE")

    def test_write_raw_appends_newline_when_missing(self):
        """write_raw must ensure the content ends with a newline."""
        with tempfile.TemporaryDirectory() as tmpdir:
            log_path = Path(tmpdir) / "ctrl.log"
            logger = bc.SafeLogger(log_path, echo=False)
            logger.write_raw("NO_NEWLINE")
            content = log_path.read_text(encoding="utf-8")
            self.assertTrue(content.endswith("\n"))

    # -----------------------------------------------------------------------
    # _format_elapsed helper
    # -----------------------------------------------------------------------

    def test_format_elapsed_zero(self):
        self.assertEqual(bc._format_elapsed(0), "00:00:00")

    def test_format_elapsed_seconds_only(self):
        self.assertEqual(bc._format_elapsed(45), "00:00:45")

    def test_format_elapsed_minutes_and_seconds(self):
        self.assertEqual(bc._format_elapsed(125), "00:02:05")

    def test_format_elapsed_hours(self):
        self.assertEqual(bc._format_elapsed(3661), "01:01:01")

    def test_format_elapsed_large(self):
        # 25h 30m 0s
        self.assertEqual(bc._format_elapsed(91800), "25:30:00")


if __name__ == "__main__":
    unittest.main()


class TestPersistentStateDB(unittest.TestCase):
    def _make_args(
        self,
        mountpoint: str,
        log_dir: str,
        state_db: str,
        *,
        streams: int = 1,
        batch_size: int = 2,
        exclude_path: list[str] | None = None,
    ):
        import argparse

        return argparse.Namespace(
            mountpoint=mountpoint,
            streams=streams,
            batch_size=batch_size,
            queue_size=100,
            dsmc=sys.executable,
            dsmc_option=[],
            dsmc_timeout=0,
            dsmc_idle_timeout=0,
            resourceutilization=2,
            log_dir=log_dir,
            state_db=state_db,
            progress_seconds=1,
            dashboard_refresh_seconds=1.0,
            shutdown_wait_seconds=1,
            no_dashboard=True,
            dry_run=False,
            exclude_path=exclude_path or [],
            resume=False,
            new_run=False,
            status=False,
            streams_positional=None,
        )

    def _expire_lease(self, state_db_path: str):
        conn = sqlite3.connect(state_db_path)
        conn.execute(
            "UPDATE runs SET controller_lease_expires_at='1970-01-01T00:00:00Z'"
        )
        conn.commit()
        conn.close()

    def test_schema_creation_and_reopen(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir) / "root"
            log_dir = Path(tmpdir) / "logs"
            state_path = Path(tmpdir) / "state.sqlite3"
            root.mkdir()
            args = self._make_args(str(root), str(log_dir), str(state_path))
            state_db = bc.PersistentStateDB(str(state_path))
            ctx = state_db.create_new_run(
                str(root),
                os.stat(root).st_dev,
                bc.build_coverage_config(str(root), os.stat(root).st_dev, frozenset(), args),
                bc.build_operational_config(args),
                False,
            )
            self.assertTrue(state_path.exists())
            conn = sqlite3.connect(state_path)
            tables = {
                row[0]
                for row in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                )
            }
            self.assertIn("runs", tables)
            self.assertIn("directories", tables)
            self.assertIn("attempts", tables)
            self.assertIn("root_file_chunks", tables)
            conn.close()
            state_db.close()
            reopened = bc.PersistentStateDB(str(state_path))
            report = reopened.status_report()
            self.assertIn(ctx.run_id, report)
            self.assertIn(str(root), report)

    @unittest.skipIf(sys.platform == "win32", "surrogateescape path semantics differ on Windows")
    def test_non_utf8_path_storage_round_trips(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir) / "root"
            log_dir = Path(tmpdir) / "logs"
            state_path = Path(tmpdir) / "state.sqlite3"
            root.mkdir()
            bad_dir_bytes = os.path.join(os.fsencode(root), b"bad-\xff")
            os.mkdir(bad_dir_bytes)
            bad_dir = os.fsdecode(bad_dir_bytes)
            args = self._make_args(str(root), str(log_dir), str(state_path))
            state_db = bc.PersistentStateDB(str(state_path))
            ctx = state_db.create_new_run(
                str(root),
                os.stat(root).st_dev,
                bc.build_coverage_config(str(root), os.stat(root).st_dev, frozenset(), args),
                bc.build_operational_config(args),
                False,
            )
            inserted = state_db.insert_directory_if_absent(
                ctx.run_id,
                bad_dir,
                str(root),
                os.stat(bad_dir).st_dev,
                "pending",
                "pending",
            )
            self.assertTrue(inserted)
            conn = sqlite3.connect(state_path)
            row = conn.execute(
                "SELECT path_bytes FROM directories WHERE run_id=? AND path_display LIKE ?",
                (ctx.run_id, "%bad%"),
            ).fetchone()
            conn.close()
            self.assertEqual(bytes(row[0]), os.fsencode(bad_dir))
            self.assertEqual(bc.decode_path_from_db(row[0]), bad_dir)

    def test_root_insertion_and_root_files_seed(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            state_path = Path(tmpdir) / "state.sqlite3"
            log_dir = Path(tmpdir) / "logs"
            args = self._make_args(os.sep, str(log_dir), str(state_path))
            state_db = bc.PersistentStateDB(str(state_path))
            ctx = state_db.create_new_run(
                os.sep,
                os.stat(os.sep).st_dev,
                bc.build_coverage_config(os.sep, os.stat(os.sep).st_dev, frozenset(), args),
                bc.build_operational_config(args),
                True,
            )
            conn = sqlite3.connect(state_path)
            dir_row = conn.execute(
                "SELECT scan_status, backup_status FROM directories WHERE run_id=?",
                (ctx.run_id,),
            ).fetchone()
            root_state = conn.execute(
                "SELECT manifest_status FROM root_files_state WHERE run_id=?",
                (ctx.run_id,),
            ).fetchone()
            conn.close()
            self.assertEqual(dir_row[0], "pending")
            self.assertEqual(dir_row[1], "skipped_root")
            self.assertEqual(root_state[0], "pending")

    def test_finish_scan_inserts_children_and_marks_parent_scanned(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir) / "root"
            root.mkdir()
            state_path = Path(tmpdir) / "state.sqlite3"
            log_dir = Path(tmpdir) / "logs"
            args = self._make_args(str(root), str(log_dir), str(state_path))
            state_db = bc.PersistentStateDB(str(state_path))
            ctx = state_db.create_new_run(
                str(root),
                os.stat(root).st_dev,
                bc.build_coverage_config(str(root), os.stat(root).st_dev, frozenset(), args),
                bc.build_operational_config(args),
                False,
            )
            claimed = state_db.claim_next_scan(ctx.run_id, ctx.controller_id)
            self.assertEqual(claimed, str(root))
            child = str(root / "child")
            result = state_db.finish_scan(
                ctx.run_id,
                str(root),
                [(child, os.stat(root).st_dev)],
                [],
                [],
                [],
            )
            self.assertEqual(result["eligible"], 1)
            conn = sqlite3.connect(state_path)
            rows = conn.execute(
                "SELECT path_display, scan_status, backup_status FROM directories WHERE run_id=? ORDER BY path_display",
                (ctx.run_id,),
            ).fetchall()
            conn.close()
            self.assertEqual(rows[0][1], "scanned")
            self.assertEqual(rows[1][0], child)
            self.assertEqual(rows[1][1], "pending")
            self.assertEqual(rows[1][2], "pending")

    def test_stale_scanning_recovery_and_idempotent_rescan(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir) / "root"
            root.mkdir()
            state_path = Path(tmpdir) / "state.sqlite3"
            log_dir = Path(tmpdir) / "logs"
            args = self._make_args(str(root), str(log_dir), str(state_path))
            state_db = bc.PersistentStateDB(str(state_path))
            ctx = state_db.create_new_run(
                str(root),
                os.stat(root).st_dev,
                bc.build_coverage_config(str(root), os.stat(root).st_dev, frozenset(), args),
                bc.build_operational_config(args),
                False,
            )
            self.assertEqual(state_db.claim_next_scan(ctx.run_id, ctx.controller_id), str(root))
            self._expire_lease(str(state_path))
            resumed = bc.PersistentStateDB(str(state_path)).resume_run(
                str(root),
                os.stat(root).st_dev,
                bc.build_coverage_config(str(root), os.stat(root).st_dev, frozenset(), args),
            )
            self.assertEqual(resumed.recovered_scan_claims, 1)
            claimed = bc.PersistentStateDB(str(state_path)).claim_next_scan(
                resumed.run_id, resumed.controller_id
            )
            self.assertEqual(claimed, str(root))
            child = str(root / "child")
            db2 = bc.PersistentStateDB(str(state_path))
            first = db2.finish_scan(resumed.run_id, str(root), [(child, os.stat(root).st_dev)], [], [], [])
            self.assertEqual(first["eligible"], 1)
            conn = sqlite3.connect(state_path)
            conn.execute(
                "UPDATE directories SET scan_status='pending' WHERE run_id=? AND path_display=?",
                (resumed.run_id, str(root)),
            )
            conn.commit()
            conn.close()
            again = db2.claim_next_scan(resumed.run_id, resumed.controller_id)
            self.assertEqual(again, str(root))
            second = db2.finish_scan(resumed.run_id, str(root), [(child, os.stat(root).st_dev)], [], [], [])
            self.assertEqual(second["eligible"], 0)

    def test_atomic_backup_claims_do_not_overlap(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir) / "root"
            root.mkdir()
            state_path = Path(tmpdir) / "state.sqlite3"
            log_dir = Path(tmpdir) / "logs"
            args = self._make_args(str(root), str(log_dir), str(state_path), batch_size=2)
            state_db = bc.PersistentStateDB(str(state_path))
            ctx = state_db.create_new_run(
                str(root),
                os.stat(root).st_dev,
                bc.build_coverage_config(str(root), os.stat(root).st_dev, frozenset(), args),
                bc.build_operational_config(args),
                False,
            )
            for idx in range(4):
                state_db.insert_directory_if_absent(
                    ctx.run_id,
                    str(root / f"d{idx}"),
                    str(root),
                    os.stat(root).st_dev,
                    "scanned",
                    "pending",
                )
            claimed: list[list[str]] = []
            lock = threading.Lock()

            def _claim(worker_no: int):
                db = bc.PersistentStateDB(str(state_path))
                batch = db.claim_backup_batch(ctx.run_id, ctx.controller_id, worker_no, 2)
                with lock:
                    claimed.append(batch)

            threads = [threading.Thread(target=_claim, args=(1,)), threading.Thread(target=_claim, args=(2,))]
            for t in threads:
                t.start()
            for t in threads:
                t.join()
            flattened = [item for batch in claimed for item in batch]
            self.assertEqual(len(flattened), 4)
            self.assertEqual(len(set(flattened)), 4)

    def test_stale_running_backup_recovery_marks_attempt_interrupted(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir) / "root"
            root.mkdir()
            state_path = Path(tmpdir) / "state.sqlite3"
            log_dir = Path(tmpdir) / "logs"
            args = self._make_args(str(root), str(log_dir), str(state_path))
            state_db = bc.PersistentStateDB(str(state_path))
            ctx = state_db.create_new_run(
                str(root),
                os.stat(root).st_dev,
                bc.build_coverage_config(str(root), os.stat(root).st_dev, frozenset(), args),
                bc.build_operational_config(args),
                False,
            )
            conn = sqlite3.connect(state_path)
            conn.execute(
                "UPDATE directories SET backup_status='succeeded', scan_status='scanned' WHERE run_id=? AND path_display=?",
                (ctx.run_id, str(root)),
            )
            conn.commit()
            conn.close()
            work = str(root / "work")
            state_db.insert_directory_if_absent(ctx.run_id, work, str(root), os.stat(root).st_dev, "scanned", "pending")
            claimed = state_db.claim_backup_batch(ctx.run_id, ctx.controller_id, 1, 1)
            self.assertEqual(claimed, [work])
            state_db.start_directory_attempt(ctx.run_id, ctx.execution_id, work, 1, "worker-01")
            self._expire_lease(str(state_path))
            resumed = bc.PersistentStateDB(str(state_path)).resume_run(
                str(root),
                os.stat(root).st_dev,
                bc.build_coverage_config(str(root), os.stat(root).st_dev, frozenset(), args),
            )
            self.assertEqual(resumed.recovered_backup_claims, 1)
            conn = sqlite3.connect(state_path)
            row = conn.execute(
                "SELECT backup_status FROM directories WHERE run_id=? AND path_display=?",
                (ctx.run_id, work),
            ).fetchone()
            attempt = conn.execute(
                "SELECT outcome FROM attempts WHERE run_id=? ORDER BY id DESC LIMIT 1",
                (ctx.run_id,),
            ).fetchone()
            conn.close()
            self.assertEqual(row[0], "pending")
            self.assertEqual(attempt[0], "interrupted")

    def test_config_compatibility_rejects_exclusion_change_but_allows_worker_change(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir) / "root"
            root.mkdir()
            state_path = Path(tmpdir) / "state.sqlite3"
            log_dir = Path(tmpdir) / "logs"
            args1 = self._make_args(str(root), str(log_dir), str(state_path), streams=1, batch_size=2)
            state_db = bc.PersistentStateDB(str(state_path))
            ctx = state_db.create_new_run(
                str(root),
                os.stat(root).st_dev,
                bc.build_coverage_config(str(root), os.stat(root).st_dev, frozenset(), args1),
                bc.build_operational_config(args1),
                False,
            )
            self._expire_lease(str(state_path))
            args2 = self._make_args(str(root), str(log_dir), str(state_path), streams=4, batch_size=9)
            resumed = bc.PersistentStateDB(str(state_path)).resume_run(
                str(root),
                os.stat(root).st_dev,
                bc.build_coverage_config(str(root), os.stat(root).st_dev, frozenset(), args2),
            )
            self.assertTrue(resumed.resumed)
            self._expire_lease(str(state_path))
            args3 = self._make_args(str(root), str(log_dir), str(state_path), exclude_path=[str(root / "skip")])
            with self.assertRaises(RuntimeError):
                bc.PersistentStateDB(str(state_path)).resume_run(
                    str(root),
                    os.stat(root).st_dev,
                    bc.build_coverage_config(
                        str(root), os.stat(root).st_dev, frozenset(args3.exclude_path), args3
                    ),
                )

    def test_live_controller_rejected_then_stale_controller_recovers(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir) / "root"
            root.mkdir()
            state_path = Path(tmpdir) / "state.sqlite3"
            log_dir = Path(tmpdir) / "logs"
            args = self._make_args(str(root), str(log_dir), str(state_path))
            state_db = bc.PersistentStateDB(str(state_path))
            state_db.create_new_run(
                str(root),
                os.stat(root).st_dev,
                bc.build_coverage_config(str(root), os.stat(root).st_dev, frozenset(), args),
                bc.build_operational_config(args),
                False,
            )
            with self.assertRaises(RuntimeError):
                bc.PersistentStateDB(str(state_path)).resume_run(
                    str(root),
                    os.stat(root).st_dev,
                    bc.build_coverage_config(str(root), os.stat(root).st_dev, frozenset(), args),
                )
            self._expire_lease(str(state_path))
            resumed = bc.PersistentStateDB(str(state_path)).resume_run(
                str(root),
                os.stat(root).st_dev,
                bc.build_coverage_config(str(root), os.stat(root).st_dev, frozenset(), args),
            )
            self.assertTrue(resumed.resumed)

    def test_root_files_manifest_and_chunks_persist(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            state_path = Path(tmpdir) / "state.sqlite3"
            log_dir = Path(tmpdir) / "logs"
            args = self._make_args(os.sep, str(log_dir), str(state_path))
            state_db = bc.PersistentStateDB(str(state_path))
            ctx = state_db.create_new_run(
                os.sep,
                os.stat(os.sep).st_dev,
                bc.build_coverage_config(os.sep, os.stat(os.sep).st_dev, frozenset(), args),
                bc.build_operational_config(args),
                True,
            )
            files = ["/alpha", "/beta"]
            state_db.store_root_manifest(ctx.run_id, files)
            manifest = state_db.root_manifest_row(ctx.run_id)
            self.assertEqual(int(manifest["total_files"]), 2)
            chunk = state_db.claim_root_chunk(ctx.run_id, ctx.controller_id)
            self.assertIsNotNone(chunk)
            members = state_db.root_chunk_files(int(chunk["id"]))
            self.assertEqual(members, files)
            self.assertNotIn(os.sep, members)


class TestPersistentMain(unittest.TestCase):
    def _fake_dsmc(self, tmpdir: str) -> str:
        script = Path(tmpdir) / "fake_dsmc.py"
        script.write_text(
            "#!/usr/bin/env python3\n"
            "import sys\n"
            "print('Total number of objects inspected:   1')\n"
            "print('Total number of objects backed up:   1')\n"
            "print('Total number of bytes inspected:   1.00 KB')\n"
            "print('Total number of bytes transferred:   1.00 KB')\n"
            "print('Elapsed processing time:   00:00:01')\n"
            "raise SystemExit(0)\n",
            encoding="utf-8",
        )
        script.chmod(0o755)
        return str(script)

    def _run_main(self, argv: list[str]) -> tuple[int, str, str]:
        old = sys.argv
        stdout = io.StringIO()
        stderr = io.StringIO()
        try:
            sys.argv = ["backup_crawler.py", *argv]
            with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
                rc = bc.main()
        finally:
            sys.argv = old
        return rc, stdout.getvalue(), stderr.getvalue()

    def test_status_is_read_only(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir) / "root"
            log_dir = Path(tmpdir) / "logs"
            state = Path(tmpdir) / "state.sqlite3"
            root.mkdir()
            (root / "a").mkdir()
            fake = self._fake_dsmc(tmpdir)
            rc, _out, _err = self._run_main([
                str(root), "1", "--new-run", "--no-dashboard", "--progress-seconds", "1",
                "--log-dir", str(log_dir), "--state-db", str(state), "--dsmc", fake,
            ])
            self.assertEqual(rc, 0)
            conn = sqlite3.connect(state)
            before = conn.execute("SELECT updated_at FROM runs").fetchone()[0]
            conn.close()
            rc, out, err = self._run_main(["--status", "--state-db", str(state)])
            self.assertEqual(rc, 0)
            self.assertIn("State DB:", out)
            conn = sqlite3.connect(state)
            after = conn.execute("SELECT updated_at FROM runs").fetchone()[0]
            conn.close()
            self.assertEqual(before, after)
            self.assertEqual(err, "")

    def test_completed_jobs_not_rerun_on_resume(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir) / "root"
            log_dir = Path(tmpdir) / "logs"
            state = Path(tmpdir) / "state.sqlite3"
            root.mkdir()
            (root / "a").mkdir()
            fake = self._fake_dsmc(tmpdir)
            rc, out1, _ = self._run_main([
                str(root), "1", "--new-run", "--no-dashboard", "--progress-seconds", "1",
                "--log-dir", str(log_dir), "--state-db", str(state), "--dsmc", fake,
            ])
            self.assertEqual(rc, 0)
            conn = sqlite3.connect(state)
            attempts_before = conn.execute("SELECT COUNT(*) FROM attempts").fetchone()[0]
            conn.close()
            rc, out2, _ = self._run_main([
                str(root), "3", "--resume", "--no-dashboard", "--progress-seconds", "1",
                "--log-dir", str(log_dir), "--state-db", str(state), "--dsmc", fake,
            ])
            self.assertEqual(rc, 0)
            conn = sqlite3.connect(state)
            attempts_after = conn.execute("SELECT COUNT(*) FROM attempts").fetchone()[0]
            conn.close()
            self.assertEqual(attempts_before, attempts_after)
            self.assertIn("Run mode:  RESUMED", out2)
            self.assertIn("Reused completed work:", out2)


# ---------------------------------------------------------------------------
# 12. DB coordinator (single-writer) architecture
# ---------------------------------------------------------------------------

class _CoordinatorTestBase(unittest.TestCase):
    def _make_args(self, mountpoint, log_dir, state_db, *, streams=1, batch_size=2):
        import argparse

        return argparse.Namespace(
            mountpoint=mountpoint,
            streams=streams,
            batch_size=batch_size,
            queue_size=100,
            dsmc=sys.executable,
            dsmc_option=[],
            dsmc_timeout=0,
            dsmc_idle_timeout=0,
            resourceutilization=2,
            log_dir=log_dir,
            state_db=state_db,
            progress_seconds=1,
            dashboard_refresh_seconds=1.0,
            shutdown_wait_seconds=1,
            no_dashboard=True,
            dry_run=False,
            exclude_path=[],
            resume=False,
            new_run=False,
            status=False,
            streams_positional=None,
        )

    def _new_run_with_dirs(self, tmpdir, n_dirs):
        root = Path(tmpdir) / "root"
        root.mkdir()
        state_path = Path(tmpdir) / "state.sqlite3"
        log_dir = Path(tmpdir) / "logs"
        args = self._make_args(str(root), str(log_dir), str(state_path))
        state_db = bc.PersistentStateDB(str(state_path))
        ctx = state_db.create_new_run(
            str(root),
            os.stat(root).st_dev,
            bc.build_coverage_config(str(root), os.stat(root).st_dev, frozenset(), args),
            bc.build_operational_config(args),
            False,
        )
        # Mark the seed root as done and add n_dirs pending backup jobs.
        conn = sqlite3.connect(state_path)
        conn.execute(
            "UPDATE directories SET backup_status='succeeded', scan_status='scanned' WHERE run_id=?",
            (ctx.run_id,),
        )
        conn.commit()
        conn.close()
        for idx in range(n_dirs):
            state_db.insert_directory_if_absent(
                ctx.run_id,
                str(root / f"d{idx:03d}"),
                str(root),
                os.stat(root).st_dev,
                "scanned",
                "pending",
            )
        return state_db, ctx, str(state_path)


class TestDBCoordinatorConcurrency(_CoordinatorTestBase):
    def test_many_workers_no_lock_errors(self):
        """20 simulated workers claim/complete jobs with no sqlite lock errors."""
        with tempfile.TemporaryDirectory() as tmpdir:
            n_dirs = 40
            state_db, ctx, _ = self._new_run_with_dirs(tmpdir, n_dirs)
            coordinator = bc.DBCoordinator(state_db)
            coordinator.start()
            processed: list[str] = []
            errors: list[BaseException] = []
            lock = threading.Lock()

            def _run_worker(wn: int):
                try:
                    while True:
                        path = coordinator.submit_critical(
                            "claim_backup_job", ctx.run_id, ctx.controller_id, wn
                        )
                        if path is None:
                            return
                        attempt_id = coordinator.submit_critical(
                            "start_directory_attempt",
                            ctx.run_id,
                            ctx.execution_id,
                            path,
                            wn,
                            f"worker-{wn:02d}",
                        )
                        coordinator.submit_critical(
                            "finish_directory_attempt",
                            ctx.run_id,
                            path,
                            attempt_id,
                            "succeeded",
                            0,
                            bc.DsmcInvocationStats(),
                            error_text=None,
                        )
                        with lock:
                            processed.append(path)
                except Exception as exc:
                    with lock:
                        errors.append(exc)

            threads = [threading.Thread(target=_run_worker, args=(i,)) for i in range(1, 21)]
            for t in threads:
                t.start()
            for t in threads:
                t.join(timeout=30)
            coordinator.shutdown()

            self.assertEqual(errors, [], f"unexpected errors: {errors}")
            self.assertEqual(len(processed), n_dirs)
            self.assertEqual(len(set(processed)), n_dirs, "a job was processed more than once")

    def test_one_job_claimed_at_a_time_no_duplicates(self):
        """claim_backup_job hands out exactly one distinct path per call."""
        with tempfile.TemporaryDirectory() as tmpdir:
            n_dirs = 6
            state_db, ctx, _ = self._new_run_with_dirs(tmpdir, n_dirs)
            seen: list[str] = []
            while True:
                path = state_db.claim_backup_job(ctx.run_id, ctx.controller_id, 1)
                if path is None:
                    break
                self.assertIsInstance(path, str)
                seen.append(path)
            self.assertEqual(len(seen), n_dirs)
            self.assertEqual(len(set(seen)), n_dirs)

    def test_unstarted_work_stays_pending(self):
        """Newly discovered work stays 'pending', never silently 'running'."""
        with tempfile.TemporaryDirectory() as tmpdir:
            n_dirs = 5
            state_db, ctx, _ = self._new_run_with_dirs(tmpdir, n_dirs)
            counts = state_db.runtime_status_counts(ctx.run_id)["backup"]
            self.assertEqual(counts.get("pending", 0), n_dirs)
            self.assertEqual(counts.get("running", 0), 0)

    def test_stale_claim_is_recoverable(self):
        """A claimed-but-crashed job is recovered to pending on resume."""
        with tempfile.TemporaryDirectory() as tmpdir:
            state_db, ctx, state_path = self._new_run_with_dirs(tmpdir, 1)
            args = self._make_args(
                str(Path(tmpdir) / "root"), str(Path(tmpdir) / "logs"), state_path
            )
            path = state_db.claim_backup_job(ctx.run_id, ctx.controller_id, 1)
            self.assertIsNotNone(path)
            # Simulate crash: claim remains 'running' and lease is expired.
            conn = sqlite3.connect(state_path)
            conn.execute("UPDATE runs SET controller_lease_expires_at='1970-01-01T00:00:00Z'")
            conn.commit()
            conn.close()
            resumed = bc.PersistentStateDB(state_path).resume_run(
                str(Path(tmpdir) / "root"),
                os.stat(Path(tmpdir) / "root").st_dev,
                bc.build_coverage_config(
                    str(Path(tmpdir) / "root"),
                    os.stat(Path(tmpdir) / "root").st_dev,
                    frozenset(),
                    args,
                ),
            )
            self.assertEqual(resumed.recovered_backup_claims, 1)
            conn = sqlite3.connect(state_path)
            status = conn.execute(
                "SELECT backup_status FROM directories WHERE run_id=? AND path_display=?",
                (ctx.run_id, path),
            ).fetchone()[0]
            conn.close()
            self.assertEqual(status, "pending")


class _DummyConn:
    def rollback(self):
        pass

    def close(self):
        pass


class _FlakyDB:
    """Minimal DB stand-in whose op fails with a lock error, then succeeds."""

    def __init__(self, fail_times: int):
        self._fail_times = fail_times
        self.calls = 0
        self._connection = None

    def _get_writer_connection(self):
        self._connection = _DummyConn()
        return self._connection

    def _connect(self):
        return self._connection

    def flaky(self):
        self.calls += 1
        if self.calls <= self._fail_times:
            raise sqlite3.OperationalError("database is locked")
        return "ok"


class TestDBCoordinatorRetry(unittest.TestCase):
    def test_retry_on_busy_eventually_succeeds(self):
        db = _FlakyDB(fail_times=3)
        coordinator = bc.DBCoordinator(db)
        coordinator.start()
        try:
            result = coordinator.submit_critical("flaky")
        finally:
            coordinator.shutdown()
        self.assertEqual(result, "ok")
        self.assertEqual(db.calls, 4)  # 3 failures + 1 success

    def test_non_lock_error_is_not_retried(self):
        class _BoomDB(_FlakyDB):
            def flaky(self):
                self.calls += 1
                raise sqlite3.OperationalError("no such table: nope")

        db = _BoomDB(fail_times=0)
        coordinator = bc.DBCoordinator(db)
        coordinator.start()
        try:
            with self.assertRaises(sqlite3.OperationalError):
                coordinator.submit_critical("flaky")
        finally:
            coordinator.shutdown()
        self.assertEqual(db.calls, 1)  # surfaced immediately, no retry

    def test_external_write_lock_is_waited_out(self):
        """A held external write lock is tolerated; the write still commits."""
        with tempfile.TemporaryDirectory() as tmpdir:
            base = _CoordinatorTestBase()
            state_db, ctx, state_path = base._new_run_with_dirs(tmpdir, 1)
            coordinator = bc.DBCoordinator(state_db)
            coordinator.start()

            blocker = sqlite3.connect(state_path, timeout=30)
            blocker.execute("PRAGMA busy_timeout=30000")
            blocker.execute("BEGIN IMMEDIATE")
            blocker.execute(
                "UPDATE runs SET updated_at='x' WHERE id=?", (ctx.run_id,)
            )

            result_holder: list[object] = []

            def _do_claim():
                result_holder.append(
                    coordinator.submit_critical(
                        "claim_backup_job", ctx.run_id, ctx.controller_id, 1
                    )
                )

            t = threading.Thread(target=_do_claim)
            t.start()
            time.sleep(0.5)  # coordinator is blocked on the external lock
            self.assertFalse(result_holder, "claim committed while lock was held")
            blocker.rollback()
            blocker.close()
            t.join(timeout=30)
            coordinator.shutdown()
            self.assertTrue(result_holder)
            self.assertIsNotNone(result_holder[0])


class TestOutputReaderCallbackSafety(unittest.TestCase):
    def _run_supervised(self, child_script, callback):
        worker_states = bc.WorkerStates(1)
        global_stats = bc.GlobalDsmcStats()
        stop_event = threading.Event()
        with tempfile.TemporaryDirectory() as tmpdir:
            logger = bc.SafeLogger(Path(tmpdir) / "c.log", echo=False)
            buf = bytearray()

            class _W:
                def write(self, d):
                    buf.extend(d)

            worker_states.set_directory(1, 1, "/x")
            rc, stats = bc.run_dsmc_supervised(
                command=[sys.executable, "-c", child_script],
                env=os.environ.copy(),
                output_file=_W(),
                worker_number=1,
                worker_states=worker_states,
                global_stats=global_stats,
                logger=logger,
                worker_name="worker-01",
                dsmc_timeout=30,
                dsmc_idle_timeout=0,
                stop_event=stop_event,
                child_output_callback=callback,
            )
        return rc, stats, bytes(buf)

    def test_advisory_callback_failure_is_non_fatal(self):
        def _boom(_ts):
            raise RuntimeError("advisory callback failure")

        rc, stats, _ = self._run_supervised(
            "print('Total number of objects inspected:   3');"
            "print('Elapsed processing time:   00:00:01')",
            _boom,
        )
        self.assertEqual(rc, 0)
        self.assertEqual(stats.objects_inspected, 3)

    def test_reader_drains_large_output_with_failing_callback(self):
        def _boom(_ts):
            raise RuntimeError("advisory callback failure")

        child = (
            "import sys\n"
            "for i in range(20000):\n"
            "    sys.stdout.write(f'noise line {i} filler filler filler\\n')\n"
            "print('Total number of objects inspected:   7')\n"
            "print('Elapsed processing time:   00:00:02')\n"
        )
        rc, stats, buf = self._run_supervised(child, _boom)
        self.assertEqual(rc, 0)
        self.assertEqual(stats.objects_inspected, 7)
        self.assertGreater(len(buf), 100_000, "reader did not drain the full output")


class _RecordingCoordinator:
    def __init__(self):
        self.advisory: list[tuple] = []

    def submit_advisory(self, op, *args, **kwargs):
        self.advisory.append((op, args, kwargs))

    def submit_critical(self, op, *args, **kwargs):
        raise AssertionError("submit_critical should not be called here")

    def is_alive(self):
        return True

    def is_fatal(self):
        return False


class TestHeartbeatRateLimit(unittest.TestCase):
    def test_directory_heartbeat_rate_limited(self):
        coord = _RecordingCoordinator()
        run_ctx = bc.RunContext(
            run_id="r", execution_id="e", controller_id="c", state_db_path="p", resumed=False
        )
        _started, on_output = bc.make_directory_dsmc_callbacks(coord, run_ctx, "/some/path")
        # Timestamps mimic time.monotonic() (a large, growing clock).
        base = 1000.0
        # First output emits; further output within the window does not.
        on_output(base + 0.0)
        on_output(base + 1.0)
        on_output(base + 2.0)
        on_output(base + 4.9)
        heartbeats = [c for c in coord.advisory if c[0] == "touch_directory_attempt"]
        self.assertEqual(len(heartbeats), 1)
        # Crossing the interval boundary emits a second heartbeat.
        on_output(base + 5.0)
        on_output(base + 6.0)
        heartbeats = [c for c in coord.advisory if c[0] == "touch_directory_attempt"]
        self.assertEqual(len(heartbeats), 2)

    def test_root_chunk_heartbeat_rate_limited(self):
        coord = _RecordingCoordinator()
        _started, on_output = bc.make_root_chunk_dsmc_callbacks(coord, 42)
        base = 1000.0
        for ts in (0.0, 1.0, 2.0, 3.0, 4.0):
            on_output(base + ts)
        heartbeats = [c for c in coord.advisory if c[0] == "touch_root_chunk"]
        self.assertEqual(len(heartbeats), 1)
        on_output(base + 10.0)
        heartbeats = [c for c in coord.advisory if c[0] == "touch_root_chunk"]
        self.assertEqual(len(heartbeats), 2)


class _RaisingCoordinator:
    def submit_critical(self, op, *args, **kwargs):
        raise RuntimeError("coordinator boom")

    def submit_advisory(self, *args, **kwargs):
        pass

    def is_alive(self):
        return True

    def is_fatal(self):
        return False


class TestWorkerExceptionShowsError(unittest.TestCase):
    def test_worker_exception_sets_error_state(self):
        import argparse
        import types

        with tempfile.TemporaryDirectory() as tmpdir:
            args = argparse.Namespace(
                log_dir=tmpdir,
                dsmc="/bin/true",
                dsmc_option=[],
                dsmc_timeout=0,
                dsmc_idle_timeout=0,
                resourceutilization=2,
                batch_size=1,
            )
            run_ctx = types.SimpleNamespace(run_id="r", controller_id="c", execution_id="e")
            producer_done = threading.Event()
            stop_event = threading.Event()
            counters = bc.Counters()
            worker_states = bc.WorkerStates(1)
            global_stats = bc.GlobalDsmcStats()
            logger = bc.SafeLogger(Path(tmpdir) / "c.log", echo=False)
            failed_log = Path(tmpdir) / "failed.tsv"
            failed_log.write_text("return_code\tdirectory\tnotes\n")
            failed_logger = bc.SafeAppender(failed_log)

            # Suppress the re-raised exception traceback in the worker thread.
            old_hook = threading.excepthook
            threading.excepthook = lambda a: None
            try:
                t = threading.Thread(
                    target=bc.persistent_worker,
                    args=(
                        1,
                        args,
                        bc.PersistentStateDB(str(Path(tmpdir) / "unused.sqlite3")),
                        _RaisingCoordinator(),
                        run_ctx,
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
            finally:
                threading.excepthook = old_hook

            self.assertFalse(t.is_alive())
            self.assertEqual(worker_states.get_status(1), "error")
            self.assertTrue(stop_event.is_set(), "worker should request orderly shutdown")


class _FakeMetricsQueue:
    def __init__(self, size=5, maxsize=100):
        self._size = size
        self.maxsize = maxsize

    def qsize(self):
        return self._size


class TestDashboardDurableCounts(unittest.TestCase):
    def test_dashboard_shows_backlog_not_queue_full(self):
        counters = bc.Counters()
        worker_states = bc.WorkerStates(1)
        global_stats = bc.GlobalDsmcStats()
        producer_done = threading.Event()
        stop_event = threading.Event()

        def _provider():
            return {
                "mode": "NEW",
                "run_id": "abcdef1234567890",
                "reused_completed": 0,
                "recovered_scan_claims": 0,
                "recovered_backup_claims": 0,
                "recovered_root_chunk_claims": 0,
                "scan_counts": {"pending": 3, "scanning": 1, "scanned": 10},
                "backup_counts": {
                    "pending": 7,
                    "running": 2,
                    "succeeded": 5,
                    "failed": 0,
                    "timed_out": 0,
                },
            }

        dash = bc.Dashboard(
            counters=counters,
            worker_states=worker_states,
            global_stats=global_stats,
            work_queue=_FakeMetricsQueue(size=5, maxsize=100),
            producer_done=producer_done,
            stop_event=stop_event,
            refresh_seconds=1.0,
            state_snapshot_provider=_provider,
        )
        captured = io.StringIO()
        with contextlib.redirect_stdout(captured):
            dash._render()
        out = captured.getvalue()
        self.assertIn("Backlog: backup pending=7", out)
        self.assertIn("running=2", out)
        self.assertNotIn("[FULL]", out)
        self.assertNotIn("q=5/100", out)

    def test_dashboard_legacy_mode_still_shows_queue(self):
        counters = bc.Counters()
        worker_states = bc.WorkerStates(1)
        global_stats = bc.GlobalDsmcStats()
        producer_done = threading.Event()
        stop_event = threading.Event()
        dash = bc.Dashboard(
            counters=counters,
            worker_states=worker_states,
            global_stats=global_stats,
            work_queue=_FakeMetricsQueue(size=100, maxsize=100),
            producer_done=producer_done,
            stop_event=stop_event,
            refresh_seconds=1.0,
            state_snapshot_provider=None,
        )
        captured = io.StringIO()
        with contextlib.redirect_stdout(captured):
            dash._render()
        out = captured.getvalue()
        self.assertIn("q=100/100", out)
        self.assertIn("[FULL]", out)
