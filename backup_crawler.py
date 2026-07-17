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
* Durable scheduler state is stored in a SQLite database (standard-library
  ``sqlite3``) so unfinished scan frontier and backup work can be resumed after
  Ctrl-C, process termination, or host reboot.  The implementation provides
  at-least-once execution semantics: after an ambiguous crash an already-
  successful ``dsmc`` invocation may be retried, but eligible directories are
  not silently omitted.

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
import calendar
import dataclasses
import hashlib
import json
import os
import queue
import re
import shlex
import shutil
import signal
import socket
import sqlite3
import subprocess
import sys
import threading
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path

# ---------------------------------------------------------------------------
# Clear Screen
# ---------------------------------------------------------------------------
def maybe_clear_screen() -> None:
    if sys.stdout.isatty() and os.environ.get("TERM"):
        os.system("clear")


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

INTERRUPTED_RC = 130
STATE_DB_FILENAME = "backup-crawler-state.sqlite3"
SQLITE_SCHEMA_VERSION = 1
SQLITE_POLICY_VERSION = "v1"
CONTROLLER_LEASE_SECS = 15
CONTROLLER_HEARTBEAT_SECS = 5

_UNIT_MULTIPLIERS: dict[str, int] = {
    "B": 1,
    "KB": 1024,
    "MB": 1024 ** 2,
    "GB": 1024 ** 3,
    "TB": 1024 ** 4,
    "PB": 1024 ** 5,
}

# IEC unit labels and thresholds used by format_bytes (1024-based).
_IEC_UNITS = ("B", "KiB", "MiB", "GiB", "TiB", "PiB")


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
    bytes_inspected: int = 0     # exact integer bytes
    bytes_transferred: int = 0   # exact integer bytes
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


def _bytes_to_si(value_str: str, unit_str: str) -> int:
    """Convert a value + IBM unit string to an exact integer byte count.

    IBM Storage Protect uses labels such as ``KB``, ``MB``, ``GB`` with
    1024-based (binary) multipliers — consistent with IEC convention.
    Sub-byte fractions are rounded to the nearest whole byte.

    An unrecognised unit falls back to a multiplier of 1 (treats the raw
    value as bytes) rather than raising an exception.  This is intentional:
    it degrades gracefully for future IBM unit strings while still recording
    a non-zero value that makes the summary visibly non-zero in the dashboard.
    """
    raw = _parse_float_field(value_str) * _UNIT_MULTIPLIERS.get(unit_str.strip().upper(), 1)
    return round(raw)


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
            val = 0
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
    """Format a byte count as a concise human-readable string using IEC units.

    Uses 1024-based (binary) multipliers and IEC unit labels:
    B, KiB, MiB, GiB, TiB, PiB.  This matches IBM Storage Protect's own
    convention (its ``KB``/``MB``/``GB`` labels are also 1024-based).
    """
    for unit in _IEC_UNITS[:-1]:
        if abs(value) < 1024.0:
            return f"{value:.2f} {unit}"
        value /= 1024.0
    return f"{value:.2f} {_IEC_UNITS[-1]}"


def format_rate(bps: float) -> str:
    """Format a bytes/sec value as a human-readable rate string."""
    return format_bytes(bps) + "/s"


def _format_elapsed(secs: float) -> str:
    """Format an elapsed duration in seconds as HH:MM:SS."""
    total = int(secs)
    h = total // 3600
    m = (total % 3600) // 60
    s = total % 60
    return f"{h:02d}:{m:02d}:{s:02d}"


def format_final_summary(
    root: str,
    streams: int,
    elapsed_secs: float,
    discovered: int,
    completed: int,
    failed: int,
    skipped: int,
    excluded: int,
    errors: int,
    gs: dict,
    is_root_crawl: bool,
    root_state: "WorkerState | None",
    scanner_done: bool,
    state_db_path: str | None = None,
    run_id: str | None = None,
    execution_id: str | None = None,
    resumed: bool = False,
    recovered_claims: dict[str, int] | None = None,
    reused_completed: int = 0,
    current_execution_stats: dict | None = None,
    status_counts: dict[str, dict[str, int]] | None = None,
    completion_passed: bool | None = None,
    run_state: str | None = None,
) -> str:
    """Build and return the comprehensive end-of-run summary string.

    All values are taken from the existing in-memory counters and the
    GlobalDsmcStats snapshot.  No counters are altered by this function.

    Parameters
    ----------
    root:
        Crawl entry point (normalised path).
    streams:
        Configured worker count (--streams).
    elapsed_secs:
        Wall-clock run duration in seconds.
    discovered / completed / failed / skipped / excluded / errors:
        Final values from Counters.snapshot().
    gs:
        Snapshot dict from GlobalDsmcStats.snapshot().
    is_root_crawl:
        True when the crawl entry point is /.
    root_state:
        WorkerState for the ROOT_FILES slot (slot 0), or None.
    scanner_done:
        Whether the directory scanner finished normally.
    """
    _SEP = "=" * 80

    lines: list[str] = [_SEP, "FINAL BACKUP SUMMARY", _SEP]

    # --- Run status ---
    if failed == 0 and errors == 0:
        outcome = "SUCCESS"
    else:
        parts = []
        if failed > 0:
            parts.append(f"{failed:,} directory job(s) failed")
        if errors > 0:
            parts.append(f"{errors:,} scan error(s)")
        outcome = "COMPLETED WITH FAILURES  (" + "; ".join(parts) + ")"

    scanner_state = "DONE" if scanner_done else "DID NOT FINISH"

    lines += [
        f"Outcome:  {outcome}",
        f"Root:     {root}",
        f"Workers:  {streams}",
        f"Elapsed:  {_format_elapsed(elapsed_secs)}",
        f"Scanner:  {scanner_state}",
    ]
    if state_db_path is not None:
        lines.append(f"State DB:  {state_db_path}")
    if run_id is not None:
        lines.append(f"Run UUID:  {run_id}")
    if execution_id is not None:
        lines.append(f"Execution: {execution_id}")
    if run_state is not None:
        lines.append(f"Run state: {run_state}")
    if state_db_path is not None:
        lines.append(f"Run mode:  {'RESUMED' if resumed else 'NEW'}")
    lines.append("")

    # --- Directory / folder accounting ---
    lines.append("Directories (crawler accounting):")
    _W = 38  # label column width
    lines += [
        f"  {'Discovered:':<{_W}} {discovered:>15,}",
        f"  {'Completed successfully:':<{_W}} {completed:>15,}",
        f"  {'Failed:':<{_W}} {failed:>15,}",
        f"  {'Excluded (config / log-dir):':<{_W}} {excluded:>15,}",
        f"  {'Skipped (nested mounts):':<{_W}} {skipped:>15,}",
        f"  {'Scanner errors:':<{_W}} {errors:>15,}",
    ]

    # Queue / in-progress at shutdown are expected to be zero on clean completion;
    # we don't have direct access here (they are runtime state), but their absence
    # from the reconciliation accounts for any non-zero residual.
    accounted = completed + failed
    if accounted == discovered:
        recon = f"OK  ({discovered:,} = {completed:,} + {failed:,})"
    else:
        unaccounted = discovered - accounted
        recon = (
            f"MISMATCH  {discovered:,} discovered, "
            f"{accounted:,} accounted, "
            f"{abs(unaccounted):,} unaccounted"
        )
    lines.append(f"  {'Reconciliation:':<{_W}} {recon}")
    lines.append("")

    if recovered_claims is not None or state_db_path is not None:
        lines.append("Durable state / recovery:")
        lines.append(f"  {'Reused completed work:':<{_W}} {reused_completed:>15,}")
        if recovered_claims is not None:
            lines.append(f"  {'Recovered stale scan claims:':<{_W}} {recovered_claims.get('scan', 0):>15,}")
            lines.append(f"  {'Recovered stale backup claims:':<{_W}} {recovered_claims.get('backup', 0):>15,}")
            lines.append(f"  {'Recovered ROOT_FILES manifest claims:':<{_W}} {recovered_claims.get('root_manifest', 0):>15,}")
            lines.append(f"  {'Recovered ROOT_FILES chunk claims:':<{_W}} {recovered_claims.get('root_chunks', 0):>15,}")
        if completion_passed is not None:
            lines.append(f"  {'Completion invariants passed:':<{_W}} {'yes' if completion_passed else 'no'}")
        lines.append("")

    # --- dsmc invocation accounting ---
    lines.append("dsmc invocation accounting:")
    lines += [
        f"  {'Total invocations completed:':<{_W}} {gs['dsmc_done']:>15,}",
        f"  {'Summaries parsed:':<{_W}} {gs['summaries_parsed']:>15,}",
        f"  {'Incomplete / missing summaries:':<{_W}} {gs['incomplete_summaries']:>15,}",
        f"  {'Active children at shutdown:':<{_W}} {gs['active_children']:>15,}",
        "",
    ]

    # --- Backed-up object / file totals ---
    lines.append("Objects reported by dsmc (from parsed summaries):")
    lines += [
        f"  {'Inspected:':<{_W}} {gs['objects_inspected']:>15,}",
        f"  {'Backed up:':<{_W}} {gs['objects_backed_up']:>15,}",
        f"  {'Updated:':<{_W}} {gs['objects_updated']:>15,}",
        f"  {'Failed:':<{_W}} {gs['objects_failed']:>15,}",
        f"  {'Retries:':<{_W}} {gs['retries']:>15,}",
    ]
    # Report additional object fields only when non-zero to keep output tidy.
    for label, key in (
        ("Rebound:", "objects_rebound"),
        ("Deleted:", "objects_deleted"),
        ("Expired:", "objects_expired"),
        ("Encrypted:", "objects_encrypted"),
        ("Grew:", "objects_grew"),
    ):
        val = gs.get(key, 0)
        if val:
            lines.append(f"  {label:<{_W}} {val:>15,}")
    lines.append("")

    # --- Data totals ---
    lines.append("Data reported by dsmc (from parsed summaries):")
    bi = gs["bytes_inspected"]
    bt = gs["bytes_transferred"]
    lines += [
        f"  Processed:  {bi:>20,} bytes  ({format_bytes(bi)})",
        f"  Sent:       {bt:>20,} bytes  ({format_bytes(bt)})",
    ]
    total_elapsed_dsmc = gs["total_elapsed_secs"]
    if total_elapsed_dsmc > 0 and bt > 0:
        rate = bt / total_elapsed_dsmc
        lines.append(f"  Effective rate:  {format_rate(rate)}")
    lines.append("")

    if current_execution_stats is not None:
        lines.append("Current execution attempt totals:")
        lines += [
            f"  {'Completed invocations this execution:':<{_W}} {int(current_execution_stats['dsmc_done']):>15,}",
            f"  {'Summaries parsed this execution:':<{_W}} {int(current_execution_stats['summaries_parsed']):>15,}",
            f"  {'Bytes processed this execution:':<{_W}} {int(current_execution_stats['bytes_inspected']):>15,}",
            f"  {'Bytes transferred this execution:':<{_W}} {int(current_execution_stats['bytes_transferred']):>15,}",
        ]
        lines.append("")

    if status_counts is not None:
        lines.append("Persisted scheduler statuses:")
        scan_counts = status_counts.get('scan', {})
        backup_counts = status_counts.get('backup', {})
        root_counts = status_counts.get('root_chunks', {})
        lines += [
            f"  {'Scan pending/scanning/scanned:':<{_W}} {scan_counts.get('pending',0)}/{scan_counts.get('scanning',0)}/{scan_counts.get('scanned',0)}",
            f"  {'Scan excluded/skipped/errors:':<{_W}} {scan_counts.get('excluded',0)}/{scan_counts.get('skipped_mount',0)}/{scan_counts.get('scan_failed',0)}",
            f"  {'Backup pending/running/succeeded:':<{_W}} {backup_counts.get('pending',0)}/{backup_counts.get('running',0)}/{backup_counts.get('succeeded',0)}",
            f"  {'Backup failed/timed_out/interrupted:':<{_W}} {backup_counts.get('failed',0)}/{backup_counts.get('timed_out',0)}/{backup_counts.get('interrupted',0)}",
        ]
        if root_counts:
            lines.append(f"  {'ROOT_FILES pending/running/succeeded:':<{_W}} {root_counts.get('pending',0)}/{root_counts.get('running',0)}/{root_counts.get('succeeded',0)}")
            lines.append(f"  {'ROOT_FILES failed/timed_out/interrupted:':<{_W}} {root_counts.get('failed',0)}/{root_counts.get('timed_out',0)}/{root_counts.get('interrupted',0)}")
        lines.append("")

    # --- ROOT_FILES accounting (only when crawl root is /) ---
    if is_root_crawl:
        lines.append("ROOT_FILES job (non-directory entries directly under /):")
        if root_state is not None:
            rf_status = root_state.status
            rf_ok = root_state.dirs_completed
            rf_fail = root_state.dirs_failed
            rf_timeout = root_state.dirs_timed_out
            rf_total = root_state.batch_total
            lines += [
                f"  {'Status:':<{_W}} {rf_status}",
                f"  {'Chunks completed:':<{_W}} {rf_ok:>15,}",
                f"  {'Chunks failed:':<{_W}} {rf_fail:>15,}",
                f"  {'Chunks timed out:':<{_W}} {rf_timeout:>15,}",
                f"  {'Total chunks:':<{_W}} {rf_total:>15,}",
            ]
        else:
            lines.append("  (no ROOT_FILES state recorded)")
        lines.append("")

    lines.append(_SEP)
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Global aggregated dsmc statistics (thread-safe)
# ---------------------------------------------------------------------------

class GlobalDsmcStats:
    """Accumulates dsmc statistics across all invocations.  Thread-safe.

    Accounting model
    ----------------
    * ``dsmc_done``          — total completed dsmc invocations (including failed
                               processes); incremented exactly once per invocation.
    * ``summaries_parsed``   — invocations whose dsmc summary was successfully
                               parsed (``has_data()`` is True).
    * ``incomplete_summaries``— invocations where summary data was absent or
                               incomplete (``has_data()`` is False); the object/byte
                               counters below therefore may undercount the true total.
    * Object counters (``objects_backed_up``, etc.) and byte counters
      (``bytes_inspected``, ``bytes_transferred``) accumulate only from
      invocations with parsed summaries.
    * ``bytes_inspected`` corresponds to "Total number of bytes inspected"
      (data processed/scanned by dsmc).
    * ``bytes_transferred`` corresponds to "Total number of bytes transferred"
      (data actually sent to the SP server).
    * All byte values are stored as exact integer bytes (no floating-point
      accumulation).
    * Dry-run invocations never call ``add_invocation``; totals therefore
      reflect only real dsmc runs.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        # Invocation accounting
        self.dsmc_done: int = 0
        self.summaries_parsed: int = 0
        self.incomplete_summaries: int = 0
        # Object counters
        self.objects_inspected: int = 0
        self.objects_backed_up: int = 0
        self.objects_updated: int = 0
        self.objects_rebound: int = 0
        self.objects_deleted: int = 0
        self.objects_expired: int = 0
        self.objects_failed: int = 0
        self.objects_encrypted: int = 0
        self.objects_grew: int = 0
        self.retries: int = 0
        # Byte counters (exact integers)
        self.bytes_inspected: int = 0
        self.bytes_transferred: int = 0
        # Sum of all per-invocation elapsed times; used for effective throughput.
        self.total_elapsed_secs: float = 0.0
        self.active_children: int = 0

    def add_invocation(self, stats: DsmcInvocationStats) -> None:
        """Merge one completed invocation's parsed stats into the global aggregate.

        Must be called exactly once per invocation after the process exits.
        Dry-run invocations must never call this method.

        If ``stats.has_data()`` is False (no summary lines were parsed) the
        ``incomplete_summaries`` counter is incremented and object/byte totals
        are left unchanged for that invocation.
        """
        with self._lock:
            self.dsmc_done += 1
            if stats.has_data():
                self.summaries_parsed += 1
                self.objects_inspected += stats.objects_inspected
                self.objects_backed_up += stats.objects_backed_up
                self.objects_updated += stats.objects_updated
                self.objects_rebound += stats.objects_rebound
                self.objects_deleted += stats.objects_deleted
                self.objects_expired += stats.objects_expired
                self.objects_failed += stats.objects_failed
                self.objects_encrypted += stats.objects_encrypted
                self.objects_grew += stats.objects_grew
                self.retries += stats.retries
                self.bytes_inspected += stats.bytes_inspected
                self.bytes_transferred += stats.bytes_transferred
                self.total_elapsed_secs += stats.elapsed_secs
            else:
                self.incomplete_summaries += 1

    def child_started(self) -> None:
        with self._lock:
            self.active_children += 1

    def child_finished(self) -> None:
        with self._lock:
            self.active_children = max(0, self.active_children - 1)

    def snapshot(self) -> dict:
        """Return a consistent point-in-time copy of all counters (no partial reads)."""
        with self._lock:
            return {
                "dsmc_done": self.dsmc_done,
                "summaries_parsed": self.summaries_parsed,
                "incomplete_summaries": self.incomplete_summaries,
                "objects_inspected": self.objects_inspected,
                "objects_backed_up": self.objects_backed_up,
                "objects_updated": self.objects_updated,
                "objects_rebound": self.objects_rebound,
                "objects_deleted": self.objects_deleted,
                "objects_expired": self.objects_expired,
                "objects_failed": self.objects_failed,
                "objects_encrypted": self.objects_encrypted,
                "objects_grew": self.objects_grew,
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
    # Hard upper bound on the displayed path column width.  Prevents very
    # wide terminals from producing an unreadably long path field and keeps
    # every worker row within one visual line on most displays.
    _MAX_PATH_DISPLAY = 78

    def __init__(
        self,
        counters: Counters,
        worker_states: WorkerStates,
        global_stats: GlobalDsmcStats,
        work_queue,
        producer_done: threading.Event,
        stop_event: threading.Event,
        refresh_seconds: float,
        state_snapshot_provider=None,
    ) -> None:
        self.counters = counters
        self.worker_states = worker_states
        self.global_stats = global_stats
        self.work_queue = work_queue
        self.producer_done = producer_done
        self.stop_event = stop_event
        self.refresh_seconds = refresh_seconds
        self.state_snapshot_provider = state_snapshot_provider
        self._rendered_lines = 0

    @staticmethod
    def truncate_path(path: str, width: int, marker: str = ".....") -> str:
        """Return *path* truncated to *width* printable characters.

        When truncation is needed, preserve both a leading and trailing segment
        and place an ASCII marker between them (default ``"....."``), e.g.
        ``"/root/.openclaw...../parse5/lib/extensions"``.

        The split gives the trailing segment a slight bias so the basename and
        nearest parent directories remain visible for operators.

        Edge cases:
        - Returns ``""`` when *width* is < 1.
        - Returns the marker clipped to *width* when *width* ≤ len(marker).
        - Returns the original value unchanged when it already fits.
        """
        if width < 1:
            return ""
        if len(path) <= width:
            return path
        mlen = len(marker)
        if width <= mlen:
            return marker[:width]
        keep = width - mlen
        # Give the trailing segment one extra character when keep is odd so
        # basename/end context is slightly favored (e.g., keep=5 -> 2+3).
        trailing_len = (keep + 1) // 2
        leading_len = keep - trailing_len
        return path[:leading_len] + marker + path[-trailing_len:]

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
                # dsmc_done = total completed invocations; summaries_parsed/dsmc_done
                # shows how many produced a parseable summary block.
                f"Backup totals: done={gs['dsmc_done']:,}"
                f"  parsed={gs['summaries_parsed']:,}/{gs['dsmc_done']:,}"
                f"  incomplete={gs['incomplete_summaries']:,}"
                f"  children={gs['active_children']}"
            ),
            (
                f"Objects: inspected={gs['objects_inspected']:,}"
                f"  backed_up={gs['objects_backed_up']:,}"
                f"  updated={gs['objects_updated']:,}"
                f"  failed={gs['objects_failed']:,}"
                f"  retries={gs['retries']:,}"
            ),
            (
                f"Data: processed={format_bytes(gs['bytes_inspected'])}"
                f"  sent={format_bytes(gs['bytes_transferred'])}"
                + rate_str
            ),
        ]
        if self.state_snapshot_provider is not None:
            ps = self.state_snapshot_provider()
            run_short = str(ps['run_id'])[:8] if ps.get('run_id') else '-'
            lines.append(
                f"State: {ps.get('mode','?')} run={run_short} reused={ps.get('reused_completed',0)} "
                f"recovered_scan={ps.get('recovered_scan_claims',0)} recovered_backup={ps.get('recovered_backup_claims',0)}"
            )
            scan_counts = ps.get('scan_counts', {})
            backup_counts = ps.get('backup_counts', {})
            lines.append(
                f"Persisted: scan p/s/d/e/m={scan_counts.get('pending',0)}/{scan_counts.get('scanning',0)}/{scan_counts.get('scanned',0)}/{scan_counts.get('excluded',0)}/{scan_counts.get('skipped_mount',0)} "
                f"backup p/r/ok/fl/to={backup_counts.get('pending',0)}/{backup_counts.get('running',0)}/{backup_counts.get('succeeded',0)}/{backup_counts.get('failed',0)}/{backup_counts.get('timed_out',0)}"
            )
        lines.append(sep)

        # Per-worker rows (worker 0 = ROOT_FILES special job, shown first when present)
        bar_width = 10
        # Layout (fixed-width prefix before path):
        # label + sp = 11 (ROOT_FILES=10 chars; regular workers such as W01 are padded)
        # [bar] + sp = bar_width+3
        # pos + sp = 7+1 = 8
        # status + sp = STATUS_WIDTH+1
        # b#NNN + sp = 6
        # pid=NNNNN + sp = 10
        # rt:NNN.Ns + sp = 11  ("rt:" prefix = 3, pad width = 7, trailing space = 1)
        # idle:NNN.Ns + sp = 13  ("idle:" prefix = 5, pad width = 7, trailing space = 1)
        # ok:NN to:NN fl:NN + sp = 18
        # rc=NNN + sp = 7
        # path (remaining)
        static_width = 11 + (bar_width + 3) + 8 + (self._STATUS_WIDTH + 1) + 6 + 10 + 11 + 13 + 18 + 7
        # Cap path width to available terminal space after static columns (no forced minimum) and
        # to _MAX_PATH_DISPLAY so very wide terminals do not produce unwieldy lines.
        path_width = min(self._MAX_PATH_DISPLAY, max(0, terminal_width - static_width))

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
            path = self.truncate_path(state.current_directory, path_width)
            # Final safety clip: ensure the rendered row never exceeds the
            # terminal width regardless of any width-calculation discrepancy.
            row = prefix + path
            if len(row) > terminal_width:
                row = row[:terminal_width]
            lines.append(row)

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

    def write_raw(self, text: str) -> None:
        """Append *text* verbatim to the log file without any timestamp prefix.

        Used for pre-formatted multi-line blocks (e.g. the final summary) where
        per-line timestamps would break the human-readable layout.  Does not echo
        to stdout; callers are responsible for printing to stdout if desired.
        """
        with self.lock:
            with self.path.open("a", encoding="utf-8", errors="backslashreplace") as fh:
                fh.write(text)
                if not text.endswith("\n"):
                    fh.write("\n")


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
# Durable SQLite state
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class RunContext:
    run_id: str
    execution_id: str
    controller_id: str
    state_db_path: str
    resumed: bool
    recovered_scan_claims: int = 0
    recovered_backup_claims: int = 0
    recovered_root_chunk_claims: int = 0
    recovered_root_manifest_claims: int = 0
    reused_completed: int = 0


def _utc_now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _utc_now_from_epoch(epoch: float) -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(epoch))


def _parse_utc_timestamp(value: str) -> float:
    return float(calendar.timegm(time.strptime(value, "%Y-%m-%dT%H:%M:%SZ")))


def encode_path_for_db(path: str) -> bytes:
    return os.fsencode(path)


def decode_path_from_db(value: bytes | memoryview | bytearray) -> str:
    return os.fsdecode(bytes(value))


def _json_dumps(data: object) -> str:
    return json.dumps(data, sort_keys=True, separators=(",", ":"))


def _path_display(path: str) -> str:
    return path.encode("utf-8", errors="backslashreplace").decode("utf-8")


def build_coverage_config(
    root: str,
    root_device: int,
    excluded_paths: frozenset[str],
    args: argparse.Namespace,
) -> dict:
    return {
        "root": root,
        "root_device": root_device,
        "excluded_paths": sorted(excluded_paths),
        "dsmc": args.dsmc,
        "dsmc_options": list(args.dsmc_option),
        "resourceutilization": args.resourceutilization,
        "mount_policy_version": SQLITE_POLICY_VERSION,
        "symlink_policy_version": SQLITE_POLICY_VERSION,
        "root_files_mode": root == os.sep,
    }


def build_operational_config(args: argparse.Namespace) -> dict:
    return {
        "streams": args.streams,
        "batch_size": args.batch_size,
        "queue_size": args.queue_size,
        "progress_seconds": args.progress_seconds,
        "dashboard_refresh_seconds": args.dashboard_refresh_seconds,
        "shutdown_wait_seconds": args.shutdown_wait_seconds,
        "dsmc_timeout": args.dsmc_timeout,
        "dsmc_idle_timeout": args.dsmc_idle_timeout,
        "log_dir": args.log_dir,
        "state_db": args.state_db,
    }


def build_config_fingerprint(config: dict) -> str:
    return hashlib.sha256(_json_dumps(config).encode("utf-8")).hexdigest()


def _stats_to_dict(stats: DsmcInvocationStats) -> dict[str, int | float]:
    return {
        "objects_inspected": stats.objects_inspected,
        "objects_backed_up": stats.objects_backed_up,
        "objects_updated": stats.objects_updated,
        "objects_rebound": stats.objects_rebound,
        "objects_deleted": stats.objects_deleted,
        "objects_expired": stats.objects_expired,
        "objects_failed": stats.objects_failed,
        "objects_encrypted": stats.objects_encrypted,
        "objects_grew": stats.objects_grew,
        "retries": stats.retries,
        "bytes_inspected": stats.bytes_inspected,
        "bytes_transferred": stats.bytes_transferred,
        "transfer_time_secs": stats.transfer_time_secs,
        "network_rate_bps": stats.network_rate_bps,
        "aggregate_rate_bps": stats.aggregate_rate_bps,
        "objects_compressed_pct": stats.objects_compressed_pct,
        "data_reduction_pct": stats.data_reduction_pct,
        "elapsed_secs": stats.elapsed_secs,
    }


class PersistentQueueMetrics:
    def __init__(self, state_db: "PersistentStateDB", run_id: str, maxsize: int) -> None:
        self._state_db = state_db
        self._run_id = run_id
        self.maxsize = maxsize

    def qsize(self) -> int:
        return self._state_db.pending_backup_count(self._run_id)


class PersistentStateDB:
    def __init__(self, path: str) -> None:
        self.path = os.path.abspath(path)
        self._local = threading.local()
        self._schema_ready = False

    def _connect(self) -> sqlite3.Connection:
        conn = getattr(self._local, "connection", None)
        if conn is None:
            # Each thread stores its own SQLite connection in thread-local state,
            # so disabling SQLite's same-thread check here is safe and avoids
            # cross-thread reuse of a single connection object. The 5-second
            # timeout matches the busy_timeout pragma below and gives writers a
            # bounded window to wait for WAL contention before surfacing an error.
            conn = sqlite3.connect(self.path, timeout=5, check_same_thread=False)
            conn.row_factory = sqlite3.Row
            self._apply_pragmas(conn)
            self._local.connection = conn
        return conn

    @staticmethod
    def _apply_pragmas(conn: sqlite3.Connection) -> None:
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=5000")
        conn.execute("PRAGMA synchronous=FULL")

    def close(self) -> None:
        conn = getattr(self._local, "connection", None)
        if conn is not None:
            conn.close()
            self._local.connection = None

    def ensure_parent_dir(self) -> None:
        parent = Path(self.path).parent
        parent.mkdir(parents=True, exist_ok=True)

    def initialize(self) -> None:
        self.ensure_parent_dir()
        conn = self._connect()
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS schema_meta (
                schema_version INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS runs (
                id TEXT PRIMARY KEY,
                schema_version INTEGER NOT NULL,
                created_at TEXT NOT NULL,
                started_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                completed_at TEXT,
                state TEXT NOT NULL,
                root_path BLOB NOT NULL,
                root_display TEXT NOT NULL,
                root_device INTEGER NOT NULL,
                coverage_config_json TEXT NOT NULL,
                coverage_fingerprint TEXT NOT NULL,
                operational_config_json TEXT NOT NULL,
                controller_id TEXT,
                controller_host TEXT,
                controller_pid INTEGER,
                controller_started_at TEXT,
                controller_heartbeat_at TEXT,
                controller_lease_expires_at TEXT,
                last_execution_id TEXT,
                execution_count INTEGER NOT NULL DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS directories (
                run_id TEXT NOT NULL,
                path_bytes BLOB NOT NULL,
                path_display TEXT NOT NULL,
                parent_path_bytes BLOB,
                device_id INTEGER,
                discovered_at TEXT NOT NULL,
                scan_status TEXT NOT NULL,
                backup_status TEXT NOT NULL,
                scan_attempts INTEGER NOT NULL DEFAULT 0,
                backup_attempts INTEGER NOT NULL DEFAULT 0,
                scan_owner TEXT,
                scan_claimed_at TEXT,
                scan_heartbeat_at TEXT,
                backup_owner TEXT,
                backup_worker_id INTEGER,
                backup_pid INTEGER,
                backup_claimed_at TEXT,
                backup_heartbeat_at TEXT,
                last_error TEXT,
                last_return_code INTEGER,
                last_started_at TEXT,
                last_finished_at TEXT,
                PRIMARY KEY (run_id, path_bytes),
                FOREIGN KEY (run_id) REFERENCES runs(id) ON DELETE CASCADE
            );
            CREATE INDEX IF NOT EXISTS idx_directories_scan_status
                ON directories(run_id, scan_status, discovered_at, path_display);
            CREATE INDEX IF NOT EXISTS idx_directories_backup_status
                ON directories(run_id, backup_status, discovered_at, path_display);
            CREATE TABLE IF NOT EXISTS attempts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT NOT NULL,
                execution_id TEXT NOT NULL,
                item_kind TEXT NOT NULL,
                path_bytes BLOB,
                root_chunk_id INTEGER,
                worker_slot INTEGER NOT NULL,
                worker_label TEXT NOT NULL,
                attempt_number INTEGER NOT NULL,
                started_at TEXT NOT NULL,
                ended_at TEXT,
                outcome TEXT NOT NULL,
                return_code INTEGER,
                summary_complete INTEGER NOT NULL DEFAULT 0,
                objects_inspected INTEGER NOT NULL DEFAULT 0,
                objects_backed_up INTEGER NOT NULL DEFAULT 0,
                objects_updated INTEGER NOT NULL DEFAULT 0,
                objects_rebound INTEGER NOT NULL DEFAULT 0,
                objects_deleted INTEGER NOT NULL DEFAULT 0,
                objects_expired INTEGER NOT NULL DEFAULT 0,
                objects_failed INTEGER NOT NULL DEFAULT 0,
                objects_encrypted INTEGER NOT NULL DEFAULT 0,
                objects_grew INTEGER NOT NULL DEFAULT 0,
                retries INTEGER NOT NULL DEFAULT 0,
                bytes_inspected INTEGER NOT NULL DEFAULT 0,
                bytes_transferred INTEGER NOT NULL DEFAULT 0,
                transfer_time_secs REAL NOT NULL DEFAULT 0,
                network_rate_bps REAL NOT NULL DEFAULT 0,
                aggregate_rate_bps REAL NOT NULL DEFAULT 0,
                objects_compressed_pct REAL NOT NULL DEFAULT 0,
                data_reduction_pct REAL NOT NULL DEFAULT 0,
                elapsed_secs REAL NOT NULL DEFAULT 0,
                error_text TEXT,
                FOREIGN KEY (run_id) REFERENCES runs(id) ON DELETE CASCADE
            );
            CREATE INDEX IF NOT EXISTS idx_attempts_run_kind
                ON attempts(run_id, item_kind, execution_id);
            CREATE TABLE IF NOT EXISTS root_files_state (
                run_id TEXT PRIMARY KEY,
                manifest_status TEXT NOT NULL,
                total_files INTEGER NOT NULL DEFAULT 0,
                total_chunks INTEGER NOT NULL DEFAULT 0,
                last_manifest_at TEXT,
                last_error TEXT,
                FOREIGN KEY (run_id) REFERENCES runs(id) ON DELETE CASCADE
            );
            CREATE TABLE IF NOT EXISTS root_files_entries (
                run_id TEXT NOT NULL,
                path_bytes BLOB NOT NULL,
                path_display TEXT NOT NULL,
                discovered_at TEXT NOT NULL,
                PRIMARY KEY (run_id, path_bytes),
                FOREIGN KEY (run_id) REFERENCES runs(id) ON DELETE CASCADE
            );
            CREATE TABLE IF NOT EXISTS root_file_chunks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT NOT NULL,
                chunk_index INTEGER NOT NULL,
                status TEXT NOT NULL,
                total_files INTEGER NOT NULL,
                claimed_by TEXT,
                claimed_at TEXT,
                heartbeat_at TEXT,
                worker_pid INTEGER,
                last_return_code INTEGER,
                last_error TEXT,
                last_started_at TEXT,
                last_finished_at TEXT,
                UNIQUE (run_id, chunk_index),
                FOREIGN KEY (run_id) REFERENCES runs(id) ON DELETE CASCADE
            );
            CREATE INDEX IF NOT EXISTS idx_root_chunks_status
                ON root_file_chunks(run_id, status, chunk_index);
            CREATE TABLE IF NOT EXISTS root_file_chunk_members (
                chunk_id INTEGER NOT NULL,
                seq INTEGER NOT NULL,
                path_bytes BLOB NOT NULL,
                path_display TEXT NOT NULL,
                PRIMARY KEY (chunk_id, seq),
                FOREIGN KEY (chunk_id) REFERENCES root_file_chunks(id) ON DELETE CASCADE
            );
            """
        )
        row = conn.execute("SELECT schema_version FROM schema_meta").fetchone()
        if row is None:
            conn.execute("INSERT INTO schema_meta(schema_version) VALUES (?)", (SQLITE_SCHEMA_VERSION,))
        elif int(row[0]) != SQLITE_SCHEMA_VERSION:
            raise RuntimeError(
                f"Unsupported state DB schema version {row[0]} at {self.path}. "
                f"Expected {SQLITE_SCHEMA_VERSION}. Automatic schema migration is not supported. To preserve the old DB, copy it aside manually before running --new-run, which archives any existing DB to a timestamped .bak file and creates a fresh state DB."
            )
        conn.commit()
        if os.path.exists(self.path):
            try:
                os.chmod(self.path, 0o600)
            except OSError:
                pass
        self._schema_ready = True

    def quick_check(self) -> None:
        result = self._connect().execute("PRAGMA quick_check").fetchone()
        if result is None or str(result[0]).lower() != "ok":
            raise RuntimeError(
                f"SQLite integrity check failed for {self.path}: {result[0] if result else 'unknown'}"
            )

    def load_run_row(self) -> sqlite3.Row | None:
        return self._connect().execute("SELECT * FROM runs ORDER BY created_at DESC LIMIT 1").fetchone()

    def archive_existing(self) -> str:
        stamp = time.strftime("%Y%m%d-%H%M%S")
        archived = f"{self.path}.{stamp}.bak"
        for suffix in ("", "-wal", "-shm"):
            src = self.path + suffix
            if os.path.exists(src):
                os.replace(src, archived + suffix)
        return archived

    def create_new_run(
        self,
        root: str,
        root_device: int,
        coverage_config: dict,
        operational_config: dict,
        is_root_crawl: bool,
    ) -> RunContext:
        self.initialize()
        self.quick_check()
        now = _utc_now()
        run_id = str(uuid.uuid4())
        execution_id = str(uuid.uuid4())
        controller_id = str(uuid.uuid4())
        fingerprint = build_config_fingerprint(coverage_config)
        lease_expires = _utc_now_from_epoch(time.time() + CONTROLLER_LEASE_SECS)
        conn = self._connect()
        # One archived/new-run DB contains at most one live run. Deleting the
        # parent run row is enough because the child tables use ON DELETE CASCADE.
        conn.execute("DELETE FROM runs")
        conn.execute(
            """
            INSERT INTO runs(
                id, schema_version, created_at, started_at, updated_at, state,
                root_path, root_display, root_device, coverage_config_json,
                coverage_fingerprint, operational_config_json, controller_id,
                controller_host, controller_pid, controller_started_at,
                controller_heartbeat_at, controller_lease_expires_at,
                last_execution_id, execution_count
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                run_id, SQLITE_SCHEMA_VERSION, now, now, now, "running",
                encode_path_for_db(root), _path_display(root), root_device,
                _json_dumps(coverage_config), fingerprint, _json_dumps(operational_config),
                controller_id, socket.gethostname(), os.getpid(), now, now, lease_expires,
                execution_id, 1,
            ),
        )
        conn.execute(
            """
            INSERT INTO directories(
                run_id, path_bytes, path_display, parent_path_bytes, device_id,
                discovered_at, scan_status, backup_status
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                run_id, encode_path_for_db(root), _path_display(root), None, root_device, now,
                "pending", "skipped_root" if is_root_crawl else "pending",
            ),
        )
        if is_root_crawl:
            conn.execute(
                "INSERT INTO root_files_state(run_id, manifest_status) VALUES (?, ?)",
                (run_id, "pending"),
            )
        conn.commit()
        return RunContext(
            run_id=run_id,
            execution_id=execution_id,
            controller_id=controller_id,
            state_db_path=self.path,
            resumed=False,
        )

    def resume_run(
        self,
        root: str,
        root_device: int,
        coverage_config: dict,
    ) -> RunContext:
        self.initialize()
        self.quick_check()
        row = self.load_run_row()
        if row is None:
            raise RuntimeError(f"No saved run found in {self.path}; start with --new-run.")
        stored_root = decode_path_from_db(row["root_path"])
        stored_coverage = json.loads(row["coverage_config_json"])
        stored_fp = str(row["coverage_fingerprint"])
        current_fp = build_config_fingerprint(coverage_config)
        if stored_root != root:
            raise RuntimeError(
                f"State DB root mismatch: saved run is for {stored_root!r}, requested root is {root!r}."
            )
        if int(row["root_device"]) != int(root_device):
            raise RuntimeError(
                f"State DB root device mismatch: saved={row['root_device']} current={root_device}."
            )
        if stored_fp != current_fp:
            raise RuntimeError(
                "Saved crawl configuration is incompatible with the requested run. "
                f"Saved coverage config: {_json_dumps(stored_coverage)}"
            )
        now_epoch = time.time()
        lease_expires = row["controller_lease_expires_at"]
        if row["controller_id"] and lease_expires and _parse_utc_timestamp(lease_expires) > now_epoch:
            raise RuntimeError(
                f"State DB {self.path} is currently owned by controller {row['controller_id']} until {lease_expires}."
            )
        execution_id = str(uuid.uuid4())
        controller_id = str(uuid.uuid4())
        now = _utc_now()
        conn = self._connect()
        conn.execute("BEGIN IMMEDIATE")
        recovered_scan = conn.execute(
            "UPDATE directories SET scan_status='pending', scan_owner=NULL, scan_claimed_at=NULL, scan_heartbeat_at=NULL "
            "WHERE run_id=? AND scan_status='scanning'",
            (row["id"],),
        ).rowcount
        recovered_backup = conn.execute(
            "UPDATE directories SET backup_status='pending', backup_owner=NULL, backup_worker_id=NULL, backup_pid=NULL, "
            "backup_claimed_at=NULL, backup_heartbeat_at=NULL, last_error=COALESCE(last_error, 'stale running claim recovered') "
            "WHERE run_id=? AND backup_status IN ('running','interrupted')",
            (row["id"],),
        ).rowcount
        recovered_manifest = conn.execute(
            "UPDATE root_files_state SET manifest_status='pending' WHERE run_id=? AND manifest_status='scanning'",
            (row["id"],),
        ).rowcount
        recovered_chunks = conn.execute(
            "UPDATE root_file_chunks SET status='pending', claimed_by=NULL, claimed_at=NULL, heartbeat_at=NULL, worker_pid=NULL, "
            "last_error=COALESCE(last_error, 'stale running chunk recovered') "
            "WHERE run_id=? AND status IN ('running','interrupted')",
            (row["id"],),
        ).rowcount
        conn.execute(
            "UPDATE attempts SET outcome='interrupted', ended_at=?, error_text=COALESCE(error_text, 'Recovered stale in-flight attempt') "
            "WHERE run_id=? AND outcome='running'",
            (now, row["id"]),
        )
        lease_expires_new = _utc_now_from_epoch(time.time() + CONTROLLER_LEASE_SECS)
        conn.execute(
            """
            UPDATE runs
               SET state='running',
                   updated_at=?,
                   controller_id=?,
                   controller_host=?,
                   controller_pid=?,
                   controller_started_at=?,
                   controller_heartbeat_at=?,
                   controller_lease_expires_at=?,
                   last_execution_id=?,
                   execution_count=execution_count+1
             WHERE id=?
            """,
            (
                now, controller_id, socket.gethostname(), os.getpid(), now, now,
                lease_expires_new, execution_id, row["id"],
            ),
        )
        conn.commit()
        reused_completed = self.directory_counts(row["id"])["completed"]
        return RunContext(
            run_id=row["id"],
            execution_id=execution_id,
            controller_id=controller_id,
            state_db_path=self.path,
            resumed=True,
            recovered_scan_claims=recovered_scan,
            recovered_backup_claims=recovered_backup,
            recovered_root_chunk_claims=recovered_chunks,
            recovered_root_manifest_claims=recovered_manifest,
            reused_completed=reused_completed,
        )

    def heartbeat_controller(self, run_id: str, controller_id: str) -> None:
        now = _utc_now()
        lease_expires = _utc_now_from_epoch(time.time() + CONTROLLER_LEASE_SECS)
        self._connect().execute(
            "UPDATE runs SET updated_at=?, controller_heartbeat_at=?, controller_lease_expires_at=? WHERE id=? AND controller_id=?",
            (now, now, lease_expires, run_id, controller_id),
        )
        self._connect().commit()

    def clear_controller(self, run_id: str, controller_id: str, final_state: str) -> None:
        now = _utc_now()
        conn = self._connect()
        conn.execute(
            "UPDATE runs SET updated_at=?, completed_at=?, state=?, controller_id=NULL, controller_host=NULL, controller_pid=NULL, controller_started_at=NULL, controller_heartbeat_at=NULL, controller_lease_expires_at=NULL WHERE id=? AND controller_id=?",
            (now, now if final_state.startswith('completed') else None, final_state, run_id, controller_id),
        )
        conn.commit()

    def mark_run_state(self, run_id: str, state: str) -> None:
        self._connect().execute("UPDATE runs SET state=?, updated_at=? WHERE id=?", (state, _utc_now(), run_id))
        self._connect().commit()

    def status_report(self) -> str:
        if not os.path.exists(self.path):
            return f"No saved run found in {self.path}"
        self.initialize()
        self.quick_check()
        row = self.load_run_row()
        if row is None:
            return f"No saved run found in {self.path}"
        counts = self.runtime_status_counts(row["id"])
        gs = self.load_global_stats(row["id"])
        lines = [
            f"State DB: {self.path}",
            f"Run UUID: {row['id']}",
            f"Root: {decode_path_from_db(row['root_path'])}",
            f"State: {row['state']}",
            f"Started: {row['started_at']}",
            f"Updated: {row['updated_at']}",
            f"Controller: {row['controller_id'] or '-'}",
            f"Controller lease expires: {row['controller_lease_expires_at'] or '-'}",
            "",
            "Scan statuses:",
        ]
        for key in ("pending", "scanning", "scanned", "scan_failed", "excluded", "skipped_mount"):
            lines.append(f"  {key}: {counts['scan'].get(key, 0)}")
        lines.append("")
        lines.append("Backup statuses:")
        for key in ("pending", "running", "succeeded", "failed", "timed_out", "interrupted", "skipped_root", "not_eligible"):
            lines.append(f"  {key}: {counts['backup'].get(key, 0)}")
        lines.extend([
            "",
            f"Attempts completed: {gs['dsmc_done']}",
            f"Summaries parsed: {gs['summaries_parsed']}/{gs['dsmc_done']}",
            f"Processed bytes: {gs['bytes_inspected']} ({format_bytes(gs['bytes_inspected'])})",
            f"Transferred bytes: {gs['bytes_transferred']} ({format_bytes(gs['bytes_transferred'])})",
        ])
        return "\n".join(lines)

    def insert_directory_if_absent(
        self,
        run_id: str,
        path: str,
        parent_path: str | None,
        device_id: int | None,
        scan_status: str,
        backup_status: str,
    ) -> bool:
        conn = self._connect()
        cur = conn.execute(
            """
            INSERT OR IGNORE INTO directories(
                run_id, path_bytes, path_display, parent_path_bytes, device_id,
                discovered_at, scan_status, backup_status
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                run_id, encode_path_for_db(path), _path_display(path),
                encode_path_for_db(parent_path) if parent_path is not None else None,
                device_id, _utc_now(), scan_status, backup_status,
            ),
        )
        conn.commit()
        return cur.rowcount > 0

    def claim_next_scan(self, run_id: str, controller_id: str) -> str | None:
        conn = self._connect()
        conn.execute("BEGIN IMMEDIATE")
        row = conn.execute(
            "SELECT path_bytes FROM directories WHERE run_id=? AND scan_status='pending' ORDER BY discovered_at, path_display LIMIT 1",
            (run_id,),
        ).fetchone()
        if row is None:
            conn.commit()
            return None
        now = _utc_now()
        conn.execute(
            "UPDATE directories SET scan_status='scanning', scan_owner=?, scan_claimed_at=?, scan_heartbeat_at=?, scan_attempts=scan_attempts+1 WHERE run_id=? AND path_bytes=?",
            (controller_id, now, now, run_id, row['path_bytes']),
        )
        conn.commit()
        return decode_path_from_db(row['path_bytes'])

    def release_scan_claim(self, run_id: str, path: str) -> None:
        conn = self._connect()
        conn.execute(
            "UPDATE directories SET scan_status='pending', scan_owner=NULL, scan_claimed_at=NULL, scan_heartbeat_at=NULL, last_error='scan interrupted before commit' WHERE run_id=? AND path_bytes=? AND scan_status='scanning'",
            (run_id, encode_path_for_db(path)),
        )
        conn.commit()

    def finish_scan(
        self,
        run_id: str,
        path: str,
        eligible_children: list[tuple[str, int | None]],
        excluded_children: list[str],
        skipped_mount_children: list[str],
        scan_failed_children: list[tuple[str, str]],
        error_text: str | None = None,
    ) -> dict[str, int]:
        conn = self._connect()
        counts = {"eligible": 0, "excluded": 0, "skipped": 0, "errors": 0}
        conn.execute("BEGIN IMMEDIATE")
        now = _utc_now()
        for child, device_id in eligible_children:
            cur = conn.execute(
                """
                INSERT OR IGNORE INTO directories(
                    run_id, path_bytes, path_display, parent_path_bytes, device_id,
                    discovered_at, scan_status, backup_status
                ) VALUES (?, ?, ?, ?, ?, ?, 'pending', 'pending')
                """,
                (
                    run_id, encode_path_for_db(child), _path_display(child), encode_path_for_db(path),
                    device_id, now,
                ),
            )
            counts["eligible"] += cur.rowcount
        for child in excluded_children:
            cur = conn.execute(
                """
                INSERT OR IGNORE INTO directories(
                    run_id, path_bytes, path_display, parent_path_bytes, device_id,
                    discovered_at, scan_status, backup_status
                ) VALUES (?, ?, ?, ?, ?, ?, 'excluded', 'not_eligible')
                """,
                (run_id, encode_path_for_db(child), _path_display(child), encode_path_for_db(path), None, now),
            )
            counts["excluded"] += cur.rowcount
        for child in skipped_mount_children:
            cur = conn.execute(
                """
                INSERT OR IGNORE INTO directories(
                    run_id, path_bytes, path_display, parent_path_bytes, device_id,
                    discovered_at, scan_status, backup_status
                ) VALUES (?, ?, ?, ?, ?, ?, 'skipped_mount', 'not_eligible')
                """,
                (run_id, encode_path_for_db(child), _path_display(child), encode_path_for_db(path), None, now),
            )
            counts["skipped"] += cur.rowcount
        for child, child_error in scan_failed_children:
            cur = conn.execute(
                """
                INSERT OR IGNORE INTO directories(
                    run_id, path_bytes, path_display, parent_path_bytes, device_id,
                    discovered_at, scan_status, backup_status, last_error
                ) VALUES (?, ?, ?, ?, ?, ?, 'scan_failed', 'not_eligible', ?)
                """,
                (run_id, encode_path_for_db(child), _path_display(child), encode_path_for_db(path), None, now, child_error),
            )
            counts["errors"] += cur.rowcount
        final_status = "scan_failed" if error_text else "scanned"
        conn.execute(
            "UPDATE directories SET scan_status=?, scan_owner=NULL, scan_claimed_at=NULL, scan_heartbeat_at=NULL, last_error=?, last_finished_at=? WHERE run_id=? AND path_bytes=?",
            (final_status, error_text, now, run_id, encode_path_for_db(path)),
        )
        conn.commit()
        return counts

    def pending_backup_count(self, run_id: str) -> int:
        row = self._connect().execute(
            "SELECT COUNT(*) AS n FROM directories WHERE run_id=? AND backup_status='pending'",
            (run_id,),
        ).fetchone()
        return int(row['n'])

    def claim_backup_batch(
        self,
        run_id: str,
        controller_id: str,
        worker_number: int,
        limit: int,
    ) -> list[str]:
        conn = self._connect()
        conn.execute("BEGIN IMMEDIATE")
        rows = conn.execute(
            "SELECT path_bytes FROM directories WHERE run_id=? AND backup_status='pending' ORDER BY discovered_at, path_display LIMIT ?",
            (run_id, limit),
        ).fetchall()
        if not rows:
            conn.commit()
            return []
        now = _utc_now()
        paths: list[str] = []
        for row in rows:
            paths.append(decode_path_from_db(row['path_bytes']))
            conn.execute(
                "UPDATE directories SET backup_status='running', backup_owner=?, backup_worker_id=?, backup_claimed_at=?, backup_heartbeat_at=? WHERE run_id=? AND path_bytes=? AND backup_status='pending'",
                (controller_id, worker_number, now, now, run_id, row['path_bytes']),
            )
        conn.commit()
        return paths

    def release_backup_claims(self, run_id: str, paths: list[str]) -> None:
        if not paths:
            return
        conn = self._connect()
        conn.execute("BEGIN IMMEDIATE")
        for path in paths:
            conn.execute(
                "UPDATE directories SET backup_status='pending', backup_owner=NULL, backup_worker_id=NULL, backup_pid=NULL, backup_claimed_at=NULL, backup_heartbeat_at=NULL WHERE run_id=? AND path_bytes=? AND backup_status='running'",
                (run_id, encode_path_for_db(path)),
            )
        conn.commit()

    def start_directory_attempt(
        self,
        run_id: str,
        execution_id: str,
        path: str,
        worker_number: int,
        worker_label: str,
    ) -> int:
        conn = self._connect()
        conn.execute("BEGIN IMMEDIATE")
        conn.execute(
            "UPDATE directories SET backup_attempts=backup_attempts+1, backup_heartbeat_at=?, last_started_at=? WHERE run_id=? AND path_bytes=?",
            (_utc_now(), _utc_now(), run_id, encode_path_for_db(path)),
        )
        row = conn.execute(
            "SELECT backup_attempts FROM directories WHERE run_id=? AND path_bytes=?",
            (run_id, encode_path_for_db(path)),
        ).fetchone()
        cur = conn.execute(
            """
            INSERT INTO attempts(
                run_id, execution_id, item_kind, path_bytes, worker_slot, worker_label,
                attempt_number, started_at, outcome
            ) VALUES (?, ?, 'directory', ?, ?, ?, ?, ?, 'running')
            """,
            (run_id, execution_id, encode_path_for_db(path), worker_number, worker_label, int(row['backup_attempts']), _utc_now()),
        )
        conn.commit()
        return int(cur.lastrowid)

    def set_directory_child_pid(self, run_id: str, path: str, pid: int | None, heartbeat_at: str | None = None) -> None:
        self._connect().execute(
            "UPDATE directories SET backup_pid=?, backup_heartbeat_at=COALESCE(?, backup_heartbeat_at) WHERE run_id=? AND path_bytes=?",
            (pid, heartbeat_at, run_id, encode_path_for_db(path)),
        )
        self._connect().commit()

    def touch_directory_attempt(self, run_id: str, path: str) -> None:
        self._connect().execute(
            "UPDATE directories SET backup_heartbeat_at=? WHERE run_id=? AND path_bytes=?",
            (_utc_now(), run_id, encode_path_for_db(path)),
        )
        self._connect().commit()

    def finish_directory_attempt(
        self,
        run_id: str,
        path: str,
        attempt_id: int,
        outcome: str,
        return_code: int,
        stats: DsmcInvocationStats,
        error_text: str | None = None,
    ) -> None:
        conn = self._connect()
        conn.execute("BEGIN IMMEDIATE")
        stats_dict = _stats_to_dict(stats)
        conn.execute(
            """
            UPDATE attempts
               SET ended_at=?, outcome=?, return_code=?, summary_complete=?,
                   objects_inspected=?, objects_backed_up=?, objects_updated=?,
                   objects_rebound=?, objects_deleted=?, objects_expired=?,
                   objects_failed=?, objects_encrypted=?, objects_grew=?, retries=?,
                   bytes_inspected=?, bytes_transferred=?, transfer_time_secs=?,
                   network_rate_bps=?, aggregate_rate_bps=?, objects_compressed_pct=?,
                   data_reduction_pct=?, elapsed_secs=?, error_text=?
             WHERE id=?
            """,
            (
                _utc_now(), outcome, return_code, 1 if stats.has_data() else 0,
                stats_dict['objects_inspected'], stats_dict['objects_backed_up'], stats_dict['objects_updated'],
                stats_dict['objects_rebound'], stats_dict['objects_deleted'], stats_dict['objects_expired'],
                stats_dict['objects_failed'], stats_dict['objects_encrypted'], stats_dict['objects_grew'], stats_dict['retries'],
                stats_dict['bytes_inspected'], stats_dict['bytes_transferred'], stats_dict['transfer_time_secs'],
                stats_dict['network_rate_bps'], stats_dict['aggregate_rate_bps'], stats_dict['objects_compressed_pct'],
                stats_dict['data_reduction_pct'], stats_dict['elapsed_secs'], error_text, attempt_id,
            ),
        )
        conn.execute(
            "UPDATE directories SET backup_status=?, backup_owner=NULL, backup_worker_id=NULL, backup_pid=NULL, backup_claimed_at=NULL, backup_heartbeat_at=NULL, last_return_code=?, last_error=?, last_finished_at=? WHERE run_id=? AND path_bytes=?",
            (outcome, return_code, error_text, _utc_now(), run_id, encode_path_for_db(path)),
        )
        conn.commit()

    def claim_root_manifest(self, run_id: str) -> bool:
        conn = self._connect()
        conn.execute("BEGIN IMMEDIATE")
        row = conn.execute(
            "SELECT manifest_status FROM root_files_state WHERE run_id=?",
            (run_id,),
        ).fetchone()
        if row is None or row['manifest_status'] != 'pending':
            conn.commit()
            return False
        conn.execute(
            "UPDATE root_files_state SET manifest_status='scanning', last_manifest_at=?, last_error=NULL WHERE run_id=?",
            (_utc_now(), run_id),
        )
        conn.commit()
        return True

    def store_root_manifest(self, run_id: str, files: list[str]) -> None:
        conn = self._connect()
        conn.execute("BEGIN IMMEDIATE")
        now = _utc_now()
        for fpath in files:
            conn.execute(
                "INSERT OR IGNORE INTO root_files_entries(run_id, path_bytes, path_display, discovered_at) VALUES (?, ?, ?, ?)",
                (run_id, encode_path_for_db(fpath), _path_display(fpath), now),
            )
        conn.execute("DELETE FROM root_file_chunk_members WHERE chunk_id IN (SELECT id FROM root_file_chunks WHERE run_id=?)", (run_id,))
        conn.execute("DELETE FROM root_file_chunks WHERE run_id=?", (run_id,))
        chunks = chunk_root_files(sorted(files))
        for idx, chunk in enumerate(chunks, start=1):
            cur = conn.execute(
                "INSERT INTO root_file_chunks(run_id, chunk_index, status, total_files) VALUES (?, ?, 'pending', ?)",
                (run_id, idx, len(chunk)),
            )
            chunk_id = int(cur.lastrowid)
            for seq, item in enumerate(chunk, start=1):
                conn.execute(
                    "INSERT INTO root_file_chunk_members(chunk_id, seq, path_bytes, path_display) VALUES (?, ?, ?, ?)",
                    (chunk_id, seq, encode_path_for_db(item), _path_display(item)),
                )
        manifest_status = 'skipped' if not files else 'ready'
        conn.execute(
            "UPDATE root_files_state SET manifest_status=?, total_files=?, total_chunks=?, last_manifest_at=?, last_error=NULL WHERE run_id=?",
            (manifest_status, len(files), len(chunks), now, run_id),
        )
        conn.commit()

    def root_manifest_row(self, run_id: str) -> sqlite3.Row | None:
        return self._connect().execute("SELECT * FROM root_files_state WHERE run_id=?", (run_id,)).fetchone()

    def claim_root_chunk(self, run_id: str, controller_id: str) -> sqlite3.Row | None:
        conn = self._connect()
        conn.execute("BEGIN IMMEDIATE")
        row = conn.execute(
            "SELECT * FROM root_file_chunks WHERE run_id=? AND status='pending' ORDER BY chunk_index LIMIT 1",
            (run_id,),
        ).fetchone()
        if row is None:
            conn.commit()
            return None
        now = _utc_now()
        conn.execute(
            "UPDATE root_file_chunks SET status='running', claimed_by=?, claimed_at=?, heartbeat_at=?, last_started_at=? WHERE id=?",
            (controller_id, now, now, now, row['id']),
        )
        conn.commit()
        return self._connect().execute("SELECT * FROM root_file_chunks WHERE id=?", (row['id'],)).fetchone()

    def root_chunk_files(self, chunk_id: int) -> list[str]:
        rows = self._connect().execute(
            "SELECT path_bytes FROM root_file_chunk_members WHERE chunk_id=? ORDER BY seq",
            (chunk_id,),
        ).fetchall()
        return [decode_path_from_db(row['path_bytes']) for row in rows]

    def start_root_chunk_attempt(
        self,
        run_id: str,
        execution_id: str,
        chunk_id: int,
        worker_slot: int,
        worker_label: str,
    ) -> int:
        conn = self._connect()
        conn.execute("BEGIN IMMEDIATE")
        row = conn.execute(
            "SELECT COUNT(*) AS n FROM attempts WHERE run_id=? AND root_chunk_id=?",
            (run_id, chunk_id),
        ).fetchone()
        cur = conn.execute(
            "INSERT INTO attempts(run_id, execution_id, item_kind, root_chunk_id, worker_slot, worker_label, attempt_number, started_at, outcome) VALUES (?, ?, 'root_files_chunk', ?, ?, ?, ?, ?, 'running')",
            (run_id, execution_id, chunk_id, worker_slot, worker_label, int(row['n']) + 1, _utc_now()),
        )
        conn.commit()
        return int(cur.lastrowid)

    def set_root_chunk_pid(self, chunk_id: int, pid: int | None) -> None:
        self._connect().execute(
            "UPDATE root_file_chunks SET worker_pid=?, heartbeat_at=? WHERE id=?",
            (pid, _utc_now(), chunk_id),
        )
        self._connect().commit()

    def touch_root_chunk(self, chunk_id: int) -> None:
        self._connect().execute(
            "UPDATE root_file_chunks SET heartbeat_at=? WHERE id=?",
            (_utc_now(), chunk_id),
        )
        self._connect().commit()

    def finish_root_chunk_attempt(
        self,
        run_id: str,
        chunk_id: int,
        attempt_id: int,
        outcome: str,
        return_code: int,
        stats: DsmcInvocationStats,
        error_text: str | None = None,
    ) -> None:
        conn = self._connect()
        conn.execute("BEGIN IMMEDIATE")
        stats_dict = _stats_to_dict(stats)
        conn.execute(
            """
            UPDATE attempts
               SET ended_at=?, outcome=?, return_code=?, summary_complete=?,
                   objects_inspected=?, objects_backed_up=?, objects_updated=?,
                   objects_rebound=?, objects_deleted=?, objects_expired=?,
                   objects_failed=?, objects_encrypted=?, objects_grew=?, retries=?,
                   bytes_inspected=?, bytes_transferred=?, transfer_time_secs=?,
                   network_rate_bps=?, aggregate_rate_bps=?, objects_compressed_pct=?,
                   data_reduction_pct=?, elapsed_secs=?, error_text=?
             WHERE id=?
            """,
            (
                _utc_now(), outcome, return_code, 1 if stats.has_data() else 0,
                stats_dict['objects_inspected'], stats_dict['objects_backed_up'], stats_dict['objects_updated'],
                stats_dict['objects_rebound'], stats_dict['objects_deleted'], stats_dict['objects_expired'],
                stats_dict['objects_failed'], stats_dict['objects_encrypted'], stats_dict['objects_grew'], stats_dict['retries'],
                stats_dict['bytes_inspected'], stats_dict['bytes_transferred'], stats_dict['transfer_time_secs'],
                stats_dict['network_rate_bps'], stats_dict['aggregate_rate_bps'], stats_dict['objects_compressed_pct'],
                stats_dict['data_reduction_pct'], stats_dict['elapsed_secs'], error_text, attempt_id,
            ),
        )
        conn.execute(
            "UPDATE root_file_chunks SET status=?, worker_pid=NULL, claimed_by=NULL, claimed_at=NULL, heartbeat_at=NULL, last_return_code=?, last_error=?, last_finished_at=? WHERE id=?",
            (outcome, return_code, error_text, _utc_now(), chunk_id),
        )
        state = conn.execute(
            "SELECT SUM(CASE WHEN status='pending' THEN 1 ELSE 0 END) AS pending, SUM(CASE WHEN status='running' THEN 1 ELSE 0 END) AS running FROM root_file_chunks WHERE run_id=?",
            (run_id,),
        ).fetchone()
        pending = int(state['pending'] or 0) if state is not None else 0
        running = int(state['running'] or 0) if state is not None else 0
        if pending == 0 and running == 0:
            conn.execute(
                "UPDATE root_files_state SET manifest_status='completed' WHERE run_id=? AND manifest_status!='skipped'",
                (run_id,),
            )
        else:
            conn.execute(
                "UPDATE root_files_state SET manifest_status='ready' WHERE run_id=? AND manifest_status!='skipped'",
                (run_id,),
            )
        conn.commit()

    def release_root_chunk(self, chunk_id: int) -> None:
        self._connect().execute(
            "UPDATE root_file_chunks SET status='pending', worker_pid=NULL, claimed_by=NULL, claimed_at=NULL, heartbeat_at=NULL WHERE id=? AND status='running'",
            (chunk_id,),
        )
        self._connect().commit()

    def runtime_status_counts(self, run_id: str) -> dict[str, dict[str, int]]:
        conn = self._connect()
        scan_counts = {row['scan_status']: int(row['n']) for row in conn.execute(
            "SELECT scan_status, COUNT(*) AS n FROM directories WHERE run_id=? GROUP BY scan_status",
            (run_id,),
        )}
        backup_counts = {row['backup_status']: int(row['n']) for row in conn.execute(
            "SELECT backup_status, COUNT(*) AS n FROM directories WHERE run_id=? GROUP BY backup_status",
            (run_id,),
        )}
        root_counts = {row['status']: int(row['n']) for row in conn.execute(
            "SELECT status, COUNT(*) AS n FROM root_file_chunks WHERE run_id=? GROUP BY status",
            (run_id,),
        )}
        return {"scan": scan_counts, "backup": backup_counts, "root_chunks": root_counts}

    def directory_counts(self, run_id: str) -> dict[str, int]:
        status = self.runtime_status_counts(run_id)
        return {
            "discovered": sum(v for k, v in status['backup'].items() if k not in ('not_eligible', 'skipped_root')),
            "completed": status['backup'].get('succeeded', 0),
            "failed": status['backup'].get('failed', 0) + status['backup'].get('timed_out', 0),
            "skipped_mounts": status['scan'].get('skipped_mount', 0),
            "excluded_paths": status['scan'].get('excluded', 0),
            "scan_errors": status['scan'].get('scan_failed', 0),
        }

    def load_global_stats(self, run_id: str, execution_id: str | None = None) -> dict[str, int | float]:
        query = """
            SELECT
                COUNT(*) AS dsmc_done,
                SUM(summary_complete) AS summaries_parsed,
                SUM(CASE WHEN summary_complete=0 THEN 1 ELSE 0 END) AS incomplete_summaries,
                SUM(objects_inspected) AS objects_inspected,
                SUM(objects_backed_up) AS objects_backed_up,
                SUM(objects_updated) AS objects_updated,
                SUM(objects_rebound) AS objects_rebound,
                SUM(objects_deleted) AS objects_deleted,
                SUM(objects_expired) AS objects_expired,
                SUM(objects_failed) AS objects_failed,
                SUM(objects_encrypted) AS objects_encrypted,
                SUM(objects_grew) AS objects_grew,
                SUM(retries) AS retries,
                SUM(bytes_inspected) AS bytes_inspected,
                SUM(bytes_transferred) AS bytes_transferred,
                SUM(elapsed_secs) AS total_elapsed_secs
              FROM attempts
             WHERE run_id=? AND outcome!='running'
        """
        params: tuple[object, ...]
        if execution_id is None:
            params = (run_id,)
        else:
            query += " AND execution_id=?"
            params = (run_id, execution_id)
        row = self._connect().execute(query, params).fetchone()
        return {
            "dsmc_done": int(row['dsmc_done'] or 0),
            "summaries_parsed": int(row['summaries_parsed'] or 0),
            "incomplete_summaries": int(row['incomplete_summaries'] or 0),
            "objects_inspected": int(row['objects_inspected'] or 0),
            "objects_backed_up": int(row['objects_backed_up'] or 0),
            "objects_updated": int(row['objects_updated'] or 0),
            "objects_rebound": int(row['objects_rebound'] or 0),
            "objects_deleted": int(row['objects_deleted'] or 0),
            "objects_expired": int(row['objects_expired'] or 0),
            "objects_failed": int(row['objects_failed'] or 0),
            "objects_encrypted": int(row['objects_encrypted'] or 0),
            "objects_grew": int(row['objects_grew'] or 0),
            "retries": int(row['retries'] or 0),
            "bytes_inspected": int(row['bytes_inspected'] or 0),
            "bytes_transferred": int(row['bytes_transferred'] or 0),
            "total_elapsed_secs": float(row['total_elapsed_secs'] or 0.0),
            "active_children": 0,
        }

    def completion_snapshot(self, run_id: str) -> dict[str, object]:
        counts = self.runtime_status_counts(run_id)
        root_state = self.root_manifest_row(run_id)
        retryable_scan = counts['scan'].get('pending', 0) + counts['scan'].get('scanning', 0)
        retryable_backup = counts['backup'].get('pending', 0) + counts['backup'].get('running', 0) + counts['backup'].get('interrupted', 0)
        retryable_root = counts['root_chunks'].get('pending', 0) + counts['root_chunks'].get('running', 0)
        root_terminal = True
        if root_state is not None and root_state['manifest_status'] not in ('skipped', 'completed'):
            root_terminal = retryable_root == 0 and root_state['manifest_status'] == 'ready' and counts['root_chunks'].get('pending', 0) == 0
        passed = retryable_scan == 0 and retryable_backup == 0 and retryable_root == 0 and root_terminal
        return {
            "counts": counts,
            "root_manifest_status": root_state['manifest_status'] if root_state is not None else None,
            "passed": passed,
        }

    def dashboard_snapshot(self, run_ctx: RunContext) -> dict[str, object]:
        row = self.load_run_row()
        completion = self.completion_snapshot(run_ctx.run_id)
        return {
            "mode": "RESUMED" if run_ctx.resumed else "NEW",
            "run_id": run_ctx.run_id,
            "execution_id": run_ctx.execution_id,
            "state_db": run_ctx.state_db_path,
            "reused_completed": run_ctx.reused_completed,
            "recovered_scan_claims": run_ctx.recovered_scan_claims,
            "recovered_backup_claims": run_ctx.recovered_backup_claims,
            "recovered_root_chunk_claims": run_ctx.recovered_root_chunk_claims,
            "run_state": row['state'] if row is not None else None,
            "scan_counts": completion['counts']['scan'],
            "backup_counts": completion['counts']['backup'],
        }


class ControllerHeartbeat(threading.Thread):
    def __init__(self, state_db: PersistentStateDB, run_ctx: RunContext, stop_event: threading.Event) -> None:
        super().__init__(name="controller-heartbeat", daemon=True)
        self._state_db = state_db
        self._run_ctx = run_ctx
        self._stop_event = stop_event

    def run(self) -> None:
        while not self._stop_event.wait(CONTROLLER_HEARTBEAT_SECS):
            self._state_db.heartbeat_controller(self._run_ctx.run_id, self._run_ctx.controller_id)


def _restore_counters_from_snapshot(counters: Counters, snapshot: dict[str, int]) -> None:
    counters.discovered = snapshot['discovered']
    counters.completed = snapshot['completed']
    counters.failed = snapshot['failed']
    counters.skipped_mounts = snapshot['skipped_mounts']
    counters.excluded_paths = snapshot['excluded_paths']
    counters.scan_errors = snapshot['scan_errors']


def _restore_global_stats_from_snapshot(global_stats: GlobalDsmcStats, snapshot: dict[str, int | float]) -> None:
    global_stats.dsmc_done = int(snapshot['dsmc_done'])
    global_stats.summaries_parsed = int(snapshot['summaries_parsed'])
    global_stats.incomplete_summaries = int(snapshot['incomplete_summaries'])
    global_stats.objects_inspected = int(snapshot['objects_inspected'])
    global_stats.objects_backed_up = int(snapshot['objects_backed_up'])
    global_stats.objects_updated = int(snapshot['objects_updated'])
    global_stats.objects_rebound = int(snapshot['objects_rebound'])
    global_stats.objects_deleted = int(snapshot['objects_deleted'])
    global_stats.objects_expired = int(snapshot['objects_expired'])
    global_stats.objects_failed = int(snapshot['objects_failed'])
    global_stats.objects_encrypted = int(snapshot['objects_encrypted'])
    global_stats.objects_grew = int(snapshot['objects_grew'])
    global_stats.retries = int(snapshot['retries'])
    global_stats.bytes_inspected = int(snapshot['bytes_inspected'])
    global_stats.bytes_transferred = int(snapshot['bytes_transferred'])
    global_stats.total_elapsed_secs = float(snapshot['total_elapsed_secs'])
    global_stats.active_children = int(snapshot.get('active_children', 0))


def classify_return_code(return_code: int, stop_event: threading.Event) -> str:
    if stop_event.is_set() and return_code in (TIMEOUT_RC, INTERRUPTED_RC):
        return 'interrupted'
    if return_code == TIMEOUT_RC:
        return 'timed_out'
    if return_code == IDLE_TIMEOUT_RC:
        return 'timed_out'
    if return_code <= MAX_DSMC_SUCCESS_RC:
        return 'succeeded'
    return 'failed'


def make_directory_dsmc_callbacks(
    state_db: PersistentStateDB,
    run_ctx: RunContext,
    current_path: str,
):
    def _on_child_started(pid: int, _start: float) -> None:
        state_db.set_directory_child_pid(run_ctx.run_id, current_path, pid, _utc_now())

    def _on_child_output(_ts: float) -> None:
        state_db.touch_directory_attempt(run_ctx.run_id, current_path)

    return _on_child_started, _on_child_output


def make_root_chunk_dsmc_callbacks(state_db: PersistentStateDB, chunk_id: int):
    def _on_root_child_started(pid: int, _start: float) -> None:
        state_db.set_root_chunk_pid(chunk_id, pid)

    def _on_root_child_output(_ts: float) -> None:
        state_db.touch_root_chunk(chunk_id)

    return _on_root_child_started, _on_root_child_output

def build_resume_command(args: argparse.Namespace, root: str) -> str:
    parts = [
        sys.executable,
        sys.argv[0],
        root,
        str(args.streams),
        '--state-db',
        args.state_db,
        '--resume',
        '--log-dir',
        args.log_dir,
        '--dsmc',
        args.dsmc,
        '--dsmc-timeout',
        str(args.dsmc_timeout),
        '--dsmc-idle-timeout',
        str(args.dsmc_idle_timeout),
        '--resourceutilization',
        str(args.resourceutilization),
        '--batch-size',
        str(args.batch_size),
        '--queue-size',
        str(args.queue_size),
    ]
    if args.no_dashboard:
        parts.append('--no-dashboard')
    else:
        parts.extend(['--dashboard-refresh-seconds', str(args.dashboard_refresh_seconds)])
    parts.extend(['--progress-seconds', str(args.progress_seconds)])
    for extra in args.exclude_path:
        parts.extend(['--exclude-path', extra])
    for option in args.dsmc_option:
        parts.extend(['--dsmc-option', option])
    return ' '.join(shlex.quote(part) for part in parts)

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
                        # Always merge exactly once per invocation; add_invocation
                        # tracks completeness internally via has_data().
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
    child_started_callback=None,
    child_output_callback=None,
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
    if child_started_callback is not None:
        child_started_callback(proc.pid, start_time)

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
                if child_output_callback is not None:
                    child_output_callback(now)
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
    if is_idle_timeout:
        synthetic_rc = IDLE_TIMEOUT_RC
    elif timed_out_reason == "stop requested":
        synthetic_rc = INTERRUPTED_RC
    else:
        synthetic_rc = TIMEOUT_RC
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
                                # Always merge into global stats (exactly once per
                                # invocation).  add_invocation tracks whether the
                                # summary was complete via has_data().
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


def persistent_scan_directories(
    root: str,
    state_db: PersistentStateDB,
    run_ctx: RunContext,
    producer_done: threading.Event,
    stop_event: threading.Event,
    counters: Counters,
    excluded_paths: frozenset[str],
    logger: SafeLogger,
) -> None:
    try:
        root_device = os.stat(root, follow_symlinks=False).st_dev
    except OSError as exc:
        logger.write(f"FATAL: cannot stat {root!r}: {exc}")
        counters.add("scan_errors")
        producer_done.set()
        return

    mount_boundaries = nested_mounts(root)
    try:
        while not stop_event.is_set():
            current = state_db.claim_next_scan(run_ctx.run_id, run_ctx.controller_id)
            if current is None:
                producer_done.set()
                return
            eligible_children: list[tuple[str, int | None]] = []
            excluded_children: list[str] = []
            skipped_mount_children: list[str] = []
            scan_failed_children: list[tuple[str, str]] = []
            parent_error: str | None = None
            try:
                with os.scandir(current) as entries:
                    for entry in entries:
                        if stop_event.is_set():
                            parent_error = "scan interrupted"
                            break
                        try:
                            if not entry.is_dir(follow_symlinks=False):
                                continue
                            child = os.path.normpath(entry.path)
                            if is_path_excluded(child, excluded_paths):
                                excluded_children.append(child)
                                logger.write(f"SKIP excluded path: {child}")
                                continue
                            if child in mount_boundaries:
                                skipped_mount_children.append(child)
                                logger.write(f"SKIP nested mount: {child}")
                                continue
                            child_dev = entry.stat(follow_symlinks=False).st_dev
                            if child_dev != root_device:
                                skipped_mount_children.append(child)
                                logger.write(f"SKIP different filesystem: {child}")
                                continue
                            eligible_children.append((child, child_dev))
                        except OSError as exc:
                            logger.write(f"SCAN ERROR: {entry.path!r}: {exc}")
                            scan_failed_children.append((os.path.normpath(entry.path), str(exc)))
            except OSError as exc:
                parent_error = str(exc)
                logger.write(f"SCAN ERROR: {current!r}: {exc}")
            if parent_error == "scan interrupted":
                state_db.release_scan_claim(run_ctx.run_id, current)
                continue
            inserted = state_db.finish_scan(
                run_ctx.run_id,
                current,
                eligible_children,
                excluded_children,
                skipped_mount_children,
                scan_failed_children,
                error_text=parent_error,
            )
            if inserted["eligible"]:
                counters.add("discovered", inserted["eligible"])
            if inserted["excluded"]:
                counters.add("excluded_paths", inserted["excluded"])
            if inserted["skipped"]:
                counters.add("skipped_mounts", inserted["skipped"])
            if inserted["errors"]:
                counters.add("scan_errors", inserted["errors"])
            if parent_error:
                counters.add("scan_errors")
    finally:
        producer_done.set()



def persistent_worker(
    worker_number: int,
    args: argparse.Namespace,
    state_db: PersistentStateDB,
    run_ctx: RunContext,
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
            if stop_event.is_set():
                worker_states.stopped(worker_number)
                return
            worker_states.waiting(worker_number)
            batch = state_db.claim_backup_batch(
                run_ctx.run_id,
                run_ctx.controller_id,
                worker_number,
                args.batch_size,
            )
            if not batch:
                if producer_done.is_set() and state_db.pending_backup_count(run_ctx.run_id) == 0:
                    worker_states.stopped(worker_number)
                    return
                time.sleep(0.2)
                continue

            worker_states.start_batch(worker_number, len(batch))
            logger.write(
                f"{worker_name}: reserved {len(batch)} "
                f"{'directory' if len(batch) == 1 else 'directories'} "
                f"(batch #{worker_states.get_batch_id(worker_number)})"
            )
            unstarted = list(batch)
            try:
                with worker_log.open("ab", buffering=0) as output:
                    output.write(
                        (
                            f"\n===== {time.strftime('%Y-%m-%d %H:%M:%S')} =====\n"
                            f"Batch size: {len(batch)}\n"
                            + "\n".join(dsm_directory_operand(p) for p in batch)
                            + "\n"
                        ).encode("utf-8", errors="backslashreplace")
                    )
                    for index, path in enumerate(batch, start=1):
                        if stop_event.is_set():
                            break
                        unstarted.pop(0)
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
                        attempt_id = state_db.start_directory_attempt(
                            run_ctx.run_id,
                            run_ctx.execution_id,
                            path,
                            worker_number,
                            worker_name,
                        )
                        return_code = 0
                        inv_stats = DsmcInvocationStats()
                        child_started_callback, child_output_callback = make_directory_dsmc_callbacks(
                            state_db,
                            run_ctx,
                            path,
                        )

                        try:
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
                                child_started_callback=child_started_callback,
                                child_output_callback=child_output_callback,
                            )
                            global_stats.add_invocation(inv_stats)
                        except OSError as exc:
                            return_code = 127
                            logger.write(f"{worker_name}: OS error running dsmc: {exc}")
                        outcome = classify_return_code(return_code, stop_event)
                        error_text = None
                        if outcome == "interrupted":
                            error_text = "interrupted by stop request"
                        elif outcome == "timed_out":
                            error_text = "timeout"
                        state_db.finish_directory_attempt(
                            run_ctx.run_id,
                            path,
                            attempt_id,
                            outcome,
                            return_code,
                            inv_stats,
                            error_text=error_text,
                        )
                        worker_states.set_result(worker_number, return_code, inv_stats)
                        if outcome == "succeeded":
                            counters.add("completed")
                            logger.write(
                                f"{worker_name}: completed {index}/{len(batch)} rc={return_code} path={path}"
                            )
                        elif outcome == "timed_out":
                            counters.add("failed")
                            logger.write(
                                f"{worker_name}: TIMEOUT {index}/{len(batch)} rc={return_code} path={path}"
                            )
                            failed_logger.write(f"{return_code}	{path}	# timeout")
                        elif outcome == "interrupted":
                            logger.write(
                                f"{worker_name}: interrupted {index}/{len(batch)} rc={return_code} path={path}"
                            )
                        else:
                            counters.add("failed")
                            logger.write(
                                f"{worker_name}: FAILED {index}/{len(batch)} rc={return_code} path={path}"
                            )
                            failed_logger.write(f"{return_code}	{path}")
            finally:
                state_db.release_backup_claims(run_ctx.run_id, unstarted)
                worker_states.idle(worker_number)
    finally:
        worker_states.stopped(worker_number)



def persistent_run_root_files_job(
    args: argparse.Namespace,
    root: str,
    excluded_paths: frozenset[str],
    state_db: PersistentStateDB,
    run_ctx: RunContext,
    worker_states: WorkerStates,
    global_stats: GlobalDsmcStats,
    logger: SafeLogger,
    failed_logger: SafeAppender,
    stop_event: threading.Event,
) -> None:
    JOB_NAME = "ROOT_FILES"
    WORKER_SLOT = 0
    worker_log = Path(args.log_dir) / "root-files-job.log"
    dsm_log_dir = Path(args.log_dir) / "root-files-dsm"
    dsm_log_dir.mkdir(parents=True, exist_ok=True)
    environment = os.environ.copy()
    environment["DSM_LOG"] = str(dsm_log_dir)
    try:
        manifest = state_db.root_manifest_row(run_ctx.run_id)
        if manifest is not None and manifest["manifest_status"] == "pending":
            worker_states.set_custom_status(WORKER_SLOT, "scanning", f"scanning {root}")
            if state_db.claim_root_manifest(run_ctx.run_id):
                files = collect_root_files(root, excluded_paths)
                logger.write(
                    f"{JOB_NAME}: found {len(files)} eligible non-directory "
                    f"{'entry' if len(files) == 1 else 'entries'} under {root!r}"
                )
                state_db.store_root_manifest(run_ctx.run_id, files)
                manifest = state_db.root_manifest_row(run_ctx.run_id)
        if manifest is None:
            worker_states.stopped(WORKER_SLOT)
            return
        if manifest["manifest_status"] == "skipped":
            logger.write(f"{JOB_NAME}: no eligible files directly under {root!r}; job skipped")
            worker_states.set_custom_status(WORKER_SLOT, "skipped", "no files under /")
            return
        total_files = int(manifest["total_files"])
        total_chunks = int(manifest["total_chunks"])
        worker_states.start_batch(WORKER_SLOT, total_chunks)
        worker_states.set_custom_status(
            WORKER_SLOT,
            "running",
            f"{total_files} file(s), {total_chunks} chunk(s)",
        )
        with worker_log.open("ab", buffering=0) as output:
            output.write(
                (
                    f"\n===== {time.strftime('%Y-%m-%d %H:%M:%S')} =====\n"
                    f"ROOT_FILES job: {total_files} file(s), {total_chunks} chunk(s)\n"
                ).encode("utf-8", errors="backslashreplace")
            )
            while not stop_event.is_set():
                chunk = state_db.claim_root_chunk(run_ctx.run_id, run_ctx.controller_id)
                if chunk is None:
                    break
                chunk_idx = int(chunk["chunk_index"])
                chunk_files = state_db.root_chunk_files(int(chunk["id"]))
                label = f"chunk {chunk_idx}/{total_chunks}"
                worker_states.set_directory(WORKER_SLOT, chunk_idx, label)
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
                attempt_id = state_db.start_root_chunk_attempt(
                    run_ctx.run_id,
                    run_ctx.execution_id,
                    int(chunk["id"]),
                    WORKER_SLOT,
                    JOB_NAME,
                )
                return_code = 0
                inv_stats = DsmcInvocationStats()
                child_started_callback, child_output_callback = make_root_chunk_dsmc_callbacks(
                    state_db,
                    int(chunk["id"]),
                )

                try:
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
                        child_started_callback=child_started_callback,
                        child_output_callback=child_output_callback,
                    )
                    global_stats.add_invocation(inv_stats)
                except OSError as exc:
                    return_code = 127
                    logger.write(f"{JOB_NAME}: OS error running dsmc: {exc}")
                outcome = classify_return_code(return_code, stop_event)
                state_db.finish_root_chunk_attempt(
                    run_ctx.run_id,
                    int(chunk["id"]),
                    attempt_id,
                    outcome,
                    return_code,
                    inv_stats,
                    error_text=("interrupted by stop request" if outcome == "interrupted" else None),
                )
                worker_states.set_result(WORKER_SLOT, return_code, inv_stats)
                if outcome == "timed_out":
                    logger.write(f"{JOB_NAME}: TIMEOUT on {label}")
                    failed_logger.write(f"{return_code}	{root} [{label}]	# timeout")
                elif outcome == "succeeded":
                    logger.write(f"{JOB_NAME}: completed {label} rc={return_code}")
                elif outcome == "interrupted":
                    logger.write(f"{JOB_NAME}: interrupted {label} rc={return_code}")
                else:
                    logger.write(f"{JOB_NAME}: FAILED {label} rc={return_code}")
                    failed_logger.write(f"{return_code}	{root} [{label}]")
    finally:
        worker_states.idle(WORKER_SLOT)
        worker_states.stopped(WORKER_SLOT)


# ---------------------------------------------------------------------------
# Progress reporter (non-TTY fallback)
# ---------------------------------------------------------------------------

def progress_reporter(
    counters: Counters,
    global_stats: GlobalDsmcStats,
    work_queue,
    producer_done: threading.Event,
    stop_event: threading.Event,
    interval: int,
    state_snapshot_provider=None,
) -> None:
    while not stop_event.wait(interval):
        discovered, completed, failed, skipped, excluded, errors = counters.snapshot()
        q_size = work_queue.qsize()
        in_progress = max(0, discovered - completed - failed - q_size)
        gs = global_stats.snapshot()
        extra = ""
        if state_snapshot_provider is not None:
            ps = state_snapshot_provider()
            extra = (
                f" mode={ps.get('mode','?')} reused={ps.get('reused_completed',0)}"
                f" recovered_scan={ps.get('recovered_scan_claims',0)}"
                f" recovered_backup={ps.get('recovered_backup_claims',0)}"
            )
        print(
            f"PROGRESS discovered={discovered} q={q_size} in-prog={in_progress} "
            f"excl={excluded} completed={completed} failed={failed} "
            f"skipped_mounts={skipped} scan_errors={errors} "
            f"children={gs['active_children']} "
            f"done={gs['dsmc_done']} parsed={gs['summaries_parsed']}/{gs['dsmc_done']} "
            f"incomplete={gs['incomplete_summaries']} "
            f"insp={gs['objects_inspected']:,} bkup={gs['objects_backed_up']:,} "
            f"updated={gs['objects_updated']:,} failed={gs['objects_failed']:,} "
            f"retries={gs['retries']:,} "
            f"processed={format_bytes(gs['bytes_inspected'])} "
            f"sent={format_bytes(gs['bytes_transferred'])}{extra}",
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
    parser.add_argument("mountpoint", nargs="?", help="Mounted filesystem to process (required unless --status is used)")
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
        "--state-db",
        default=None,
        metavar="PATH",
        help=(
            "Path to the durable SQLite state database. Default: <log-dir>/" + STATE_DB_FILENAME
        ),
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Resume an unfinished compatible run from the SQLite state database",
    )
    parser.add_argument(
        "--new-run",
        action="store_true",
        help="Start a new run, archiving any existing state DB rather than overwriting it",
    )
    parser.add_argument(
        "--status",
        action="store_true",
        help="Print saved state DB status and exit without starting scanner or workers",
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

    if args.resume and args.new_run:
        parser.error("--resume and --new-run are mutually exclusive")
    if args.status and (args.resume or args.new_run):
        parser.error("--status cannot be combined with --resume or --new-run")
    if args.mountpoint is None and not args.status:
        parser.error("mountpoint is required unless --status is used")
    if args.status and args.mountpoint is not None and args.streams_positional is not None:
        parser.error("WORKERS positional argument cannot be used with --status")
    requested_durable_state_mode = args.resume or args.new_run or args.status or args.state_db is not None
    if args.dry_run and requested_durable_state_mode:
        parser.error("--dry-run cannot be combined with --state-db/--resume/--new-run/--status")
    if args.status and args.state_db is None:
        parser.error("--status requires --state-db PATH")
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

def run_legacy_main(args: argparse.Namespace) -> int:
    maybe_clear_screen()
    start_time = time.monotonic()
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
        f"dsmc_insp={gs['objects_inspected']:,} dsmc_bkup={gs['objects_backed_up']:,} "
        f"processed={format_bytes(gs['bytes_inspected'])} "
        f"sent={format_bytes(gs['bytes_transferred'])}"
    )
    # Machine-readable final stats line useful for auditing and log parsing.
    # bytes_inspected / bytes_transferred are exact integer byte counts.
    logger.write(
        f"FINAL DSMC STATS"
        f" invocations={gs['dsmc_done']}"
        f" parsed={gs['summaries_parsed']}"
        f" incomplete={gs['incomplete_summaries']}"
        f" objects_inspected={gs['objects_inspected']}"
        f" objects_backed_up={gs['objects_backed_up']}"
        f" objects_updated={gs['objects_updated']}"
        f" objects_failed={gs['objects_failed']}"
        f" retries={gs['retries']}"
        f" bytes_inspected={gs['bytes_inspected']}"
        f" ({format_bytes(gs['bytes_inspected'])})"
        f" bytes_transferred={gs['bytes_transferred']}"
        f" ({format_bytes(gs['bytes_transferred'])})"
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
    root_state: WorkerState | None = None
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

    # ---- Human-readable final summary (stdout + controller log) ----
    elapsed_secs = time.monotonic() - start_time
    summary = format_final_summary(
        root=root,
        streams=args.streams,
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
        scanner_done=producer_done.is_set(),
    )
    # Print to stdout always (dashboard mode or not); dashboard.finish() has already
    # run inside the finally block above, so no subsequent redraw can overwrite this.
    print(summary, flush=True)
    # Write verbatim to the controller log (no per-line timestamp so the block
    # remains human-readable when tailing the log file).
    logger.write_raw(summary + "\n")

    return 0 if failed == 0 and errors == 0 else 1


def run_status_mode(args: argparse.Namespace) -> int:
    state_db = PersistentStateDB(os.path.abspath(args.state_db))
    try:
        print(state_db.status_report(), flush=True)
    finally:
        state_db.close()
    return 0


def run_persistent_main(args: argparse.Namespace) -> int:
    maybe_clear_screen()
    start_time = time.monotonic()
    root = os.path.normpath(os.path.realpath(args.mountpoint))
    args.mountpoint = root
    args.log_dir = os.path.abspath(args.log_dir)
    if args.state_db is None:
        args.state_db = os.path.join(args.log_dir, STATE_DB_FILENAME)
    args.state_db = os.path.abspath(args.state_db)

    if not os.path.isdir(root):
        print(f"ERROR: not a directory: {root}", file=sys.stderr)
        return 2

    resolved_dsmc = shutil.which(args.dsmc)
    if resolved_dsmc is None:
        print(f"ERROR: dsmc executable not found: {args.dsmc}", file=sys.stderr)
        return 2
    args.dsmc = resolved_dsmc

    excluded_paths_set: set[str] = set()
    log_dir_norm = os.path.normpath(args.log_dir)
    if is_path_excluded(log_dir_norm, frozenset([root])):
        excluded_paths_set.add(log_dir_norm)
    for raw_excl in args.exclude_path:
        norm = os.path.normpath(os.path.abspath(raw_excl))
        if norm == root:
            print(
                f"WARNING: --exclude-path {raw_excl!r} resolves to the crawl root; ignoring to avoid excluding all work",
                file=sys.stderr,
            )
            continue
        excluded_paths_set.add(norm)
    state_db_norm = os.path.normpath(args.state_db)
    for extra in (state_db_norm, state_db_norm + '-wal', state_db_norm + '-shm'):
        if is_path_excluded(extra, frozenset([root])):
            excluded_paths_set.add(extra)
    excluded_paths = frozenset(excluded_paths_set)

    dashboard_enabled = should_enable_dashboard(args.no_dashboard)
    log_dir = Path(args.log_dir)
    log_dir.mkdir(parents=True, exist_ok=True)
    logger = SafeLogger(log_dir / 'controller.log', echo=not dashboard_enabled)
    failed_path = log_dir / 'failed-directories.tsv'
    if not failed_path.exists():
        failed_path.write_text('return_code\tdirectory\tnotes\n', encoding='utf-8')
    failed_logger = SafeAppender(failed_path)

    state_db = PersistentStateDB(args.state_db)
    root_device = os.stat(root, follow_symlinks=False).st_dev
    coverage_config = build_coverage_config(root, root_device, excluded_paths, args)
    operational_config = build_operational_config(args)
    state_db_exists = os.path.exists(args.state_db)
    try:
        if args.new_run and state_db_exists:
            archived = state_db.archive_existing()
            logger.write(f"INFO: archived previous state DB to {archived}")
            state_db_exists = False
        if state_db_exists:
            if args.resume:
                run_ctx = state_db.resume_run(root, root_device, coverage_config)
            else:
                row = state_db.load_run_row()
                state = row['state'] if row is not None else 'unknown'
                print(
                    f"ERROR: state DB already exists at {args.state_db} with run state {state!r}. "
                    'Use --resume to continue, --new-run to archive it and start over, or --status to inspect it.',
                    file=sys.stderr,
                )
                return 2
        else:
            if args.resume:
                print(f"ERROR: no saved state DB found at {args.state_db}; start with --new-run", file=sys.stderr)
                return 2
            run_ctx = state_db.create_new_run(root, root_device, coverage_config, operational_config, root == os.sep)

        counts_snapshot = state_db.directory_counts(run_ctx.run_id)
        stats_snapshot = state_db.load_global_stats(run_ctx.run_id)
        counters = Counters()
        _restore_counters_from_snapshot(counters, counts_snapshot)
        global_stats = GlobalDsmcStats()
        _restore_global_stats_from_snapshot(global_stats, stats_snapshot)
        is_root_crawl = (root == os.sep)
        worker_states = WorkerStates(args.streams, has_root_files_job=is_root_crawl)
        producer_done = threading.Event()
        stop_event = threading.Event()
        metrics_queue = PersistentQueueMetrics(state_db, run_ctx.run_id, args.queue_size)
        heartbeat = ControllerHeartbeat(state_db, run_ctx, stop_event)

        logger.write(
            f"START root={root} streams={args.streams} batch_size={args.batch_size} "
            f"dashboard={dashboard_enabled} state_db={args.state_db} mode={'resume' if run_ctx.resumed else 'new'} "
            f"dsmc_timeout={args.dsmc_timeout} dsmc_idle_timeout={args.dsmc_idle_timeout} "
            f"root_files_job={is_root_crawl} run_id={run_ctx.run_id} execution_id={run_ctx.execution_id}"
        )
        if run_ctx.resumed:
            logger.write(
                f"RESUME recovered scan_claims={run_ctx.recovered_scan_claims} "
                f"backup_claims={run_ctx.recovered_backup_claims} root_manifest_claims={run_ctx.recovered_root_manifest_claims} "
                f"root_chunk_claims={run_ctx.recovered_root_chunk_claims} reused_completed={run_ctx.reused_completed}"
            )
        if is_root_crawl:
            logger.write(
                'INFO: crawl root is /; / will not be submitted as an ordinary directory job. '
                'Non-directory entries under / are handled by the dedicated ROOT_FILES job.'
            )
        if log_dir_norm in excluded_paths_set:
            logger.write(f"INFO: log directory {log_dir_norm!r} is inside crawl root; automatically excluded from scanning")
        if state_db_norm in excluded_paths_set:
            logger.write(f"INFO: state DB {state_db_norm!r} is inside crawl root; automatically excluded from scanning")
        for excl in sorted(excluded_paths_set - {log_dir_norm, state_db_norm, state_db_norm + '-wal', state_db_norm + '-shm'}):
            logger.write(f"INFO: user-specified exclusion: {excl!r}")
        if root not in mounted_paths():
            logger.write(
                'WARNING: supplied path is not listed as a mountpoint; processing remains restricted to its current filesystem'
            )

        producer = threading.Thread(
            name='directory-scanner',
            target=persistent_scan_directories,
            args=(root, state_db, run_ctx, producer_done, stop_event, counters, excluded_paths, logger),
            daemon=True,
        )
        workers = [
            threading.Thread(
                name=f'worker-{number:02d}',
                target=persistent_worker,
                args=(number, args, state_db, run_ctx, producer_done, stop_event, counters, worker_states, global_stats, logger, failed_logger),
                daemon=True,
            )
            for number in range(1, args.streams + 1)
        ]
        root_files_thread = None
        if is_root_crawl:
            root_files_thread = threading.Thread(
                name='root-files-job',
                target=persistent_run_root_files_job,
                args=(args, root, excluded_paths, state_db, run_ctx, worker_states, global_stats, logger, failed_logger, stop_event),
                daemon=False,
            )
        state_snapshot_provider = lambda: state_db.dashboard_snapshot(run_ctx)
        if dashboard_enabled:
            dashboard = Dashboard(
                counters=counters,
                worker_states=worker_states,
                global_stats=global_stats,
                work_queue=metrics_queue,
                producer_done=producer_done,
                stop_event=stop_event,
                refresh_seconds=args.dashboard_refresh_seconds,
                state_snapshot_provider=state_snapshot_provider,
            )
            reporter = threading.Thread(name='dashboard', target=dashboard.run, daemon=True)
        else:
            dashboard = None
            reporter = threading.Thread(
                name='progress-reporter',
                target=progress_reporter,
                args=(counters, global_stats, metrics_queue, producer_done, stop_event, args.progress_seconds, state_snapshot_provider),
                daemon=True,
            )

        heartbeat.start()
        try:
            for thread in workers:
                thread.start()
            reporter.start()
            producer.start()
            if root_files_thread is not None:
                root_files_thread.start()
            producer.join()
            for thread in workers:
                thread.join()
            if root_files_thread is not None:
                root_files_thread.join()
        except KeyboardInterrupt:
            logger.write('INTERRUPTED: requesting stop; terminating active dsmc children')
            stop_event.set()
            producer_done.set()
            for thread in [*workers, root_files_thread] if root_files_thread else workers:
                thread.join(timeout=args.shutdown_wait_seconds)
                if thread.is_alive():
                    logger.write(
                        f"WARNING: {thread.name} did not exit within {args.shutdown_wait_seconds}s after interrupt"
                    )
            state_db.mark_run_state(run_ctx.run_id, 'interrupted')
            resume_cmd = build_resume_command(args, root)
            print(f"Interrupted. Resume with: {resume_cmd}", flush=True)
            state_db.clear_controller(run_ctx.run_id, run_ctx.controller_id, 'interrupted')
            return 130
        finally:
            stop_event.set()
            reporter.join(timeout=2)
            if dashboard is not None:
                dashboard.finish()

        counts_snapshot = state_db.directory_counts(run_ctx.run_id)
        gs = global_stats.snapshot()
        logger.write(
            f"END discovered={counts_snapshot['discovered']} completed={counts_snapshot['completed']} failed={counts_snapshot['failed']} "
            f"skipped_mounts={counts_snapshot['skipped_mounts']} excluded_paths={counts_snapshot['excluded_paths']} scan_errors={counts_snapshot['scan_errors']} "
            f"dsmc_insp={gs['objects_inspected']:,} dsmc_bkup={gs['objects_backed_up']:,} processed={format_bytes(gs['bytes_inspected'])} sent={format_bytes(gs['bytes_transferred'])}"
        )
        completion = state_db.completion_snapshot(run_ctx.run_id)
        root_state = None
        if is_root_crawl:
            all_states = worker_states.snapshot()
            root_state = next((s for s in all_states if s.worker_number == 0), None)
        current_exec_stats = state_db.load_global_stats(run_ctx.run_id, run_ctx.execution_id)
        elapsed_secs = time.monotonic() - start_time
        if completion['passed']:
            final_state = 'completed' if counts_snapshot['failed'] == 0 and counts_snapshot['scan_errors'] == 0 else 'completed_with_errors'
        else:
            final_state = 'interrupted'
        state_db.clear_controller(run_ctx.run_id, run_ctx.controller_id, final_state)
        summary = format_final_summary(
            root=root,
            streams=args.streams,
            elapsed_secs=elapsed_secs,
            discovered=counts_snapshot['discovered'],
            completed=counts_snapshot['completed'],
            failed=counts_snapshot['failed'],
            skipped=counts_snapshot['skipped_mounts'],
            excluded=counts_snapshot['excluded_paths'],
            errors=counts_snapshot['scan_errors'],
            gs=gs,
            is_root_crawl=is_root_crawl,
            root_state=root_state,
            scanner_done=producer_done.is_set(),
            state_db_path=args.state_db,
            run_id=run_ctx.run_id,
            execution_id=run_ctx.execution_id,
            resumed=run_ctx.resumed,
            recovered_claims={
                'scan': run_ctx.recovered_scan_claims,
                'backup': run_ctx.recovered_backup_claims,
                'root_manifest': run_ctx.recovered_root_manifest_claims,
                'root_chunks': run_ctx.recovered_root_chunk_claims,
            },
            reused_completed=run_ctx.reused_completed,
            current_execution_stats=current_exec_stats,
            status_counts=completion['counts'],
            completion_passed=bool(completion['passed']),
            run_state=final_state,
        )
        print(summary, flush=True)
        logger.write_raw(summary + '\n')
        return 0 if final_state == 'completed' else 1
    finally:
        state_db.close()


def main() -> int:
    args = parse_args()
    if args.status:
        return run_status_mode(args)
    if args.dry_run:
        return run_legacy_main(args)
    return run_persistent_main(args)


if __name__ == "__main__":
    raise SystemExit(main())
