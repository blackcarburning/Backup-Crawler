# Backup-Crawler

Parallel IBM Storage Protect (`dsmc`) crawler for one mounted filesystem, with a
SQLite-backed durable scheduler that can resume unfinished scan and backup work
after Ctrl-C, process termination, or host reboot.

## Prerequisites

No extra Python package is required on normal Linux Python 3 installations:
`sqlite3` is part of the Python standard library.

Verify that the local Python build includes SQLite support:

```bash
python3 -c 'import sqlite3; print(sqlite3.sqlite_version)'
```

If that import fails, install the SQLite-enabled Python package for your
distribution. Representative examples:

- Debian/Ubuntu: install the distro's Python 3 package set that includes the
  `sqlite3` module for your Python version.
- Fedora/RHEL-family: install the distribution Python 3 build with SQLite
  support.

`dsmc` must also be installed and reachable via `PATH`, or passed explicitly
with `--dsmc /path/to/dsmc`.

## Durable restart / resume model

The crawler stores run metadata, the directory scan frontier, per-directory
backup status, immutable attempt history, and ROOT_FILES chunk state in a
SQLite database.

### Guarantee

The implementation provides **at-least-once execution semantics**:

- if a crash happens before SQLite can durably record successful completion,
  the corresponding `dsmc` invocation may be retried on resume;
- unfinished eligible directories are not silently omitted;
- completed terminal work is reused and not routinely re-run.

This is **not** exactly-once remote backup execution.

### Storage location

By default the durable state DB is created under `--log-dir` as:

```text
<log-dir>/backup-crawler-state.sqlite3
```

Use `--state-db PATH` to place it elsewhere. Prefer a **local durable
filesystem**. SQLite locking on unreliable network filesystems can be unsafe.
Do not place the DB under `/tmp` if you expect recovery after reboot.

If the DB lives under the crawl root, the crawler automatically excludes the DB
file plus its `-wal` and `-shm` sidecars from both scanning and ROOT_FILES
processing.

## Lifecycle commands

### Start a new persistent crawl

```bash
python3 ./backup_crawler.py /data 4 \
  --state-db /var/lib/backup-crawler/data.sqlite3 \
  --new-run
```

If the state DB already exists, `--new-run` archives the old DB to a timestamped
`.bak` path before creating the new run.

### Resume an unfinished crawl

```bash
python3 ./backup_crawler.py /data 4 \
  --state-db /var/lib/backup-crawler/data.sqlite3 \
  --resume
```

On resume the crawler:

- verifies schema integrity and configuration compatibility;
- rejects a second live controller still holding the DB lease;
- recovers stale scanner claims, worker claims, and ROOT_FILES chunk claims;
- keeps completed terminal outcomes;
- continues pending work from durable state.

### Inspect saved state without running work

```bash
python3 ./backup_crawler.py --status \
  --state-db /var/lib/backup-crawler/data.sqlite3
```

`--status` is read-only: it prints the saved run UUID, state, persisted scan and
backup counts, controller lease state, and aggregate attempt totals without
starting scanner or worker threads.

## CLI notes

Key options:

| Option | Meaning |
|---|---|
| `--state-db PATH` | Durable SQLite database path |
| `--resume` | Resume an unfinished compatible run |
| `--new-run` | Archive/replace prior state and start a new crawl |
| `--status` | Print saved DB state and exit |
| `--streams / WORKERS` | Parallel `dsmc` worker count |
| `--batch-size` | Number of pending directories each worker claims at a time |
| `--queue-size` | Dashboard/progress queue sizing for pending work display |
| `--exclude-path PATH` | Exclude a subtree (repeatable) |
| `--dsmc-timeout SECS` | Hard per-invocation timeout (`0` disables) |
| `--dsmc-idle-timeout SECS` | No-output timeout (`0` disables; default `180`) |
| `--idle-timeout-retries N` | Automatic retries after idle timeout (default `3`; `0` disables auto-retry) |
| `--dry-run` | Legacy non-persistent scan/schedule simulation |

Rules:

- `--resume` and `--new-run` are mutually exclusive.
- `--status` cannot be combined with `--resume` or `--new-run`.
- `--status` requires `--state-db PATH`.
- `--dry-run` cannot be combined with durable state options. Dry-run never marks
  durable state complete.
- Dashboard `quiet` is a visual state after 60s without output; automatic idle
  timeout termination occurs at the configured `--dsmc-idle-timeout` threshold
  (180s by default).
- If a state DB already exists and neither `--resume` nor `--new-run` is given,
  the crawler stops and asks you to choose explicitly.

## Configuration compatibility

Resume is allowed only when the saved run matches the requested root and
coverage-affecting configuration, including:

- canonical crawl root,
- root device ID,
- exclusion set,
- `dsmc` executable path,
- extra `dsmc` options,
- `resourceutilization`,
- mount/symlink policy version,
- ROOT_FILES mode.

Operational tuning such as worker count, batch size, queue size, dashboard
refresh interval, and timeout values may change between executions.

## Scheduling model

1. A single **scanner** thread traverses the filesystem starting at the
   supplied mountpoint, staying on the same device and honoring path
   exclusions.
2. Every directory is persisted exactly once in SQLite using its raw filesystem
   byte representation (`os.fsencode(...)` stored as `BLOB`) so unusual and
   non-UTF-8 names round-trip safely.
3. Each directory has separate durable states for:
   - scanning (`pending`, `scanning`, `scanned`, `scan_failed`, `excluded`,
     `skipped_mount`)
   - backup (`pending`, `running`, `succeeded`, `failed`, `timed_out`,
     `interrupted`, `skipped_root`, `not_eligible`)
4. Workers atomically claim pending backup rows in batches and run one
   `dsmc incremental -subdir=no` per directory.
5. Idle-timeout attempts (`rc=125`) are recorded immutably; if retries remain,
   the directory is atomically moved back to pending with a durable retry
   backoff (`retry_not_before`) so resume honors delayed retries.
6. Immutable attempt rows record every real `dsmc` invocation, including
   retries after stale-claim recovery.

## Crash and Ctrl-C behavior

### Ctrl-C / SIGINT

On controlled interruption the crawler:

- stops claiming new scan or backup work,
- terminates active `dsmc` process groups,
- leaves unstarted claimed work pending,
- marks interrupted in-flight attempts retryable,
- updates the run state to `interrupted`,
- prints an exact `--resume` command,
- exits with code `130`.

### Crash / reboot / kill -9

On the next `--resume`, stale `scanning` and `running` claims are recovered back
to pending so the crawler can continue. If a `dsmc` invocation actually
succeeded just before the crash but SQLite did not record success yet, the job
is retried under the at-least-once rule.

## Special handling of the filesystem root (`/`)

When the crawl entry point is exactly `/`, the script uses `/` as a traversal
anchor only and never submits it to `dsmc` as an ordinary directory operand.

Instead:

- child directories of `/` become normal durable directory jobs;
- non-directory entries directly under `/` are collected into a persistent
  `ROOT_FILES` manifest;
- the manifest is split into argv-safe chunks;
- each chunk is tracked durably and resumed independently;
- completed chunks stay complete; stale running chunks are retried.

Example root crawl:

```bash
python3 ./backup_crawler.py / 4 \
  --state-db /var/lib/backup-crawler/root.sqlite3 \
  --new-run
```

Example non-root mount crawl:

```bash
python3 ./backup_crawler.py /srv/data 4 \
  --state-db /var/lib/backup-crawler/srv-data.sqlite3 \
  --new-run
```

## Dashboard / progress / final summary

The dashboard and final summary now include durable-state information such as:

- whether the execution is **NEW** or **RESUMED**,
- the state DB path and run UUID,
- recovered stale scan/backup claims,
- reused completed work,
- persisted scan status counts,
- persisted backup status counts,
- current execution attempt totals,
- persisted whole-run attempt totals,
- completion-invariant status.

Attempt/object/byte totals are derived from completed attempt summaries and may
include repeated inspections/transfers from resumed retries after ambiguous crash
windows. That duplication is expected under the at-least-once model.

## Operational notes

- The crawler preserves current IBM return-code success semantics: `rc <= 4` is
  treated as success.
- The crawler does **not** create a point-in-time filesystem snapshot. Files or
  directories created while a long crawl is running may be picked up only if
  discovered before completion. For strict snapshot semantics, use filesystem
  snapshots outside the crawler.
- Back up the SQLite DB with normal file-copy tools when the crawler is idle, or
  copy the DB together with its `-wal` and `-shm` sidecars.
- If the DB is corrupt or schema-incompatible, copy it aside for diagnosis
  before starting a new run. Do not delete it first.

## Testing

Repository tests live in `tests/test_backup_crawler.py`.

Typical commands:

```bash
python3 -m unittest discover tests -v
python3 -m pytest tests/ -v
```

In environments without `pytest`, the `unittest` command above is sufficient.
