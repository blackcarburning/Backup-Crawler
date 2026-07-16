# Backup-Crawler

Parallel IBM Storage Protect (`dsmc`) crawler for one mounted filesystem.

## Scheduling model

1. A single **scanner** thread traverses the filesystem starting at the supplied
   mountpoint, staying on the same device (skipping nested mounts/bind-mounts).
   Every discovered directory is pushed to a bounded shared queue.
2. **N worker** threads pull directories from the queue in batches.  When a
   worker finishes its current batch it immediately loops back and requests
   another batch from the queue.  Workers exit only after the scanner is done
   *and* the queue is empty.
3. A **dashboard** (or periodic progress lines for non-TTY output) shows the
   live state of the scanner, queue, and every worker.

## Usage

```bash
# Using the positional shortcut
python3 ./backup_crawler.py /mountpoint 4

# Using the named option (equivalent)
python3 ./backup_crawler.py /mountpoint --streams 4 \
  --batch-size 20 \
  --queue-size 1000
```

Both forms set the number of parallel `dsmc` worker processes.  When both are
supplied, `--streams` takes precedence and the positional `WORKERS` value is
ignored.  If neither is given the default is **3**.

### Live dashboard

When stdout is a TTY, the script renders a live ASCII dashboard.  Example:

```
Scanner: RUNNING  found=342  q=85  in-prog=10  skipped=3  errors=0
Overall: completed=247  failed=0
--------------------------------------------------------------------------------
W01 [########----]  8/20 running          b#6    ok:76 fl:0   rc=0    2.3s  /data/subdir/a…
W02 [####--------]  4/20 running          b#5    ok:60 fl:1   rc=4    0.8s  /data/subdir/b…
W03               --/-- waiting_for_work  b#4    ok:40 fl:0                 
```

**Global header columns**

| Field | Meaning |
|---|---|
| `Scanner: RUNNING/DONE` | Whether the directory scanner is still traversing |
| `found=N` | Directories discovered so far |
| `q=N` | Approximate number waiting in the shared queue |
| `in-prog=N` | Directories dequeued by workers but not yet counted |
| `skipped=N` | Directories skipped due to filesystem boundaries |
| `errors=N` | Scan errors (e.g. permission denied during traversal) |
| `completed=N` | Directories successfully backed up (`rc ≤ 4`) |
| `failed=N` | Directories where `dsmc` returned `rc > 4` |
| `(X.X%)` | Percentage complete (shown once scanner finishes) |

**Per-worker columns**

| Field | Meaning |
|---|---|
| `W01` | Worker number |
| `[####----]` | Progress bar for current batch (shown while running) |
| `8/20` | Current directory index / total directories in batch |
| `running` / `waiting_for_work` / `idle` / `done` | Worker state |
| `b#N` | Global batch sequence number assigned to this worker |
| `ok:N fl:N` | Cumulative directories completed / failed by this worker |
| `rc=N` | Return code of the most recently finished `dsmc` invocation |
| `N.Ns` | Elapsed time for current directory; `~N.Ns` = batch elapsed when between dirs |
| path | Current directory being processed (truncated to fit terminal) |

Worker states:
- **`waiting_for_work`** – worker finished a batch and is blocking on the queue for the next one; if the scanner is still `RUNNING` it is waiting for more directories to be discovered; if the scanner is `DONE` all remaining queued work is being consumed.
- **`running`** – actively invoking `dsmc incremental` for a directory.
- **`idle`** – brief transient state immediately after a batch completes, before the worker re-enters the wait loop.
- **`done`** – worker has exited because the scanner finished and the queue is empty.

Control the dashboard with:

```bash
python3 ./backup_crawler.py /mountpoint \
  --dashboard-refresh-seconds 0.5
```

Disable the dashboard (or for non-interactive runs):

```bash
python3 ./backup_crawler.py /mountpoint --no-dashboard
```

In non-TTY contexts the script automatically falls back to plain periodic
progress lines controlled by `--progress-seconds`.

## Notes

- Filesystem traversal stays within the supplied mountpoint filesystem and skips nested mounts/filesystems.
- Queueing remains bounded and workers still reserve batches dynamically.
- Return codes `<= 4` are treated as success (matching `dsmc` convention).
- For accurate per-worker `X/Y` status, each worker runs one `dsmc incremental` per directory instead of one multi-operand invocation.
- Detailed controller and worker logs are written under `--log-dir`.
- `--dry-run` keeps scan/scheduling behavior without running `dsmc`.
- At shutdown, the controller logs a warning if `completed + failed ≠ discovered`.
- No SQLite database is used; all state is held in memory and written to the log files under `--log-dir`.
