# Backup-Crawler

Parallel IBM Storage Protect (`dsmc`) crawler for one mounted filesystem.

## Scheduling model

1. A single **scanner** thread traverses the filesystem starting at the
   supplied mountpoint, staying on the same device (skipping nested
   mounts/bind-mounts).  Every discovered directory is pushed to a bounded
   shared queue.
2. **N worker** threads pull directories from the queue in batches.  Each
   worker runs one `dsmc incremental -subdir=no` per directory, streams
   combined stdout/stderr to a per-worker log file, and parses IBM summary
   statistics from the output.  When a worker finishes a batch it immediately
   loops back and requests another until the scanner is done *and* the queue
   is empty.
3. A **dashboard** (or periodic progress lines for non-TTY output) shows the
   live state of the scanner, queue, and every worker.

## Start-up latency note

Individual `dsmc` invocations have substantial session start-up latency —
commonly **10–20 seconds** — even for a single object.  This is expected
behaviour; do not interpret it as a hang.

**Recommendation**: start with **4 workers** and measure throughput before
increasing.  Very high worker counts can exhaust the SP server's session pool
and reduce overall throughput.

## Usage

```bash
# Positional shortcut (4 parallel workers)
python3 ./backup_crawler.py /mountpoint 4

# Named option (equivalent)
python3 ./backup_crawler.py /mountpoint --streams 4 \
  --batch-size 20 \
  --queue-size 1000
```

Both forms set the number of parallel `dsmc` worker processes.  When both are
supplied, `--streams` takes precedence.  Default is **3**.

### Key options

| Option | Default | Description |
|---|---|---|
| `--streams / WORKERS` | `3` | Parallel dsmc workers |
| `--batch-size` | `20` | Directories reserved by each worker at a time |
| `--queue-size` | `1000` | Bounded queue capacity |
| `--dsmc-timeout SECS` | `0` (disabled) | Hard wall-clock timeout per directory invocation |
| `--dsmc-idle-timeout SECS` | `0` (disabled) | Timeout when dsmc produces no output |
| `--exclude-path PATH` | — | Exclude a subtree (repeatable) |
| `--log-dir PATH` | `./sp-parallel-logs` | Controller + dsmc log directory |
| `--resourceutilization N` | `2` | dsmc resource-utilisation level (1–100) |
| `--no-dashboard` | off | Force plain progress output |
| `--progress-seconds N` | `30` | Interval for non-dashboard progress lines |
| `--dry-run` | off | Scan and schedule without running dsmc |

### Timeout options

`--dsmc-timeout SECONDS` sets a hard wall-clock timeout for a single `dsmc`
directory invocation.  A value of `0` (the default) disables it.

`--dsmc-idle-timeout SECONDS` sets an idle timeout: the invocation is killed if
dsmc produces no output for the configured duration.  A value of `0` disables it.

Because dsmc can legitimately stay silent while processing large files, both
timeouts default to `0`.  Enable them only if you observe genuine stalls, and
use conservative values (e.g. `--dsmc-timeout 1800`).

On timeout the child process group is sent SIGTERM then SIGKILL if it does
not exit within 2 s.  The directory is recorded in `failed-directories.tsv`
with synthetic return code `124` (hard timeout) or `125` (idle timeout).

### Path exclusions

The crawler automatically excludes `--log-dir` if it resolves inside the
crawl root (e.g. crawling `/tmp` with logs under `/tmp/sp-parallel-logs`).

Additional subtrees can be excluded with `--exclude-path PATH` (repeatable):

```bash
python3 ./backup_crawler.py /data 4 \
  --exclude-path /data/scratch \
  --exclude-path /data/cache
```

Exclusions use path-aware containment (not string prefix matching), so
`/data/scratch` does not accidentally exclude `/data/scratchfoo`.

A separate **excluded** counter tracks paths skipped by exclusion rules,
distinct from the mount-boundary skipped counter.

### Live dashboard

When stdout is a TTY, the script renders a live ASCII dashboard.  Example:

```
Scanner: RUNNING  found=342  q=85/1000  in-prog=10  excl=2  skipped=3  errors=0
Overall: completed=247  failed=0  (scanning, total growing)
dsmc:  insp=1  bkup=0  fail=0  retries=0  bytes_i=4.00 KB  bytes_x=0 B  children=4
--------------------------------------------------------------------------------
W01 [##########]  8/20 running         b#6   pid=5432   rt:2.3s    idle:0.1s    ok:76 to:0 fl:0  rc=0   /data/subdir/a…
W02 [####------]  4/20 quiet           b#5   pid=5431   rt:75.2s   idle:63.1s   ok:60 to:0 fl:1  rc=4   /data/subdir/b…
W03             --/-- waiting_for_work b#4                          idle:          ok:40 to:0 fl:0        
W04             --/-- done             b#3                          idle:          ok:20 to:0 fl:0        
```

**Global header rows**

| Field | Meaning |
|---|---|
| `Scanner: RUNNING/DONE` | Whether the directory scanner is still traversing |
| `found=N` | Directories discovered so far |
| `q=N/MAX` | Approximate queue depth / capacity |
| `[FULL]` | Queue is saturated (backpressure: scanner blocks until workers consume) |
| `in-prog=N` | Directories dequeued by workers but not yet counted |
| `excl=N` | Directories skipped by exclusion rules |
| `skipped=N` | Directories skipped due to filesystem mount boundaries |
| `errors=N` | Scan errors (e.g. permission denied during traversal) |
| `completed=N` | Directories successfully backed up (`rc ≤ 4`) |
| `failed=N` | Directories where `dsmc` returned `rc > 4` or timed out |
| `(X.X%)` | Percentage complete (only after scanner finishes) |
| `(scanning, total growing)` | Scanner still running; final total not yet known |
| `insp/bkup/fail/retries` | Aggregate dsmc object counts |
| `bytes_i/bytes_x` | Aggregate bytes inspected / transferred (human-readable) |
| `children=N` | Number of active dsmc child processes right now |
| `rate=X/s` | Effective aggregate throughput (bytes_transferred / total_elapsed) |

**Per-worker columns**

| Field | Meaning |
|---|---|
| `W01` | Worker number |
| `[####----]` | Progress bar for current batch |
| `8/20` | Current directory index / total in batch |
| `running` / `quiet` / `waiting_for_work` / etc. | Worker state |
| `b#N` | Global batch sequence number |
| `pid=N` | PID of the active `dsmc` child process |
| `rt:N.Ns` | Runtime of current invocation; `~N.Ns` = batch elapsed between dirs |
| `idle:N.Ns` | Seconds since dsmc last produced output |
| `ok:N to:N fl:N` | Cumulative completed / timed-out / failed by this worker |
| `rc=N` | Return code of the most recently finished invocation |
| path | Current directory (truncated to fit terminal) |

**Worker states**

| State | Meaning |
|---|---|
| `waiting_for_work` | Blocking on the queue for the next batch |
| `starting_dsmc` | Popen in progress; PID not yet assigned |
| `running` | dsmc is active and has recently produced output |
| `quiet` | dsmc is running but has produced no output for ≥ 60 s. This means no *recent output*, **not** that the process is hung (dsmc legitimately stays silent while hashing or transferring large files) |
| `finished_batch` | All directories in the batch are done; about to request the next one |
| `timed_out` | Last invocation was killed by a timeout |
| `done` | Worker has exited; scanner done and queue empty |
| `error` | Unexpected condition |

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

## Process supervision design

Each `dsmc` child is launched with:
- `stdin=subprocess.DEVNULL` — prevents it from blocking on interactive input.
- `preexec_fn=os.setsid` (POSIX) — puts the child in its own session / process
  group so that timeouts can terminate the entire subtree.

A dedicated **reader thread** drains the child's combined stdout+stderr,
appending raw bytes to the per-worker log and parsing IBM summary statistics
from each line.  The **supervision loop** in the worker thread monitors:

1. Hard wall-clock elapsed time (vs `--dsmc-timeout`).
2. Idle time since the last output byte (vs `--dsmc-idle-timeout`).
3. `stop_event` for graceful Ctrl-C shutdown.

On a timeout the process group receives SIGTERM, waits up to 2 s, then
SIGKILL.  The worker marks the directory failed and continues normally.

## Scheduling design

- The scanner thread enqueues directories without a time limit (`producer.join()`
  waits indefinitely).  Removing the previous fixed 30-second join timeout
  prevents premature shutdown during large crawls.
- `work_queue.join()` blocks until every enqueued directory has a matching
  `task_done()` call from a worker.  Workers always call `task_done()` in a
  `finally` block, even if the batch was interrupted.
- On Ctrl-C, `stop_event` is set, workers kill active children and exit after
  their current batch, and worker threads are joined with a configurable
  `--shutdown-wait-seconds` timeout.
- At the end a reconciliation check logs a warning if
  `completed + failed ≠ discovered`.

## Why the apparent hang occurred

When the crawler ran against `/tmp` with 30 workers:
1. The bounded queue filled (`q=1000`, backpressure), blocking the scanner.
2. Workers had claimed large batches (batch-size 20 × 30 workers = up to 600
   paths in-flight) but were waiting for dsmc to finish its ~19 s session
   start-up for each path.  With all 30 workers doing this simultaneously the
   dashboard showed `in-prog=264`, `status=running`, all on the first directory
   of their current batch.
3. Some workers were processing paths inside `/tmp/sp-parallel-logs` (the log
   directory itself), causing redundant work.  This is now prevented by the
   automatic log-directory exclusion.

## Notes

- Filesystem traversal stays within the supplied mountpoint and skips nested
  mounts / different-device filesystems.
- Return codes `≤ 4` are treated as success (matching `dsmc` convention).
- Detailed controller and worker logs are written under `--log-dir`.
- No SQLite database is used; all state is held in memory and written to log
  files.  The `failed-directories.tsv` file records every failed path with its
  return code.

## Running tests

```bash
pip install pytest
python3 -m pytest tests/ -v
```
