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

## Special handling of the filesystem root (/)

When the crawl entry point is exactly `/`, the script uses `/` as a
**traversal anchor only** and never submits it to `dsmc` as an ordinary
directory job.  Passing `/` to `dsmc incremental` can be interpreted by IBM
Storage Protect as a full filesystem/volume backup, which may take orders of
magnitude longer than a per-directory invocation.

### What happens instead

| Component | Behaviour |
|---|---|
| **Scanner** | Scans `/` for child directories, then traverses them recursively as normal.  `/` itself is **not** enqueued in the work queue. |
| **ROOT_FILES job** | A dedicated background thread (dashboard row `ROOT_FILES`, worker slot 0) collects every non-directory entry directly under `/` and invokes `dsmc incremental` with those entries as **explicit file operands**. |
| **Regular workers** | Receive all child directories of `/` through the normal dynamic queue, as if the entry point had been any other directory. |

### ROOT_FILES command strategy

The job builds commands like:

```bash
dsmc incremental -resourceutilization=2 /etc.conf /initrd.img /vmlinuz …
```

Explicit file operands tell `dsmc` to back up only those exact entries.  There
is no `-subdir=no` flag because no directory operand is passed; recursion is
not possible.

If the combined length of the file-path arguments would exceed 128 KB (well
below the Linux `ARG_MAX`), the list is automatically split into multiple
chunks.  Each chunk is a separate `dsmc` invocation tracked individually in
the dashboard and logs.

### Symlink policy

Symlinks directly under `/` are included in the ROOT_FILES job because
`DirEntry.is_dir(follow_symlinks=False)` returns `False` for all symlinks
(even those pointing at directories).  They are backed up as **link objects**,
not descended into.  This is consistent with `dsmc`'s default symlink handling.

### No eligible files

If there are no non-directory entries directly under `/`, the ROOT_FILES job
logs a skip reason and exits without invoking `dsmc`.

### Dashboard row

The `ROOT_FILES` row appears at the top of the worker section:

```
ROOT_FILES [##########]  1/1  running  b#1  pid=12345  rt:2.3s  idle:0.1s  ok:0 to:0 fl:0  rc=    /file1 /file2 (1 of N chunks)
```

| Field | Meaning |
|---|---|
| `ROOT_FILES` | Label for the special job (slot 0, never confused with a regular worker) |
| `N/M` | Current chunk / total chunks |
| `ok:N` | Successfully completed chunks |
| `fl:N` | Failed chunks |
| `to:N` | Timed-out chunks |

The ROOT_FILES row disappears from `status=running` once the job finishes, and
the responsible resources are fully released back to the runtime.

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
Backup totals: done=247  parsed=247/247  incomplete=0  children=4
Objects: inspected=1,234  backed_up=56  updated=3  failed=0  retries=0
Data: processed=18.00 MiB  sent=3.25 MiB
--------------------------------------------------------------------------------
ROOT_FILES [##########]  1/1  running  b#1  pid=9990   rt:5.1s    idle:0.2s    ok:0 to:0 fl:0  rc=    3 files, 1 chunk(s)
W01        [##########]  8/20 running  b#6  pid=5432   rt:2.3s    idle:0.1s    ok:76 to:0 fl:0  rc=0   /data/subdir/a…
W02        [####------]  4/20 quiet   b#5  pid=5431   rt:75.2s   idle:63.1s   ok:60 to:0 fl:1  rc=4   /data/subdir/b…
W03                    --/-- waiting_for_work b#4                 idle:          ok:40 to:0 fl:0        
W04                    --/-- done     b#3                         idle:          ok:20 to:0 fl:0        
```

**Backup totals block** (above separator and worker rows)

| Field | Meaning |
|---|---|
| `done=N` | Total completed `dsmc` invocations (includes failed/timed-out processes) |
| `parsed=N/N` | Invocations with parsed summary / total invocations done |
| `incomplete=N` | Invocations where the summary was absent or unparseable |
| `children=N` | Active `dsmc` child processes right now |
| `inspected=N` | Cumulative "Total number of objects inspected" from parsed summaries |
| `backed_up=N` | Cumulative **"Total number of objects backed up"** — the primary file-count metric |
| `updated=N` | Cumulative objects updated (re-sent) |
| `failed=N` | Cumulative objects that dsmc failed to back up |
| `retries=N` | Cumulative retries |
| `processed=X` | Cumulative "Total number of bytes inspected" — data scanned/processed |
| `sent=X` | Cumulative "Total number of bytes transferred" — data actually sent to the server |
| `rate=X/s` | Effective aggregate throughput (only when data was transferred) |

> **Terminology note**
> - **`backed_up`** = dsmc "Total number of objects backed up" — new objects sent on this run.
> - **`updated`** = dsmc "Total number of objects updated" — objects re-sent due to changes.
> - **`processed`** = dsmc "Total number of bytes inspected" — all bytes scanned.
> - **`sent`** = dsmc "Total number of bytes transferred" — bytes actually sent.
> - These are separate from the **directory** counters (`completed`, `failed`), which count how many directory invocations finished, not how many files they contained.

**Totals update after each completed process** (not continuously during streaming), because dsmc emits its summary block only at exit.

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
