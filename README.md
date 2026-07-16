# Backup-Crawler

Parallel IBM Storage Protect (`dsmc`) crawler for one mounted filesystem.

## Usage

```bash
python3 ./backup_crawler.py /mountpoint \
  --streams 4 \
  --batch-size 20 \
  --queue-size 1000
```

### Live dashboard

When stdout is a TTY, the script renders a live ASCII dashboard with one row per worker:

- progress bar per worker
- current directory index within the reserved batch (`X/Y`)
- current directory path

Control it with:

```bash
python3 ./backup_crawler.py /mountpoint \
  --dashboard-refresh-seconds 0.5
```

Disable the dashboard (or for non-interactive runs):

```bash
python3 ./backup_crawler.py /mountpoint --no-dashboard
```

In non-TTY contexts, the script automatically falls back to plain periodic progress lines.

## Notes

- Filesystem traversal stays within the supplied mountpoint filesystem and skips nested mounts/filesystems.
- Queueing remains bounded and workers still reserve batches dynamically.
- Return codes `<= 4` are treated as success.
- For accurate per-worker `X/Y` status, each worker reserves a batch and executes one `dsmc incremental` per directory in that batch instead of one multi-operand invocation.
- Detailed controller and worker logs are written under `--log-dir`.
- `--dry-run` keeps scan/scheduling behavior without running `dsmc`.
