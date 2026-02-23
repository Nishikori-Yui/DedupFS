# DedupFS Rust Worker

High-throughput background worker for scan/hash execution in DedupFS hybrid architecture.

## Scope

- Executes `scan` and `hash` jobs as the only runtime engine for those job kinds
- Reads/writes the shared SQLite schema used by Python API service
- Enforces `/libraries` path whitelist and relative-path safety checks
- Does **not** implement deletion, trash move, or any destructive filesystem operation
- Does **not** implement similarity/heuristic deduplication

## Run

```bash
cd rust-worker
cargo run -- --job-id <job-id>
```

Or auto-pick the oldest runnable Rust scan/hash job:

```bash
cd rust-worker
cargo run
```

## Config

Configuration can be provided via:

1. `--config /path/to/worker.toml`
2. Environment variables (`DEDUPFS_*`, `DEDUPFS_RUST_WORKER_*`)
3. Built-in defaults

See `worker.example.toml` for all fields.
