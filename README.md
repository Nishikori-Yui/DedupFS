# DedupFS

[English](./README.md) | [简体中文](./README.zh-CN.md)

> WARNING
> This project has not been validated by real-world production testing.
> Do not deploy to production environments.

Deterministic NAS-scale deduplication system with Python control plane and Rust data plane.

![Python](https://img.shields.io/badge/Python-3.12%2B-3776AB?logo=python&logoColor=white)
![Rust](https://img.shields.io/badge/Rust-stable-000000?logo=rust)
![FastAPI](https://img.shields.io/badge/FastAPI-API-009688?logo=fastapi&logoColor=white)
![SQLite](https://img.shields.io/badge/SQLite-WAL-003B57?logo=sqlite&logoColor=white)
![Stage](https://img.shields.io/badge/Stage-Active%20Development-orange)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)

## Runtime architecture

- Python API:
  - policy and validation boundary,
  - job and thumbnail queue admission,
  - FSM ownership and stale-lease recovery,
  - atomic thumbnail queue backpressure admission.
- Rust worker:
  - scan/hash execution,
  - thumbnail generation and thumbnail cleanup execution,
  - lease heartbeat updates,
  - global thumbnail I/O budget reservation (`io_rate_limits`).
- Protocol:
  - DB-first contract only (`jobs`, `thumbnails`, `thumbnail_cleanup_jobs`, `library_files`).

## Run services

### 1) Start Python API

```bash
pip install -e .
DEDUPFS_LIBRARIES_ROOT=/libraries DEDUPFS_STATE_ROOT=/state uvicorn dedupfs.main:app --host 0.0.0.0 --port 8080
```

### 2) Start Rust worker

```bash
cd rust-worker
cargo run -- --worker-id rust-worker-1 --daemon
```

Daemon cycle order:
1. claim one scan/hash job if available,
2. else claim one thumbnail generation task,
3. else claim one thumbnail cleanup job,
4. else claim one WAL maintenance job,
5. if idle, apply bounded backoff (`DEDUPFS_RUST_WORKER_POLL_SECONDS` to `DEDUPFS_RUST_WORKER_MAX_POLL_SECONDS`) with jitter.

Single-shot mode is still available:

```bash
cargo run -- --worker-id rust-worker-1
```

Claim paths include stale-lease recovery:
- stale `running` scan/hash rows are reclassified to `retryable`,
- stale `running` thumbnail/cleanup rows are requeued to `pending`.

## Job API quick usage

Create scan/hash job:

```bash
curl -sS -X POST http://127.0.0.1:8080/api/v1/jobs \
  -H 'Content-Type: application/json' \
  -d '{"kind":"scan","payload":{}}'
```

## Duplicate group API quick usage

List duplicate groups with keyset pagination:

```bash
curl -sS "http://127.0.0.1:8080/api/v1/duplicates/groups?limit=100"
```

Fetch the next page with `next_cursor`:

```bash
curl -sS "http://127.0.0.1:8080/api/v1/duplicates/groups?limit=100&cursor=<next_cursor>"
```

List files for one duplicate group:

```bash
curl -sS "http://127.0.0.1:8080/api/v1/duplicates/groups/sha256:<hash_hex>/files?limit=200"
```

## Web UI

Open dashboard:

```bash
open http://127.0.0.1:8080/ui
```

Dashboard behavior:
- duplicate groups are rendered with keyset-backed virtualized list
- selecting a group loads group files page-by-page
- thumbnails are requested lazily (`/api/v1/thumbs/request`) and resolved asynchronously

Optional duplicate-query benchmark:

```bash
python scripts/benchmark_duplicates.py --state-root /tmp/dedupfs-bench --groups 5000 --files-per-group 2 --page-size 200 --explain
```

High-cardinality UI profile (duplicate-group paging + virtualization thresholds):

```bash
python scripts/profile_ui_duplicates.py \
  --state-root /tmp/dedupfs-ui-profile \
  --groups 20000 \
  --files-per-group 2 \
  --page-size-candidates 120,160,200 \
  --overscan-candidates 4,6,8 \
  --viewport-heights 720,900,1080 \
  --group-row-height 84 \
  --target-max-dom-rows 24 \
  --report-json /tmp/dedupfs-ui-profile/report.json
```

Thumbnail queue throughput/contention benchmark with regression thresholds:

```bash
python scripts/benchmark_thumbnail_queue.py \
  --state-root /tmp/dedupfs-thumb-bench \
  --files 5000 \
  --requests 12000 \
  --workers 12 \
  --hot-file-count 800 \
  --queue-capacity 50000 \
  --min-throughput-rps 250 \
  --max-failed-ratio 0.001 \
  --max-queue-full-ratio 0.01 \
  --min-dedupe-ratio 0.85
```

## Thumbnail API quick usage

Request an on-demand thumbnail (non-blocking queue admission):

```bash
curl -sS -X POST http://127.0.0.1:8080/api/v1/thumbs/request \
  -H 'Content-Type: application/json' \
  -d '{"file_id": 1, "max_dimension": 256, "output_format": "jpeg"}'
```

Query thumbnail status:

```bash
curl -sS http://127.0.0.1:8080/api/v1/thumbs/<thumb_key>
```

Fetch thumbnail content when status is `ready`:

```bash
curl -sS http://127.0.0.1:8080/api/v1/thumbs/<thumb_key>/content -o thumb.jpg
```

Schedule group cleanup:

```bash
curl -sS -X POST http://127.0.0.1:8080/api/v1/thumbs/cleanup/group \
  -H 'Content-Type: application/json' \
  -d '{"group_key":"sha256:...", "delay_seconds": 600}'
```

Get thumbnail queue and cleanup lag metrics:

```bash
curl -sS http://127.0.0.1:8080/api/v1/thumbs/metrics
```

## WAL maintenance API quick usage

Request a WAL checkpoint job (policy admission only, worker executes asynchronously):

```bash
curl -sS -X POST http://127.0.0.1:8080/api/v1/maintenance/wal/checkpoint \
  -H 'Content-Type: application/json' \
  -d '{"mode":"passive","reason":"manual-maintenance"}'
```

Get latest WAL maintenance job:

```bash
curl -sS http://127.0.0.1:8080/api/v1/maintenance/wal/checkpoint/latest
```

Get WAL maintenance metrics:

```bash
curl -sS http://127.0.0.1:8080/api/v1/maintenance/wal/metrics
```

## Thumbnail safety and cache

- Source files are validated under `/libraries/*` with path traversal rejection and realpath prefix checks.
- Thumbnail cache is written only under `/state/thumbs` (or configured state-root thumbs path).
- Cleanup removes only terminal (`ready/failed`) thumbnail cache files and thumbnail DB rows.
- Thumbnails never affect deduplication or deletion decisions.

## Key thumbnail configs

- `DEDUPFS_THUMBNAIL_MAX_DIMENSION`
- `DEDUPFS_THUMBNAIL_IMAGE_CONCURRENCY`
- `DEDUPFS_THUMBNAIL_VIDEO_CONCURRENCY`
- `DEDUPFS_THUMBNAIL_QUEUE_CAPACITY`
- `DEDUPFS_THUMBNAIL_IO_RATE_LIMIT_MIB_PER_SEC`
- `DEDUPFS_THUMBNAIL_RETRY_BASE_SECONDS`
- `DEDUPFS_THUMBNAIL_RETRY_MAX_SECONDS`
- `DEDUPFS_THUMBNAIL_FFMPEG_BIN`
- `DEDUPFS_THUMBNAIL_FFMPEG_TIMEOUT_SECONDS`
- `DEDUPFS_RUST_WORKER_POLL_SECONDS`
- `DEDUPFS_RUST_WORKER_MAX_POLL_SECONDS`
- `DEDUPFS_RUST_WORKER_POLL_JITTER_MILLIS`
- `DEDUPFS_WAL_CHECKPOINT_DEFAULT_MODE`
- `DEDUPFS_WAL_CHECKPOINT_MIN_INTERVAL_SECONDS`
- `DEDUPFS_WAL_CHECKPOINT_ALLOW_TRUNCATE`
- `DEDUPFS_WAL_CHECKPOINT_RETRY_SECONDS`

## Optional containerized thumbnail e2e harness

Run only when Docker is available:

```bash
DEDUPFS_RUN_CONTAINER_E2E=1 pytest -q tests/test_thumbnail_worker_container_e2e.py
```

## Protocol specification

See `docs/PROTOCOL.md` for legal value sets, lease invariants, mutex guarantees, write whitelists, and pagination cursor contract.

## License

Apache License 2.0. See `LICENSE`.
