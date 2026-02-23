# DedupFS Python ↔ Rust DB Protocol

[English](./PROTOCOL.md) | [简体中文](./PROTOCOL.zh-CN.md)

## 1. Scope

This document defines the authoritative, DB-first protocol shared by the Python control plane and the Rust data plane.

Protocol goals:
- deterministic behavior
- auditable state transitions
- migration-safe compatibility
- strict separation between policy (Python) and execution (Rust)

## 2. Enum Persistence Rule (Mandatory)

All enum-like fields persisted in SQLite must use **lowercase enum values** (not enum names).

Mandatory rule:
- Python ORM (`SAEnum`) must persist enum `.value` strings.
- Rust SQL must read/write the same lowercase values.
- Migration logic must normalize historical uppercase rows to lowercase before protocol-sensitive constraints are rebuilt.

## 3. Legal Value Sets

### 3.1 `jobs` table protocol values

| Field | Allowed values |
|---|---|
| `kind` | `scan`, `hash`, `delete`, `thumbnail` |
| `status` | `pending`, `running`, `completed`, `failed`, `cancelled`, `retryable` |

### 3.2 `scan_sessions` and `library_files`

| Table | Field | Allowed values |
|---|---|---|
| `scan_sessions` | `status` | `running`, `succeeded`, `failed` |
| `library_files` | `hash_algorithm` | `blake3`, `sha256` |

### 3.3 `thumbnails` and `thumbnail_cleanup_jobs`

| Table | Field | Allowed values |
|---|---|---|
| `thumbnails` | `status` | `pending`, `running`, `ready`, `failed` |
| `thumbnails` | `media_type` | `image`, `video` |
| `thumbnails` | `format` | `jpeg`, `webp` |
| `thumbnail_cleanup_jobs` | `status` | `pending`, `running`, `completed`, `failed` |

### 3.4 `wal_maintenance_jobs`

| Field | Allowed values |
|---|---|
| `requested_mode` | `passive`, `restart`, `truncate` |
| `status` | `pending`, `running`, `retryable`, `completed`, `failed` |

## 4. Lease / Heartbeat Semantics

### 4.1 Common lease fields

The following fields are lease contract fields:
- `worker_id`
- `worker_heartbeat_at`
- `lease_expires_at`

Lease stale condition:
- stale when `lease_expires_at IS NULL OR lease_expires_at <= now()` for a row expected to be running.

### 4.2 `jobs` lease semantics (scan/hash)

- Claim transition: `pending -> running` and assign lease owner fields.
- Heartbeat updates: lease owner refreshes `worker_heartbeat_at` and extends `lease_expires_at`.
- Finish transition: terminal status clears `lease_expires_at`.
- Recovery: Python control-plane recovery and Rust claim-path recovery both classify stale `running` scan/hash jobs to `retryable`, clear lease owner fields, and write deterministic recovery error metadata.

### 4.3 `thumbnails` lease semantics

- Python queue admission is atomic under queue-capacity policy (single DB conditional insert path).
- Rust claims one `pending` thumbnail row into `running` under lease.
- Rust refreshes lease while generating thumbnails.
- Rust claim path must requeue stale `running` rows whose lease is expired (`running -> pending`, clear lease owner fields).
- Finish success: `running -> ready` and clear lease expiry.
- Finish failure: `running -> failed`, persist `error_code/error_message`, persist `retry_after`, clear lease expiry.
- Retry behavior: Python can requeue a failed row to `pending` only after `retry_after` is reached.

### 4.4 `thumbnail_cleanup_jobs` lease semantics

- Python schedules cleanup as `pending` with `execute_after`.
- Rust claims due cleanup rows (`execute_after <= now`) into `running` under lease, but only when no `pending/running` thumbnail rows remain for the same `group_key`.
- Rust claim path must requeue stale `running` cleanup rows whose lease is expired (`running -> pending`, clear lease owner fields).
- Rust completes as `completed` or marks `failed` with error metadata.
- Cleanup execution is terminal-state safe: only `ready/failed` thumbnail rows are deleted.

### 4.5 `wal_maintenance_jobs` lease semantics

- Python enqueues WAL maintenance rows as `pending` with `execute_after`.
- Rust claims one due `pending/retryable` row into `running` under lease.
- Rust claim path must recover stale `running` rows (`running -> retryable`) with deterministic `error_code/error_message`, incremented `retry_count`, and `retry_after`.
- Rust execution uses `PRAGMA wal_checkpoint(mode)` and persists checkpoint stats:
  - `checkpoint_busy`
  - `checkpoint_log_frames`
  - `checkpointed_frames`
- Busy checkpoint (`checkpoint_busy > 0`) transitions to `retryable` with backoff (`retry_after`) and preserved audit fields.
- Success transitions to `completed`; execution errors transition to `failed`.

## 5. Single Active Scan/Hash Mutex

DB-level invariant:
- At most one active scan/hash row may be in `pending` or `running` at any time.

Implementation:
- Unique partial index `ix_jobs_single_active_scan_hash` on `jobs((1))` with predicate:
  - `lower(status) IN ('pending', 'running') AND lower(kind) IN ('scan', 'hash')`

Migration-safe recovery strategy:
- Before rebuilding this index, migration logic must:
  - normalize enum values to lowercase,
- detect duplicate running scan/hash rows,
- keep a deterministic winner,
- convert all other running duplicates to `retryable` with recovery error code,
- convert extra `pending` scan/hash rows to `retryable` when needed to satisfy pending/running single-active invariant,
- then rebuild the unique index.

## 6. Pagination Cursor Contracts

### 6.1 Job list pagination

Job list pagination uses a deterministic keyset cursor:
- order: `created_at DESC, id DESC`
- cursor anchor: DB row identified by the previous page tail `id`
- next-page filter:
  - `created_at < anchor.created_at`
  - OR (`created_at = anchor.created_at` AND `id < anchor.id`)

Stability guarantees:
- no duplicates across pages
- no gaps caused by non-unique timestamps
- deterministic traversal under high insertion rate

### 6.2 Duplicate-group pagination

Duplicate-group list endpoint (`GET /api/v1/duplicates/groups`) uses keyset pagination over grouped rows:
- source set: `library_files` rows with
  - `is_missing = 0`
  - `needs_hash = 0`
  - `hash_algorithm IS NOT NULL`
  - `content_hash IS NOT NULL`
- group by: `(hash_algorithm, content_hash)`
- include groups with `COUNT(*) > 1` only
- order: `file_count DESC, total_size_bytes DESC, hash_algorithm ASC, content_hash_hex ASC`
- cursor payload (base64url JSON):
  - `file_count`
  - `total_size_bytes`
  - `hash_algorithm`
  - `content_hash_hex`

Group key format:
- `group_key = <hash_algorithm>:<content_hash_hex>`
- `content_hash_hex` must match algorithm digest length:
  - `sha256`: 64 hex chars
  - `blake3`: 64 hex chars
- example: `sha256:4b227777d4dd1fc61c6f884f48641d02...`

Stability guarantees:
- no duplicates across pages for a stable snapshot
- deterministic order with tie-breakers on `(hash_algorithm, content_hash_hex)`

### 6.3 Duplicate-group file pagination

Duplicate-group file endpoint (`GET /api/v1/duplicates/groups/{group_key}/files`) uses keyset pagination:
- filter:
  - `is_missing = 0`
  - `needs_hash = 0`
  - `hash_algorithm = parsed(group_key.algorithm)`
  - `content_hash = parsed(group_key.hash_hex)`
- order: `id ASC`
- cursor: previous page tail `id` (string integer)

Migration safety note:
- migration `0011_duplicate_group_query_indexes` must ensure required `library_files` columns exist before writing migration version.
- when legacy schemas miss `is_missing/needs_hash/hash_algorithm/content_hash`, migration backfills columns first, then creates `ix_library_files_dedup_group`.

## 7. Rust Writable Field Whitelists

### 7.1 Scan/hash jobs (`jobs`)

- claim path: `status`, `worker_id`, `worker_heartbeat_at`, `lease_expires_at`, `started_at`, `updated_at`
- heartbeat path: `processed_items`, `progress`, `worker_heartbeat_at`, `lease_expires_at`, `updated_at`
- finish path: `status`, `progress`, `error_code`, `error_message`, `finished_at`, `worker_heartbeat_at`, `lease_expires_at`, `updated_at`

### 7.2 Thumbnail generation (`thumbnails`)

- claim path: `status`, `worker_id`, `worker_heartbeat_at`, `lease_expires_at`, `started_at`, `updated_at`
- heartbeat path: `worker_heartbeat_at`, `lease_expires_at`, `updated_at`
- finish success path: `status`, `width`, `height`, `bytes_size`, `error_code`, `error_message`, `error_count`, `retry_after`, `finished_at`, `worker_heartbeat_at`, `lease_expires_at`, `updated_at`
- finish failure path: `status`, `error_code`, `error_message`, `error_count`, `retry_after`, `finished_at`, `worker_heartbeat_at`, `lease_expires_at`, `updated_at`

### 7.3 Thumbnail cleanup (`thumbnail_cleanup_jobs`)

- claim path: `status`, `worker_id`, `worker_heartbeat_at`, `lease_expires_at`, `finished_at`, `updated_at`
- finish path: `status`, `error_code`, `error_message`, `finished_at`, `worker_heartbeat_at`, `lease_expires_at`, `updated_at`

### 7.4 Global I/O limiter (`io_rate_limits`)

- reserve path: `next_available_at_ms`, `updated_at` for known `bucket_key` rows
- bootstrap path: insert missing `bucket_key` row when absent
- current bucket key: `thumbnail_io_global`

### 7.5 WAL maintenance (`wal_maintenance_jobs`)

- claim path: `status`, `worker_id`, `worker_heartbeat_at`, `lease_expires_at`, `started_at`, `updated_at`, `finished_at`
- retryable recovery path: `status`, `retry_count`, `retry_after`, `worker_id`, `worker_heartbeat_at`, `lease_expires_at`, `error_code`, `error_message`, `finished_at`, `updated_at`
- success path: `status`, `checkpoint_busy`, `checkpoint_log_frames`, `checkpointed_frames`, `error_code`, `error_message`, `finished_at`, `worker_heartbeat_at`, `lease_expires_at`, `updated_at`
- retry path: `status`, `retry_count`, `retry_after`, `checkpoint_busy`, `checkpoint_log_frames`, `checkpointed_frames`, `error_code`, `error_message`, `finished_at`, `worker_heartbeat_at`, `lease_expires_at`, `updated_at`
- failure path: `status`, `error_code`, `error_message`, `finished_at`, `worker_heartbeat_at`, `lease_expires_at`, `updated_at`

Rust forbidden writes:
- policy-only fields outside the whitelists
- deletion authorization or dedup semantic policy fields
- any similarity/heuristic duplicate logic

## 8. Thumbnail Path and Safety Contract

- Source media paths must be validated under `/libraries/*` with relative-path validation + realpath/prefix checks.
- Thumbnail outputs must be written only under `/state/thumbs` (or configured equivalent under state root).
- Cleanup may delete only thumbnail cache files and thumbnail index rows.
- Cleanup must never mutate original media files under `/libraries`.
