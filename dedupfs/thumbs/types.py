from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from dedupfs.db.models import ThumbnailCleanupStatus, ThumbnailFormat, ThumbnailMediaType, ThumbnailStatus


@dataclass(frozen=True)
class ThumbnailSnapshot:
    id: int
    thumb_key: str
    file_id: int
    group_key: str | None
    status: ThumbnailStatus
    media_type: ThumbnailMediaType
    format: ThumbnailFormat
    max_dimension: int
    version: int
    source_size_bytes: int
    source_mtime_ns: int
    output_relpath: str | None
    width: int | None
    height: int | None
    bytes_size: int | None
    error_code: str | None
    error_message: str | None
    error_count: int
    retry_after: datetime | None
    worker_id: str | None
    worker_heartbeat_at: datetime | None
    lease_expires_at: datetime | None
    created_at: datetime
    updated_at: datetime
    started_at: datetime | None
    finished_at: datetime | None


@dataclass(frozen=True)
class ThumbnailCleanupSnapshot:
    id: int
    group_key: str
    status: ThumbnailCleanupStatus
    execute_after: datetime
    worker_id: str | None
    worker_heartbeat_at: datetime | None
    lease_expires_at: datetime | None
    error_code: str | None
    error_message: str | None
    created_at: datetime
    updated_at: datetime
    finished_at: datetime | None


@dataclass(frozen=True)
class ThumbnailMetricsSnapshot:
    generated_at: datetime
    queue_depth: int
    queue_pending: int
    queue_running: int
    retry_backlog: int
    retry_ready: int
    cleanup_pending: int
    cleanup_running: int
    cleanup_overdue: int
    cleanup_max_lag_seconds: int
