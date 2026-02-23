from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field


class RequestThumbnailRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    file_id: int = Field(ge=1)
    max_dimension: int | None = Field(default=None, ge=1, le=4096)
    output_format: str | None = None


class ScheduleGroupCleanupRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    group_key: str = Field(min_length=1, max_length=256)
    delay_seconds: int | None = Field(default=None, ge=0, le=86400)


class ThumbnailResponse(BaseModel):
    id: int
    thumb_key: str
    file_id: int
    group_key: str | None
    status: str
    media_type: str
    format: str
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
    content_url: str | None = None


class ThumbnailCleanupResponse(BaseModel):
    id: int
    group_key: str
    status: str
    execute_after: datetime
    worker_id: str | None
    worker_heartbeat_at: datetime | None
    lease_expires_at: datetime | None
    error_code: str | None
    error_message: str | None
    created_at: datetime
    updated_at: datetime
    finished_at: datetime | None


class ThumbnailMetricsResponse(BaseModel):
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
