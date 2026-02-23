from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field


class WalCheckpointRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    mode: str | None = Field(default=None, max_length=16)
    reason: str | None = Field(default=None, max_length=2048)
    force: bool = False


class WalMaintenanceResponse(BaseModel):
    id: int
    requested_mode: str
    status: str
    requested_by: str | None
    reason: str | None
    execute_after: datetime
    retry_count: int
    retry_after: datetime | None
    worker_id: str | None
    worker_heartbeat_at: datetime | None
    lease_expires_at: datetime | None
    checkpoint_busy: int | None
    checkpoint_log_frames: int | None
    checkpointed_frames: int | None
    error_code: str | None
    error_message: str | None
    created_at: datetime
    updated_at: datetime
    started_at: datetime | None
    finished_at: datetime | None


class WalMaintenanceMetricsResponse(BaseModel):
    generated_at: datetime
    pending: int
    running: int
    retryable: int
    failed: int
    completed: int
    latest_completed_at: datetime | None
