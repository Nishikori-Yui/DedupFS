from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from dedupfs.db.models import WalCheckpointMode, WalMaintenanceStatus


@dataclass(slots=True)
class WalMaintenanceSnapshot:
    id: int
    requested_mode: WalCheckpointMode
    status: WalMaintenanceStatus
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


@dataclass(slots=True)
class WalMaintenanceMetrics:
    generated_at: datetime
    pending: int
    running: int
    retryable: int
    failed: int
    completed: int
    latest_completed_at: datetime | None
