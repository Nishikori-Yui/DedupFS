from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from dedupfs.db.models import JobKind, JobStatus



@dataclass(slots=True)
class JobSnapshot:
    id: str
    kind: JobKind
    status: JobStatus
    dry_run: bool
    worker_id: str | None
    worker_heartbeat_at: datetime | None
    lease_expires_at: datetime | None
    progress: float
    total_items: int | None
    processed_items: int
    payload: dict[str, Any]
    error_code: str | None
    error_message: str | None
    created_at: datetime
    updated_at: datetime
    started_at: datetime | None
    finished_at: datetime | None
