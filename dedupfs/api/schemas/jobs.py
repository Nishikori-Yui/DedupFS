from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from dedupfs.db.models import JobKind


class CreateJobRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    kind: JobKind
    payload: dict[str, Any] = Field(default_factory=dict)
    dry_run: bool | None = None


class ClaimJobRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    worker_id: str = Field(min_length=1, max_length=128)


class JobProgressRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    worker_id: str = Field(min_length=1, max_length=128)
    progress: float | None = Field(default=None, ge=0.0, le=1.0)
    processed_items: int | None = Field(default=None, ge=0)


class FinishJobRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    worker_id: str = Field(min_length=1, max_length=128)
    success: bool
    error_message: str | None = None


class CancelJobRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    error_message: str | None = None


class JobListResponse(BaseModel):
    items: list["JobResponse"]
    next_cursor: str | None


class JobResponse(BaseModel):
    id: str
    kind: str
    status: str
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
