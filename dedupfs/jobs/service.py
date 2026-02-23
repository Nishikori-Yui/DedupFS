from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import uuid4

from sqlalchemy import and_, or_, select, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, sessionmaker

from dedupfs.core.config import Settings
from dedupfs.db.models import Job, JobKind, JobStatus
from dedupfs.jobs.types import JobSnapshot


class JobConflictError(RuntimeError):
    pass


class JobNotFoundError(RuntimeError):
    pass


class InvalidJobStateError(RuntimeError):
    pass


class JobPolicyError(RuntimeError):
    pass


@dataclass(frozen=True)
class JobListResult:
    items: list[JobSnapshot]
    next_cursor: str | None


ALLOWED_TRANSITIONS: dict[JobStatus, set[JobStatus]] = {
    JobStatus.PENDING: {JobStatus.RUNNING, JobStatus.CANCELLED},
    JobStatus.RUNNING: {JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED, JobStatus.RETRYABLE},
    JobStatus.RETRYABLE: {JobStatus.PENDING, JobStatus.CANCELLED, JobStatus.FAILED},
    JobStatus.COMPLETED: set(),
    JobStatus.FAILED: set(),
    JobStatus.CANCELLED: set(),
}


class JobService:
    def __init__(self, settings: Settings, session_factory: sessionmaker[Session]):
        self._settings = settings
        self._session_factory = session_factory

    def _now(self) -> datetime:
        return datetime.now(tz=timezone.utc)

    def _lease_delta(self) -> timedelta:
        return timedelta(seconds=self._settings.job_lock_ttl_seconds)

    def _coerce_utc(self, value: datetime | None) -> datetime | None:
        if value is None:
            return None
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value

    def _requires_scan_hash_mutex(self, kind: JobKind) -> bool:
        return kind in {JobKind.SCAN, JobKind.HASH}

    def _enforce_job_policy(self, kind: JobKind, dry_run: bool) -> None:
        if self._settings.dry_run and not dry_run:
            raise JobPolicyError("Global dry-run mode forbids real-run jobs")
        if kind == JobKind.DELETE and not dry_run and not self._settings.allow_real_delete:
            raise JobPolicyError("Real delete is disabled by configuration")

    def _enforce_transition(self, from_status: JobStatus, to_status: JobStatus) -> None:
        if to_status not in ALLOWED_TRANSITIONS[from_status]:
            raise InvalidJobStateError(f"Illegal transition: {from_status.value} -> {to_status.value}")

    def create_job(
        self,
        kind: JobKind,
        payload: dict[str, Any] | None = None,
        dry_run: bool | None = None,
    ) -> JobSnapshot:
        job_id = str(uuid4())
        effective_dry_run = self._settings.dry_run if dry_run is None else dry_run
        self._enforce_job_policy(kind=kind, dry_run=effective_dry_run)
        with self._session_factory() as session:
            if self._requires_scan_hash_mutex(kind):
                self.recover_stale_jobs(session=session)
                active = session.scalar(
                    select(Job.id).where(
                        Job.kind.in_([JobKind.SCAN, JobKind.HASH]),
                        Job.status.in_([JobStatus.PENDING, JobStatus.RUNNING, JobStatus.RETRYABLE]),
                    )
                )
                if active is not None:
                    raise JobConflictError("A scan/hash job is already active")

            job = Job(
                id=job_id,
                kind=kind,
                status=JobStatus.PENDING,
                dry_run=effective_dry_run,
                payload=payload or {},
            )
            session.add(job)
            try:
                session.commit()
            except IntegrityError as exc:
                session.rollback()
                if self._requires_scan_hash_mutex(kind):
                    raise JobConflictError("A scan/hash job is already active") from exc
                raise
            session.refresh(job)
            return self._to_snapshot(job)

    def get_job(self, job_id: str) -> JobSnapshot:
        with self._session_factory() as session:
            self.recover_stale_jobs(session=session)
            job = session.get(Job, job_id)
            if job is None:
                raise JobNotFoundError(f"Job not found: {job_id}")
            return self._to_snapshot(job)

    def list_jobs(self, *, limit: int = 50, cursor: str | None = None) -> JobListResult:
        bounded_limit = max(1, min(limit, 200))
        with self._session_factory() as session:
            self.recover_stale_jobs(session=session)
            stmt = select(Job).order_by(Job.created_at.desc(), Job.id.desc()).limit(bounded_limit + 1)
            if cursor:
                anchor_exists = session.scalar(select(Job.id).where(Job.id == cursor))
                if anchor_exists is None:
                    raise ValueError(f"Invalid pagination cursor: {cursor}")
                anchor_created_at = select(Job.created_at).where(Job.id == cursor).scalar_subquery()
                stmt = stmt.where(
                    or_(
                        Job.created_at < anchor_created_at,
                        and_(Job.created_at == anchor_created_at, Job.id < cursor),
                    )
                )
            rows = list(session.scalars(stmt).all())
            items = rows[:bounded_limit]
            next_cursor = items[-1].id if len(rows) > bounded_limit and items else None
            return JobListResult(items=[self._to_snapshot(row) for row in items], next_cursor=next_cursor)

    def claim_pending_scan_hash_job(self, worker_id: str) -> JobSnapshot | None:
        normalized_worker_id = worker_id.strip()
        if not normalized_worker_id:
            raise ValueError("worker_id cannot be blank")

        with self._session_factory() as session:
            self.recover_stale_jobs(session=session)
            now = self._now()
            lease_expires_at = now + self._lease_delta()
            try:
                claim_result = session.execute(
                    text(
                        """
                        WITH candidate AS (
                            SELECT id
                            FROM jobs
                            WHERE kind IN ('scan', 'hash')
                              AND status = 'pending'
                            ORDER BY created_at ASC, id ASC
                            LIMIT 1
                        )
                        UPDATE jobs
                        SET status = 'running',
                            started_at = COALESCE(started_at, :now),
                            worker_id = :worker_id,
                            worker_heartbeat_at = :now,
                            lease_expires_at = :lease_expires_at,
                            updated_at = :now
                        WHERE id IN (SELECT id FROM candidate)
                          AND status = 'pending'
                        RETURNING id
                        """
                    ),
                    {
                        "now": now,
                        "worker_id": normalized_worker_id,
                        "lease_expires_at": lease_expires_at,
                    },
                )
                claimed_id = claim_result.scalar_one_or_none()
                if claimed_id is None:
                    session.rollback()
                    return None
                session.commit()
            except IntegrityError as exc:
                session.rollback()
                raise JobConflictError("Failed to claim scan/hash job due to global mutex contention") from exc

            claimed_job = session.get(Job, claimed_id)
            if claimed_job is None:
                raise JobConflictError("Claimed job disappeared before snapshot fetch")
            return self._to_snapshot(claimed_job)

    def heartbeat(
        self,
        job_id: str,
        worker_id: str,
        progress: float | None = None,
        processed_items: int | None = None,
    ) -> JobSnapshot:
        normalized_worker_id = worker_id.strip()
        if not normalized_worker_id:
            raise ValueError("worker_id cannot be blank")

        with self._session_factory() as session:
            job = session.get(Job, job_id)
            if job is None:
                raise JobNotFoundError(f"Job not found: {job_id}")
            if job.status != JobStatus.RUNNING:
                raise InvalidJobStateError(f"Job {job_id} is not running")
            now = self._now()
            lease_expires_at = self._coerce_utc(job.lease_expires_at)
            if lease_expires_at is None or lease_expires_at <= now:
                self._enforce_transition(job.status, JobStatus.RETRYABLE)
                job.status = JobStatus.RETRYABLE
                job.error_code = "LEASE_EXPIRED"
                job.error_message = "Lease expired before heartbeat"
                job.finished_at = now
                job.updated_at = now
                job.worker_id = None
                job.worker_heartbeat_at = None
                job.lease_expires_at = None
                session.commit()
                session.refresh(job)
                raise JobConflictError("Lease expired")
            if job.worker_id is not None and job.worker_id != normalized_worker_id:
                raise JobConflictError("Job is already bound to a different worker")

            if progress is not None:
                if progress < 0.0 or progress > 1.0:
                    raise ValueError("Progress must be in [0.0, 1.0]")
                job.progress = progress
            if processed_items is not None:
                if processed_items < 0:
                    raise ValueError("processed_items must be >= 0")
                job.processed_items = processed_items
            job.worker_id = normalized_worker_id
            job.worker_heartbeat_at = now
            job.lease_expires_at = now + self._lease_delta()
            job.updated_at = now
            session.commit()
            session.refresh(job)
            return self._to_snapshot(job)

    def finish_job(self, job_id: str, *, worker_id: str, success: bool, error_message: str | None = None) -> JobSnapshot:
        normalized_worker_id = worker_id.strip()
        if not normalized_worker_id:
            raise ValueError("worker_id cannot be blank")

        with self._session_factory() as session:
            job = session.get(Job, job_id)
            if job is None:
                raise JobNotFoundError(f"Job not found: {job_id}")
            if job.status != JobStatus.RUNNING:
                raise InvalidJobStateError(f"Job {job_id} is not running")
            if job.worker_id != normalized_worker_id:
                raise JobConflictError("Only current lease owner can finish the job")
            next_status = JobStatus.COMPLETED if success else JobStatus.FAILED
            self._enforce_transition(job.status, next_status)
            now = self._now()
            job.status = next_status
            job.progress = 1.0 if success else job.progress
            job.error_message = error_message
            job.error_code = None if success else "WORKER_FAILURE"
            job.finished_at = now
            job.worker_heartbeat_at = now
            job.lease_expires_at = None
            job.updated_at = now
            session.commit()
            session.refresh(job)
            return self._to_snapshot(job)

    def reset_retryable_job(self, job_id: str) -> JobSnapshot:
        with self._session_factory() as session:
            job = session.get(Job, job_id)
            if job is None:
                raise JobNotFoundError(f"Job not found: {job_id}")
            self._enforce_transition(job.status, JobStatus.PENDING)
            now = self._now()
            job.status = JobStatus.PENDING
            job.worker_id = None
            job.worker_heartbeat_at = None
            job.lease_expires_at = None
            job.error_code = None
            job.error_message = None
            job.finished_at = None
            job.updated_at = now
            session.commit()
            session.refresh(job)
            return self._to_snapshot(job)

    def cancel_job(self, job_id: str, error_message: str | None = None) -> JobSnapshot:
        with self._session_factory() as session:
            job = session.get(Job, job_id)
            if job is None:
                raise JobNotFoundError(f"Job not found: {job_id}")
            target = JobStatus.CANCELLED
            self._enforce_transition(job.status, target)
            now = self._now()
            job.status = target
            job.finished_at = now
            job.updated_at = now
            job.lease_expires_at = None
            if error_message:
                job.error_message = error_message
            session.commit()
            session.refresh(job)
            return self._to_snapshot(job)

    def recover_stale_jobs(self, *, session: Session | None = None) -> int:
        owns_session = session is None
        local_session = session or self._session_factory()
        now = self._now()
        stale_jobs = list(
            local_session.scalars(
                select(Job).where(
                    Job.status == JobStatus.RUNNING,
                    Job.kind.in_([JobKind.SCAN, JobKind.HASH]),
                    or_(Job.lease_expires_at.is_(None), Job.lease_expires_at <= now),
                )
            ).all()
        )
        for job in stale_jobs:
            self._enforce_transition(job.status, JobStatus.RETRYABLE)
            job.status = JobStatus.RETRYABLE
            job.error_code = "LEASE_EXPIRED"
            job.error_message = "Lease expired and recovered by control plane"
            job.finished_at = now
            job.updated_at = now
            job.worker_id = None
            job.worker_heartbeat_at = None
            job.lease_expires_at = None
        if stale_jobs and owns_session:
            local_session.commit()
        elif stale_jobs:
            local_session.flush()
        if owns_session:
            local_session.close()
        return len(stale_jobs)

    def _to_snapshot(self, job: Job) -> JobSnapshot:
        return JobSnapshot(
            id=job.id,
            kind=job.kind,
            status=job.status,
            dry_run=job.dry_run,
            worker_id=job.worker_id,
            worker_heartbeat_at=job.worker_heartbeat_at,
            lease_expires_at=job.lease_expires_at,
            progress=job.progress,
            total_items=job.total_items,
            processed_items=job.processed_items,
            payload=job.payload,
            error_code=job.error_code,
            error_message=job.error_message,
            created_at=job.created_at,
            updated_at=job.updated_at,
            started_at=job.started_at,
            finished_at=job.finished_at,
        )


def snapshot_to_dict(snapshot: JobSnapshot) -> dict[str, Any]:
    return {
        "id": snapshot.id,
        "kind": snapshot.kind.value,
        "status": snapshot.status.value,
        "dry_run": snapshot.dry_run,
        "worker_id": snapshot.worker_id,
        "worker_heartbeat_at": snapshot.worker_heartbeat_at,
        "lease_expires_at": snapshot.lease_expires_at,
        "progress": snapshot.progress,
        "total_items": snapshot.total_items,
        "processed_items": snapshot.processed_items,
        "payload": snapshot.payload,
        "error_code": snapshot.error_code,
        "error_message": snapshot.error_message,
        "created_at": snapshot.created_at,
        "updated_at": snapshot.updated_at,
        "started_at": snapshot.started_at,
        "finished_at": snapshot.finished_at,
    }
