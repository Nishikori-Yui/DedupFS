from __future__ import annotations

from datetime import datetime, timedelta, timezone

from sqlalchemy import delete, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from dedupfs.core.config import Settings
from dedupfs.db.models import JobLock


class JobLockService:
    def __init__(self, settings: Settings):
        self._settings = settings

    def _now(self) -> datetime:
        return datetime.now(tz=timezone.utc)

    def acquire(self, session: Session, lock_key: str, owner_job_id: str) -> bool:
        now = self._now()
        expires_at = now + timedelta(seconds=self._settings.job_lock_ttl_seconds)

        session.execute(
            delete(JobLock).where(
                JobLock.lock_key == lock_key,
                JobLock.expires_at <= now,
            )
        )

        lock = JobLock(
            lock_key=lock_key,
            owner_job_id=owner_job_id,
            acquired_at=now,
            heartbeat_at=now,
            expires_at=expires_at,
        )
        session.add(lock)

        try:
            session.flush()
            return True
        except IntegrityError:
            session.rollback()
            return False

    def refresh(self, session: Session, lock_key: str, owner_job_id: str) -> bool:
        now = self._now()
        expires_at = now + timedelta(seconds=self._settings.job_lock_ttl_seconds)

        lock = session.scalar(
            select(JobLock).where(
                JobLock.lock_key == lock_key,
                JobLock.owner_job_id == owner_job_id,
            )
        )
        if lock is None:
            return False

        lock.heartbeat_at = now
        lock.expires_at = expires_at
        session.flush()
        return True

    def release(self, session: Session, lock_key: str, owner_job_id: str) -> None:
        session.execute(
            delete(JobLock).where(
                JobLock.lock_key == lock_key,
                JobLock.owner_job_id == owner_job_id,
            )
        )

    def is_owned_and_alive(self, session: Session, lock_key: str, owner_job_id: str) -> bool:
        now = self._now()
        lock = session.scalar(
            select(JobLock).where(
                JobLock.lock_key == lock_key,
                JobLock.owner_job_id == owner_job_id,
            )
        )
        if lock is None:
            return False
        return lock.expires_at > now

    def cleanup_expired(self, session: Session) -> int:
        now = self._now()
        result = session.execute(delete(JobLock).where(JobLock.expires_at <= now))
        return int(result.rowcount or 0)
