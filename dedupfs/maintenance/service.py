from __future__ import annotations

from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.orm import Session, sessionmaker

from dedupfs.core.config import Settings
from dedupfs.db.models import WalCheckpointMode, WalMaintenanceJob, WalMaintenanceStatus
from dedupfs.maintenance.types import WalMaintenanceMetrics, WalMaintenanceSnapshot


class WalMaintenancePolicyError(RuntimeError):
    pass


class WalMaintenanceConflictError(RuntimeError):
    pass


class WalMaintenanceNotFoundError(RuntimeError):
    pass


class WalMaintenanceService:
    def __init__(self, settings: Settings, session_factory: sessionmaker[Session]):
        self._settings = settings
        self._session_factory = session_factory

    def _now(self) -> datetime:
        return datetime.now(tz=timezone.utc)

    def _coerce_utc(self, value: datetime | None) -> datetime | None:
        if value is None:
            return None
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value

    def _normalize_mode(self, raw_mode: str | None) -> WalCheckpointMode:
        if raw_mode is None:
            return WalCheckpointMode(self._settings.wal_checkpoint_default_mode)
        token = raw_mode.strip().lower()
        try:
            return WalCheckpointMode(token)
        except ValueError as exc:
            allowed = ", ".join(mode.value for mode in WalCheckpointMode)
            raise ValueError(f"Invalid WAL checkpoint mode: {raw_mode}. Allowed: {allowed}") from exc

    def _to_snapshot(self, row: WalMaintenanceJob) -> WalMaintenanceSnapshot:
        return WalMaintenanceSnapshot(
            id=row.id,
            requested_mode=row.requested_mode,
            status=row.status,
            requested_by=row.requested_by,
            reason=row.reason,
            execute_after=row.execute_after,
            retry_count=row.retry_count,
            retry_after=row.retry_after,
            worker_id=row.worker_id,
            worker_heartbeat_at=row.worker_heartbeat_at,
            lease_expires_at=row.lease_expires_at,
            checkpoint_busy=row.checkpoint_busy,
            checkpoint_log_frames=row.checkpoint_log_frames,
            checkpointed_frames=row.checkpointed_frames,
            error_code=row.error_code,
            error_message=row.error_message,
            created_at=row.created_at,
            updated_at=row.updated_at,
            started_at=row.started_at,
            finished_at=row.finished_at,
        )

    def request_checkpoint(
        self,
        *,
        mode: str | None = None,
        reason: str | None = None,
        requested_by: str | None = "api",
        force: bool = False,
    ) -> WalMaintenanceSnapshot:
        normalized_mode = self._normalize_mode(mode)
        if normalized_mode == WalCheckpointMode.TRUNCATE and not self._settings.wal_checkpoint_allow_truncate:
            raise WalMaintenancePolicyError("WAL truncate checkpoint is disabled by policy")

        now = self._now()
        with self._session_factory() as session:
            active = session.scalar(
                select(WalMaintenanceJob)
                .where(WalMaintenanceJob.status.in_([WalMaintenanceStatus.PENDING, WalMaintenanceStatus.RUNNING, WalMaintenanceStatus.RETRYABLE]))
                .order_by(WalMaintenanceJob.created_at.desc(), WalMaintenanceJob.id.desc())
                .limit(1)
            )
            if active is not None:
                return self._to_snapshot(active)

            if not force:
                latest_completed = session.scalar(
                    select(WalMaintenanceJob)
                    .where(WalMaintenanceJob.status == WalMaintenanceStatus.COMPLETED)
                    .order_by(WalMaintenanceJob.finished_at.desc(), WalMaintenanceJob.id.desc())
                    .limit(1)
                )
                if latest_completed is not None and latest_completed.finished_at is not None:
                    latest_finished_at = self._coerce_utc(latest_completed.finished_at)
                    assert latest_finished_at is not None
                    next_allowed = latest_finished_at + timedelta(seconds=self._settings.wal_checkpoint_min_interval_seconds)
                    if now < next_allowed:
                        wait_seconds = int((next_allowed - now).total_seconds())
                        raise WalMaintenanceConflictError(
                            f"WAL checkpoint is rate-limited by policy, retry after {wait_seconds} seconds"
                        )

            row = WalMaintenanceJob(
                requested_mode=normalized_mode,
                status=WalMaintenanceStatus.PENDING,
                requested_by=(requested_by or "api").strip()[:64] or "api",
                reason=reason,
                execute_after=now,
                retry_after=now,
                retry_count=0,
                created_at=now,
                updated_at=now,
            )
            session.add(row)
            session.commit()
            session.refresh(row)
            return self._to_snapshot(row)

    def get_latest(self) -> WalMaintenanceSnapshot:
        with self._session_factory() as session:
            row = session.scalar(
                select(WalMaintenanceJob)
                .order_by(WalMaintenanceJob.created_at.desc(), WalMaintenanceJob.id.desc())
                .limit(1)
            )
            if row is None:
                raise WalMaintenanceNotFoundError("No WAL maintenance jobs found")
            return self._to_snapshot(row)

    def get_metrics(self) -> WalMaintenanceMetrics:
        now = self._now()
        with self._session_factory() as session:
            counts = dict(
                session.execute(
                    select(WalMaintenanceJob.status, func.count())
                    .group_by(WalMaintenanceJob.status)
                ).all()
            )
            latest_completed_at = session.scalar(
                select(func.max(WalMaintenanceJob.finished_at)).where(
                    WalMaintenanceJob.status == WalMaintenanceStatus.COMPLETED
                )
            )

        return WalMaintenanceMetrics(
            generated_at=now,
            pending=int(counts.get(WalMaintenanceStatus.PENDING, 0)),
            running=int(counts.get(WalMaintenanceStatus.RUNNING, 0)),
            retryable=int(counts.get(WalMaintenanceStatus.RETRYABLE, 0)),
            failed=int(counts.get(WalMaintenanceStatus.FAILED, 0)),
            completed=int(counts.get(WalMaintenanceStatus.COMPLETED, 0)),
            latest_completed_at=self._coerce_utc(latest_completed_at),
        )


def wal_maintenance_snapshot_to_dict(snapshot: WalMaintenanceSnapshot) -> dict[str, Any]:
    payload = asdict(snapshot)
    payload["requested_mode"] = snapshot.requested_mode.value
    payload["status"] = snapshot.status.value
    return payload


def wal_maintenance_metrics_to_dict(snapshot: WalMaintenanceMetrics) -> dict[str, Any]:
    return asdict(snapshot)
