from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

import dedupfs.db.session as db_session_module
from fastapi.testclient import TestClient

from dedupfs.api.app import create_app
from dedupfs.core.config import get_settings
from dedupfs.db.init_db import initialize_database
from dedupfs.db.models import WalCheckpointMode, WalMaintenanceJob, WalMaintenanceStatus
from dedupfs.maintenance.service import (
    WalMaintenanceConflictError,
    WalMaintenancePolicyError,
    WalMaintenanceService,
)


def make_wal_service(
    tmp_path: Path,
    *,
    min_interval_seconds: int = 900,
    allow_truncate: bool = False,
    default_mode: str = "passive",
    state_suffix: str = "default",
) -> WalMaintenanceService:
    state_root = tmp_path / f"state_{state_suffix}"
    state_root.mkdir(parents=True, exist_ok=True)

    os.environ["DEDUPFS_LIBRARIES_ROOT"] = "/libraries"
    os.environ["DEDUPFS_STATE_ROOT"] = state_root.as_posix()
    os.environ["DEDUPFS_DRY_RUN"] = "true"
    os.environ["DEDUPFS_ALLOW_REAL_DELETE"] = "false"
    os.environ["DEDUPFS_WAL_CHECKPOINT_MIN_INTERVAL_SECONDS"] = str(min_interval_seconds)
    os.environ["DEDUPFS_WAL_CHECKPOINT_ALLOW_TRUNCATE"] = "true" if allow_truncate else "false"
    os.environ["DEDUPFS_WAL_CHECKPOINT_DEFAULT_MODE"] = default_mode

    get_settings.cache_clear()
    db_session_module._engine = None
    db_session_module._session_factory = None
    initialize_database()
    return WalMaintenanceService(get_settings(), db_session_module.get_session_factory())


def test_request_checkpoint_creates_pending_row_with_default_mode(tmp_path: Path) -> None:
    service = make_wal_service(tmp_path)

    snapshot = service.request_checkpoint(reason="manual-checkpoint")

    assert snapshot.id > 0
    assert snapshot.requested_mode == WalCheckpointMode.PASSIVE
    assert snapshot.status == WalMaintenanceStatus.PENDING
    assert snapshot.retry_count == 0
    assert snapshot.worker_id is None

    with db_session_module.get_session_factory()() as session:
        rows = session.query(WalMaintenanceJob).all()
    assert len(rows) == 1
    assert rows[0].status == WalMaintenanceStatus.PENDING


def test_request_checkpoint_returns_existing_active_job_without_duplicate_insert(tmp_path: Path) -> None:
    service = make_wal_service(tmp_path)

    first = service.request_checkpoint(mode="restart", reason="first")
    second = service.request_checkpoint(mode="passive", reason="second")

    assert second.id == first.id
    assert second.status == WalMaintenanceStatus.PENDING

    with db_session_module.get_session_factory()() as session:
        count = int(session.query(WalMaintenanceJob).count())
    assert count == 1


def test_request_checkpoint_respects_min_interval_and_force_override(tmp_path: Path) -> None:
    service = make_wal_service(tmp_path, min_interval_seconds=3600)

    first = service.request_checkpoint(mode="passive")
    now = datetime.now(tz=timezone.utc)

    with db_session_module.get_session_factory()() as session:
        row = session.query(WalMaintenanceJob).filter(WalMaintenanceJob.id == first.id).one()
        row.status = WalMaintenanceStatus.COMPLETED
        row.finished_at = now
        session.commit()

    try:
        service.request_checkpoint(mode="passive")
    except WalMaintenanceConflictError:
        pass
    else:
        raise AssertionError("expected WalMaintenanceConflictError")

    forced = service.request_checkpoint(mode="passive", force=True)
    assert forced.id != first.id
    assert forced.status == WalMaintenanceStatus.PENDING


def test_truncate_checkpoint_is_guarded_by_policy(tmp_path: Path) -> None:
    blocked_service = make_wal_service(tmp_path, allow_truncate=False, state_suffix="blocked")

    try:
        blocked_service.request_checkpoint(mode="truncate")
    except WalMaintenancePolicyError:
        pass
    else:
        raise AssertionError("expected WalMaintenancePolicyError")

    allowed_service = make_wal_service(tmp_path, allow_truncate=True, state_suffix="allowed")
    allowed = allowed_service.request_checkpoint(mode="truncate")
    assert allowed.requested_mode == WalCheckpointMode.TRUNCATE


def test_wal_maintenance_api_latest_and_metrics(tmp_path: Path) -> None:
    service = make_wal_service(tmp_path, min_interval_seconds=1)

    app = create_app()
    client = TestClient(app)

    not_found = client.get("/api/v1/maintenance/wal/checkpoint/latest")
    assert not_found.status_code == 404

    create_resp = client.post(
        "/api/v1/maintenance/wal/checkpoint",
        json={"mode": "restart", "reason": "api"},
    )
    assert create_resp.status_code == 202
    created = create_resp.json()
    assert created["requested_mode"] == "restart"
    assert created["status"] == "pending"

    latest_resp = client.get("/api/v1/maintenance/wal/checkpoint/latest")
    assert latest_resp.status_code == 200
    latest = latest_resp.json()
    assert latest["id"] == created["id"]

    with db_session_module.get_session_factory()() as session:
        row = session.query(WalMaintenanceJob).filter(WalMaintenanceJob.id == int(created["id"])).one()
        row.status = WalMaintenanceStatus.COMPLETED
        row.finished_at = datetime.now(tz=timezone.utc) - timedelta(seconds=1)
        session.add(
            WalMaintenanceJob(
                requested_mode=WalCheckpointMode.PASSIVE,
                status=WalMaintenanceStatus.FAILED,
                requested_by="test",
                reason="failed",
                execute_after=datetime.now(tz=timezone.utc),
                retry_count=1,
            )
        )
        session.commit()

    metrics_resp = client.get("/api/v1/maintenance/wal/metrics")
    assert metrics_resp.status_code == 200
    metrics = metrics_resp.json()
    assert metrics["pending"] == 0
    assert metrics["running"] == 0
    assert metrics["retryable"] == 0
    assert metrics["failed"] == 1
    assert metrics["completed"] == 1
    assert metrics["latest_completed_at"] is not None


def test_wal_maintenance_api_rejects_invalid_mode(tmp_path: Path) -> None:
    make_wal_service(tmp_path)
    app = create_app()
    client = TestClient(app)

    response = client.post("/api/v1/maintenance/wal/checkpoint", json={"mode": "invalid"})
    assert response.status_code == 422
