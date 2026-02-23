from __future__ import annotations

import os
import time
from pathlib import Path

import dedupfs.db.session as db_session_module
from dedupfs.core.config import get_settings
from dedupfs.db.init_db import initialize_database
from dedupfs.db.models import JobKind, JobStatus
from dedupfs.jobs.service import InvalidJobStateError, JobConflictError, JobPolicyError, JobService


def make_service(tmp_path: Path, *, dry_run: bool, allow_real_delete: bool) -> JobService:
    state_root = tmp_path / f"state_{int(dry_run)}_{int(allow_real_delete)}"
    state_root.mkdir(parents=True, exist_ok=True)

    os.environ["DEDUPFS_LIBRARIES_ROOT"] = "/libraries"
    os.environ["DEDUPFS_STATE_ROOT"] = state_root.as_posix()
    os.environ["DEDUPFS_DRY_RUN"] = "true" if dry_run else "false"
    os.environ["DEDUPFS_ALLOW_REAL_DELETE"] = "true" if allow_real_delete else "false"
    os.environ["DEDUPFS_JOB_LOCK_TTL_SECONDS"] = "1"

    get_settings.cache_clear()
    db_session_module._engine = None
    db_session_module._session_factory = None
    initialize_database()
    return JobService(get_settings(), db_session_module.get_session_factory())


def test_global_dry_run_blocks_real_run_jobs(tmp_path: Path) -> None:
    service = make_service(tmp_path, dry_run=True, allow_real_delete=False)
    try:
        service.create_job(kind=JobKind.SCAN, payload={}, dry_run=False)
    except JobPolicyError:
        pass
    else:
        raise AssertionError("expected JobPolicyError")


def test_non_delete_job_can_run_real_mode_when_global_dry_run_is_disabled(tmp_path: Path) -> None:
    service = make_service(tmp_path, dry_run=False, allow_real_delete=False)
    job = service.create_job(kind=JobKind.SCAN, payload={}, dry_run=False)
    assert job.dry_run is False


def test_delete_job_real_mode_blocked_when_real_delete_disabled(tmp_path: Path) -> None:
    service = make_service(tmp_path, dry_run=False, allow_real_delete=False)
    try:
        service.create_job(kind=JobKind.DELETE, payload={}, dry_run=False)
    except JobPolicyError:
        pass
    else:
        raise AssertionError("expected JobPolicyError")


def test_delete_job_real_mode_allowed_when_explicitly_enabled(tmp_path: Path) -> None:
    service = make_service(tmp_path, dry_run=False, allow_real_delete=True)
    job = service.create_job(kind=JobKind.DELETE, payload={}, dry_run=False)
    assert job.kind == JobKind.DELETE
    assert job.dry_run is False


def test_scan_hash_jobs_remain_mutually_exclusive(tmp_path: Path) -> None:
    service = make_service(tmp_path, dry_run=True, allow_real_delete=False)
    service.create_job(kind=JobKind.SCAN, payload={})
    try:
        service.create_job(kind=JobKind.HASH, payload={})
    except JobConflictError:
        pass
    else:
        raise AssertionError("expected JobConflictError")


def test_fsm_transition_legality(tmp_path: Path) -> None:
    service = make_service(tmp_path, dry_run=True, allow_real_delete=False)
    pending = service.create_job(kind=JobKind.SCAN, payload={})
    claimed = service.claim_pending_scan_hash_job(worker_id="worker-a")
    assert claimed is not None
    assert claimed.status == JobStatus.RUNNING

    finished = service.finish_job(claimed.id, worker_id="worker-a", success=True)
    assert finished.status == JobStatus.COMPLETED

    try:
        service.reset_retryable_job(finished.id)
    except InvalidJobStateError:
        pass
    else:
        raise AssertionError("expected InvalidJobStateError")


def test_stale_lease_becomes_retryable(tmp_path: Path) -> None:
    service = make_service(tmp_path, dry_run=True, allow_real_delete=False)
    job = service.create_job(kind=JobKind.SCAN, payload={})
    claimed = service.claim_pending_scan_hash_job(worker_id="worker-a")
    assert claimed is not None
    time.sleep(1.2)
    recovered = service.recover_stale_jobs()
    assert recovered >= 1
    updated = service.get_job(job.id)
    assert updated.status == JobStatus.RETRYABLE


def test_heartbeat_on_expired_lease_clears_worker_binding(tmp_path: Path) -> None:
    service = make_service(tmp_path, dry_run=True, allow_real_delete=False)
    created = service.create_job(kind=JobKind.SCAN, payload={})
    claimed = service.claim_pending_scan_hash_job(worker_id="worker-a")
    assert claimed is not None
    time.sleep(1.2)

    try:
        service.heartbeat(created.id, worker_id="worker-a", progress=0.2, processed_items=1)
    except JobConflictError:
        pass
    else:
        raise AssertionError("expected JobConflictError")

    updated = service.get_job(created.id)
    assert updated.status == JobStatus.RETRYABLE
    assert updated.worker_id is None
    assert updated.worker_heartbeat_at is None
    assert updated.lease_expires_at is None
    assert updated.error_code == "LEASE_EXPIRED"


def test_list_jobs_cursor_pagination_is_gap_free(tmp_path: Path) -> None:
    service = make_service(tmp_path, dry_run=True, allow_real_delete=False)
    created_ids = [service.create_job(kind=JobKind.DELETE, payload={"i": i}).id for i in range(8)]

    page1 = service.list_jobs(limit=3)
    page2 = service.list_jobs(limit=3, cursor=page1.next_cursor)
    page3 = service.list_jobs(limit=3, cursor=page2.next_cursor)

    seen = [job.id for job in page1.items + page2.items + page3.items]
    assert len(seen) == len(set(seen))
    assert set(seen) == set(created_ids)


def test_list_jobs_rejects_unknown_cursor(tmp_path: Path) -> None:
    service = make_service(tmp_path, dry_run=True, allow_real_delete=False)
    service.create_job(kind=JobKind.DELETE, payload={})
    try:
        service.list_jobs(limit=10, cursor="does-not-exist")
    except ValueError:
        pass
    else:
        raise AssertionError("expected ValueError for unknown cursor")
