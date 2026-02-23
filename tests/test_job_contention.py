from __future__ import annotations

import os
import threading
from pathlib import Path

import dedupfs.db.session as db_session_module
from dedupfs.core.config import get_settings
from dedupfs.db.init_db import initialize_database
from dedupfs.db.models import JobKind
from dedupfs.jobs.service import JobConflictError, JobService


def setup_env(tmp_path: Path) -> JobService:
    state_root = tmp_path / "state"
    state_root.mkdir(parents=True, exist_ok=True)
    os.environ["DEDUPFS_LIBRARIES_ROOT"] = "/libraries"
    os.environ["DEDUPFS_STATE_ROOT"] = state_root.as_posix()
    os.environ["DEDUPFS_DRY_RUN"] = "true"
    os.environ["DEDUPFS_ALLOW_REAL_DELETE"] = "false"
    os.environ["DEDUPFS_JOB_LOCK_TTL_SECONDS"] = "30"

    get_settings.cache_clear()
    db_session_module._engine = None
    db_session_module._session_factory = None
    initialize_database()
    return JobService(get_settings(), db_session_module.get_session_factory())


def test_two_workers_claim_only_one_running_job(tmp_path: Path) -> None:
    service = setup_env(tmp_path)
    service.create_job(kind=JobKind.SCAN, payload={})

    results: list[str] = []

    def claim(worker_id: str) -> None:
        job = service.claim_pending_scan_hash_job(worker_id=worker_id)
        if job is None:
            results.append("none")
        else:
            results.append(job.worker_id or "")

    t1 = threading.Thread(target=claim, args=("worker-a",))
    t2 = threading.Thread(target=claim, args=("worker-b",))
    t1.start()
    t2.start()
    t1.join()
    t2.join()

    winners = [entry for entry in results if entry in {"worker-a", "worker-b"}]
    assert len(winners) == 1
    assert results.count("none") == 1


def test_concurrent_scan_job_creation_allows_only_one_active_job(tmp_path: Path) -> None:
    service = setup_env(tmp_path)
    barrier = threading.Barrier(2)
    created_ids: list[str] = []
    conflicts: list[str] = []
    lock = threading.Lock()

    def create_scan() -> None:
        try:
            barrier.wait(timeout=2)
            snapshot = service.create_job(kind=JobKind.SCAN, payload={})
            with lock:
                created_ids.append(snapshot.id)
        except JobConflictError:
            with lock:
                conflicts.append("conflict")

    t1 = threading.Thread(target=create_scan)
    t2 = threading.Thread(target=create_scan)
    t1.start()
    t2.start()
    t1.join()
    t2.join()

    assert len(created_ids) == 1
    assert len(conflicts) == 1
