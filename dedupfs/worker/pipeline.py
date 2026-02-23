from __future__ import annotations

import subprocess
from pathlib import Path
from typing import Any, Sequence

from dedupfs.core.config import get_settings
from dedupfs.db.models import JobKind
from dedupfs.db.session import get_session_factory
from dedupfs.jobs.service import JobService
from dedupfs.thumbs.service import ThumbnailService


class RustWorkerExecutionError(RuntimeError):
    pass


def enqueue_scan_job(
    library_names: Sequence[str] | None = None,
    batch_size: int | None = None,
    *,
    dry_run: bool | None = None,
) -> str:
    settings = get_settings()
    job_service = JobService(settings=settings, session_factory=get_session_factory())

    payload: dict[str, Any] = {
        "library_names": list(library_names) if library_names is not None else None,
        "batch_size": batch_size,
    }
    snapshot = job_service.create_job(
        kind=JobKind.SCAN,
        payload=payload,
        dry_run=dry_run,
    )
    return snapshot.id


def enqueue_hash_job(
    *,
    max_files: int | None = None,
    fetch_batch_size: int | None = None,
    algorithm: str | None = None,
    dry_run: bool | None = None,
) -> str:
    settings = get_settings()
    job_service = JobService(settings=settings, session_factory=get_session_factory())

    payload: dict[str, Any] = {
        "max_files": max_files,
        "fetch_batch_size": fetch_batch_size,
        "algorithm": algorithm,
    }
    snapshot = job_service.create_job(
        kind=JobKind.HASH,
        payload=payload,
        dry_run=dry_run,
    )
    return snapshot.id


def run_rust_worker_once(
    *,
    job_id: str | None = None,
    config_path: str | Path | None = None,
    worker_id: str | None = None,
) -> None:
    repo_root = Path(__file__).resolve().parents[2]
    worker_dir = repo_root / "rust-worker"
    if not worker_dir.exists():
        raise RustWorkerExecutionError(f"rust-worker directory is missing: {worker_dir.as_posix()}")

    command = ["cargo", "run", "--", *([] if job_id is None else ["--job-id", job_id])]
    if config_path is not None:
        command.extend(["--config", str(config_path)])
    if worker_id is not None:
        command.extend(["--worker-id", worker_id])

    result = subprocess.run(
        command,
        cwd=worker_dir,
        check=False,
        capture_output=True,
        text=True,
    )

    if result.returncode != 0:
        stderr = result.stderr.strip()
        stdout = result.stdout.strip()
        details = stderr or stdout or "rust worker process failed without output"
        raise RustWorkerExecutionError(details)


def request_thumbnail(
    *,
    file_id: int,
    max_dimension: int | None = None,
    output_format: str | None = None,
) -> str:
    service = ThumbnailService(settings=get_settings(), session_factory=get_session_factory())
    snapshot = service.request_thumbnail(
        file_id=file_id,
        max_dimension=max_dimension,
        output_format=output_format,
    )
    return snapshot.thumb_key


def schedule_thumbnail_group_cleanup(*, group_key: str, delay_seconds: int | None = None) -> int:
    service = ThumbnailService(settings=get_settings(), session_factory=get_session_factory())
    snapshot = service.schedule_group_cleanup(group_key=group_key, delay_seconds=delay_seconds)
    return snapshot.id
