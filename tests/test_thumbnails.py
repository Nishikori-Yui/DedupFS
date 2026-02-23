from __future__ import annotations

import os
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path

import dedupfs.db.session as db_session_module
from fastapi.testclient import TestClient

from dedupfs.api.app import create_app
from dedupfs.core.config import get_settings
from dedupfs.db.init_db import initialize_database
from dedupfs.db.models import (
    HashAlgorithm,
    LibraryFile,
    LibraryRoot,
    Thumbnail,
    ThumbnailCleanupJob,
    ThumbnailCleanupStatus,
    ThumbnailStatus,
)
from dedupfs.thumbs.service import ThumbnailPolicyError, ThumbnailService
from dedupfs.thumbs.service import ThumbnailQueueFullError


def make_thumbnail_service(tmp_path: Path, *, queue_capacity: int = 64) -> ThumbnailService:
    state_root = tmp_path / "state"
    state_root.mkdir(parents=True, exist_ok=True)

    os.environ["DEDUPFS_LIBRARIES_ROOT"] = "/libraries"
    os.environ["DEDUPFS_STATE_ROOT"] = state_root.as_posix()
    os.environ["DEDUPFS_DRY_RUN"] = "true"
    os.environ["DEDUPFS_ALLOW_REAL_DELETE"] = "false"
    os.environ["DEDUPFS_THUMBNAIL_QUEUE_CAPACITY"] = str(queue_capacity)

    get_settings.cache_clear()
    db_session_module._engine = None
    db_session_module._session_factory = None
    initialize_database()
    return ThumbnailService(get_settings(), db_session_module.get_session_factory())


def seed_file(
    service: ThumbnailService,
    *,
    root_path: str,
    relative_path: str,
    with_group_key: bool = False,
) -> int:
    session_factory = db_session_module.get_session_factory()
    with session_factory() as session:
        root = session.query(LibraryRoot).filter(LibraryRoot.root_path == root_path).one_or_none()
        if root is None:
            root = LibraryRoot(name=f"lib-{abs(hash(root_path)) % 100000}", root_path=root_path)
            session.add(root)
            session.flush()

        row = LibraryFile(
            library_id=root.id,
            relative_path=relative_path,
            size_bytes=1024,
            mtime_ns=1700000000000000000,
            is_missing=False,
            needs_hash=False,
            hash_algorithm=HashAlgorithm.SHA256 if with_group_key else None,
            content_hash=(b"\x11" * 32) if with_group_key else None,
        )
        session.add(row)
        session.commit()
        return int(row.id)


def test_request_thumbnail_deduplicates_concurrent_requests(tmp_path: Path) -> None:
    service = make_thumbnail_service(tmp_path)
    file_id = seed_file(service, root_path="/libraries/lib-a", relative_path="media/photo.jpg")

    keys: list[str] = []
    lock = threading.Lock()

    def enqueue() -> None:
        snapshot = service.request_thumbnail(file_id=file_id, max_dimension=256, output_format="jpeg")
        with lock:
            keys.append(snapshot.thumb_key)

    threads = [threading.Thread(target=enqueue) for _ in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert len(set(keys)) == 1

    session_factory = db_session_module.get_session_factory()
    with session_factory() as session:
        count = int(session.query(Thumbnail).count())
    assert count == 1


def test_thumbnail_failure_backoff_and_retry_requeue(tmp_path: Path) -> None:
    service = make_thumbnail_service(tmp_path)
    file_id = seed_file(service, root_path="/libraries/lib-b", relative_path="media/clip.mp4")

    first = service.request_thumbnail(file_id=file_id)
    assert first.status == ThumbnailStatus.PENDING

    session_factory = db_session_module.get_session_factory()
    with session_factory() as session:
        row = session.query(Thumbnail).filter(Thumbnail.thumb_key == first.thumb_key).one()
        row.status = ThumbnailStatus.FAILED
        row.error_code = "THUMB_VIDEO_FFMPEG_FAILED"
        row.error_message = "ffmpeg failed"
        row.error_count = 1
        row.retry_after = datetime.now(tz=timezone.utc) + timedelta(minutes=5)
        session.commit()

    blocked = service.request_thumbnail(file_id=file_id)
    assert blocked.status == ThumbnailStatus.FAILED
    assert blocked.error_code == "THUMB_VIDEO_FFMPEG_FAILED"

    with session_factory() as session:
        row = session.query(Thumbnail).filter(Thumbnail.thumb_key == first.thumb_key).one()
        row.retry_after = datetime.now(tz=timezone.utc) - timedelta(seconds=1)
        session.commit()

    retried = service.request_thumbnail(file_id=file_id)
    assert retried.status == ThumbnailStatus.PENDING
    assert retried.error_code is None


def test_group_cleanup_only_removes_thumbnails_and_is_idempotent(tmp_path: Path) -> None:
    service = make_thumbnail_service(tmp_path)
    file_id = seed_file(
        service,
        root_path="/libraries/lib-c",
        relative_path="media/cover.jpg",
        with_group_key=True,
    )

    snapshot = service.request_thumbnail(file_id=file_id)
    output_path = service.resolve_thumbnail_output_path(snapshot)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_bytes(b"thumb-bytes")

    with db_session_module.get_session_factory()() as session:
        row = session.query(Thumbnail).filter(Thumbnail.thumb_key == snapshot.thumb_key).one()
        row.status = ThumbnailStatus.READY
        session.commit()

    cleaned_1 = service.prune_group_thumbnails(group_key=snapshot.group_key or "")
    cleaned_2 = service.prune_group_thumbnails(group_key=snapshot.group_key or "")

    assert cleaned_1 == 1
    assert cleaned_2 == 0
    assert not output_path.exists()

    with db_session_module.get_session_factory()() as session:
        file_count = int(session.query(LibraryFile).filter(LibraryFile.id == file_id).count())
    assert file_count == 1


def test_thumbnail_request_rejects_library_root_outside_libraries(tmp_path: Path) -> None:
    service = make_thumbnail_service(tmp_path)
    file_id = seed_file(service, root_path="/tmp/escape", relative_path="media/picture.jpg")

    try:
        service.request_thumbnail(file_id=file_id)
    except ThumbnailPolicyError:
        pass
    else:
        raise AssertionError("expected ThumbnailPolicyError")


def test_thumbnail_metrics_endpoint_reports_backlog_and_cleanup_lag(tmp_path: Path) -> None:
    service = make_thumbnail_service(tmp_path)
    file_id = seed_file(service, root_path="/libraries/lib-metrics", relative_path="media/sample.jpg")
    queued = service.request_thumbnail(file_id=file_id)
    now = datetime.now(tz=timezone.utc)

    with db_session_module.get_session_factory()() as session:
        row = session.query(Thumbnail).filter(Thumbnail.thumb_key == queued.thumb_key).one()
        row.status = ThumbnailStatus.FAILED
        row.error_code = "THUMB_GENERATION_FAILED"
        row.error_message = "decode failed"
        row.error_count = 2
        row.retry_after = now + timedelta(minutes=5)

        cleanup = ThumbnailCleanupJob(
            group_key="sha256:metrics-group",
            status=ThumbnailCleanupStatus.PENDING,
            execute_after=now - timedelta(seconds=15),
        )
        session.add(cleanup)
        session.commit()

    app = create_app()
    client = TestClient(app)
    response = client.get("/api/v1/thumbs/metrics")
    assert response.status_code == 200
    payload = response.json()
    assert payload["queue_depth"] == 0
    assert payload["queue_pending"] == 0
    assert payload["queue_running"] == 0
    assert payload["retry_backlog"] == 1
    assert payload["retry_ready"] == 0
    assert payload["cleanup_pending"] == 1
    assert payload["cleanup_running"] == 0
    assert payload["cleanup_overdue"] == 1
    assert payload["cleanup_max_lag_seconds"] >= 10


def test_thumbnail_queue_capacity_is_atomic_under_concurrency(tmp_path: Path) -> None:
    service = make_thumbnail_service(tmp_path, queue_capacity=1)
    file_a = seed_file(service, root_path="/libraries/lib-cap", relative_path="media/a.jpg")
    file_b = seed_file(service, root_path="/libraries/lib-cap", relative_path="media/b.jpg")

    outcomes: list[str] = []
    lock = threading.Lock()

    def enqueue(file_id: int) -> None:
        try:
            service.request_thumbnail(file_id=file_id, max_dimension=128, output_format="jpeg")
            value = "queued"
        except ThumbnailQueueFullError:
            value = "full"
        with lock:
            outcomes.append(value)

    t1 = threading.Thread(target=enqueue, args=(file_a,))
    t2 = threading.Thread(target=enqueue, args=(file_b,))
    t1.start()
    t2.start()
    t1.join()
    t2.join()

    assert outcomes.count("queued") == 1
    assert outcomes.count("full") == 1

    with db_session_module.get_session_factory()() as session:
        queue_depth = int(
            session.query(Thumbnail)
            .filter(Thumbnail.status.in_([ThumbnailStatus.PENDING, ThumbnailStatus.RUNNING]))
            .count()
        )
    assert queue_depth <= 1


def test_group_cleanup_keeps_active_rows_and_files(tmp_path: Path) -> None:
    service = make_thumbnail_service(tmp_path)
    file_ready = seed_file(
        service,
        root_path="/libraries/lib-cleanup",
        relative_path="media/ready.jpg",
        with_group_key=True,
    )
    file_running = seed_file(
        service,
        root_path="/libraries/lib-cleanup",
        relative_path="media/running.jpg",
        with_group_key=True,
    )

    ready = service.request_thumbnail(file_id=file_ready)
    running = service.request_thumbnail(file_id=file_running)
    ready_path = service.resolve_thumbnail_output_path(ready)
    running_path = service.resolve_thumbnail_output_path(running)
    ready_path.parent.mkdir(parents=True, exist_ok=True)
    ready_path.write_bytes(b"ready-thumb")
    running_path.parent.mkdir(parents=True, exist_ok=True)
    running_path.write_bytes(b"running-thumb")

    with db_session_module.get_session_factory()() as session:
        ready_row = session.query(Thumbnail).filter(Thumbnail.thumb_key == ready.thumb_key).one()
        running_row = session.query(Thumbnail).filter(Thumbnail.thumb_key == running.thumb_key).one()
        ready_row.status = ThumbnailStatus.READY
        running_row.status = ThumbnailStatus.RUNNING
        session.commit()

    cleaned = service.prune_group_thumbnails(group_key=ready.group_key or "")
    assert cleaned == 1
    assert not ready_path.exists()
    assert running_path.exists()

    with db_session_module.get_session_factory()() as session:
        running_count = int(session.query(Thumbnail).filter(Thumbnail.thumb_key == running.thumb_key).count())
        ready_count = int(session.query(Thumbnail).filter(Thumbnail.thumb_key == ready.thumb_key).count())
    assert running_count == 1
    assert ready_count == 0
