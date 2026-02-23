from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

import pytest

import dedupfs.db.session as db_session_module
from dedupfs.core.config import get_settings
from dedupfs.db.init_db import initialize_database
from dedupfs.db.models import LibraryFile, LibraryRoot, Thumbnail, ThumbnailStatus
from dedupfs.thumbs.service import ThumbnailService


def _tiny_png_bytes() -> bytes:
    return bytes.fromhex(
        "89504e470d0a1a0a0000000d4948445200000001000000010802000000907753de"
        "0000000c4944415408d763f8ffff3f0005fe02fea7d64f0000000049454e44ae426082"
    )


@pytest.mark.integration
def test_rust_thumbnail_worker_container_harness(tmp_path: Path) -> None:
    if os.environ.get("DEDUPFS_RUN_CONTAINER_E2E") != "1":
        pytest.skip("set DEDUPFS_RUN_CONTAINER_E2E=1 to run containerized e2e harness")
    if shutil.which("docker") is None:
        pytest.skip("docker not available in PATH")

    repo_root = Path(__file__).resolve().parents[1]
    libraries_root = tmp_path / "libraries"
    state_root = tmp_path / "state"
    media_path = libraries_root / "lib-a" / "media" / "sample.png"
    media_path.parent.mkdir(parents=True, exist_ok=True)
    media_path.write_bytes(_tiny_png_bytes())

    stat = media_path.stat()

    os.environ["DEDUPFS_LIBRARIES_ROOT"] = "/libraries"
    os.environ["DEDUPFS_STATE_ROOT"] = state_root.as_posix()
    os.environ["DEDUPFS_DRY_RUN"] = "true"
    os.environ["DEDUPFS_ALLOW_REAL_DELETE"] = "false"

    get_settings.cache_clear()
    db_session_module._engine = None
    db_session_module._session_factory = None
    initialize_database()

    session_factory = db_session_module.get_session_factory()
    with session_factory() as session:
        root = LibraryRoot(name="lib-a", root_path="/libraries/lib-a")
        session.add(root)
        session.flush()

        file_row = LibraryFile(
            library_id=root.id,
            relative_path="media/sample.png",
            size_bytes=stat.st_size,
            mtime_ns=stat.st_mtime_ns,
            is_missing=False,
            needs_hash=False,
            hash_algorithm=None,
            content_hash=None,
        )
        session.add(file_row)
        session.commit()
        file_id = int(file_row.id)

    service = ThumbnailService(get_settings(), session_factory)
    snapshot = service.request_thumbnail(file_id=file_id, max_dimension=128, output_format="jpeg")
    assert snapshot.status == ThumbnailStatus.PENDING

    command = [
        "docker",
        "run",
        "--rm",
        "-v",
        f"{repo_root.as_posix()}:/workspace",
        "-v",
        f"{libraries_root.as_posix()}:/libraries:ro",
        "-v",
        f"{state_root.as_posix()}:/state",
        "-w",
        "/workspace/rust-worker",
        "-e",
        "DEDUPFS_LIBRARIES_ROOT=/libraries",
        "-e",
        "DEDUPFS_STATE_ROOT=/state",
        "-e",
        "DEDUPFS_RUST_WORKER_POLL_SECONDS=1",
        "-e",
        "DEDUPFS_RUST_WORKER_MAX_POLL_SECONDS=2",
        "rust:1.82-bookworm",
        "bash",
        "-lc",
        "cargo run --quiet -- --worker-id e2e-worker",
    ]
    result = subprocess.run(command, capture_output=True, text=True, timeout=900)
    if result.returncode != 0:
        raise AssertionError(
            "containerized rust worker execution failed:\n"
            f"stdout:\n{result.stdout}\n\n"
            f"stderr:\n{result.stderr}"
        )

    refreshed = service.get_thumbnail(snapshot.thumb_key)
    assert refreshed.status == ThumbnailStatus.READY
    output_path = service.resolve_thumbnail_output_path(refreshed)
    assert output_path.exists()
    assert output_path.stat().st_size > 0

    with session_factory() as session:
        persisted = session.query(Thumbnail).filter(Thumbnail.thumb_key == snapshot.thumb_key).one()
        assert persisted.status == ThumbnailStatus.READY
