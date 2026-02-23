from __future__ import annotations

import base64
import json
import os
from pathlib import Path

import dedupfs.db.session as db_session_module
from fastapi.testclient import TestClient

from dedupfs.api.app import create_app
from dedupfs.core.config import get_settings
from dedupfs.db.init_db import initialize_database
from dedupfs.db.models import HashAlgorithm, LibraryFile, LibraryRoot
from dedupfs.duplicates.service import DuplicateService


def make_duplicate_service(tmp_path: Path) -> DuplicateService:
    state_root = tmp_path / "state"
    state_root.mkdir(parents=True, exist_ok=True)

    os.environ["DEDUPFS_LIBRARIES_ROOT"] = "/libraries"
    os.environ["DEDUPFS_STATE_ROOT"] = state_root.as_posix()
    os.environ["DEDUPFS_DRY_RUN"] = "true"
    os.environ["DEDUPFS_ALLOW_REAL_DELETE"] = "false"

    get_settings.cache_clear()
    db_session_module._engine = None
    db_session_module._session_factory = None
    initialize_database()
    return DuplicateService(get_settings(), db_session_module.get_session_factory())


def _ensure_root(root_path: str) -> int:
    with db_session_module.get_session_factory()() as session:
        root = session.query(LibraryRoot).filter(LibraryRoot.root_path == root_path).one_or_none()
        if root is None:
            root = LibraryRoot(name=f"lib-{abs(hash(root_path)) % 100000}", root_path=root_path)
            session.add(root)
            session.flush()
        session.commit()
        return int(root.id)


def _seed_file(
    *,
    library_id: int,
    relative_path: str,
    size_bytes: int,
    hash_algorithm: HashAlgorithm | None,
    content_hash: bytes | None,
    is_missing: bool = False,
    needs_hash: bool = False,
) -> int:
    with db_session_module.get_session_factory()() as session:
        row = LibraryFile(
            library_id=library_id,
            relative_path=relative_path,
            size_bytes=size_bytes,
            mtime_ns=1700000000000000000,
            is_missing=is_missing,
            needs_hash=needs_hash,
            hash_algorithm=hash_algorithm,
            content_hash=content_hash,
        )
        session.add(row)
        session.commit()
        return int(row.id)


def test_duplicate_groups_keyset_pagination_is_stable(tmp_path: Path) -> None:
    service = make_duplicate_service(tmp_path)

    lib_a = _ensure_root("/libraries/lib-a")
    lib_b = _ensure_root("/libraries/lib-b")

    hash_a = b"\x11" * 32
    hash_b = b"\x22" * 32
    hash_c = b"\x33" * 32

    for idx in range(3):
        _seed_file(
            library_id=lib_a,
            relative_path=f"media/a-{idx}.jpg",
            size_bytes=100,
            hash_algorithm=HashAlgorithm.SHA256,
            content_hash=hash_a,
        )

    for idx in range(2):
        _seed_file(
            library_id=lib_b,
            relative_path=f"media/b-{idx}.jpg",
            size_bytes=200,
            hash_algorithm=HashAlgorithm.BLAKE3,
            content_hash=hash_b,
        )

    _seed_file(
        library_id=lib_a,
        relative_path="media/c-0.jpg",
        size_bytes=500,
        hash_algorithm=HashAlgorithm.SHA256,
        content_hash=hash_c,
    )

    _seed_file(
        library_id=lib_a,
        relative_path="media/a-missing.jpg",
        size_bytes=100,
        hash_algorithm=HashAlgorithm.SHA256,
        content_hash=hash_a,
        is_missing=True,
    )
    _seed_file(
        library_id=lib_a,
        relative_path="media/a-stale.jpg",
        size_bytes=100,
        hash_algorithm=HashAlgorithm.SHA256,
        content_hash=hash_a,
        needs_hash=True,
    )

    page_1 = service.list_groups(limit=1)
    page_2 = service.list_groups(limit=1, cursor=page_1.next_cursor)

    assert len(page_1.items) == 1
    assert len(page_2.items) == 1
    assert page_1.next_cursor is not None
    assert page_2.next_cursor is None

    first = page_1.items[0]
    second = page_2.items[0]

    assert first.file_count == 3
    assert first.total_size_bytes == 300
    assert first.duplicate_waste_bytes == 200
    assert first.group_key == f"sha256:{hash_a.hex()}"

    assert second.file_count == 2
    assert second.total_size_bytes == 400
    assert second.duplicate_waste_bytes == 200
    assert second.group_key == f"blake3:{hash_b.hex()}"


def test_duplicate_groups_reject_invalid_cursor(tmp_path: Path) -> None:
    service = make_duplicate_service(tmp_path)

    try:
        service.list_groups(limit=10, cursor="invalid-cursor")
    except ValueError:
        pass
    else:
        raise AssertionError("expected ValueError for invalid cursor")

    bad_payload = {
        "file_count": 3,
        "total_size_bytes": 300,
        "hash_algorithm": "sha256",
        "content_hash_hex": "aa",
    }
    encoded = base64.urlsafe_b64encode(json.dumps(bad_payload).encode("utf-8")).decode("ascii").rstrip("=")
    try:
        service.list_groups(limit=10, cursor=encoded)
    except ValueError:
        pass
    else:
        raise AssertionError("expected ValueError for invalid cursor hash length")


def test_duplicate_group_files_pagination_and_group_key_validation(tmp_path: Path) -> None:
    service = make_duplicate_service(tmp_path)
    library_id = _ensure_root("/libraries/lib-files")

    content_hash = b"\x44" * 32
    file_ids = [
        _seed_file(
            library_id=library_id,
            relative_path=f"media/f-{idx}.jpg",
            size_bytes=128,
            hash_algorithm=HashAlgorithm.SHA256,
            content_hash=content_hash,
        )
        for idx in range(5)
    ]

    page_1 = service.list_group_files(group_key=f"sha256:{content_hash.hex()}", limit=2)
    page_2 = service.list_group_files(
        group_key=f"sha256:{content_hash.hex()}",
        limit=2,
        cursor=page_1.next_cursor,
    )
    page_3 = service.list_group_files(
        group_key=f"sha256:{content_hash.hex()}",
        limit=2,
        cursor=page_2.next_cursor,
    )

    seen = [item.file_id for item in page_1.items + page_2.items + page_3.items]
    assert seen == file_ids
    assert len(seen) == len(set(seen))

    try:
        service.list_group_files(group_key="invalid", limit=10)
    except ValueError:
        pass
    else:
        raise AssertionError("expected ValueError for invalid group key")

    try:
        service.list_group_files(group_key="sha256:abcd", limit=10)
    except ValueError:
        pass
    else:
        raise AssertionError("expected ValueError for invalid group hash length")


def test_duplicate_groups_api_routes(tmp_path: Path) -> None:
    make_duplicate_service(tmp_path)
    library_id = _ensure_root("/libraries/lib-api")
    content_hash = b"\x55" * 32

    for idx in range(2):
        _seed_file(
            library_id=library_id,
            relative_path=f"media/api-{idx}.jpg",
            size_bytes=64,
            hash_algorithm=HashAlgorithm.SHA256,
            content_hash=content_hash,
        )

    app = create_app()
    client = TestClient(app)

    groups_response = client.get("/api/v1/duplicates/groups", params={"limit": 10})
    assert groups_response.status_code == 200
    groups_payload = groups_response.json()
    assert len(groups_payload["items"]) == 1
    assert groups_payload["items"][0]["group_key"] == f"sha256:{content_hash.hex()}"

    files_response = client.get(
        f"/api/v1/duplicates/groups/sha256:{content_hash.hex()}/files",
        params={"limit": 10},
    )
    assert files_response.status_code == 200
    files_payload = files_response.json()
    assert len(files_payload["items"]) == 2
