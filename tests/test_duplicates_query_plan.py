from __future__ import annotations

import os
from pathlib import Path

import dedupfs.db.session as db_session_module
from sqlalchemy import text

from dedupfs.core.config import get_settings
from dedupfs.db.init_db import initialize_database
from dedupfs.db.models import HashAlgorithm, LibraryFile, LibraryRoot
from dedupfs.duplicates.service import DuplicateService


def _prepare_env(tmp_path: Path) -> DuplicateService:
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


def _seed_rows(total_groups: int, rows_per_group: int) -> None:
    with db_session_module.get_session_factory()() as session:
        root = LibraryRoot(name="lib-plan", root_path="/libraries/lib-plan")
        session.add(root)
        session.flush()

        for group_idx in range(total_groups):
            hash_seed = bytes([group_idx % 256]) * 32
            for row_idx in range(rows_per_group):
                session.add(
                    LibraryFile(
                        library_id=root.id,
                        relative_path=f"media/g{group_idx}-f{row_idx}.jpg",
                        size_bytes=100 + row_idx,
                        mtime_ns=1700000000000000000 + row_idx,
                        is_missing=False,
                        needs_hash=False,
                        hash_algorithm=HashAlgorithm.SHA256,
                        content_hash=hash_seed,
                    )
                )

        session.commit()


def test_duplicate_group_query_plan_uses_dedup_group_index(tmp_path: Path) -> None:
    service = _prepare_env(tmp_path)
    _seed_rows(total_groups=20, rows_per_group=3)

    page = service.list_groups(limit=10)
    assert len(page.items) == 10

    with db_session_module.get_session_factory()() as session:
        rows = session.execute(
            text(
                """
                EXPLAIN QUERY PLAN
                SELECT hash_algorithm, content_hash, COUNT(1)
                FROM library_files INDEXED BY ix_library_files_dedup_group
                WHERE is_missing = 0
                  AND needs_hash = 0
                  AND hash_algorithm IS NOT NULL
                  AND content_hash IS NOT NULL
                GROUP BY hash_algorithm, content_hash
                HAVING COUNT(1) > 1
                """
            )
        ).all()

    details = " ".join(str(row[3]) for row in rows)
    assert "ix_library_files_dedup_group" in details
