from __future__ import annotations

import argparse
import os
import time
from pathlib import Path

import dedupfs.db.session as db_session_module
from sqlalchemy import text

from dedupfs.core.config import get_settings
from dedupfs.db.init_db import initialize_database
from dedupfs.db.models import HashAlgorithm, LibraryFile, LibraryRoot
from dedupfs.duplicates.service import DuplicateService


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Benchmark duplicate-group query path")
    parser.add_argument("--state-root", required=True, help="State root directory")
    parser.add_argument("--groups", type=int, default=5000, help="Number of duplicate groups")
    parser.add_argument("--files-per-group", type=int, default=2, help="Files per duplicate group")
    parser.add_argument("--page-size", type=int, default=200, help="Group page size")
    parser.add_argument("--explain", action="store_true", help="Print query plan details")
    return parser.parse_args()


def configure_env(state_root: Path) -> None:
    state_root.mkdir(parents=True, exist_ok=True)
    os.environ["DEDUPFS_LIBRARIES_ROOT"] = "/libraries"
    os.environ["DEDUPFS_STATE_ROOT"] = state_root.as_posix()
    os.environ["DEDUPFS_DRY_RUN"] = "true"
    os.environ["DEDUPFS_ALLOW_REAL_DELETE"] = "false"

    get_settings.cache_clear()
    db_session_module._engine = None
    db_session_module._session_factory = None


def seed_fixture(total_groups: int, files_per_group: int) -> None:
    session_factory = db_session_module.get_session_factory()
    with session_factory() as session:
        root = LibraryRoot(name="bench-lib", root_path="/libraries/bench-lib")
        session.add(root)
        session.flush()

        batch: list[LibraryFile] = []
        for group_idx in range(total_groups):
            hash_seed = group_idx.to_bytes(4, "little", signed=False) * 8
            for file_idx in range(files_per_group):
                batch.append(
                    LibraryFile(
                        library_id=root.id,
                        relative_path=f"fixture/g{group_idx}/f{file_idx}.jpg",
                        size_bytes=2048 + file_idx,
                        mtime_ns=1700000000000000000 + file_idx,
                        is_missing=False,
                        needs_hash=False,
                        hash_algorithm=HashAlgorithm.SHA256,
                        content_hash=hash_seed,
                    )
                )
            if len(batch) >= 2000:
                session.add_all(batch)
                session.flush()
                batch.clear()

        if batch:
            session.add_all(batch)
            session.flush()

        session.commit()


def benchmark(page_size: int) -> tuple[int, float]:
    service = DuplicateService(get_settings(), db_session_module.get_session_factory())

    start = time.perf_counter()
    cursor: str | None = None
    total_groups = 0
    while True:
        page = service.list_groups(limit=page_size, cursor=cursor)
        total_groups += len(page.items)
        cursor = page.next_cursor
        if cursor is None:
            break
    elapsed = time.perf_counter() - start
    return total_groups, elapsed


def maybe_print_explain() -> None:
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

    print("Query plan:")
    for row in rows:
        print(f"- {row[3]}")


def main() -> None:
    args = parse_args()
    configure_env(Path(args.state_root))
    initialize_database()
    seed_fixture(total_groups=args.groups, files_per_group=args.files_per_group)
    total_groups, elapsed = benchmark(page_size=args.page_size)
    print(f"groups={total_groups} elapsed_seconds={elapsed:.3f} page_size={args.page_size}")
    if args.explain:
        maybe_print_explain()


if __name__ == "__main__":
    main()
