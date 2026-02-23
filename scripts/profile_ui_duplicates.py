from __future__ import annotations

import argparse
import json
import math
import os
import time
from pathlib import Path

import dedupfs.db.session as db_session_module
from dedupfs.core.config import get_settings
from dedupfs.db.init_db import initialize_database
from dedupfs.db.models import HashAlgorithm, LibraryFile, LibraryRoot
from dedupfs.duplicates.service import DuplicateService


def parse_int_list(raw: str) -> list[int]:
    values: list[int] = []
    for token in raw.split(","):
        token = token.strip()
        if not token:
            continue
        values.append(int(token))
    if not values:
        raise ValueError("at least one candidate value is required")
    return values


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Profile duplicate-group paging and UI virtualization thresholds")
    parser.add_argument("--state-root", required=True, help="State root directory")
    parser.add_argument("--groups", type=int, default=50000, help="Duplicate-group cardinality")
    parser.add_argument("--files-per-group", type=int, default=2, help="Files per duplicate group")
    parser.add_argument("--page-size-candidates", default="120,160,200", help="Comma-separated page size candidates")
    parser.add_argument("--overscan-candidates", default="4,6,8", help="Comma-separated overscan candidates")
    parser.add_argument("--viewport-heights", default="720,900,1080", help="Comma-separated viewport heights in px")
    parser.add_argument("--group-row-height", type=int, default=84, help="Virtualized group row height")
    parser.add_argument("--target-max-dom-rows", type=int, default=26, help="Target upper bound for virtualized DOM rows")
    parser.add_argument("--report-json", default=None, help="Optional JSON output file")
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
        root = LibraryRoot(name="ui-profiler-lib", root_path="/libraries/ui-profiler-lib")
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
            if len(batch) >= 4000:
                session.add_all(batch)
                session.flush()
                batch.clear()

        if batch:
            session.add_all(batch)
            session.flush()
        session.commit()


def benchmark_page_size(page_size: int) -> dict[str, float]:
    service = DuplicateService(get_settings(), db_session_module.get_session_factory())
    cursor: str | None = None
    total_groups = 0
    total_pages = 0
    start = time.perf_counter()
    while True:
        page = service.list_groups(limit=page_size, cursor=cursor)
        total_pages += 1
        total_groups += len(page.items)
        cursor = page.next_cursor
        if cursor is None:
            break
    elapsed = time.perf_counter() - start
    per_page_ms = (elapsed / max(total_pages, 1)) * 1000.0
    groups_per_second = total_groups / elapsed if elapsed > 0 else 0.0
    return {
        "page_size": float(page_size),
        "pages": float(total_pages),
        "groups": float(total_groups),
        "elapsed_seconds": elapsed,
        "per_page_ms": per_page_ms,
        "groups_per_second": groups_per_second,
    }


def evaluate_overscan(
    *,
    overscan: int,
    viewport_heights: list[int],
    row_height: int,
    target_max_dom_rows: int,
) -> dict[str, object]:
    max_dom_rows = 0
    rows_by_viewport: dict[str, int] = {}
    for height in viewport_heights:
        visible = max(1, math.ceil(height / row_height))
        dom_rows = visible + overscan * 2
        rows_by_viewport[str(height)] = dom_rows
        max_dom_rows = max(max_dom_rows, dom_rows)
    return {
        "overscan": overscan,
        "rows_by_viewport": rows_by_viewport,
        "max_dom_rows": max_dom_rows,
        "within_target": max_dom_rows <= target_max_dom_rows,
    }


def main() -> None:
    args = parse_args()
    page_size_candidates = parse_int_list(args.page_size_candidates)
    overscan_candidates = parse_int_list(args.overscan_candidates)
    viewport_heights = parse_int_list(args.viewport_heights)

    configure_env(Path(args.state_root))
    initialize_database()
    seed_fixture(total_groups=args.groups, files_per_group=args.files_per_group)

    page_results = [benchmark_page_size(candidate) for candidate in page_size_candidates]
    page_results.sort(key=lambda item: item["elapsed_seconds"])
    recommended_page_size = int(page_results[0]["page_size"])

    overscan_results = [
        evaluate_overscan(
            overscan=value,
            viewport_heights=viewport_heights,
            row_height=args.group_row_height,
            target_max_dom_rows=args.target_max_dom_rows,
        )
        for value in overscan_candidates
    ]
    preferred = [item for item in overscan_results if bool(item["within_target"])]
    if preferred:
        recommended_overscan = int(sorted(preferred, key=lambda item: int(item["max_dom_rows"]))[0]["overscan"])
    else:
        recommended_overscan = int(sorted(overscan_results, key=lambda item: int(item["max_dom_rows"]))[0]["overscan"])

    report = {
        "groups": args.groups,
        "files_per_group": args.files_per_group,
        "group_row_height": args.group_row_height,
        "target_max_dom_rows": args.target_max_dom_rows,
        "page_results": page_results,
        "overscan_results": overscan_results,
        "recommended": {
            "group_page_size": recommended_page_size,
            "group_overscan": recommended_overscan,
        },
    }

    print("== Duplicate Page-Size Results ==")
    for item in page_results:
        print(
            "page_size={page_size:.0f} groups={groups:.0f} pages={pages:.0f} "
            "elapsed={elapsed_seconds:.3f}s per_page={per_page_ms:.2f}ms "
            "groups_per_sec={groups_per_second:.1f}".format(**item)
        )

    print("== Virtualized DOM Rows by Overscan ==")
    for item in overscan_results:
        rows = ", ".join(f"{k}px->{v}" for k, v in sorted(item["rows_by_viewport"].items()))
        print(
            f"overscan={item['overscan']} max_dom_rows={item['max_dom_rows']} "
            f"within_target={item['within_target']} [{rows}]"
        )

    print("== Recommendation ==")
    print(f"group_page_size={recommended_page_size}")
    print(f"group_overscan={recommended_overscan}")

    if args.report_json:
        report_path = Path(args.report_json)
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
        print(f"report_json={report_path.as_posix()}")


if __name__ == "__main__":
    main()
