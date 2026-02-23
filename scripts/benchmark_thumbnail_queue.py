from __future__ import annotations

import argparse
import collections
import os
import random
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path

import dedupfs.db.session as db_session_module
from dedupfs.core.config import get_settings
from dedupfs.db.init_db import initialize_database
from dedupfs.db.models import HashAlgorithm, LibraryFile, LibraryRoot
from dedupfs.thumbs.service import ThumbnailQueueFullError, ThumbnailService


@dataclass(slots=True)
class RunStats:
    elapsed_seconds: float
    requests: int
    accepted: int
    queue_full: int
    failed: int
    unique_thumb_keys: int
    latency_p50_ms: float
    latency_p95_ms: float
    error_top: list[tuple[str, int]]

    @property
    def throughput_rps(self) -> float:
        if self.elapsed_seconds <= 0:
            return 0.0
        return self.requests / self.elapsed_seconds

    @property
    def queue_full_ratio(self) -> float:
        if self.requests <= 0:
            return 0.0
        return self.queue_full / self.requests

    @property
    def failed_ratio(self) -> float:
        if self.requests <= 0:
            return 0.0
        return self.failed / self.requests

    @property
    def dedupe_ratio(self) -> float:
        if self.accepted <= 0:
            return 1.0
        return 1.0 - (self.unique_thumb_keys / self.accepted)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Benchmark thumbnail queue admission throughput and contention")
    parser.add_argument("--state-root", required=True, help="State root directory")
    parser.add_argument("--files", type=int, default=5000, help="Number of seeded files")
    parser.add_argument("--requests", type=int, default=20000, help="Total request count")
    parser.add_argument("--workers", type=int, default=12, help="Concurrent request workers")
    parser.add_argument("--hot-file-count", type=int, default=1000, help="Hot-set size used to generate contention")
    parser.add_argument("--queue-capacity", type=int, default=50000, help="Thumbnail queue capacity")
    parser.add_argument("--max-dimension", type=int, default=256, help="Requested thumbnail max dimension")
    parser.add_argument("--output-format", default="jpeg", choices=("jpeg", "webp"), help="Requested output format")
    parser.add_argument("--seed", type=int, default=20260224, help="Random seed")
    parser.add_argument("--min-throughput-rps", type=float, default=None, help="Fail if throughput is below threshold")
    parser.add_argument("--max-queue-full-ratio", type=float, default=None, help="Fail if queue-full ratio is above threshold")
    parser.add_argument("--min-dedupe-ratio", type=float, default=None, help="Fail if dedupe ratio is below threshold")
    parser.add_argument("--max-failed-ratio", type=float, default=None, help="Fail if failed ratio is above threshold")
    return parser.parse_args()


def configure_env(state_root: Path, queue_capacity: int) -> None:
    state_root.mkdir(parents=True, exist_ok=True)
    os.environ["DEDUPFS_LIBRARIES_ROOT"] = "/libraries"
    os.environ["DEDUPFS_STATE_ROOT"] = state_root.as_posix()
    os.environ["DEDUPFS_DRY_RUN"] = "true"
    os.environ["DEDUPFS_ALLOW_REAL_DELETE"] = "false"
    os.environ["DEDUPFS_THUMBNAIL_QUEUE_CAPACITY"] = str(max(1, queue_capacity))

    get_settings.cache_clear()
    db_session_module._engine = None
    db_session_module._session_factory = None


def seed_files(total_files: int) -> list[int]:
    session_factory = db_session_module.get_session_factory()
    with session_factory() as session:
        root = LibraryRoot(name="thumb-bench-lib", root_path="/libraries/thumb-bench-lib")
        session.add(root)
        session.flush()

        batch: list[LibraryFile] = []
        file_ids: list[int] = []
        for index in range(total_files):
            row = LibraryFile(
                library_id=root.id,
                relative_path=f"fixture/f-{index}.jpg",
                size_bytes=1024 + (index % 128),
                mtime_ns=1700000000000000000 + index,
                is_missing=False,
                needs_hash=False,
                hash_algorithm=HashAlgorithm.SHA256,
                content_hash=(index.to_bytes(4, "little", signed=False) * 8),
            )
            batch.append(row)
            if len(batch) >= 3000:
                session.add_all(batch)
                session.flush()
                file_ids.extend(int(item.id) for item in batch)
                batch.clear()

        if batch:
            session.add_all(batch)
            session.flush()
            file_ids.extend(int(item.id) for item in batch)
            batch.clear()

        session.commit()
    return file_ids


def percentile(values: list[float], ratio: float) -> float:
    if not values:
        return 0.0
    if len(values) == 1:
        return values[0]
    sorted_values = sorted(values)
    index = int(round((len(sorted_values) - 1) * ratio))
    index = max(0, min(index, len(sorted_values) - 1))
    return sorted_values[index]


def run_benchmark(
    *,
    service: ThumbnailService,
    file_ids: list[int],
    requests: int,
    workers: int,
    hot_file_count: int,
    max_dimension: int,
    output_format: str,
    seed: int,
) -> RunStats:
    rng = random.Random(seed)
    hot_size = max(1, min(hot_file_count, len(file_ids)))
    hot_ids = file_ids[:hot_size]
    request_ids = [rng.choice(hot_ids) for _ in range(requests)]

    accepted = 0
    queue_full = 0
    failed = 0
    thumb_keys: set[str] = set()
    latencies_ms: list[float] = []
    error_counter: collections.Counter[str] = collections.Counter()

    def request_once(file_id: int) -> tuple[str, str | None, float, str | None]:
        started = time.perf_counter()
        try:
            snapshot = service.request_thumbnail(
                file_id=file_id,
                max_dimension=max_dimension,
                output_format=output_format,
            )
            return ("accepted", snapshot.thumb_key, (time.perf_counter() - started) * 1000.0, None)
        except ThumbnailQueueFullError:
            return ("queue_full", None, (time.perf_counter() - started) * 1000.0, None)
        except Exception as exc:
            signature = f"{exc.__class__.__name__}:{str(exc).strip()[:180]}"
            return ("failed", None, (time.perf_counter() - started) * 1000.0, signature)

    start = time.perf_counter()
    with ThreadPoolExecutor(max_workers=max(1, workers)) as executor:
        future_to_file = {
            executor.submit(request_once, file_id): file_id
            for file_id in request_ids
        }
        for future in as_completed(future_to_file):
            try:
                status, thumb_key, latency_ms, error_sig = future.result()
                if status == "accepted":
                    accepted += 1
                    if thumb_key is not None:
                        thumb_keys.add(thumb_key)
                elif status == "queue_full":
                    queue_full += 1
                else:
                    failed += 1
                    if error_sig:
                        error_counter[error_sig] += 1
                latencies_ms.append(latency_ms)
            except Exception:
                # This branch should be unreachable because request_once catches known outcomes.
                failed += 1
                error_counter["FUTURE_UNEXPECTED_EXCEPTION"] += 1
                latencies_ms.append(0.0)
    elapsed = time.perf_counter() - start

    if not latencies_ms:
        latencies_ms.append(0.0)

    return RunStats(
        elapsed_seconds=elapsed,
        requests=requests,
        accepted=accepted,
        queue_full=queue_full,
        failed=failed,
        unique_thumb_keys=len(thumb_keys),
        latency_p50_ms=percentile(latencies_ms, 0.50),
        latency_p95_ms=percentile(latencies_ms, 0.95),
        error_top=error_counter.most_common(5),
    )


def assert_thresholds(args: argparse.Namespace, stats: RunStats) -> None:
    failures: list[str] = []
    if args.min_throughput_rps is not None and stats.throughput_rps < args.min_throughput_rps:
        failures.append(
            f"throughput_rps={stats.throughput_rps:.2f} < min_throughput_rps={args.min_throughput_rps:.2f}"
        )
    if args.max_queue_full_ratio is not None and stats.queue_full_ratio > args.max_queue_full_ratio:
        failures.append(
            f"queue_full_ratio={stats.queue_full_ratio:.4f} > max_queue_full_ratio={args.max_queue_full_ratio:.4f}"
        )
    if args.min_dedupe_ratio is not None and stats.dedupe_ratio < args.min_dedupe_ratio:
        failures.append(
            f"dedupe_ratio={stats.dedupe_ratio:.4f} < min_dedupe_ratio={args.min_dedupe_ratio:.4f}"
        )
    if args.max_failed_ratio is not None and stats.failed_ratio > args.max_failed_ratio:
        failures.append(
            f"failed_ratio={stats.failed_ratio:.4f} > max_failed_ratio={args.max_failed_ratio:.4f}"
        )
    if failures:
        raise RuntimeError("; ".join(failures))


def main() -> None:
    args = parse_args()
    configure_env(Path(args.state_root), queue_capacity=args.queue_capacity)
    initialize_database()
    file_ids = seed_files(total_files=max(1, args.files))
    service = ThumbnailService(get_settings(), db_session_module.get_session_factory())

    stats = run_benchmark(
        service=service,
        file_ids=file_ids,
        requests=max(1, args.requests),
        workers=max(1, args.workers),
        hot_file_count=max(1, args.hot_file_count),
        max_dimension=max(16, args.max_dimension),
        output_format=args.output_format,
        seed=args.seed,
    )

    print("== Thumbnail Queue Benchmark ==")
    print(f"requests={stats.requests}")
    print(f"accepted={stats.accepted}")
    print(f"queue_full={stats.queue_full}")
    print(f"failed={stats.failed}")
    print(f"unique_thumb_keys={stats.unique_thumb_keys}")
    print(f"elapsed_seconds={stats.elapsed_seconds:.3f}")
    print(f"throughput_rps={stats.throughput_rps:.2f}")
    print(f"queue_full_ratio={stats.queue_full_ratio:.4f}")
    print(f"failed_ratio={stats.failed_ratio:.4f}")
    print(f"dedupe_ratio={stats.dedupe_ratio:.4f}")
    print(f"latency_p50_ms={stats.latency_p50_ms:.2f}")
    print(f"latency_p95_ms={stats.latency_p95_ms:.2f}")
    if stats.error_top:
        print("top_errors:")
        for signature, count in stats.error_top:
            print(f"- {count}x {signature}")

    assert_thresholds(args, stats)


if __name__ == "__main__":
    main()
