from __future__ import annotations

import base64
import json
from dataclasses import dataclass
from typing import Any

from sqlalchemy import text
from sqlalchemy.orm import Session, sessionmaker

from dedupfs.core.config import Settings
from dedupfs.db.models import HashAlgorithm
from dedupfs.duplicates.types import (
    DuplicateFileListResult,
    DuplicateFileSnapshot,
    DuplicateGroupListResult,
    DuplicateGroupSnapshot,
)


class DuplicateQueryError(RuntimeError):
    pass


@dataclass(frozen=True)
class _GroupCursor:
    file_count: int
    total_size_bytes: int
    hash_algorithm: HashAlgorithm
    content_hash_hex: str


class DuplicateService:
    def __init__(self, settings: Settings, session_factory: sessionmaker[Session]):
        self._settings = settings
        self._session_factory = session_factory

    def _normalize_limit(self, limit: int | None) -> int:
        if limit is None:
            return int(self._settings.default_page_size)
        bounded = max(1, min(int(limit), int(self._settings.max_page_size)))
        return bounded

    def _encode_group_cursor(self, snapshot: DuplicateGroupSnapshot) -> str:
        payload = {
            "file_count": snapshot.file_count,
            "total_size_bytes": snapshot.total_size_bytes,
            "hash_algorithm": snapshot.hash_algorithm.value,
            "content_hash_hex": snapshot.content_hash_hex,
        }
        raw = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
        return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")

    def _decode_group_cursor(self, cursor: str) -> _GroupCursor:
        token = cursor.strip()
        if not token:
            raise ValueError("Invalid duplicate groups cursor")

        padded = token + ("=" * (-len(token) % 4))
        try:
            decoded = base64.urlsafe_b64decode(padded.encode("ascii")).decode("utf-8")
            payload = json.loads(decoded)
        except Exception as exc:  # noqa: BLE001
            raise ValueError("Invalid duplicate groups cursor") from exc

        try:
            file_count = int(payload["file_count"])
            total_size_bytes = int(payload["total_size_bytes"])
            hash_algorithm = HashAlgorithm(str(payload["hash_algorithm"]).lower())
            content_hash_hex = str(payload["content_hash_hex"]).lower()
        except Exception as exc:  # noqa: BLE001
            raise ValueError("Invalid duplicate groups cursor") from exc

        if file_count < 2 or total_size_bytes < 1:
            raise ValueError("Invalid duplicate groups cursor")
        expected_hex_length = self._expected_hash_hex_length(hash_algorithm)
        if len(content_hash_hex) != expected_hex_length:
            raise ValueError("Invalid duplicate groups cursor")
        if len(content_hash_hex) % 2 != 0:
            raise ValueError("Invalid duplicate groups cursor")
        try:
            bytes.fromhex(content_hash_hex)
        except ValueError as exc:
            raise ValueError("Invalid duplicate groups cursor") from exc

        return _GroupCursor(
            file_count=file_count,
            total_size_bytes=total_size_bytes,
            hash_algorithm=hash_algorithm,
            content_hash_hex=content_hash_hex,
        )

    def _normalize_file_cursor(self, cursor: str | None) -> int | None:
        if cursor is None:
            return None
        token = cursor.strip()
        if not token:
            raise ValueError("Invalid duplicate files cursor")
        try:
            anchor = int(token)
        except ValueError as exc:
            raise ValueError("Invalid duplicate files cursor") from exc
        if anchor < 1:
            raise ValueError("Invalid duplicate files cursor")
        return anchor

    def _expected_hash_hex_length(self, algorithm: HashAlgorithm) -> int:
        if algorithm in {HashAlgorithm.BLAKE3, HashAlgorithm.SHA256}:
            return 64
        raise ValueError("group_key has unsupported algorithm")

    def _parse_group_key(self, group_key: str) -> tuple[HashAlgorithm, str, bytes]:
        token = group_key.strip().lower()
        if not token:
            raise ValueError("group_key cannot be blank")

        algorithm_raw, sep, hash_hex = token.partition(":")
        if sep != ":":
            raise ValueError("group_key must follow <algorithm>:<hash_hex>")
        try:
            algorithm = HashAlgorithm(algorithm_raw)
        except ValueError as exc:
            raise ValueError("group_key has unsupported algorithm") from exc

        expected_hex_length = self._expected_hash_hex_length(algorithm)
        if len(hash_hex) != expected_hex_length:
            raise ValueError(
                f"group_key hash_hex length must be {expected_hex_length} for algorithm {algorithm.value}"
            )
        if len(hash_hex) % 2 != 0:
            raise ValueError("group_key hash_hex must have even length")
        try:
            hash_blob = bytes.fromhex(hash_hex)
        except ValueError as exc:
            raise ValueError("group_key hash_hex is not valid hex") from exc

        return algorithm, hash_hex, hash_blob

    def list_groups(self, *, limit: int | None = None, cursor: str | None = None) -> DuplicateGroupListResult:
        bounded_limit = self._normalize_limit(limit)
        cursor_state = self._decode_group_cursor(cursor) if cursor else None

        where_clause = ""
        params: dict[str, Any] = {
            "limit_plus_one": bounded_limit + 1,
        }
        if cursor_state is not None:
            where_clause = (
                "WHERE (file_count < :cursor_file_count "
                "OR (file_count = :cursor_file_count AND total_size_bytes < :cursor_total_size_bytes) "
                "OR (file_count = :cursor_file_count AND total_size_bytes = :cursor_total_size_bytes "
                "AND hash_algorithm > :cursor_hash_algorithm) "
                "OR (file_count = :cursor_file_count AND total_size_bytes = :cursor_total_size_bytes "
                "AND hash_algorithm = :cursor_hash_algorithm AND content_hash_hex > :cursor_content_hash_hex))"
            )
            params.update(
                {
                    "cursor_file_count": cursor_state.file_count,
                    "cursor_total_size_bytes": cursor_state.total_size_bytes,
                    "cursor_hash_algorithm": cursor_state.hash_algorithm.value,
                    "cursor_content_hash_hex": cursor_state.content_hash_hex,
                }
            )

        statement = text(
            f"""
            WITH grouped AS (
                SELECT
                    hash_algorithm,
                    lower(hex(content_hash)) AS content_hash_hex,
                    COUNT(1) AS file_count,
                    SUM(size_bytes) AS total_size_bytes,
                    SUM(size_bytes) - MIN(size_bytes) AS duplicate_waste_bytes,
                    MIN(id) AS sample_file_id
                FROM library_files INDEXED BY ix_library_files_dedup_group
                WHERE is_missing = 0
                  AND needs_hash = 0
                  AND hash_algorithm IS NOT NULL
                  AND content_hash IS NOT NULL
                GROUP BY hash_algorithm, content_hash
                HAVING COUNT(1) > 1
            )
            SELECT
                hash_algorithm,
                content_hash_hex,
                file_count,
                total_size_bytes,
                duplicate_waste_bytes,
                sample_file_id
            FROM grouped
            {where_clause}
            ORDER BY file_count DESC, total_size_bytes DESC, hash_algorithm ASC, content_hash_hex ASC
            LIMIT :limit_plus_one
            """
        )

        with self._session_factory() as session:
            rows = list(session.execute(statement, params).mappings().all())

        snapshots: list[DuplicateGroupSnapshot] = []
        for row in rows[:bounded_limit]:
            try:
                algorithm = HashAlgorithm(str(row["hash_algorithm"]).lower())
            except ValueError as exc:
                raise DuplicateQueryError("Invalid hash algorithm value found in duplicate group rows") from exc
            hash_hex = str(row["content_hash_hex"]).lower()
            snapshots.append(
                DuplicateGroupSnapshot(
                    group_key=f"{algorithm.value}:{hash_hex}",
                    hash_algorithm=algorithm,
                    content_hash_hex=hash_hex,
                    file_count=int(row["file_count"]),
                    total_size_bytes=int(row["total_size_bytes"]),
                    duplicate_waste_bytes=int(row["duplicate_waste_bytes"]),
                    sample_file_id=int(row["sample_file_id"]),
                )
            )

        next_cursor = self._encode_group_cursor(snapshots[-1]) if len(rows) > bounded_limit and snapshots else None
        return DuplicateGroupListResult(items=snapshots, next_cursor=next_cursor)

    def list_group_files(
        self,
        *,
        group_key: str,
        limit: int | None = None,
        cursor: str | None = None,
    ) -> DuplicateFileListResult:
        bounded_limit = self._normalize_limit(limit)
        cursor_id = self._normalize_file_cursor(cursor)
        algorithm, _hash_hex, hash_blob = self._parse_group_key(group_key)

        where_cursor = ""
        params: dict[str, Any] = {
            "hash_algorithm": algorithm.value,
            "content_hash": hash_blob,
            "limit_plus_one": bounded_limit + 1,
        }
        if cursor_id is not None:
            where_cursor = "AND lf.id > :cursor_id"
            params["cursor_id"] = cursor_id

        statement = text(
            f"""
            SELECT
                lf.id AS file_id,
                lf.library_id AS library_id,
                lr.name AS library_name,
                lf.relative_path AS relative_path,
                lf.size_bytes AS size_bytes,
                lf.mtime_ns AS mtime_ns,
                lf.hashed_at AS hashed_at
            FROM library_files AS lf INDEXED BY ix_library_files_dedup_group
            JOIN library_roots AS lr ON lr.id = lf.library_id
            WHERE lf.is_missing = 0
              AND lf.needs_hash = 0
              AND lf.hash_algorithm = :hash_algorithm
              AND lf.content_hash = :content_hash
              {where_cursor}
            ORDER BY lf.id ASC
            LIMIT :limit_plus_one
            """
        )

        with self._session_factory() as session:
            rows = list(session.execute(statement, params).mappings().all())

        snapshots = [
            DuplicateFileSnapshot(
                file_id=int(row["file_id"]),
                library_id=int(row["library_id"]),
                library_name=str(row["library_name"]),
                relative_path=str(row["relative_path"]),
                size_bytes=int(row["size_bytes"]),
                mtime_ns=int(row["mtime_ns"]),
                hashed_at=row["hashed_at"],
            )
            for row in rows[:bounded_limit]
        ]

        next_cursor = str(snapshots[-1].file_id) if len(rows) > bounded_limit and snapshots else None
        return DuplicateFileListResult(items=snapshots, next_cursor=next_cursor)


def duplicate_group_snapshot_to_dict(snapshot: DuplicateGroupSnapshot) -> dict[str, Any]:
    return {
        "group_key": snapshot.group_key,
        "hash_algorithm": snapshot.hash_algorithm.value,
        "content_hash_hex": snapshot.content_hash_hex,
        "file_count": snapshot.file_count,
        "total_size_bytes": snapshot.total_size_bytes,
        "duplicate_waste_bytes": snapshot.duplicate_waste_bytes,
        "sample_file_id": snapshot.sample_file_id,
    }


def duplicate_file_snapshot_to_dict(snapshot: DuplicateFileSnapshot) -> dict[str, Any]:
    return {
        "file_id": snapshot.file_id,
        "library_id": snapshot.library_id,
        "library_name": snapshot.library_name,
        "relative_path": snapshot.relative_path,
        "size_bytes": snapshot.size_bytes,
        "mtime_ns": snapshot.mtime_ns,
        "hashed_at": snapshot.hashed_at,
    }
