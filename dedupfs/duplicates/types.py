from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from dedupfs.db.models import HashAlgorithm


@dataclass(slots=True)
class DuplicateGroupSnapshot:
    group_key: str
    hash_algorithm: HashAlgorithm
    content_hash_hex: str
    file_count: int
    total_size_bytes: int
    duplicate_waste_bytes: int
    sample_file_id: int


@dataclass(slots=True)
class DuplicateFileSnapshot:
    file_id: int
    library_id: int
    library_name: str
    relative_path: str
    size_bytes: int
    mtime_ns: int
    hashed_at: datetime | None


@dataclass(slots=True)
class DuplicateGroupListResult:
    items: list[DuplicateGroupSnapshot]
    next_cursor: str | None


@dataclass(slots=True)
class DuplicateFileListResult:
    items: list[DuplicateFileSnapshot]
    next_cursor: str | None
