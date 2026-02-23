from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel


class DuplicateGroupResponse(BaseModel):
    group_key: str
    hash_algorithm: str
    content_hash_hex: str
    file_count: int
    total_size_bytes: int
    duplicate_waste_bytes: int
    sample_file_id: int


class DuplicateGroupListResponse(BaseModel):
    items: list[DuplicateGroupResponse]
    next_cursor: str | None


class DuplicateFileResponse(BaseModel):
    file_id: int
    library_id: int
    library_name: str
    relative_path: str
    size_bytes: int
    mtime_ns: int
    hashed_at: datetime | None


class DuplicateFileListResponse(BaseModel):
    items: list[DuplicateFileResponse]
    next_cursor: str | None
