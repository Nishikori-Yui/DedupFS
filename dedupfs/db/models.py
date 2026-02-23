from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any

from sqlalchemy import (
    JSON,
    BigInteger,
    Boolean,
    DateTime,
    Enum as SAEnum,
    Float,
    ForeignKey,
    Index,
    Integer,
    LargeBinary,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


def _enum_values(enum_cls: type[Enum]) -> list[str]:
    return [member.value for member in enum_cls]


class JobKind(str, Enum):
    SCAN = "scan"
    HASH = "hash"
    DELETE = "delete"
    THUMBNAIL = "thumbnail"


class JobStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    RETRYABLE = "retryable"


class HashAlgorithm(str, Enum):
    BLAKE3 = "blake3"
    SHA256 = "sha256"


class ThumbnailStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    READY = "ready"
    FAILED = "failed"


class ThumbnailMediaType(str, Enum):
    IMAGE = "image"
    VIDEO = "video"


class ThumbnailFormat(str, Enum):
    JPEG = "jpeg"
    WEBP = "webp"


class ThumbnailCleanupStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


class ScanSessionStatus(str, Enum):
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


class WalCheckpointMode(str, Enum):
    PASSIVE = "passive"
    RESTART = "restart"
    TRUNCATE = "truncate"


class WalMaintenanceStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    RETRYABLE = "retryable"


class Job(Base):
    __tablename__ = "jobs"

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    kind: Mapped[JobKind] = mapped_column(
        SAEnum(JobKind, native_enum=False, values_callable=_enum_values),
        nullable=False,
    )
    status: Mapped[JobStatus] = mapped_column(
        SAEnum(JobStatus, native_enum=False, values_callable=_enum_values),
        nullable=False,
        default=JobStatus.PENDING,
    )
    dry_run: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    worker_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    worker_heartbeat_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    progress: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    total_items: Mapped[int | None] = mapped_column(Integer, nullable=True)
    processed_items: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    payload: Mapped[dict[str, Any]] = mapped_column(JSON(none_as_null=True), nullable=False, default=dict)
    error_code: Mapped[str | None] = mapped_column(String(64), nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now()
    )
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    __table_args__ = (
        Index("ix_jobs_kind_status", "kind", "status"),
        Index("ix_jobs_running_lease", "status", "lease_expires_at"),
        Index("ix_jobs_created_at", "created_at"),
        Index("ix_jobs_created_id", "created_at", "id"),
        Index("ix_jobs_status_updated", "status", "updated_at"),
    )


class JobLock(Base):
    __tablename__ = "job_locks"

    lock_key: Mapped[str] = mapped_column(String(100), primary_key=True)
    owner_job_id: Mapped[str] = mapped_column(String(36), ForeignKey("jobs.id", ondelete="CASCADE"), nullable=False)
    acquired_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    heartbeat_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    __table_args__ = (
        Index("ix_job_locks_owner_job_id", "owner_job_id"),
        Index("ix_job_locks_expires_at", "expires_at"),
    )


class LibraryRoot(Base):
    __tablename__ = "library_roots"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False, unique=True)
    root_path: Mapped[str] = mapped_column(String(2048), nullable=False, unique=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now()
    )
    last_scanned_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    __table_args__ = (Index("ix_library_roots_last_scanned_at", "last_scanned_at"),)


class ScanSession(Base):
    __tablename__ = "scan_sessions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    status: Mapped[ScanSessionStatus] = mapped_column(
        SAEnum(ScanSessionStatus, native_enum=False, values_callable=_enum_values),
        nullable=False,
        default=ScanSessionStatus.RUNNING,
    )
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)

    files_seen: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    directories_seen: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    bytes_seen: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    error_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    __table_args__ = (
        Index("ix_scan_sessions_status_started", "status", "started_at"),
        Index("ix_scan_sessions_finished_at", "finished_at"),
    )


class LibraryFile(Base):
    __tablename__ = "library_files"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    library_id: Mapped[int] = mapped_column(Integer, ForeignKey("library_roots.id", ondelete="CASCADE"), nullable=False)
    relative_path: Mapped[str] = mapped_column(String(4096), nullable=False)

    size_bytes: Mapped[int] = mapped_column(BigInteger, nullable=False)
    mtime_ns: Mapped[int] = mapped_column(BigInteger, nullable=False)
    inode: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    device: Mapped[int | None] = mapped_column(BigInteger, nullable=True)

    is_missing: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    needs_hash: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    last_seen_scan_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey("scan_sessions.id", ondelete="SET NULL"),
        nullable=True,
    )

    hash_algorithm: Mapped[HashAlgorithm | None] = mapped_column(
        SAEnum(HashAlgorithm, native_enum=False, values_callable=_enum_values),
        nullable=True,
    )
    content_hash: Mapped[bytes | None] = mapped_column(LargeBinary, nullable=True)
    hashed_size_bytes: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    hashed_mtime_ns: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    hashed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    hash_error_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    hash_last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    hash_last_error_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    hash_retry_after: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    hash_claim_token: Mapped[str | None] = mapped_column(String(64), nullable=True)
    hash_claimed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now()
    )

    __table_args__ = (
        UniqueConstraint("library_id", "relative_path", name="uq_library_files_library_id_relative_path"),
        Index("ix_library_files_library_seen", "library_id", "last_seen_scan_id"),
        Index("ix_library_files_needs_hash", "needs_hash", "is_missing", "id"),
        Index("ix_library_files_hash_lookup", "hash_algorithm", "content_hash", "size_bytes", "is_missing"),
        Index("ix_library_files_dedup_group", "is_missing", "needs_hash", "hash_algorithm", "content_hash", "id"),
        Index("ix_library_files_library_path", "library_id", "relative_path"),
        Index("ix_library_files_library_mtime_size", "library_id", "mtime_ns", "size_bytes"),
        Index("ix_library_files_hash_retry", "needs_hash", "is_missing", "hash_retry_after", "id"),
        Index("ix_library_files_hash_claimed", "hash_claim_token", "hash_claimed_at"),
    )


class Thumbnail(Base):
    __tablename__ = "thumbnails"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    thumb_key: Mapped[str] = mapped_column(String(128), nullable=False, unique=True)
    file_id: Mapped[int] = mapped_column(Integer, ForeignKey("library_files.id", ondelete="CASCADE"), nullable=False)
    group_key: Mapped[str | None] = mapped_column(String(256), nullable=True)
    status: Mapped[ThumbnailStatus] = mapped_column(
        SAEnum(ThumbnailStatus, native_enum=False, values_callable=_enum_values),
        nullable=False,
        default=ThumbnailStatus.PENDING,
    )
    media_type: Mapped[ThumbnailMediaType] = mapped_column(
        SAEnum(ThumbnailMediaType, native_enum=False, values_callable=_enum_values),
        nullable=False,
    )
    format: Mapped[ThumbnailFormat] = mapped_column(
        SAEnum(ThumbnailFormat, native_enum=False, values_callable=_enum_values),
        nullable=False,
        default=ThumbnailFormat.JPEG,
    )
    max_dimension: Mapped[int] = mapped_column(Integer, nullable=False, default=256)
    version: Mapped[int] = mapped_column(Integer, nullable=False, default=1)

    source_size_bytes: Mapped[int] = mapped_column(BigInteger, nullable=False)
    source_mtime_ns: Mapped[int] = mapped_column(BigInteger, nullable=False)

    output_relpath: Mapped[str | None] = mapped_column(String(1024), nullable=True)
    width: Mapped[int | None] = mapped_column(Integer, nullable=True)
    height: Mapped[int | None] = mapped_column(Integer, nullable=True)
    bytes_size: Mapped[int | None] = mapped_column(BigInteger, nullable=True)

    error_code: Mapped[str | None] = mapped_column(String(64), nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    error_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    retry_after: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    worker_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    worker_heartbeat_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now()
    )
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    __table_args__ = (
        Index("ix_thumbnails_status_retry", "status", "retry_after", "id"),
        Index("ix_thumbnails_file_variant", "file_id", "max_dimension", "format"),
        Index("ix_thumbnails_group_status", "group_key", "status"),
        Index("ix_thumbnails_running_lease", "status", "lease_expires_at"),
        Index("ix_thumbnails_updated", "updated_at"),
    )


class ThumbnailCleanupJob(Base):
    __tablename__ = "thumbnail_cleanup_jobs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    group_key: Mapped[str] = mapped_column(String(256), nullable=False, unique=True)
    status: Mapped[ThumbnailCleanupStatus] = mapped_column(
        SAEnum(ThumbnailCleanupStatus, native_enum=False, values_callable=_enum_values),
        nullable=False,
        default=ThumbnailCleanupStatus.PENDING,
    )
    execute_after: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())

    worker_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    worker_heartbeat_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    error_code: Mapped[str | None] = mapped_column(String(64), nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now()
    )
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    __table_args__ = (
        Index("ix_thumbnail_cleanup_status_execute", "status", "execute_after"),
        Index("ix_thumbnail_cleanup_running_lease", "status", "lease_expires_at"),
        Index("ix_thumbnail_cleanup_updated", "updated_at"),
    )


class WalMaintenanceJob(Base):
    __tablename__ = "wal_maintenance_jobs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    requested_mode: Mapped[WalCheckpointMode] = mapped_column(
        SAEnum(WalCheckpointMode, native_enum=False, values_callable=_enum_values),
        nullable=False,
        default=WalCheckpointMode.PASSIVE,
    )
    status: Mapped[WalMaintenanceStatus] = mapped_column(
        SAEnum(WalMaintenanceStatus, native_enum=False, values_callable=_enum_values),
        nullable=False,
        default=WalMaintenanceStatus.PENDING,
    )
    requested_by: Mapped[str | None] = mapped_column(String(64), nullable=True)
    reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    execute_after: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    retry_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    retry_after: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    worker_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    worker_heartbeat_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    checkpoint_busy: Mapped[int | None] = mapped_column(Integer, nullable=True)
    checkpoint_log_frames: Mapped[int | None] = mapped_column(Integer, nullable=True)
    checkpointed_frames: Mapped[int | None] = mapped_column(Integer, nullable=True)

    error_code: Mapped[str | None] = mapped_column(String(64), nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now()
    )
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    __table_args__ = (
        Index("ix_wal_jobs_status_execute", "status", "execute_after", "id"),
        Index("ix_wal_jobs_retry_after", "status", "retry_after", "id"),
        Index("ix_wal_jobs_running_lease", "status", "lease_expires_at"),
        Index("ix_wal_jobs_created_at", "created_at"),
    )


class SchemaMigration(Base):
    __tablename__ = "schema_migrations"

    version: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    applied_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
