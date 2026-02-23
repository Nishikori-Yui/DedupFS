from __future__ import annotations

import hashlib
from datetime import datetime, timedelta, timezone
from pathlib import Path

from sqlalchemy import func, select, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, sessionmaker

from dedupfs.core.config import Settings
from dedupfs.core.path_safety import PathSafetyError, validate_library_relative_path
from dedupfs.db.models import (
    LibraryFile,
    LibraryRoot,
    Thumbnail,
    ThumbnailCleanupJob,
    ThumbnailCleanupStatus,
    ThumbnailFormat,
    ThumbnailMediaType,
    ThumbnailStatus,
)
from dedupfs.thumbs.types import ThumbnailCleanupSnapshot, ThumbnailSnapshot
from dedupfs.thumbs.types import ThumbnailMetricsSnapshot

_IMAGE_EXTENSIONS = {
    ".jpg",
    ".jpeg",
    ".png",
    ".bmp",
    ".gif",
    ".tif",
    ".tiff",
    ".webp",
}
_VIDEO_EXTENSIONS = {
    ".mp4",
    ".mov",
    ".m4v",
    ".avi",
    ".mkv",
    ".webm",
    ".mpeg",
    ".mpg",
    ".wmv",
}


class ThumbnailNotFoundError(RuntimeError):
    pass


class ThumbnailPolicyError(RuntimeError):
    pass


class ThumbnailQueueFullError(RuntimeError):
    pass


class ThumbnailCleanupError(RuntimeError):
    pass


class ThumbnailService:
    def __init__(self, settings: Settings, session_factory: sessionmaker[Session]):
        self._settings = settings
        self._session_factory = session_factory

    def _now(self) -> datetime:
        return datetime.now(tz=timezone.utc)

    def _coerce_utc(self, value: datetime | None) -> datetime | None:
        if value is None:
            return None
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value

    def _normalize_format(self, raw_format: str | None) -> ThumbnailFormat:
        if raw_format is None:
            return ThumbnailFormat(self._settings.thumbnail_default_format)
        normalized = raw_format.strip().lower()
        try:
            return ThumbnailFormat(normalized)
        except ValueError as exc:
            allowed = ", ".join(item.value for item in ThumbnailFormat)
            raise ThumbnailPolicyError(f"Unsupported thumbnail format: {raw_format}. Allowed: {allowed}") from exc

    def _normalize_dimension(self, requested: int | None) -> int:
        if requested is None:
            return int(self._settings.thumbnail_max_dimension)
        if requested <= 0:
            raise ThumbnailPolicyError("max_dimension must be greater than zero")
        if requested > int(self._settings.thumbnail_max_dimension):
            raise ThumbnailPolicyError(
                f"max_dimension exceeds configured limit {self._settings.thumbnail_max_dimension}"
            )
        return requested

    def _infer_media_type(self, relative_path: str) -> ThumbnailMediaType:
        extension = Path(relative_path).suffix.lower()
        if extension in _IMAGE_EXTENSIONS:
            return ThumbnailMediaType.IMAGE
        if extension in _VIDEO_EXTENSIONS:
            return ThumbnailMediaType.VIDEO
        raise ThumbnailPolicyError(f"Unsupported media type for thumbnail generation: {extension or '<none>'}")

    def _validate_source_path(self, root_path: str, relative_path: str) -> None:
        libraries_root = self._settings.libraries_root.resolve(strict=False)
        root_real = Path(root_path).resolve(strict=False)
        if root_real != libraries_root and libraries_root not in root_real.parents:
            raise ThumbnailPolicyError("Library root path escapes /libraries")

        rel = validate_library_relative_path(relative_path)
        candidate = (root_real / rel).resolve(strict=False)
        if candidate != root_real and root_real not in candidate.parents:
            raise ThumbnailPolicyError("Library file path escapes library root")

    def _build_group_key(self, item: LibraryFile) -> str | None:
        if item.hash_algorithm is None or item.content_hash is None:
            return None
        return f"{item.hash_algorithm.value}:{item.content_hash.hex()}"

    def _build_thumb_key(self, *, item: LibraryFile, max_dimension: int, output_format: ThumbnailFormat) -> str:
        if item.hash_algorithm is not None and item.content_hash is not None:
            source_fingerprint = f"{item.hash_algorithm.value}:{item.content_hash.hex()}"
        else:
            source_fingerprint = f"meta:{item.size_bytes}:{item.mtime_ns}"
        material = (
            f"{item.id}:{source_fingerprint}:{max_dimension}:{output_format.value}:thumb-v2"
        ).encode("utf-8")
        return hashlib.sha256(material).hexdigest()

    def _build_output_relpath(self, thumb_key: str, output_format: ThumbnailFormat) -> str:
        extension = "jpg" if output_format == ThumbnailFormat.JPEG else "webp"
        return f"{thumb_key[0:2]}/{thumb_key[2:4]}/{thumb_key}.{extension}"

    def _to_snapshot(self, item: Thumbnail) -> ThumbnailSnapshot:
        return ThumbnailSnapshot(
            id=item.id,
            thumb_key=item.thumb_key,
            file_id=item.file_id,
            group_key=item.group_key,
            status=item.status,
            media_type=item.media_type,
            format=item.format,
            max_dimension=item.max_dimension,
            version=item.version,
            source_size_bytes=item.source_size_bytes,
            source_mtime_ns=item.source_mtime_ns,
            output_relpath=item.output_relpath,
            width=item.width,
            height=item.height,
            bytes_size=item.bytes_size,
            error_code=item.error_code,
            error_message=item.error_message,
            error_count=item.error_count,
            retry_after=item.retry_after,
            worker_id=item.worker_id,
            worker_heartbeat_at=item.worker_heartbeat_at,
            lease_expires_at=item.lease_expires_at,
            created_at=item.created_at,
            updated_at=item.updated_at,
            started_at=item.started_at,
            finished_at=item.finished_at,
        )

    def _to_cleanup_snapshot(self, item: ThumbnailCleanupJob) -> ThumbnailCleanupSnapshot:
        return ThumbnailCleanupSnapshot(
            id=item.id,
            group_key=item.group_key,
            status=item.status,
            execute_after=item.execute_after,
            worker_id=item.worker_id,
            worker_heartbeat_at=item.worker_heartbeat_at,
            lease_expires_at=item.lease_expires_at,
            error_code=item.error_code,
            error_message=item.error_message,
            created_at=item.created_at,
            updated_at=item.updated_at,
            finished_at=item.finished_at,
        )

    def request_thumbnail(
        self,
        *,
        file_id: int,
        max_dimension: int | None = None,
        output_format: str | None = None,
    ) -> ThumbnailSnapshot:
        normalized_format = self._normalize_format(output_format)
        normalized_dimension = self._normalize_dimension(max_dimension)
        now = self._now()

        with self._session_factory() as session:
            item = session.get(LibraryFile, file_id)
            if item is None:
                raise ThumbnailNotFoundError(f"File not found: {file_id}")
            if item.is_missing:
                raise ThumbnailPolicyError("Missing files cannot be thumbnailed")

            root = session.get(LibraryRoot, item.library_id)
            if root is None:
                raise ThumbnailPolicyError(f"Library root missing for file {file_id}")

            self._validate_source_path(root.root_path, item.relative_path)
            media_type = self._infer_media_type(item.relative_path)
            thumb_key = self._build_thumb_key(
                item=item,
                max_dimension=normalized_dimension,
                output_format=normalized_format,
            )
            output_relpath = self._build_output_relpath(thumb_key, normalized_format)
            group_key = self._build_group_key(item)

            existing = session.scalar(select(Thumbnail).where(Thumbnail.thumb_key == thumb_key))
            if existing is not None:
                if existing.status == ThumbnailStatus.FAILED:
                    retry_after = self._coerce_utc(existing.retry_after)
                    if retry_after is None or retry_after <= now:
                        existing.status = ThumbnailStatus.PENDING
                        existing.error_code = None
                        existing.error_message = None
                        existing.retry_after = None
                        existing.worker_id = None
                        existing.worker_heartbeat_at = None
                        existing.lease_expires_at = None
                        existing.started_at = None
                        existing.finished_at = None
                        existing.updated_at = now
                        session.commit()
                        session.refresh(existing)
                return self._to_snapshot(existing)

            queued = Thumbnail(
                thumb_key=thumb_key,
                file_id=item.id,
                group_key=group_key,
                status=ThumbnailStatus.PENDING,
                media_type=media_type,
                format=normalized_format,
                max_dimension=normalized_dimension,
                version=2,
                source_size_bytes=item.size_bytes,
                source_mtime_ns=item.mtime_ns,
                output_relpath=output_relpath,
            )

            try:
                insert_result = session.execute(
                    text(
                        """
                        INSERT INTO thumbnails (
                            thumb_key,
                            file_id,
                            group_key,
                            status,
                            media_type,
                            format,
                            max_dimension,
                            version,
                            source_size_bytes,
                            source_mtime_ns,
                            output_relpath,
                            error_count
                        )
                        SELECT
                            :thumb_key,
                            :file_id,
                            :group_key,
                            'pending',
                            :media_type,
                            :format,
                            :max_dimension,
                            :version,
                            :source_size_bytes,
                            :source_mtime_ns,
                            :output_relpath,
                            0
                        WHERE (
                            SELECT COUNT(1)
                            FROM thumbnails
                            WHERE status IN ('pending', 'running')
                        ) < :queue_capacity
                        """
                    ),
                    {
                        "thumb_key": queued.thumb_key,
                        "file_id": queued.file_id,
                        "group_key": queued.group_key,
                        "media_type": queued.media_type.value,
                        "format": queued.format.value,
                        "max_dimension": queued.max_dimension,
                        "version": queued.version,
                        "source_size_bytes": queued.source_size_bytes,
                        "source_mtime_ns": queued.source_mtime_ns,
                        "output_relpath": queued.output_relpath,
                        "queue_capacity": int(self._settings.thumbnail_queue_capacity),
                    },
                )
                _ = insert_result
                inserted_row = session.scalar(select(Thumbnail).where(Thumbnail.thumb_key == thumb_key))
                if inserted_row is None:
                    session.rollback()
                    existing = session.scalar(select(Thumbnail).where(Thumbnail.thumb_key == thumb_key))
                    if existing is not None:
                        return self._to_snapshot(existing)
                    raise ThumbnailQueueFullError("Thumbnail queue is at capacity; please retry later")
                session.commit()
            except IntegrityError:
                session.rollback()
                existing = session.scalar(select(Thumbnail).where(Thumbnail.thumb_key == thumb_key))
                if existing is None:
                    raise
                return self._to_snapshot(existing)

            created = session.scalar(select(Thumbnail).where(Thumbnail.thumb_key == thumb_key))
            if created is None:
                raise ThumbnailPolicyError("Failed to load newly queued thumbnail task")
            return self._to_snapshot(created)

    def get_thumbnail(self, thumb_key: str) -> ThumbnailSnapshot:
        with self._session_factory() as session:
            item = session.scalar(select(Thumbnail).where(Thumbnail.thumb_key == thumb_key))
            if item is None:
                raise ThumbnailNotFoundError(f"Thumbnail not found: {thumb_key}")
            return self._to_snapshot(item)

    def resolve_thumbnail_output_path(self, snapshot: ThumbnailSnapshot) -> Path:
        if snapshot.output_relpath is None:
            raise ThumbnailPolicyError("Thumbnail output path is empty")

        relative = validate_library_relative_path(snapshot.output_relpath)
        root = self._settings.thumbs_root.resolve(strict=False)
        candidate = (root / relative).resolve(strict=False)
        if candidate != root and root not in candidate.parents:
            raise ThumbnailPolicyError("Thumbnail output path escapes thumbs root")
        return candidate

    def schedule_group_cleanup(
        self,
        *,
        group_key: str,
        delay_seconds: int | None = None,
    ) -> ThumbnailCleanupSnapshot:
        normalized_group = group_key.strip()
        if not normalized_group:
            raise ThumbnailCleanupError("group_key cannot be blank")

        if delay_seconds is not None and delay_seconds < 0:
            raise ThumbnailCleanupError("delay_seconds cannot be negative")

        delay = delay_seconds if delay_seconds is not None else int(self._settings.thumbnail_cleanup_delay_seconds)
        now = self._now()
        execute_after = now + timedelta(seconds=delay)

        with self._session_factory() as session:
            record = session.scalar(
                select(ThumbnailCleanupJob).where(ThumbnailCleanupJob.group_key == normalized_group)
            )
            if record is None:
                record = ThumbnailCleanupJob(
                    group_key=normalized_group,
                    status=ThumbnailCleanupStatus.PENDING,
                    execute_after=execute_after,
                )
                session.add(record)
            else:
                record.status = ThumbnailCleanupStatus.PENDING
                record.execute_after = execute_after
                record.worker_id = None
                record.worker_heartbeat_at = None
                record.lease_expires_at = None
                record.error_code = None
                record.error_message = None
                record.finished_at = None
                record.updated_at = now

            session.commit()
            session.refresh(record)
            return self._to_cleanup_snapshot(record)

    def prune_group_thumbnails(self, *, group_key: str) -> int:
        normalized_group = group_key.strip()
        if not normalized_group:
            raise ThumbnailCleanupError("group_key cannot be blank")

        with self._session_factory() as session:
            rows = list(
                session.scalars(
                    select(Thumbnail).where(
                        Thumbnail.group_key == normalized_group,
                        Thumbnail.status.in_([ThumbnailStatus.READY, ThumbnailStatus.FAILED]),
                    )
                ).all()
            )
            for row in rows:
                if not row.output_relpath:
                    continue
                try:
                    path = self.resolve_thumbnail_output_path(self._to_snapshot(row))
                except (ThumbnailPolicyError, PathSafetyError):
                    continue
                if path.exists():
                    path.unlink(missing_ok=True)

            deleted = (
                session.query(Thumbnail)
                .filter(
                    Thumbnail.group_key == normalized_group,
                    Thumbnail.status.in_([ThumbnailStatus.READY, ThumbnailStatus.FAILED]),
                )
                .delete()
            )
            session.commit()
            return int(deleted)

    def get_metrics(self) -> ThumbnailMetricsSnapshot:
        now = self._now()
        with self._session_factory() as session:
            queue_pending = int(
                session.scalar(
                    select(func.count(Thumbnail.id)).where(Thumbnail.status == ThumbnailStatus.PENDING)
                )
                or 0
            )
            queue_running = int(
                session.scalar(
                    select(func.count(Thumbnail.id)).where(Thumbnail.status == ThumbnailStatus.RUNNING)
                )
                or 0
            )
            retry_backlog = int(
                session.scalar(
                    select(func.count(Thumbnail.id)).where(
                        Thumbnail.status == ThumbnailStatus.FAILED,
                        Thumbnail.retry_after.is_not(None),
                        Thumbnail.retry_after > now,
                    )
                )
                or 0
            )
            retry_ready = int(
                session.scalar(
                    select(func.count(Thumbnail.id)).where(
                        Thumbnail.status == ThumbnailStatus.FAILED,
                        func.coalesce(Thumbnail.retry_after, now) <= now,
                    )
                )
                or 0
            )
            cleanup_pending = int(
                session.scalar(
                    select(func.count(ThumbnailCleanupJob.id)).where(
                        ThumbnailCleanupJob.status == ThumbnailCleanupStatus.PENDING
                    )
                )
                or 0
            )
            cleanup_running = int(
                session.scalar(
                    select(func.count(ThumbnailCleanupJob.id)).where(
                        ThumbnailCleanupJob.status == ThumbnailCleanupStatus.RUNNING
                    )
                )
                or 0
            )
            cleanup_overdue = int(
                session.scalar(
                    select(func.count(ThumbnailCleanupJob.id)).where(
                        ThumbnailCleanupJob.status == ThumbnailCleanupStatus.PENDING,
                        ThumbnailCleanupJob.execute_after <= now,
                    )
                )
                or 0
            )
            oldest_due = session.scalar(
                select(func.min(ThumbnailCleanupJob.execute_after)).where(
                    ThumbnailCleanupJob.status == ThumbnailCleanupStatus.PENDING,
                    ThumbnailCleanupJob.execute_after <= now,
                )
            )

        oldest_due_utc = self._coerce_utc(oldest_due)
        cleanup_max_lag_seconds = 0
        if oldest_due_utc is not None:
            cleanup_max_lag_seconds = max(0, int((now - oldest_due_utc).total_seconds()))

        return ThumbnailMetricsSnapshot(
            generated_at=now,
            queue_depth=queue_pending + queue_running,
            queue_pending=queue_pending,
            queue_running=queue_running,
            retry_backlog=retry_backlog,
            retry_ready=retry_ready,
            cleanup_pending=cleanup_pending,
            cleanup_running=cleanup_running,
            cleanup_overdue=cleanup_overdue,
            cleanup_max_lag_seconds=cleanup_max_lag_seconds,
        )


def thumbnail_snapshot_to_dict(snapshot: ThumbnailSnapshot) -> dict[str, object]:
    return {
        "id": snapshot.id,
        "thumb_key": snapshot.thumb_key,
        "file_id": snapshot.file_id,
        "group_key": snapshot.group_key,
        "status": snapshot.status.value,
        "media_type": snapshot.media_type.value,
        "format": snapshot.format.value,
        "max_dimension": snapshot.max_dimension,
        "version": snapshot.version,
        "source_size_bytes": snapshot.source_size_bytes,
        "source_mtime_ns": snapshot.source_mtime_ns,
        "output_relpath": snapshot.output_relpath,
        "width": snapshot.width,
        "height": snapshot.height,
        "bytes_size": snapshot.bytes_size,
        "error_code": snapshot.error_code,
        "error_message": snapshot.error_message,
        "error_count": snapshot.error_count,
        "retry_after": snapshot.retry_after,
        "worker_id": snapshot.worker_id,
        "worker_heartbeat_at": snapshot.worker_heartbeat_at,
        "lease_expires_at": snapshot.lease_expires_at,
        "created_at": snapshot.created_at,
        "updated_at": snapshot.updated_at,
        "started_at": snapshot.started_at,
        "finished_at": snapshot.finished_at,
    }


def thumbnail_cleanup_snapshot_to_dict(snapshot: ThumbnailCleanupSnapshot) -> dict[str, object]:
    return {
        "id": snapshot.id,
        "group_key": snapshot.group_key,
        "status": snapshot.status.value,
        "execute_after": snapshot.execute_after,
        "worker_id": snapshot.worker_id,
        "worker_heartbeat_at": snapshot.worker_heartbeat_at,
        "lease_expires_at": snapshot.lease_expires_at,
        "error_code": snapshot.error_code,
        "error_message": snapshot.error_message,
        "created_at": snapshot.created_at,
        "updated_at": snapshot.updated_at,
        "finished_at": snapshot.finished_at,
    }


def thumbnail_metrics_snapshot_to_dict(snapshot: ThumbnailMetricsSnapshot) -> dict[str, object]:
    return {
        "generated_at": snapshot.generated_at,
        "queue_depth": snapshot.queue_depth,
        "queue_pending": snapshot.queue_pending,
        "queue_running": snapshot.queue_running,
        "retry_backlog": snapshot.retry_backlog,
        "retry_ready": snapshot.retry_ready,
        "cleanup_pending": snapshot.cleanup_pending,
        "cleanup_running": snapshot.cleanup_running,
        "cleanup_overdue": snapshot.cleanup_overdue,
        "cleanup_max_lag_seconds": snapshot.cleanup_max_lag_seconds,
    }
