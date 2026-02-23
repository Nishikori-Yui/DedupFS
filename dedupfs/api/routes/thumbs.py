from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.responses import FileResponse

from dedupfs.api.schemas.thumbs import (
    RequestThumbnailRequest,
    ScheduleGroupCleanupRequest,
    ThumbnailCleanupResponse,
    ThumbnailMetricsResponse,
    ThumbnailResponse,
)
from dedupfs.core.config import get_settings
from dedupfs.db.session import get_session_factory
from dedupfs.thumbs.service import (
    ThumbnailCleanupError,
    ThumbnailNotFoundError,
    ThumbnailPolicyError,
    ThumbnailQueueFullError,
    ThumbnailService,
    thumbnail_cleanup_snapshot_to_dict,
    thumbnail_metrics_snapshot_to_dict,
    thumbnail_snapshot_to_dict,
)

router = APIRouter(prefix="/thumbs", tags=["thumbs"])


def get_thumbnail_service() -> ThumbnailService:
    return ThumbnailService(settings=get_settings(), session_factory=get_session_factory())


@router.post("/request", response_model=ThumbnailResponse, status_code=status.HTTP_202_ACCEPTED)
def request_thumbnail(
    request: RequestThumbnailRequest,
    service: ThumbnailService = Depends(get_thumbnail_service),
) -> ThumbnailResponse:
    try:
        snapshot = service.request_thumbnail(
            file_id=request.file_id,
            max_dimension=request.max_dimension,
            output_format=request.output_format,
        )
    except ThumbnailNotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    except ThumbnailQueueFullError as exc:
        raise HTTPException(status_code=status.HTTP_429_TOO_MANY_REQUESTS, detail=str(exc)) from exc
    except (ThumbnailPolicyError, ValueError) as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc

    payload = thumbnail_snapshot_to_dict(snapshot)
    payload["content_url"] = (
        f"/api/v1/thumbs/{snapshot.thumb_key}/content" if snapshot.status.value == "ready" else None
    )
    return ThumbnailResponse.model_validate(payload)


@router.get("/metrics", response_model=ThumbnailMetricsResponse)
def get_thumbnail_metrics(service: ThumbnailService = Depends(get_thumbnail_service)) -> ThumbnailMetricsResponse:
    snapshot = service.get_metrics()
    return ThumbnailMetricsResponse.model_validate(thumbnail_metrics_snapshot_to_dict(snapshot))


@router.get("/{thumb_key}", response_model=ThumbnailResponse)
def get_thumbnail(thumb_key: str, service: ThumbnailService = Depends(get_thumbnail_service)) -> ThumbnailResponse:
    try:
        snapshot = service.get_thumbnail(thumb_key)
    except ThumbnailNotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc

    payload = thumbnail_snapshot_to_dict(snapshot)
    payload["content_url"] = (
        f"/api/v1/thumbs/{snapshot.thumb_key}/content" if snapshot.status.value == "ready" else None
    )
    return ThumbnailResponse.model_validate(payload)


@router.get("/{thumb_key}/content")
def get_thumbnail_content(thumb_key: str, service: ThumbnailService = Depends(get_thumbnail_service)) -> FileResponse:
    try:
        snapshot = service.get_thumbnail(thumb_key)
    except ThumbnailNotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc

    if snapshot.status.value != "ready":
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Thumbnail is not ready")

    try:
        output_path = service.resolve_thumbnail_output_path(snapshot)
    except ThumbnailPolicyError as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc

    if not output_path.is_file():
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Thumbnail file missing")

    media_type = "image/jpeg" if snapshot.format.value == "jpeg" else "image/webp"
    return FileResponse(path=output_path, media_type=media_type)


@router.post("/cleanup/group", response_model=ThumbnailCleanupResponse)
def schedule_group_cleanup(
    request: ScheduleGroupCleanupRequest,
    service: ThumbnailService = Depends(get_thumbnail_service),
) -> ThumbnailCleanupResponse:
    try:
        snapshot = service.schedule_group_cleanup(
            group_key=request.group_key,
            delay_seconds=request.delay_seconds,
        )
    except ThumbnailCleanupError as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc

    return ThumbnailCleanupResponse.model_validate(thumbnail_cleanup_snapshot_to_dict(snapshot))
