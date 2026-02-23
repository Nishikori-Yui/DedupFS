from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query, status

from dedupfs.api.schemas.duplicates import (
    DuplicateFileListResponse,
    DuplicateFileResponse,
    DuplicateGroupListResponse,
    DuplicateGroupResponse,
)
from dedupfs.core.config import get_settings
from dedupfs.db.session import get_session_factory
from dedupfs.duplicates.service import (
    DuplicateQueryError,
    DuplicateService,
    duplicate_file_snapshot_to_dict,
    duplicate_group_snapshot_to_dict,
)

router = APIRouter(prefix="/duplicates", tags=["duplicates"])


def get_duplicate_service() -> DuplicateService:
    return DuplicateService(settings=get_settings(), session_factory=get_session_factory())


@router.get("/groups", response_model=DuplicateGroupListResponse)
def list_duplicate_groups(
    limit: int = Query(default=100, ge=1, le=1000),
    cursor: str | None = None,
    service: DuplicateService = Depends(get_duplicate_service),
) -> DuplicateGroupListResponse:
    try:
        result = service.list_groups(limit=limit, cursor=cursor)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(exc)) from exc
    except DuplicateQueryError as exc:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc)) from exc

    return DuplicateGroupListResponse(
        items=[DuplicateGroupResponse.model_validate(duplicate_group_snapshot_to_dict(item)) for item in result.items],
        next_cursor=result.next_cursor,
    )


@router.get("/groups/{group_key}/files", response_model=DuplicateFileListResponse)
def list_duplicate_group_files(
    group_key: str,
    limit: int = Query(default=100, ge=1, le=1000),
    cursor: str | None = None,
    service: DuplicateService = Depends(get_duplicate_service),
) -> DuplicateFileListResponse:
    try:
        result = service.list_group_files(group_key=group_key, limit=limit, cursor=cursor)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(exc)) from exc

    return DuplicateFileListResponse(
        items=[DuplicateFileResponse.model_validate(duplicate_file_snapshot_to_dict(item)) for item in result.items],
        next_cursor=result.next_cursor,
    )
