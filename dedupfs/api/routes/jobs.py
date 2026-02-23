from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query, status

from dedupfs.api.schemas.jobs import (
    CancelJobRequest,
    ClaimJobRequest,
    CreateJobRequest,
    FinishJobRequest,
    JobListResponse,
    JobProgressRequest,
    JobResponse,
)
from dedupfs.core.config import get_settings
from dedupfs.db.session import get_session_factory
from dedupfs.jobs.service import (
    InvalidJobStateError,
    JobConflictError,
    JobNotFoundError,
    JobPolicyError,
    JobService,
    snapshot_to_dict,
)

router = APIRouter(prefix="/jobs", tags=["jobs"])


def get_job_service() -> JobService:
    return JobService(settings=get_settings(), session_factory=get_session_factory())


@router.post("", response_model=JobResponse, status_code=status.HTTP_201_CREATED)
def create_job(request: CreateJobRequest, service: JobService = Depends(get_job_service)) -> JobResponse:
    try:
        job = service.create_job(kind=request.kind, payload=request.payload, dry_run=request.dry_run)
    except (JobPolicyError, JobConflictError) as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc
    return JobResponse.model_validate(snapshot_to_dict(job))


@router.get("", response_model=JobListResponse)
def list_jobs(
    limit: int = Query(default=50, ge=1, le=200),
    cursor: str | None = None,
    service: JobService = Depends(get_job_service),
) -> JobListResponse:
    try:
        result = service.list_jobs(limit=limit, cursor=cursor)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(exc)) from exc
    return JobListResponse(items=[JobResponse.model_validate(snapshot_to_dict(item)) for item in result.items], next_cursor=result.next_cursor)


@router.get("/{job_id}", response_model=JobResponse)
def get_job(job_id: str, service: JobService = Depends(get_job_service)) -> JobResponse:
    try:
        job = service.get_job(job_id)
    except JobNotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    return JobResponse.model_validate(snapshot_to_dict(job))


@router.post("/scan-hash/claim", response_model=JobResponse)
def claim_scan_hash_job(request: ClaimJobRequest, service: JobService = Depends(get_job_service)) -> JobResponse:
    try:
        job = service.claim_pending_scan_hash_job(worker_id=request.worker_id)
    except JobConflictError as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc
    if job is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No pending scan/hash job available")
    return JobResponse.model_validate(snapshot_to_dict(job))


@router.post("/{job_id}/heartbeat", response_model=JobResponse)
def heartbeat_job(job_id: str, request: JobProgressRequest, service: JobService = Depends(get_job_service)) -> JobResponse:
    try:
        job = service.heartbeat(
            job_id,
            worker_id=request.worker_id,
            progress=request.progress,
            processed_items=request.processed_items,
        )
    except JobNotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    except (JobConflictError, InvalidJobStateError) as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(exc)) from exc
    return JobResponse.model_validate(snapshot_to_dict(job))


@router.post("/{job_id}/finish", response_model=JobResponse)
def finish_job(job_id: str, request: FinishJobRequest, service: JobService = Depends(get_job_service)) -> JobResponse:
    try:
        job = service.finish_job(
            job_id,
            worker_id=request.worker_id,
            success=request.success,
            error_message=request.error_message,
        )
    except JobNotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    except (InvalidJobStateError, JobConflictError) as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(exc)) from exc
    return JobResponse.model_validate(snapshot_to_dict(job))


@router.post("/{job_id}/cancel", response_model=JobResponse)
def cancel_job(job_id: str, request: CancelJobRequest, service: JobService = Depends(get_job_service)) -> JobResponse:
    try:
        job = service.cancel_job(job_id, error_message=request.error_message)
    except JobNotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    except InvalidJobStateError as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc
    return JobResponse.model_validate(snapshot_to_dict(job))


@router.post("/{job_id}/reset", response_model=JobResponse)
def reset_retryable_job(job_id: str, service: JobService = Depends(get_job_service)) -> JobResponse:
    try:
        job = service.reset_retryable_job(job_id)
    except JobNotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    except InvalidJobStateError as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc
    return JobResponse.model_validate(snapshot_to_dict(job))


@router.post("/recover-stale")
def recover_stale_jobs(service: JobService = Depends(get_job_service)) -> dict[str, int]:
    recovered = service.recover_stale_jobs()
    return {"recovered": recovered}
