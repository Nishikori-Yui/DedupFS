from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, status

from dedupfs.api.schemas.maintenance import (
    WalCheckpointRequest,
    WalMaintenanceMetricsResponse,
    WalMaintenanceResponse,
)
from dedupfs.core.config import get_settings
from dedupfs.db.session import get_session_factory
from dedupfs.maintenance.service import (
    WalMaintenanceConflictError,
    WalMaintenanceNotFoundError,
    WalMaintenancePolicyError,
    WalMaintenanceService,
    wal_maintenance_metrics_to_dict,
    wal_maintenance_snapshot_to_dict,
)

router = APIRouter(prefix="/maintenance", tags=["maintenance"])


def get_wal_maintenance_service() -> WalMaintenanceService:
    return WalMaintenanceService(settings=get_settings(), session_factory=get_session_factory())


@router.post("/wal/checkpoint", response_model=WalMaintenanceResponse, status_code=status.HTTP_202_ACCEPTED)
def request_wal_checkpoint(
    request: WalCheckpointRequest,
    service: WalMaintenanceService = Depends(get_wal_maintenance_service),
) -> WalMaintenanceResponse:
    try:
        snapshot = service.request_checkpoint(mode=request.mode, reason=request.reason, force=request.force)
    except WalMaintenancePolicyError as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc
    except WalMaintenanceConflictError as exc:
        raise HTTPException(status_code=status.HTTP_429_TOO_MANY_REQUESTS, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(exc)) from exc

    return WalMaintenanceResponse.model_validate(wal_maintenance_snapshot_to_dict(snapshot))


@router.get("/wal/checkpoint/latest", response_model=WalMaintenanceResponse)
def get_latest_wal_checkpoint(
    service: WalMaintenanceService = Depends(get_wal_maintenance_service),
) -> WalMaintenanceResponse:
    try:
        snapshot = service.get_latest()
    except WalMaintenanceNotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    return WalMaintenanceResponse.model_validate(wal_maintenance_snapshot_to_dict(snapshot))


@router.get("/wal/metrics", response_model=WalMaintenanceMetricsResponse)
def get_wal_metrics(service: WalMaintenanceService = Depends(get_wal_maintenance_service)) -> WalMaintenanceMetricsResponse:
    metrics = service.get_metrics()
    return WalMaintenanceMetricsResponse.model_validate(wal_maintenance_metrics_to_dict(metrics))
