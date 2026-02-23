from __future__ import annotations

from datetime import datetime, timezone

from fastapi import APIRouter

from dedupfs.core.config import get_settings

router = APIRouter(tags=["health"])


@router.get("/health")
def get_health() -> dict[str, object]:
    settings = get_settings()
    return {
        "status": "ok",
        "service": settings.app_name,
        "environment": settings.environment,
        "dry_run": settings.dry_run,
        "timestamp": datetime.now(tz=timezone.utc),
    }
