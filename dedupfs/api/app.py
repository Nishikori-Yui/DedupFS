from __future__ import annotations

from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from dedupfs.api.routes.duplicates import router as duplicates_router
from dedupfs.api.routes.health import router as health_router
from dedupfs.api.routes.jobs import router as jobs_router
from dedupfs.api.routes.maintenance import router as maintenance_router
from dedupfs.api.routes.thumbs import router as thumbs_router
from dedupfs.api.routes.ui import router as ui_router
from dedupfs.core.config import get_settings
from dedupfs.core.logging import configure_logging
from dedupfs.db.init_db import initialize_database


@asynccontextmanager
async def lifespan(_app: FastAPI):
    settings = get_settings()
    configure_logging(settings.log_level)
    initialize_database()
    yield


def create_app() -> FastAPI:
    settings = get_settings()
    app = FastAPI(title=settings.app_name, lifespan=lifespan)
    app.include_router(ui_router)
    app.include_router(health_router, prefix="/api/v1")
    app.include_router(jobs_router, prefix="/api/v1")
    app.include_router(duplicates_router, prefix="/api/v1")
    app.include_router(maintenance_router, prefix="/api/v1")
    app.include_router(thumbs_router, prefix="/api/v1")

    ui_static_root = Path(__file__).resolve().parents[1] / "ui"
    app.mount("/ui/static", StaticFiles(directory=ui_static_root), name="ui-static")
    return app
