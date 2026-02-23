from __future__ import annotations

from pathlib import Path

from fastapi import APIRouter, HTTPException, status
from fastapi.responses import FileResponse, RedirectResponse

router = APIRouter(tags=["ui"])

_UI_ROOT = Path(__file__).resolve().parents[2] / "ui"
_INDEX_FILE = _UI_ROOT / "index.html"


@router.get("/", include_in_schema=False)
def redirect_root_to_ui() -> RedirectResponse:
    return RedirectResponse(url="/ui", status_code=status.HTTP_307_TEMPORARY_REDIRECT)


@router.get("/ui", include_in_schema=False)
def serve_dashboard() -> FileResponse:
    if not _INDEX_FILE.is_file():
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="UI bundle not found")
    return FileResponse(path=_INDEX_FILE, media_type="text/html")
