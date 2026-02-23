from __future__ import annotations

import os
from pathlib import Path

import dedupfs.db.session as db_session_module
from fastapi.testclient import TestClient

from dedupfs.api.app import create_app
from dedupfs.core.config import get_settings
from dedupfs.db.init_db import initialize_database


def _prepare_env(tmp_path: Path) -> None:
    state_root = tmp_path / "state"
    state_root.mkdir(parents=True, exist_ok=True)

    os.environ["DEDUPFS_LIBRARIES_ROOT"] = "/libraries"
    os.environ["DEDUPFS_STATE_ROOT"] = state_root.as_posix()
    os.environ["DEDUPFS_DRY_RUN"] = "true"
    os.environ["DEDUPFS_ALLOW_REAL_DELETE"] = "false"

    get_settings.cache_clear()
    db_session_module._engine = None
    db_session_module._session_factory = None
    initialize_database()


def test_ui_routes_and_assets_are_served(tmp_path: Path) -> None:
    _prepare_env(tmp_path)
    app = create_app()
    client = TestClient(app)

    root = client.get("/", follow_redirects=False)
    assert root.status_code == 307
    assert root.headers["location"] == "/ui"

    ui_page = client.get("/ui")
    assert ui_page.status_code == 200
    assert "Duplicate Groups" in ui_page.text

    script = client.get("/ui/static/app.js")
    assert script.status_code == 200
    assert "loadGroups" in script.text
