from __future__ import annotations

from sqlalchemy import text

from dedupfs.db.migrations import apply_migrations
from dedupfs.db.models import Base
from dedupfs.db.session import get_engine


def initialize_database() -> None:
    engine = get_engine()
    Base.metadata.create_all(bind=engine)
    apply_migrations(engine)

    if engine.url.drivername.startswith("sqlite"):
        with engine.connect() as conn:
            conn.execute(text("PRAGMA optimize;"))
            conn.commit()
