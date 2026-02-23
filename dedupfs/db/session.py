from __future__ import annotations

from typing import Iterator

from sqlalchemy import Engine, create_engine, event
from sqlalchemy.orm import Session, sessionmaker

from dedupfs.core.config import get_settings

_engine: Engine | None = None
_session_factory: sessionmaker[Session] | None = None


def _configure_sqlite_pragma(engine: Engine) -> None:
    if not engine.url.drivername.startswith("sqlite"):
        return

    @event.listens_for(engine, "connect")
    def _set_sqlite_pragma(dbapi_connection, _connection_record) -> None:  # type: ignore[no-untyped-def]
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA journal_mode=WAL;")
        cursor.execute("PRAGMA synchronous=NORMAL;")
        cursor.execute("PRAGMA temp_store=MEMORY;")
        cursor.execute("PRAGMA foreign_keys=ON;")
        cursor.close()


def get_engine() -> Engine:
    global _engine
    if _engine is not None:
        return _engine

    settings = get_settings()
    connect_args: dict[str, object] = {}
    if settings.effective_database_url.startswith("sqlite"):
        connect_args["check_same_thread"] = False

    _engine = create_engine(
        settings.effective_database_url,
        pool_pre_ping=True,
        future=True,
        connect_args=connect_args,
    )
    _configure_sqlite_pragma(_engine)
    return _engine


def get_session_factory() -> sessionmaker[Session]:
    global _session_factory
    if _session_factory is not None:
        return _session_factory

    _session_factory = sessionmaker(
        bind=get_engine(),
        autocommit=False,
        autoflush=False,
        expire_on_commit=False,
        class_=Session,
    )
    return _session_factory


def get_db_session() -> Iterator[Session]:
    session = get_session_factory()()
    try:
        yield session
    finally:
        session.close()
