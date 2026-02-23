from __future__ import annotations

from pathlib import Path

from sqlalchemy import create_engine, text

from dedupfs.db.migrations import MIGRATIONS, apply_migrations


def _column_names(conn, table_name: str) -> set[str]:
    rows = conn.execute(text(f"PRAGMA table_info('{table_name}')")).mappings().all()
    return {str(row["name"]) for row in rows}


def _index_names(conn, table_name: str) -> set[str]:
    rows = conn.execute(text(f"PRAGMA index_list('{table_name}')")).mappings().all()
    return {str(row["name"]) for row in rows}


def test_apply_migrations_upgrades_legacy_schema(tmp_path: Path) -> None:
    db_path = tmp_path / "legacy.sqlite3"
    engine = create_engine(f"sqlite:///{db_path.as_posix()}", future=True)

    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE scan_sessions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    status VARCHAR(16) NOT NULL,
                    started_at DATETIME NOT NULL,
                    finished_at DATETIME,
                    error_message TEXT,
                    files_seen BIGINT NOT NULL DEFAULT 0,
                    directories_seen BIGINT NOT NULL DEFAULT 0,
                    bytes_seen BIGINT NOT NULL DEFAULT 0
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE library_files (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    library_id INTEGER NOT NULL,
                    relative_path VARCHAR(4096) NOT NULL,
                    size_bytes BIGINT NOT NULL,
                    mtime_ns BIGINT NOT NULL,
                    inode BIGINT,
                    device BIGINT,
                    is_missing BOOLEAN NOT NULL DEFAULT 0,
                    needs_hash BOOLEAN NOT NULL DEFAULT 1,
                    last_seen_scan_id INTEGER,
                    hash_algorithm VARCHAR(16),
                    content_hash BLOB,
                    hashed_size_bytes BIGINT,
                    hashed_mtime_ns BIGINT,
                    hashed_at DATETIME,
                    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
        )

    apply_migrations(engine)
    apply_migrations(engine)

    with engine.begin() as conn:
        scan_columns = _column_names(conn, "scan_sessions")
        file_columns = _column_names(conn, "library_files")
        file_indexes = _index_names(conn, "library_files")
        thumbnail_columns = _column_names(conn, "thumbnails")
        cleanup_columns = _column_names(conn, "thumbnail_cleanup_jobs")
        wal_columns = _column_names(conn, "wal_maintenance_jobs")
        wal_indexes = _index_names(conn, "wal_maintenance_jobs")
        io_rate_columns = _column_names(conn, "io_rate_limits")
        migration_versions = [
            int(row[0])
            for row in conn.execute(text("SELECT version FROM schema_migrations ORDER BY version ASC")).all()
        ]

    assert "error_count" in scan_columns
    assert {
        "hash_error_count",
        "hash_last_error",
        "hash_last_error_at",
        "hash_retry_after",
        "hash_claim_token",
        "hash_claimed_at",
    }.issubset(file_columns)
    assert {"thumb_key", "file_id", "status", "media_type", "output_relpath"}.issubset(thumbnail_columns)
    assert {"group_key", "status", "execute_after"}.issubset(cleanup_columns)
    assert {
        "requested_mode",
        "status",
        "execute_after",
        "retry_after",
        "worker_id",
        "worker_heartbeat_at",
        "lease_expires_at",
    }.issubset(wal_columns)
    assert {
        "ix_wal_jobs_status_execute",
        "ix_wal_jobs_retry_after",
        "ix_wal_jobs_running_lease",
        "ix_wal_jobs_created_at",
    }.issubset(wal_indexes)
    assert {"bucket_key", "next_available_at_ms", "updated_at"}.issubset(io_rate_columns)
    assert "ix_library_files_dedup_group" in file_indexes
    assert migration_versions == [step.version for step in MIGRATIONS]


def test_apply_migrations_removes_legacy_job_execution_backend_column(tmp_path: Path) -> None:
    db_path = tmp_path / "legacy_jobs.sqlite3"
    engine = create_engine(f"sqlite:///{db_path.as_posix()}", future=True)

    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE jobs (
                    id VARCHAR(36) PRIMARY KEY,
                    kind VARCHAR(16) NOT NULL,
                    status VARCHAR(16) NOT NULL,
                    dry_run BOOLEAN NOT NULL DEFAULT 1,
                    execution_backend VARCHAR(16) NOT NULL DEFAULT 'rust',
                    worker_id VARCHAR(128),
                    worker_heartbeat_at DATETIME,
                    progress FLOAT NOT NULL DEFAULT 0.0,
                    total_items INTEGER,
                    processed_items INTEGER NOT NULL DEFAULT 0,
                    payload JSON NOT NULL,
                    error_message TEXT,
                    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    started_at DATETIME,
                    finished_at DATETIME
                )
                """
            )
        )
        conn.execute(
            text(
                "CREATE INDEX ix_jobs_execution_kind_status ON jobs "
                "(execution_backend, kind, status)"
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE job_locks (
                    lock_key VARCHAR(100) PRIMARY KEY,
                    owner_job_id VARCHAR(36) NOT NULL,
                    acquired_at DATETIME NOT NULL,
                    heartbeat_at DATETIME NOT NULL,
                    expires_at DATETIME NOT NULL,
                    FOREIGN KEY(owner_job_id) REFERENCES jobs(id) ON DELETE CASCADE
                )
                """
            )
        )
        conn.execute(
            text(
                "INSERT INTO jobs(id, kind, status, dry_run, execution_backend, payload) "
                "VALUES ('job-1', 'scan', 'running', 1, 'rust', '{}')"
            )
        )
        conn.execute(
            text(
                "INSERT INTO job_locks(lock_key, owner_job_id, acquired_at, heartbeat_at, expires_at) "
                "VALUES ('scan_hash_mutex', 'job-1', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)"
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE schema_migrations (
                    version INTEGER PRIMARY KEY,
                    name VARCHAR(255) NOT NULL,
                    applied_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
        )
        conn.execute(
            text(
                "INSERT INTO schema_migrations(version, name) VALUES "
                "(1, 'baseline'), "
                "(2, 'scan_sessions_error_count'), "
                "(3, 'hash_retry_and_claim_columns'), "
                "(4, 'job_execution_backend')"
            )
        )

    apply_migrations(engine)

    with engine.begin() as conn:
        job_columns = _column_names(conn, "jobs")
        job_indexes = _index_names(conn, "jobs")
        migration_versions = [
            int(row[0])
            for row in conn.execute(text("SELECT version FROM schema_migrations ORDER BY version ASC")).all()
        ]
        job_count = int(conn.execute(text("SELECT COUNT(1) FROM jobs")).scalar_one())

    assert "execution_backend" not in job_columns
    assert "worker_id" in job_columns
    assert "worker_heartbeat_at" in job_columns
    assert "lease_expires_at" in job_columns
    assert "error_code" in job_columns
    assert "ix_jobs_execution_kind_status" not in job_indexes
    assert {
        "ix_jobs_kind_status",
        "ix_jobs_created_at",
        "ix_jobs_created_id",
        "ix_jobs_status_updated",
        "ix_jobs_running_lease",
        "ix_jobs_single_active_scan_hash",
    }.issubset(job_indexes)
    assert migration_versions == [step.version for step in MIGRATIONS]
    assert job_count == 1


def test_apply_migrations_normalizes_enum_storage_and_recovers_duplicate_running_jobs(tmp_path: Path) -> None:
    db_path = tmp_path / "legacy_enum_values.sqlite3"
    engine = create_engine(f"sqlite:///{db_path.as_posix()}", future=True)

    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE jobs (
                    id VARCHAR(36) PRIMARY KEY,
                    kind VARCHAR(16) NOT NULL,
                    status VARCHAR(16) NOT NULL,
                    dry_run BOOLEAN NOT NULL DEFAULT 1,
                    worker_id VARCHAR(128),
                    worker_heartbeat_at DATETIME,
                    lease_expires_at DATETIME,
                    progress FLOAT NOT NULL DEFAULT 0.0,
                    total_items INTEGER,
                    processed_items INTEGER NOT NULL DEFAULT 0,
                    payload JSON NOT NULL,
                    error_code VARCHAR(64),
                    error_message TEXT,
                    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    started_at DATETIME,
                    finished_at DATETIME
                )
                """
            )
        )
        conn.execute(
            text(
                "CREATE UNIQUE INDEX ix_jobs_single_active_scan_hash ON jobs(status) "
                "WHERE status = 'running' AND kind IN ('scan', 'hash')"
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE scan_sessions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    status VARCHAR(16) NOT NULL
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE library_files (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    hash_algorithm VARCHAR(16)
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE schema_migrations (
                    version INTEGER PRIMARY KEY,
                    name VARCHAR(255) NOT NULL,
                    applied_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
        )
        conn.execute(
            text(
                "INSERT INTO jobs(id, kind, status, payload, created_at, updated_at) VALUES "
                "('job-a', 'SCAN', 'RUNNING', '{}', '2026-01-01 00:00:00', '2026-01-01 00:00:00'), "
                "('job-b', 'HASH', 'RUNNING', '{}', '2026-01-02 00:00:00', '2026-01-02 00:00:00')"
            )
        )
        conn.execute(text("INSERT INTO scan_sessions(status) VALUES ('SUCCEEDED')"))
        conn.execute(text("INSERT INTO library_files(hash_algorithm) VALUES ('SHA256')"))
        conn.execute(
            text(
                "INSERT INTO schema_migrations(version, name) VALUES "
                "(1, 'baseline'), "
                "(2, 'scan_sessions_error_count'), "
                "(3, 'hash_retry_and_claim_columns'), "
                "(4, 'legacy_job_execution_backend_marker'), "
                "(5, 'remove_job_execution_backend'), "
                "(6, 'jobs_lease_protocol')"
            )
        )

    apply_migrations(engine)

    with engine.begin() as conn:
        jobs = conn.execute(
            text("SELECT id, kind, status, error_code FROM jobs ORDER BY created_at ASC, id ASC")
        ).all()
        scan_status = conn.execute(text("SELECT status FROM scan_sessions")).scalar_one()
        hash_algorithm = conn.execute(text("SELECT hash_algorithm FROM library_files")).scalar_one()
        file_columns = _column_names(conn, "library_files")
        file_indexes = _index_names(conn, "library_files")
        running_count = int(
            conn.execute(
                text(
                    "SELECT COUNT(1) FROM jobs "
                    "WHERE lower(status) = 'running' AND lower(kind) IN ('scan', 'hash')"
                )
            ).scalar_one()
        )
        migration_versions = [
            int(row[0])
            for row in conn.execute(text("SELECT version FROM schema_migrations ORDER BY version ASC")).all()
        ]

    assert jobs[0][1] == "scan"
    assert jobs[0][2] == "running"
    assert jobs[0][3] is None
    assert jobs[1][1] == "hash"
    assert jobs[1][2] == "retryable"
    assert jobs[1][3] == "MIGRATION_MUTEX_RECOVERY"
    assert scan_status == "succeeded"
    assert hash_algorithm == "sha256"
    assert {"is_missing", "needs_hash", "content_hash"}.issubset(file_columns)
    assert "ix_library_files_dedup_group" in file_indexes
    assert running_count == 1
    assert migration_versions == [step.version for step in MIGRATIONS]


def test_apply_migrations_resolves_duplicate_pending_scan_hash_jobs(tmp_path: Path) -> None:
    db_path = tmp_path / "legacy_pending_duplicates.sqlite3"
    engine = create_engine(f"sqlite:///{db_path.as_posix()}", future=True)

    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE jobs (
                    id VARCHAR(36) PRIMARY KEY,
                    kind VARCHAR(16) NOT NULL,
                    status VARCHAR(16) NOT NULL,
                    dry_run BOOLEAN NOT NULL DEFAULT 1,
                    worker_id VARCHAR(128),
                    worker_heartbeat_at DATETIME,
                    lease_expires_at DATETIME,
                    progress FLOAT NOT NULL DEFAULT 0.0,
                    total_items INTEGER,
                    processed_items INTEGER NOT NULL DEFAULT 0,
                    payload JSON NOT NULL,
                    error_code VARCHAR(64),
                    error_message TEXT,
                    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    started_at DATETIME,
                    finished_at DATETIME
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE schema_migrations (
                    version INTEGER PRIMARY KEY,
                    name VARCHAR(255) NOT NULL,
                    applied_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
        )
        conn.execute(
            text(
                "INSERT INTO jobs(id, kind, status, payload, created_at, updated_at) VALUES "
                "('job-a', 'scan', 'pending', '{}', '2026-01-01 00:00:00', '2026-01-01 00:00:00'), "
                "('job-b', 'hash', 'pending', '{}', '2026-01-02 00:00:00', '2026-01-02 00:00:00')"
            )
        )
        conn.execute(
            text(
                "INSERT INTO schema_migrations(version, name) VALUES "
                "(1, 'baseline'), "
                "(2, 'scan_sessions_error_count'), "
                "(3, 'hash_retry_and_claim_columns'), "
                "(4, 'legacy_job_execution_backend_marker'), "
                "(5, 'remove_job_execution_backend'), "
                "(6, 'jobs_lease_protocol'), "
                "(7, 'normalize_enum_storage_for_rust_protocol'), "
                "(8, 'thumbnail_protocol_tables')"
            )
        )

    apply_migrations(engine)

    with engine.begin() as conn:
        active_count = int(
            conn.execute(
                text(
                    "SELECT COUNT(1) FROM jobs "
                    "WHERE lower(kind) IN ('scan', 'hash') AND lower(status) IN ('pending', 'running')"
                )
            ).scalar_one()
        )
        recovered = conn.execute(
            text(
                "SELECT id, status, error_code FROM jobs "
                "WHERE id = 'job-b'"
            )
        ).one()
        migration_versions = [
            int(row[0])
            for row in conn.execute(text("SELECT version FROM schema_migrations ORDER BY version ASC")).all()
        ]

    assert active_count == 1
    assert recovered[1] == "retryable"
    assert recovered[2] == "MIGRATION_ACTIVE_RECOVERY"
    assert migration_versions == [step.version for step in MIGRATIONS]


def test_apply_migrations_normalizes_existing_wal_maintenance_rows(tmp_path: Path) -> None:
    db_path = tmp_path / "legacy_wal_maintenance.sqlite3"
    engine = create_engine(f"sqlite:///{db_path.as_posix()}", future=True)

    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE wal_maintenance_jobs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    requested_mode VARCHAR(16) NOT NULL DEFAULT 'PASSIVE',
                    status VARCHAR(16) NOT NULL DEFAULT 'PENDING',
                    execute_after DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE schema_migrations (
                    version INTEGER PRIMARY KEY,
                    name VARCHAR(255) NOT NULL,
                    applied_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
        )
        conn.execute(
            text(
                "INSERT INTO wal_maintenance_jobs(requested_mode, status, execute_after) "
                "VALUES ('RESTART', 'RETRYABLE', CURRENT_TIMESTAMP)"
            )
        )
        conn.execute(
            text(
                "INSERT INTO schema_migrations(version, name) VALUES "
                "(1, 'baseline'), "
                "(2, 'scan_sessions_error_count'), "
                "(3, 'hash_retry_and_claim_columns'), "
                "(4, 'legacy_job_execution_backend_marker'), "
                "(5, 'remove_job_execution_backend'), "
                "(6, 'jobs_lease_protocol'), "
                "(7, 'normalize_enum_storage_for_rust_protocol'), "
                "(8, 'thumbnail_protocol_tables'), "
                "(9, 'scan_hash_admission_mutex'), "
                "(10, 'global_io_rate_limit_table'), "
                "(11, 'duplicate_group_query_indexes')"
            )
        )

    apply_migrations(engine)

    with engine.begin() as conn:
        row = conn.execute(
            text("SELECT requested_mode, status, retry_after FROM wal_maintenance_jobs WHERE id = 1")
        ).one()
        indexes = _index_names(conn, "wal_maintenance_jobs")
        migration_versions = [
            int(item[0])
            for item in conn.execute(text("SELECT version FROM schema_migrations ORDER BY version ASC")).all()
        ]

    assert row[0] == "restart"
    assert row[1] == "retryable"
    assert row[2] is not None
    assert {
        "ix_wal_jobs_status_execute",
        "ix_wal_jobs_retry_after",
        "ix_wal_jobs_running_lease",
        "ix_wal_jobs_created_at",
    }.issubset(indexes)
    assert migration_versions == [step.version for step in MIGRATIONS]
