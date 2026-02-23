from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

from sqlalchemy import Connection, Engine, inspect, text


@dataclass(frozen=True)
class MigrationStep:
    version: int
    name: str
    apply: Callable[[Connection], None]


def _ensure_schema_migrations_table(conn: Connection) -> None:
    conn.execute(
        text(
            """
            CREATE TABLE IF NOT EXISTS schema_migrations (
                version INTEGER PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                applied_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
    )


def _column_exists(conn: Connection, table_name: str, column_name: str) -> bool:
    if not _table_exists(conn, table_name):
        return False

    dialect_name = conn.engine.dialect.name
    if dialect_name == "sqlite":
        rows = conn.execute(text(f"PRAGMA table_info('{table_name}')")).mappings().all()
        return any(str(row["name"]) == column_name for row in rows)

    inspector = inspect(conn)
    return any(col["name"] == column_name for col in inspector.get_columns(table_name))


def _index_exists(conn: Connection, table_name: str, index_name: str) -> bool:
    if not _table_exists(conn, table_name):
        return False

    if conn.engine.dialect.name == "sqlite":
        rows = conn.execute(text(f"PRAGMA index_list('{table_name}')")).mappings().all()
        return any(str(row["name"]) == index_name for row in rows)

    inspector = inspect(conn)
    indexes = inspector.get_indexes(table_name)
    return any(index.get("name") == index_name for index in indexes)


def _table_exists(conn: Connection, table_name: str) -> bool:
    inspector = inspect(conn)
    return inspector.has_table(table_name)


def _migration_0001_baseline(_conn: Connection) -> None:
    return


def _migration_0002_scan_session_error_count(conn: Connection) -> None:
    if not _column_exists(conn, "scan_sessions", "error_count"):
        conn.execute(text("ALTER TABLE scan_sessions ADD COLUMN error_count INTEGER NOT NULL DEFAULT 0"))


def _migration_0003_hash_retry_and_claim_columns(conn: Connection) -> None:
    if not _column_exists(conn, "library_files", "hash_error_count"):
        conn.execute(text("ALTER TABLE library_files ADD COLUMN hash_error_count INTEGER NOT NULL DEFAULT 0"))

    if not _column_exists(conn, "library_files", "hash_last_error"):
        conn.execute(text("ALTER TABLE library_files ADD COLUMN hash_last_error TEXT"))

    if not _column_exists(conn, "library_files", "hash_last_error_at"):
        conn.execute(text("ALTER TABLE library_files ADD COLUMN hash_last_error_at DATETIME"))

    if not _column_exists(conn, "library_files", "hash_retry_after"):
        conn.execute(text("ALTER TABLE library_files ADD COLUMN hash_retry_after DATETIME"))

    if not _column_exists(conn, "library_files", "hash_claim_token"):
        conn.execute(text("ALTER TABLE library_files ADD COLUMN hash_claim_token VARCHAR(64)"))

    if not _column_exists(conn, "library_files", "hash_claimed_at"):
        conn.execute(text("ALTER TABLE library_files ADD COLUMN hash_claimed_at DATETIME"))

    if not _index_exists(conn, "library_files", "ix_library_files_hash_retry"):
        conn.execute(
            text(
                "CREATE INDEX ix_library_files_hash_retry ON library_files "
                "(needs_hash, is_missing, hash_retry_after, id)"
            )
        )

    if not _index_exists(conn, "library_files", "ix_library_files_hash_claimed"):
        conn.execute(
            text("CREATE INDEX ix_library_files_hash_claimed ON library_files (hash_claim_token, hash_claimed_at)")
        )


def _ensure_jobs_indexes(conn: Connection) -> None:
    if not _table_exists(conn, "jobs"):
        return

    if not _index_exists(conn, "jobs", "ix_jobs_kind_status"):
        conn.execute(text("CREATE INDEX ix_jobs_kind_status ON jobs (kind, status)"))

    if not _index_exists(conn, "jobs", "ix_jobs_created_at"):
        conn.execute(text("CREATE INDEX ix_jobs_created_at ON jobs (created_at)"))

    if not _index_exists(conn, "jobs", "ix_jobs_created_id"):
        conn.execute(text("CREATE INDEX ix_jobs_created_id ON jobs (created_at, id)"))

    if not _index_exists(conn, "jobs", "ix_jobs_status_updated"):
        conn.execute(text("CREATE INDEX ix_jobs_status_updated ON jobs (status, updated_at)"))


def _migration_0004_legacy_job_execution_backend_marker(_conn: Connection) -> None:
    return


def _migration_0005_remove_job_execution_backend(conn: Connection) -> None:
    if not _table_exists(conn, "jobs"):
        return

    if not _column_exists(conn, "jobs", "worker_id"):
        conn.execute(text("ALTER TABLE jobs ADD COLUMN worker_id VARCHAR(128)"))

    if not _column_exists(conn, "jobs", "worker_heartbeat_at"):
        conn.execute(text("ALTER TABLE jobs ADD COLUMN worker_heartbeat_at DATETIME"))

    conn.execute(text("DROP INDEX IF EXISTS ix_jobs_execution_kind_status"))

    if not _column_exists(conn, "jobs", "execution_backend"):
        _ensure_jobs_indexes(conn)
        return

    if conn.engine.dialect.name == "sqlite":
        conn.execute(text("DROP TABLE IF EXISTS jobs__new"))
        conn.execute(
            text(
                """
                CREATE TABLE jobs__new (
                    id VARCHAR(36) PRIMARY KEY,
                    kind VARCHAR(16) NOT NULL,
                    status VARCHAR(16) NOT NULL,
                    dry_run BOOLEAN NOT NULL DEFAULT 1,
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
                """
                INSERT INTO jobs__new (
                    id,
                    kind,
                    status,
                    dry_run,
                    worker_id,
                    worker_heartbeat_at,
                    progress,
                    total_items,
                    processed_items,
                    payload,
                    error_message,
                    created_at,
                    updated_at,
                    started_at,
                    finished_at
                )
                SELECT
                    id,
                    kind,
                    status,
                    dry_run,
                    worker_id,
                    worker_heartbeat_at,
                    progress,
                    total_items,
                    processed_items,
                    payload,
                    error_message,
                    created_at,
                    updated_at,
                    started_at,
                    finished_at
                FROM jobs
                """
            )
        )

        conn.execute(text("DROP TABLE jobs"))
        conn.execute(text("ALTER TABLE jobs__new RENAME TO jobs"))
    else:
        conn.execute(text("ALTER TABLE jobs DROP COLUMN execution_backend"))

    _ensure_jobs_indexes(conn)


def _normalize_job_enum_values(conn: Connection) -> None:
    conn.execute(
        text(
            """
            UPDATE jobs
            SET kind = lower(kind),
                status = lower(status)
            WHERE kind != lower(kind) OR status != lower(status)
            """
        )
    )


def _normalize_scan_session_status(conn: Connection) -> None:
    if not _table_exists(conn, "scan_sessions"):
        return
    if not _column_exists(conn, "scan_sessions", "status"):
        return
    conn.execute(
        text(
            """
            UPDATE scan_sessions
            SET status = lower(status)
            WHERE status != lower(status)
            """
        )
    )


def _normalize_library_file_hash_algorithm(conn: Connection) -> None:
    if not _table_exists(conn, "library_files"):
        return
    if not _column_exists(conn, "library_files", "hash_algorithm"):
        return
    conn.execute(
        text(
            """
            UPDATE library_files
            SET hash_algorithm = lower(hash_algorithm)
            WHERE hash_algorithm IS NOT NULL AND hash_algorithm != lower(hash_algorithm)
            """
        )
    )


def _resolve_duplicate_running_scan_hash_jobs(conn: Connection) -> None:
    conn.execute(
        text(
            """
            WITH ranked AS (
                SELECT
                    id,
                    ROW_NUMBER() OVER (
                        ORDER BY created_at ASC, id ASC
                    ) AS row_num
                FROM jobs
                WHERE lower(status) = 'running'
                  AND lower(kind) IN ('scan', 'hash')
            )
            UPDATE jobs
            SET status = 'retryable',
                worker_id = NULL,
                worker_heartbeat_at = NULL,
                lease_expires_at = NULL,
                error_code = 'MIGRATION_MUTEX_RECOVERY',
                error_message = CASE
                    WHEN error_message IS NULL OR trim(error_message) = ''
                    THEN 'Reclassified during migration to satisfy single running scan/hash invariant'
                    ELSE error_message
                END,
                finished_at = COALESCE(finished_at, CURRENT_TIMESTAMP),
                updated_at = CURRENT_TIMESTAMP
            WHERE id IN (
                SELECT id
                FROM ranked
                WHERE row_num > 1
            )
            """
        )
    )


def _resolve_duplicate_pending_or_running_scan_hash_jobs(conn: Connection) -> None:
    conn.execute(
        text(
            """
            WITH ranked AS (
                SELECT
                    id,
                    ROW_NUMBER() OVER (
                        ORDER BY
                            CASE lower(status)
                                WHEN 'running' THEN 0
                                WHEN 'pending' THEN 1
                                ELSE 2
                            END,
                            created_at ASC,
                            id ASC
                    ) AS row_num
                FROM jobs
                WHERE lower(kind) IN ('scan', 'hash')
                  AND lower(status) IN ('pending', 'running')
            )
            UPDATE jobs
            SET status = 'retryable',
                worker_id = NULL,
                worker_heartbeat_at = NULL,
                lease_expires_at = NULL,
                error_code = CASE
                    WHEN error_code IS NULL OR trim(error_code) = ''
                    THEN 'MIGRATION_ACTIVE_RECOVERY'
                    ELSE error_code
                END,
                error_message = CASE
                    WHEN error_message IS NULL OR trim(error_message) = ''
                    THEN 'Reclassified during migration to satisfy single pending/running scan/hash invariant'
                    ELSE error_message
                END,
                finished_at = COALESCE(finished_at, CURRENT_TIMESTAMP),
                updated_at = CURRENT_TIMESTAMP
            WHERE id IN (
                SELECT id
                FROM ranked
                WHERE row_num > 1
            )
            """
        )
    )


def _drop_single_active_scan_hash_index(conn: Connection) -> None:
    conn.execute(text("DROP INDEX IF EXISTS ix_jobs_single_active_scan_hash"))


def _rebuild_single_active_scan_hash_index(conn: Connection) -> None:
    _drop_single_active_scan_hash_index(conn)
    conn.execute(
        text(
            "CREATE UNIQUE INDEX ix_jobs_single_active_scan_hash "
            "ON jobs((1)) WHERE lower(status) IN ('pending', 'running') AND lower(kind) IN ('scan', 'hash')"
        )
    )


def _migration_0006_jobs_lease_protocol(conn: Connection) -> None:
    if not _table_exists(conn, "jobs"):
        return

    if not _column_exists(conn, "jobs", "lease_expires_at"):
        conn.execute(text("ALTER TABLE jobs ADD COLUMN lease_expires_at DATETIME"))

    if not _column_exists(conn, "jobs", "error_code"):
        conn.execute(text("ALTER TABLE jobs ADD COLUMN error_code VARCHAR(64)"))

    if not _index_exists(conn, "jobs", "ix_jobs_running_lease"):
        conn.execute(text("CREATE INDEX ix_jobs_running_lease ON jobs (status, lease_expires_at)"))

    _drop_single_active_scan_hash_index(conn)
    _normalize_job_enum_values(conn)
    _resolve_duplicate_running_scan_hash_jobs(conn)
    _resolve_duplicate_pending_or_running_scan_hash_jobs(conn)
    _rebuild_single_active_scan_hash_index(conn)


def _migration_0007_normalize_enum_storage_for_rust_protocol(conn: Connection) -> None:
    if not _table_exists(conn, "jobs"):
        return

    _drop_single_active_scan_hash_index(conn)
    _normalize_job_enum_values(conn)
    _normalize_scan_session_status(conn)
    _normalize_library_file_hash_algorithm(conn)
    _resolve_duplicate_running_scan_hash_jobs(conn)
    _resolve_duplicate_pending_or_running_scan_hash_jobs(conn)
    _rebuild_single_active_scan_hash_index(conn)


def _migration_0009_scan_hash_admission_mutex(conn: Connection) -> None:
    if not _table_exists(conn, "jobs"):
        return

    _ensure_jobs_indexes(conn)
    _drop_single_active_scan_hash_index(conn)
    _normalize_job_enum_values(conn)
    _resolve_duplicate_running_scan_hash_jobs(conn)
    _resolve_duplicate_pending_or_running_scan_hash_jobs(conn)
    _rebuild_single_active_scan_hash_index(conn)


def _migration_0008_thumbnail_protocol_tables(conn: Connection) -> None:
    if _table_exists(conn, "library_files") and not _table_exists(conn, "thumbnails"):
        conn.execute(
            text(
                """
                CREATE TABLE thumbnails (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    thumb_key VARCHAR(128) NOT NULL UNIQUE,
                    file_id INTEGER NOT NULL REFERENCES library_files(id) ON DELETE CASCADE,
                    group_key VARCHAR(256),
                    status VARCHAR(16) NOT NULL DEFAULT 'pending',
                    media_type VARCHAR(16) NOT NULL,
                    format VARCHAR(16) NOT NULL DEFAULT 'jpeg',
                    max_dimension INTEGER NOT NULL DEFAULT 256,
                    version INTEGER NOT NULL DEFAULT 1,
                    source_size_bytes BIGINT NOT NULL,
                    source_mtime_ns BIGINT NOT NULL,
                    output_relpath VARCHAR(1024),
                    width INTEGER,
                    height INTEGER,
                    bytes_size BIGINT,
                    error_code VARCHAR(64),
                    error_message TEXT,
                    error_count INTEGER NOT NULL DEFAULT 0,
                    retry_after DATETIME,
                    worker_id VARCHAR(128),
                    worker_heartbeat_at DATETIME,
                    lease_expires_at DATETIME,
                    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    started_at DATETIME,
                    finished_at DATETIME
                )
                """
            )
        )

    if not _table_exists(conn, "thumbnail_cleanup_jobs"):
        conn.execute(
            text(
                """
                CREATE TABLE thumbnail_cleanup_jobs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    group_key VARCHAR(256) NOT NULL UNIQUE,
                    status VARCHAR(16) NOT NULL DEFAULT 'pending',
                    execute_after DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    worker_id VARCHAR(128),
                    worker_heartbeat_at DATETIME,
                    lease_expires_at DATETIME,
                    error_code VARCHAR(64),
                    error_message TEXT,
                    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    finished_at DATETIME
                )
                """
            )
        )

    if _table_exists(conn, "thumbnails"):
        conn.execute(text("CREATE INDEX IF NOT EXISTS ix_thumbnails_status_retry ON thumbnails (status, retry_after, id)"))
        conn.execute(
            text("CREATE INDEX IF NOT EXISTS ix_thumbnails_file_variant ON thumbnails (file_id, max_dimension, format)")
        )
        conn.execute(text("CREATE INDEX IF NOT EXISTS ix_thumbnails_group_status ON thumbnails (group_key, status)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS ix_thumbnails_running_lease ON thumbnails (status, lease_expires_at)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS ix_thumbnails_updated ON thumbnails (updated_at)"))

    if _table_exists(conn, "thumbnail_cleanup_jobs"):
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_thumbnail_cleanup_status_execute "
                "ON thumbnail_cleanup_jobs (status, execute_after)"
            )
        )
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_thumbnail_cleanup_running_lease "
                "ON thumbnail_cleanup_jobs (status, lease_expires_at)"
            )
        )
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_thumbnail_cleanup_updated "
                "ON thumbnail_cleanup_jobs (updated_at)"
            )
        )

    if _table_exists(conn, "thumbnails"):
        conn.execute(
            text(
                """
                UPDATE thumbnails
                SET status = lower(status),
                    media_type = lower(media_type),
                    format = lower(format)
                WHERE status != lower(status)
                   OR media_type != lower(media_type)
                   OR format != lower(format)
                """
            )
        )

    if _table_exists(conn, "thumbnail_cleanup_jobs"):
        conn.execute(
            text(
                """
                UPDATE thumbnail_cleanup_jobs
                SET status = lower(status)
                WHERE status != lower(status)
                """
            )
        )


def _migration_0010_global_io_rate_limit_table(conn: Connection) -> None:
    if _table_exists(conn, "io_rate_limits"):
        return
    conn.execute(
        text(
            """
            CREATE TABLE io_rate_limits (
                bucket_key VARCHAR(64) PRIMARY KEY,
                next_available_at_ms BIGINT NOT NULL DEFAULT 0,
                updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
    )


def _migration_0011_duplicate_group_query_indexes(conn: Connection) -> None:
    if not _table_exists(conn, "library_files"):
        return
    if not _column_exists(conn, "library_files", "id"):
        raise RuntimeError("library_files table missing required primary key column: id")

    if not _column_exists(conn, "library_files", "is_missing"):
        conn.execute(text("ALTER TABLE library_files ADD COLUMN is_missing BOOLEAN NOT NULL DEFAULT 0"))

    if not _column_exists(conn, "library_files", "needs_hash"):
        conn.execute(text("ALTER TABLE library_files ADD COLUMN needs_hash BOOLEAN NOT NULL DEFAULT 1"))

    if not _column_exists(conn, "library_files", "hash_algorithm"):
        conn.execute(text("ALTER TABLE library_files ADD COLUMN hash_algorithm VARCHAR(16)"))

    if not _column_exists(conn, "library_files", "content_hash"):
        conn.execute(text("ALTER TABLE library_files ADD COLUMN content_hash BLOB"))

    conn.execute(
        text(
            "CREATE INDEX IF NOT EXISTS ix_library_files_dedup_group "
            "ON library_files (is_missing, needs_hash, hash_algorithm, content_hash, id)"
        )
    )


def _migration_0012_wal_maintenance_jobs(conn: Connection) -> None:
    if not _table_exists(conn, "wal_maintenance_jobs"):
        conn.execute(
            text(
                """
                CREATE TABLE wal_maintenance_jobs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    requested_mode VARCHAR(16) NOT NULL DEFAULT 'passive',
                    status VARCHAR(16) NOT NULL DEFAULT 'pending',
                    requested_by VARCHAR(64),
                    reason TEXT,
                    execute_after DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    retry_count INTEGER NOT NULL DEFAULT 0,
                    retry_after DATETIME,
                    worker_id VARCHAR(128),
                    worker_heartbeat_at DATETIME,
                    lease_expires_at DATETIME,
                    checkpoint_busy INTEGER,
                    checkpoint_log_frames INTEGER,
                    checkpointed_frames INTEGER,
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
    else:
        if not _column_exists(conn, "wal_maintenance_jobs", "requested_mode"):
            conn.execute(
                text(
                    "ALTER TABLE wal_maintenance_jobs "
                    "ADD COLUMN requested_mode VARCHAR(16) NOT NULL DEFAULT 'passive'"
                )
            )
        if not _column_exists(conn, "wal_maintenance_jobs", "status"):
            conn.execute(
                text(
                    "ALTER TABLE wal_maintenance_jobs "
                    "ADD COLUMN status VARCHAR(16) NOT NULL DEFAULT 'pending'"
                )
            )
        if not _column_exists(conn, "wal_maintenance_jobs", "requested_by"):
            conn.execute(text("ALTER TABLE wal_maintenance_jobs ADD COLUMN requested_by VARCHAR(64)"))
        if not _column_exists(conn, "wal_maintenance_jobs", "reason"):
            conn.execute(text("ALTER TABLE wal_maintenance_jobs ADD COLUMN reason TEXT"))
        if not _column_exists(conn, "wal_maintenance_jobs", "execute_after"):
            conn.execute(
                text(
                    "ALTER TABLE wal_maintenance_jobs "
                    "ADD COLUMN execute_after DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP"
                )
            )
        if not _column_exists(conn, "wal_maintenance_jobs", "retry_count"):
            conn.execute(
                text("ALTER TABLE wal_maintenance_jobs ADD COLUMN retry_count INTEGER NOT NULL DEFAULT 0")
            )
        if not _column_exists(conn, "wal_maintenance_jobs", "retry_after"):
            conn.execute(text("ALTER TABLE wal_maintenance_jobs ADD COLUMN retry_after DATETIME"))
        if not _column_exists(conn, "wal_maintenance_jobs", "worker_id"):
            conn.execute(text("ALTER TABLE wal_maintenance_jobs ADD COLUMN worker_id VARCHAR(128)"))
        if not _column_exists(conn, "wal_maintenance_jobs", "worker_heartbeat_at"):
            conn.execute(text("ALTER TABLE wal_maintenance_jobs ADD COLUMN worker_heartbeat_at DATETIME"))
        if not _column_exists(conn, "wal_maintenance_jobs", "lease_expires_at"):
            conn.execute(text("ALTER TABLE wal_maintenance_jobs ADD COLUMN lease_expires_at DATETIME"))
        if not _column_exists(conn, "wal_maintenance_jobs", "checkpoint_busy"):
            conn.execute(text("ALTER TABLE wal_maintenance_jobs ADD COLUMN checkpoint_busy INTEGER"))
        if not _column_exists(conn, "wal_maintenance_jobs", "checkpoint_log_frames"):
            conn.execute(text("ALTER TABLE wal_maintenance_jobs ADD COLUMN checkpoint_log_frames INTEGER"))
        if not _column_exists(conn, "wal_maintenance_jobs", "checkpointed_frames"):
            conn.execute(text("ALTER TABLE wal_maintenance_jobs ADD COLUMN checkpointed_frames INTEGER"))
        if not _column_exists(conn, "wal_maintenance_jobs", "error_code"):
            conn.execute(text("ALTER TABLE wal_maintenance_jobs ADD COLUMN error_code VARCHAR(64)"))
        if not _column_exists(conn, "wal_maintenance_jobs", "error_message"):
            conn.execute(text("ALTER TABLE wal_maintenance_jobs ADD COLUMN error_message TEXT"))
        if not _column_exists(conn, "wal_maintenance_jobs", "created_at"):
            conn.execute(text("ALTER TABLE wal_maintenance_jobs ADD COLUMN created_at DATETIME"))
        if not _column_exists(conn, "wal_maintenance_jobs", "updated_at"):
            conn.execute(text("ALTER TABLE wal_maintenance_jobs ADD COLUMN updated_at DATETIME"))
        if not _column_exists(conn, "wal_maintenance_jobs", "started_at"):
            conn.execute(text("ALTER TABLE wal_maintenance_jobs ADD COLUMN started_at DATETIME"))
        if not _column_exists(conn, "wal_maintenance_jobs", "finished_at"):
            conn.execute(text("ALTER TABLE wal_maintenance_jobs ADD COLUMN finished_at DATETIME"))

    if _table_exists(conn, "wal_maintenance_jobs"):
        conn.execute(
            text(
                """
                UPDATE wal_maintenance_jobs
                SET requested_mode = lower(requested_mode),
                    status = lower(status)
                WHERE requested_mode != lower(requested_mode)
                   OR status != lower(status)
                """
            )
        )
        conn.execute(
            text(
                """
                UPDATE wal_maintenance_jobs
                SET retry_after = execute_after
                WHERE status = 'retryable'
                  AND retry_after IS NULL
                """
            )
        )
        conn.execute(
            text(
                """
                UPDATE wal_maintenance_jobs
                SET created_at = COALESCE(created_at, CURRENT_TIMESTAMP),
                    updated_at = COALESCE(updated_at, created_at, CURRENT_TIMESTAMP)
                WHERE created_at IS NULL OR updated_at IS NULL
                """
            )
        )
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_wal_jobs_status_execute "
                "ON wal_maintenance_jobs (status, execute_after, id)"
            )
        )
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_wal_jobs_retry_after "
                "ON wal_maintenance_jobs (status, retry_after, id)"
            )
        )
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_wal_jobs_running_lease "
                "ON wal_maintenance_jobs (status, lease_expires_at)"
            )
        )
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_wal_jobs_created_at "
                "ON wal_maintenance_jobs (created_at)"
            )
        )


MIGRATIONS: tuple[MigrationStep, ...] = (
    MigrationStep(version=1, name="baseline", apply=_migration_0001_baseline),
    MigrationStep(version=2, name="scan_sessions_error_count", apply=_migration_0002_scan_session_error_count),
    MigrationStep(version=3, name="hash_retry_and_claim_columns", apply=_migration_0003_hash_retry_and_claim_columns),
    MigrationStep(
        version=4,
        name="legacy_job_execution_backend_marker",
        apply=_migration_0004_legacy_job_execution_backend_marker,
    ),
    MigrationStep(version=5, name="remove_job_execution_backend", apply=_migration_0005_remove_job_execution_backend),
    MigrationStep(version=6, name="jobs_lease_protocol", apply=_migration_0006_jobs_lease_protocol),
    MigrationStep(
        version=7,
        name="normalize_enum_storage_for_rust_protocol",
        apply=_migration_0007_normalize_enum_storage_for_rust_protocol,
    ),
    MigrationStep(
        version=8,
        name="thumbnail_protocol_tables",
        apply=_migration_0008_thumbnail_protocol_tables,
    ),
    MigrationStep(
        version=9,
        name="scan_hash_admission_mutex",
        apply=_migration_0009_scan_hash_admission_mutex,
    ),
    MigrationStep(
        version=10,
        name="global_io_rate_limit_table",
        apply=_migration_0010_global_io_rate_limit_table,
    ),
    MigrationStep(
        version=11,
        name="duplicate_group_query_indexes",
        apply=_migration_0011_duplicate_group_query_indexes,
    ),
    MigrationStep(
        version=12,
        name="wal_maintenance_jobs",
        apply=_migration_0012_wal_maintenance_jobs,
    ),
)


def apply_migrations(engine: Engine) -> None:
    with engine.begin() as conn:
        _ensure_schema_migrations_table(conn)

        existing_versions = {
            int(row[0])
            for row in conn.execute(text("SELECT version FROM schema_migrations ORDER BY version ASC")).all()
        }

        for step in MIGRATIONS:
            if step.version in existing_versions:
                continue

            step.apply(conn)
            conn.execute(
                text("INSERT INTO schema_migrations(version, name) VALUES (:version, :name)"),
                {"version": step.version, "name": step.name},
            )
