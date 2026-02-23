use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{anyhow, bail, Context, Result};
use rusqlite::{params, Connection};
use serde_json::Value;

use crate::config::WorkerConfig;
use crate::db::{refresh_job_lease, JobRecord};
use crate::path_safety::{
    normalize_library_name, resolve_root_under_libraries, to_posix_relative_path,
};

#[derive(Debug, Clone)]
struct LibraryTarget {
    id: i64,
    root_path_real: PathBuf,
}

#[derive(Debug, Default)]
struct ScanCounters {
    files_seen: i64,
    directories_seen: i64,
    bytes_seen: i64,
    batch_writes: i64,
    missing_marked: i64,
    error_count: i64,
    error_samples: Vec<String>,
}

pub fn run_scan_job(conn: &mut Connection, config: &WorkerConfig, job: &JobRecord) -> Result<()> {
    let batch_size = extract_optional_u64(&job.payload, "batch_size")
        .map(|v| v.max(1) as usize)
        .unwrap_or(config.scan_write_batch_size);
    let library_names = extract_library_names(&job.payload)?;

    let targets = prepare_targets(conn, config, library_names.as_deref())?;
    let scan_session_id = create_scan_session(conn)?;

    let mut counters = ScanCounters::default();
    for target in &targets {
        let local = scan_single_library(conn, config, job, target, scan_session_id, batch_size)?;
        counters.files_seen += local.files_seen;
        counters.directories_seen += local.directories_seen;
        counters.bytes_seen += local.bytes_seen;
        counters.batch_writes += local.batch_writes;
        counters.error_count += local.error_count;

        for sample in local.error_samples {
            if counters.error_samples.len() < 20 {
                counters.error_samples.push(sample);
            }
        }
    }

    if counters.error_count == 0 {
        for target in &targets {
            counters.missing_marked += mark_missing_files(conn, target.id, scan_session_id)?;
            conn.execute(
                "UPDATE library_roots SET last_scanned_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP WHERE id = ?1",
                params![target.id],
            )?;
        }

        conn.execute(
            "
            UPDATE scan_sessions
            SET status = 'succeeded',
                finished_at = CURRENT_TIMESTAMP,
                files_seen = ?1,
                directories_seen = ?2,
                bytes_seen = ?3,
                error_count = 0,
                error_message = NULL
            WHERE id = ?4
            ",
            params![
                counters.files_seen,
                counters.directories_seen,
                counters.bytes_seen,
                scan_session_id
            ],
        )?;
    } else {
        let error_message = format_error_message(counters.error_count, &counters.error_samples);
        conn.execute(
            "
            UPDATE scan_sessions
            SET status = 'failed',
                finished_at = CURRENT_TIMESTAMP,
                files_seen = ?1,
                directories_seen = ?2,
                bytes_seen = ?3,
                error_count = ?4,
                error_message = ?5
            WHERE id = ?6
            ",
            params![
                counters.files_seen,
                counters.directories_seen,
                counters.bytes_seen,
                counters.error_count,
                error_message,
                scan_session_id
            ],
        )?;

        refresh_job_lease(conn, config, &job.id, counters.files_seen, 1.0)?;
        bail!(format_error_message(
            counters.error_count,
            &counters.error_samples
        ));
    }

    refresh_job_lease(conn, config, &job.id, counters.files_seen, 1.0)?;
    Ok(())
}

fn create_scan_session(conn: &Connection) -> Result<i64> {
    conn.execute(
        "
        INSERT INTO scan_sessions (status, files_seen, directories_seen, bytes_seen, error_count)
        VALUES ('running', 0, 0, 0, 0)
        ",
        [],
    )?;
    Ok(conn.last_insert_rowid())
}

fn prepare_targets(
    conn: &Connection,
    config: &WorkerConfig,
    library_names: Option<&[String]>,
) -> Result<Vec<LibraryTarget>> {
    let names = if let Some(names) = library_names {
        names.to_vec()
    } else {
        discover_library_names(config)?
    };

    let mut dedup = Vec::new();
    let mut seen = HashSet::new();
    for raw_name in names {
        let name = normalize_library_name(&raw_name)?;
        if seen.insert(name.clone()) {
            dedup.push(name);
        }
    }

    dedup.sort();

    let mut targets = Vec::with_capacity(dedup.len());
    for name in dedup {
        let root = config.libraries_root.join(&name);
        let root_real = resolve_root_under_libraries(&config.libraries_root_real, &root)?;
        if !root_real.is_dir() {
            bail!("library root is not a directory: {}", root_real.display());
        }

        conn.execute(
            "
            INSERT INTO library_roots (name, root_path)
            VALUES (?1, ?2)
            ON CONFLICT(name) DO UPDATE SET
                root_path = excluded.root_path,
                updated_at = CURRENT_TIMESTAMP
            ",
            params![name, root_real.to_string_lossy().to_string()],
        )?;

        let id = conn.query_row(
            "SELECT id FROM library_roots WHERE name = ?1",
            params![name],
            |row| row.get::<_, i64>(0),
        )?;

        targets.push(LibraryTarget {
            id,
            root_path_real: root_real,
        });
    }

    Ok(targets)
}

fn discover_library_names(config: &WorkerConfig) -> Result<Vec<String>> {
    let mut names = Vec::new();
    for entry in fs::read_dir(&config.libraries_root_real).with_context(|| {
        format!(
            "failed to read libraries root: {}",
            config.libraries_root_real.display()
        )
    })? {
        let entry = match entry {
            Ok(entry) => entry,
            Err(_) => continue,
        };
        let file_type = match entry.file_type() {
            Ok(value) => value,
            Err(_) => continue,
        };
        if !file_type.is_dir() || file_type.is_symlink() {
            continue;
        }
        names.push(entry.file_name().to_string_lossy().to_string());
    }
    Ok(names)
}

fn scan_single_library(
    conn: &mut Connection,
    config: &WorkerConfig,
    job: &JobRecord,
    target: &LibraryTarget,
    scan_session_id: i64,
    batch_size: usize,
) -> Result<ScanCounters> {
    let mut counters = ScanCounters::default();
    let mut stack = vec![target.root_path_real.clone()];
    let mut batch: Vec<(i64, String, i64, i64, Option<i64>, Option<i64>, i64)> =
        Vec::with_capacity(batch_size);

    while let Some(current) = stack.pop() {
        counters.directories_seen += 1;

        let entries = match fs::read_dir(&current) {
            Ok(entries) => entries,
            Err(error) => {
                counters.error_count += 1;
                push_error_sample(&mut counters.error_samples, &current, &error.to_string());
                continue;
            }
        };

        for entry in entries {
            let entry = match entry {
                Ok(entry) => entry,
                Err(error) => {
                    counters.error_count += 1;
                    push_error_sample(&mut counters.error_samples, &current, &error.to_string());
                    continue;
                }
            };

            let entry_path = entry.path();
            let metadata = match fs::symlink_metadata(&entry_path) {
                Ok(metadata) => metadata,
                Err(error) => {
                    counters.error_count += 1;
                    push_error_sample(&mut counters.error_samples, &entry_path, &error.to_string());
                    continue;
                }
            };

            if metadata.file_type().is_symlink() {
                continue;
            }

            let resolved = match entry_path.canonicalize() {
                Ok(path) => path,
                Err(error) => {
                    counters.error_count += 1;
                    push_error_sample(&mut counters.error_samples, &entry_path, &error.to_string());
                    continue;
                }
            };

            if !resolved.starts_with(&target.root_path_real) {
                continue;
            }

            if metadata.is_dir() {
                stack.push(resolved);
                continue;
            }

            if !metadata.is_file() {
                continue;
            }

            let relative = resolved
                .strip_prefix(&target.root_path_real)
                .with_context(|| {
                    format!("failed to compute relative path for {}", resolved.display())
                })?;
            let relative_path = to_posix_relative_path(relative)?;

            let (size_bytes, mtime_ns, inode, device) = metadata_to_row(&metadata)?;
            batch.push((
                target.id,
                relative_path,
                size_bytes,
                mtime_ns,
                inode,
                device,
                scan_session_id,
            ));

            counters.files_seen += 1;
            counters.bytes_seen = counters.bytes_seen.saturating_add(size_bytes);

            if counters.files_seen % 256 == 0 {
                refresh_job_lease(conn, config, &job.id, counters.files_seen, 0.0)?;
            }

            if batch.len() >= batch_size {
                upsert_file_batch(conn, &batch)?;
                batch.clear();
                counters.batch_writes += 1;
            }
        }
    }

    if !batch.is_empty() {
        upsert_file_batch(conn, &batch)?;
        counters.batch_writes += 1;
    }

    Ok(counters)
}

fn upsert_file_batch(
    conn: &mut Connection,
    rows: &[(i64, String, i64, i64, Option<i64>, Option<i64>, i64)],
) -> Result<()> {
    if rows.is_empty() {
        return Ok(());
    }

    let tx = conn.transaction()?;
    let mut stmt = tx.prepare_cached(
        "
        INSERT INTO library_files (
            library_id,
            relative_path,
            size_bytes,
            mtime_ns,
            inode,
            device,
            is_missing,
            needs_hash,
            last_seen_scan_id
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, 0, 1, ?7)
        ON CONFLICT(library_id, relative_path) DO UPDATE SET
            size_bytes = excluded.size_bytes,
            mtime_ns = excluded.mtime_ns,
            inode = excluded.inode,
            device = excluded.device,
            is_missing = 0,
            last_seen_scan_id = excluded.last_seen_scan_id,
            needs_hash = CASE
                WHEN library_files.size_bytes != excluded.size_bytes
                  OR library_files.mtime_ns != excluded.mtime_ns
                  OR IFNULL(library_files.inode, -1) != IFNULL(excluded.inode, -1)
                  OR IFNULL(library_files.device, -1) != IFNULL(excluded.device, -1)
                  OR library_files.is_missing = 1
                THEN 1 ELSE library_files.needs_hash
            END,
            hash_algorithm = CASE
                WHEN library_files.size_bytes != excluded.size_bytes
                  OR library_files.mtime_ns != excluded.mtime_ns
                  OR IFNULL(library_files.inode, -1) != IFNULL(excluded.inode, -1)
                  OR IFNULL(library_files.device, -1) != IFNULL(excluded.device, -1)
                  OR library_files.is_missing = 1
                THEN NULL ELSE library_files.hash_algorithm
            END,
            content_hash = CASE
                WHEN library_files.size_bytes != excluded.size_bytes
                  OR library_files.mtime_ns != excluded.mtime_ns
                  OR IFNULL(library_files.inode, -1) != IFNULL(excluded.inode, -1)
                  OR IFNULL(library_files.device, -1) != IFNULL(excluded.device, -1)
                  OR library_files.is_missing = 1
                THEN NULL ELSE library_files.content_hash
            END,
            hashed_size_bytes = CASE
                WHEN library_files.size_bytes != excluded.size_bytes
                  OR library_files.mtime_ns != excluded.mtime_ns
                  OR IFNULL(library_files.inode, -1) != IFNULL(excluded.inode, -1)
                  OR IFNULL(library_files.device, -1) != IFNULL(excluded.device, -1)
                  OR library_files.is_missing = 1
                THEN NULL ELSE library_files.hashed_size_bytes
            END,
            hashed_mtime_ns = CASE
                WHEN library_files.size_bytes != excluded.size_bytes
                  OR library_files.mtime_ns != excluded.mtime_ns
                  OR IFNULL(library_files.inode, -1) != IFNULL(excluded.inode, -1)
                  OR IFNULL(library_files.device, -1) != IFNULL(excluded.device, -1)
                  OR library_files.is_missing = 1
                THEN NULL ELSE library_files.hashed_mtime_ns
            END,
            hashed_at = CASE
                WHEN library_files.size_bytes != excluded.size_bytes
                  OR library_files.mtime_ns != excluded.mtime_ns
                  OR IFNULL(library_files.inode, -1) != IFNULL(excluded.inode, -1)
                  OR IFNULL(library_files.device, -1) != IFNULL(excluded.device, -1)
                  OR library_files.is_missing = 1
                THEN NULL ELSE library_files.hashed_at
            END,
            hash_error_count = CASE
                WHEN library_files.size_bytes != excluded.size_bytes
                  OR library_files.mtime_ns != excluded.mtime_ns
                  OR IFNULL(library_files.inode, -1) != IFNULL(excluded.inode, -1)
                  OR IFNULL(library_files.device, -1) != IFNULL(excluded.device, -1)
                  OR library_files.is_missing = 1
                THEN 0 ELSE library_files.hash_error_count
            END,
            hash_last_error = CASE
                WHEN library_files.size_bytes != excluded.size_bytes
                  OR library_files.mtime_ns != excluded.mtime_ns
                  OR IFNULL(library_files.inode, -1) != IFNULL(excluded.inode, -1)
                  OR IFNULL(library_files.device, -1) != IFNULL(excluded.device, -1)
                  OR library_files.is_missing = 1
                THEN NULL ELSE library_files.hash_last_error
            END,
            hash_last_error_at = CASE
                WHEN library_files.size_bytes != excluded.size_bytes
                  OR library_files.mtime_ns != excluded.mtime_ns
                  OR IFNULL(library_files.inode, -1) != IFNULL(excluded.inode, -1)
                  OR IFNULL(library_files.device, -1) != IFNULL(excluded.device, -1)
                  OR library_files.is_missing = 1
                THEN NULL ELSE library_files.hash_last_error_at
            END,
            hash_retry_after = CASE
                WHEN library_files.size_bytes != excluded.size_bytes
                  OR library_files.mtime_ns != excluded.mtime_ns
                  OR IFNULL(library_files.inode, -1) != IFNULL(excluded.inode, -1)
                  OR IFNULL(library_files.device, -1) != IFNULL(excluded.device, -1)
                  OR library_files.is_missing = 1
                THEN NULL ELSE library_files.hash_retry_after
            END,
            hash_claim_token = CASE
                WHEN library_files.size_bytes != excluded.size_bytes
                  OR library_files.mtime_ns != excluded.mtime_ns
                  OR IFNULL(library_files.inode, -1) != IFNULL(excluded.inode, -1)
                  OR IFNULL(library_files.device, -1) != IFNULL(excluded.device, -1)
                  OR library_files.is_missing = 1
                THEN NULL ELSE library_files.hash_claim_token
            END,
            hash_claimed_at = CASE
                WHEN library_files.size_bytes != excluded.size_bytes
                  OR library_files.mtime_ns != excluded.mtime_ns
                  OR IFNULL(library_files.inode, -1) != IFNULL(excluded.inode, -1)
                  OR IFNULL(library_files.device, -1) != IFNULL(excluded.device, -1)
                  OR library_files.is_missing = 1
                THEN NULL ELSE library_files.hash_claimed_at
            END,
            updated_at = CURRENT_TIMESTAMP
        ",
    )?;

    for (library_id, relative_path, size_bytes, mtime_ns, inode, device, scan_id) in rows {
        stmt.execute(params![
            library_id,
            relative_path,
            size_bytes,
            mtime_ns,
            inode,
            device,
            scan_id
        ])?;
    }

    drop(stmt);
    tx.commit()?;
    Ok(())
}

fn mark_missing_files(conn: &Connection, library_id: i64, scan_session_id: i64) -> Result<i64> {
    let affected = conn.execute(
        "
        UPDATE library_files
        SET is_missing = 1,
            needs_hash = 0,
            hash_claim_token = NULL,
            hash_claimed_at = NULL,
            hash_retry_after = NULL,
            updated_at = CURRENT_TIMESTAMP
        WHERE library_id = ?1
          AND (last_seen_scan_id IS NULL OR last_seen_scan_id != ?2)
          AND is_missing = 0
        ",
        params![library_id, scan_session_id],
    )?;
    Ok(affected as i64)
}

fn push_error_sample(samples: &mut Vec<String>, path: &Path, message: &str) {
    if samples.len() >= 20 {
        return;
    }
    samples.push(format!("{}: {}", path.display(), message));
}

fn format_error_message(error_count: i64, samples: &[String]) -> String {
    if samples.is_empty() {
        return format!("Scan encountered {error_count} filesystem access errors");
    }
    format!(
        "Scan encountered {error_count} filesystem access errors: {}",
        samples.join(" | ")
    )
}

fn extract_optional_u64(payload: &Value, key: &str) -> Option<u64> {
    payload.get(key).and_then(|value| value.as_u64())
}

fn extract_library_names(payload: &Value) -> Result<Option<Vec<String>>> {
    let Some(value) = payload.get("library_names") else {
        return Ok(None);
    };
    if value.is_null() {
        return Ok(None);
    }

    let array = value
        .as_array()
        .ok_or_else(|| anyhow!("payload.library_names must be an array"))?;

    let mut names = Vec::new();
    for item in array {
        names.push(
            item.as_str()
                .ok_or_else(|| anyhow!("payload.library_names must contain strings"))?
                .to_string(),
        );
    }

    Ok(Some(names))
}

#[cfg(unix)]
fn metadata_to_row(metadata: &fs::Metadata) -> Result<(i64, i64, Option<i64>, Option<i64>)> {
    use std::os::unix::fs::MetadataExt;

    let size_bytes = i64::try_from(metadata.size()).context("file size over i64 range")?;
    let mtime_ns = metadata
        .mtime()
        .saturating_mul(1_000_000_000)
        .saturating_add(i64::from(metadata.mtime_nsec()));
    let inode = Some(i64::try_from(metadata.ino()).context("inode over i64 range")?);
    let device = Some(i64::try_from(metadata.dev()).context("device over i64 range")?);
    Ok((size_bytes, mtime_ns, inode, device))
}

#[cfg(not(unix))]
fn metadata_to_row(metadata: &fs::Metadata) -> Result<(i64, i64, Option<i64>, Option<i64>)> {
    let size_bytes = i64::try_from(metadata.len()).context("file size over i64 range")?;
    let modified = metadata
        .modified()
        .context("failed to read metadata modified timestamp")?;
    let duration = modified
        .duration_since(std::time::UNIX_EPOCH)
        .context("modified timestamp before UNIX_EPOCH")?;
    let mtime_ns = i64::try_from(duration.as_nanos()).context("mtime_ns over i64 range")?;
    Ok((size_bytes, mtime_ns, None, None))
}
