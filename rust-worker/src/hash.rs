use std::fs;
use std::io::Read;
use std::path::PathBuf;
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use blake3::Hasher as Blake3Hasher;
use rand::distributions::{Alphanumeric, DistString};
use rusqlite::{params, Connection};
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::config::{HashAlgorithm, WorkerConfig};
use crate::db::{refresh_job_lease, JobRecord};
use crate::path_safety::{resolve_root_under_libraries, validate_relative_path};

#[derive(Debug)]
struct HashCandidate {
    id: i64,
    relative_path: String,
    expected_size: i64,
    expected_mtime_ns: i64,
    hash_error_count: i64,
    root_path: String,
}

#[derive(Debug, Default)]
struct HashCounters {
    processed_files: i64,
    hashed_files: i64,
    requeued_files: i64,
    missing_files: i64,
    failed_files: i64,
    bytes_hashed: i64,
}

pub fn run_hash_job(conn: &mut Connection, config: &WorkerConfig, job: &JobRecord) -> Result<()> {
    let max_files = extract_optional_u64(&job.payload, "max_files").map(|value| value as i64);
    let fetch_batch_size = extract_optional_u64(&job.payload, "fetch_batch_size")
        .map(|value| value.max(1) as usize)
        .unwrap_or(config.hash_fetch_batch_size);

    let algorithm = extract_optional_string(&job.payload, "algorithm")
        .map(|value| HashAlgorithm::parse(&value))
        .transpose()?
        .unwrap_or(config.hash_algorithm);

    let mut counters = HashCounters::default();
    let mut limiter = IoRateLimiter::new(config.io_rate_limit_mib_per_sec);

    loop {
        if let Some(limit) = max_files {
            if counters.processed_files >= limit {
                break;
            }
        }

        let remaining = max_files
            .map(|limit| (limit - counters.processed_files).max(0) as usize)
            .unwrap_or(fetch_batch_size);
        let current_batch_size = remaining.min(fetch_batch_size);
        if current_batch_size == 0 {
            break;
        }

        let claim_token = Alphanumeric.sample_string(&mut rand::thread_rng(), 32);
        let candidates = claim_candidates(conn, config, current_batch_size, &claim_token)?;
        if candidates.is_empty() {
            break;
        }

        for candidate in candidates {
            counters.processed_files += 1;

            match process_candidate(conn, config, &candidate, algorithm, &mut limiter)? {
                CandidateOutcome::Hashed(bytes_hashed) => {
                    counters.hashed_files += 1;
                    counters.bytes_hashed += bytes_hashed as i64;
                }
                CandidateOutcome::Requeued => counters.requeued_files += 1,
                CandidateOutcome::Missing => counters.missing_files += 1,
                CandidateOutcome::Failed => counters.failed_files += 1,
            }

            if counters.processed_files % 64 == 0 {
                refresh_job_lease(conn, config, &job.id, counters.processed_files, 0.0)?;
            }
        }
    }

    refresh_job_lease(conn, config, &job.id, counters.processed_files, 1.0)?;
    println!(
        "hash summary processed={} hashed={} requeued={} missing={} failed={} bytes_hashed={}",
        counters.processed_files,
        counters.hashed_files,
        counters.requeued_files,
        counters.missing_files,
        counters.failed_files,
        counters.bytes_hashed
    );
    Ok(())
}

fn claim_candidates(
    conn: &Connection,
    config: &WorkerConfig,
    batch_size: usize,
    claim_token: &str,
) -> Result<Vec<HashCandidate>> {
    let claim_expiry = format!("-{} seconds", config.hash_claim_ttl_seconds);

    let mut candidate_ids = Vec::new();
    {
        let mut stmt = conn.prepare(
            "
            SELECT id
            FROM library_files
            WHERE needs_hash = 1
              AND is_missing = 0
              AND (hash_retry_after IS NULL OR datetime(hash_retry_after) <= CURRENT_TIMESTAMP)
              AND (
                hash_claim_token IS NULL
                OR hash_claimed_at IS NULL
                OR datetime(hash_claimed_at) <= datetime('now', ?1)
              )
            ORDER BY id ASC
            LIMIT ?2
            ",
        )?;

        let rows = stmt.query_map(params![claim_expiry, batch_size as i64], |row| {
            row.get::<_, i64>(0)
        })?;
        for row in rows {
            candidate_ids.push(row?);
        }
    }

    if candidate_ids.is_empty() {
        return Ok(Vec::new());
    }

    for id in &candidate_ids {
        conn.execute(
            "
            UPDATE library_files
            SET hash_claim_token = ?1,
                hash_claimed_at = CURRENT_TIMESTAMP,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ?2
              AND (
                hash_claim_token IS NULL
                OR hash_claimed_at IS NULL
                OR datetime(hash_claimed_at) <= datetime('now', ?3)
              )
            ",
            params![claim_token, id, claim_expiry],
        )?;
    }

    let mut stmt = conn.prepare(
        "
        SELECT f.id, f.relative_path, f.size_bytes, f.mtime_ns, COALESCE(f.hash_error_count, 0), r.root_path
        FROM library_files f
        JOIN library_roots r ON r.id = f.library_id
        WHERE f.hash_claim_token = ?1
        ORDER BY f.id ASC
        ",
    )?;

    let rows = stmt.query_map(params![claim_token], |row| {
        Ok(HashCandidate {
            id: row.get::<_, i64>(0)?,
            relative_path: row.get::<_, String>(1)?,
            expected_size: row.get::<_, i64>(2)?,
            expected_mtime_ns: row.get::<_, i64>(3)?,
            hash_error_count: row.get::<_, i64>(4)?,
            root_path: row.get::<_, String>(5)?,
        })
    })?;

    let mut candidates = Vec::new();
    for row in rows {
        candidates.push(row?);
    }

    Ok(candidates)
}

enum CandidateOutcome {
    Hashed(u64),
    Requeued,
    Missing,
    Failed,
}

fn process_candidate(
    conn: &Connection,
    config: &WorkerConfig,
    candidate: &HashCandidate,
    algorithm: HashAlgorithm,
    limiter: &mut IoRateLimiter,
) -> Result<CandidateOutcome> {
    let path = resolve_candidate_path(config, &candidate.root_path, &candidate.relative_path)?;

    if !path.exists() || !path.is_file() {
        conn.execute(
            "
            UPDATE library_files
            SET is_missing = 1,
                needs_hash = 0,
                hash_claim_token = NULL,
                hash_claimed_at = NULL,
                hash_retry_after = NULL,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ?1
            ",
            params![candidate.id],
        )?;
        return Ok(CandidateOutcome::Missing);
    }

    let stat_before = match fs::metadata(&path) {
        Ok(meta) => meta,
        Err(error) => {
            mark_failure(conn, config, candidate, &error.to_string())?;
            return Ok(CandidateOutcome::Failed);
        }
    };

    let (size_before, mtime_before, inode_before, device_before) = metadata_to_row(&stat_before)?;
    if size_before != candidate.expected_size || mtime_before != candidate.expected_mtime_ns {
        mark_requeue(
            conn,
            candidate,
            size_before,
            mtime_before,
            inode_before,
            device_before,
        )?;
        return Ok(CandidateOutcome::Requeued);
    }

    let (digest, bytes_hashed) =
        match compute_hash(&path, algorithm, config.hash_read_chunk_bytes, limiter) {
            Ok(value) => value,
            Err(error) => {
                mark_failure(conn, config, candidate, &error.to_string())?;
                return Ok(CandidateOutcome::Failed);
            }
        };

    let stat_after = match fs::metadata(&path) {
        Ok(meta) => meta,
        Err(error) => {
            mark_failure(conn, config, candidate, &error.to_string())?;
            return Ok(CandidateOutcome::Failed);
        }
    };

    let (size_after, mtime_after, inode_after, device_after) = metadata_to_row(&stat_after)?;
    if size_after != candidate.expected_size || mtime_after != candidate.expected_mtime_ns {
        mark_requeue(
            conn,
            candidate,
            size_after,
            mtime_after,
            inode_after,
            device_after,
        )?;
        return Ok(CandidateOutcome::Requeued);
    }

    conn.execute(
        "
        UPDATE library_files
        SET is_missing = 0,
            needs_hash = 0,
            hash_algorithm = ?1,
            content_hash = ?2,
            hashed_size_bytes = ?3,
            hashed_mtime_ns = ?4,
            hashed_at = CURRENT_TIMESTAMP,
            hash_error_count = 0,
            hash_last_error = NULL,
            hash_last_error_at = NULL,
            hash_retry_after = NULL,
            hash_claim_token = NULL,
            hash_claimed_at = NULL,
            updated_at = CURRENT_TIMESTAMP
        WHERE id = ?5
        ",
        params![
            algorithm.as_db_value(),
            digest,
            size_after,
            mtime_after,
            candidate.id
        ],
    )?;

    Ok(CandidateOutcome::Hashed(bytes_hashed))
}

fn resolve_candidate_path(
    config: &WorkerConfig,
    root_path: &str,
    relative_path: &str,
) -> Result<PathBuf> {
    let root =
        resolve_root_under_libraries(&config.libraries_root_real, &PathBuf::from(root_path))?;
    let relative = validate_relative_path(relative_path)?;
    let candidate = root.join(relative);

    if candidate.exists() {
        let real_candidate = candidate.canonicalize().with_context(|| {
            format!("failed to resolve candidate path: {}", candidate.display())
        })?;
        if !real_candidate.starts_with(&root) {
            bail!("candidate path escapes library root");
        }
        return Ok(real_candidate);
    }

    Ok(candidate)
}

fn mark_requeue(
    conn: &Connection,
    candidate: &HashCandidate,
    size_bytes: i64,
    mtime_ns: i64,
    inode: Option<i64>,
    device: Option<i64>,
) -> Result<()> {
    conn.execute(
        "
        UPDATE library_files
        SET size_bytes = ?1,
            mtime_ns = ?2,
            inode = ?3,
            device = ?4,
            is_missing = 0,
            needs_hash = 1,
            hash_algorithm = NULL,
            content_hash = NULL,
            hashed_size_bytes = NULL,
            hashed_mtime_ns = NULL,
            hashed_at = NULL,
            hash_error_count = 0,
            hash_last_error = NULL,
            hash_last_error_at = NULL,
            hash_retry_after = NULL,
            hash_claim_token = NULL,
            hash_claimed_at = NULL,
            updated_at = CURRENT_TIMESTAMP
        WHERE id = ?5
        ",
        params![size_bytes, mtime_ns, inode, device, candidate.id],
    )?;
    Ok(())
}

fn mark_failure(
    conn: &Connection,
    config: &WorkerConfig,
    candidate: &HashCandidate,
    message: &str,
) -> Result<()> {
    let next_error_count = candidate.hash_error_count.saturating_add(1);
    let retry_seconds = calculate_retry_delay_seconds(
        config.hash_retry_base_seconds,
        config.hash_retry_max_seconds,
        next_error_count as u64,
    );
    let retry_modifier = format!("+{} seconds", retry_seconds);

    conn.execute(
        "
        UPDATE library_files
        SET needs_hash = 1,
            hash_error_count = ?1,
            hash_last_error = ?2,
            hash_last_error_at = CURRENT_TIMESTAMP,
            hash_retry_after = datetime('now', ?3),
            hash_claim_token = NULL,
            hash_claimed_at = NULL,
            updated_at = CURRENT_TIMESTAMP
        WHERE id = ?4
        ",
        params![next_error_count, message, retry_modifier, candidate.id],
    )?;

    Ok(())
}

fn compute_hash(
    path: &PathBuf,
    algorithm: HashAlgorithm,
    chunk_size: usize,
    limiter: &mut IoRateLimiter,
) -> Result<(Vec<u8>, u64)> {
    let mut file = fs::File::open(path)
        .with_context(|| format!("failed to open file for hashing: {}", path.display()))?;

    let mut buffer = vec![0_u8; chunk_size];
    let mut total_bytes = 0_u64;

    match algorithm {
        HashAlgorithm::Blake3 => {
            let mut hasher = Blake3Hasher::new();
            loop {
                let bytes_read = file.read(&mut buffer)?;
                if bytes_read == 0 {
                    break;
                }
                hasher.update(&buffer[..bytes_read]);
                total_bytes = total_bytes.saturating_add(bytes_read as u64);
                limiter.consume(bytes_read);
            }
            Ok((hasher.finalize().as_bytes().to_vec(), total_bytes))
        }
        HashAlgorithm::Sha256 => {
            let mut hasher = Sha256::new();
            loop {
                let bytes_read = file.read(&mut buffer)?;
                if bytes_read == 0 {
                    break;
                }
                hasher.update(&buffer[..bytes_read]);
                total_bytes = total_bytes.saturating_add(bytes_read as u64);
                limiter.consume(bytes_read);
            }
            Ok((hasher.finalize().to_vec(), total_bytes))
        }
    }
}

fn calculate_retry_delay_seconds(base_seconds: u64, max_seconds: u64, error_count: u64) -> u64 {
    let capped_power = error_count.saturating_sub(1).min(10);
    let delay = base_seconds.saturating_mul(1_u64 << capped_power);
    delay.min(max_seconds)
}

fn extract_optional_u64(payload: &Value, key: &str) -> Option<u64> {
    payload.get(key).and_then(|value| value.as_u64())
}

fn extract_optional_string(payload: &Value, key: &str) -> Option<String> {
    payload
        .get(key)
        .and_then(|value| value.as_str())
        .map(ToString::to_string)
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

struct IoRateLimiter {
    bytes_per_second: Option<f64>,
    window_start: Instant,
    bytes_in_window: u64,
}

impl IoRateLimiter {
    fn new(mib_per_sec: Option<u64>) -> Self {
        Self {
            bytes_per_second: mib_per_sec.map(|mib| (mib * 1024 * 1024) as f64),
            window_start: Instant::now(),
            bytes_in_window: 0,
        }
    }

    fn consume(&mut self, bytes: usize) {
        let Some(limit) = self.bytes_per_second else {
            return;
        };

        self.bytes_in_window = self.bytes_in_window.saturating_add(bytes as u64);
        let elapsed = self.window_start.elapsed().as_secs_f64();
        let expected = (self.bytes_in_window as f64) / limit;

        if expected > elapsed {
            thread::sleep(Duration::from_secs_f64(expected - elapsed));
        }

        if self.window_start.elapsed().as_secs_f64() >= 1.0 {
            self.window_start = Instant::now();
            self.bytes_in_window = 0;
        }
    }
}
