mod config;
mod db;
mod hash;
mod path_safety;
mod scan;
mod thumbnail;

use std::path::PathBuf;
use std::thread;
use std::time::Duration;

use anyhow::{bail, Result};
use clap::Parser;
use rand::Rng;

use crate::config::WorkerConfig;
use crate::db::{
    claim_scan_hash_job, claim_thumbnail_cleanup_job, claim_thumbnail_task,
    claim_wal_maintenance_job, execute_wal_checkpoint, finish_job,
    finish_thumbnail_cleanup_job, finish_thumbnail_failure, finish_thumbnail_success,
    finish_wal_maintenance_failure, finish_wal_maintenance_success,
    has_runnable_scan_hash_work, has_runnable_thumbnail_cleanup_work, has_runnable_thumbnail_work,
    has_runnable_wal_maintenance_work, open_connection, requeue_wal_maintenance_retry, JobKind,
};
use crate::hash::run_hash_job;
use crate::scan::run_scan_job;
use crate::thumbnail::{classify_thumbnail_error, run_thumbnail_cleanup_task, run_thumbnail_task};

#[derive(Debug, Parser)]
#[command(name = "dedupfs-rust-worker", version)]
struct Cli {
    #[arg(long)]
    config: Option<PathBuf>,

    #[arg(long)]
    job_id: Option<String>,

    #[arg(long)]
    worker_id: Option<String>,

    #[arg(long, default_value_t = false)]
    daemon: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CycleOutcome {
    DidWork,
    Idle,
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    let config = WorkerConfig::load(cli.config.as_deref(), cli.worker_id.as_deref())?;

    let mut conn = open_connection(&config.database_path)?;

    if cli.daemon {
        if cli.job_id.is_some() {
            bail!("--job-id cannot be used with --daemon");
        }
        return run_daemon_loop(&mut conn, &config);
    }

    match run_worker_cycle(&mut conn, &config, cli.job_id.as_deref(), true) {
        Ok(CycleOutcome::DidWork) => Ok(()),
        Ok(CycleOutcome::Idle) => {
            println!("no runnable rust tasks found");
            Ok(())
        }
        Err(error) => Err(error),
    }
}

fn run_daemon_loop(conn: &mut rusqlite::Connection, config: &WorkerConfig) -> Result<()> {
    let mut idle_backoff_seconds = config.rust_worker_poll_seconds.max(1);

    loop {
        match run_worker_cycle(conn, config, None, false) {
            Ok(CycleOutcome::DidWork) => {
                idle_backoff_seconds = config.rust_worker_poll_seconds.max(1);
            }
            Ok(CycleOutcome::Idle) => {
                sleep_with_jitter(idle_backoff_seconds, config.rust_worker_poll_jitter_millis);
                idle_backoff_seconds = next_idle_backoff_seconds(
                    idle_backoff_seconds,
                    config.rust_worker_poll_seconds,
                    config.rust_worker_max_poll_seconds,
                );
            }
            Err(error) => {
                let error_message = sanitize_error_message(&error.to_string(), &config);
                eprintln!(
                    "worker={} daemon-cycle-error={}",
                    config.worker_id, error_message
                );
                sleep_with_jitter(idle_backoff_seconds, config.rust_worker_poll_jitter_millis);
                idle_backoff_seconds = next_idle_backoff_seconds(
                    idle_backoff_seconds,
                    config.rust_worker_poll_seconds,
                    config.rust_worker_max_poll_seconds,
                );
            }
        }
    }
}

fn run_worker_cycle(
    conn: &mut rusqlite::Connection,
    config: &WorkerConfig,
    requested_job_id: Option<&str>,
    propagate_task_errors: bool,
) -> Result<CycleOutcome> {
    let scan_hash_runnable = if requested_job_id.is_some() {
        true
    } else {
        has_runnable_scan_hash_work(conn)?
    };
    if scan_hash_runnable {
        if let Some(job) = claim_scan_hash_job(conn, config, requested_job_id)? {
            println!(
                "worker={} backend=rust concurrency={} job={} kind={:?}",
                config.worker_id, config.concurrency, job.id, job.kind
            );

            let result = match job.kind {
                JobKind::Scan => run_scan_job(conn, config, &job),
                JobKind::Hash => run_hash_job(conn, config, &job),
            };

            return match result {
                Ok(()) => {
                    finish_job(conn, config, &job.id, true, None)?;
                    println!("job {} finished successfully", job.id);
                    Ok(CycleOutcome::DidWork)
                }
                Err(error) => {
                    let message = sanitize_error_message(&error.to_string(), config);
                    let _ = finish_job(conn, config, &job.id, false, Some(&message));
                    if propagate_task_errors {
                        Err(error)
                    } else {
                        eprintln!("job {} failed and persisted as failed: {}", job.id, message);
                        Ok(CycleOutcome::DidWork)
                    }
                }
            };
        }
    }

    if has_runnable_thumbnail_work(conn)? {
        if let Some(task) = claim_thumbnail_task(conn, config)? {
            println!(
                "worker={} thumbnail_task={} file_id={} media_type={}",
                config.worker_id, task.thumb_key, task.file_id, task.media_type
            );

            return match run_thumbnail_task(conn, config, &task) {
                Ok((width, height, bytes_size)) => {
                    finish_thumbnail_success(conn, config, task.id, width, height, bytes_size)?;
                    println!(
                        "thumbnail task {} finished successfully ({}x{}, {} bytes)",
                        task.thumb_key, width, height, bytes_size
                    );
                    Ok(CycleOutcome::DidWork)
                }
                Err(error) => {
                    let error_code = classify_thumbnail_error(&error);
                    let error_message = sanitize_error_message(&error.to_string(), config);
                    let _ = finish_thumbnail_failure(
                        conn,
                        config,
                        task.id,
                        task.error_count,
                        error_code,
                        &error_message,
                    );
                    if propagate_task_errors {
                        Err(error)
                    } else {
                        eprintln!(
                            "thumbnail task {} failed and persisted as failed: {}",
                            task.thumb_key, error_message
                        );
                        Ok(CycleOutcome::DidWork)
                    }
                }
            };
        }
    }

    if has_runnable_thumbnail_cleanup_work(conn)? {
        if let Some(cleanup) = claim_thumbnail_cleanup_job(conn, config)? {
            println!(
                "worker={} thumbnail_cleanup_job={} group_key={}",
                config.worker_id, cleanup.id, cleanup.group_key
            );

            return match run_thumbnail_cleanup_task(conn, config, &cleanup) {
                Ok(removed_rows) => {
                    finish_thumbnail_cleanup_job(conn, config, cleanup.id, true, None, None)?;
                    println!(
                        "thumbnail cleanup job {} finished successfully (removed rows={})",
                        cleanup.id, removed_rows
                    );
                    Ok(CycleOutcome::DidWork)
                }
                Err(error) => {
                    let error_message = sanitize_error_message(&error.to_string(), config);
                    let _ = finish_thumbnail_cleanup_job(
                        conn,
                        config,
                        cleanup.id,
                        false,
                        Some("THUMB_CLEANUP_FAILED"),
                        Some(&error_message),
                    );
                    if propagate_task_errors {
                        Err(error)
                    } else {
                        eprintln!(
                            "thumbnail cleanup job {} failed and persisted as failed: {}",
                            cleanup.id, error_message
                        );
                        Ok(CycleOutcome::DidWork)
                    }
                }
            };
        }
    }

    if has_runnable_wal_maintenance_work(conn)? {
        if let Some(maintenance_job) = claim_wal_maintenance_job(conn, config)? {
            println!(
                "worker={} wal_maintenance_job={} mode={:?}",
                config.worker_id, maintenance_job.id, maintenance_job.requested_mode
            );

            return match execute_wal_checkpoint(conn, maintenance_job.requested_mode) {
                Ok(stats) => {
                    if stats.busy > 0 {
                        let busy_message = format!(
                            "WAL checkpoint busy={} log_frames={} checkpointed_frames={}",
                            stats.busy, stats.log_frames, stats.checkpointed_frames
                        );
                        let _ = requeue_wal_maintenance_retry(
                            conn,
                            config,
                            maintenance_job.id,
                            maintenance_job.retry_count,
                            "WAL_CHECKPOINT_BUSY",
                            &busy_message,
                            stats,
                        );
                        eprintln!(
                            "wal maintenance job {} busy; requeued for retry",
                            maintenance_job.id
                        );
                        Ok(CycleOutcome::DidWork)
                    } else {
                        finish_wal_maintenance_success(conn, config, maintenance_job.id, stats)?;
                        println!(
                            "wal maintenance job {} finished successfully (log_frames={}, checkpointed_frames={})",
                            maintenance_job.id, stats.log_frames, stats.checkpointed_frames
                        );
                        Ok(CycleOutcome::DidWork)
                    }
                }
                Err(error) => {
                    let message = sanitize_error_message(&error.to_string(), config);
                    let _ = finish_wal_maintenance_failure(
                        conn,
                        config,
                        maintenance_job.id,
                        "WAL_CHECKPOINT_FAILED",
                        &message,
                    );
                    if propagate_task_errors {
                        Err(error)
                    } else {
                        eprintln!(
                            "wal maintenance job {} failed and persisted as failed: {}",
                            maintenance_job.id, message
                        );
                        Ok(CycleOutcome::DidWork)
                    }
                }
            };
        }
    }

    Ok(CycleOutcome::Idle)
}

fn sleep_with_jitter(base_seconds: u64, jitter_millis: u64) {
    let bounded_base = base_seconds.max(1);
    let jitter = if jitter_millis == 0 {
        0
    } else {
        rand::thread_rng().gen_range(0..=jitter_millis)
    };
    thread::sleep(Duration::from_secs(bounded_base) + Duration::from_millis(jitter));
}

fn next_idle_backoff_seconds(current: u64, base: u64, max: u64) -> u64 {
    let bounded_base = base.max(1);
    let bounded_max = max.max(bounded_base);
    current.max(bounded_base).saturating_mul(2).min(bounded_max)
}

fn sanitize_error_message(raw: &str, config: &WorkerConfig) -> String {
    let mut sanitized = raw.to_string();
    let libraries_real = config.libraries_root_real.to_string_lossy().to_string();
    let thumbs_real = config.thumbs_root_real.to_string_lossy().to_string();
    if !libraries_real.is_empty() {
        sanitized = sanitized.replace(&libraries_real, "/libraries");
    }
    if !thumbs_real.is_empty() {
        sanitized = sanitized.replace(&thumbs_real, "/state/thumbs");
    }
    const LIMIT: usize = 1024;
    if sanitized.chars().count() > LIMIT {
        sanitized = sanitized.chars().take(LIMIT).collect::<String>() + "...(truncated)";
    }
    sanitized
}

#[cfg(test)]
mod tests {
    use super::next_idle_backoff_seconds;

    #[test]
    fn idle_backoff_is_bounded_and_monotonic() {
        let base = 5;
        let max = 20;
        assert_eq!(next_idle_backoff_seconds(5, base, max), 10);
        assert_eq!(next_idle_backoff_seconds(10, base, max), 20);
        assert_eq!(next_idle_backoff_seconds(20, base, max), 20);
        assert_eq!(next_idle_backoff_seconds(30, base, max), 20);
    }
}
