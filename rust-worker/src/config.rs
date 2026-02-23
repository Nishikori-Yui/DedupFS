use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use rand::distributions::{Alphanumeric, DistString};
use serde::Deserialize;

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum HashAlgorithm {
    Blake3,
    Sha256,
}

impl HashAlgorithm {
    pub fn as_db_value(self) -> &'static str {
        match self {
            HashAlgorithm::Blake3 => "blake3",
            HashAlgorithm::Sha256 => "sha256",
        }
    }

    pub fn parse(raw: &str) -> Result<Self> {
        match raw.trim().to_lowercase().as_str() {
            "blake3" => Ok(HashAlgorithm::Blake3),
            "sha256" => Ok(HashAlgorithm::Sha256),
            _ => bail!("unsupported hash algorithm: {raw}"),
        }
    }
}

#[derive(Debug, Default, Deserialize)]
struct PartialWorkerConfig {
    state_root: Option<PathBuf>,
    libraries_root: Option<PathBuf>,
    database_path: Option<PathBuf>,
    thumbs_root: Option<PathBuf>,
    concurrency: Option<usize>,
    io_rate_limit_mib_per_sec: Option<u64>,
    hash_algorithm: Option<HashAlgorithm>,
    scan_write_batch_size: Option<usize>,
    hash_fetch_batch_size: Option<usize>,
    hash_read_chunk_bytes: Option<usize>,
    hash_claim_ttl_seconds: Option<u64>,
    hash_retry_base_seconds: Option<u64>,
    hash_retry_max_seconds: Option<u64>,
    job_lock_ttl_seconds: Option<u64>,
    thumbnail_image_concurrency: Option<usize>,
    thumbnail_video_concurrency: Option<usize>,
    thumbnail_io_rate_limit_mib_per_sec: Option<u64>,
    thumbnail_retry_base_seconds: Option<u64>,
    thumbnail_retry_max_seconds: Option<u64>,
    thumbnail_ffmpeg_bin: Option<String>,
    thumbnail_ffmpeg_timeout_seconds: Option<u64>,
    thumbnail_max_dimension: Option<usize>,
    rust_worker_poll_seconds: Option<u64>,
    rust_worker_max_poll_seconds: Option<u64>,
    rust_worker_poll_jitter_millis: Option<u64>,
    wal_checkpoint_retry_seconds: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct WorkerConfig {
    pub libraries_root: PathBuf,
    pub libraries_root_real: PathBuf,
    pub database_path: PathBuf,
    pub thumbs_root_real: PathBuf,
    pub concurrency: usize,
    pub io_rate_limit_mib_per_sec: Option<u64>,
    pub hash_algorithm: HashAlgorithm,
    pub scan_write_batch_size: usize,
    pub hash_fetch_batch_size: usize,
    pub hash_read_chunk_bytes: usize,
    pub hash_claim_ttl_seconds: u64,
    pub hash_retry_base_seconds: u64,
    pub hash_retry_max_seconds: u64,
    pub job_lock_ttl_seconds: u64,
    pub thumbnail_image_concurrency: usize,
    pub thumbnail_video_concurrency: usize,
    pub thumbnail_io_rate_limit_mib_per_sec: Option<u64>,
    pub thumbnail_retry_base_seconds: u64,
    pub thumbnail_retry_max_seconds: u64,
    pub thumbnail_ffmpeg_bin: String,
    pub thumbnail_ffmpeg_timeout_seconds: u64,
    pub thumbnail_max_dimension: usize,
    pub rust_worker_poll_seconds: u64,
    pub rust_worker_max_poll_seconds: u64,
    pub rust_worker_poll_jitter_millis: u64,
    pub wal_checkpoint_retry_seconds: u64,
    pub worker_id: String,
}

impl WorkerConfig {
    pub fn load(config_path: Option<&Path>, worker_id_override: Option<&str>) -> Result<Self> {
        let mut partial = PartialWorkerConfig::default();

        if let Some(path) = config_path {
            let content = fs::read_to_string(path)
                .with_context(|| format!("failed to read config file: {}", path.display()))?;
            partial = toml::from_str(&content)
                .with_context(|| format!("failed to parse config TOML: {}", path.display()))?;
        }

        if let Ok(value) = std::env::var("DEDUPFS_LIBRARIES_ROOT") {
            partial.libraries_root = Some(PathBuf::from(value));
        }
        if let Ok(value) = std::env::var("DEDUPFS_STATE_ROOT") {
            let state_root = PathBuf::from(value);
            partial.state_root = Some(state_root.clone());
            partial.database_path = Some(state_root.join("dedupfs.sqlite3"));
            if partial.thumbs_root.is_none() {
                partial.thumbs_root = Some(state_root.join("thumbs"));
            }
        }
        if let Ok(value) = std::env::var("DEDUPFS_THUMBS_ROOT") {
            partial.thumbs_root = Some(PathBuf::from(value));
        }
        if let Ok(value) = std::env::var("DEDUPFS_DATABASE_URL") {
            if let Some(path) = value.strip_prefix("sqlite:///") {
                partial.database_path = Some(PathBuf::from(path));
            }
        }
        if let Ok(value) = std::env::var("DEDUPFS_RUST_WORKER_CONCURRENCY") {
            partial.concurrency = Some(
                value
                    .parse()
                    .context("invalid DEDUPFS_RUST_WORKER_CONCURRENCY")?,
            );
        }
        if let Ok(value) = std::env::var("DEDUPFS_RUST_WORKER_IO_RATE_LIMIT_MIB_PER_SEC") {
            partial.io_rate_limit_mib_per_sec = Some(
                value
                    .parse()
                    .context("invalid DEDUPFS_RUST_WORKER_IO_RATE_LIMIT_MIB_PER_SEC")?,
            );
        }
        if let Ok(value) = std::env::var("DEDUPFS_DEFAULT_HASH_ALGORITHM") {
            partial.hash_algorithm = Some(HashAlgorithm::parse(&value)?);
        }
        if let Ok(value) = std::env::var("DEDUPFS_SCAN_WRITE_BATCH_SIZE") {
            partial.scan_write_batch_size = Some(
                value
                    .parse()
                    .context("invalid DEDUPFS_SCAN_WRITE_BATCH_SIZE")?,
            );
        }
        if let Ok(value) = std::env::var("DEDUPFS_HASH_FETCH_BATCH_SIZE") {
            partial.hash_fetch_batch_size = Some(
                value
                    .parse()
                    .context("invalid DEDUPFS_HASH_FETCH_BATCH_SIZE")?,
            );
        }
        if let Ok(value) = std::env::var("DEDUPFS_HASH_READ_CHUNK_BYTES") {
            partial.hash_read_chunk_bytes = Some(
                value
                    .parse()
                    .context("invalid DEDUPFS_HASH_READ_CHUNK_BYTES")?,
            );
        }
        if let Ok(value) = std::env::var("DEDUPFS_HASH_CLAIM_TTL_SECONDS") {
            partial.hash_claim_ttl_seconds = Some(
                value
                    .parse()
                    .context("invalid DEDUPFS_HASH_CLAIM_TTL_SECONDS")?,
            );
        }
        if let Ok(value) = std::env::var("DEDUPFS_HASH_RETRY_BASE_SECONDS") {
            partial.hash_retry_base_seconds = Some(
                value
                    .parse()
                    .context("invalid DEDUPFS_HASH_RETRY_BASE_SECONDS")?,
            );
        }
        if let Ok(value) = std::env::var("DEDUPFS_HASH_RETRY_MAX_SECONDS") {
            partial.hash_retry_max_seconds = Some(
                value
                    .parse()
                    .context("invalid DEDUPFS_HASH_RETRY_MAX_SECONDS")?,
            );
        }
        if let Ok(value) = std::env::var("DEDUPFS_JOB_LOCK_TTL_SECONDS") {
            partial.job_lock_ttl_seconds = Some(
                value
                    .parse()
                    .context("invalid DEDUPFS_JOB_LOCK_TTL_SECONDS")?,
            );
        }
        if let Ok(value) = std::env::var("DEDUPFS_THUMBNAIL_IMAGE_CONCURRENCY") {
            partial.thumbnail_image_concurrency = Some(
                value
                    .parse()
                    .context("invalid DEDUPFS_THUMBNAIL_IMAGE_CONCURRENCY")?,
            );
        }
        if let Ok(value) = std::env::var("DEDUPFS_THUMBNAIL_VIDEO_CONCURRENCY") {
            partial.thumbnail_video_concurrency = Some(
                value
                    .parse()
                    .context("invalid DEDUPFS_THUMBNAIL_VIDEO_CONCURRENCY")?,
            );
        }
        if let Ok(value) = std::env::var("DEDUPFS_THUMBNAIL_IO_RATE_LIMIT_MIB_PER_SEC") {
            partial.thumbnail_io_rate_limit_mib_per_sec = Some(
                value
                    .parse()
                    .context("invalid DEDUPFS_THUMBNAIL_IO_RATE_LIMIT_MIB_PER_SEC")?,
            );
        }
        if let Ok(value) = std::env::var("DEDUPFS_THUMBNAIL_RETRY_BASE_SECONDS") {
            partial.thumbnail_retry_base_seconds = Some(
                value
                    .parse()
                    .context("invalid DEDUPFS_THUMBNAIL_RETRY_BASE_SECONDS")?,
            );
        }
        if let Ok(value) = std::env::var("DEDUPFS_THUMBNAIL_RETRY_MAX_SECONDS") {
            partial.thumbnail_retry_max_seconds = Some(
                value
                    .parse()
                    .context("invalid DEDUPFS_THUMBNAIL_RETRY_MAX_SECONDS")?,
            );
        }
        if let Ok(value) = std::env::var("DEDUPFS_THUMBNAIL_FFMPEG_BIN") {
            partial.thumbnail_ffmpeg_bin = Some(value);
        }
        if let Ok(value) = std::env::var("DEDUPFS_THUMBNAIL_FFMPEG_TIMEOUT_SECONDS") {
            partial.thumbnail_ffmpeg_timeout_seconds = Some(
                value
                    .parse()
                    .context("invalid DEDUPFS_THUMBNAIL_FFMPEG_TIMEOUT_SECONDS")?,
            );
        }
        if let Ok(value) = std::env::var("DEDUPFS_THUMBNAIL_MAX_DIMENSION") {
            partial.thumbnail_max_dimension = Some(
                value
                    .parse()
                    .context("invalid DEDUPFS_THUMBNAIL_MAX_DIMENSION")?,
            );
        }
        if let Ok(value) = std::env::var("DEDUPFS_RUST_WORKER_POLL_SECONDS") {
            partial.rust_worker_poll_seconds = Some(
                value
                    .parse()
                    .context("invalid DEDUPFS_RUST_WORKER_POLL_SECONDS")?,
            );
        }
        if let Ok(value) = std::env::var("DEDUPFS_RUST_WORKER_MAX_POLL_SECONDS") {
            partial.rust_worker_max_poll_seconds = Some(
                value
                    .parse()
                    .context("invalid DEDUPFS_RUST_WORKER_MAX_POLL_SECONDS")?,
            );
        }
        if let Ok(value) = std::env::var("DEDUPFS_RUST_WORKER_POLL_JITTER_MILLIS") {
            partial.rust_worker_poll_jitter_millis = Some(
                value
                    .parse()
                    .context("invalid DEDUPFS_RUST_WORKER_POLL_JITTER_MILLIS")?,
            );
        }
        if let Ok(value) = std::env::var("DEDUPFS_WAL_CHECKPOINT_RETRY_SECONDS") {
            partial.wal_checkpoint_retry_seconds = Some(
                value
                    .parse()
                    .context("invalid DEDUPFS_WAL_CHECKPOINT_RETRY_SECONDS")?,
            );
        }

        let libraries_root = partial
            .libraries_root
            .unwrap_or_else(|| PathBuf::from("/libraries"));
        if !libraries_root.is_absolute() {
            bail!("libraries_root must be absolute");
        }
        if libraries_root.as_os_str() != "/libraries" {
            bail!("libraries_root must resolve to /libraries");
        }

        let libraries_root_real = match libraries_root.canonicalize() {
            Ok(path) => {
                if !path.is_dir() {
                    bail!("libraries_root is not a directory: {}", path.display());
                }
                path
            }
            Err(_) => libraries_root.clone(),
        };

        let database_path = partial
            .database_path
            .unwrap_or_else(|| PathBuf::from("/state/dedupfs.sqlite3"));
        if !database_path.is_absolute() {
            bail!("database_path must be absolute");
        }

        let state_root = partial
            .state_root
            .unwrap_or_else(|| PathBuf::from("/state"));
        if !state_root.is_absolute() {
            bail!("state_root must be absolute");
        }
        fs::create_dir_all(&state_root)
            .with_context(|| format!("failed to create state_root: {}", state_root.display()))?;
        let state_root_real = state_root
            .canonicalize()
            .with_context(|| format!("failed to resolve state_root: {}", state_root.display()))?;
        if !state_root_real.is_dir() {
            bail!(
                "state_root is not a directory: {}",
                state_root_real.display()
            );
        }
        if database_path != state_root && !database_path.starts_with(&state_root) {
            bail!("database_path must be under state_root");
        }

        let thumbs_root = partial
            .thumbs_root
            .unwrap_or_else(|| PathBuf::from("/state/thumbs"));
        if !thumbs_root.is_absolute() {
            bail!("thumbs_root must be absolute");
        }
        fs::create_dir_all(&thumbs_root)
            .with_context(|| format!("failed to create thumbs_root: {}", thumbs_root.display()))?;
        let thumbs_root_real = thumbs_root
            .canonicalize()
            .with_context(|| format!("failed to resolve thumbs_root: {}", thumbs_root.display()))?;
        if !thumbs_root_real.is_dir() {
            bail!(
                "thumbs_root is not a directory: {}",
                thumbs_root_real.display()
            );
        }
        if thumbs_root_real != state_root_real && !thumbs_root_real.starts_with(&state_root_real) {
            bail!("thumbs_root must resolve under state_root");
        }

        let worker_id = match worker_id_override {
            Some(value) if !value.trim().is_empty() => value.trim().to_string(),
            Some(_) => bail!("worker_id cannot be blank"),
            None => {
                let suffix = Alphanumeric.sample_string(&mut rand::thread_rng(), 10);
                format!("rust-worker-{suffix}")
            }
        };

        let concurrency = partial.concurrency.unwrap_or(4).max(1);
        let scan_write_batch_size = partial.scan_write_batch_size.unwrap_or(2000).max(1);
        let hash_fetch_batch_size = partial.hash_fetch_batch_size.unwrap_or(512).max(1);
        let hash_read_chunk_bytes = partial
            .hash_read_chunk_bytes
            .unwrap_or(4 * 1024 * 1024)
            .max(1024);
        let hash_claim_ttl_seconds = partial.hash_claim_ttl_seconds.unwrap_or(600).max(1);
        let hash_retry_base_seconds = partial.hash_retry_base_seconds.unwrap_or(30).max(1);
        let hash_retry_max_seconds = partial
            .hash_retry_max_seconds
            .unwrap_or(3600)
            .max(hash_retry_base_seconds);
        let job_lock_ttl_seconds = partial.job_lock_ttl_seconds.unwrap_or(300).max(1);

        let thumbnail_image_concurrency = partial.thumbnail_image_concurrency.unwrap_or(2).max(1);
        let thumbnail_video_concurrency = partial.thumbnail_video_concurrency.unwrap_or(1).max(1);
        let thumbnail_retry_base_seconds =
            partial.thumbnail_retry_base_seconds.unwrap_or(30).max(1);
        let thumbnail_retry_max_seconds = partial
            .thumbnail_retry_max_seconds
            .unwrap_or(1800)
            .max(thumbnail_retry_base_seconds);
        let thumbnail_ffmpeg_bin = partial
            .thumbnail_ffmpeg_bin
            .unwrap_or_else(|| "ffmpeg".to_string())
            .trim()
            .to_string();
        if thumbnail_ffmpeg_bin.is_empty() {
            bail!("thumbnail_ffmpeg_bin cannot be blank");
        }
        let thumbnail_ffmpeg_timeout_seconds = partial
            .thumbnail_ffmpeg_timeout_seconds
            .unwrap_or(120)
            .max(1);
        let thumbnail_max_dimension = partial.thumbnail_max_dimension.unwrap_or(256).max(16);
        let rust_worker_poll_seconds = partial.rust_worker_poll_seconds.unwrap_or(5).max(1);
        let rust_worker_max_poll_seconds = partial
            .rust_worker_max_poll_seconds
            .unwrap_or(30)
            .max(rust_worker_poll_seconds);
        let rust_worker_poll_jitter_millis = partial.rust_worker_poll_jitter_millis.unwrap_or(250);
        let wal_checkpoint_retry_seconds = partial.wal_checkpoint_retry_seconds.unwrap_or(120).max(1);

        Ok(Self {
            libraries_root,
            libraries_root_real,
            database_path,
            thumbs_root_real,
            concurrency,
            io_rate_limit_mib_per_sec: partial.io_rate_limit_mib_per_sec,
            hash_algorithm: partial.hash_algorithm.unwrap_or(HashAlgorithm::Blake3),
            scan_write_batch_size,
            hash_fetch_batch_size,
            hash_read_chunk_bytes,
            hash_claim_ttl_seconds,
            hash_retry_base_seconds,
            hash_retry_max_seconds,
            job_lock_ttl_seconds,
            thumbnail_image_concurrency,
            thumbnail_video_concurrency,
            thumbnail_io_rate_limit_mib_per_sec: partial.thumbnail_io_rate_limit_mib_per_sec,
            thumbnail_retry_base_seconds,
            thumbnail_retry_max_seconds,
            thumbnail_ffmpeg_bin,
            thumbnail_ffmpeg_timeout_seconds,
            thumbnail_max_dimension,
            rust_worker_poll_seconds,
            rust_worker_max_poll_seconds,
            rust_worker_poll_jitter_millis,
            wal_checkpoint_retry_seconds,
            worker_id,
        })
    }
}
