use std::fs;
use std::io::Read;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use image::{ImageFormat, ImageReader};
use rusqlite::Connection;

use crate::config::WorkerConfig;
use crate::db::{
    delete_group_thumbnail_rows, list_group_thumbnail_outputs, refresh_thumbnail_cleanup_lease,
    refresh_thumbnail_lease, reserve_global_io_budget, ThumbnailCleanupRecord, ThumbnailTaskRecord,
};
use crate::path_safety::{resolve_root_under_libraries, validate_relative_path};

pub fn run_thumbnail_task(
    conn: &Connection,
    config: &WorkerConfig,
    task: &ThumbnailTaskRecord,
) -> Result<(i64, i64, i64)> {
    refresh_thumbnail_lease(conn, config, task.id)?;
    let mut lease_refresher = LeaseRefresher::new(conn, config, task.id);
    lease_refresher.maybe_refresh()?;

    let source_path = resolve_source_path(config, task)?;
    let metadata = fs::metadata(&source_path)
        .with_context(|| format!("failed to read source metadata: {}", source_path.display()))?;

    let source_size =
        i64::try_from(metadata.len()).context("thumbnail source size over i64 range")?;
    if source_size != task.source_size_bytes {
        bail!("source size changed before thumbnail generation");
    }
    let source_mtime_ns = metadata_mtime_ns(&metadata)?;
    if source_mtime_ns != task.source_mtime_ns {
        bail!("source mtime changed before thumbnail generation");
    }

    let output_path = resolve_output_path(config, task)?;
    let output_path = normalize_output_target(config, &output_path)?;

    let temp_path = output_path.with_file_name(format!("{}.tmp", task.thumb_key));
    let _temp_guard = TempFileGuard::new(temp_path.clone());
    let max_dimension = usize::try_from(task.max_dimension)
        .ok()
        .map(|value| value.min(config.thumbnail_max_dimension))
        .unwrap_or(config.thumbnail_max_dimension)
        .max(16);

    reserve_thumbnail_io_budget(conn, config, metadata.len())?;

    let (width, height) = match task.media_type.as_str() {
        "image" => generate_image_thumbnail(
            &source_path,
            &temp_path,
            max_dimension,
            &task.format,
            &mut lease_refresher,
        )?,
        "video" => generate_video_thumbnail(
            config,
            &source_path,
            &temp_path,
            max_dimension,
            &task.format,
            &mut lease_refresher,
        )?,
        _ => bail!("unsupported thumbnail media_type: {}", task.media_type),
    };
    lease_refresher.maybe_refresh()?;
    reserve_thumbnail_io_budget(conn, config, metadata.len())?;

    if output_path.exists() {
        fs::remove_file(&output_path).with_context(|| {
            format!(
                "failed to replace existing thumbnail output file: {}",
                output_path.display()
            )
        })?;
    }
    fs::rename(&temp_path, &output_path).with_context(|| {
        format!(
            "failed to move thumbnail temp output into final path: {}",
            output_path.display()
        )
    })?;

    let output_bytes = i64::try_from(
        fs::metadata(&output_path)
            .with_context(|| format!("failed to stat thumbnail output: {}", output_path.display()))?
            .len(),
    )
    .context("thumbnail output size over i64 range")?;

    Ok((i64::from(width), i64::from(height), output_bytes))
}

pub fn run_thumbnail_cleanup_task(
    conn: &Connection,
    config: &WorkerConfig,
    cleanup: &ThumbnailCleanupRecord,
) -> Result<usize> {
    refresh_thumbnail_cleanup_lease(conn, config, cleanup.id)?;
    let outputs = list_group_thumbnail_outputs(conn, &cleanup.group_key)?;

    for (index, (_, relpath)) in outputs.into_iter().enumerate() {
        if index % 128 == 0 {
            refresh_thumbnail_cleanup_lease(conn, config, cleanup.id)?;
        }
        if relpath.trim().is_empty() {
            continue;
        }

        let relative = validate_relative_path(&relpath)
            .with_context(|| format!("invalid thumbnail relative path in DB: {relpath}"))?;
        let absolute = config.thumbs_root_real.join(relative);
        let normalized = match normalize_existing_output_target(config, &absolute) {
            Ok(path) => path,
            Err(error) => {
                if !absolute.exists() {
                    continue;
                }
                return Err(error);
            }
        };

        if normalized != config.thumbs_root_real
            && !normalized.starts_with(&config.thumbs_root_real)
        {
            bail!(
                "thumbnail output path escapes thumbs root: {}",
                normalized.display()
            );
        }

        match fs::remove_file(&normalized) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(error).with_context(|| {
                    format!("failed to remove thumbnail file: {}", normalized.display())
                })
            }
        }
    }

    let removed = delete_group_thumbnail_rows(conn, &cleanup.group_key)?;
    Ok(removed)
}

pub fn classify_thumbnail_error(error: &anyhow::Error) -> &'static str {
    let message = error.to_string().to_lowercase();
    if message.contains("ffmpeg") {
        return "THUMB_VIDEO_FFMPEG_FAILED";
    }
    if message.contains("path") || message.contains("escape") {
        return "THUMB_PATH_POLICY_REJECTED";
    }
    if message.contains("format") || message.contains("decode") {
        return "THUMB_DECODE_FAILED";
    }
    "THUMB_GENERATION_FAILED"
}

fn resolve_source_path(config: &WorkerConfig, task: &ThumbnailTaskRecord) -> Result<PathBuf> {
    let root =
        resolve_root_under_libraries(&config.libraries_root_real, &PathBuf::from(&task.root_path))?;
    let relative = validate_relative_path(&task.relative_path)?;
    let candidate = root.join(relative);

    if candidate.exists() {
        let real_candidate = candidate.canonicalize().with_context(|| {
            format!(
                "failed to resolve source candidate path: {}",
                candidate.display()
            )
        })?;
        if !real_candidate.starts_with(&root) {
            bail!("source candidate path escapes library root");
        }
        return Ok(real_candidate);
    }

    bail!("source media file does not exist: {}", candidate.display())
}

fn resolve_output_path(config: &WorkerConfig, task: &ThumbnailTaskRecord) -> Result<PathBuf> {
    let relative = validate_relative_path(&task.output_relpath).with_context(|| {
        format!(
            "invalid thumbnail output relative path for thumb_key {}",
            task.thumb_key
        )
    })?;

    let candidate = config.thumbs_root_real.join(relative);
    if candidate != config.thumbs_root_real && !candidate.starts_with(&config.thumbs_root_real) {
        bail!("thumbnail output path escapes thumbs root");
    }

    Ok(candidate)
}

fn generate_image_thumbnail(
    source_path: &PathBuf,
    output_path: &PathBuf,
    max_dimension: usize,
    output_format: &str,
    lease_refresher: &mut LeaseRefresher<'_>,
) -> Result<(u32, u32)> {
    lease_refresher.maybe_refresh()?;
    let image = ImageReader::open(source_path)
        .with_context(|| format!("failed to open source image: {}", source_path.display()))?
        .with_guessed_format()
        .context("failed to guess source image format")?
        .decode()
        .context("failed to decode source image")?;

    let thumb = image.thumbnail(max_dimension as u32, max_dimension as u32);
    let (width, height) = (thumb.width(), thumb.height());

    lease_refresher.maybe_refresh()?;
    let format = parse_output_format(output_format)?;
    thumb
        .save_with_format(output_path, format)
        .with_context(|| format!("failed to write image thumbnail: {}", output_path.display()))?;

    Ok((width, height))
}

fn generate_video_thumbnail(
    config: &WorkerConfig,
    source_path: &PathBuf,
    output_path: &PathBuf,
    max_dimension: usize,
    output_format: &str,
    lease_refresher: &mut LeaseRefresher<'_>,
) -> Result<(u32, u32)> {
    let frame_path = output_path.with_file_name(format!(
        "{}-frame.jpg",
        output_path
            .file_stem()
            .and_then(|value| value.to_str())
            .unwrap_or("frame")
    ));
    let _frame_guard = TempFileGuard::new(frame_path.clone());

    let mut ffmpeg_child = Command::new(&config.thumbnail_ffmpeg_bin)
        .arg("-v")
        .arg("error")
        .arg("-y")
        .arg("-ss")
        .arg("00:00:01")
        .arg("-i")
        .arg(source_path)
        .arg("-frames:v")
        .arg("1")
        .arg(&frame_path)
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .with_context(|| {
            format!(
                "failed to execute ffmpeg binary '{}'",
                config.thumbnail_ffmpeg_bin
            )
        })?;

    let ffmpeg_timeout = Duration::from_secs(config.thumbnail_ffmpeg_timeout_seconds);
    let ffmpeg_started_at = Instant::now();
    loop {
        lease_refresher.maybe_refresh()?;
        if let Some(status) = ffmpeg_child
            .try_wait()
            .context("failed waiting for ffmpeg process")?
        {
            if !status.success() {
                let stderr = read_child_stderr(&mut ffmpeg_child);
                bail!(
                    "ffmpeg frame extraction failed: {}",
                    truncate_error_message(&stderr, 2048)
                );
            }
            break;
        }
        if ffmpeg_started_at.elapsed() >= ffmpeg_timeout {
            let _ = ffmpeg_child.kill();
            let _ = ffmpeg_child.wait();
            bail!(
                "ffmpeg frame extraction timed out after {} seconds",
                config.thumbnail_ffmpeg_timeout_seconds
            );
        }
        thread::sleep(Duration::from_millis(200));
    }

    lease_refresher.maybe_refresh()?;
    let image = ImageReader::open(&frame_path)
        .with_context(|| format!("failed to open extracted frame: {}", frame_path.display()))?
        .with_guessed_format()
        .context("failed to detect frame format")?
        .decode()
        .context("failed to decode extracted frame")?;

    let thumb = image.thumbnail(max_dimension as u32, max_dimension as u32);
    let (width, height) = (thumb.width(), thumb.height());

    lease_refresher.maybe_refresh()?;
    let format = parse_output_format(output_format)?;
    thumb
        .save_with_format(output_path, format)
        .with_context(|| format!("failed to write video thumbnail: {}", output_path.display()))?;

    Ok((width, height))
}

fn parse_output_format(raw_format: &str) -> Result<ImageFormat> {
    match raw_format {
        "jpeg" => Ok(ImageFormat::Jpeg),
        "webp" => Ok(ImageFormat::WebP),
        _ => bail!("unsupported thumbnail output format: {raw_format}"),
    }
}

fn normalize_output_target(config: &WorkerConfig, path: &PathBuf) -> Result<PathBuf> {
    let parent = path
        .parent()
        .ok_or_else(|| anyhow::anyhow!("thumbnail output path has no parent directory"))?;
    fs::create_dir_all(parent).with_context(|| {
        format!(
            "failed to create thumbnail output directory: {}",
            parent.display()
        )
    })?;
    let parent_real = parent.canonicalize().with_context(|| {
        format!(
            "failed to resolve thumbnail output directory: {}",
            parent.display()
        )
    })?;
    if !parent_real.starts_with(&config.thumbs_root_real) {
        bail!(
            "thumbnail output directory escapes thumbs root: {}",
            parent_real.display()
        );
    }
    let filename = path
        .file_name()
        .ok_or_else(|| anyhow::anyhow!("thumbnail output path is missing filename"))?;
    Ok(parent_real.join(filename))
}

fn normalize_existing_output_target(config: &WorkerConfig, path: &PathBuf) -> Result<PathBuf> {
    let parent = path
        .parent()
        .ok_or_else(|| anyhow::anyhow!("thumbnail output path has no parent directory"))?;
    let parent_real = parent.canonicalize().with_context(|| {
        format!(
            "failed to resolve thumbnail output directory: {}",
            parent.display()
        )
    })?;
    if !parent_real.starts_with(&config.thumbs_root_real) {
        bail!(
            "thumbnail output directory escapes thumbs root: {}",
            parent_real.display()
        );
    }
    let filename = path
        .file_name()
        .ok_or_else(|| anyhow::anyhow!("thumbnail output path is missing filename"))?;
    Ok(parent_real.join(filename))
}

fn read_child_stderr(child: &mut std::process::Child) -> String {
    let mut stderr = String::new();
    if let Some(mut pipe) = child.stderr.take() {
        let _ = pipe.read_to_string(&mut stderr);
    }
    stderr
}

fn truncate_error_message(raw: &str, max_chars: usize) -> String {
    if raw.chars().count() <= max_chars {
        return raw.to_string();
    }
    raw.chars().take(max_chars).collect::<String>() + "...(truncated)"
}

struct LeaseRefresher<'a> {
    conn: &'a Connection,
    config: &'a WorkerConfig,
    task_id: i64,
    interval: Duration,
    last_refresh_at: Instant,
}

impl<'a> LeaseRefresher<'a> {
    fn new(conn: &'a Connection, config: &'a WorkerConfig, task_id: i64) -> Self {
        let interval_seconds = (config.job_lock_ttl_seconds / 3).max(1);
        Self {
            conn,
            config,
            task_id,
            interval: Duration::from_secs(interval_seconds),
            last_refresh_at: Instant::now(),
        }
    }

    fn maybe_refresh(&mut self) -> Result<()> {
        if self.last_refresh_at.elapsed() >= self.interval {
            refresh_thumbnail_lease(self.conn, self.config, self.task_id)?;
            self.last_refresh_at = Instant::now();
        }
        Ok(())
    }
}

fn reserve_thumbnail_io_budget(conn: &Connection, config: &WorkerConfig, bytes: u64) -> Result<()> {
    let delay = reserve_global_io_budget(
        conn,
        "thumbnail_io_global",
        bytes,
        config.thumbnail_io_rate_limit_mib_per_sec,
    )?;
    if !delay.is_zero() {
        thread::sleep(delay);
    }
    Ok(())
}

struct TempFileGuard {
    path: PathBuf,
}

impl TempFileGuard {
    fn new(path: PathBuf) -> Self {
        Self { path }
    }
}

impl Drop for TempFileGuard {
    fn drop(&mut self) {
        match fs::remove_file(&self.path) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(_) => {}
        }
    }
}

#[cfg(unix)]
fn metadata_mtime_ns(metadata: &fs::Metadata) -> Result<i64> {
    use std::os::unix::fs::MetadataExt;

    Ok(metadata
        .mtime()
        .saturating_mul(1_000_000_000)
        .saturating_add(i64::from(metadata.mtime_nsec())))
}

#[cfg(not(unix))]
fn metadata_mtime_ns(metadata: &fs::Metadata) -> Result<i64> {
    let modified = metadata
        .modified()
        .context("failed to read source modified timestamp")?;
    let duration = modified
        .duration_since(std::time::UNIX_EPOCH)
        .context("source modified timestamp before UNIX_EPOCH")?;
    i64::try_from(duration.as_nanos()).context("source mtime_ns over i64 range")
}
