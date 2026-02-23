from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from pydantic import Field, PositiveInt, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

SUPPORTED_HASH_ALGORITHMS = {"blake3", "sha256"}


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="DEDUPFS_",
        env_file=".env",
        case_sensitive=False,
        extra="ignore",
    )

    app_name: str = "DedupFS"
    environment: str = "production"
    api_host: str = "0.0.0.0"
    api_port: int = 8080
    log_level: str = "INFO"

    dry_run: bool = True
    allow_real_delete: bool = False

    libraries_root: Path = Field(default=Path("/libraries"))
    state_root: Path = Field(default=Path("/state"))
    database_url: str | None = None

    job_lock_ttl_seconds: PositiveInt = 300
    job_lock_heartbeat_seconds: PositiveInt = 30

    scan_write_batch_size: PositiveInt = 2000
    hash_fetch_batch_size: PositiveInt = 512
    hash_read_chunk_bytes: PositiveInt = 4 * 1024 * 1024
    hash_claim_ttl_seconds: PositiveInt = 600
    hash_retry_base_seconds: PositiveInt = 30
    hash_retry_max_seconds: PositiveInt = 3600
    default_hash_algorithm: str = "blake3"

    thumbs_root: Path = Field(default=Path("/state/thumbs"))
    thumbnail_max_dimension: PositiveInt = 256
    thumbnail_default_format: str = "jpeg"
    thumbnail_image_concurrency: PositiveInt = 2
    thumbnail_video_concurrency: PositiveInt = 1
    thumbnail_queue_capacity: PositiveInt = 50000
    thumbnail_io_rate_limit_mib_per_sec: PositiveInt | None = None
    thumbnail_retry_base_seconds: PositiveInt = 30
    thumbnail_retry_max_seconds: PositiveInt = 1800
    thumbnail_cleanup_delay_seconds: PositiveInt = 600
    thumbnail_ffmpeg_bin: str = "ffmpeg"
    thumbnail_ffmpeg_timeout_seconds: PositiveInt = 120

    default_page_size: PositiveInt = 100
    max_page_size: PositiveInt = 1000

    rust_worker_concurrency: PositiveInt = 4
    rust_worker_poll_seconds: PositiveInt = 5
    rust_worker_io_rate_limit_mib_per_sec: PositiveInt | None = None
    wal_checkpoint_default_mode: str = "passive"
    wal_checkpoint_min_interval_seconds: PositiveInt = 900
    wal_checkpoint_retry_seconds: PositiveInt = 120
    wal_checkpoint_allow_truncate: bool = False

    @field_validator("libraries_root", "state_root", "thumbs_root", mode="before")
    @classmethod
    def _normalize_path(cls, value: str | Path) -> Path:
        raw = str(value)
        if "~" in raw:
            raise ValueError("Home expansion syntax is not allowed in paths")
        if "$" in raw:
            raise ValueError("Environment variable syntax is not allowed in paths")
        path = Path(raw)
        if not path.is_absolute():
            raise ValueError("Path settings must be absolute")
        return path

    @model_validator(mode="after")
    def _validate_runtime_constraints(self) -> "Settings":
        self.libraries_root = self.libraries_root.resolve(strict=False)
        self.state_root = self.state_root.resolve(strict=False)
        self.thumbs_root = self.thumbs_root.resolve(strict=False)

        if self.libraries_root.as_posix() != "/libraries":
            raise ValueError("libraries_root must resolve to /libraries")

        self.state_root.mkdir(parents=True, exist_ok=True)
        if self.thumbs_root.as_posix() == "/state/thumbs" and self.state_root.as_posix() != "/state":
            self.thumbs_root = (self.state_root / "thumbs").resolve(strict=False)
        self.thumbs_root.mkdir(parents=True, exist_ok=True)
        if self.state_root != self.thumbs_root and self.state_root not in self.thumbs_root.parents:
            raise ValueError("thumbs_root must be under state_root")

        if self.allow_real_delete and self.dry_run:
            raise ValueError("allow_real_delete cannot be true while dry_run is enabled")

        if self.max_page_size < self.default_page_size:
            raise ValueError("max_page_size must be greater than or equal to default_page_size")

        normalized_algorithm = self.default_hash_algorithm.lower().strip()
        if normalized_algorithm not in SUPPORTED_HASH_ALGORITHMS:
            raise ValueError(f"default_hash_algorithm must be one of {sorted(SUPPORTED_HASH_ALGORITHMS)}")
        self.default_hash_algorithm = normalized_algorithm

        if self.hash_retry_max_seconds < self.hash_retry_base_seconds:
            raise ValueError("hash_retry_max_seconds must be greater than or equal to hash_retry_base_seconds")

        normalized_thumb_format = self.thumbnail_default_format.lower().strip()
        if normalized_thumb_format not in {"jpeg", "webp"}:
            raise ValueError("thumbnail_default_format must be one of ['jpeg', 'webp']")
        self.thumbnail_default_format = normalized_thumb_format

        if self.thumbnail_retry_max_seconds < self.thumbnail_retry_base_seconds:
            raise ValueError("thumbnail_retry_max_seconds must be >= thumbnail_retry_base_seconds")

        normalized_wal_mode = self.wal_checkpoint_default_mode.lower().strip()
        if normalized_wal_mode not in {"passive", "restart", "truncate"}:
            raise ValueError("wal_checkpoint_default_mode must be one of ['passive', 'restart', 'truncate']")
        self.wal_checkpoint_default_mode = normalized_wal_mode

        return self

    @property
    def effective_database_url(self) -> str:
        if self.database_url:
            return self.database_url
        db_path = self.state_root / "dedupfs.sqlite3"
        return f"sqlite:///{db_path.as_posix()}"


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
