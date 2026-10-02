"""Typed settings, loaded from environment variables prefixed with ``MODSTREAM_``."""

from __future__ import annotations

import secrets
from typing import Literal

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="MODSTREAM_", env_file=".env", extra="ignore")

    env: Literal["dev", "test", "prod"] = "dev"
    secret_key: str = Field(default_factory=lambda: secrets.token_hex(32))
    log_json: bool = False

    # Infrastructure. Leave redis_url unset to run everything in one process.
    database_url: str = "sqlite+aiosqlite:///instance/modstream.db"
    create_schema: bool = True  # turn off when schema changes are applied by a deploy step
    redis_url: str | None = None
    upload_dir: str = "instance/uploads"
    max_upload_bytes: int = 5 * 1024 * 1024

    # Detection
    scorer: Literal["auto", "detoxify", "none"] = "auto"  # auto = use the model if installed, else rules only
    model_variant: Literal["unbiased", "original", "multilingual"] = "unbiased"
    flag_threshold: float = 0.7
    review_threshold: float = 0.4
    warmup: bool = True

    # Micro-batching for the synchronous path
    batch_max_size: int = 32
    batch_max_wait_ms: float = 5.0
    batch_max_queue: int = 10_000

    # Stream workers (asynchronous path)
    run_worker: bool = True  # embed a worker in the API process (dev); False when workers run separately
    worker_concurrency: int = 1
    worker_batch_size: int = 64
    worker_block_ms: int = 500
    worker_reclaim_idle_ms: int = 30_000
    worker_max_deliveries: int = 5
    messages_maxlen: int = 1_000_000
    events_maxlen: int = 10_000

    # Live fan-out to SSE / WebSocket subscribers
    subscriber_queue_size: int = 1_000
    stream_heartbeat_s: float = 15.0
    stream_max_seconds: float = 3_600.0  # recycle long-lived connections so load rebalances after scale-out
    stream_backlog: int = 50

    enable_simulator: bool = True
    simulator_max_messages: int = 50_000

    @model_validator(mode="after")
    def _require_shared_secret(self):
        # A per-process random key breaks sessions as soon as there is more than one process.
        if self.env == "prod" and "secret_key" not in self.model_fields_set:
            raise ValueError("MODSTREAM_SECRET_KEY must be set when MODSTREAM_ENV=prod")
        return self
