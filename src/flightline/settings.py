"""Runtime configuration, read from FLIGHTLINE_* environment variables."""

from __future__ import annotations

import socket
from functools import lru_cache
from pathlib import Path
from typing import Literal

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="FLIGHTLINE_", env_file=".env", extra="ignore")

    # Shared
    redis_url: str = "redis://localhost:6379/0"
    catalog_path: Path = Path("config/catalog.yaml")
    rules_path: Path = Path("config/rules.yaml")
    shards: int = Field(default=4, ge=1, le=256)
    log_level: str = "INFO"
    metrics_port: int = 9100
    consumer_name: str = Field(default_factory=socket.gethostname)

    # Ingest API
    api_keys: list[SecretStr] = Field(default_factory=list)
    max_frames_per_request: int = 5000
    max_clock_skew_s: float = 30.0
    # Shed load (HTTP 503) rather than let an unbounded backlog build in Redis.
    backpressure_max_lag: int = 200_000

    # Writer
    sink: Literal["local", "s3"] = "local"
    local_sink_path: Path = Path("data/lake")
    s3_bucket: str = "flightline-telemetry"
    s3_prefix: str = "telemetry"
    s3_endpoint_url: str | None = None
    flush_max_rows: int = 250_000
    flush_interval_s: float = 5.0
    reclaim_idle_ms: int = 60_000

    # Alerter
    alerter_replicas: int = Field(default=1, ge=1)
    alerter_ordinal: int | None = None
    pagerduty_url: str = "https://events.pagerduty.com/v2/enqueue"
    pagerduty_routing_key: SecretStr | None = None
    slack_webhook_url: SecretStr | None = None
    # Rules reference runbooks by repo path; pages need an absolute link.
    runbook_base_url: str = ""
    notify_timeout_s: float = 3.0
    notify_max_attempts: int = 4


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
