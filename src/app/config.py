"""Application-wide settings (environment variables with the API_ prefix, or .env).

Database settings are NOT duplicated here: the API reads the same TELEMETRY_* variables as the
Kafka consumer (data_engineering.streaming.settings.ConsumerSettings), so both always point at
the same store. Cooling thresholds stay in config/cooling_state.yaml (COOLING_STATE_CONFIG).
"""

from typing import Literal

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict

from data_engineering.paths import REPO_ROOT


class AppSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="API_", env_file=REPO_ROOT / ".env", extra="ignore")

    environment: Literal["development", "test", "production"] = "development"
    docs_enabled: bool = True                      # /docs, /redoc, /openapi.json
    db_connect_timeout_s: int = Field(3, ge=1, le=60)   # PostgreSQL; fail fast instead of hanging requests
    threadpool_tokens: int = Field(40, ge=1, le=1000)   # concurrent sync handlers (anyio default is 40)
    cors_origins: list[str] = Field(default_factory=list)   # empty = CORS disabled (no browser frontend yet)
    model_training_enabled: bool = False           # POST /cooling-model/train (CPU-heavy; off by default)

    # ---- access control (no authentication existed before; both are opt-in) ----
    auth_token: SecretStr | None = None            # if set: every /api/v1 route except /health needs "Bearer <token>"
    allowed_assets: list[str] = Field(default_factory=list)   # operator asset scope; empty = all assets

    # ---- query bounds (dashboard, telemetry, copilot tools) ----
    query_timeout_s: float = Field(10.0, gt=0, le=120)          # per SQL statement
    max_telemetry_range_hours: int = Field(24, ge=1, le=168)    # widest time range one request may read
    max_telemetry_points: int = Field(5000, ge=10, le=50000)    # points (rows or buckets) per response
    dashboard_trend_minutes: int = Field(60, ge=5, le=1440)     # default trend window of the overview
    dashboard_trend_interval_minutes: int = Field(5, ge=1, le=60)

    # ---- cooling model ----
    model_max_age_days: int | None = Field(None, ge=1)          # older active artifacts -> predictions UNAVAILABLE

    # ---- digital twin live updates (SSE) ----
    twin_poll_interval_s: float = Field(10.0, ge=1, le=300)    # ONE shared poll for all subscribers
    twin_heartbeat_s: float = Field(15.0, ge=1, le=120)
    twin_max_subscribers: int = Field(50, ge=1, le=1000)

    # ---- copilot conversation memory (bounded, in-process) ----
    copilot_max_conversations: int = Field(200, ge=1, le=10000)
    copilot_max_turns: int = Field(10, ge=1, le=50)
    copilot_conversation_ttl_s: int = Field(3600, ge=60, le=86400)
