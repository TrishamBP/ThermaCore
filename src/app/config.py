"""Application-wide settings (environment variables with the API_ prefix, or .env).

Database settings are NOT duplicated here: the API reads the same TELEMETRY_* variables as the
Kafka consumer (data_engineering.streaming.settings.ConsumerSettings), so both always point at
the same store. Cooling thresholds stay in config/cooling_state.yaml (COOLING_STATE_CONFIG).
"""

from typing import Literal

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from data_engineering.paths import REPO_ROOT


class AppSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="API_", env_file=REPO_ROOT / ".env", extra="ignore")

    environment: Literal["development", "test", "production"] = "development"
    docs_enabled: bool = True                      # /docs, /redoc, /openapi.json
    db_connect_timeout_s: int = Field(3, ge=1, le=60)   # PostgreSQL; fail fast instead of hanging requests
    threadpool_tokens: int = Field(40, ge=1, le=1000)   # concurrent sync handlers (anyio default is 40)
    cors_origins: list[str] = Field(default_factory=list)   # empty = CORS disabled (no browser frontend yet)
