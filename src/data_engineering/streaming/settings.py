"""Kafka producer and consumer settings from environment variables (optionally loaded from .env).

Every setting has a local-development default, so nothing is required to talk to the
Docker Compose broker on localhost:9092. CLI flags override environment values.
"""

import os
import re
from dataclasses import dataclass, fields, replace
from pathlib import Path
from typing import Mapping

from dotenv import load_dotenv

from data_engineering.paths import REPO_ROOT

DEFAULT_TOPIC = "datacenter.telemetry"
COMPRESSION_TYPES = ("none", "gzip", "snappy", "lz4", "zstd")
_TOPIC_RE = re.compile(r"^[A-Za-z0-9._-]{1,249}$")
_HOST_PORT_RE = re.compile(r"^(?P<host>[A-Za-z0-9.\-]+|\[[0-9A-Fa-f:]+\]):(?P<port>\d{1,5})$")


class SettingsError(ValueError):
    pass


@dataclass(frozen=True)
class ProducerSettings:
    bootstrap_servers: str = "127.0.0.1:9092"
    topic: str = DEFAULT_TOPIC
    client_id: str = "dco-telemetry-producer"
    acks: str = "all"
    enable_idempotence: bool = True
    delivery_timeout_ms: int = 120_000
    request_timeout_ms: int = 30_000
    retry_backoff_ms: int = 100
    linger_ms: int = 20
    batch_size_bytes: int = 65_536
    compression_type: str = "lz4"
    queue_max_messages: int = 100_000
    flush_timeout_s: float = 30.0
    metadata_timeout_s: float = 10.0
    rate_events_per_s: float = 0.0       # 0 = no rate limit
    speedup: float = 0.0                 # simulated seconds per wall-clock second; 0 = off
    topic_partitions: int = 3
    topic_replication_factor: int = 1
    topic_retention_ms: int = 604_800_000  # 7 days

    def validate(self) -> "ProducerSettings":
        problems = []
        servers = [s.strip() for s in self.bootstrap_servers.split(",") if s.strip()]
        if not servers:
            problems.append("KAFKA_BOOTSTRAP_SERVERS is empty")
        for server in servers:
            match = _HOST_PORT_RE.match(server)
            if not match or not 1 <= int(match["port"]) <= 65535:
                problems.append(f"bootstrap server {server!r} is not host:port")
        if not _TOPIC_RE.match(self.topic) or self.topic in {".", ".."}:
            problems.append(f"invalid topic name {self.topic!r}")
        if self.acks not in {"all", "-1", "0", "1"}:
            problems.append(f"KAFKA_ACKS must be all, -1, 0 or 1 (got {self.acks!r})")
        if self.enable_idempotence and self.acks not in {"all", "-1"}:
            problems.append("idempotence requires KAFKA_ACKS=all")
        if self.compression_type not in COMPRESSION_TYPES:
            problems.append(f"KAFKA_COMPRESSION_TYPE must be one of {COMPRESSION_TYPES}")
        for name in ("delivery_timeout_ms", "request_timeout_ms", "batch_size_bytes", "queue_max_messages",
                     "topic_partitions", "topic_replication_factor", "topic_retention_ms"):
            if getattr(self, name) <= 0:
                problems.append(f"{name} must be > 0")
        for name in ("retry_backoff_ms", "linger_ms", "rate_events_per_s", "speedup"):
            if getattr(self, name) < 0:
                problems.append(f"{name} must be >= 0")
        if self.flush_timeout_s <= 0 or self.metadata_timeout_s <= 0:
            problems.append("flush and metadata timeouts must be > 0")
        if self.delivery_timeout_ms < self.request_timeout_ms + self.linger_ms:
            problems.append("KAFKA_DELIVERY_TIMEOUT_MS must be >= KAFKA_REQUEST_TIMEOUT_MS + KAFKA_LINGER_MS")
        if self.rate_events_per_s > 0 and self.speedup > 0:
            problems.append("set either a publishing rate or a speedup, not both")
        if problems:
            raise SettingsError("; ".join(problems))
        return self

    def producer_config(self) -> dict:
        """librdkafka configuration for confluent_kafka.Producer."""
        return {
            "bootstrap.servers": self.bootstrap_servers,
            "client.id": self.client_id,
            "acks": self.acks,
            "enable.idempotence": self.enable_idempotence,
            "delivery.timeout.ms": self.delivery_timeout_ms,
            "request.timeout.ms": self.request_timeout_ms,
            "retry.backoff.ms": self.retry_backoff_ms,
            "linger.ms": self.linger_ms,
            "batch.size": self.batch_size_bytes,
            "compression.type": self.compression_type,
            "queue.buffering.max.messages": self.queue_max_messages,
        }


ENV_VARS = {
    "bootstrap_servers": "KAFKA_BOOTSTRAP_SERVERS",
    "topic": "KAFKA_TOPIC",
    "client_id": "KAFKA_CLIENT_ID",
    "acks": "KAFKA_ACKS",
    "enable_idempotence": "KAFKA_ENABLE_IDEMPOTENCE",
    "delivery_timeout_ms": "KAFKA_DELIVERY_TIMEOUT_MS",
    "request_timeout_ms": "KAFKA_REQUEST_TIMEOUT_MS",
    "retry_backoff_ms": "KAFKA_RETRY_BACKOFF_MS",
    "linger_ms": "KAFKA_LINGER_MS",
    "batch_size_bytes": "KAFKA_BATCH_SIZE_BYTES",
    "compression_type": "KAFKA_COMPRESSION_TYPE",
    "queue_max_messages": "KAFKA_QUEUE_MAX_MESSAGES",
    "flush_timeout_s": "KAFKA_FLUSH_TIMEOUT_S",
    "metadata_timeout_s": "KAFKA_METADATA_TIMEOUT_S",
    "rate_events_per_s": "PRODUCER_RATE_EVENTS_PER_S",
    "speedup": "PRODUCER_SPEEDUP",
    "topic_partitions": "KAFKA_TOPIC_PARTITIONS",
    "topic_replication_factor": "KAFKA_TOPIC_REPLICATION_FACTOR",
    "topic_retention_ms": "KAFKA_TOPIC_RETENTION_MS",
}


def _parse(env_var: str, raw: str, kind: type):
    raw = raw.strip()
    try:
        if kind is bool:
            lowered = raw.lower()
            if lowered in {"1", "true", "yes", "on"}:
                return True
            if lowered in {"0", "false", "no", "off"}:
                return False
            raise ValueError(raw)
        return kind(raw)
    except ValueError:
        raise SettingsError(f"{env_var}={raw!r} is not a valid {kind.__name__}") from None


def _load(cls, env_vars: dict[str, str], env: Mapping[str, str] | None, use_dotenv: bool, overrides: dict):
    """Defaults <- environment (os.environ + .env unless `env` is given) <- non-None overrides."""
    if env is None:
        if use_dotenv:
            load_dotenv(REPO_ROOT / ".env", override=False)
        env = os.environ
    defaults = cls()
    values = {}
    for f in fields(cls):
        raw = env.get(env_vars[f.name])
        if raw is not None and raw.strip() != "":
            values[f.name] = _parse(env_vars[f.name], raw, type(getattr(defaults, f.name)))
    settings = replace(defaults, **values)
    settings = replace(settings, **{k: v for k, v in overrides.items() if v is not None})
    return settings.validate()


def load_settings(env: Mapping[str, str] | None = None, use_dotenv: bool = True,
                  **overrides) -> ProducerSettings:
    """Producer settings from env (default: os.environ + .env), then non-None overrides."""
    return _load(ProducerSettings, ENV_VARS, env, use_dotenv, overrides)


DEFAULT_DLQ_TOPIC = "datacenter.telemetry.dlq"
DB_BACKENDS = ("sqlite", "postgres")
PARTITION_STRATEGIES = ("region_date", "date")
PARQUET_COMPRESSIONS = ("zstd", "snappy", "gzip", "none")
LOG_LEVELS = ("DEBUG", "INFO", "WARNING", "ERROR")


@dataclass(frozen=True)
class ConsumerSettings:
    # Kafka
    bootstrap_servers: str = "127.0.0.1:9092"
    topic: str = DEFAULT_TOPIC
    dlq_topic: str = DEFAULT_DLQ_TOPIC
    group_id: str = "dco-telemetry-sink"
    client_id: str = "dco-telemetry-consumer"
    auto_offset_reset: str = "earliest"     # where a NEW group starts; committed offsets always win
    session_timeout_ms: int = 45_000
    max_poll_interval_ms: int = 300_000
    metadata_timeout_s: float = 10.0
    # Batching, retries, shutdown
    batch_max_messages: int = 1_000
    batch_max_wait_s: float = 2.0
    max_retries: int = 5
    retry_backoff_initial_s: float = 0.5
    retry_backoff_max_s: float = 10.0
    dlq_flush_timeout_s: float = 10.0
    shutdown_timeout_s: float = 30.0
    # Database
    db_backend: str = "sqlite"
    sqlite_path: str = "data/telemetry_store.db"
    postgres_url: str = ""                  # postgresql://USER:PASSWORD@HOST:PORT/DBNAME
    # Parquet
    parquet_enabled: bool = True
    parquet_dir: str = "data/telemetry"
    parquet_partitioning: str = "region_date"
    parquet_flush_rows: int = 50_000
    parquet_flush_interval_s: float = 60.0
    parquet_max_rows_per_file: int = 250_000
    parquet_compression: str = "zstd"
    log_level: str = "INFO"

    def validate(self) -> "ConsumerSettings":
        problems = []
        servers = [s.strip() for s in self.bootstrap_servers.split(",") if s.strip()]
        if not servers or not all(_HOST_PORT_RE.match(s) for s in servers):
            problems.append(f"KAFKA_BOOTSTRAP_SERVERS {self.bootstrap_servers!r} is not a host:port list")
        for name in ("topic", "dlq_topic"):
            if not _TOPIC_RE.match(getattr(self, name)):
                problems.append(f"invalid topic name {getattr(self, name)!r}")
        if self.topic == self.dlq_topic:
            problems.append("KAFKA_DLQ_TOPIC must differ from KAFKA_TOPIC")
        if not self.group_id.strip():
            problems.append("KAFKA_GROUP_ID is empty")
        if self.auto_offset_reset not in ("earliest", "latest"):
            problems.append("KAFKA_AUTO_OFFSET_RESET must be earliest or latest")
        for name in ("batch_max_messages", "max_retries", "parquet_flush_rows", "parquet_max_rows_per_file",
                     "session_timeout_ms", "max_poll_interval_ms"):
            if getattr(self, name) <= 0:
                problems.append(f"{name} must be > 0")
        for name in ("batch_max_wait_s", "retry_backoff_initial_s", "retry_backoff_max_s", "dlq_flush_timeout_s",
                     "shutdown_timeout_s", "metadata_timeout_s", "parquet_flush_interval_s"):
            if getattr(self, name) <= 0:
                problems.append(f"{name} must be > 0")
        if self.retry_backoff_max_s < self.retry_backoff_initial_s:
            problems.append("CONSUMER_RETRY_BACKOFF_MAX_S must be >= CONSUMER_RETRY_BACKOFF_INITIAL_S")
        # All retries of one batch must finish before Kafka considers the consumer dead.
        if self.max_retries * self.retry_backoff_max_s + self.batch_max_wait_s >= self.max_poll_interval_ms / 1000:
            problems.append("CONSUMER_MAX_RETRIES x CONSUMER_RETRY_BACKOFF_MAX_S must stay below "
                            "KAFKA_MAX_POLL_INTERVAL_MS, or the group evicts the consumer mid-retry")
        if self.db_backend not in DB_BACKENDS:
            problems.append(f"TELEMETRY_DB_BACKEND must be one of {DB_BACKENDS}")
        if self.db_backend == "postgres" and not self.postgres_url.startswith(("postgresql://", "postgres://")):
            problems.append("TELEMETRY_POSTGRES_URL must be a postgresql:// URL when TELEMETRY_DB_BACKEND=postgres")
        if self.db_backend == "sqlite" and not self.sqlite_path.strip():
            problems.append("TELEMETRY_SQLITE_PATH is empty")
        if self.parquet_partitioning not in PARTITION_STRATEGIES:
            problems.append(f"PARQUET_PARTITION_STRATEGY must be one of {PARTITION_STRATEGIES}")
        if self.parquet_compression not in PARQUET_COMPRESSIONS:
            problems.append(f"PARQUET_COMPRESSION must be one of {PARQUET_COMPRESSIONS}")
        if self.log_level.upper() not in LOG_LEVELS:
            problems.append(f"LOG_LEVEL must be one of {LOG_LEVELS}")
        if problems:
            raise SettingsError("; ".join(problems))
        return self

    def resolve_path(self, value: str) -> Path:
        """Relative paths are relative to the repository root, not the current directory."""
        path = Path(value)
        return path if path.is_absolute() else REPO_ROOT / path

    def redacted_postgres_url(self) -> str:
        """The URL without its password, for logs."""
        return redact_url(self.postgres_url)

    def consumer_config(self) -> dict:
        """librdkafka configuration for confluent_kafka.Consumer."""
        return {
            "bootstrap.servers": self.bootstrap_servers,
            "group.id": self.group_id,
            "client.id": self.client_id,
            "enable.auto.commit": False,        # offsets are committed explicitly after persistence
            "enable.auto.offset.store": False,  # and nothing is stored implicitly either
            "auto.offset.reset": self.auto_offset_reset,
            "session.timeout.ms": self.session_timeout_ms,
            "max.poll.interval.ms": self.max_poll_interval_ms,
            "enable.partition.eof": False,
        }

    def dlq_producer_config(self) -> dict:
        timeout_ms = int(self.dlq_flush_timeout_s * 1000)
        return {
            "bootstrap.servers": self.bootstrap_servers,
            "client.id": f"{self.client_id}-dlq",
            "acks": "all",
            "enable.idempotence": True,
            "delivery.timeout.ms": timeout_ms,
            "request.timeout.ms": min(30_000, timeout_ms),
            "linger.ms": 0,
        }


def redact_url(url: str) -> str:
    return _PASSWORD_IN_URL_RE.sub(r"\1:***@", url)


_PASSWORD_IN_URL_RE = re.compile(r"(://[^:/@]+):[^@]*@")

CONSUMER_ENV_VARS = {
    "bootstrap_servers": "KAFKA_BOOTSTRAP_SERVERS",
    "topic": "KAFKA_TOPIC",
    "dlq_topic": "KAFKA_DLQ_TOPIC",
    "group_id": "KAFKA_GROUP_ID",
    "client_id": "KAFKA_CONSUMER_CLIENT_ID",
    "auto_offset_reset": "KAFKA_AUTO_OFFSET_RESET",
    "session_timeout_ms": "KAFKA_SESSION_TIMEOUT_MS",
    "max_poll_interval_ms": "KAFKA_MAX_POLL_INTERVAL_MS",
    "metadata_timeout_s": "KAFKA_METADATA_TIMEOUT_S",
    "batch_max_messages": "CONSUMER_BATCH_MAX_MESSAGES",
    "batch_max_wait_s": "CONSUMER_BATCH_MAX_WAIT_S",
    "max_retries": "CONSUMER_MAX_RETRIES",
    "retry_backoff_initial_s": "CONSUMER_RETRY_BACKOFF_INITIAL_S",
    "retry_backoff_max_s": "CONSUMER_RETRY_BACKOFF_MAX_S",
    "dlq_flush_timeout_s": "CONSUMER_DLQ_FLUSH_TIMEOUT_S",
    "shutdown_timeout_s": "CONSUMER_SHUTDOWN_TIMEOUT_S",
    "db_backend": "TELEMETRY_DB_BACKEND",
    "sqlite_path": "TELEMETRY_SQLITE_PATH",
    "postgres_url": "TELEMETRY_POSTGRES_URL",
    "parquet_enabled": "PARQUET_ENABLED",
    "parquet_dir": "PARQUET_OUTPUT_DIR",
    "parquet_partitioning": "PARQUET_PARTITION_STRATEGY",
    "parquet_flush_rows": "PARQUET_FLUSH_ROWS",
    "parquet_flush_interval_s": "PARQUET_FLUSH_INTERVAL_S",
    "parquet_max_rows_per_file": "PARQUET_MAX_ROWS_PER_FILE",
    "parquet_compression": "PARQUET_COMPRESSION",
    "log_level": "LOG_LEVEL",
}


def load_consumer_settings(env: Mapping[str, str] | None = None, use_dotenv: bool = True,
                           **overrides) -> ConsumerSettings:
    """Consumer settings from env (default: os.environ + .env), then non-None overrides."""
    return _load(ConsumerSettings, CONSUMER_ENV_VARS, env, use_dotenv, overrides)
