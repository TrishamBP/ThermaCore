"""Application-wide constants."""

API_TITLE = "Data Center Operations Intelligence API"
API_VERSION = "0.2.0"
API_V1_PREFIX = "/api/v1"
# Pre-v1 paths (/api/health, /api/telemetry/cooling-state) remain as deprecated aliases.
LEGACY_API_PREFIX = "/api"
REQUEST_ID_HEADER = "X-Request-ID"
REQUEST_ID_PATTERN = r"^[A-Za-z0-9._-]{1,64}$"
