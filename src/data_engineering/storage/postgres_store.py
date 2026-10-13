"""PostgreSQL backend (psycopg 3). Install with `uv sync --extra postgres`.

Same SQL as SQLite (placeholders become %s); native TIMESTAMPTZ, DOUBLE PRECISION and
JSONB columns. The connection runs in autocommit mode with explicit BEGIN/COMMIT, so
read-only helper queries never leave an idle open transaction behind.
Connection URLs are never logged with their password (see ConsumerSettings.redacted_postgres_url).
"""

from datetime import datetime, timezone

from data_engineering.storage.base import SCHEMA_DIR, SqlTelemetryStore

# SQLSTATE classes that are worth retrying: connection exceptions, transaction rollback
# (serialization failure / deadlock), insufficient resources, operator intervention.
_TRANSIENT_SQLSTATE_PREFIXES = ("08", "40", "53", "57P")


class PostgresTelemetryStore(SqlTelemetryStore):
    backend = "postgres"
    placeholder = "%s"
    schema_file = SCHEMA_DIR / "telemetry_store_postgres.sql"

    def __init__(self, url: str, search_path: str | None = None, connect_timeout_s: int = 10,
                 read_only: bool = False, query_timeout_s: float | None = None) -> None:
        try:
            import psycopg
        except ImportError as exc:
            raise RuntimeError("PostgreSQL support needs psycopg: `uv sync --extra postgres`") from exc
        self._psycopg = psycopg
        self.url = url
        self.search_path = search_path
        self.connect_timeout_s = connect_timeout_s
        self.read_only, self.query_timeout_s = read_only, query_timeout_s
        super().__init__()

    def describe(self) -> str:
        from data_engineering.streaming.settings import redact_url
        return f"postgres:{redact_url(self.url)}" + (f" (search_path={self.search_path})" if self.search_path else "")

    def _connect(self):
        kwargs = {"autocommit": True, "connect_timeout": self.connect_timeout_s}
        options = []
        if self.search_path:
            options.append(f"-c search_path={self.search_path}")
        if self.query_timeout_s:
            options.append(f"-c statement_timeout={int(self.query_timeout_s * 1000)}")
        if self.read_only:
            options.append("-c default_transaction_read_only=on")
        if options:
            kwargs["options"] = " ".join(options)
        return self._psycopg.connect(self.url, **kwargs)

    def is_transient(self, exc: BaseException) -> bool:
        pg = self._psycopg
        if isinstance(exc, (pg.OperationalError, pg.InterfaceError)):
            return True
        sqlstate = getattr(exc, "sqlstate", None) or ""
        return sqlstate.startswith(_TRANSIENT_SQLSTATE_PREFIXES)

    def _begin(self) -> None:
        self.conn.execute("BEGIN")

    def _commit(self) -> None:
        self.conn.execute("COMMIT")

    def _rollback(self) -> None:
        try:
            self.conn.execute("ROLLBACK")
        except self._psycopg.Error:
            pass  # connection already broken; reconnect() will replace it

    def _apply_schema(self, ddl: str) -> None:
        self.conn.execute(ddl)   # no parameters, so several statements are allowed

    def _greatest(self, a: str, b: str) -> str:
        return f"GREATEST({a}, {b})"

    def _least(self, a: str, b: str) -> str:
        return f"LEAST({a}, {b})"

    def _ts_in(self, value: datetime) -> datetime:
        if value.tzinfo is None:
            raise ValueError("naive datetime passed to the store")
        return value.astimezone(timezone.utc)

    def _ts_out(self, value) -> datetime:
        return value.astimezone(timezone.utc) if isinstance(value, datetime) else value

    def _json_in(self, value):
        from psycopg.types.json import Jsonb
        return Jsonb(value)

    def _json_out(self, value):
        import json
        return json.loads(value) if isinstance(value, str) else value

    def _table_exists(self, table: str) -> bool:
        row = self.conn.execute("SELECT to_regclass(%s)", (table,)).fetchone()
        return row is not None and row[0] is not None

    def _columns(self, table: str) -> list[str]:
        return [r[0] for r in self.conn.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = current_schema() AND table_name = %s ORDER BY ordinal_position", (table,))]
