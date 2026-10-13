"""SQLite backend (default for local development).

- WAL journal: readers (e.g. ad-hoc queries) don't block the consumer's writes.
- synchronous=FULL: a committed batch survives power loss. Offsets are committed to Kafka
  right after the database commit, so a commit that could be rolled back by a crash
  (synchronous=NORMAL in WAL mode) could lose data that Kafka will not redeliver.
- Transactions are explicit (BEGIN IMMEDIATE takes the write lock up front, so a batch
  never fails halfway on a lock).
"""

import json
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path

from data_engineering.storage.base import SCHEMA_DIR, SqlTelemetryStore

_TS_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"
_TRANSIENT_MESSAGES = ("database is locked", "database table is locked", "disk i/o error",
                       "unable to open database file", "database is busy")


class SqliteTelemetryStore(SqlTelemetryStore):
    backend = "sqlite"
    placeholder = "?"
    schema_file = SCHEMA_DIR / "telemetry_store_sqlite.sql"

    def __init__(self, path: Path | str, busy_timeout_ms: int = 5_000, check_same_thread: bool = True,
                 read_only: bool = False, query_timeout_s: float | None = None) -> None:
        # check_same_thread=False only for callers that hand one connection between threads strictly
        # sequentially (FastAPI: a sync dependency, the sync handler and the teardown run in different
        # worker threads). A connection must never be used by two threads at the same time.
        self.path = Path(path)
        self.busy_timeout_ms = busy_timeout_ms
        self.check_same_thread = check_same_thread
        self.read_only, self.query_timeout_s = read_only, query_timeout_s
        super().__init__()

    def describe(self) -> str:
        return f"sqlite:{self.path}"

    def _connect(self):
        if self.read_only:
            # mode=ro: the connection cannot write; query_only additionally rejects writes at statement level.
            conn = sqlite3.connect(f"{self.path.resolve().as_uri()}?mode=ro", uri=True, isolation_level=None,
                                   timeout=self.busy_timeout_ms / 1000, check_same_thread=self.check_same_thread)
            conn.execute("PRAGMA query_only = ON")
            conn.execute(f"PRAGMA busy_timeout = {int(self.busy_timeout_ms)}")
            self._install_timeout(conn)
            return conn
        self.path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(self.path, isolation_level=None, timeout=self.busy_timeout_ms / 1000,
                               check_same_thread=self.check_same_thread)
        self._install_timeout(conn)
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("PRAGMA synchronous = FULL")
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute(f"PRAGMA busy_timeout = {int(self.busy_timeout_ms)}")
        return conn

    def _install_timeout(self, conn) -> None:
        if self.query_timeout_s:
            # Aborts a running statement ("interrupted") once the per-statement deadline set by
            # _fetchall/_execute has passed.
            conn.set_progress_handler(
                lambda: 1 if self._deadline is not None and time.monotonic() > self._deadline else 0, 10_000)

    def is_transient(self, exc: BaseException) -> bool:
        return isinstance(exc, sqlite3.OperationalError) and any(
            m in str(exc).lower() for m in _TRANSIENT_MESSAGES)

    def _begin(self) -> None:
        self.conn.execute("BEGIN IMMEDIATE")

    def _commit(self) -> None:
        self.conn.execute("COMMIT")

    def _rollback(self) -> None:
        if self.conn.in_transaction:
            self.conn.execute("ROLLBACK")

    def _apply_schema(self, ddl: str) -> None:
        self.conn.executescript(ddl)

    def _greatest(self, a: str, b: str) -> str:
        return f"max({a}, {b})"

    def _least(self, a: str, b: str) -> str:
        return f"min({a}, {b})"

    def _ts_in(self, value: datetime) -> str:
        if value.tzinfo is None:
            raise ValueError("naive datetime passed to the store")
        return value.astimezone(timezone.utc).strftime(_TS_FORMAT)

    def _ts_out(self, value) -> datetime:
        return datetime.fromisoformat(value).astimezone(timezone.utc) if isinstance(value, str) else value

    def _json_in(self, value) -> str:
        return json.dumps(value, separators=(",", ":"))

    def _json_out(self, value):
        return json.loads(value) if isinstance(value, str) else value

    def _table_exists(self, table: str) -> bool:
        return self.conn.execute("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
                                 (table,)).fetchone() is not None

    def _columns(self, table: str) -> list[str]:
        return [row[1] for row in self.conn.execute(f"PRAGMA table_info({table})")]
