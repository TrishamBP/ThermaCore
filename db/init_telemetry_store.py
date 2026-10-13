"""Create the consumer's telemetry store schema (idempotent). Run once before the consumer.

    uv run python db/init_telemetry_store.py                      # SQLite (TELEMETRY_SQLITE_PATH)
    TELEMETRY_DB_BACKEND=postgres uv run python db/init_telemetry_store.py

Schema files: db/telemetry_store_sqlite.sql, db/telemetry_store_postgres.sql.
Kept separate from the consumer on purpose: the consumer refuses to start on an
uninitialized database instead of silently creating tables.
"""

import argparse
import sys

from data_engineering.storage.base import open_store
from data_engineering.streaming.settings import load_consumer_settings


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--db-backend", choices=["sqlite", "postgres"])
    ap.add_argument("--sqlite-path")
    args = ap.parse_args(argv)
    try:
        settings = load_consumer_settings(db_backend=args.db_backend, sqlite_path=args.sqlite_path)
        store = open_store(settings)
    except Exception as exc:
        print(f"error: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 2
    try:
        store.initialize()
        print(f"initialized {store.describe()} (schema version {store.schema_version()}): "
              + ", ".join(f"{t}={n}" for t, n in store.counts().items()))
    except Exception as exc:
        print(f"error: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 2
    finally:
        store.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
