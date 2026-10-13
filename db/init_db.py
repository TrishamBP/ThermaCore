"""Create the SQLite database and all Phase 1 tables (idempotent).

    uv run python db/init_db.py
"""

import sys

from data_engineering.db import connect, init_db
from data_engineering.paths import db_path


def main() -> int:
    conn = connect()
    try:
        init_db(conn)
        tables = [r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type IN ('table', 'view') ORDER BY name")]
    finally:
        conn.close()
    print(f"initialized {db_path()}: {', '.join(tables)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
