"""Load the original CSV rows into SQLite `source_records`, idempotently.

    uv run python -m data_engineering.ingestion.load_source

Every source column is kept with the type declared in dataset_metadata.json.
Values are read with the csv module, which performs no null-token conversion:
the region code "NA" (North America) stays the string "NA". Only empty cells
become NULL.
"""

import csv
import json
import sqlite3
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from data_engineering.db import connect, init_db, table_columns
from data_engineering.ingestion.download_dataset import sha256_file
from data_engineering.logging_utils import get_logger
from data_engineering.paths import DATASET_SLUG, METADATA_JSON, RAW_DATASET_DIR, SOURCE_CSV

KEY_COLUMNS = ("source_file", "source_row_number", "provenance")
log = get_logger("load_source")


class SourceSchemaError(ValueError):
    pass


class SourceChangedError(RuntimeError):
    pass


@dataclass
class LoadResult:
    source_file: str
    rows_in_file: int
    inserted: int
    already_present: int


def read_metadata(dataset_dir: Path) -> dict:
    return json.loads((dataset_dir / METADATA_JSON).read_text(encoding="utf-8"))


def caster(dtype: str):
    if dtype == "object":
        return str
    if dtype.startswith("int"):
        return int
    if dtype.startswith("float"):
        return float
    raise SourceSchemaError(f"unsupported source dtype {dtype!r}")


def load_source(
    conn: sqlite3.Connection,
    dataset_dir: Path = RAW_DATASET_DIR,
    csv_name: str = SOURCE_CSV,
    slug: str = DATASET_SLUG,
) -> LoadResult:
    metadata = read_metadata(dataset_dir)
    features: dict[str, str] = metadata["features"]
    columns = list(features)

    db_columns = [c for c in table_columns(conn, "source_records") if c not in KEY_COLUMNS]
    if db_columns != columns:
        raise SourceSchemaError(
            f"source_records columns do not match {METADATA_JSON} features: "
            f"only in db={sorted(set(db_columns) - set(columns))}, "
            f"only in metadata={sorted(set(columns) - set(db_columns))}"
        )

    csv_path = dataset_dir / csv_name
    sha = sha256_file(csv_path)
    existing = conn.execute(
        "SELECT sha256 FROM source_files WHERE source_file = ?", (csv_name,)
    ).fetchone()
    if existing and existing["sha256"] != sha:
        raise SourceChangedError(
            f"{csv_name} on disk differs from the copy already loaded (sha256 changed). "
            "Re-profile the dataset and rebuild the database deliberately."
        )

    casts = [caster(features[c]) for c in columns]
    rows = []
    with csv_path.open(newline="", encoding="utf-8-sig") as fh:
        reader = csv.reader(fh)
        header = next(reader)
        if header != columns:
            raise SourceSchemaError(f"CSV header does not match {METADATA_JSON} feature order")
        for row_number, raw in enumerate(reader, start=1):
            if len(raw) != len(columns):
                raise SourceSchemaError(f"row {row_number}: expected {len(columns)} fields, got {len(raw)}")
            try:
                values = [None if v == "" else cast(v) for v, cast in zip(raw, casts)]
            except ValueError as exc:
                raise SourceSchemaError(f"row {row_number}: {exc}") from exc
            rows.append((csv_name, row_number, *values))

    if rows and metadata.get("rows") is not None and metadata["rows"] != len(rows):
        log.warning("row count differs from metadata",
                    extra={"context": {"csv_rows": len(rows), "metadata_rows": metadata["rows"]}})

    placeholders = ", ".join("?" for _ in range(len(columns) + 2))
    insert_sql = (
        f"INSERT INTO source_records (source_file, source_row_number, {', '.join(columns)}) "
        f"VALUES ({placeholders}) ON CONFLICT (source_file, source_row_number) DO NOTHING"
    )
    with conn:
        conn.execute(
            "INSERT INTO source_files (source_file, dataset_slug, sha256, size_bytes, row_count, "
            "column_count, source_description, loaded_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?) "
            "ON CONFLICT (source_file) DO NOTHING",
            (csv_name, slug, sha, csv_path.stat().st_size, len(rows), len(columns),
             metadata.get("description"), datetime.now(timezone.utc).isoformat()),
        )
        before = conn.total_changes
        conn.executemany(insert_sql, rows)
        inserted = conn.total_changes - before

    result = LoadResult(csv_name, len(rows), inserted, len(rows) - inserted)
    log.info("source load complete", extra={"context": result.__dict__})
    return result


def main() -> int:
    conn = connect()
    try:
        init_db(conn)
        load_source(conn)
    except (SourceSchemaError, SourceChangedError, FileNotFoundError):
        log.exception("source load failed")
        return 1
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
