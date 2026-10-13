"""Select the source records for the scenario region and export them to data/processed/.

    uv run python -m data_engineering.processing.filter_region

The region code comes from config/simulation.yaml (`scenario.region_code`) and is
matched exactly against the observed `datacenter_region` values. Records from other
regions are never relabeled. The original rows stay unchanged in `source_records`;
this step only records filter membership and writes a CSV copy.
"""

import csv
import sqlite3
import sys
from pathlib import Path

from data_engineering.config import load_config
from data_engineering.db import connect, init_db
from data_engineering.logging_utils import get_logger
from data_engineering.paths import PROCESSED_DIR, SOURCE_CSV

log = get_logger("filter_region")


class NoRegionRecordsError(RuntimeError):
    pass


def observed_regions(conn: sqlite3.Connection, source_file: str = SOURCE_CSV) -> dict[str, int]:
    rows = conn.execute(
        "SELECT datacenter_region, COUNT(*) AS n FROM source_records WHERE source_file = ? "
        "GROUP BY datacenter_region ORDER BY datacenter_region",
        (source_file,),
    )
    return {r["datacenter_region"]: r["n"] for r in rows}


def filter_region(
    conn: sqlite3.Connection,
    region_code: str,
    source_file: str = SOURCE_CSV,
    out_csv: Path | None = None,
) -> int:
    regions = observed_regions(conn, source_file)
    count = regions.get(region_code, 0)
    if count == 0:
        raise NoRegionRecordsError(
            f"no records with datacenter_region == {region_code!r}; observed values: {regions}"
        )
    filter_name = f"datacenter_region={region_code}"  # same format as config.region_filter_name
    with conn:
        conn.execute(
            "INSERT INTO region_filtered_records (filter_name, source_file, source_row_number) "
            "SELECT ?, source_file, source_row_number FROM source_records "
            "WHERE source_file = ? AND datacenter_region = ? "
            "ON CONFLICT DO NOTHING",
            (filter_name, source_file, region_code),
        )

    if out_csv is not None:
        out_csv.parent.mkdir(parents=True, exist_ok=True)
        cursor = conn.execute(
            "SELECT * FROM filtered_source_records WHERE filter_name = ? AND source_file = ? "
            "ORDER BY source_row_number",
            (filter_name, source_file),
        )
        names = [d[0] for d in cursor.description if d[0] != "filter_name"]
        with out_csv.open("w", newline="", encoding="utf-8") as fh:
            writer = csv.writer(fh)
            writer.writerow(names)
            for row in cursor:
                writer.writerow([row[n] for n in names])

    log.info("region filter applied", extra={"context": {
        "filter": filter_name, "records": count, "observed_regions": regions,
        "out_csv": str(out_csv) if out_csv else None}})
    return count


def main() -> int:
    config = load_config()
    region_code = config["scenario"]["region_code"]
    out_csv = PROCESSED_DIR / f"{Path(SOURCE_CSV).stem}_region_{region_code}.csv"
    conn = connect()
    try:
        init_db(conn)
        filter_region(conn, region_code, out_csv=out_csv)
    except NoRegionRecordsError:
        log.exception("region filter failed")
        return 1
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
