"""Run Phase 1 end to end: download -> load source -> filter region -> simulate -> validate.

    uv run python -m data_engineering.pipeline
"""

import sys
from pathlib import Path

from data_engineering.config import load_config
from data_engineering.db import connect, init_db
from data_engineering.ingestion.download_dataset import download
from data_engineering.ingestion.load_source import load_source
from data_engineering.logging_utils import get_logger
from data_engineering.paths import PROCESSED_DIR, SOURCE_CSV
from data_engineering.processing.filter_region import filter_region
from data_engineering.simulation.generate_telemetry import run_simulation
from data_engineering.validation import validate_dataset

log = get_logger("pipeline")


def main() -> int:
    config = load_config()
    code = config["scenario"]["region_code"]
    try:
        download()
        conn = connect()
        try:
            init_db(conn)
            load_source(conn)
            filter_region(conn, code, out_csv=PROCESSED_DIR / f"{Path(SOURCE_CSV).stem}_region_{code}.csv")
            run_simulation(conn, config)
        finally:
            conn.close()
    except Exception:
        log.exception("phase 1 pipeline failed")
        return 1
    status = validate_dataset.main()
    log.info("phase 1 pipeline finished", extra={"context": {"status": "SUCCESS" if status == 0 else "FAILED"}})
    return status


if __name__ == "__main__":
    sys.exit(main())
