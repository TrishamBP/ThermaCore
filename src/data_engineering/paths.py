"""Repository paths and dataset constants."""

import os
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]

DATASET_SLUG = "deeplumiere/hyperscale-data-center-dataset"
SOURCE_CSV = "green_ai_datacenter.csv"
METADATA_JSON = "dataset_metadata.json"
EXPECTED_FILES = (SOURCE_CSV, METADATA_JSON, "dataset_summary.txt", "correlation_matrix.csv")

DATA_DIR = REPO_ROOT / "data"
RAW_DATASET_DIR = DATA_DIR / "raw" / "hyperscale-data-center-dataset"
PROCESSED_DIR = DATA_DIR / "processed"
SIMULATION_DIR = DATA_DIR / "simulation"
LOG_DIR = REPO_ROOT / "logs"

SCHEMA_PATH = REPO_ROOT / "db" / "schema.sql"
CONFIG_PATH = REPO_ROOT / "config" / "simulation.yaml"


def db_path() -> Path:
    """SQLite database location; override with DATACENTER_OPS_DB_PATH."""
    return Path(os.environ.get("DATACENTER_OPS_DB_PATH", DATA_DIR / "datacenter_ops.db"))
