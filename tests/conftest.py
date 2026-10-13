"""Shared fixtures.

The fixture dataset is SYNTHETIC test data generated here (not copied from Kaggle). It
reproduces the real file layout: the same 35 columns, a dataset_metadata.json, and the
literal region code "NA", so ingestion, filtering and simulation can be tested without
downloading anything.
"""

import copy
import csv
import json
import random
import sqlite3
from pathlib import Path

import pytest

from data_engineering.config import load_config
from data_engineering.db import connect, init_db
from data_engineering.ingestion.download_dataset import write_manifest
from data_engineering.ingestion.load_source import KEY_COLUMNS, load_source
from data_engineering.paths import METADATA_JSON, SOURCE_CSV
from data_engineering.processing.filter_region import filter_region

SQL_TO_DTYPE = {"TEXT": "object", "INTEGER": "int64", "REAL": "float64"}
REGION_COUNTS = {"NA": 40, "EU": 15, "APAC": 10}
CATEGORIES = {
    "server_type": ["Compute", "GPU", "Storage", "Edge"],
    "cooling_type": ["Air", "Liquid", "Hybrid"],
    "time_of_day": ["Night", "Off-Peak", "Peak"],
    "maintenance_status": ["Normal", "Recent", "Pending"],
    "energy_source_primary": ["Grid", "Solar", "Wind", "Hydro"],
    "energy_efficiency_class": ["Low", "Medium", "High"],
    "failure_risk_level": ["Low", "Medium", "High"],
}


def source_features() -> dict[str, str]:
    conn = sqlite3.connect(":memory:")
    init_db(conn)
    info = conn.execute("PRAGMA table_info(source_records)").fetchall()
    conn.close()
    return {row[1]: SQL_TO_DTYPE[row[2]] for row in info if row[1] not in KEY_COLUMNS}


def _fixture_value(rng: random.Random, name: str, dtype: str, row: dict):
    if name in CATEGORIES:
        return rng.choice(CATEGORIES[name])
    if dtype == "int64":
        return rng.randint(1, 42)
    if name == "workload_intensity":
        return round(rng.uniform(0.1, 1.0), 3)
    if name == "gpu_utilization":
        return round(rng.uniform(0, 100), 3) if row["server_type"] == "GPU" else 0.0
    if name == "cpu_utilization":
        return round(row["workload_intensity"] * 90 + rng.uniform(-3, 3), 3)
    if name == "power_consumption_kw":
        return round(50 + 3 * row["gpu_utilization"] + rng.uniform(0, 20), 3)
    return round(rng.uniform(1, 100), 3)


def write_fixture_dataset(directory: Path, region_counts: dict[str, int] = REGION_COUNTS) -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    features = source_features()
    rng = random.Random(1234)
    rows = []
    for region, n in region_counts.items():
        for _ in range(n):
            row: dict = {}
            for name, dtype in features.items():
                row[name] = region if name == "datacenter_region" else _fixture_value(rng, name, dtype, row)
            rows.append(row)
    with (directory / SOURCE_CSV).open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(features))
        writer.writeheader()
        writer.writerows(rows)
    (directory / METADATA_JSON).write_text(json.dumps(
        {"dataset_name": "pytest fixture (synthetic)", "rows": len(rows), "columns": len(features),
         "features": features, "description": "Synthetic pytest fixture."}), encoding="utf-8")
    write_manifest(directory, "pytest/fixture")
    return directory


@pytest.fixture
def dataset_dir(tmp_path: Path) -> Path:
    return write_fixture_dataset(tmp_path / "raw")


@pytest.fixture
def conn(tmp_path: Path):
    connection = connect(tmp_path / "test.db")
    init_db(connection)
    yield connection
    connection.close()


@pytest.fixture
def loaded_conn(conn, dataset_dir):
    load_source(conn, dataset_dir)
    filter_region(conn, "NA")
    return conn


@pytest.fixture
def sim_config() -> dict:
    config = copy.deepcopy(load_config())
    config["simulation"]["asset_count"] = 4
    config["simulation"]["missing_rate"] = 0.02
    for event in config["anomalies"]["events"]:
        event["count"] = 1
    return config


# ---------------------------------------------------------------- telemetry store

POSTGRES_TEST_URL_ENV = "TELEMETRY_TEST_POSTGRES_URL"


def _sqlite_store(path):
    from data_engineering.storage.sqlite_store import SqliteTelemetryStore
    store = SqliteTelemetryStore(path)
    store.initialize()
    return store


@pytest.fixture
def sqlite_store(tmp_path):
    store = _sqlite_store(tmp_path / "telemetry_store.db")
    yield store
    store.close()


@pytest.fixture(params=["sqlite", pytest.param("postgres", marks=pytest.mark.postgres)])
def store(request, tmp_path):
    """Initialized telemetry store; the postgres variant runs only when
    TELEMETRY_TEST_POSTGRES_URL is set (each test gets its own schema, dropped afterwards)."""
    import os
    import uuid
    if request.param == "sqlite":
        s = _sqlite_store(tmp_path / "telemetry_store.db")
        yield s
        s.close()
        return
    url = os.environ.get(POSTGRES_TEST_URL_ENV)
    if not url:
        pytest.skip(f"{POSTGRES_TEST_URL_ENV} not set; PostgreSQL store tests not run")
    psycopg = pytest.importorskip("psycopg")
    schema = f"test_{uuid.uuid4().hex[:10]}"
    with psycopg.connect(url, autocommit=True) as admin:
        admin.execute(f"CREATE SCHEMA {schema}")
    from data_engineering.storage.postgres_store import PostgresTelemetryStore
    s = PostgresTelemetryStore(url, search_path=schema)
    s.initialize()
    yield s
    s.close()
    with psycopg.connect(url, autocommit=True) as admin:
        admin.execute(f"DROP SCHEMA {schema} CASCADE")


# ---------------------------------------------------------------- HTTP API (app package)

@pytest.fixture
def anyio_backend():
    return "asyncio"


def make_api_app(store_path=None, *, store_settings=None, cooling_config=None, **app_settings):
    """create_app() wired to a test store; nothing is read from the real .env for the database."""
    from app.config import AppSettings
    from app.main import create_app
    from cooling_helpers import all_limits_config
    from data_engineering.streaming.settings import load_consumer_settings
    store_settings = store_settings or load_consumer_settings(env={"TELEMETRY_SQLITE_PATH": str(store_path)})
    return create_app(AppSettings(environment="test", **app_settings), store_settings=store_settings,
                      cooling_config=cooling_config or all_limits_config())


@pytest.fixture
def api_app(sqlite_store):
    return make_api_app(sqlite_store.path)


@pytest.fixture
async def api_client(api_app):
    """Async client over ASGITransport with the app's lifespan running (as uvicorn would)."""
    import httpx
    async with api_app.router.lifespan_context(api_app):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=api_app), base_url="http://test") as client:
            yield client
