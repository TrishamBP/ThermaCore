"""Validate Phase 1: source import, regional filter, time-series coverage, simulated telemetry.

    uv run python -m data_engineering.validation.validate_dataset [--run-id RUN_ID]

Writes data/processed/validation_report.{json,md} and exits non-zero if any check fails.
"""

import argparse
import csv
import json
import sqlite3
import sys
from collections import Counter
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

from data_engineering.config import load_config, region_filter_name
from data_engineering.db import connect, init_db
from data_engineering.ingestion.download_dataset import verify_manifest
from data_engineering.ingestion.load_source import read_metadata
from data_engineering.logging_utils import get_logger
from data_engineering.paths import PROCESSED_DIR, RAW_DATASET_DIR, SOURCE_CSV
from data_engineering.simulation.generate_telemetry import (
    METRICS, generate, load_seed_rows, parse_utc)

log = get_logger("validate_dataset")

TIMESTAMP_FORMATS = ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d",
                     "%d/%m/%Y %H:%M", "%m/%d/%Y %H:%M", "%d/%m/%Y", "%m/%d/%Y")


@dataclass
class Check:
    section: str
    name: str
    passed: bool
    detail: str


def _parses_as_timestamp(value: str) -> bool:
    try:
        datetime.fromisoformat(value.replace("Z", "+00:00"))
        return True
    except ValueError:
        pass
    for fmt in TIMESTAMP_FORMATS:
        try:
            datetime.strptime(value, fmt)
            return True
        except ValueError:
            continue
    return False


def assess_source_time_series(csv_path: Path, sample_rows: int = 2000) -> dict:
    """Look for timestamp and identifier columns that would make the source a real time series."""
    with csv_path.open(newline="", encoding="utf-8-sig") as fh:
        reader = csv.DictReader(fh)
        columns = list(reader.fieldnames or [])
        sample = [row for _, row in zip(range(sample_rows), reader)]
    timestamp_columns = [
        c for c in columns
        if sample and sum(_parses_as_timestamp(r[c]) for r in sample if r[c]) >= 0.95 * len(sample)
        and not all(r[c].lstrip("-").replace(".", "", 1).isdigit() for r in sample if r[c])
    ]
    id_like = [c for c in columns if c.lower().endswith(("_id", "id")) or c.lower() in {"id", "uuid"}]
    unique_columns = [c for c in columns if sample and len({r[c] for r in sample}) == len(sample)]
    has_series = bool(timestamp_columns) and bool(id_like or unique_columns)
    return {
        "columns": len(columns),
        "timestamp_columns": timestamp_columns,
        "identifier_columns": id_like,
        "unique_per_row_columns_in_sample": unique_columns,
        "time_bucket_columns": [c for c in columns if "time" in c.lower() and c not in timestamp_columns],
        "has_minute_level_time_series": has_series,
        "verdict": ("timestamped observations present; check per-asset coverage" if has_series else
                    "no timestamp or asset identifier columns: rows are independent records, "
                    "not a time series; synthetic telemetry is required"),
    }


def grid_coverage(timestamps: list[str], start: datetime, expected_slots: int, freq_s: int) -> dict:
    """Coverage of one asset's timestamps against the expected regular grid."""
    expected = {(start + timedelta(seconds=i * freq_s)).strftime("%Y-%m-%dT%H:%M:%SZ")
                for i in range(expected_slots)}
    counts = Counter(timestamps)
    observed = set(counts)
    return {
        "earliest": min(timestamps) if timestamps else None,
        "latest": max(timestamps) if timestamps else None,
        "expected_slots": expected_slots,
        "observed_unique": len(observed),
        "duplicate_timestamps": sum(n - 1 for n in counts.values() if n > 1),
        "missing_slots": len(expected - observed),
        "off_grid": len(observed - expected),
    }


def check_source(conn: sqlite3.Connection, dataset_dir: Path) -> tuple[list[Check], dict]:
    checks, facts = [], {}
    problems = verify_manifest(dataset_dir)
    checks.append(Check("source", "manifest checksums match", not problems, "; ".join(problems) or "ok"))

    metadata = read_metadata(dataset_dir)
    csv_path = dataset_dir / SOURCE_CSV
    with csv_path.open(newline="", encoding="utf-8-sig") as fh:
        reader = csv.reader(fh)
        header = next(reader)
        csv_rows = 0
        empty_cells = Counter()
        regions = Counter()
        region_idx = header.index("datacenter_region")
        for row in reader:
            csv_rows += 1
            regions[row[region_idx]] += 1
            for name, value in zip(header, row):
                if value == "":
                    empty_cells[name] += 1
    facts.update(csv_rows=csv_rows, csv_columns=len(header), csv_regions=dict(sorted(regions.items())),
                 csv_empty_cells=sum(empty_cells.values()))

    checks.append(Check("source", "CSV header matches dataset_metadata.json features",
                        header == list(metadata["features"]), f"{len(header)} columns"))
    checks.append(Check("source", "CSV row count matches metadata",
                        csv_rows == metadata.get("rows"), f"csv={csv_rows}, metadata={metadata.get('rows')}"))

    db_rows = conn.execute("SELECT COUNT(*) FROM source_records WHERE source_file = ?", (SOURCE_CSV,)).fetchone()[0]
    checks.append(Check("source", "source_records row count equals CSV rows", db_rows == csv_rows,
                        f"db={db_rows}, csv={csv_rows}"))
    dupes = conn.execute("SELECT COUNT(*) FROM (SELECT source_file, source_row_number FROM source_records "
                         "GROUP BY 1, 2 HAVING COUNT(*) > 1)").fetchone()[0]
    checks.append(Check("source", "no duplicate source rows", dupes == 0, f"duplicates={dupes}"))

    null_total = 0
    for column in metadata["features"]:
        null_total += conn.execute(f"SELECT COUNT(*) FROM source_records WHERE {column} IS NULL").fetchone()[0]
    checks.append(Check("source", "NULLs in source_records equal empty CSV cells",
                        null_total == sum(empty_cells.values()),
                        f"db_nulls={null_total}, csv_empty={sum(empty_cells.values())}"))

    db_regions = dict(conn.execute("SELECT datacenter_region, COUNT(*) FROM source_records "
                                   "GROUP BY 1 ORDER BY 1").fetchall())
    facts["db_regions"] = db_regions
    checks.append(Check("source", "region counts in DB match CSV (no 'NA' -> NULL conversion)",
                        db_regions == dict(regions), json.dumps(db_regions)))
    return checks, facts


def check_filter(conn: sqlite3.Connection, config: dict) -> tuple[list[Check], dict]:
    code = config["scenario"]["region_code"]
    name = region_filter_name(config)
    expected = conn.execute("SELECT COUNT(*) FROM source_records WHERE datacenter_region = ?", (code,)).fetchone()[0]
    filtered = conn.execute("SELECT COUNT(*) FROM region_filtered_records WHERE filter_name = ?", (name,)).fetchone()[0]
    wrong = conn.execute("SELECT COUNT(*) FROM filtered_source_records WHERE filter_name = ? "
                         "AND datacenter_region IS NOT ?", (name, code)).fetchone()[0]
    checks = [
        Check("filter", f"region filter selects every '{code}' record", filtered == expected and expected > 0,
              f"filtered={filtered}, source '{code}'={expected}"),
        Check("filter", "filtered records contain no other region", wrong == 0, f"other-region rows={wrong}"),
    ]
    out_csv = PROCESSED_DIR / f"{Path(SOURCE_CSV).stem}_region_{code}.csv"
    if out_csv.exists():
        with out_csv.open(newline="", encoding="utf-8") as fh:
            n = sum(1 for _ in fh) - 1
        checks.append(Check("filter", "processed CSV row count equals filter count", n == filtered,
                            f"{out_csv.name}: {n} rows"))
    else:
        checks.append(Check("filter", "processed CSV exists", False, f"{out_csv} not found"))
    return checks, {"region_code": code, "region_label": config["scenario"].get("region_label"),
                    "filtered_records": filtered}


def latest_run_id(conn: sqlite3.Connection) -> str | None:
    row = conn.execute("SELECT run_id FROM simulation_runs ORDER BY created_at DESC LIMIT 1").fetchone()
    return row["run_id"] if row else None


def check_simulation(conn: sqlite3.Connection, run_id: str) -> tuple[list[Check], dict]:
    checks = []
    run = dict(conn.execute("SELECT * FROM simulation_runs WHERE run_id = ?", (run_id,)).fetchone())
    run_config = json.loads(run["config_json"])
    start = parse_utc(run["start_ts_utc"])
    n_slots, freq_s = run["expected_slots"], run["frequency_seconds"]

    assets = [dict(r) for r in conn.execute("SELECT * FROM asset_metadata WHERE run_id = ?", (run_id,))]
    checks.append(Check("simulation", "asset count matches config", len(assets) == run["asset_count"],
                        f"assets={len(assets)}"))
    checks.append(Check("simulation", "all asset IDs labeled synthetic",
                        all(a["identifier_origin"] == "synthetic" for a in assets), "identifier_origin"))
    bad_baselines = conn.execute(
        "SELECT COUNT(*) FROM asset_metadata a LEFT JOIN region_filtered_records f "
        "ON f.source_file = a.baseline_source_file AND f.source_row_number = a.baseline_source_row_number "
        "AND f.filter_name = ? WHERE a.run_id = ? AND f.filter_name IS NULL",
        (run["region_filter"], run_id)).fetchone()[0]
    checks.append(Check("simulation", "every asset baseline is a filtered-region source row",
                        bad_baselines == 0, f"non-matching baselines={bad_baselines}"))

    span_end = start + timedelta(seconds=(n_slots - 1) * freq_s)
    covers = (span_end - start + timedelta(seconds=freq_s)) == timedelta(minutes=run["duration_minutes"])
    checks.append(Check("simulation", "time grid spans the configured duration",
                        covers and run["duration_minutes"] == 1440,
                        f"{run['start_ts_utc']} .. {span_end:%Y-%m-%dT%H:%M:%SZ}, {run['duration_minutes']} min"))

    per_asset = {}
    for a in assets:
        ts = [r[0] for r in conn.execute("SELECT ts_utc FROM telemetry_readings WHERE run_id = ? AND asset_id = ?",
                                         (run_id, a["asset_id"]))]
        observed = [r[0] for r in conn.execute(
            "SELECT ts_utc FROM telemetry_readings WHERE run_id = ? AND asset_id = ? AND is_missing = 0",
            (run_id, a["asset_id"]))]
        grid = grid_coverage(ts, start, n_slots, freq_s)
        per_asset[a["asset_id"]] = {"slots": len(ts), "observed_readings": len(observed),
                                    "missing_readings": len(ts) - len(observed), **grid}
    exact = all(v["slots"] == n_slots == 1440 and v["missing_slots"] == 0 and v["off_grid"] == 0
                for v in per_asset.values())
    checks.append(Check("simulation", "each asset has exactly 1,440 expected minute slots", exact,
                        f"slot counts={sorted({v['slots'] for v in per_asset.values()})}"))

    dup = conn.execute("SELECT COUNT(*) FROM (SELECT asset_id, ts_utc FROM telemetry_readings WHERE run_id = ? "
                       "GROUP BY 1, 2 HAVING COUNT(*) > 1)", (run_id,)).fetchone()[0]
    checks.append(Check("simulation", "no duplicate asset/timestamp combinations", dup == 0, f"duplicates={dup}"))

    any_value = " OR ".join(f"{m} IS NOT NULL" for m in METRICS)
    any_null = " OR ".join(f"{m} IS NULL" for m in METRICS)
    missing = conn.execute("SELECT COUNT(*) FROM telemetry_readings WHERE run_id = ? AND is_missing = 1",
                           (run_id,)).fetchone()[0]
    missing_with_values = conn.execute(f"SELECT COUNT(*) FROM telemetry_readings WHERE run_id = ? "
                                       f"AND is_missing = 1 AND ({any_value})", (run_id,)).fetchone()[0]
    observed_with_nulls = conn.execute(f"SELECT COUNT(*) FROM telemetry_readings WHERE run_id = ? "
                                       f"AND is_missing = 0 AND ({any_null})", (run_id,)).fetchone()[0]
    checks.append(Check("simulation", "missing readings are explicit NULL slots (not zero, not dropped)",
                        missing_with_values == 0 and observed_with_nulls == 0 and (missing > 0 or run["missing_rate"] == 0),
                        f"missing slots={missing}, missing-with-values={missing_with_values}, "
                        f"observed-with-nulls={observed_with_nulls}"))

    non_synth = conn.execute("SELECT COUNT(*) FROM telemetry_readings WHERE run_id = ? AND provenance != 'synthetic'",
                             (run_id,)).fetchone()[0]
    checks.append(Check("simulation", "all telemetry labeled provenance='synthetic'", non_synth == 0,
                        f"non-synthetic rows={non_synth}"))

    events = conn.execute("SELECT COUNT(*) FROM anomaly_events WHERE run_id = ?", (run_id,)).fetchone()[0]
    configured = sum(int(e["count"]) for e in run_config.get("anomalies", {}).get("events", []))
    labels = conn.execute("SELECT COUNT(*) FROM anomaly_labels WHERE run_id = ?", (run_id,)).fetchone()[0]
    unflagged = conn.execute(
        "SELECT COUNT(*) FROM anomaly_labels l JOIN telemetry_readings t USING (run_id, asset_id, ts_utc) "
        "WHERE l.run_id = ? AND t.is_anomalous = 0", (run_id,)).fetchone()[0]
    mismatched = 0
    for m in METRICS:
        mismatched += conn.execute(
            f"SELECT COUNT(*) FROM anomaly_labels l JOIN telemetry_readings t USING (run_id, asset_id, ts_utc) "
            f"WHERE l.run_id = ? AND l.metric = ? AND (t.{m} != l.injected_value OR l.injected_value = l.clean_value)",
            (run_id, m)).fetchone()[0]
    flagged_without_label = conn.execute(
        "SELECT COUNT(*) FROM telemetry_readings t WHERE run_id = ? AND is_anomalous = 1 AND NOT EXISTS "
        "(SELECT 1 FROM anomaly_labels l WHERE l.run_id = t.run_id AND l.asset_id = t.asset_id AND l.ts_utc = t.ts_utc)",
        (run_id,)).fetchone()[0]
    anomalous_slots = conn.execute("SELECT COUNT(*) FROM telemetry_readings WHERE run_id = ? AND is_anomalous = 1",
                                   (run_id,)).fetchone()[0]
    checks.append(Check("simulation", "injected anomalies are identifiable with clean baseline retained",
                        events == configured and labels > 0 and unflagged == 0 and mismatched == 0
                        and flagged_without_label == 0,
                        f"events={events}/{configured}, labeled cells={labels}, unflagged={unflagged}, "
                        f"value mismatches={mismatched}, flagged-without-label={flagged_without_label}"))

    seed_rows = load_seed_rows(conn, run["region_filter"], run["source_file"])
    regenerated = generate(run_config, seed_rows, run["source_file"], run["source_sha256"])
    checks.append(Check("simulation", "reproducible: regenerating with the same seed gives identical output",
                        regenerated.content_sha256 == run["content_sha256"]
                        and regenerated.run["run_id"] == run_id,
                        f"content_sha256={run['content_sha256'][:16]}..."))

    by_type = Counter(r["anomaly_type"] for r in conn.execute(
        "SELECT anomaly_type FROM anomaly_events WHERE run_id = ?", (run_id,)))
    facts = {
        "run_id": run_id, "seed": run["seed"], "generator": f"{run['generator_name']} {run['generator_version']}",
        "start_ts_utc": run["start_ts_utc"], "end_ts_utc": f"{span_end:%Y-%m-%dT%H:%M:%SZ}",
        "assets": len(assets), "expected_slots_per_asset": n_slots,
        "total_slots": sum(v["slots"] for v in per_asset.values()),
        "observed_readings": sum(v["observed_readings"] for v in per_asset.values()),
        "missing_readings": missing, "configured_missing_rate": run["missing_rate"],
        "anomaly_events": events, "anomaly_events_by_type": dict(by_type),
        "anomalous_slots": anomalous_slots, "anomaly_labeled_cells": labels,
        "assets_by_server_type": dict(Counter(a["server_type"] for a in assets)),
        "source_file": run["source_file"], "source_sha256": run["source_sha256"],
        "region_filter": run["region_filter"], "per_asset": per_asset,
    }
    return checks, facts


def render_markdown(report: dict) -> str:
    s, f, c, sim = report["source"], report["filter"], report["coverage"], report.get("simulation")
    lines = [
        "# Phase 1 Validation Report", "",
        f"Generated: {report['generated_at']}  ",
        f"Overall: **{'PASS' if report['passed'] else 'FAIL'}** "
        f"({sum(ch['passed'] for ch in report['checks'])}/{len(report['checks'])} checks passed)", "",
        "## Provenance", "",
        "- `source_records`: original rows from Kaggle `deeplumiere/hyperscale-data-center-dataset`, unchanged. "
        "The dataset's own metadata describes it as an *engineered synthetic dataset*.",
        "- `telemetry_readings`, `asset_metadata`, `anomaly_*`: **synthetic**, generated by this project. "
        "Not real historical telemetry. Asset IDs are simulation constructs.", "",
        "## Source", "",
        f"- `{SOURCE_CSV}`: {s['csv_rows']:,} rows x {s['csv_columns']} columns, {s['csv_empty_cells']} empty cells",
        "- Regions: " + ", ".join(f"{k} {v:,}" for k, v in s["csv_regions"].items()), "",
        "## Regional filter", "",
        f"- `{f['region_code']}` ({f['region_label']}): **{f['filtered_records']:,}** records", "",
        "## Source time-series coverage", "",
        f"- Timestamp columns: {c['timestamp_columns'] or 'none'}; identifier columns: {c['identifier_columns'] or 'none'}",
        f"- Time-bucket columns (categorical, not timestamps): {c['time_bucket_columns'] or 'none'}",
        f"- Verdict: {c['verdict']}", "",
    ]
    if sim:
        lines += [
            "## Simulated telemetry (synthetic)", "",
            f"- Run `{sim['run_id']}` (seed {sim['seed']}, {sim['generator']})",
            f"- Window: {sim['start_ts_utc']} .. {sim['end_ts_utc']}",
            f"- Assets: {sim['assets']} ({', '.join(f'{k} {v}' for k, v in sorted(sim['assets_by_server_type'].items()))})",
            f"- Slots: {sim['expected_slots_per_asset']} per asset, {sim['total_slots']:,} total",
            f"- Observed readings: {sim['observed_readings']:,}; missing: {sim['missing_readings']:,} "
            f"(configured rate {sim['configured_missing_rate']})",
            f"- Injected anomaly events: {sim['anomaly_events']} "
            f"({', '.join(f'{k} {v}' for k, v in sorted(sim['anomaly_events_by_type'].items()))}); "
            f"anomalous slots {sim['anomalous_slots']}, labeled cells {sim['anomaly_labeled_cells']}", "",
        ]
    lines += ["## Checks", "", "| Section | Check | Result | Detail |", "|---|---|---|---|"]
    lines += [f"| {ch['section']} | {ch['name']} | {'PASS' if ch['passed'] else 'FAIL'} | {ch['detail']} |"
              for ch in report["checks"]]
    return "\n".join(lines) + "\n"


def validate(conn: sqlite3.Connection, config: dict, dataset_dir: Path = RAW_DATASET_DIR,
             run_id: str | None = None) -> dict:
    checks, report = [], {"generated_at": datetime.now(timezone.utc).isoformat()}
    source_checks, report["source"] = check_source(conn, dataset_dir)
    filter_checks, report["filter"] = check_filter(conn, config)
    report["coverage"] = assess_source_time_series(dataset_dir / SOURCE_CSV)
    checks += source_checks + filter_checks
    run_id = run_id or latest_run_id(conn)
    if run_id is None:
        checks.append(Check("simulation", "a simulation run exists", False, "no rows in simulation_runs"))
    else:
        sim_checks, report["simulation"] = check_simulation(conn, run_id)
        checks += sim_checks
    report["checks"] = [asdict(ch) for ch in checks]
    report["passed"] = all(ch.passed for ch in checks)
    return report


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--run-id", default=None, help="simulation run to validate (default: latest)")
    args = ap.parse_args()
    conn = connect()
    try:
        init_db(conn)
        report = validate(conn, load_config(), run_id=args.run_id)
    finally:
        conn.close()
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    (PROCESSED_DIR / "validation_report.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    (PROCESSED_DIR / "validation_report.md").write_text(render_markdown(report), encoding="utf-8")
    for ch in report["checks"]:
        print(f"[{'PASS' if ch['passed'] else 'FAIL'}] {ch['section']}: {ch['name']} -- {ch['detail']}")
    log.info("validation finished", extra={"context": {
        "passed": report["passed"], "report": str(PROCESSED_DIR / "validation_report.md")}})
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    sys.exit(main())
