import csv
import json

import pytest

from conftest import REGION_COUNTS
from data_engineering.ingestion.download_dataset import verify_manifest
from data_engineering.ingestion.load_source import SourceChangedError, SourceSchemaError, load_source
from data_engineering.paths import METADATA_JSON, SOURCE_CSV
from data_engineering.processing.filter_region import NoRegionRecordsError, filter_region
from data_engineering.validation.validate_dataset import assess_source_time_series

TOTAL = sum(REGION_COUNTS.values())


def test_load_source_preserves_all_rows_and_na_region(conn, dataset_dir):
    result = load_source(conn, dataset_dir)
    assert (result.rows_in_file, result.inserted) == (TOTAL, TOTAL)
    regions = dict(conn.execute("SELECT datacenter_region, COUNT(*) FROM source_records GROUP BY 1").fetchall())
    assert regions == REGION_COUNTS                      # "NA" stays a string, never NULL
    assert conn.execute("SELECT COUNT(*) FROM source_records WHERE datacenter_region IS NULL").fetchone()[0] == 0


def test_load_source_keeps_every_column_and_value(conn, dataset_dir):
    load_source(conn, dataset_dir)
    with (dataset_dir / SOURCE_CSV).open(newline="", encoding="utf-8") as fh:
        first = next(csv.DictReader(fh))
    row = dict(conn.execute("SELECT * FROM source_records WHERE source_row_number = 1").fetchone())
    features = json.loads((dataset_dir / METADATA_JSON).read_text())["features"]
    for name, dtype in features.items():
        expected = first[name] if dtype == "object" else (int(first[name]) if dtype.startswith("int") else float(first[name]))
        assert row[name] == expected, name
    assert row["provenance"] == "dataset"


def test_load_source_is_idempotent(conn, dataset_dir):
    load_source(conn, dataset_dir)
    again = load_source(conn, dataset_dir)
    assert (again.inserted, again.already_present) == (0, TOTAL)
    assert conn.execute("SELECT COUNT(*) FROM source_records").fetchone()[0] == TOTAL


def test_load_source_refuses_changed_file(conn, dataset_dir):
    load_source(conn, dataset_dir)
    with (dataset_dir / SOURCE_CSV).open("a", encoding="utf-8") as fh:
        fh.write("\n")
    with pytest.raises(SourceChangedError):
        load_source(conn, dataset_dir)


def test_load_source_rejects_header_mismatch(conn, dataset_dir):
    path = dataset_dir / SOURCE_CSV
    lines = path.read_text(encoding="utf-8").splitlines()
    lines[0] = lines[0].replace("pue", "pue_renamed")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    with pytest.raises(SourceSchemaError):
        load_source(conn, dataset_dir)


def test_empty_cell_is_null_not_zero(conn, dataset_dir):
    path = dataset_dir / SOURCE_CSV
    with path.open(newline="", encoding="utf-8") as fh:
        rows = list(csv.reader(fh))
    pue_idx = rows[0].index("pue")
    rows[1][pue_idx] = ""
    with path.open("w", newline="", encoding="utf-8") as fh:
        csv.writer(fh).writerows(rows)
    load_source(conn, dataset_dir)
    assert conn.execute("SELECT pue FROM source_records WHERE source_row_number = 1").fetchone()[0] is None


def test_manifest_detects_modified_file(dataset_dir):
    assert verify_manifest(dataset_dir) == []
    (dataset_dir / SOURCE_CSV).write_text("tampered", encoding="utf-8")
    assert any("checksum mismatch" in p for p in verify_manifest(dataset_dir))


def test_filter_region_selects_only_exact_code(conn, dataset_dir):
    load_source(conn, dataset_dir)
    assert filter_region(conn, "NA") == REGION_COUNTS["NA"]
    assert filter_region(conn, "NA") == REGION_COUNTS["NA"]          # idempotent
    rows = conn.execute("SELECT DISTINCT datacenter_region FROM filtered_source_records "
                        "WHERE filter_name = 'datacenter_region=NA'").fetchall()
    assert [r[0] for r in rows] == ["NA"]
    assert conn.execute("SELECT COUNT(*) FROM region_filtered_records").fetchone()[0] == REGION_COUNTS["NA"]


def test_filter_region_exports_csv(conn, dataset_dir, tmp_path):
    load_source(conn, dataset_dir)
    out = tmp_path / "processed" / "na.csv"
    filter_region(conn, "NA", out_csv=out)
    with out.open(newline="", encoding="utf-8") as fh:
        rows = list(csv.DictReader(fh))
    assert len(rows) == REGION_COUNTS["NA"]
    assert {r["datacenter_region"] for r in rows} == {"NA"}
    assert "source_row_number" in rows[0]


def test_filter_region_without_records_fails_instead_of_relabeling(conn, tmp_path):
    from conftest import write_fixture_dataset
    load_source(conn, write_fixture_dataset(tmp_path / "eu_only", {"EU": 5}))
    with pytest.raises(NoRegionRecordsError):
        filter_region(conn, "NA")


def test_time_series_assessment_finds_no_timestamps_in_independent_records(dataset_dir):
    result = assess_source_time_series(dataset_dir / SOURCE_CSV)
    assert result["timestamp_columns"] == []
    assert result["has_minute_level_time_series"] is False
    assert "time_of_day" in result["time_bucket_columns"]


def test_time_series_assessment_detects_real_timestamps(tmp_path):
    path = tmp_path / "ts.csv"
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(["device_id", "timestamp", "value"])
        for minute in range(30):
            writer.writerow(["d1", f"2024-01-01 00:{minute:02d}:00", minute])
    result = assess_source_time_series(path)
    assert result["timestamp_columns"] == ["timestamp"]
    assert result["has_minute_level_time_series"] is True
