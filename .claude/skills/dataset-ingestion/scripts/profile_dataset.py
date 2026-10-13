"""Profile tabular dataset files (CSV/TSV/JSONL) using only the standard library.

Usage:
    python -I profile_dataset.py <dataset_dir> [--out profile.json] [--manifest]
                                 [--max-rows N] [--samples K]

Reports per file: row count, columns, inferred types, null counts, capped
distinct counts, numeric min/max, sample values, timestamp parse rate and
monotonicity, and candidate identifier columns. Parquet/XLSX files are
listed; Parquet is profiled only if duckdb happens to be importable.

The script never executes or imports anything from the dataset directory.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

# Only empty cells are null by default. Tokens like "NA" can be real values
# (e.g. a region code for North America); opt in with --null-tokens.
NULL_TOKENS = {""}
DISTINCT_CAP = 1000
TABULAR = {".csv", ".tsv", ".txt", ".jsonl", ".ndjson"}
BINARY_TABULAR = {".parquet", ".xlsx", ".xls"}

TIMESTAMP_FORMATS = (
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d %H:%M:%S.%f",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%dT%H:%M:%S.%f",
    "%Y-%m-%d %H:%M",
    "%Y-%m-%d",
    "%d/%m/%Y %H:%M",
    "%m/%d/%Y %H:%M",
    "%d/%m/%Y",
    "%m/%d/%Y",
    "%Y/%m/%d %H:%M:%S",
    "%Y/%m/%d",
)


def parse_timestamp(value: str) -> datetime | None:
    v = value.strip()
    try:
        return datetime.fromisoformat(v.replace("Z", "+00:00"))
    except ValueError:
        pass
    for fmt in TIMESTAMP_FORMATS:
        try:
            return datetime.strptime(v, fmt)
        except ValueError:
            continue
    # Epoch seconds / milliseconds in a plausible range (2000-01-01 .. 2100-01-01).
    try:
        n = float(v)
    except ValueError:
        return None
    for scale in (1, 1000):
        s = n / scale
        if 946684800 <= s <= 4102444800:
            return datetime.fromtimestamp(s, tz=timezone.utc)
    return None


def classify(value: str) -> str:
    v = value.strip()
    if v.lower() in {"true", "false"}:
        return "bool"
    try:
        int(v)
        return "int"
    except ValueError:
        pass
    try:
        float(v)
        return "float"
    except ValueError:
        pass
    if parse_timestamp(v) is not None:
        return "timestamp"
    return "string"


class ColumnStats:
    def __init__(self, name: str, samples: int) -> None:
        self.name = name
        self.count = 0
        self.nulls = 0
        self.types: dict[str, int] = {}
        self.distinct: set[str] = set()
        self.distinct_capped = False
        self.min: float | None = None
        self.max: float | None = None
        self.samples: list[str] = []
        self.max_samples = samples
        self.ts_ok = 0
        self.ts_prev: datetime | None = None
        self.ts_monotonic = True
        self.ts_min: datetime | None = None
        self.ts_max: datetime | None = None

    def add(self, raw: object) -> None:
        self.count += 1
        value = "" if raw is None else str(raw)
        if value.strip().lower() in NULL_TOKENS:
            self.nulls += 1
            return
        kind = classify(value)
        self.types[kind] = self.types.get(kind, 0) + 1
        if not self.distinct_capped:
            self.distinct.add(value)
            if len(self.distinct) > DISTINCT_CAP:
                self.distinct_capped = True
        if len(self.samples) < self.max_samples and value not in self.samples:
            self.samples.append(value)
        if kind in {"int", "float"}:
            n = float(value)
            self.min = n if self.min is None else min(self.min, n)
            self.max = n if self.max is None else max(self.max, n)
        # Timestamps are either date-like strings or integer epoch values.
        if kind in {"timestamp", "int"}:
            ts = parse_timestamp(value)
            if ts is not None:
                self.ts_ok += 1
                naive = ts.replace(tzinfo=None)
                if self.ts_prev is not None and naive < self.ts_prev:
                    self.ts_monotonic = False
                self.ts_prev = naive
                self.ts_min = naive if self.ts_min is None else min(self.ts_min, naive)
                self.ts_max = naive if self.ts_max is None else max(self.ts_max, naive)

    def report(self) -> dict:
        non_null = self.count - self.nulls
        inferred = max(self.types, key=self.types.get) if self.types else "empty"
        out = {
            "inferred_type": inferred,
            "type_counts": self.types,
            "nulls": self.nulls,
            "null_pct": round(100 * self.nulls / self.count, 2) if self.count else 0.0,
            "distinct": f">{DISTINCT_CAP}" if self.distinct_capped else len(self.distinct),
            "samples": self.samples,
        }
        if self.min is not None:
            out["min"], out["max"] = self.min, self.max
        ts_rate = self.ts_ok / non_null if non_null else 0.0
        # Plain integers parse as epoch only in a narrow range; require a high rate.
        if self.ts_ok and (inferred == "timestamp" or ts_rate >= 0.95):
            out["timestamp"] = {
                "parse_rate": round(ts_rate, 4),
                "min": self.ts_min.isoformat() if self.ts_min else None,
                "max": self.ts_max.isoformat() if self.ts_max else None,
                "monotonic_in_file_order": self.ts_monotonic,
            }
        return out


def iter_rows(path: Path):
    suffix = path.suffix.lower()
    if suffix in {".jsonl", ".ndjson"}:
        with path.open(encoding="utf-8", errors="replace") as fh:
            for line in fh:
                line = line.strip()
                if line:
                    obj = json.loads(line)
                    if isinstance(obj, dict):
                        yield obj
        return
    with path.open(newline="", encoding="utf-8-sig", errors="replace") as fh:
        head = fh.read(65536)
        fh.seek(0)
        try:
            dialect = csv.Sniffer().sniff(head, delimiters=",;\t|")
        except csv.Error:
            dialect = csv.excel_tab if suffix == ".tsv" else csv.excel
        yield from csv.DictReader(fh, dialect=dialect)


def profile_text_file(path: Path, max_rows: int | None, samples: int) -> dict:
    stats: dict[str, ColumnStats] = {}
    rows = 0
    for row in iter_rows(path):
        if max_rows is not None and rows >= max_rows:
            break
        rows += 1
        for col, value in row.items():
            if col is None:  # extra fields beyond header
                col = "__extra_fields__"
            if col not in stats:
                stats[col] = ColumnStats(col, samples)
            stats[col].add(value)
    columns = {name: s.report() for name, s in stats.items()}
    id_candidates = [
        name for name, c in columns.items()
        if c["inferred_type"] in {"string", "int"}
        and "timestamp" not in c
        and c["null_pct"] == 0
        and isinstance(c["distinct"], int) and 1 < c["distinct"] < max(rows, 2)
    ]
    unique_cols = [
        name for name, c in columns.items()
        if isinstance(c["distinct"], int) and c["distinct"] == rows and rows > 1
    ]
    return {
        "rows_profiled": rows,
        "truncated": max_rows is not None and rows >= max_rows,
        "columns": columns,
        "timestamp_candidates": [n for n, c in columns.items() if "timestamp" in c],
        "repeating_identifier_candidates": id_candidates,
        "unique_per_row_columns": unique_cols,
    }


def profile_parquet(path: Path) -> dict:
    try:
        import duckdb  # type: ignore
    except ImportError:
        return {"skipped": "duckdb not installed; Parquet listed but not profiled"}
    con = duckdb.connect()
    rel = con.sql(f"SELECT * FROM read_parquet('{path.as_posix()}')")
    summary = con.sql(f"SUMMARIZE SELECT * FROM read_parquet('{path.as_posix()}')")
    return {
        "rows_profiled": rel.count("*").fetchone()[0],
        "columns": {r[0]: dict(zip(summary.columns[1:], map(str, r[1:]))) for r in summary.fetchall()},
    }


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("dataset_dir", type=Path)
    ap.add_argument("--out", type=Path, help="write JSON profile here (default: stdout)")
    ap.add_argument("--manifest", action="store_true", help="write MANIFEST.json into dataset_dir")
    ap.add_argument("--max-rows", type=int, default=None, help="profile at most N rows per file")
    ap.add_argument("--samples", type=int, default=5, help="sample values per column")
    ap.add_argument("--null-tokens", default="",
                    help="extra comma-separated values to treat as null, e.g. 'nan,null,n/a'")
    args = ap.parse_args()
    NULL_TOKENS.update(t.strip().lower() for t in args.null_tokens.split(",") if t.strip())

    root: Path = args.dataset_dir
    if not root.is_dir():
        print(f"error: {root} is not a directory", file=sys.stderr)
        return 2

    files = sorted(p for p in root.rglob("*") if p.is_file() and p.name != "MANIFEST.json")
    report: dict = {
        "dataset_dir": root.as_posix(),
        "profiled_at": datetime.now(timezone.utc).isoformat(),
        "files": {},
        "other_files": [],
    }
    manifest = []
    for path in files:
        rel = path.relative_to(root).as_posix()
        suffix = path.suffix.lower()
        if args.manifest:
            manifest.append({"file": rel, "bytes": path.stat().st_size, "sha256": sha256(path)})
        if suffix in TABULAR:
            report["files"][rel] = profile_text_file(path, args.max_rows, args.samples)
        elif suffix == ".parquet":
            report["files"][rel] = profile_parquet(path)
        elif suffix in BINARY_TABULAR:
            report["files"][rel] = {"skipped": f"{suffix} not supported by stdlib profiler"}
        else:
            report["other_files"].append(rel)

    if args.manifest:
        (root / "MANIFEST.json").write_text(
            json.dumps({"generated_at": report["profiled_at"], "files": manifest}, indent=2),
            encoding="utf-8",
        )

    text = json.dumps(report, indent=2, default=str)
    if args.out:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(text, encoding="utf-8")
        for rel, info in report["files"].items():
            cols = info.get("columns", {})
            print(f"{rel}: {info.get('rows_profiled', '?')} rows, {len(cols)} columns; "
                  f"timestamps={info.get('timestamp_candidates', [])} "
                  f"ids={info.get('repeating_identifier_candidates', [])}")
        print(f"profile written to {args.out}")
    else:
        print(text)
    return 0


if __name__ == "__main__":
    sys.exit(main())
