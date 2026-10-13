"""Download the Kaggle dataset into data/raw/ and write a checksum manifest.

    uv run python -m data_engineering.ingestion.download_dataset [--force]

Uses the official Kaggle CLI. Public datasets can be downloaded without
credentials with Kaggle CLI 2.x; if Kaggle requires authentication, see README.md.
Downloaded files are never modified after download.
"""

import argparse
import hashlib
import json
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

from data_engineering.logging_utils import get_logger
from data_engineering.paths import DATASET_SLUG, EXPECTED_FILES, RAW_DATASET_DIR

MANIFEST_NAME = "MANIFEST.json"
log = get_logger("download_dataset")


class DownloadError(RuntimeError):
    pass


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def write_manifest(dataset_dir: Path, slug: str = DATASET_SLUG) -> dict:
    files = sorted(p for p in dataset_dir.iterdir() if p.is_file() and p.name != MANIFEST_NAME)
    manifest = {
        "dataset_slug": slug,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "files": [
            {"file": p.name, "bytes": p.stat().st_size, "sha256": sha256_file(p)} for p in files
        ],
    }
    (dataset_dir / MANIFEST_NAME).write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return manifest


def verify_manifest(dataset_dir: Path) -> list[str]:
    """Return a list of problems; empty means every manifest entry matches the files on disk."""
    manifest_path = dataset_dir / MANIFEST_NAME
    if not manifest_path.exists():
        return [f"{MANIFEST_NAME} not found in {dataset_dir}"]
    problems = []
    for entry in json.loads(manifest_path.read_text(encoding="utf-8"))["files"]:
        path = dataset_dir / entry["file"]
        if not path.exists():
            problems.append(f"missing file {entry['file']}")
        elif sha256_file(path) != entry["sha256"]:
            problems.append(f"checksum mismatch for {entry['file']}")
    return problems


def missing_expected_files(dataset_dir: Path) -> list[str]:
    return [name for name in EXPECTED_FILES if not (dataset_dir / name).exists()]


def download(dataset_dir: Path = RAW_DATASET_DIR, slug: str = DATASET_SLUG, force: bool = False) -> dict:
    dataset_dir.mkdir(parents=True, exist_ok=True)
    if not force and not missing_expected_files(dataset_dir):
        log.info("dataset already present; skipping download", extra={"context": {"dir": str(dataset_dir)}})
        if not (dataset_dir / MANIFEST_NAME).exists():
            return write_manifest(dataset_dir, slug)
        problems = verify_manifest(dataset_dir)
        if problems:
            raise DownloadError(f"existing files do not match manifest: {problems}; rerun with --force")
        return json.loads((dataset_dir / MANIFEST_NAME).read_text(encoding="utf-8"))

    kaggle = shutil.which("kaggle")
    if kaggle is None:
        raise DownloadError("Kaggle CLI not found. Run `uv sync` and use `uv run ...`.")
    cmd = [kaggle, "datasets", "download", "-d", slug, "-p", str(dataset_dir), "--unzip", "--force"]
    log.info("downloading dataset", extra={"context": {"slug": slug, "dir": str(dataset_dir)}})
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise DownloadError(
            "Kaggle download failed (exit %d): %s\nIf this is an authentication error, configure "
            "a Kaggle API token (see README.md) and rerun." % (result.returncode, result.stderr.strip())
        )
    missing = missing_expected_files(dataset_dir)
    if missing:
        raise DownloadError(f"download finished but expected files are missing: {missing}")
    manifest = write_manifest(dataset_dir, slug)
    log.info("download complete", extra={"context": {"files": [f["file"] for f in manifest["files"]]}})
    return manifest


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--force", action="store_true", help="re-download even if files exist")
    args = ap.parse_args()
    try:
        download(force=args.force)
    except DownloadError:
        log.exception("download failed")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
