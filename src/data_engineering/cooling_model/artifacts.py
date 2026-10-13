"""Versioned model artifacts: XGBoost native .ubj files + JSON metadata (no pickle).

    <artifact_dir>/<version>/model_h5.ubj, model_h10.ubj, metadata.json
    <artifact_dir>/ACTIVE                     # name of the version in use

A version is written to a temporary directory and renamed when complete; ACTIVE is replaced
atomically last, so a crashed or partial training run never becomes the active model.
"""

import json
import os
import shutil
from pathlib import Path

import xgboost as xgb

ARTIFACT_SCHEMA_VERSION = 1
REQUIRED_METADATA = ("artifact_schema_version", "version", "created_at", "horizons_minutes", "feature_names",
                     "feature_config", "categories", "feature_ranges", "model_label", "validity", "evaluation",
                     "training_data", "xgboost_version")


class ModelNotTrainedError(LookupError):
    pass


class InvalidModelArtifactError(RuntimeError):
    pass


def save_artifact(artifact_dir: Path, version: str, boosters: dict[int, xgb.Booster], metadata: dict) -> Path:
    artifact_dir.mkdir(parents=True, exist_ok=True)
    final = artifact_dir / version
    tmp = artifact_dir / f".tmp-{version}"
    if tmp.exists():
        shutil.rmtree(tmp)
    tmp.mkdir()
    for h, booster in boosters.items():
        booster.save_model(str(tmp / f"model_h{h}.ubj"))
    (tmp / "metadata.json").write_text(json.dumps(metadata, indent=2, default=str), encoding="utf-8")
    if final.exists():
        shutil.rmtree(final)
    tmp.rename(final)
    pointer_tmp = artifact_dir / "ACTIVE.tmp"
    pointer_tmp.write_text(version, encoding="utf-8")
    os.replace(pointer_tmp, artifact_dir / "ACTIVE")
    return final


def active_version(artifact_dir: Path) -> str | None:
    pointer = artifact_dir / "ACTIVE"
    return pointer.read_text(encoding="utf-8").strip() if pointer.exists() else None


def load_artifact(artifact_dir: Path, version: str | None = None) -> tuple[dict[int, xgb.Booster], dict]:
    version = version or active_version(artifact_dir)
    if not version:
        raise ModelNotTrainedError(f"no trained cooling response model in {artifact_dir} "
                                   "(run `uv run python -m data_engineering.cooling_model.training`)")
    directory = artifact_dir / version
    try:
        metadata = json.loads((directory / "metadata.json").read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise InvalidModelArtifactError(f"artifact {version}: metadata.json missing") from exc
    except json.JSONDecodeError as exc:
        raise InvalidModelArtifactError(f"artifact {version}: metadata.json is not valid JSON") from exc
    missing = [k for k in REQUIRED_METADATA if k not in metadata]
    if missing:
        raise InvalidModelArtifactError(f"artifact {version}: metadata lacks {missing}")
    if metadata["artifact_schema_version"] != ARTIFACT_SCHEMA_VERSION:
        raise InvalidModelArtifactError(f"artifact {version}: schema version {metadata['artifact_schema_version']} "
                                        f"is not supported (expected {ARTIFACT_SCHEMA_VERSION})")
    if str(metadata["xgboost_version"]).split(".")[0] != xgb.__version__.split(".")[0]:
        raise InvalidModelArtifactError(f"artifact {version}: trained with xgboost {metadata['xgboost_version']}, "
                                        f"installed {xgb.__version__} (major version differs; retrain)")
    boosters = {}
    for h in metadata["horizons_minutes"]:
        path = directory / f"model_h{h}.ubj"
        if not path.exists():
            raise InvalidModelArtifactError(f"artifact {version}: {path.name} missing")
        booster = xgb.Booster()
        try:
            booster.load_model(str(path))
        except xgb.core.XGBoostError as exc:
            raise InvalidModelArtifactError(f"artifact {version}: {path.name} is not a valid XGBoost model") from exc
        if booster.num_features() != len(metadata["feature_names"]):
            raise InvalidModelArtifactError(f"artifact {version}: {path.name} expects {booster.num_features()} "
                                            f"features, metadata lists {len(metadata['feature_names'])}")
        boosters[int(h)] = booster
    return boosters, metadata
