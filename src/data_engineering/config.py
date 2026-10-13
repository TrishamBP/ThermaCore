"""Load config/simulation.yaml."""

from pathlib import Path

import yaml

from data_engineering.paths import CONFIG_PATH


def load_config(path: Path | None = None) -> dict:
    with open(path or CONFIG_PATH, encoding="utf-8") as fh:
        return yaml.safe_load(fh)


def region_filter_name(config: dict) -> str:
    return f"datacenter_region={config['scenario']['region_code']}"
