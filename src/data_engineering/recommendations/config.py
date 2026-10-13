"""Settings: LLM provider (environment / .env) and workflow heuristics (config/recommendations.yaml)."""

import os
from dataclasses import dataclass
from typing import Literal
from pathlib import Path

import yaml
from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict

from data_engineering.energy.fan_peer import FanPeerConfig
from data_engineering.paths import REPO_ROOT

DEFAULT_CONFIG_PATH = REPO_ROOT / "config" / "recommendations.yaml"


class RecommendationConfigError(ValueError):
    pass


class LLMSettings(BaseSettings):
    """FIREWORKS_* variables. The key stays server-side; it is never logged or returned by the API."""
    model_config = SettingsConfigDict(env_file=REPO_ROOT / ".env", extra="ignore")

    fireworks_api_key: SecretStr | None = None
    fireworks_base_url: str = "https://api.fireworks.ai/inference/v1"
    fireworks_main_model: str = "accounts/fireworks/models/glm-5p3-flash"
    # The requested accounts/fireworks/models/deepseek-v4-flash-0731 returned 404 (not deployed) for
    # this account on 2026-10-13; deepseek-v4p1-flash is the DeepSeek V4 Flash release that is served.
    fireworks_subagent_model: str = "accounts/fireworks/models/deepseek-v4p1-flash"
    fireworks_timeout_s: float = Field(60.0, gt=0, le=300)
    fireworks_max_retries: int = Field(1, ge=0, le=5)        # transport retries per call; worst case
    #   per call is about (1 + retries) x timeout (a 3 x 60 s re-analysis was observed with 2)
    fireworks_max_tokens: int = Field(8192, ge=256, le=32768)   # both models reason before answering
    # low: bounded reasoning before the tool call (at 4096 tokens and default effort, both models often
    # exhausted the budget reasoning and never called the tool). GLM-5.3 rejects "none" (thinking-only).
    fireworks_reasoning_effort: Literal["low", "medium", "high"] = "low"
    agents_llm_enabled: bool = True                           # false: deterministic workflow, no LLM calls

    @property
    def llm_available(self) -> bool:
        return self.agents_llm_enabled and self.fireworks_api_key is not None \
            and bool(self.fireworks_api_key.get_secret_value())


@dataclass(frozen=True)
class WorkflowConfig:
    comparison_windows: int = 3
    maintenance_min_persistent_windows: int = 3
    history_deviation_z: float = 3.0
    max_reanalysis_attempts: int = 1
    fan_peer: FanPeerConfig = FanPeerConfig()
    min_elevated_windows: int = 2
    source: Path = DEFAULT_CONFIG_PATH


def load_workflow_config(path: Path | str | None = None) -> WorkflowConfig:
    path = Path(path or os.environ.get("RECOMMENDATIONS_CONFIG") or DEFAULT_CONFIG_PATH)
    try:
        raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except FileNotFoundError as exc:
        raise RecommendationConfigError(f"recommendations config not found: {path}") from exc
    except yaml.YAMLError as exc:
        raise RecommendationConfigError(f"invalid YAML in {path}: {exc}") from exc
    h, e, w = raw.get("history") or {}, raw.get("evidence") or {}, raw.get("workflow") or {}
    ew = dict(raw.get("energy_waste") or {})
    min_elevated = ew.pop("min_elevated_windows", 2)
    if "peer_group_by" in ew:
        ew["group_by"] = tuple(ew.pop("peer_group_by"))
    try:
        fan_peer = FanPeerConfig(**ew)
    except TypeError as exc:
        raise RecommendationConfigError(f"energy_waste: {exc}") from exc
    cfg = WorkflowConfig(comparison_windows=h.get("comparison_windows", 3),
                         maintenance_min_persistent_windows=e.get("maintenance_min_persistent_windows", 3),
                         history_deviation_z=e.get("history_deviation_z", 3.0),
                         max_reanalysis_attempts=w.get("max_reanalysis_attempts", 1), fan_peer=fan_peer,
                         min_elevated_windows=min_elevated, source=path)
    problems = []
    if not isinstance(cfg.comparison_windows, int) or not 1 <= cfg.comparison_windows <= 24:
        problems.append("history.comparison_windows must be an integer in [1, 24]")
    if not isinstance(cfg.maintenance_min_persistent_windows, int) or \
            not 2 <= cfg.maintenance_min_persistent_windows <= (cfg.comparison_windows or 0) + 1:
        problems.append("evidence.maintenance_min_persistent_windows must be in [2, comparison_windows + 1]")
    if not isinstance(cfg.history_deviation_z, (int, float)) or cfg.history_deviation_z <= 0:
        problems.append("evidence.history_deviation_z must be > 0")
    if not isinstance(cfg.max_reanalysis_attempts, int) or not 0 <= cfg.max_reanalysis_attempts <= 3:
        problems.append("workflow.max_reanalysis_attempts must be an integer in [0, 3]")
    fp = cfg.fan_peer
    if not set(fp.group_by) <= {"cooling_type", "asset_type"} or not fp.group_by:
        problems.append("energy_waste.peer_group_by must be a non-empty subset of [cooling_type, asset_type]")
    if not (0 < fp.workload_tolerance <= 1 and fp.inlet_tolerance_c > 0 and fp.min_excess_fraction > 0
            and 50 <= fp.reference_percentile < 100 and fp.min_reference_windows >= 1
            and fp.min_reference_assets >= 1 and 15 <= fp.lookback_minutes <= 10080):
        problems.append("energy_waste: tolerances/thresholds out of range")
    if not isinstance(cfg.min_elevated_windows, int) or cfg.min_elevated_windows < 1:
        problems.append("energy_waste.min_elevated_windows must be a positive integer")
    if problems:
        raise RecommendationConfigError("; ".join(problems))
    return cfg
