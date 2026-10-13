"""Run one recommendation analysis from the command line (recommendation-only; prints JSON).

    uv run python -m data_engineering.recommendations.cli --asset SIM-NA-013 --run sim-c9944cc35156 \
        --as-of 2026-01-01T13:45:00Z [--no-llm]
"""

import argparse
import json
import sys
from datetime import datetime

from data_engineering.cooling.config import load_cooling_config
from data_engineering.cooling.estimator import UnknownScopeError
from data_engineering.cooling_model.artifacts import InvalidModelArtifactError, ModelNotTrainedError
from data_engineering.cooling_model.config import load_settings
from data_engineering.cooling_model.predictor import CoolingResponseModel
from data_engineering.recommendations.config import LLMSettings, load_workflow_config
from data_engineering.recommendations.graph import RecommendationEngine
from data_engineering.recommendations.llm import build_llms
from data_engineering.storage.base import open_store
from data_engineering.streaming.settings import load_consumer_settings


def build_engine(no_llm: bool = False) -> RecommendationEngine:
    store_settings, model_settings = load_consumer_settings(), load_settings()
    try:
        model = CoolingResponseModel.load(model_settings.model.artifact_dir)
    except (ModelNotTrainedError, InvalidModelArtifactError) as exc:
        print(f"warning: no cooling model ({exc}); forecasts unavailable", file=sys.stderr)
        model = None
    main_llm, sub_llm = (None, None) if no_llm else build_llms(LLMSettings())

    def store_factory():
        store = open_store(store_settings, sqlite_check_same_thread=False)
        store.check_initialized()
        return store

    return RecommendationEngine(store_factory=store_factory, cooling_config=load_cooling_config(),
                                model_provider=lambda: model, model_settings=model_settings,
                                workflow=load_workflow_config(), main_llm=main_llm, sub_llm=sub_llm)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Run the multi-agent cooling recommendation workflow for one asset.")
    ap.add_argument("--asset", required=True)
    ap.add_argument("--run", help="simulation_run_id (default: the most recently updated run)")
    ap.add_argument("--as-of", help="window end in event time, ISO 8601 with timezone (default: newest reading)")
    ap.add_argument("--no-llm", action="store_true", help="deterministic workflow without LLM calls")
    args = ap.parse_args(argv)
    as_of = datetime.fromisoformat(args.as_of.replace("Z", "+00:00")) if args.as_of else None
    if as_of is not None and as_of.tzinfo is None:
        ap.error("--as-of must include a timezone")
    try:
        result = build_engine(args.no_llm).analyze(args.asset, args.run, as_of)
    except UnknownScopeError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    sys.stdout.reconfigure(encoding="utf-8")         # Windows consoles default to cp1252 (no °, …)
    print(json.dumps(result, indent=2, default=str, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
