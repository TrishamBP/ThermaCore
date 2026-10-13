"""Builds the copilot engine once (lifespan) on top of the existing services."""

from app.cooling.service import CoolingModelService
from data_engineering.copilot.graph import CopilotEngine
from data_engineering.copilot.memory import ConversationStore
from data_engineering.copilot.tools import CopilotTools


def build_copilot(app_state, main_llm, sub_llm) -> CopilotEngine:
    settings, factory, registry = app_state.settings, app_state.store_factory, app_state.cooling_model_registry

    def read_only():
        return factory.open(read_only=True)

    def with_store(fn):
        store = read_only()
        try:
            return fn(store)
        finally:
            store.close()

    def availability():
        return CoolingModelService(None, registry).availability(settings.model_max_age_days)

    def tools_factory(scope):
        return CopilotTools(store_factory=read_only, scope=scope, cooling_config=app_state.cooling_config,
                            model_provider=lambda: registry.model, model_availability=availability,
                            model_settings=registry.settings, workflow=app_state.recommendation_engine.workflow,
                            sub_llm=sub_llm, max_points=settings.max_telemetry_points)

    return CopilotEngine(
        tools_factory=tools_factory,
        known_assets=lambda run_id: with_store(lambda s: [a["asset_id"] for a in s.run_assets(run_id)]),
        data_end=lambda run_id: with_store(lambda s: max((a["last_event_timestamp"] for a in s.run_assets(run_id)),
                                                         default=None)),
        latest_run=lambda: with_store(lambda s: s.latest_run_id()),
        memory=ConversationStore(settings.copilot_max_conversations, settings.copilot_max_turns,
                                 settings.copilot_conversation_ttl_s),
        main_llm=main_llm, sub_llm=sub_llm, default_window_minutes=settings.dashboard_trend_minutes,
        max_window_minutes=settings.max_telemetry_range_hours * 60)
