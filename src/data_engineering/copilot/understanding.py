"""Deterministic question understanding: intents, assets, time window, follow-up references.

This is the authority for anything checkable: asset ids must exist in the operator's scope, time windows
must be within bounds. An optional LLM plan (QueryPlan) can add intents and resolve references, but its
assets and windows are re-validated here. Ambiguity produces a clarification question, not a guess.

Time basis: the telemetry is simulated with its own event time, so "last 2 hours" means the 2 hours
ending at the newest reading of the run (or at the previous turn's window end for follow-ups), not the
wall clock. The response states this interpretation.
"""

import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta

KEYWORDS = {
    "forecast": ("forecast", "predict", "will it", "will the", "next 5", "next 10", "going to", "expected to"),
    "comparison": ("compare", "comparison", "versus", " vs ", " vs.", "difference between", "warmer than",
                   "hotter than", "cooler than", "higher than", "lower than"),
    "trend": ("trend", "rising", "increasing", "falling", "decreasing", "over time", "climbing", "dropping"),
    "recommendations": ("recommendation", "recommend", "suggestion", "advice"),
    "energy": ("energy", "power", "pue", "waste", " kw", "consumption", "efficien"),
    "anomaly": ("anomal", "airflow", "abnormal", "fault", "issue", "problem", "unusual", "sensor", "stale"),
    "maintenance_priority": ("investigate first", "priorit", "maintenance", "inspect", "look at first"),
    "cooling_setting": ("fan speed to", "set the fan", "setpoint", " rpm", "reduce the fan", "increase the fan",
                        "lower the fan", "cooling setting", "what if"),
    "explanation": ("why", "cause", "reason", "explain", "what is driving"),
    "workload": ("workload", "utilization", "utilisation", "cpu", "gpu", "load"),
    "thermal": ("temperature", "inlet", "exhaust", "thermal", "headroom", "hot", "heat", "cooling"),
    "status": ("status", "current", "latest", "right now", "overview", "how is", "how are", "state of"),
    "history": ("history", "historical", "earlier", "previous", "past", "last"),
}
ALL_ASSET_WORDS = ("which assets", "which servers", "all assets", "all servers", "any assets", "any servers",
                   "every asset", "every server", "across assets", "fleet", "data center", "datacenter")
CONTEXT_WORDS = ("it", "its", "this asset", "that asset", "this server", "that server", "same asset", "same server",
                 "them", "these", "those", "the asset", "the server")
TOPOLOGY_WORDS = ("cooling zone", "zone", "room", "aisle", "rack", "hall", "row")
_UNITS = {"m": 1, "min": 1, "mins": 1, "minute": 1, "minutes": 1, "h": 60, "hr": 60, "hrs": 60, "hour": 60,
          "hours": 60, "d": 1440, "day": 1440, "days": 1440}
_WINDOW = re.compile(r"\b(?:last|past|previous|prior|over the)\s+(\d{1,4})\s*(minutes?|mins?|m|hours?|hrs?|h|days?|d)\b",
                     re.IGNORECASE)
_WINDOW_ONE = re.compile(r"\b(?:last|past|previous|prior)\s+(hour|day|minute)\b", re.IGNORECASE)
_RPM = re.compile(r"\b(\d{3,5}(?:\.\d+)?)\s*rpm\b", re.IGNORECASE)
_ISO = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}(?::\d{2})?(?:Z|[+-]\d{2}:\d{2})")


@dataclass
class Understanding:
    intents: list[str]
    asset_ids: list[str]
    all_assets: bool
    window_minutes: int | None
    explicit_start: datetime | None
    explicit_end: datetime | None
    candidate_settings: list[float]
    from_context: list[str] = field(default_factory=list)
    clarification: str | None = None
    notes: list[str] = field(default_factory=list)


def _words(text: str) -> str:
    return f" {re.sub(r'[^a-z0-9_.:+-]+', ' ', text.lower())} "


def find_assets(text: str, known: list[str]) -> tuple[list[str], list[str]]:
    """-> (asset ids found, unresolved short references like 'asset 13')."""
    low = text.lower()
    found = [a for a in known if re.search(rf"(?<![\w-]){re.escape(a.lower())}(?![\w-])", low)]
    unresolved = []
    for m in re.finditer(r"\b(?:asset|server|node)\s+#?(\d{1,4})\b", low):
        n = int(m.group(1))
        matches = [a for a in known if re.search(rf"-0*{n}$", a)]
        if len(matches) == 1 and matches[0] not in found:
            found.append(matches[0])
        elif not matches or len(matches) > 1:
            unresolved.append(m.group(0))
    return found, unresolved


def understand(question: str, known_assets: list[str], previous=None, requested_asset: str | None = None,
               max_window_minutes: int = 1440) -> Understanding:
    q = question.strip()
    w = _words(q)
    intents = [i for i, keys in KEYWORDS.items() if any(k in w or k in q.lower() for k in keys)]
    assets, unresolved = find_assets(q, known_assets)
    if requested_asset and requested_asset not in assets:
        assets.insert(0, requested_asset)
    all_assets = any(k in q.lower() for k in ALL_ASSET_WORDS)
    from_context, notes, clarification = [], [], None

    window = None
    if m := _WINDOW.search(q):
        window = int(m.group(1)) * _UNITS[m.group(2).lower()]
    elif m := _WINDOW_ONE.search(q):
        window = _UNITS[m.group(1).lower()]
    stamps = [datetime.fromisoformat(s.replace("Z", "+00:00")) for s in _ISO.findall(q)]
    start = min(stamps) if len(stamps) >= 2 else None
    end = max(stamps) if stamps else None
    if window is not None and window > max_window_minutes:
        notes.append(f"requested window of {window} minutes exceeds the maximum of {max_window_minutes}; "
                     f"using {max_window_minutes}")
        window = max_window_minutes

    refers_back = any(f" {k} " in w for k in CONTEXT_WORDS) or q.lower().startswith(("what about", "and ", "how about"))
    if previous is not None and refers_back:
        if not assets and not all_assets:
            assets, all_assets = list(previous.asset_ids), previous.all_assets
            if assets or all_assets:
                from_context.append("assets")
        if not intents or intents == ["history"]:
            intents = [i for i in previous.intents if i != "history"] + intents
            from_context.append("intents")
        if window is None and end is None and previous.start_time and previous.end_time:
            from_context.append("time window")
    if any(f" {k} " in w for k in TOPOLOGY_WORDS):
        clarification = ("The telemetry has no room, zone, aisle or rack topology, so I can't tell which assets "
                         "belong to that area. Which asset ids should I use?")
    elif "other" in w and len(assets) <= 1 and not all_assets and ("compare" in w or "comparison" in intents):
        clarification = ("Which other asset should I compare with? Name an asset id"
                         + (f" (currently: {assets[0]})." if assets else "."))
    elif unresolved:
        clarification = f"I couldn't match {', '.join(unresolved)} to exactly one asset id. Which asset do you mean?"
    if not intents:
        intents = ["status"]
    if "comparison" in intents and len(assets) < 2 and not all_assets and clarification is None:
        clarification = "Which assets should I compare? Name at least two asset ids, or ask about all assets."
    needs_asset = {"forecast", "cooling_setting", "explanation"} & set(intents)
    if needs_asset and not assets and not all_assets and clarification is None:
        clarification = "Which asset do you mean? Name an asset id (for example " \
                        f"{known_assets[0] if known_assets else 'SIM-NA-001'})."
    return Understanding(intents=intents, asset_ids=assets, all_assets=all_assets, window_minutes=window,
                         explicit_start=start, explicit_end=end,
                         candidate_settings=[float(x) for x in _RPM.findall(q)],
                         from_context=from_context, clarification=clarification, notes=notes)


def resolve_window(u: Understanding, previous, data_end: datetime, default_minutes: int,
                   max_minutes: int) -> tuple[datetime, datetime, str]:
    """-> (start, end, basis). Windows end at the newest reading unless the question gives times."""
    if u.explicit_start and u.explicit_end:
        start, end, basis = u.explicit_start, u.explicit_end, "explicit timestamps in the question"
    elif u.explicit_end:
        start, end = u.explicit_end - timedelta(minutes=u.window_minutes or default_minutes), u.explicit_end
        basis = "explicit end time in the question"
    elif u.window_minutes:
        start, end = data_end - timedelta(minutes=u.window_minutes), data_end
        basis = f"the {u.window_minutes} minutes ending at the newest reading (simulated event time)"
    elif previous is not None and "time window" in u.from_context:
        start = datetime.fromisoformat(previous.start_time)
        end = datetime.fromisoformat(previous.end_time)
        basis = "same window as the previous question"
    else:
        start, end = data_end - timedelta(minutes=default_minutes), data_end
        basis = f"default: the {default_minutes} minutes ending at the newest reading (simulated event time)"
    if end > data_end:
        end = data_end
    if (end - start) > timedelta(minutes=max_minutes):
        start = end - timedelta(minutes=max_minutes)
        basis += f" (clamped to the maximum of {max_minutes} minutes)"
    return start, end, basis
