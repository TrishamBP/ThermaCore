"""Structured LLM calls through LangChain's ChatFireworks (function calling, include_raw for usage).

Every call has a timeout, bounded transport retries (ChatFireworks max_retries) and one bounded
re-ask when the output does not match the schema. Failures raise LLMError with a kind
(timeout | provider | parse); callers decide the fallback. Prompts, outputs and keys are never logged.
"""

import time
from dataclasses import asdict, dataclass
from typing import Protocol, TypeVar

from pydantic import BaseModel

from data_engineering.recommendations.config import LLMSettings

T = TypeVar("T", bound=BaseModel)


class LLMError(RuntimeError):
    def __init__(self, kind: str, message: str) -> None:
        super().__init__(message)
        self.kind = kind


@dataclass
class LLMCall:
    model: str
    latency_ms: float
    attempts: int
    input_tokens: int | None = None
    output_tokens: int | None = None
    total_tokens: int | None = None

    def to_dict(self) -> dict:
        return asdict(self)


class StructuredLLM(Protocol):
    model: str

    def invoke(self, schema: type[T], system: str, user: str) -> tuple[T, LLMCall]: ...


def langchain_base_url(url: str) -> str:
    """ChatFireworks appends /v1 itself; FIREWORKS_BASE_URL is the OpenAI-compatible .../inference/v1."""
    return url.rstrip("/").removesuffix("/v1")


def _error_summary(err, usage: dict) -> str:
    """Error type and pydantic error locations only (never model content)."""
    errors = getattr(err, "errors", None)
    if callable(errors):
        try:
            locs = sorted({".".join(str(x) for x in e.get("loc", ())) + ":" + e.get("type", "") for e in errors()})
            return f"{type(err).__name__}: {', '.join(locs)[:300]}"
        except Exception:
            pass
    return f"{type(err).__name__ if err else 'no tool call'}; output_tokens={usage.get('output_tokens')}"


def _classify(exc: Exception) -> str:
    name = type(exc).__name__.lower()
    return "timeout" if "timeout" in name or "timedout" in name else "provider"


class FireworksStructuredLLM:
    def __init__(self, model: str, settings: LLMSettings, parse_retries: int = 1) -> None:
        from langchain_fireworks import ChatFireworks
        if settings.fireworks_api_key is None:
            raise LLMError("disabled", "FIREWORKS_API_KEY is not set")
        self.model, self.parse_retries = model, parse_retries
        self._chat = ChatFireworks(model=model, api_key=settings.fireworks_api_key.get_secret_value(),
                                   base_url=langchain_base_url(settings.fireworks_base_url),
                                   timeout=settings.fireworks_timeout_s, max_retries=settings.fireworks_max_retries,
                                   max_tokens=settings.fireworks_max_tokens, temperature=0,
                                   model_kwargs={"reasoning_effort": settings.fireworks_reasoning_effort})

    def invoke(self, schema: type[T], system: str, user: str) -> tuple[T, LLMCall]:
        from langchain_core.messages import HumanMessage, SystemMessage
        runnable = self._chat.with_structured_output(schema, method="function_calling", include_raw=True)
        started, messages = time.monotonic(), [SystemMessage(system), HumanMessage(user)]
        usage = {"input_tokens": 0, "output_tokens": 0, "total_tokens": 0}
        for attempt in range(1, self.parse_retries + 2):
            try:
                out = runnable.invoke(messages)
            except Exception as exc:                  # provider SDK / network errors
                raise LLMError(_classify(exc), f"{self.model}: {type(exc).__name__}") from exc
            meta = getattr(out.get("raw"), "usage_metadata", None) or {}
            for k in usage:
                usage[k] += int(meta.get(k) or 0)
            if out.get("parsed") is not None:
                return out["parsed"], LLMCall(self.model, round((time.monotonic() - started) * 1000, 1), attempt,
                                              **usage)
            last_error = _error_summary(out.get("parsing_error"), meta)
            messages = [*messages, HumanMessage(
                "Your previous answer did not match the required schema "
                f"({str(out.get('parsing_error'))[:400]}). Call the tool again with valid arguments only.")]
        raise LLMError("parse", f"{self.model}: output did not match {schema.__name__} after "
                                f"{self.parse_retries + 1} attempts ({last_error})")


def build_llms(settings: LLMSettings) -> tuple[StructuredLLM | None, StructuredLLM | None]:
    """(main, subagent) or (None, None) when LLMs are disabled or no key is configured."""
    if not settings.llm_available:
        return None, None
    return (FireworksStructuredLLM(settings.fireworks_main_model, settings),
            FireworksStructuredLLM(settings.fireworks_subagent_model, settings))
