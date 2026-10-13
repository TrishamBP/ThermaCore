"""Copilot route (mounted under /api/v1)."""

from typing import Any

from fastapi import APIRouter, Request
from pydantic import Field, model_validator

from app.common.schemas.base import CustomModel, UtcDatetime
from app.common.schemas.responses import ErrorResponse
from app.common.security.auth import OperatorScope
from app.telemetry.constants import ID_PATTERN

router = APIRouter(prefix="/copilot", tags=["copilot"])


class TimeRange(CustomModel):
    start: UtcDatetime | None = None
    end: UtcDatetime | None = None

    @model_validator(mode="after")
    def _order(self):
        if self.start and self.end and self.start >= self.end:
            raise ValueError("time_range.start must be before time_range.end")
        return self


class ChatRequest(CustomModel):
    conversation_id: str | None = Field(None, pattern=r"^conv-[0-9a-f]{20}$")
    message: str = Field(..., min_length=1, max_length=2000)
    asset_id: str | None = Field(None, pattern=ID_PATTERN, description="Optional asset context")
    run_id: str | None = Field(None, pattern=ID_PATTERN)
    time_range: TimeRange | None = None


class ChatResponse(CustomModel):
    conversation_id: str
    answer: str
    clarification_needed: bool
    interpretation: dict[str, Any] | None
    sections: dict[str, list[str]]
    evidence: list[dict[str, Any]]
    metrics: dict[str, Any]
    predictions: list[dict[str, Any]]
    recommendations: list[dict[str, Any]]
    optimizer: dict[str, Any] | None
    warnings: list[str]
    agent_execution_summary: dict[str, Any]
    read_only: bool
    automatic_control_executed: bool
    units_note: str
    unavailable_measurements: dict[str, str]


@router.post("/chat", response_model=ChatResponse,
             responses={403: {"model": ErrorResponse}, 422: {"model": ErrorResponse}, 503: {"model": ErrorResponse}},
             summary="Ask a question about telemetry, forecasts and recommendations (read-only)")
def chat(body: ChatRequest, request: Request, scope: OperatorScope) -> dict:
    tr = body.time_range or TimeRange()
    return request.app.state.copilot.chat(body.message, scope=scope, conversation_id=body.conversation_id,
                                          asset_id=body.asset_id, run_id=body.run_id, start_time=tr.start,
                                          end_time=tr.end)


