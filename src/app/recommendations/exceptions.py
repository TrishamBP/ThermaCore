"""Recommendation domain exceptions (rendered as ErrorResponse by app.exceptions)."""

from app.exceptions import NotFoundError


class RecommendationNotFoundError(NotFoundError):
    code = "unknown_recommendation"


class AnalysisNotFoundError(NotFoundError):
    code = "unknown_analysis"


class AlertNotFoundError(NotFoundError):
    code = "unknown_alert"
