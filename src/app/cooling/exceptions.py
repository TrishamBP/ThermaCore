"""Cooling model domain exceptions (rendered as ErrorResponse by app.exceptions)."""

from app.exceptions import AppError, InvalidRequestError, ServiceUnavailableError


class ModelNotTrainedApiError(ServiceUnavailableError):
    code, message = "model_not_trained", "No cooling response model is trained"


class InvalidModelArtifactApiError(ServiceUnavailableError):
    code, message = "model_artifact_invalid", "The active cooling model artifact is invalid"


class TrainingDisabledError(AppError):
    status_code, code, message = 403, "training_disabled", "Model training over the API is disabled"


class TrainingInProgressError(AppError):
    status_code, code, message = 409, "training_in_progress", "A training run is already in progress"


class InsufficientDataError(InvalidRequestError):
    code = "insufficient_data"
