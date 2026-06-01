from __future__ import annotations

from typing import Any


class IngestionError(RuntimeError):
    def __init__(
        self,
        *,
        stage: str,
        retryable: bool,
        message: str,
        error_class: str | None = None,
    ) -> None:
        super().__init__(message)
        self.stage = stage
        self.retryable = retryable
        self.error_class = error_class or type(self).__name__


class HttpFetchError(IngestionError):
    def __init__(
        self,
        message: str,
        *,
        stage: str,
        retryable: bool,
        status_code: int | None = None,
        error_class: str | None = None,
    ) -> None:
        super().__init__(
            stage=stage,
            retryable=retryable,
            message=message,
            error_class=error_class,
        )
        self.status_code = status_code


class FeedFetchError(HttpFetchError):
    def __init__(
        self,
        message: str,
        *,
        retryable: bool,
        status_code: int | None = None,
        error_class: str | None = None,
    ) -> None:
        super().__init__(
            message,
            stage="feed_fetch",
            retryable=retryable,
            status_code=status_code,
            error_class=error_class,
        )


def wrap_error(stage: str, retryable: bool, error: Exception) -> IngestionError:
    if isinstance(error, IngestionError):
        return error
    return IngestionError(
        stage=stage,
        retryable=retryable,
        message=str(error),
        error_class=type(error).__name__,
    )


def error_fields(error: Exception) -> dict[str, Any]:
    if isinstance(error, IngestionError):
        fields: dict[str, Any] = {
            "stage": error.stage,
            "retryable": error.retryable,
            "error_class": error.error_class,
            "error_message": str(error),
        }
        if isinstance(error, HttpFetchError) and error.status_code is not None:
            fields["status_code"] = error.status_code
        return fields
    return {
        "stage": "unexpected",
        "retryable": False,
        "error_class": type(error).__name__,
        "error_message": str(error),
    }
