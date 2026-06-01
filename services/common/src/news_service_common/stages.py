from __future__ import annotations

from collections.abc import Callable

from news_service_common.errors import IngestionError, wrap_error


def run_stage[T](stage: str, retryable: bool, operation: Callable[[], T]) -> T:
    try:
        return operation()
    except IngestionError:
        raise
    except Exception as error:
        raise wrap_error(stage, retryable, error) from error
