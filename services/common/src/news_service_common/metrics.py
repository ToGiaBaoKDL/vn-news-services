from __future__ import annotations

from typing import Any

from news_service_common.events import JsonEventPublisher, make_pipeline_metric_event
from news_service_common.telemetry import log_event


def publish_pipeline_metric_safely(
    *,
    service_name: str,
    publisher: JsonEventPublisher,
    metric_name: str,
    metric_value: int | float = 1,
    metric_unit: str = "count",
    dimensions: dict[str, Any] | None = None,
    timeout_seconds: float = 2.0,
) -> None:
    try:
        publisher.publish(
            "pipeline_metric",
            make_pipeline_metric_event(
                service=service_name,
                metric_name=metric_name,
                metric_value=metric_value,
                metric_unit=metric_unit,
                dimensions=dimensions,
            ),
        )
        publisher.flush(timeout_seconds)
    except Exception as error:
        log_event(
            service_name,
            "pipeline_metric_publish_failed",
            level="warning",
            metric_name=metric_name,
            error_class=type(error).__name__,
            error_message=str(error),
        )
