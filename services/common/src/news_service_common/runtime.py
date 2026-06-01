from __future__ import annotations

import signal
import time
import traceback
from dataclasses import dataclass
from typing import Any

from news_platform.config import get_topic_name

from news_service_common.errors import IngestionError, error_fields
from news_service_common.events import (
    ConsumedEvent,
    JsonEventConsumer,
    JsonEventPublisher,
    make_dlq_event,
)
from news_service_common.telemetry import log_event


@dataclass
class ShutdownSignal:
    requested: bool = False

    def install(self) -> None:
        signal.signal(signal.SIGINT, self._request)
        signal.signal(signal.SIGTERM, self._request)

    def _request(self, signum: int, frame: Any) -> None:
        self.requested = True


def elapsed_ms(started_at: float) -> int:
    return round((time.perf_counter() - started_at) * 1000)


def handle_consumed_error(
    *,
    service_name: str,
    failure_event: str,
    dlq_event: str,
    config: dict[str, Any],
    publisher: JsonEventPublisher,
    consumer: JsonEventConsumer,
    consumed: ConsumedEvent | None,
    error: Exception,
    started_at: float,
) -> int:
    fields = error_fields(error)
    if not isinstance(error, IngestionError):
        fields["stack_trace"] = traceback.format_exc()
    if consumed and isinstance(error, IngestionError) and not error.retryable:
        event = make_dlq_event(
            source_topic=consumed.topic,
            source_partition=consumed.partition,
            source_offset=consumed.offset,
            error_class=fields["error_class"],
            error_message=fields["error_message"],
            payload={
                "service": service_name,
                "failed_event": consumed.payload_for_dlq(),
                "stage": fields["stage"],
            },
        )
        publisher.publish("dlq", event)
        publisher.flush()
        consumer.commit(consumed)
        log_event(
            service_name,
            dlq_event,
            level="error",
            duration_ms=elapsed_ms(started_at),
            dlq_topic=get_topic_name(config, "dlq"),
            **fields,
        )
        return 0
    log_event(
        service_name,
        failure_event,
        level="error",
        duration_ms=elapsed_ms(started_at),
        **fields,
    )
    return 1


def handle_unconsumed_error(
    *,
    service_name: str,
    failure_event: str,
    error: Exception,
    started_at: float,
    **extra_fields: Any,
) -> int:
    fields = error_fields(error)
    if not isinstance(error, IngestionError):
        fields["stack_trace"] = traceback.format_exc()
    log_event(
        service_name,
        failure_event,
        level="error",
        duration_ms=elapsed_ms(started_at),
        **extra_fields,
        **fields,
    )
    return 1
