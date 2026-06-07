from __future__ import annotations

import signal
import time
import traceback
from collections.abc import Callable
from dataclasses import dataclass
from random import Random
from typing import Any

from news_platform.config import get_topic_name

from news_service_common.errors import IngestionError, error_fields
from news_service_common.events import (
    ConsumedEvent,
    JsonEventConsumer,
    JsonEventPublisher,
    make_dlq_event,
)
from news_service_common.telemetry import log_event, log_metric

RETRYABLE_CONSUMED_ERROR = 2


@dataclass
class ShutdownSignal:
    requested: bool = False

    def install(self) -> None:
        signal.signal(signal.SIGINT, self._request)
        signal.signal(signal.SIGTERM, self._request)

    def _request(self, signum: int, frame: Any) -> None:
        self.requested = True


@dataclass
class ConsumedRetryBackoff:
    base_delay_seconds: float
    max_delay_seconds: float
    jitter_seconds: float
    sleep: Callable[[float], None] = time.sleep
    random: Random | None = None
    poll_seconds: float = 0.5

    def __post_init__(self) -> None:
        self.attempts: dict[tuple[str, int, int], int] = {}
        if self.random is None:
            self.random = Random()

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> ConsumedRetryBackoff:
        retry = config["event_bus"].get("consumer_retry", {})
        return cls(
            base_delay_seconds=float(retry.get("base_delay_seconds", 1)),
            max_delay_seconds=float(retry.get("max_delay_seconds", 60)),
            jitter_seconds=float(retry.get("jitter_seconds", 1)),
        )

    def reset(self, consumed: ConsumedEvent | None = None) -> None:
        if consumed is None:
            self.attempts.clear()
            return
        self.attempts.pop(self._key(consumed), None)

    def wait(
        self,
        *,
        service_name: str,
        consumed: ConsumedEvent,
        shutdown: ShutdownSignal | None,
    ) -> None:
        key = self._key(consumed)
        attempt = self.attempts.get(key, 0) + 1
        self.attempts[key] = attempt
        delay_seconds = self._delay_seconds(attempt)
        log_event(
            service_name,
            "consumed_event_retry_wait",
            level="warning",
            topic=consumed.topic,
            partition=consumed.partition,
            offset=consumed.offset,
            attempt=attempt,
            delay_seconds=round(delay_seconds, 3),
        )
        remaining_seconds = delay_seconds
        while remaining_seconds > 0:
            if shutdown and shutdown.requested:
                return
            interval_seconds = min(self.poll_seconds, remaining_seconds)
            self.sleep(interval_seconds)
            remaining_seconds -= interval_seconds

    def _delay_seconds(self, attempt: int) -> float:
        base_delay = min(
            self.max_delay_seconds,
            self.base_delay_seconds * (2 ** (attempt - 1)),
        )
        jitter = self.random.uniform(0, self.jitter_seconds) if self.jitter_seconds else 0
        return min(self.max_delay_seconds, base_delay + jitter)

    @staticmethod
    def _key(consumed: ConsumedEvent) -> tuple[str, int, int]:
        return (consumed.topic, consumed.partition, consumed.offset)


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
    retry_backoff: ConsumedRetryBackoff | None = None,
    shutdown: ShutdownSignal | None = None,
) -> int:
    fields = error_fields(error)
    if not isinstance(error, IngestionError):
        fields["stack_trace"] = traceback.format_exc()
    if consumed:
        fields.update(
            {
                "topic": consumed.topic,
                "partition": consumed.partition,
                "offset": consumed.offset,
            }
        )
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
        log_metric(
            service_name,
            "dlq_events_total",
            1,
            level="error",
            dlq_topic=get_topic_name(config, "dlq"),
            source_topic=consumed.topic,
            source_partition=consumed.partition,
            source_offset=consumed.offset,
        )
        return 0
    if consumed and isinstance(error, IngestionError) and error.retryable:
        consumer.seek(consumed)
        log_event(
            service_name,
            failure_event,
            level="error",
            duration_ms=elapsed_ms(started_at),
            **fields,
        )
        if retry_backoff:
            retry_backoff.wait(
                service_name=service_name,
                consumed=consumed,
                shutdown=shutdown,
            )
        return RETRYABLE_CONSUMED_ERROR
    log_event(
        service_name,
        failure_event,
        level="error",
        duration_ms=elapsed_ms(started_at),
        **fields,
    )
    return 1


def should_stop_after_process(result: int, *, once: bool) -> int | None:
    if once and result == RETRYABLE_CONSUMED_ERROR:
        return 1
    if once or (result and result != RETRYABLE_CONSUMED_ERROR):
        return result
    return None


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
