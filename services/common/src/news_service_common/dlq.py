from __future__ import annotations

import argparse
import time
from typing import Any, Literal

from news_platform.config import get_topic_key, load_settings
from news_platform.contracts.events import EVENT_CONTRACTS, EVENT_TOPIC_KEYS, BaseEvent, NewsDlq

from news_service_common.errors import IngestionError
from news_service_common.events import ConsumedEvent, JsonEventConsumer, JsonEventPublisher
from news_service_common.runtime import elapsed_ms, handle_unconsumed_error
from news_service_common.stages import run_stage
from news_service_common.telemetry import log_event

SERVICE_NAME = "dlq_operator"
DlqAction = Literal["inspect", "replay", "commit"]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Inspect, replay, or commit Redpanda DLQ events.")
    parser.add_argument("--action", choices=["inspect", "replay", "commit"], default="inspect")
    parser.add_argument("--group-id", default="dlq_operator")
    parser.add_argument("--poll-timeout-seconds", type=float, default=10.0)
    parser.add_argument("--max-events", type=int, default=1)
    parser.add_argument("--reason", help="Required disposition reason for replay or commit.")
    parser.add_argument("--operator-id", default="dlq_operator")
    args = parser.parse_args()
    if args.action in {"replay", "commit"} and not args.reason:
        parser.error("--reason is required for replay or commit")
    return args


def run() -> int:
    args = parse_args()
    started_at = time.perf_counter()
    try:
        config = run_stage("config_load", False, load_settings)
        consumer = run_stage(
            "event_bus_connect",
            True,
            lambda: JsonEventConsumer(config, group_id=args.group_id),
        )
        publisher = (
            run_stage("event_bus_connect", True, lambda: JsonEventPublisher(config))
            if args.action == "replay"
            else None
        )
    except Exception as error:
        return handle_unconsumed_error(
            service_name=SERVICE_NAME,
            failure_event="dlq_operator_failed",
            error=error,
            started_at=started_at,
            group_id=args.group_id,
            action=args.action,
        )

    try:
        processed_count = 0
        while processed_count < args.max_events:
            consumed: ConsumedEvent | None = None
            try:
                consumed = run_stage(
                    "event_consume",
                    True,
                    lambda: consumer.consume_one("dlq", timeout_seconds=args.poll_timeout_seconds),
                )
                if consumed is None:
                    log_event(
                        SERVICE_NAME,
                        "dlq_empty",
                        action=args.action,
                        processed_count=processed_count,
                        duration_ms=elapsed_ms(started_at),
                    )
                    return 0
                process_dlq_event(
                    action=args.action,
                    config=config,
                    consumed=consumed,
                    consumer=consumer,
                    publisher=publisher,
                    reason=args.reason,
                    operator_id=args.operator_id,
                )
            except Exception as error:
                offset_fields = (
                    {
                        "topic": consumed.topic,
                        "partition": consumed.partition,
                        "offset": consumed.offset,
                    }
                    if consumed
                    else {}
                )
                return handle_unconsumed_error(
                    service_name=SERVICE_NAME,
                    failure_event="dlq_operator_failed",
                    error=error,
                    started_at=started_at,
                    action=args.action,
                    **offset_fields,
                )
            processed_count += 1
        log_event(
            SERVICE_NAME,
            "dlq_operator_completed",
            action=args.action,
            processed_count=processed_count,
            duration_ms=elapsed_ms(started_at),
        )
        return 0
    finally:
        consumer.close()


def process_dlq_event(
    *,
    action: DlqAction,
    config: dict[str, Any],
    consumed: ConsumedEvent,
    consumer: JsonEventConsumer,
    publisher: JsonEventPublisher | None,
    reason: str | None = None,
    operator_id: str = SERVICE_NAME,
) -> NewsDlq:
    dlq_event = run_stage(
        "event_decode",
        False,
        lambda: NewsDlq.model_validate(consumed.decode_value()),
    )
    common_fields = {
        "source_topic": dlq_event.source_topic,
        "source_partition": dlq_event.source_partition,
        "source_offset": dlq_event.source_offset,
        "error_class": dlq_event.error_class,
        "error_message": dlq_event.error_message,
        "topic": consumed.topic,
        "partition": consumed.partition,
        "offset": consumed.offset,
        "operator_id": operator_id,
    }
    if action == "inspect":
        log_event(SERVICE_NAME, "dlq_event_inspected", committed=False, **common_fields)
        return dlq_event
    if action == "commit":
        run_stage("event_commit", True, lambda: consumer.commit(consumed))
        log_event(
            SERVICE_NAME,
            "dlq_event_committed",
            committed=True,
            disposition_action="commit",
            disposition_reason=reason,
            **common_fields,
        )
        return dlq_event
    if publisher is None:
        raise RuntimeError("DLQ replay requires an event publisher")
    replayed_topic_key = replay_dlq_event(config=config, publisher=publisher, dlq_event=dlq_event)
    run_stage("event_commit", True, lambda: consumer.commit(consumed))
    log_event(
        SERVICE_NAME,
        "dlq_event_replayed",
        committed=True,
        disposition_action="replay",
        disposition_reason=reason,
        replayed_topic_key=replayed_topic_key,
        **common_fields,
    )
    return dlq_event


def replay_dlq_event(
    *,
    config: dict[str, Any],
    publisher: JsonEventPublisher,
    dlq_event: NewsDlq,
) -> str:
    topic_key = get_topic_key(config, dlq_event.source_topic)
    if topic_key == "dlq":
        raise IngestionError(
            stage="dlq_replay",
            retryable=False,
            message="Refusing to replay a DLQ event back to the DLQ topic",
        )
    failed_event = decoded_failed_event(dlq_event)
    schema_version = failed_event.get("schema_version")
    expected_schema_version = EVENT_TOPIC_KEYS[topic_key]
    if schema_version != expected_schema_version:
        raise IngestionError(
            stage="dlq_replay",
            retryable=False,
            message=(
                "DLQ failed event schema does not match source topic: "
                f"{schema_version} != {expected_schema_version}"
            ),
        )
    event = validate_replay_event(schema_version, failed_event)
    run_stage("event_publish", True, lambda: publisher.publish(topic_key, event))
    run_stage("event_publish", True, publisher.flush)
    return topic_key


def decoded_failed_event(dlq_event: NewsDlq) -> dict[str, Any]:
    failed_event = dlq_event.payload.get("failed_event")
    if not isinstance(failed_event, dict):
        raise IngestionError(
            stage="dlq_replay",
            retryable=False,
            message="DLQ payload does not include failed_event",
        )
    if failed_event.get("encoding") != "json":
        raise IngestionError(
            stage="dlq_replay",
            retryable=False,
            message="Only JSON-decoded DLQ failed events can be replayed",
        )
    value = failed_event.get("value")
    if not isinstance(value, dict):
        raise IngestionError(
            stage="dlq_replay",
            retryable=False,
            message="DLQ failed_event.value must be a JSON object",
        )
    return value


def validate_replay_event(schema_version: Any, value: dict[str, Any]) -> BaseEvent:
    if not isinstance(schema_version, str) or schema_version not in EVENT_CONTRACTS:
        raise IngestionError(
            stage="dlq_replay",
            retryable=False,
            message=f"Unsupported DLQ failed event schema: {schema_version}",
        )
    return EVENT_CONTRACTS[schema_version].model_validate(value)
