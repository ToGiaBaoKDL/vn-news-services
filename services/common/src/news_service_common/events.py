from __future__ import annotations

import base64
import json
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import httpx
from confluent_kafka import Consumer, KafkaError, KafkaException, Message, Producer, TopicPartition
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer
from confluent_kafka.serialization import MessageField, SerializationContext
from news_platform.config import get_topic_name
from news_platform.contracts.events import (
    EVENT_TOPIC_KEYS,
    ArticleExtracted,
    ArticleFetched,
    ArticleFetchRequested,
    BaseEvent,
    FeedItemDiscovered,
    NewsDlq,
    event_json_schema,
)
from news_platform.ids import make_stable_id

from news_service_common.errors import IngestionError


@dataclass(frozen=True)
class CachedSchemaIds:
    ids: frozenset[int]
    loaded_at: float


class UnexpectedSchemaIdError(ValueError):
    pass


@dataclass(frozen=True)
class ConsumedEvent:
    message: Message
    expected_schema_ids: frozenset[int] | None = None
    expected_schema_ids_loader: Callable[[], frozenset[int]] | None = None
    expected_schema_ids_refresher: Callable[[], frozenset[int]] | None = None

    @property
    def topic(self) -> str:
        return self.message.topic()

    @property
    def partition(self) -> int:
        return self.message.partition()

    @property
    def offset(self) -> int:
        return self.message.offset()

    def decode_value(self) -> dict[str, Any]:
        expected_schema_ids = self.resolve_expected_schema_ids()
        try:
            return decode_schema_registry_json(
                self.message.value(),
                expected_schema_ids=expected_schema_ids,
            )
        except UnexpectedSchemaIdError:
            if not self.expected_schema_ids_refresher:
                raise
            return decode_schema_registry_json(
                self.message.value(),
                expected_schema_ids=self.expected_schema_ids_refresher(),
            )

    def payload_for_dlq(self) -> dict[str, Any]:
        payload = self.message.value()
        try:
            return {"encoding": "json", "value": decode_schema_registry_json(payload)}
        except Exception:
            return {
                "encoding": "base64",
                "value": base64.b64encode(payload or b"").decode(),
            }

    def resolve_expected_schema_ids(self) -> frozenset[int] | None:
        if self.expected_schema_ids is not None:
            return self.expected_schema_ids
        if self.expected_schema_ids_loader is not None:
            return self.expected_schema_ids_loader()
        return None


class JsonEventPublisher:
    def __init__(self, config: dict[str, Any]) -> None:
        event_bus = config["event_bus"]
        self.config = config
        self.producer = Producer(
            {
                "bootstrap.servers": event_bus["bootstrap_servers"],
                "enable.idempotence": True,
            }
        )
        self.schema_registry = SchemaRegistryClient({"url": event_bus["schema_registry_url"]})
        self.serializers: dict[str, JSONSerializer] = {}
        self.delivery_errors: list[str] = []

    def publish(self, topic_key: str, event: BaseEvent) -> None:
        event_name = EVENT_TOPIC_KEYS[topic_key]
        topic = get_topic_name(self.config, topic_key)
        serializer = self.serializers.setdefault(
            event_name,
            JSONSerializer(
                json.dumps(event_json_schema(event_name), sort_keys=True),
                self.schema_registry,
                conf={"auto.register.schemas": False},
            ),
        )
        value = serializer(
            event.model_dump(mode="json"),
            SerializationContext(topic, MessageField.VALUE),
        )
        self.producer.produce(
            topic=topic,
            key=event_message_key(topic_key, event).encode(),
            value=value,
            on_delivery=self._on_delivery,
        )
        self.producer.poll(0)

    def flush(self) -> None:
        pending = self.producer.flush(10)
        delivery_errors, self.delivery_errors = self.delivery_errors, []
        if pending:
            msg = f"Timed out with {pending} pending Redpanda messages"
            raise RuntimeError(msg)
        if delivery_errors:
            msg = f"Redpanda delivery failed: {delivery_errors}"
            raise RuntimeError(msg)

    def _on_delivery(self, error: KafkaError | None, message: Any) -> None:
        if error:
            self.delivery_errors.append(str(error))


class JsonEventConsumer:
    def __init__(self, config: dict[str, Any], *, group_id: str) -> None:
        self.config = config
        self.subscribed_topic: str | None = None
        self.schema_registry_url = config["event_bus"]["schema_registry_url"].rstrip("/")
        self.schema_id_cache_ttl_seconds = float(
            config["event_bus"].get("schema_id_cache_ttl_seconds", 300)
        )
        self.schema_ids_by_subject: dict[str, CachedSchemaIds] = {}
        self.consumer = Consumer(
            {
                "bootstrap.servers": config["event_bus"]["bootstrap_servers"],
                "group.id": group_id,
                "enable.auto.commit": False,
                "auto.offset.reset": "earliest",
            }
        )

    def consume_one(self, topic_key: str, *, timeout_seconds: float) -> ConsumedEvent | None:
        topic = get_topic_name(self.config, topic_key)
        if topic != self.subscribed_topic:
            self.consumer.subscribe([topic])
            self.subscribed_topic = topic
        message = self.consumer.poll(timeout_seconds)
        if message is None:
            return None
        if message.error():
            raise KafkaException(message.error())
        subject = f"{topic}-value"
        return ConsumedEvent(
            message=message,
            expected_schema_ids_loader=lambda: self.schema_ids_for_subject(subject),
            expected_schema_ids_refresher=lambda: self.schema_ids_for_subject(
                subject,
                refresh=True,
            ),
        )

    def commit(self, event: ConsumedEvent) -> None:
        self.consumer.commit(event.message, asynchronous=False)

    def seek(self, event: ConsumedEvent) -> None:
        self.consumer.seek(TopicPartition(event.topic, event.partition, event.offset))

    def close(self) -> None:
        self.consumer.close()

    def schema_ids_for_subject(self, subject: str, *, refresh: bool = False) -> frozenset[int]:
        cached = self.schema_ids_by_subject.get(subject)
        if (
            refresh
            or cached is None
            or (time.monotonic() - cached.loaded_at) >= self.schema_id_cache_ttl_seconds
        ):
            self.schema_ids_by_subject[subject] = CachedSchemaIds(
                ids=fetch_subject_schema_ids(
                    self.schema_registry_url,
                    subject,
                ),
                loaded_at=time.monotonic(),
            )
        return self.schema_ids_by_subject[subject].ids


def fetch_subject_schema_ids(registry_url: str, subject: str) -> frozenset[int]:
    try:
        versions_response = httpx.get(
            f"{registry_url}/subjects/{subject}/versions",
            timeout=10,
        )
        versions_response.raise_for_status()
        versions = versions_response.json()
        if not isinstance(versions, list) or not versions:
            raise ValueError(f"Schema subject has no versions: {subject}")

        schema_ids = set()
        for version in versions:
            schema_response = httpx.get(
                f"{registry_url}/subjects/{subject}/versions/{version}",
                timeout=10,
            )
            schema_response.raise_for_status()
            schema_id = schema_response.json().get("id")
            if not isinstance(schema_id, int) or schema_id <= 0:
                raise ValueError(f"Invalid schema id for {subject} version {version}")
            schema_ids.add(schema_id)
    except httpx.HTTPError as error:
        raise IngestionError(
            stage="schema_registry",
            retryable=True,
            message=f"Schema Registry is unavailable for subject {subject}: {error}",
            error_class=type(error).__name__,
        ) from error

    return frozenset(schema_ids)


def decode_schema_registry_json(
    payload: bytes | None,
    *,
    expected_schema_ids: frozenset[int] | None = None,
) -> dict[str, Any]:
    if payload is None:
        raise ValueError("Cannot decode empty Redpanda message payload")
    if len(payload) < 5 or payload[0] != 0:
        raise ValueError("Expected Schema Registry JSON framing")
    schema_id = int.from_bytes(payload[1:5], byteorder="big")
    if schema_id <= 0:
        raise ValueError("Expected positive Schema Registry schema id")
    if expected_schema_ids is not None and schema_id not in expected_schema_ids:
        raise UnexpectedSchemaIdError(
            f"Unexpected Schema Registry schema id {schema_id}; "
            f"expected one of {sorted(expected_schema_ids)}"
        )
    data = payload[5:]
    value = json.loads(data.decode())
    if not isinstance(value, dict):
        raise ValueError("Expected decoded event payload to be a JSON object")
    return value


def event_json_bytes(event: BaseEvent) -> bytes:
    return json.dumps(
        event.model_dump(mode="json"),
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode()


def event_message_key(topic_key: str, event: BaseEvent) -> str:
    if isinstance(event, FeedItemDiscovered):
        return event.feed_item_id
    if isinstance(
        event,
        (
            ArticleFetchRequested,
            ArticleFetched,
            ArticleExtracted,
        ),
    ):
        return event.article_id
    if isinstance(event, NewsDlq):
        return (
            f"{event.source_topic}:"
            f"{'' if event.source_partition is None else event.source_partition}:"
            f"{'' if event.source_offset is None else event.source_offset}"
        )
    raise ValueError(f"Unsupported event type for topic {topic_key}: {type(event).__name__}")


def make_dlq_event(
    *,
    source_topic: str,
    source_partition: int | None,
    source_offset: int | None,
    error_class: str,
    error_message: str,
    payload: dict[str, Any],
) -> NewsDlq:
    event_id = make_stable_id(
        "event",
        "news.dlq.v1",
        source_topic,
        "" if source_partition is None else str(source_partition),
        "" if source_offset is None else str(source_offset),
        error_class,
        error_message,
    )
    return NewsDlq(
        schema_version="news.dlq.v1",
        event_id=event_id,
        event_time=datetime.now(UTC),
        source_topic=source_topic,
        source_partition=source_partition,
        source_offset=source_offset,
        error_class=error_class,
        error_message=error_message,
        payload=payload,
    )
