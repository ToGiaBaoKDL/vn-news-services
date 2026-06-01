from __future__ import annotations

import base64
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from confluent_kafka import Consumer, KafkaError, KafkaException, Message, Producer
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


@dataclass(frozen=True)
class ConsumedEvent:
    message: Message

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
        return decode_schema_registry_json(self.message.value())

    def payload_for_dlq(self) -> dict[str, Any]:
        payload = self.message.value()
        try:
            return {"encoding": "json", "value": self.decode_value()}
        except Exception:
            return {
                "encoding": "base64",
                "value": base64.b64encode(payload or b"").decode(),
            }


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
        return ConsumedEvent(message=message)

    def commit(self, event: ConsumedEvent) -> None:
        self.consumer.commit(event.message, asynchronous=False)

    def close(self) -> None:
        self.consumer.close()


def decode_schema_registry_json(payload: bytes | None) -> dict[str, Any]:
    if payload is None:
        raise ValueError("Cannot decode empty Redpanda message payload")
    if len(payload) < 5 or payload[0] != 0:
        raise ValueError("Expected Schema Registry JSON framing")
    data = payload[5:]
    value = json.loads(data.decode())
    if not isinstance(value, dict):
        raise ValueError("Expected decoded event payload to be a JSON object")
    return value


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
