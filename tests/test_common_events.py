from __future__ import annotations

import json
import time
from datetime import UTC, date, datetime

import pytest
from news_platform.contracts.events import ArticleFetchRequested, FeedItemDiscovered

from news_service_common.errors import IngestionError
from news_service_common.events import (
    ConsumedEvent,
    JsonEventConsumer,
    UnexpectedSchemaIdError,
    decode_schema_registry_json,
    event_message_key,
    make_dlq_event,
)
from news_service_common.runtime import (
    ConsumedRetryBackoff,
    ShutdownSignal,
    handle_consumed_error,
    should_stop_after_process,
)


def test_decode_schema_registry_json_payload() -> None:
    payload = b"\x00\x00\x00\x00\x01" + json.dumps({"hello": "world"}).encode()

    assert decode_schema_registry_json(payload) == {"hello": "world"}


def test_decode_schema_registry_json_accepts_expected_schema_id() -> None:
    payload = b"\x00\x00\x00\x00\x02" + json.dumps({"hello": "world"}).encode()

    assert decode_schema_registry_json(payload, expected_schema_ids=frozenset({2})) == {
        "hello": "world"
    }


def test_decode_schema_registry_json_rejects_unexpected_schema_id() -> None:
    payload = b"\x00\x00\x00\x00\x03" + json.dumps({"hello": "world"}).encode()

    with pytest.raises(UnexpectedSchemaIdError, match="Unexpected Schema Registry schema id 3"):
        decode_schema_registry_json(payload, expected_schema_ids=frozenset({2}))


def test_consumed_event_refreshes_schema_ids_after_unknown_schema() -> None:
    payload = b"\x00\x00\x00\x00\x03" + json.dumps({"hello": "world"}).encode()
    refreshes = 0

    def refresh() -> frozenset[int]:
        nonlocal refreshes
        refreshes += 1
        return frozenset({2, 3})

    consumed = ConsumedEvent(
        message=FakeMessage(payload),
        expected_schema_ids_loader=lambda: frozenset({2}),
        expected_schema_ids_refresher=refresh,
    )

    assert consumed.decode_value() == {"hello": "world"}
    assert refreshes == 1


def test_decode_schema_registry_json_rejects_zero_schema_id() -> None:
    payload = b"\x00\x00\x00\x00\x00" + json.dumps({"hello": "world"}).encode()

    with pytest.raises(ValueError, match="positive Schema Registry schema id"):
        decode_schema_registry_json(payload)


def test_decode_schema_registry_json_rejects_unframed_payload() -> None:
    with pytest.raises(ValueError, match="Schema Registry JSON framing"):
        decode_schema_registry_json(b'{"hello":"world"}')


def test_consumed_event_preserves_invalid_payload_for_dlq() -> None:
    consumed = ConsumedEvent(message=FakeMessage(b"not-json"))

    assert consumed.payload_for_dlq() == {
        "encoding": "base64",
        "value": "bm90LWpzb24=",
    }


def test_event_message_keys_preserve_aggregate_ordering() -> None:
    discovered = FeedItemDiscovered(
        schema_version="feed_item.discovered.v2",
        event_id="event_1",
        event_time=datetime(2026, 6, 1, 2, tzinfo=UTC),
        run_id="rss_run_1",
        source_id="vnexpress",
        ingest_date=date(2026, 6, 1),
        feed_item_id="feed_item_1",
        article_id="article_1",
        feed_id="kinh_doanh",
        article_url="https://vnexpress.net/a.html",
        title="A",
        discovered_at=datetime(2026, 6, 1, 2, tzinfo=UTC),
        record_hash="hash_1",
    )
    fetch = ArticleFetchRequested(
        schema_version="article.fetch_requested.v3",
        event_id="event_2",
        event_time=datetime(2026, 6, 1, 2, tzinfo=UTC),
        run_id="rss_run_1",
        source_id="vnexpress",
        ingest_date=date(2026, 6, 1),
        article_id="article_1",
        requested_url="https://vnexpress.net/a.html",
        request_revision="record_1",
    )

    assert event_message_key("feed_item_discovered", discovered) == "feed_item_1"
    assert event_message_key("article_fetch_requested", fetch) == "article_1"


def test_permanent_consumed_error_routes_invalid_payload_to_dlq() -> None:
    consumed = ConsumedEvent(message=FakeMessage(b"not-json"))
    publisher = FakePublisher()
    consumer = FakeConsumer()

    result = handle_consumed_error(
        service_name="article_fetcher",
        failure_event="article_fetch_failed",
        dlq_event="article_fetch_dlq",
        config={"event_bus": {"topics": {"dlq": {"name": "news.dlq.v1"}}}},
        publisher=publisher,
        consumer=consumer,
        consumed=consumed,
        error=IngestionError(stage="event_decode", retryable=False, message="bad payload"),
        started_at=time.perf_counter(),
    )

    assert result == 0
    assert consumer.committed == [consumed]
    assert consumer.seeked == []
    assert publisher.flushed is True
    topic_key, event = publisher.events[0]
    assert topic_key == "dlq"
    assert event.payload["failed_event"]["encoding"] == "base64"


def test_retryable_consumed_error_keeps_worker_alive_without_commit() -> None:
    consumed = ConsumedEvent(message=FakeMessage(b"not-json"))
    publisher = FakePublisher()
    consumer = FakeConsumer()

    result = handle_consumed_error(
        service_name="article_fetcher",
        failure_event="article_fetch_failed",
        dlq_event="article_fetch_dlq",
        config={"event_bus": {"topics": {"dlq": {"name": "news.dlq.v1"}}}},
        publisher=publisher,
        consumer=consumer,
        consumed=consumed,
        error=IngestionError(stage="payload_write", retryable=True, message="S3 unavailable"),
        started_at=time.perf_counter(),
    )

    assert result == 2
    assert consumer.committed == []
    assert consumer.seeked == [consumed]
    assert publisher.events == []
    assert publisher.flushed is False


def test_retryable_consumed_error_waits_with_bounded_backoff() -> None:
    consumed = ConsumedEvent(message=FakeMessage(b"not-json"))
    publisher = FakePublisher()
    consumer = FakeConsumer()
    sleeps: list[float] = []
    backoff = ConsumedRetryBackoff(
        base_delay_seconds=2,
        max_delay_seconds=5,
        jitter_seconds=0,
        sleep=sleeps.append,
        poll_seconds=10,
    )

    for _ in range(3):
        result = handle_consumed_error(
            service_name="article_fetcher",
            failure_event="article_fetch_failed",
            dlq_event="article_fetch_dlq",
            config={"event_bus": {"topics": {"dlq": {"name": "news.dlq.v1"}}}},
            publisher=publisher,
            consumer=consumer,
            consumed=consumed,
            error=IngestionError(
                stage="payload_write",
                retryable=True,
                message="S3 unavailable",
            ),
            started_at=time.perf_counter(),
            retry_backoff=backoff,
            shutdown=ShutdownSignal(),
        )
        assert result == 2

    assert consumer.committed == []
    assert consumer.seeked == [consumed, consumed, consumed]
    assert sleeps == [2, 4, 5]


def test_unexpected_consumed_error_stops_worker_without_commit() -> None:
    consumed = ConsumedEvent(message=FakeMessage(b"not-json"))
    publisher = FakePublisher()
    consumer = FakeConsumer()

    result = handle_consumed_error(
        service_name="article_fetcher",
        failure_event="article_fetch_failed",
        dlq_event="article_fetch_dlq",
        config={"event_bus": {"topics": {"dlq": {"name": "news.dlq.v1"}}}},
        publisher=publisher,
        consumer=consumer,
        consumed=consumed,
        error=RuntimeError("unexpected bug"),
        started_at=time.perf_counter(),
    )

    assert result == 1
    assert consumer.committed == []
    assert consumer.seeked == []
    assert publisher.events == []
    assert publisher.flushed is False


def test_retryable_consumed_result_keeps_daemon_running_but_fails_once() -> None:
    assert should_stop_after_process(2, once=False) is None
    assert should_stop_after_process(2, once=True) == 1


def test_make_dlq_event_is_stable_for_same_source_message() -> None:
    first = make_dlq_event(
        source_topic="news.article.fetch_requested.v3",
        source_partition=0,
        source_offset=10,
        error_class="ValueError",
        error_message="bad input",
        payload={"article_id": "article_1"},
    )
    second = make_dlq_event(
        source_topic="news.article.fetch_requested.v3",
        source_partition=0,
        source_offset=10,
        error_class="ValueError",
        error_message="bad input",
        payload={"article_id": "article_1"},
    )

    assert first.event_id == second.event_id
    assert first.schema_version == "news.dlq.v1"


def test_schema_id_cache_refreshes_after_ttl(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple[str, str]] = []
    clock = iter([0.0, 100.0, 301.0, 301.0])
    consumer = JsonEventConsumer.__new__(JsonEventConsumer)
    consumer.schema_registry_url = "http://redpanda:8081"
    consumer.schema_id_cache_ttl_seconds = 300
    consumer.schema_ids_by_subject = {}

    monkeypatch.setattr("news_service_common.events.time.monotonic", lambda: next(clock))

    def fake_fetch(registry_url: str, subject: str) -> frozenset[int]:
        calls.append((registry_url, subject))
        return frozenset({len(calls)})

    monkeypatch.setattr("news_service_common.events.fetch_subject_schema_ids", fake_fetch)

    assert consumer.schema_ids_for_subject("topic-value") == frozenset({1})
    assert consumer.schema_ids_for_subject("topic-value") == frozenset({1})
    assert consumer.schema_ids_for_subject("topic-value") == frozenset({2})
    assert calls == [
        ("http://redpanda:8081", "topic-value"),
        ("http://redpanda:8081", "topic-value"),
    ]


class FakeMessage:
    def __init__(self, value: bytes) -> None:
        self._value = value

    def value(self) -> bytes:
        return self._value

    def topic(self) -> str:
        return "news.article.fetch_requested.v3"

    def partition(self) -> int:
        return 0

    def offset(self) -> int:
        return 10


class FakePublisher:
    def __init__(self) -> None:
        self.events: list[tuple[str, object]] = []
        self.flushed = False

    def publish(self, topic_key: str, event: object) -> None:
        self.events.append((topic_key, event))

    def flush(self) -> None:
        self.flushed = True


class FakeConsumer:
    def __init__(self) -> None:
        self.committed: list[ConsumedEvent] = []
        self.seeked: list[ConsumedEvent] = []

    def commit(self, event: ConsumedEvent) -> None:
        self.committed.append(event)

    def seek(self, event: ConsumedEvent) -> None:
        self.seeked.append(event)
