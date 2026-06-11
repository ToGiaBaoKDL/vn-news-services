from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import pytest
from news_platform.contracts.events import ArticleFetchRequested, NewsDlq

from news_service_common.dlq import process_dlq_event, replay_dlq_event
from news_service_common.errors import IngestionError


class FakeConsumed:
    topic = "news.dlq.v1"
    partition = 0
    offset = 12

    def __init__(self, payload: dict[str, Any]) -> None:
        self.payload = payload

    def decode_value(self) -> dict[str, Any]:
        return self.payload


class FakeConsumer:
    def __init__(self) -> None:
        self.committed: list[FakeConsumed] = []

    def commit(self, consumed: FakeConsumed) -> None:
        self.committed.append(consumed)


class FakePublisher:
    def __init__(self) -> None:
        self.events: list[tuple[str, object]] = []
        self.flushed = False

    def publish(self, topic_key: str, event: object) -> None:
        self.events.append((topic_key, event))

    def flush(self) -> None:
        self.flushed = True


def test_replay_dlq_event_publishes_failed_event_to_original_topic() -> None:
    publisher = FakePublisher()

    topic_key = replay_dlq_event(
        config=config(),
        publisher=publisher,
        dlq_event=dlq_event(),
    )

    assert topic_key == "article_fetch_requested"
    assert publisher.flushed is True
    assert publisher.events == [
        ("article_fetch_requested", ArticleFetchRequested.model_validate(failed_event()))
    ]


def test_replay_dlq_event_rejects_base64_payload() -> None:
    event = dlq_event(
        payload={
            "service": "article_fetcher",
            "stage": "event_decode",
            "failed_event": {"encoding": "base64", "value": "AAAA"},
        }
    )

    with pytest.raises(IngestionError, match="Only JSON-decoded"):
        replay_dlq_event(config=config(), publisher=FakePublisher(), dlq_event=event)


def test_process_dlq_commit_disposes_event_without_replay() -> None:
    consumer = FakeConsumer()
    consumed = FakeConsumed(dlq_event().model_dump(mode="json"))
    publisher = FakePublisher()

    process_dlq_event(
        action="commit",
        config=config(),
        consumed=consumed,
        consumer=consumer,
        publisher=publisher,
        reason="accepted permanent invalid input",
        operator_id="test_operator",
    )

    assert consumer.committed == [consumed]
    assert publisher.events == []


def test_process_dlq_replay_commits_after_publish() -> None:
    consumer = FakeConsumer()
    consumed = FakeConsumed(dlq_event().model_dump(mode="json"))
    publisher = FakePublisher()

    process_dlq_event(
        action="replay",
        config=config(),
        consumed=consumed,
        consumer=consumer,
        publisher=publisher,
        reason="parser fixed",
        operator_id="test_operator",
    )

    assert publisher.events[0][0] == "article_fetch_requested"
    assert consumer.committed == [consumed]


def failed_event() -> dict[str, Any]:
    return {
        "schema_version": "article.fetch_requested.v3",
        "event_id": "event_fetch_request",
        "event_time": "2026-06-01T02:00:00Z",
        "run_id": "rss_run_1",
        "source_id": "vnexpress",
        "ingest_date": "2026-06-01",
        "article_id": "article_1",
        "requested_url": "https://vnexpress.net/a.html",
        "request_revision": "record_1",
        "priority": 5,
    }


def dlq_event(payload: dict[str, Any] | None = None) -> NewsDlq:
    return NewsDlq(
        schema_version="news.dlq.v1",
        event_id="event_dlq_1",
        event_time=datetime(2026, 6, 1, 3, tzinfo=UTC),
        source_topic="news.article.fetch_requested.v3",
        source_partition=1,
        source_offset=9,
        error_class="HttpFetchError",
        error_message="fetch failed",
        payload=payload
        or {
            "service": "article_fetcher",
            "stage": "article_fetch",
            "failed_event": {"encoding": "json", "value": failed_event()},
        },
    )


def config() -> dict[str, Any]:
    return {
        "event_bus": {
            "topics": {
                "article_fetch_requested": {
                    "name": "news.article.fetch_requested.v3",
                    "partitions": 3,
                    "retention_ms": 1,
                },
                "dlq": {"name": "news.dlq.v1", "partitions": 1, "retention_ms": 1},
            }
        }
    }
