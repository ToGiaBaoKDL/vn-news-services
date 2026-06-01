from __future__ import annotations

from datetime import UTC, datetime

import pytest
from news_platform.storage import StorageLayout

from news_feed_ingestor.models import FeedCheckpoint, FeedResponse
from news_feed_ingestor.service import FeedIngestor
from news_service_common.errors import IngestionError

RSS = b"""<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <item>
      <title>Gia vang tang</title>
      <description><![CDATA[Tom tat bai viet.]]></description>
      <pubDate>Mon, 01 Jun 2026 05:00:00 +0700</pubDate>
      <link>https://vnexpress.net/a.html</link>
    </item>
  </channel>
</rss>
"""

RSS_WITH_NEW_ITEM = RSS.replace(
    b"  </channel>",
    b"""    <item>
      <title>Lai suat giam</title>
      <description><![CDATA[Tom tat moi.]]></description>
      <pubDate>Mon, 01 Jun 2026 06:00:00 +0700</pubDate>
      <link>https://vnexpress.net/b.html</link>
    </item>
  </channel>""",
)

RSS_WITH_CHANGED_ITEM = RSS.replace(
    b"<title>Gia vang tang</title>", b"<title>Gia vang tang manh</title>"
)


class FakeFeedClient:
    def __init__(self, responses: list[FeedResponse]) -> None:
        self.responses = responses
        self.checkpoints: list[FeedCheckpoint | None] = []

    def fetch(self, url: str, checkpoint: FeedCheckpoint | None = None) -> FeedResponse:
        self.checkpoints.append(checkpoint)
        return self.responses.pop(0)


class FakeObjectStore:
    def __init__(self) -> None:
        self.checkpoints: dict[str, FeedCheckpoint] = {}
        self.payloads: dict[str, bytes] = {}

    def read_json(self, uri: str) -> dict | None:
        checkpoint = self.checkpoints.get(uri)
        return checkpoint.to_dict() if checkpoint else None

    def write_json(self, uri: str, value: dict) -> None:
        self.checkpoints[uri] = FeedCheckpoint.from_dict(value)

    def exists(self, uri: str) -> bool:
        return uri in self.payloads

    def write_compressed(self, uri: str, payload: bytes, *, content_type: str) -> None:
        assert content_type == "application/rss+xml"
        self.payloads[uri] = payload


class CorruptCheckpointObjectStore(FakeObjectStore):
    def read_json(self, uri: str) -> dict | None:
        return {"invalid": True}


class FakePublisher:
    def __init__(self) -> None:
        self.events: list[tuple[str, object]] = []

    def publish(self, topic_key: str, event: object) -> None:
        self.events.append((topic_key, event))

    def flush(self) -> None:
        return None


class FailingOncePublisher(FakePublisher):
    def __init__(self) -> None:
        super().__init__()
        self.fail_next_flush = True

    def flush(self) -> None:
        if self.fail_next_flush:
            self.fail_next_flush = False
            raise RuntimeError("Redpanda unavailable")


def test_scrape_writes_payload_checkpoint_and_events() -> None:
    client = FakeFeedClient([FeedResponse(200, RSS, '"etag"', "Mon, 01 Jun 2026 00:00:00 GMT")])
    store = FakeObjectStore()
    publisher = FakePublisher()

    outcome = make_ingestor(client, store, publisher).scrape(
        source(),
        feed(),
        observed_at=datetime(2026, 6, 1, 2, tzinfo=UTC),
    )

    assert outcome.status == "published"
    assert outcome.parsed_item_count == 1
    assert outcome.discovered_item_count == 1
    assert len(store.payloads) == 1
    assert [topic for topic, _ in publisher.events] == [
        "feed_item_discovered",
        "article_fetch_requested",
    ]
    assert publisher.events[0][1].feed_item_id.startswith("feed_item_")
    assert next(iter(store.checkpoints.values())).events_published is True


def test_scrape_uses_checkpoint_for_not_modified_feed() -> None:
    client = FakeFeedClient(
        [
            FeedResponse(200, RSS, '"etag"', "Mon, 01 Jun 2026 00:00:00 GMT"),
            FeedResponse(304, b"", '"etag"', "Mon, 01 Jun 2026 00:00:00 GMT"),
        ]
    )
    store = FakeObjectStore()
    publisher = FakePublisher()
    ingestor = make_ingestor(client, store, publisher)
    ingestor.scrape(source(), feed(), observed_at=datetime(2026, 6, 1, 2, tzinfo=UTC))

    outcome = ingestor.scrape(source(), feed(), observed_at=datetime(2026, 6, 1, 3, tzinfo=UTC))

    assert outcome.status == "not_modified"
    assert outcome.parsed_item_count == 0
    assert outcome.discovered_item_count == 0
    assert client.checkpoints[-1].events_published is True
    assert len(publisher.events) == 2


def test_scrape_rejects_not_modified_feed_without_checkpoint() -> None:
    ingestor = make_ingestor(
        FakeFeedClient([FeedResponse(304, b"", None, None)]),
        FakeObjectStore(),
        FakePublisher(),
    )

    with pytest.raises(IngestionError) as raised:
        ingestor.scrape(source(), feed(), observed_at=datetime(2026, 6, 1, 2, tzinfo=UTC))

    assert raised.value.stage == "feed_fetch"
    assert raised.value.retryable is False


def test_scrape_rejects_corrupt_checkpoint() -> None:
    ingestor = make_ingestor(
        FakeFeedClient([FeedResponse(200, RSS, None, None)]),
        CorruptCheckpointObjectStore(),
        FakePublisher(),
    )

    with pytest.raises(IngestionError) as raised:
        ingestor.scrape(source(), feed(), observed_at=datetime(2026, 6, 1, 2, tzinfo=UTC))

    assert raised.value.stage == "checkpoint_read"
    assert raised.value.retryable is False


def test_scrape_ignores_volatile_channel_metadata() -> None:
    updated_rss = RSS.replace(
        b"<channel>",
        b"<channel><pubDate>Mon, 01 Jun 2026 10:00:00 +0700</pubDate>",
    )
    client = FakeFeedClient(
        [
            FeedResponse(200, RSS, None, None),
            FeedResponse(200, updated_rss, None, None),
        ]
    )
    store = FakeObjectStore()
    publisher = FakePublisher()
    ingestor = make_ingestor(client, store, publisher)
    ingestor.scrape(source(), feed(), observed_at=datetime(2026, 6, 1, 2, tzinfo=UTC))

    outcome = ingestor.scrape(source(), feed(), observed_at=datetime(2026, 6, 1, 3, tzinfo=UTC))

    assert outcome.status == "unchanged"
    assert outcome.parsed_item_count == 1
    assert outcome.discovered_item_count == 0
    assert len(store.payloads) == 1
    assert len(publisher.events) == 2


def test_scrape_publishes_only_new_or_changed_items() -> None:
    client = FakeFeedClient(
        [
            FeedResponse(200, RSS, None, None),
            FeedResponse(200, RSS_WITH_NEW_ITEM, None, None),
        ]
    )
    store = FakeObjectStore()
    publisher = FakePublisher()
    ingestor = make_ingestor(client, store, publisher)
    ingestor.scrape(source(), feed(), observed_at=datetime(2026, 6, 1, 2, tzinfo=UTC))

    outcome = ingestor.scrape(source(), feed(), observed_at=datetime(2026, 6, 1, 3, tzinfo=UTC))

    assert outcome.status == "published"
    assert outcome.parsed_item_count == 2
    assert outcome.discovered_item_count == 1
    assert outcome.skipped_item_count == 0
    assert len(store.payloads) == 2
    assert len(publisher.events) == 4


def test_scrape_republishes_changed_item_record() -> None:
    client = FakeFeedClient(
        [
            FeedResponse(200, RSS, None, None),
            FeedResponse(200, RSS_WITH_CHANGED_ITEM, None, None),
        ]
    )
    store = FakeObjectStore()
    publisher = FakePublisher()
    ingestor = make_ingestor(client, store, publisher)
    ingestor.scrape(source(), feed(), observed_at=datetime(2026, 6, 1, 2, tzinfo=UTC))

    outcome = ingestor.scrape(source(), feed(), observed_at=datetime(2026, 6, 1, 3, tzinfo=UTC))

    assert outcome.status == "published"
    assert outcome.discovered_item_count == 1
    assert len(store.payloads) == 2
    assert [topic for topic, _ in publisher.events] == [
        "feed_item_discovered",
        "article_fetch_requested",
        "feed_item_discovered",
        "article_fetch_requested",
    ]
    assert publisher.events[0][1].feed_item_id == publisher.events[2][1].feed_item_id
    assert publisher.events[0][1].event_id != publisher.events[2][1].event_id
    assert publisher.events[1][1].article_id == publisher.events[3][1].article_id
    assert publisher.events[1][1].event_id == publisher.events[3][1].event_id


def test_same_article_in_different_feeds_gets_feed_aware_event_ids() -> None:
    client = FakeFeedClient(
        [
            FeedResponse(200, RSS, None, None),
            FeedResponse(200, RSS, None, None),
        ]
    )
    store = FakeObjectStore()
    publisher = FakePublisher()
    ingestor = make_ingestor(client, store, publisher)

    ingestor.scrape(source(), feed("kinh_doanh"), observed_at=datetime(2026, 6, 1, 2, tzinfo=UTC))
    ingestor.scrape(source(), feed("thoi_su"), observed_at=datetime(2026, 6, 1, 3, tzinfo=UTC))

    first_discovered = publisher.events[0][1]
    first_fetch = publisher.events[1][1]
    second_discovered = publisher.events[2][1]
    second_fetch = publisher.events[3][1]
    assert first_discovered.feed_item_id != second_discovered.feed_item_id
    assert first_discovered.event_id != second_discovered.event_id
    assert first_fetch.article_id == second_fetch.article_id
    assert first_fetch.event_id == second_fetch.event_id


def test_scrape_recovers_after_publish_flush_failure() -> None:
    client = FakeFeedClient(
        [
            FeedResponse(200, RSS, None, None),
            FeedResponse(200, RSS, None, None),
        ]
    )
    store = FakeObjectStore()
    publisher = FailingOncePublisher()
    ingestor = make_ingestor(client, store, publisher)

    with pytest.raises(IngestionError) as raised:
        ingestor.scrape(source(), feed(), observed_at=datetime(2026, 6, 1, 2, tzinfo=UTC))

    assert raised.value.stage == "event_publish"
    assert raised.value.retryable is True
    assert len(store.payloads) == 1
    assert next(iter(store.checkpoints.values())).events_published is False

    outcome = ingestor.scrape(source(), feed(), observed_at=datetime(2026, 6, 1, 3, tzinfo=UTC))

    assert outcome.status == "published"
    assert outcome.discovered_item_count == 1
    assert len(store.payloads) == 1
    assert len(publisher.events) == 4
    assert publisher.events[0][1].event_id == publisher.events[2][1].event_id
    assert publisher.events[1][1].event_id == publisher.events[3][1].event_id
    assert next(iter(store.checkpoints.values())).events_published is True


def make_ingestor(
    client: FakeFeedClient,
    store: FakeObjectStore,
    publisher: FakePublisher,
) -> FeedIngestor:
    layout = StorageLayout(
        buckets={"landing": "tgb-prod-landing-a7k3p9"},
        warehouse_prefix="warehouse",
        payload_prefix="payloads",
    )
    return FeedIngestor(
        feed_client=client,
        object_store=store,
        publisher=publisher,
        storage_layout=layout,
    )


def source() -> dict:
    return {"source_id": "vnexpress"}


def feed(feed_id: str = "kinh_doanh") -> dict:
    return {
        "feed_id": feed_id,
        "category": feed_id,
        "url": "https://vnexpress.net/rss/kinh-doanh.rss",
    }
