from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from datetime import UTC, datetime
from typing import Any, Protocol

from news_platform.contracts.events import (
    ArticleFetchRequested,
    BaseEvent,
    FeedItemDiscovered,
)
from news_platform.ids import make_stable_id, normalize_article_url
from news_platform.storage import StorageLayout

from news_feed_ingestor.models import (
    FeedCheckpoint,
    FeedItem,
    FeedResponse,
    ScrapeOutcome,
)
from news_feed_ingestor.rss import parse_rss_items
from news_service_common.errors import IngestionError
from news_service_common.stages import run_stage

MAX_CHECKPOINT_ITEMS = 1000


class FeedClient(Protocol):
    def fetch(self, url: str, checkpoint: FeedCheckpoint | None = None) -> FeedResponse: ...


class ObjectStore(Protocol):
    def read_json(self, uri: str) -> dict[str, Any] | None: ...

    def write_json(self, uri: str, value: dict[str, Any]) -> None: ...

    def exists(self, uri: str) -> bool: ...

    def write_compressed(self, uri: str, payload: bytes, *, content_type: str) -> None: ...


class Publisher(Protocol):
    def publish(self, topic_key: str, event: BaseEvent) -> None: ...

    def flush(self) -> None: ...


class FeedIngestor:
    def __init__(
        self,
        *,
        feed_client: FeedClient,
        object_store: ObjectStore,
        publisher: Publisher,
        storage_layout: StorageLayout,
    ) -> None:
        self.feed_client = feed_client
        self.object_store = object_store
        self.publisher = publisher
        self.storage_layout = storage_layout

    def scrape(
        self,
        source: dict,
        feed: dict,
        *,
        observed_at: datetime | None = None,
    ) -> ScrapeOutcome:
        observed_at = observed_at or datetime.now(UTC)
        source_id = source["source_id"]
        feed_id = feed["feed_id"]
        checkpoint_uri = self.storage_layout.rss_checkpoint_uri(source_id, feed_id)
        current_checkpoint = run_stage(
            "checkpoint_read",
            True,
            lambda: read_feed_checkpoint(self.object_store, checkpoint_uri),
        )
        response = run_stage(
            "feed_fetch",
            True,
            lambda: self.feed_client.fetch(feed["url"], current_checkpoint),
        )
        if response.not_modified:
            if not current_checkpoint or not current_checkpoint.events_published:
                raise IngestionError(
                    stage="feed_fetch",
                    retryable=False,
                    message="Received HTTP 304 without a published checkpoint",
                )
            return ScrapeOutcome(
                status="not_modified",
                source_id=source_id,
                feed_id=feed_id,
                payload_uri=current_checkpoint.payload_uri,
                parsed_item_count=0,
                discovered_item_count=0,
                skipped_item_count=0,
                duplicate_item_count=0,
            )

        parsed_feed = run_stage("feed_parse", False, lambda: parse_rss_items(response.content))
        item_record_hashes = {
            feed_item_id(source_id, feed_id, item): item_record_hash(item)
            for item in parsed_feed.items
        }
        content_hash = semantic_feed_hash(item_record_hashes)
        if (
            current_checkpoint
            and current_checkpoint.content_hash == content_hash
            and current_checkpoint.events_published
        ):
            run_stage(
                "checkpoint_write",
                True,
                lambda: write_feed_checkpoint(
                    self.object_store,
                    checkpoint_uri,
                    replace(
                        current_checkpoint,
                        etag=response.etag,
                        last_modified=response.last_modified,
                    ),
                ),
            )
            return ScrapeOutcome(
                status="unchanged",
                source_id=source_id,
                feed_id=feed_id,
                payload_uri=current_checkpoint.payload_uri,
                parsed_item_count=len(parsed_feed.items),
                discovered_item_count=0,
                skipped_item_count=parsed_feed.skipped_items,
                duplicate_item_count=parsed_feed.duplicate_item_count,
            )

        run_id = make_stable_id("rss_run", source_id, feed_id, content_hash)
        payload_uri = self.storage_layout.rss_payload_uri(
            source_id,
            feed_id,
            observed_at.date(),
            run_id,
        )
        payload_exists = run_stage(
            "payload_write",
            True,
            lambda: self.object_store.exists(payload_uri),
        )
        if not payload_exists:
            run_stage(
                "payload_write",
                True,
                lambda: self.object_store.write_compressed(
                    payload_uri,
                    response.content,
                    content_type="application/rss+xml",
                ),
            )

        checkpoint = FeedCheckpoint(
            content_hash=content_hash,
            payload_uri=payload_uri,
            observed_at=(
                current_checkpoint.observed_at
                if current_checkpoint and current_checkpoint.content_hash == content_hash
                else observed_at
            ),
            etag=response.etag,
            last_modified=response.last_modified,
            events_published=False,
            item_record_hashes=(
                current_checkpoint.item_record_hashes if current_checkpoint else {}
            ),
        )
        run_stage(
            "checkpoint_write",
            True,
            lambda: write_feed_checkpoint(self.object_store, checkpoint_uri, checkpoint),
        )
        discovered_item_count = 0
        for item in parsed_feed.items:
            item_id = feed_item_id(source_id, feed_id, item)
            record_hash = item_record_hashes[item_id]
            if checkpoint.item_record_hashes.get(item_id) != record_hash:
                run_stage(
                    "event_publish",
                    True,
                    lambda item=item, record_hash=record_hash: self._publish_item(
                        source_id,
                        feed,
                        item,
                        checkpoint,
                        run_id,
                        record_hash,
                    ),
                )
                discovered_item_count += 1
        run_stage("event_publish", True, self.publisher.flush)
        run_stage(
            "checkpoint_write",
            True,
            lambda: write_feed_checkpoint(
                self.object_store,
                checkpoint_uri,
                replace(
                    checkpoint,
                    events_published=True,
                    item_record_hashes=remember_item_records(
                        checkpoint.item_record_hashes,
                        item_record_hashes,
                    ),
                ),
            ),
        )
        return ScrapeOutcome(
            status="published",
            source_id=source_id,
            feed_id=feed_id,
            payload_uri=payload_uri,
            parsed_item_count=len(parsed_feed.items),
            discovered_item_count=discovered_item_count,
            skipped_item_count=parsed_feed.skipped_items,
            duplicate_item_count=parsed_feed.duplicate_item_count,
        )

    def _publish_item(
        self,
        source_id: str,
        feed: dict,
        item: FeedItem,
        checkpoint: FeedCheckpoint,
        run_id: str,
        record_hash: str,
    ) -> None:
        article_url = normalize_article_url(item.article_url)
        item_id = feed_item_id(source_id, feed["feed_id"], item)
        article_id = make_stable_id("article", article_url)
        discovered_event = FeedItemDiscovered(
            schema_version="feed_item.discovered.v2",
            event_id=make_stable_id("event", "feed_item.discovered.v2", item_id, record_hash),
            event_time=checkpoint.observed_at,
            run_id=run_id,
            source_id=source_id,
            ingest_date=checkpoint.observed_at.date(),
            feed_item_id=item_id,
            article_id=article_id,
            feed_id=feed["feed_id"],
            category=feed["category"],
            article_url=article_url,
            title=item.title,
            summary=item.summary,
            published_at=item.published_at,
            discovered_at=checkpoint.observed_at,
            payload_uri=checkpoint.payload_uri,
            record_hash=record_hash,
        )
        self.publisher.publish("feed_item_discovered", discovered_event)
        self._publish_fetch_request(
            source_id=source_id,
            item=item,
            run_id=run_id,
            event_time=checkpoint.observed_at,
            request_revision=record_hash,
        )

    def _publish_fetch_request(
        self,
        *,
        source_id: str,
        item: FeedItem,
        run_id: str,
        event_time: datetime,
        request_revision: str,
    ) -> None:
        requested_url = normalize_article_url(item.article_url)
        article_id = make_stable_id("article", requested_url)
        fetch_event = ArticleFetchRequested(
            schema_version="article.fetch_requested.v3",
            event_id=make_stable_id(
                "event",
                "article.fetch_requested.v3",
                article_id,
                request_revision,
            ),
            event_time=event_time,
            run_id=run_id,
            source_id=source_id,
            ingest_date=event_time.date(),
            article_id=article_id,
            requested_url=requested_url,
            request_revision=request_revision,
        )
        self.publisher.publish("article_fetch_requested", fetch_event)


def feed_item_id(source_id: str, feed_id: str, item: FeedItem) -> str:
    return make_stable_id("feed_item", source_id, feed_id, normalize_article_url(item.article_url))


def item_record_hash(item: FeedItem) -> str:
    return make_stable_id(
        "record",
        normalize_article_url(item.article_url),
        item.title,
        item.summary or "",
        item.published_at.isoformat() if item.published_at else "",
    )


def semantic_feed_hash(item_record_hashes: dict[str, str]) -> str:
    value = json.dumps(item_record_hashes, separators=(",", ":"), sort_keys=True).encode()
    return hashlib.sha256(value).hexdigest()


def remember_item_records(previous: dict[str, str], current: dict[str, str]) -> dict[str, str]:
    remembered = {**previous, **current}
    return dict(list(remembered.items())[-MAX_CHECKPOINT_ITEMS:])


def read_feed_checkpoint(object_store: ObjectStore, uri: str) -> FeedCheckpoint | None:
    try:
        value = object_store.read_json(uri)
        return FeedCheckpoint.from_dict(value) if value is not None else None
    except (AttributeError, KeyError, TypeError, ValueError) as error:
        raise IngestionError(
            stage="checkpoint_read",
            retryable=False,
            message=f"Invalid checkpoint at {uri}: {error}",
            error_class=type(error).__name__,
        ) from error


def write_feed_checkpoint(
    object_store: ObjectStore,
    uri: str,
    checkpoint: FeedCheckpoint,
) -> None:
    object_store.write_json(uri, checkpoint.to_dict())
