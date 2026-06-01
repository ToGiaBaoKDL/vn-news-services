from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any


@dataclass(frozen=True)
class FeedItem:
    article_url: str
    title: str
    summary: str | None
    published_at: datetime | None


@dataclass(frozen=True)
class ParsedFeed:
    items: list[FeedItem]
    skipped_items: int
    duplicate_item_count: int


@dataclass(frozen=True)
class FeedResponse:
    status_code: int
    content: bytes
    etag: str | None
    last_modified: str | None

    @property
    def not_modified(self) -> bool:
        return self.status_code == 304


@dataclass(frozen=True)
class FeedCheckpoint:
    content_hash: str
    payload_uri: str
    observed_at: datetime
    etag: str | None
    last_modified: str | None
    events_published: bool
    item_record_hashes: dict[str, str]

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> FeedCheckpoint:
        return cls(
            content_hash=value["content_hash"],
            payload_uri=value["payload_uri"],
            observed_at=datetime.fromisoformat(value["observed_at"]),
            etag=value.get("etag"),
            last_modified=value.get("last_modified"),
            events_published=value["events_published"],
            item_record_hashes=value.get("item_record_hashes", {}),
        )

    def to_dict(self) -> dict[str, Any]:
        value = asdict(self)
        value["observed_at"] = self.observed_at.isoformat()
        return value


@dataclass(frozen=True)
class ScrapeOutcome:
    status: str
    source_id: str
    feed_id: str
    payload_uri: str | None
    parsed_item_count: int
    discovered_item_count: int
    skipped_item_count: int
    duplicate_item_count: int
