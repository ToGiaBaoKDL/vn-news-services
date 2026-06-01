from __future__ import annotations

from typing import Any


def select_enabled_source(sources: list[dict[str, Any]], source_id: str) -> dict[str, Any]:
    for source in sources:
        if source["source_id"] == source_id:
            if not source["enabled"]:
                msg = f"Source is disabled: {source_id}"
                raise ValueError(msg)
            return source
    msg = f"Unknown source: {source_id}"
    raise ValueError(msg)


def select_feed(source: dict[str, Any], feed_id: str) -> dict[str, Any]:
    for feed in source["feed_discovery"]["feeds"]:
        if feed["feed_id"] == feed_id:
            return feed
    msg = f"Unknown feed for {source['source_id']}: {feed_id}"
    raise ValueError(msg)
