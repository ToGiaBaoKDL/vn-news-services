from __future__ import annotations

import json
from argparse import Namespace

from news_feed_ingestor import main as feed_main
from news_service_common.errors import IngestionError


def test_scrape_all_feeds_continues_after_feed_failure(monkeypatch) -> None:
    source = {
        "source_id": "vnexpress",
        "enabled": True,
        "crawl": {"delay_seconds": 0},
        "feed_discovery": {
            "feeds": [
                {"feed_id": "kinh_doanh"},
                {"feed_id": "thoi_su"},
            ]
        },
    }
    visited_feed_ids: list[str] = []
    logs: list[tuple[str, dict]] = []

    monkeypatch.setattr(feed_main, "load_settings", lambda: {"crawl": {"retry": {}}})
    monkeypatch.setattr(feed_main, "load_sources", lambda settings: [source])
    monkeypatch.setattr(
        feed_main,
        "scrape_feed",
        lambda **kwargs: (
            visited_feed_ids.append(kwargs["feed"]["feed_id"])
            or int(kwargs["feed"]["feed_id"] == "kinh_doanh")
        ),
    )
    monkeypatch.setattr(feed_main.time, "sleep", lambda seconds: None)
    monkeypatch.setattr(
        feed_main,
        "log_event",
        lambda service, event, **fields: logs.append((event, fields)),
    )

    result = feed_main.scrape(make_args(all_feeds=True))

    assert result == 1
    assert visited_feed_ids == ["kinh_doanh", "thoi_su"]
    assert logs[-1] == (
        "rss_source_completed",
        {
            "source_id": "vnexpress",
            "feed_count": 2,
            "failed_feed_count": 1,
            "dry_run": True,
        },
    )


def test_scrape_feed_logs_unconsumed_failure(monkeypatch, capsys) -> None:
    def fail(**kwargs) -> None:
        raise IngestionError(
            stage="feed_fetch",
            retryable=True,
            message="temporary RSS outage",
        )

    monkeypatch.setattr(feed_main, "_scrape_feed", fail)

    result = feed_main.scrape_feed(
        args=make_args(),
        config={},
        source={"source_id": "vnexpress"},
        feed={"feed_id": "kinh_doanh"},
        retry={},
        object_store=None,
        publisher=None,
    )

    assert result == 1
    record = json.loads(capsys.readouterr().err)
    assert record["event"] == "rss_feed_failed"
    assert record["source_id"] == "vnexpress"
    assert record["feed_id"] == "kinh_doanh"
    assert record["stage"] == "feed_fetch"
    assert record["retryable"] is True


def make_args(*, all_feeds: bool = False) -> Namespace:
    return Namespace(
        source_id="vnexpress",
        feed_id="kinh_doanh",
        all_feeds=all_feeds,
        dry_run=True,
    )
