from __future__ import annotations

import json
from argparse import Namespace

from news_feed_ingestor import main as feed_main
from news_service_common.errors import IngestionError


def test_scrape_all_feeds_succeeds_with_partial_nonfatal_failures(monkeypatch) -> None:
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
            or ((True, False) if kwargs["feed"]["feed_id"] == "kinh_doanh" else (False, False))
        ),
    )
    monkeypatch.setattr(feed_main.time, "sleep", lambda seconds: None)
    monkeypatch.setattr(
        feed_main,
        "log_event",
        lambda service, event, **fields: logs.append((event, fields)),
    )

    result = feed_main.scrape(make_args(all_feeds=True))

    assert result == 0
    assert visited_feed_ids == ["kinh_doanh", "thoi_su"]
    assert logs[-1] == (
        "rss_source_completed",
        {
            "source_id": "vnexpress",
            "feed_count": 2,
            "successful_feed_count": 1,
            "failed_feed_count": 1,
            "fatal_feed_count": 0,
            "dry_run": True,
        },
    )


def test_scrape_all_feeds_fails_when_every_feed_fails(monkeypatch) -> None:
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

    monkeypatch.setattr(feed_main, "load_settings", lambda: {"crawl": {"retry": {}}})
    monkeypatch.setattr(feed_main, "load_sources", lambda settings: [source])
    monkeypatch.setattr(feed_main, "scrape_feed", lambda **kwargs: (True, False))
    monkeypatch.setattr(feed_main.time, "sleep", lambda seconds: None)
    monkeypatch.setattr(feed_main, "log_event", lambda *args, **kwargs: None)

    result = feed_main.scrape(make_args(all_feeds=True))

    assert result == 1


def test_scrape_all_feeds_fails_on_fatal_failure(monkeypatch) -> None:
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

    monkeypatch.setattr(feed_main, "load_settings", lambda: {"crawl": {"retry": {}}})
    monkeypatch.setattr(feed_main, "load_sources", lambda settings: [source])
    monkeypatch.setattr(
        feed_main,
        "scrape_feed",
        lambda **kwargs: (
            (True, True) if kwargs["feed"]["feed_id"] == "kinh_doanh" else (False, False)
        ),
    )
    monkeypatch.setattr(feed_main.time, "sleep", lambda seconds: None)
    monkeypatch.setattr(feed_main, "log_event", lambda *args, **kwargs: None)

    result = feed_main.scrape(make_args(all_feeds=True))

    assert result == 1


def test_scrape_single_feed_logs_task_scope(monkeypatch) -> None:
    source = {
        "source_id": "vnexpress",
        "enabled": True,
        "crawl": {"delay_seconds": 0},
        "feed_discovery": {"feeds": [{"feed_id": "kinh_doanh"}]},
    }
    logs: list[tuple[str, dict]] = []

    monkeypatch.setattr(feed_main, "load_settings", lambda: {"crawl": {"retry": {}}})
    monkeypatch.setattr(feed_main, "load_sources", lambda settings: [source])
    monkeypatch.setattr(feed_main, "scrape_feed", lambda **kwargs: (False, False))
    monkeypatch.setattr(
        feed_main,
        "log_event",
        lambda service, event, **fields: logs.append((event, fields)),
    )
    monkeypatch.setattr(feed_main, "log_metric", lambda *args, **kwargs: None)

    result = feed_main.scrape(make_args())

    assert result == 0
    assert [event for event, _ in logs] == ["rss_feed_task_started", "rss_feed_task_completed"]
    assert logs[-1][1]["source_id"] == "vnexpress"
    assert logs[-1][1]["feed_id"] == "kinh_doanh"
    assert logs[-1][1]["status"] == "success"


def test_scrape_single_feed_failure_fails_task(monkeypatch) -> None:
    source = {
        "source_id": "vnexpress",
        "enabled": True,
        "crawl": {"delay_seconds": 0},
        "feed_discovery": {"feeds": [{"feed_id": "kinh_doanh"}]},
    }

    monkeypatch.setattr(feed_main, "load_settings", lambda: {"crawl": {"retry": {}}})
    monkeypatch.setattr(feed_main, "load_sources", lambda settings: [source])
    monkeypatch.setattr(feed_main, "scrape_feed", lambda **kwargs: (True, False))
    monkeypatch.setattr(feed_main, "log_event", lambda *args, **kwargs: None)
    monkeypatch.setattr(feed_main, "log_metric", lambda *args, **kwargs: None)

    assert feed_main.scrape(make_args()) == 1


def test_scrape_single_feed_respects_source_cooldown(monkeypatch) -> None:
    source = {
        "source_id": "vnexpress",
        "enabled": True,
        "crawl": {"delay_seconds": 5},
        "feed_discovery": {"feeds": [{"feed_id": "kinh_doanh"}]},
    }
    sleeps: list[int] = []

    monkeypatch.setattr(feed_main, "scrape_feed", lambda **kwargs: (False, False))
    monkeypatch.setattr(feed_main, "log_event", lambda *args, **kwargs: None)
    monkeypatch.setattr(feed_main, "log_metric", lambda *args, **kwargs: None)
    monkeypatch.setattr(feed_main.time, "sleep", lambda seconds: sleeps.append(seconds))

    assert (
        feed_main.scrape_feed_task(
            args=make_args(dry_run=False),
            config={},
            source=source,
            feed={"feed_id": "kinh_doanh"},
            retry={},
            object_store=None,
            publisher=None,
            started_at=0,
        )
        == 0
    )
    assert sleeps == [5]


def test_scrape_feed_logs_retryable_failure_as_fatal(monkeypatch, capsys) -> None:
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

    assert result == (True, True)
    record = json.loads(capsys.readouterr().err)
    assert record["event"] == "rss_feed_failed"
    assert record["source_id"] == "vnexpress"
    assert record["feed_id"] == "kinh_doanh"
    assert record["stage"] == "feed_fetch"
    assert record["retryable"] is True


def test_scrape_feed_logs_nonretryable_failure_as_nonfatal(monkeypatch, capsys) -> None:
    def fail(**kwargs) -> None:
        raise IngestionError(
            stage="feed_parse",
            retryable=False,
            message="empty RSS feed",
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

    assert result == (True, False)
    record = json.loads(capsys.readouterr().err)
    assert record["event"] == "rss_feed_failed"
    assert record["stage"] == "feed_parse"
    assert record["retryable"] is False


def make_args(*, all_feeds: bool = False, dry_run: bool = True) -> Namespace:
    return Namespace(
        source_id="vnexpress",
        feed_id="kinh_doanh",
        all_feeds=all_feeds,
        dry_run=dry_run,
    )
