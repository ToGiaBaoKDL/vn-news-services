from __future__ import annotations

import json
from io import StringIO

from news_service_common.errors import FeedFetchError, error_fields
from news_service_common.telemetry import log_metric, write_event


def test_write_event_uses_consistent_json_envelope() -> None:
    stream = StringIO()

    write_event(stream, "feed_ingestor", "rss_feed_validated", source_id="vnexpress")

    record = json.loads(stream.getvalue())
    assert record["event"] == "rss_feed_validated"
    assert record["level"] == "info"
    assert record["service"] == "feed_ingestor"
    assert record["source_id"] == "vnexpress"
    assert record["timestamp"].endswith("+00:00")


def test_log_metric_uses_consistent_json_envelope(capsys) -> None:
    log_metric("article_fetcher", "article_fetch_events_total", 1, source_id="vnexpress")

    record = json.loads(capsys.readouterr().out)
    assert record["event"] == "metric_observed"
    assert record["metric_name"] == "article_fetch_events_total"
    assert record["metric_unit"] == "count"
    assert record["metric_value"] == 1
    assert record["service"] == "article_fetcher"
    assert record["source_id"] == "vnexpress"


def test_error_fields_include_recovery_metadata() -> None:
    fields = error_fields(
        FeedFetchError(
            "Service unavailable",
            retryable=True,
            status_code=503,
            error_class="HTTPStatusError",
        )
    )

    assert fields == {
        "stage": "feed_fetch",
        "retryable": True,
        "error_class": "HTTPStatusError",
        "error_message": "Service unavailable",
        "status_code": 503,
    }
