from __future__ import annotations

import httpx
import pytest

from news_feed_ingestor.adapters import HttpFeedClient
from news_service_common.errors import FeedFetchError


def test_http_feed_client_retries_transient_status() -> None:
    request_count = 0
    retry_logs: list[dict] = []
    sleeps: list[float] = []

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal request_count
        request_count += 1
        status_code = 503 if request_count < 3 else 200
        return httpx.Response(status_code, content=b"<rss />", request=request)

    client = make_client(
        handler,
        sleep=sleeps.append,
        on_retry=lambda **fields: retry_logs.append(fields),
    )

    response = client.fetch("https://vnexpress.net/rss/kinh-doanh.rss")

    assert response.status_code == 200
    assert request_count == 3
    assert sleeps == [1, 2]
    assert [entry["attempt"] for entry in retry_logs] == [1, 2]
    assert all(entry["status_code"] == 503 for entry in retry_logs)


def test_http_feed_client_does_not_retry_permanent_status() -> None:
    request_count = 0
    retry_logs: list[dict] = []

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal request_count
        request_count += 1
        return httpx.Response(404, request=request)

    client = make_client(handler, on_retry=lambda **fields: retry_logs.append(fields))

    with pytest.raises(FeedFetchError) as raised:
        client.fetch("https://vnexpress.net/rss/missing.rss")

    assert raised.value.retryable is False
    assert raised.value.status_code == 404
    assert request_count == 1
    assert retry_logs == []


def test_http_feed_client_rejects_not_modified_without_checkpoint() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(304, request=request)

    with pytest.raises(FeedFetchError) as raised:
        make_client(handler).fetch("https://vnexpress.net/rss/kinh-doanh.rss")

    assert raised.value.retryable is False
    assert raised.value.status_code == 304


def test_http_feed_client_rejects_oversized_response() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=b"x" * 11, request=request)

    with pytest.raises(FeedFetchError, match="RSS response exceeds") as raised:
        make_client(handler, max_feed_bytes=10).fetch("https://vnexpress.net/rss/kinh-doanh.rss")

    assert raised.value.retryable is False


def make_client(
    handler: httpx.MockTransport | httpx.BaseTransport | object,
    *,
    sleep=lambda seconds: None,
    on_retry=None,
    max_feed_bytes: int = 1024,
) -> HttpFeedClient:
    transport = (
        handler if isinstance(handler, httpx.BaseTransport) else httpx.MockTransport(handler)
    )
    return HttpFeedClient(
        user_agent="test",
        timeout_seconds=1,
        max_feed_bytes=max_feed_bytes,
        retry_attempts=3,
        retry_backoff_seconds=1,
        sleep=sleep,
        client_factory=lambda: httpx.Client(transport=transport),
        on_retry=on_retry,
    )
