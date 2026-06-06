from __future__ import annotations

from datetime import UTC, date, datetime

import httpx
import pytest
from news_platform.contracts.events import ArticleFetchRequested
from news_platform.storage import StorageLayout

from news_article_fetcher.http import ArticleHttpClient
from news_article_fetcher.models import ArticleHttpResponse
from news_article_fetcher.service import ArticleFetcher
from news_service_common.errors import HttpFetchError, IngestionError

HTML = b"<html><head><title>Tin kinh doanh</title></head><body>OK</body></html>"


class FakeHttpClient:
    def __init__(self, content: bytes = HTML) -> None:
        self.content = content

    def fetch(self, url: str) -> ArticleHttpResponse:
        return ArticleHttpResponse(
            status_code=200,
            content=self.content,
            content_type="text/html; charset=utf-8",
        )


class FakeObjectStore:
    def __init__(self) -> None:
        self.payloads: dict[str, tuple[bytes, str]] = {}
        self.checkpoints: dict[str, dict] = {}

    def exists(self, uri: str) -> bool:
        return uri in self.payloads

    def write_compressed(self, uri: str, payload: bytes, *, content_type: str) -> None:
        self.payloads[uri] = (payload, content_type)

    def read_json(self, uri: str) -> dict | None:
        return self.checkpoints.get(uri)

    def write_json(self, uri: str, value: dict) -> None:
        self.checkpoints[uri] = value


class CheckpointFailingObjectStore(FakeObjectStore):
    def __init__(self) -> None:
        super().__init__()
        self.fail_next_checkpoint_write = True

    def write_json(self, uri: str, value: dict) -> None:
        if self.fail_next_checkpoint_write:
            self.fail_next_checkpoint_write = False
            raise RuntimeError("checkpoint unavailable")
        super().write_json(uri, value)


class FakePublisher:
    def __init__(self) -> None:
        self.events: list[tuple[str, object]] = []

    def publish(self, topic_key: str, event: object) -> None:
        self.events.append((topic_key, event))

    def flush(self) -> None:
        return None


def test_article_fetcher_stores_html_and_publishes_event() -> None:
    store = FakeObjectStore()
    publisher = FakePublisher()
    fetcher = ArticleFetcher(
        http_client=FakeHttpClient(),
        object_store=store,
        publisher=publisher,
        storage_layout=layout(),
    )

    outcome = fetcher.fetch(
        fetch_requested_event(),
        observed_at=datetime(2026, 6, 1, 3, tzinfo=UTC),
    )

    assert outcome.status == "published"
    assert outcome.content_length_bytes == len(HTML)
    assert len(store.payloads) == 1
    assert [topic for topic, _ in publisher.events] == ["article_fetched"]
    published = publisher.events[0][1]
    assert published.schema_version == "article.fetched.v2"
    assert published.fetch_status == "success"
    assert published.payload_uri == outcome.payload_uri
    assert published.source_document_id == outcome.source_document_id


def test_article_fetcher_does_not_refetch_completed_article() -> None:
    store = FakeObjectStore()
    publisher = FakePublisher()
    fetcher = ArticleFetcher(
        http_client=FakeHttpClient(),
        object_store=store,
        publisher=publisher,
        storage_layout=layout(),
    )
    event = fetch_requested_event()

    first = fetcher.fetch(event, observed_at=datetime(2026, 6, 1, 3, tzinfo=UTC))
    second = fetcher.fetch(event, observed_at=datetime(2026, 6, 1, 4, tzinfo=UTC))

    assert first.payload_uri == second.payload_uri
    assert second.status == "already_processed"
    assert len(store.payloads) == 1
    assert len(publisher.events) == 1


def test_article_fetcher_refetches_article_for_new_request_revision() -> None:
    store = FakeObjectStore()
    publisher = FakePublisher()
    fetcher = ArticleFetcher(
        http_client=FakeHttpClient(b"<html>old</html>"),
        object_store=store,
        publisher=publisher,
        storage_layout=layout(),
    )
    event = fetch_requested_event()

    first = fetcher.fetch(event, observed_at=datetime(2026, 6, 1, 3, tzinfo=UTC))
    fetcher.http_client = FakeHttpClient(b"<html>new</html>")
    second = fetcher.fetch(
        event.model_copy(update={"request_revision": "record_2"}),
        observed_at=datetime(2026, 6, 1, 4, tzinfo=UTC),
    )

    assert first.source_document_id != second.source_document_id
    assert first.payload_uri != second.payload_uri
    assert len(store.payloads) == 2
    assert len(publisher.events) == 2


def test_article_fetcher_uses_new_payload_for_different_article() -> None:
    store = FakeObjectStore()
    publisher = FakePublisher()
    fetcher = ArticleFetcher(
        http_client=FakeHttpClient(b"<html>old</html>"),
        object_store=store,
        publisher=publisher,
        storage_layout=layout(),
    )
    first_event = fetch_requested_event()
    second_event = fetch_requested_event().model_copy(
        update={
            "event_id": "event_fetch_request_2",
            "article_id": "article_2",
            "requested_url": "https://vnexpress.net/b.html",
        }
    )

    first = fetcher.fetch(first_event, observed_at=datetime(2026, 6, 1, 3, tzinfo=UTC))
    fetcher.http_client = FakeHttpClient(b"<html>new</html>")
    second = fetcher.fetch(second_event, observed_at=datetime(2026, 6, 1, 4, tzinfo=UTC))

    assert first.source_document_id != second.source_document_id
    assert first.payload_uri != second.payload_uri
    assert len(store.payloads) == 2


def test_article_fetcher_retry_after_checkpoint_failure_keeps_documents_immutable() -> None:
    store = CheckpointFailingObjectStore()
    publisher = FakePublisher()
    fetcher = ArticleFetcher(
        http_client=FakeHttpClient(b"<html>old</html>"),
        object_store=store,
        publisher=publisher,
        storage_layout=layout(),
    )
    event = fetch_requested_event()

    with pytest.raises(IngestionError) as raised:
        fetcher.fetch(event, observed_at=datetime(2026, 6, 1, 3, tzinfo=UTC))

    assert raised.value.stage == "checkpoint_write"
    fetcher.http_client = FakeHttpClient(b"<html>new</html>")
    second = fetcher.fetch(event, observed_at=datetime(2026, 6, 1, 4, tzinfo=UTC))

    first_published = publisher.events[0][1]
    second_published = publisher.events[1][1]
    assert first_published.source_document_id != second.source_document_id
    assert first_published.payload_uri != second.payload_uri
    assert second_published.payload_uri == second.payload_uri
    assert len(store.payloads) == 2


def test_article_fetcher_rejects_corrupt_completion_checkpoint() -> None:
    store = FakeObjectStore()
    store.checkpoints[layout().article_fetch_checkpoint_uri("article_1", "record_1")] = {}
    fetcher = ArticleFetcher(
        http_client=FakeHttpClient(),
        object_store=store,
        publisher=FakePublisher(),
        storage_layout=layout(),
    )

    with pytest.raises(IngestionError) as raised:
        fetcher.fetch(fetch_requested_event())

    assert raised.value.stage == "checkpoint_read"
    assert raised.value.retryable is False


def test_article_http_client_retries_transient_status() -> None:
    request_count = 0
    retry_logs: list[dict] = []
    sleeps: list[float] = []

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal request_count
        request_count += 1
        status_code = 503 if request_count < 3 else 200
        return httpx.Response(status_code, content=HTML, request=request)

    client = make_client(
        handler,
        sleep=sleeps.append,
        on_retry=lambda **fields: retry_logs.append(fields),
    )

    response = client.fetch("https://vnexpress.net/a.html")

    assert response.status_code == 200
    assert request_count == 3
    assert sleeps == [1, 2]
    assert [entry["attempt"] for entry in retry_logs] == [1, 2]


def test_article_http_client_rejects_permanent_status() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(404, request=request)

    with pytest.raises(HttpFetchError) as raised:
        make_client(handler).fetch("https://vnexpress.net/missing.html")

    assert raised.value.stage == "article_fetch"
    assert raised.value.retryable is False
    assert raised.value.status_code == 404


def test_article_http_client_rejects_oversized_response() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=b"x" * 11, request=request)

    with pytest.raises(HttpFetchError, match="Article response exceeds") as raised:
        make_client(handler, max_article_bytes=10).fetch("https://vnexpress.net/a.html")

    assert raised.value.retryable is False


def fetch_requested_event() -> ArticleFetchRequested:
    return ArticleFetchRequested(
        schema_version="article.fetch_requested.v3",
        event_id="event_fetch_request",
        event_time=datetime(2026, 6, 1, 2, tzinfo=UTC),
        run_id="rss_run_1",
        source_id="vnexpress",
        ingest_date=date(2026, 6, 1),
        article_id="article_1",
        requested_url="https://vnexpress.net/a.html",
        request_revision="record_1",
    )


def layout() -> StorageLayout:
    return StorageLayout(
        buckets={"landing": "tgb-prod-landing-a7k3p9"},
        payload_prefix="payloads",
    )


def make_client(
    handler,
    *,
    sleep=lambda seconds: None,
    on_retry=None,
    max_article_bytes: int = 1024,
) -> ArticleHttpClient:
    transport = httpx.MockTransport(handler)
    return ArticleHttpClient(
        user_agent="test",
        timeout_seconds=1,
        max_article_bytes=max_article_bytes,
        retry_attempts=3,
        retry_backoff_seconds=1,
        sleep=sleep,
        client_factory=lambda: httpx.Client(transport=transport),
        on_retry=on_retry,
    )
