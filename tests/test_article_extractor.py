from __future__ import annotations

from datetime import UTC, date, datetime

import pytest
from news_platform.contracts.events import ArticleFetched

from news_article_extractor.parser import extract_article
from news_article_extractor.service import ArticleExtractor
from news_service_common.errors import IngestionError

HTML = b"""<!doctype html>
<html>
  <head>
    <link rel="canonical" href="https://vnexpress.net/a.html">
    <meta property="og:title" content="Doanh nghiep tang truong">
    <meta property="og:description" content="Tom tat bai viet">
    <meta property="article:published_time" content="2026-06-01T08:00:00+07:00">
    <meta name="author" content="Nguyen Van A">
  </head>
  <body>
    <script>ignored()</script>
    <p>Day la doan noi dung dau tien cua bai viet kinh doanh.</p>
    <p>Day la doan noi dung thu hai voi them thong tin chi tiet.</p>
  </body>
</html>
"""


class FakeObjectStore:
    def __init__(self, payload: bytes = HTML) -> None:
        self.payload = payload

    def read_compressed(self, uri: str) -> bytes:
        return self.payload


class FakePublisher:
    def __init__(self) -> None:
        self.events: list[tuple[str, object]] = []

    def publish(self, topic_key: str, event: object) -> None:
        self.events.append((topic_key, event))

    def flush(self) -> None:
        return None


def test_extract_article_from_html() -> None:
    article = extract_article(HTML, fallback_url="https://vnexpress.net/fallback.html")

    assert article.canonical_url == "https://vnexpress.net/a.html"
    assert article.title == "Doanh nghiep tang truong"
    assert article.summary == "Tom tat bai viet"
    assert article.author == "Nguyen Van A"
    assert article.published_at.isoformat() == "2026-06-01T08:00:00+07:00"
    assert "doan noi dung dau tien" in article.body_text


def test_extract_article_uses_og_url_as_canonical_fallback() -> None:
    html = HTML.replace(
        b'<link rel="canonical" href="https://vnexpress.net/a.html">',
        b'<meta property="og:url" content="https://vnexpress.net/og-a.html">',
    )

    article = extract_article(
        html,
        fallback_url="https://vnexpress.net/fallback.html",
        require_canonical_url=True,
    )

    assert article.canonical_url == "https://vnexpress.net/og-a.html"


def test_article_extractor_publishes_extracted_event() -> None:
    publisher = FakePublisher()
    extractor = ArticleExtractor(
        object_store=FakeObjectStore(),
        publisher=publisher,
    )

    outcome = extractor.extract(
        fetched_event(),
        observed_at=datetime(2026, 6, 1, 4, tzinfo=UTC),
    )

    assert outcome.status == "published"
    assert outcome.body_length > 20
    assert [topic for topic, _ in publisher.events] == ["article_extracted"]
    event = publisher.events[0][1]
    assert event.schema_version == "article.extracted.v2"
    assert event.title == "Doanh nghiep tang truong"
    assert event.source_document_id == "source_doc_1"
    assert str(event.requested_url) == "https://vnexpress.net/a.html"
    assert str(event.canonical_url) == "https://vnexpress.net/a.html"
    assert event.extraction_status == "success"


def test_article_extractor_event_id_changes_for_new_source_document() -> None:
    publisher = FakePublisher()
    extractor = ArticleExtractor(
        object_store=FakeObjectStore(),
        publisher=publisher,
    )

    extractor.extract(
        fetched_event().model_copy(update={"source_document_id": "source_doc_1"}),
        observed_at=datetime(2026, 6, 1, 4, tzinfo=UTC),
    )
    extractor.extract(
        fetched_event().model_copy(update={"source_document_id": "source_doc_2"}),
        observed_at=datetime(2026, 6, 1, 5, tzinfo=UTC),
    )

    assert publisher.events[0][1].content_hash == publisher.events[1][1].content_hash
    assert publisher.events[0][1].event_id != publisher.events[1][1].event_id


def test_article_extractor_requires_canonical_url_when_configured() -> None:
    html = HTML.replace(
        b'<link rel="canonical" href="https://vnexpress.net/a.html">',
        b"",
    )
    extractor = ArticleExtractor(
        object_store=FakeObjectStore(html),
        publisher=FakePublisher(),
        source={
            "article": {
                "extractor": "html_article",
                "attribution_policy": "canonical_link_required",
            }
        },
    )

    with pytest.raises(IngestionError) as raised:
        extractor.extract(fetched_event())

    assert raised.value.stage == "article_extract"
    assert raised.value.retryable is False
    assert "canonical URL" in str(raised.value)


def test_article_extractor_rejects_missing_body() -> None:
    extractor = ArticleExtractor(
        object_store=FakeObjectStore(b"<html><head><title>Only title</title></head></html>"),
        publisher=FakePublisher(),
    )

    with pytest.raises(IngestionError) as raised:
        extractor.extract(fetched_event())

    assert raised.value.stage == "article_extract"
    assert raised.value.retryable is False


def test_article_extractor_rejects_failed_fetch_event() -> None:
    event = fetched_event().model_copy(update={"fetch_status": "failed", "payload_uri": None})
    extractor = ArticleExtractor(object_store=FakeObjectStore(), publisher=FakePublisher())

    with pytest.raises(IngestionError) as raised:
        extractor.extract(event)

    assert raised.value.stage == "article_extract"
    assert raised.value.retryable is False


def fetched_event() -> ArticleFetched:
    return ArticleFetched(
        schema_version="article.fetched.v2",
        event_id="event_article_fetched",
        event_time=datetime(2026, 6, 1, 3, tzinfo=UTC),
        run_id="rss_run_1",
        source_id="vnexpress",
        ingest_date=date(2026, 6, 1),
        article_id="article_1",
        requested_url="https://vnexpress.net/a.html",
        source_document_id="source_doc_1",
        fetched_at=datetime(2026, 6, 1, 3, tzinfo=UTC),
        status_code=200,
        content_type="text/html",
        content_length_bytes=len(HTML),
        payload_uri="s3://tgb-prod-landing-a7k3p9/payloads/article_html/a.html.zst",
        content_hash="abc123",
        fetch_status="success",
    )
