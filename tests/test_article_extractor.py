from __future__ import annotations

import hashlib
from datetime import UTC, date, datetime

import pytest
from news_platform.config import load_sources
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
    assert [block.type for block in article.content_blocks] == ["paragraph", "paragraph"]
    assert article.content_blocks[0].ordinal == 0


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


@pytest.mark.parametrize(
    ("source_id", "domain", "title_meta", "canonical_tag"),
    [
        ("baochinhphu", "baochinhphu.vn", "og:title", "link"),
        ("cafef", "cafef.vn", "twitter:title", "og:url"),
        ("dantri", "dantri.com.vn", "og:title", "link"),
        ("genk", "genk.vn", "twitter:title", "link"),
        ("kenh14", "kenh14.vn", "og:title", "og:url"),
        ("thanhnien", "thanhnien.vn", "twitter:title", "link"),
        ("tienphong", "tienphong.vn", "og:title", "link"),
        ("tuoitre", "tuoitre.vn", "og:title", "og:url"),
        ("vneconomy", "vneconomy.vn", "twitter:title", "link"),
        ("vnexpress", "vnexpress.net", "og:title", "link"),
    ],
)
def test_extract_article_source_fixtures_require_canonical_url(
    source_id: str,
    domain: str,
    title_meta: str,
    canonical_tag: str,
) -> None:
    canonical_url = f"https://{domain}/fixture-{source_id}.html"
    article = extract_article(
        source_fixture_html(
            source_id=source_id,
            domain=domain,
            title_meta=title_meta,
            canonical_tag=canonical_tag,
        ),
        fallback_url=f"https://{domain}/fallback.html",
        require_canonical_url=True,
    )

    assert article.canonical_url == canonical_url
    assert article.title == f"Tieu de {source_id}"
    assert f"noi dung {source_id}" in article.body_text


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
    assert outcome.block_count == 2
    assert [topic for topic, _ in publisher.events] == ["article_extracted"]
    event = publisher.events[0][1]
    assert event.schema_version == "article.extracted.v3"
    assert event.title == "Doanh nghiep tang truong"
    assert event.source_document_id == "source_doc_1"
    assert event.source_payload_uri == fetched_event().payload_uri
    assert event.extractor_version == "html_article_blocks_v1"
    assert event.content_blocks[0].text.startswith("Day la doan noi dung dau tien")
    assert str(event.requested_url) == "https://vnexpress.net/a.html"
    assert str(event.canonical_url) == "https://vnexpress.net/a.html"
    assert event.extraction_status == "success"


def test_article_extractor_resolves_relative_canonical_url() -> None:
    html = HTML.replace(
        b'<link rel="canonical" href="https://vnexpress.net/a.html">',
        b'<link rel="canonical" href="/oto-xe-may/v-car/article.html">',
    )
    publisher = FakePublisher()
    extractor = ArticleExtractor(
        object_store=FakeObjectStore(html),
        publisher=publisher,
        source={
            "article": {
                "extractor": "html_article",
                "attribution_policy": "canonical_link_required",
            }
        },
    )

    extractor.extract(fetched_event(html))

    event = publisher.events[0][1]
    assert str(event.canonical_url) == "https://vnexpress.net/oto-xe-may/v-car/article.html"


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
        extractor.extract(fetched_event(html))

    assert raised.value.stage == "article_extract"
    assert raised.value.retryable is False
    assert "canonical URL" in str(raised.value)


def test_article_extractor_rejects_missing_body() -> None:
    html = b"<html><head><title>Only title</title></head></html>"
    extractor = ArticleExtractor(
        object_store=FakeObjectStore(html),
        publisher=FakePublisher(),
    )

    with pytest.raises(IngestionError) as raised:
        extractor.extract(fetched_event(html))

    assert raised.value.stage == "article_extract"
    assert raised.value.retryable is False
    assert "rejected document" in str(raised.value)


@pytest.mark.parametrize(
    "source_id",
    [
        "baochinhphu",
        "cafef",
        "dantri",
        "genk",
        "kenh14",
        "thanhnien",
        "tienphong",
        "tuoitre",
        "vneconomy",
        "vnexpress",
    ],
)
def test_source_configured_extraction_selectors_keep_content_and_drop_boilerplate(
    source_id: str,
) -> None:
    source = {source["source_id"]: source for source in load_sources()}[source_id]
    article = extract_article(
        source_layout_fixture(source_id),
        fallback_url=f"https://{source['domain']}/fixture.html",
        require_canonical_url=True,
        extraction_config=source["article"]["extraction"],
    )

    assert article.rejection_reason is None
    assert article.body_text is not None
    assert f"NOI_DUNG_CHINH_{source_id}" in article.body_text
    assert f"BOILERPLATE_{source_id}" not in article.body_text
    assert len(article.content_blocks) >= 2
    assert [block.ordinal for block in article.content_blocks] == list(
        range(len(article.content_blocks))
    )


def test_article_extractor_rejects_payload_hash_mismatch() -> None:
    extractor = ArticleExtractor(
        object_store=FakeObjectStore(b"<html>unexpected</html>"),
        publisher=FakePublisher(),
    )

    with pytest.raises(IngestionError) as raised:
        extractor.extract(fetched_event())

    assert raised.value.stage == "payload_read"
    assert raised.value.retryable is False


def test_article_extractor_rejects_failed_fetch_event() -> None:
    event = fetched_event().model_copy(update={"fetch_status": "failed", "payload_uri": None})
    extractor = ArticleExtractor(object_store=FakeObjectStore(), publisher=FakePublisher())

    with pytest.raises(IngestionError) as raised:
        extractor.extract(event)

    assert raised.value.stage == "article_extract"
    assert raised.value.retryable is False


def fetched_event(payload: bytes = HTML) -> ArticleFetched:
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
        content_hash=hashlib.sha256(payload).hexdigest(),
        fetch_status="success",
    )


def source_fixture_html(
    *,
    source_id: str,
    domain: str,
    title_meta: str,
    canonical_tag: str,
) -> bytes:
    canonical_url = f"https://{domain}/fixture-{source_id}.html"
    title_tag = (
        f'<meta property="{title_meta}" content="Tieu de {source_id}">'
        if title_meta.startswith("og:")
        else f'<meta name="{title_meta}" content="Tieu de {source_id}">'
    )
    canonical = (
        f'<link rel="canonical" href="{canonical_url}">'
        if canonical_tag == "link"
        else f'<meta property="og:url" content="{canonical_url}">'
    )
    return f"""<!doctype html>
<html>
  <head>
    {canonical}
    {title_tag}
    <meta name="description" content="Tom tat {source_id}">
    <meta name="author" content="Tac gia {source_id}">
  </head>
  <body>
    <p>Day la doan noi dung {source_id} dau tien du dai de trich xuat.</p>
    <p>Day la doan noi dung {source_id} thu hai de kiem tra ghep van ban.</p>
  </body>
</html>
""".encode()


def source_layout_fixture(source_id: str) -> bytes:
    fixtures = {
        "baochinhphu": """
          <div class="detail-content afcbc-body clearfix">
            <p>NOI_DUNG_CHINH_baochinhphu doan mot du dai de kiem tra trich xuat.</p>
            <p>NOI_DUNG_CHINH_baochinhphu doan hai giu dung thu tu ngu nghia.</p>
            <div class="kbwscwlrl-content"><p>BOILERPLATE_baochinhphu tin lien quan.</p></div>
          </div>
        """,
        "cafef": """
          <div id="mainContent">
            <div class="detail-content afcbc-body">
              <p>NOI_DUNG_CHINH_cafef doan mot du dai de kiem tra trich xuat day du.</p>
              <p>NOI_DUNG_CHINH_cafef doan hai giu dung thu tu ngu nghia on dinh.</p>
              <div id="listNewsInContent"><p>BOILERPLATE_cafef tin lien quan.</p></div>
            </div>
          </div>
        """,
        "dantri": """
          <article data-slot="container">
            <div id="desktop-in-article" data-slot="content">
              <p>NOI_DUNG_CHINH_dantri doan mot du dai de kiem tra trich xuat.</p>
              <p>NOI_DUNG_CHINH_dantri doan hai giu dung thu tu ngu nghia.</p>
              <section class="article-business"><p>BOILERPLATE_dantri doanh nghiep.</p></section>
            </div>
          </article>
        """,
        "genk": """
          <div id="ContentDetail" class="knc-content detail-content">
            <p>NOI_DUNG_CHINH_genk doan mot du dai de kiem tra trich xuat day du.</p>
            <p>NOI_DUNG_CHINH_genk doan hai giu dung thu tu ngu nghia on dinh.</p>
            <div class="link-source-detail"><p>BOILERPLATE_genk nguon bai viet.</p></div>
          </div>
        """,
        "kenh14": """
          <div class="detail-content afcbc-body">
            <p>NOI_DUNG_CHINH_kenh14 doan mot du dai de kiem tra trich xuat.</p>
            <p>NOI_DUNG_CHINH_kenh14 doan hai giu dung thu tu ngu nghia.</p>
            <div class="knc-relate-wrapper"><p>BOILERPLATE_kenh14 tin lien quan.</p></div>
          </div>
        """,
        "thanhnien": """
          <div class="detail-content afcbc-body">
            <p>NOI_DUNG_CHINH_thanhnien doan mot du dai de kiem tra trich xuat.</p>
            <p>NOI_DUNG_CHINH_thanhnien doan hai giu dung thu tu ngu nghia.</p>
            <div class="seo-suggest-link"><p>BOILERPLATE_thanhnien goi y lien ket.</p></div>
          </div>
        """,
        "tienphong": """
          <div class="article__body zce-content-body cms-body">
            <p>NOI_DUNG_CHINH_tienphong doan mot du dai de kiem tra trich xuat.</p>
            <p>NOI_DUNG_CHINH_tienphong doan hai giu dung thu tu ngu nghia.</p>
            <div class="article-relate"><p>BOILERPLATE_tienphong tin lien quan.</p></div>
          </div>
        """,
        "tuoitre": """
          <div class="detail-content afcbc-body">
            <p>NOI_DUNG_CHINH_tuoitre doan mot du dai de kiem tra trich xuat.</p>
            <p>NOI_DUNG_CHINH_tuoitre doan hai giu dung thu tu ngu nghia.</p>
            <div class="link-inline-content"><p>BOILERPLATE_tuoitre lien ket noi dung.</p></div>
          </div>
        """,
        "vneconomy": """
          <div class="ct-edtior-web news-type1">
            <p>NOI_DUNG_CHINH_vneconomy doan mot du dai de kiem tra trich xuat.</p>
            <p>NOI_DUNG_CHINH_vneconomy doan hai giu dung thu tu ngu nghia.</p>
            <div class="news-general"><p>BOILERPLATE_vneconomy tin doc them.</p></div>
          </div>
        """,
        "vnexpress": """
          <article class="fck_detail">
            <p>NOI_DUNG_CHINH_vnexpress doan mot du dai de kiem tra trich xuat.</p>
            <p>NOI_DUNG_CHINH_vnexpress doan hai giu dung thu tu ngu nghia.</p>
            <div id="article-end"><p>BOILERPLATE_vnexpress ket thuc bai viet.</p></div>
          </article>
        """,
    }
    domain = "example.test"
    return f"""<!doctype html>
<html>
  <head>
    <link rel="canonical" href="https://{domain}/{source_id}.html">
    <meta property="og:title" content="Tieu de {source_id}">
  </head>
  <body>
    {fixtures[source_id]}
  </body>
</html>
""".encode()
