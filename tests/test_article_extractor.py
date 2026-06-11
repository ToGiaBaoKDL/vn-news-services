from __future__ import annotations

import hashlib
import json
from datetime import UTC, date, datetime

import pytest
from news_platform.contracts.events import ArticleFetched
from news_platform.storage import StorageLayout

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


SOURCE_EXTRACTION_FIXTURES = {
    "baochinhphu": {
        "domain": "baochinhphu.vn",
        "content_selectors": [".detail-content.afcbc-body", ".detail-content"],
        "exclude_selectors": [".kbwscwlrl-content"],
    },
    "cafef": {
        "domain": "cafef.vn",
        "content_selectors": [".detail-content.afcbc-body", "#mainContent"],
        "exclude_selectors": ["#listNewsInContent"],
    },
    "dantri": {
        "domain": "dantri.com.vn",
        "content_selectors": ["#desktop-in-article", '[data-slot="content"]'],
        "exclude_selectors": [".article-business"],
    },
    "genk": {
        "domain": "genk.vn",
        "content_selectors": ["#ContentDetail", ".knc-content.detail-content"],
        "exclude_selectors": [".link-source-detail"],
    },
    "kenh14": {
        "domain": "kenh14.vn",
        "content_selectors": [".detail-content.afcbc-body", ".detail-content"],
        "exclude_selectors": [".knc-relate-wrapper"],
    },
    "thanhnien": {
        "domain": "thanhnien.vn",
        "content_selectors": [".detail-content.afcbc-body", ".detail-content"],
        "exclude_selectors": [".seo-suggest-link"],
    },
    "tienphong": {
        "domain": "tienphong.vn",
        "content_selectors": [".article__body.cms-body", ".article__body"],
        "exclude_selectors": [".article-relate"],
    },
    "tuoitre": {
        "domain": "tuoitre.vn",
        "content_selectors": [".detail-content.afcbc-body", ".detail-content"],
        "exclude_selectors": [".link-inline-content"],
    },
    "vneconomy": {
        "domain": "vneconomy.vn",
        "content_selectors": [".ct-edtior-web.news-type1", ".ct-edtior-web"],
        "exclude_selectors": [".news-general"],
    },
    "vnexpress": {
        "domain": "vnexpress.net",
        "content_selectors": ["article.fck_detail", ".fck_detail"],
        "exclude_selectors": ["#article-end"],
    },
}


class FakeObjectStore:
    def __init__(self, payload: bytes = HTML) -> None:
        self.payload = payload
        self.objects: dict[str, tuple[bytes, str]] = {}

    def read_compressed(self, uri: str) -> bytes:
        return self.payload

    def exists(self, uri: str) -> bool:
        return uri in self.objects

    def write_compressed(self, uri: str, payload: bytes, *, content_type: str) -> None:
        self.objects[uri] = (payload, content_type)


class FakePublisher:
    def __init__(self) -> None:
        self.events: list[tuple[str, object]] = []

    def publish(self, topic_key: str, event: object) -> None:
        self.events.append((topic_key, event))

    def flush(self) -> None:
        return None


def storage_layout() -> StorageLayout:
    return StorageLayout(
        buckets={
            "landing": "tgb-prod-landing-a7k3p9",
            "curated": "tgb-prod-curated-m4q8x2",
            "analytics": "tgb-prod-analytics-r9v2c6",
        },
        payload_prefix="payloads",
    )


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


def test_extract_article_collects_article_images() -> None:
    html = b"""<!doctype html>
<html>
  <head>
    <link rel="canonical" href="https://vnexpress.net/a.html">
    <meta property="og:title" content="Doanh nghiep tang truong">
  </head>
  <body>
    <article class="fck_detail">
      <p>Day la doan noi dung dau tien cua bai viet kinh doanh.</p>
      <figure class="VCSortableInPreviewMode">
        <img
          src="data:image/gif;base64,R0lGODlhAQABAAAAACH5BAEKAAEALAAAAAABAAEAAAICTAEAOw=="
          data-src="/images/a.jpg"
          alt="Anh minh hoa"
        />
        <figcaption>Chu thich anh minh hoa co du do dai de trich xuat.</figcaption>
      </figure>
      <p>Day la doan noi dung thu hai voi them thong tin chi tiet.</p>
      <div class="related-news">
        <img src="https://cdn.example.test/related.jpg" alt="Tin lien quan" />
      </div>
    </article>
  </body>
</html>
"""

    article = extract_article(
        html,
        fallback_url="https://vnexpress.net/a.html",
        extraction_config={"exclude_selectors": [".related-news"]},
    )

    assert len(article.images) == 1
    assert article.images[0].url == "https://vnexpress.net/images/a.jpg"
    assert article.images[0].alt == "Anh minh hoa"
    assert article.images[0].caption == "Chu thich anh minh hoa co du do dai de trich xuat."
    assert article.images[0].ordinal == 0


def test_extract_article_preserves_inline_suggested_link_text() -> None:
    html = b"""<!doctype html>
<html>
  <head>
    <link rel="canonical" href="https://thanhnien.vn/a.html">
    <meta property="og:title" content="Tin thoi su">
  </head>
  <body>
    <article class="detail-content">
      <p>
        Day la doan noi dung co
        <a class="seo-suggest-link link-inline-content" href="/tag.html">tu khoa quan trong</a>
        trong cau.
      </p>
      <a class="link-inline-content" href="/related.html">
        <img src="https://cdn.example.test/related.jpg" alt="Tin goi y" />
      </a>
      <div class="link-inline-content"><p>BOILERPLATE_TIN_GOI_Y.</p></div>
      <p>Day la doan noi dung thu hai voi thong tin bo sung cho bai viet.</p>
    </article>
  </body>
</html>
"""

    article = extract_article(
        html,
        fallback_url="https://thanhnien.vn/a.html",
        extraction_config={"exclude_selectors": [".seo-suggest-link", ".link-inline-content"]},
    )

    assert "tu khoa quan trong" in article.body_text
    assert "BOILERPLATE_TIN_GOI_Y" not in article.body_text
    assert article.images == []


def test_extract_article_drops_recommendation_link_blocks() -> None:
    html = b"""<!doctype html>
<html>
  <head>
    <link rel="canonical" href="https://vnexpress.net/a.html">
    <meta property="og:title" content="Tin thu gian">
  </head>
  <body>
    <article class="fck_detail">
      <p>Day la doan noi dung chinh cua bai viet voi thong tin du dai.</p>
      <p>&gt;&gt; Cau chuyen goi y khac khong thuoc noi dung bai viet</p>
      <p>Xem them nhieu video va chuyen la khac tai day</p>
      <p>Day la doan noi dung tiep theo cua bai viet can duoc giu lai.</p>
    </article>
  </body>
</html>
"""

    article = extract_article(
        html,
        fallback_url="https://vnexpress.net/a.html",
    )

    assert "doan noi dung chinh" in article.body_text
    assert "doan noi dung tiep theo" in article.body_text
    assert "Cau chuyen goi y" not in article.body_text
    assert "Xem them nhieu video" not in article.body_text


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
        storage_layout=storage_layout(),
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
    assert event.images == []
    assert str(event.requested_url) == "https://vnexpress.net/a.html"
    assert str(event.canonical_url) == "https://vnexpress.net/a.html"
    assert event.extraction_status == "success"


def test_article_extractor_stores_oversized_extracted_payload() -> None:
    body = "".join(
        f"<p>Day la doan noi dung dai thu {index} voi thong tin phuc vu truy xuat.</p>"
        for index in range(20)
    )
    html = HTML.replace(
        b"<p>Day la doan noi dung dau tien cua bai viet kinh doanh.</p>\n"
        b"    <p>Day la doan noi dung thu hai voi them thong tin chi tiet.</p>",
        body.encode(),
    )
    object_store = FakeObjectStore(html)
    publisher = FakePublisher()
    extractor = ArticleExtractor(
        object_store=object_store,
        publisher=publisher,
        storage_layout=storage_layout(),
        max_inline_event_bytes=1300,
    )

    outcome = extractor.extract(fetched_event(html))

    event = publisher.events[0][1]
    assert outcome.extracted_payload_uri == event.extracted_payload_uri
    assert event.body_text == ""
    assert event.content_blocks == []
    assert event.images == []
    assert event.extracted_payload_uri is not None
    assert event.extracted_payload_hash is not None
    assert f"extracted_payload_hash={event.extracted_payload_hash}" in event.extracted_payload_uri
    payload, content_type = object_store.objects[event.extracted_payload_uri]
    payload_json = json.loads(payload)
    assert content_type == "application/vnd.vn-news.article-extracted+json"
    assert len(payload) > outcome.inline_event_bytes
    assert payload_json["body_text"].startswith("Day la doan noi dung dai thu 0")
    assert len(payload_json["content_blocks"]) == 20
    assert hashlib.sha256(payload).hexdigest() == event.extracted_payload_hash


def test_article_extractor_resolves_relative_canonical_url() -> None:
    html = HTML.replace(
        b'<link rel="canonical" href="https://vnexpress.net/a.html">',
        b'<link rel="canonical" href="/oto-xe-may/v-car/article.html">',
    )
    publisher = FakePublisher()
    extractor = ArticleExtractor(
        object_store=FakeObjectStore(html),
        publisher=publisher,
        storage_layout=storage_layout(),
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
        storage_layout=storage_layout(),
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
        storage_layout=storage_layout(),
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
        storage_layout=storage_layout(),
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
    source = SOURCE_EXTRACTION_FIXTURES[source_id]
    article = extract_article(
        source_layout_fixture(source_id),
        fallback_url=f"https://{source['domain']}/fixture.html",
        require_canonical_url=True,
        extraction_config={
            "content_selectors": source["content_selectors"],
            "exclude_selectors": source["exclude_selectors"],
            "min_text_chars": 120,
        },
    )

    assert article.rejection_reason is None
    assert article.body_text is not None
    assert f"NOI_DUNG_CHINH_{source_id}" in article.body_text
    assert f"BOILERPLATE_{source_id}" not in article.body_text
    assert len(article.content_blocks) >= 2
    assert [block.ordinal for block in article.content_blocks] == list(
        range(len(article.content_blocks))
    )
    assert [image.ordinal for image in article.images] == list(range(len(article.images)))


def test_article_extractor_rejects_payload_hash_mismatch() -> None:
    extractor = ArticleExtractor(
        object_store=FakeObjectStore(b"<html>unexpected</html>"),
        publisher=FakePublisher(),
        storage_layout=storage_layout(),
    )

    with pytest.raises(IngestionError) as raised:
        extractor.extract(fetched_event())

    assert raised.value.stage == "payload_read"
    assert raised.value.retryable is False


def test_article_extractor_rejects_failed_fetch_event() -> None:
    event = fetched_event().model_copy(update={"fetch_status": "failed", "payload_uri": None})
    extractor = ArticleExtractor(
        object_store=FakeObjectStore(),
        publisher=FakePublisher(),
        storage_layout=storage_layout(),
    )

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
