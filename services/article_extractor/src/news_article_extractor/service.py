from __future__ import annotations

import hashlib
from datetime import UTC, datetime

from news_platform.contracts.events import ArticleExtracted, ArticleFetched
from news_platform.ids import make_stable_id, normalize_article_url

from news_article_extractor.models import ArticleExtractOutcome
from news_article_extractor.parser import extract_article
from news_service_common.errors import IngestionError
from news_service_common.events import JsonEventPublisher
from news_service_common.stages import run_stage
from news_service_common.storage import S3PayloadStore


class ArticleExtractor:
    def __init__(
        self,
        *,
        object_store: S3PayloadStore,
        publisher: JsonEventPublisher,
        source: dict | None = None,
    ) -> None:
        self.object_store = object_store
        self.publisher = publisher
        self.source = source or {}

    def extract(
        self,
        event: ArticleFetched,
        *,
        observed_at: datetime | None = None,
    ) -> ArticleExtractOutcome:
        observed_at = observed_at or datetime.now(UTC)
        if event.fetch_status != "success" or not event.payload_uri:
            raise IngestionError(
                stage="article_extract",
                retryable=False,
                message="Cannot extract article without a successful fetched payload",
            )
        payload = run_stage(
            "payload_read", True, lambda: self.object_store.read_compressed(event.payload_uri)
        )
        payload_hash = hashlib.sha256(payload).hexdigest()
        if payload_hash != event.content_hash:
            raise IngestionError(
                stage="payload_read",
                retryable=False,
                message="Fetched article payload does not match its content hash",
            )
        article_config = self.source.get("article", {})
        extractor_name = article_config.get("extractor", "html_article")
        if extractor_name != "html_article":
            raise IngestionError(
                stage="article_extract",
                retryable=False,
                message=f"Unsupported article extractor: {extractor_name}",
            )
        require_canonical_url = (
            article_config.get("attribution_policy") == "canonical_link_required"
        )
        article = run_stage(
            "article_extract",
            False,
            lambda: extract_article(
                payload,
                fallback_url=str(event.requested_url),
                require_canonical_url=require_canonical_url,
            ),
        )
        if require_canonical_url and not article.canonical_url:
            raise IngestionError(
                stage="article_extract",
                retryable=False,
                message="Article extraction did not produce required canonical URL",
            )
        if not article.title or not article.body_text:
            raise IngestionError(
                stage="article_extract",
                retryable=False,
                message="Article extraction did not produce required title and body_text",
            )
        content_hash = hashlib.sha256(article.body_text.encode()).hexdigest()
        extracted_event = ArticleExtracted(
            schema_version="article.extracted.v2",
            event_id=make_stable_id(
                "event",
                "article.extracted.v2",
                event.article_id,
                event.source_document_id,
                content_hash,
            ),
            event_time=observed_at,
            run_id=event.run_id,
            source_id=event.source_id,
            ingest_date=event.ingest_date,
            article_id=event.article_id,
            requested_url=event.requested_url,
            canonical_url=normalize_article_url(str(article.canonical_url or event.requested_url)),
            title=article.title,
            summary=article.summary,
            body_text=article.body_text,
            author=article.author,
            published_at=article.published_at,
            content_hash=content_hash,
            source_document_id=event.source_document_id,
            extraction_status="success",
        )
        run_stage(
            "event_publish",
            True,
            lambda: self.publisher.publish("article_extracted", extracted_event),
        )
        run_stage("event_publish", True, self.publisher.flush)
        return ArticleExtractOutcome(
            status="published",
            source_id=event.source_id,
            article_id=event.article_id,
            title=article.title,
            body_length=len(article.body_text),
            content_hash=content_hash,
        )
