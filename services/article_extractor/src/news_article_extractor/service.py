from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from urllib.parse import urljoin, urlsplit

from news_platform.contracts.events import (
    ArticleExtracted,
    ArticleFetched,
    ArticleImage,
    ArticleTextBlock,
)
from news_platform.ids import make_stable_id, normalize_article_url
from news_platform.storage import StorageLayout

from news_article_extractor.models import ArticleExtractOutcome
from news_article_extractor.parser import extract_article
from news_service_common.errors import IngestionError
from news_service_common.events import JsonEventPublisher, event_json_bytes
from news_service_common.stages import run_stage
from news_service_common.storage import S3PayloadStore

DEFAULT_ARTICLE_EXTRACTED_MAX_BYTES = 524288
EXTRACTED_PAYLOAD_CONTENT_TYPE = "application/vnd.vn-news.article-extracted+json"
SCHEMA_REGISTRY_FRAME_BYTES = 5


class ArticleExtractor:
    def __init__(
        self,
        *,
        object_store: S3PayloadStore,
        publisher: JsonEventPublisher,
        storage_layout: StorageLayout,
        max_inline_event_bytes: int = DEFAULT_ARTICLE_EXTRACTED_MAX_BYTES,
        source: dict | None = None,
    ) -> None:
        self.object_store = object_store
        self.publisher = publisher
        self.storage_layout = storage_layout
        self.max_inline_event_bytes = max_inline_event_bytes
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
                extraction_config=article_config.get("extraction"),
            ),
        )
        if require_canonical_url and not article.canonical_url:
            raise IngestionError(
                stage="article_extract",
                retryable=False,
                message="Article extraction did not produce required canonical URL",
            )
        if article.rejection_reason:
            raise IngestionError(
                stage="article_extract",
                retryable=False,
                message=f"Article extraction rejected document: {article.rejection_reason}",
            )
        if not article.title or not article.body_text:
            raise IngestionError(
                stage="article_extract",
                retryable=False,
                message="Article extraction did not produce required title and body_text",
            )
        canonical_url = resolved_article_url(
            article.canonical_url,
            fallback_url=str(event.requested_url),
        )
        content_hash = hashlib.sha256(article.body_text.encode()).hexdigest()
        full_event = run_stage(
            "article_extract",
            False,
            lambda: ArticleExtracted(
                schema_version="article.extracted.v3",
                event_id=make_stable_id(
                    "event",
                    "article.extracted.v3",
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
                canonical_url=canonical_url,
                title=article.title,
                summary=article.summary,
                body_text=article.body_text,
                content_blocks=[
                    ArticleTextBlock(
                        type=block.type,
                        text=block.text,
                        ordinal=block.ordinal,
                    )
                    for block in article.content_blocks
                ],
                images=[
                    ArticleImage(
                        url=image.url,
                        alt=image.alt,
                        caption=image.caption,
                        ordinal=image.ordinal,
                    )
                    for image in article.images
                ],
                author=article.author,
                published_at=article.published_at,
                content_hash=content_hash,
                source_document_id=event.source_document_id,
                source_payload_uri=event.payload_uri,
                extractor_version=article.extractor_version,
                extraction_status="success",
            ),
        )
        extracted_event, inline_event_bytes = self._bounded_extracted_event(full_event)
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
            block_count=len(article.content_blocks),
            image_count=len(article.images),
            content_hash=content_hash,
            inline_event_bytes=inline_event_bytes,
            extracted_payload_uri=extracted_event.extracted_payload_uri,
        )

    def _bounded_extracted_event(self, event: ArticleExtracted) -> tuple[ArticleExtracted, int]:
        inline_bytes = event_json_bytes(event)
        inline_wire_bytes = event_wire_bytes(inline_bytes)
        if inline_wire_bytes <= self.max_inline_event_bytes:
            return event, inline_wire_bytes

        payload_hash = hashlib.sha256(inline_bytes).hexdigest()
        payload_uri = self.storage_layout.extracted_payload_uri(
            event.source_id,
            event.ingest_date,
            event.article_id,
            event.source_document_id,
            payload_hash,
        )
        payload_exists = run_stage(
            "payload_write",
            True,
            lambda: self.object_store.exists(payload_uri),
        )
        if not payload_exists:
            run_stage(
                "payload_write",
                True,
                lambda: self.object_store.write_compressed(
                    payload_uri,
                    inline_bytes,
                    content_type=EXTRACTED_PAYLOAD_CONTENT_TYPE,
                ),
            )

        slim_event = event.model_copy(
            update={
                "body_text": "",
                "content_blocks": [],
                "images": [],
                "extracted_payload_uri": payload_uri,
                "extracted_payload_hash": payload_hash,
            },
        )
        slim_wire_bytes = event_wire_bytes(event_json_bytes(slim_event))
        if slim_wire_bytes > self.max_inline_event_bytes:
            raise IngestionError(
                stage="article_extract",
                retryable=False,
                message=(
                    "Slim extracted event exceeds configured inline limit: "
                    f"{slim_wire_bytes} > {self.max_inline_event_bytes}"
                ),
            )
        return slim_event, slim_wire_bytes


def article_extracted_max_bytes(config: dict) -> int:
    return int(
        config.get("event_bus", {})
        .get("inline_event_limits", {})
        .get("article_extracted_max_bytes", DEFAULT_ARTICLE_EXTRACTED_MAX_BYTES)
    )


def event_wire_bytes(json_payload: bytes) -> int:
    return len(json_payload) + SCHEMA_REGISTRY_FRAME_BYTES


def resolved_article_url(candidate_url: str | None, *, fallback_url: str) -> str:
    resolved_url = urljoin(fallback_url, candidate_url or fallback_url)
    normalized_url = normalize_article_url(resolved_url)
    parsed = urlsplit(normalized_url)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise IngestionError(
            stage="article_extract",
            retryable=False,
            message=f"Article extraction produced invalid canonical URL: {candidate_url}",
        )
    return normalized_url
