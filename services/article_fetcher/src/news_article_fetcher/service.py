from __future__ import annotations

import hashlib
from dataclasses import asdict
from datetime import UTC, datetime

from news_platform.contracts.events import ArticleFetched, ArticleFetchRequested
from news_platform.ids import make_stable_id
from news_platform.storage import StorageLayout

from news_article_fetcher.http import ArticleHttpClient
from news_article_fetcher.models import ArticleFetchOutcome
from news_service_common.errors import IngestionError
from news_service_common.events import JsonEventPublisher
from news_service_common.stages import run_stage
from news_service_common.storage import S3PayloadStore


class ArticleFetcher:
    def __init__(
        self,
        *,
        http_client: ArticleHttpClient,
        object_store: S3PayloadStore,
        publisher: JsonEventPublisher,
        storage_layout: StorageLayout,
    ) -> None:
        self.http_client = http_client
        self.object_store = object_store
        self.publisher = publisher
        self.storage_layout = storage_layout

    def fetch(
        self,
        event: ArticleFetchRequested,
        *,
        observed_at: datetime | None = None,
    ) -> ArticleFetchOutcome:
        observed_at = observed_at or datetime.now(UTC)
        checkpoint_uri = self.storage_layout.article_fetch_checkpoint_uri(event.article_id)
        checkpoint = run_stage(
            "checkpoint_read",
            True,
            lambda: self._read_checkpoint(checkpoint_uri),
        )
        if checkpoint is not None:
            return run_stage(
                "checkpoint_read",
                False,
                lambda: ArticleFetchOutcome.from_checkpoint(checkpoint),
            )
        response = run_stage(
            "article_fetch",
            True,
            lambda: self.http_client.fetch(str(event.requested_url)),
        )
        content_hash = hashlib.sha256(response.content).hexdigest()
        source_document_id = make_stable_id("source_doc", event.article_id, content_hash)
        payload_uri = self.storage_layout.article_payload_uri(
            event.source_id,
            event.ingest_date,
            event.article_id,
            source_document_id,
        )
        payload_exists = run_stage(
            "payload_write", True, lambda: self.object_store.exists(payload_uri)
        )
        if not payload_exists:
            run_stage(
                "payload_write",
                True,
                lambda: self.object_store.write_compressed(
                    payload_uri,
                    response.content,
                    content_type=response.content_type or "text/html",
                ),
            )
        fetched_event = ArticleFetched(
            schema_version="article.fetched.v2",
            event_id=make_stable_id(
                "event",
                "article.fetched.v2",
                event.article_id,
                content_hash,
            ),
            event_time=observed_at,
            run_id=event.run_id,
            source_id=event.source_id,
            ingest_date=event.ingest_date,
            article_id=event.article_id,
            requested_url=event.requested_url,
            source_document_id=source_document_id,
            fetched_at=observed_at,
            status_code=response.status_code,
            content_type=response.content_type,
            content_length_bytes=len(response.content),
            payload_uri=payload_uri,
            content_hash=content_hash,
            fetch_status="success",
        )
        run_stage(
            "event_publish", True, lambda: self.publisher.publish("article_fetched", fetched_event)
        )
        run_stage("event_publish", True, self.publisher.flush)
        outcome = ArticleFetchOutcome(
            status="published",
            source_id=event.source_id,
            article_id=event.article_id,
            source_document_id=source_document_id,
            payload_uri=payload_uri,
            status_code=response.status_code,
            content_type=response.content_type,
            content_length_bytes=len(response.content),
            content_hash=content_hash,
        )
        run_stage(
            "checkpoint_write",
            True,
            lambda: self.object_store.write_json(checkpoint_uri, asdict(outcome)),
        )
        return outcome

    def _read_checkpoint(self, checkpoint_uri: str) -> dict | None:
        try:
            return self.object_store.read_json(checkpoint_uri)
        except (TypeError, ValueError) as error:
            raise IngestionError(
                stage="checkpoint_read",
                retryable=False,
                message=f"Invalid checkpoint at {checkpoint_uri}: {error}",
                error_class=type(error).__name__,
            ) from error
