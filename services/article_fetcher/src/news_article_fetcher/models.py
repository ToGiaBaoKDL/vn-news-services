from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Any


@dataclass(frozen=True)
class ArticleHttpResponse:
    status_code: int
    content: bytes
    content_type: str | None


@dataclass(frozen=True)
class ArticleFetchOutcome:
    status: str
    source_id: str
    article_id: str
    source_document_id: str
    payload_uri: str
    status_code: int
    content_type: str | None
    content_length_bytes: int
    content_hash: str

    @classmethod
    def from_checkpoint(cls, value: dict[str, Any]) -> ArticleFetchOutcome:
        return replace(cls(**value), status="already_processed")
