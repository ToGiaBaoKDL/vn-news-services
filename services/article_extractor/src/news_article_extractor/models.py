from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class ExtractedTextBlock:
    type: str
    text: str
    ordinal: int


@dataclass(frozen=True)
class ExtractedArticle:
    canonical_url: str | None
    title: str | None
    summary: str | None
    body_text: str | None
    content_blocks: list[ExtractedTextBlock]
    author: str | None
    published_at: datetime | None
    extractor_version: str
    rejection_reason: str | None = None


@dataclass(frozen=True)
class ArticleExtractOutcome:
    status: str
    source_id: str
    article_id: str
    title: str
    body_length: int
    block_count: int
    content_hash: str
