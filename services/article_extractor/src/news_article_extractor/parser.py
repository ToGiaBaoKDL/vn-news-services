from __future__ import annotations

import html
import re
from datetime import datetime
from email.utils import parsedate_to_datetime
from typing import Any

from bs4 import BeautifulSoup
from bs4.element import Tag

from news_article_extractor.models import ExtractedArticle, ExtractedTextBlock

EXTRACTOR_VERSION = "html_article_blocks_v1"

DEFAULT_CONTENT_SELECTORS = [
    "article",
    ".fck_detail",
    ".detail-content",
    ".afcbc-body",
    "#ContentDetail",
    "#mainContent",
    "#desktop-in-article",
    ".article__body",
    ".ct-edtior-web",
]

DEFAULT_EXCLUDE_SELECTORS = [
    "script",
    "style",
    "noscript",
    "iframe",
    "form",
    "button",
    "nav",
    "header",
    "footer",
    "aside",
    ".ads",
    ".ads_detail",
    ".banner",
    ".social",
    ".social-wrapper",
    ".detail-social",
    ".article__social",
    ".related-news",
    ".article-relate",
    ".relationnews",
    ".link-source-detail",
    ".google-news",
    ".gg-news",
    ".tagdetail",
    ".detail-tag",
]

BLOCK_TAGS = {"p", "h2", "h3", "li", "blockquote", "figcaption"}
HEADING_TAGS = {"h2", "h3"}
BOILERPLATE_PATTERNS = [
    re.compile(pattern, re.IGNORECASE)
    for pattern in (
        r"^\s*(tin liên quan|xem thêm|đọc tiếp)\s*$",
        r"^\s*(theo dõi|bấm theo dõi|mời bạn đọc)\b",
        r"^\s*(nguồn|source)\s*:",
        r"^\s*chia sẻ\s*$",
        r"^\s*google news\s*$",
    )
]


def extract_article(
    payload: bytes,
    *,
    fallback_url: str,
    require_canonical_url: bool = False,
    extraction_config: dict[str, Any] | None = None,
) -> ExtractedArticle:
    config = extraction_config or {}
    soup = BeautifulSoup(payload.decode("utf-8", errors="replace"), "html.parser")
    title = first_present(
        meta_content(soup, "og:title"),
        meta_content(soup, "twitter:title"),
        normalized_text(soup.title.get_text(" ", strip=True) if soup.title else None),
    )
    summary = first_present(
        meta_content(soup, "og:description"),
        meta_content(soup, "description"),
        meta_content(soup, "twitter:description"),
    )
    author = first_present(
        meta_content(soup, "author"),
        meta_content(soup, "article:author"),
        meta_content(soup, "byl"),
    )
    published_at = first_datetime(
        meta_content(soup, "article:published_time"),
        meta_content(soup, "pubdate"),
        meta_content(soup, "publishdate"),
        meta_content(soup, "date"),
    )
    canonical_candidate = first_present(
        canonical_link(soup),
        meta_content(soup, "og:url"),
        meta_content(soup, "twitter:url"),
    )
    canonical_url = canonical_candidate or (None if require_canonical_url else fallback_url)

    blocks = extract_content_blocks(soup, config)
    body_text = "\n\n".join(block.text for block in blocks) or None
    rejection_reason = content_rejection_reason(body_text, blocks, config)
    if rejection_reason:
        body_text = None

    return ExtractedArticle(
        canonical_url=canonical_url,
        title=clean_title(title),
        summary=summary,
        body_text=body_text,
        content_blocks=blocks,
        author=author,
        published_at=published_at,
        extractor_version=EXTRACTOR_VERSION,
        rejection_reason=rejection_reason,
    )


def meta_content(soup: BeautifulSoup, key: str) -> str | None:
    key = key.lower()
    for node in soup.find_all("meta"):
        if not isinstance(node, Tag):
            continue
        candidate = str(node.get("property") or node.get("name") or "").lower()
        if candidate == key:
            return normalized_text(str(node.get("content") or ""))
    return None


def canonical_link(soup: BeautifulSoup) -> str | None:
    for node in soup.find_all("link"):
        if not isinstance(node, Tag):
            continue
        rel = node.get("rel") or []
        rel_values = rel if isinstance(rel, list) else str(rel).split()
        if "canonical" in {value.lower() for value in rel_values}:
            href = node.get("href")
            if href:
                return normalized_text(str(href))
    return None


def extract_content_blocks(
    soup: BeautifulSoup,
    config: dict[str, Any],
) -> list[ExtractedTextBlock]:
    remove_unwanted_nodes(soup, config)
    containers = select_containers(soup, config)
    blocks: list[ExtractedTextBlock] = []
    seen: set[str] = set()

    for container in containers:
        for node in container.find_all(BLOCK_TAGS):
            if not isinstance(node, Tag) or has_block_ancestor(node, container):
                continue
            text = normalized_text(node.get_text(" ", strip=True))
            if should_skip_block(text):
                continue
            dedupe_key = re.sub(r"\W+", "", text.lower())
            if dedupe_key in seen:
                continue
            seen.add(dedupe_key)
            blocks.append(
                ExtractedTextBlock(
                    type=block_type(node),
                    text=text,
                    ordinal=len(blocks),
                )
            )
    return blocks


def remove_unwanted_nodes(soup: BeautifulSoup, config: dict[str, Any]) -> None:
    selectors = [*DEFAULT_EXCLUDE_SELECTORS, *string_list(config.get("exclude_selectors"))]
    for selector in selectors:
        for node in soup.select(selector):
            node.decompose()


def select_containers(soup: BeautifulSoup, config: dict[str, Any]) -> list[Tag]:
    selectors = string_list(config.get("content_selectors")) or DEFAULT_CONTENT_SELECTORS
    containers: list[Tag] = []
    for selector in selectors:
        containers.extend(node for node in soup.select(selector) if isinstance(node, Tag))
    if not containers and soup.body:
        containers = [soup.body]
    return without_nested_duplicates(containers)


def without_nested_duplicates(containers: list[Tag]) -> list[Tag]:
    unique: list[Tag] = []
    seen_ids: set[int] = set()
    for container in containers:
        if id(container) in seen_ids:
            continue
        if any(parent in unique for parent in container.parents):
            continue
        seen_ids.add(id(container))
        unique.append(container)
    return unique


def has_block_ancestor(node: Tag, container: Tag) -> bool:
    for parent in node.parents:
        if parent is container:
            return False
        if isinstance(parent, Tag) and parent.name in BLOCK_TAGS:
            return True
    return False


def block_type(node: Tag) -> str:
    if node.name in HEADING_TAGS:
        return "heading"
    if node.name == "li":
        return "list_item"
    if node.name == "blockquote":
        return "quote"
    if node.name == "figcaption" or has_caption_marker(node):
        return "caption"
    return "paragraph"


def has_caption_marker(node: Tag) -> bool:
    for parent in [node, *node.parents]:
        if not isinstance(parent, Tag):
            continue
        classes = " ".join(parent.get("class", []))
        if "caption" in classes.lower() or "PhotoCMS_Caption" in classes:
            return True
    return False


def should_skip_block(text: str) -> bool:
    if len(text) < 20:
        return True
    return any(pattern.search(text) for pattern in BOILERPLATE_PATTERNS)


def content_rejection_reason(
    body_text: str | None,
    blocks: list[ExtractedTextBlock],
    config: dict[str, Any],
) -> str | None:
    min_text_chars = int(config.get("min_text_chars", 80))
    min_blocks = int(config.get("min_blocks", 1))
    if not body_text:
        return "no content blocks matched configured selectors"
    if len(body_text) < min_text_chars:
        return f"extracted text is too short: {len(body_text)} < {min_text_chars}"
    if len(blocks) < min_blocks:
        return f"too few semantic blocks: {len(blocks)} < {min_blocks}"
    if boilerplate_dominated(body_text, config):
        return "extracted text is boilerplate dominated"
    return None


def boilerplate_dominated(body_text: str, config: dict[str, Any]) -> bool:
    markers = [marker.lower() for marker in string_list(config.get("boilerplate_markers"))]
    if not markers:
        return False
    lowered = body_text.lower()
    hits = sum(lowered.count(marker) for marker in markers)
    return hits >= 3 and len(body_text) < 1000


def clean_title(value: str | None) -> str | None:
    if not value:
        return None
    return normalized_text(re.split(r"\s+[-|]\s+", value, maxsplit=1)[0])


def first_present(*values: str | None) -> str | None:
    for value in values:
        if value:
            return value
    return None


def first_datetime(*values: str | None) -> datetime | None:
    for value in values:
        parsed = parse_datetime(value)
        if parsed:
            return parsed
    return None


def parse_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    normalized = value.strip()
    try:
        return datetime.fromisoformat(normalized.replace("Z", "+00:00"))
    except ValueError:
        pass
    try:
        return parsedate_to_datetime(normalized)
    except (TypeError, ValueError):
        return None


def normalized_text(value: str | None) -> str:
    return re.sub(r"\s+", " ", html.unescape(value or "")).strip()


def string_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [item.strip() for item in value if isinstance(item, str) and item.strip()]
