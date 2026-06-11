from __future__ import annotations

import html
import re
from datetime import datetime
from email.utils import parsedate_to_datetime
from typing import Any
from urllib.parse import urljoin, urlsplit

from bs4 import BeautifulSoup
from bs4.element import Tag

from news_article_extractor.models import ExtractedArticle, ExtractedImage, ExtractedTextBlock

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
    ".kbwscwlrl",
    ".kbwscwlrl-content",
    ".link-source-detail",
    ".google-news",
    ".gg-news",
    ".tagdetail",
    ".detail-tag",
]

DEFAULT_UNWRAP_SELECTORS = [
    "a.link-inline-content",
    "a.seo-suggest-link",
]

BLOCK_TAGS = {"p", "h2", "h3", "li", "blockquote", "figcaption"}
HEADING_TAGS = {"h2", "h3"}
BOILERPLATE_PATTERNS = [
    re.compile(pattern, re.IGNORECASE)
    for pattern in (
        r"^\s*>>\s*",
        r"^\s*(tin liên quan|tin lien quan|xem thêm|xem them|đọc tiếp|doc tiep)\b",
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

    remove_unwanted_nodes(soup, config)
    containers = select_containers(soup, config)
    blocks = content_blocks_from_containers(containers)
    images = extract_images(containers, fallback_url=fallback_url)
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
        images=images,
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
    return content_blocks_from_containers(select_containers(soup, config))


def content_blocks_from_containers(containers: list[Tag]) -> list[ExtractedTextBlock]:
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


def extract_images(containers: list[Tag], *, fallback_url: str) -> list[ExtractedImage]:
    images: list[ExtractedImage] = []
    seen_urls: set[str] = set()
    for container in containers:
        for node in container.find_all("img"):
            if not isinstance(node, Tag):
                continue
            url = image_url(node, fallback_url=fallback_url)
            if not url or url in seen_urls:
                continue
            seen_urls.add(url)
            images.append(
                ExtractedImage(
                    url=url,
                    alt=normalized_text(str(node.get("alt") or "")) or None,
                    caption=image_caption(node),
                    ordinal=len(images),
                )
            )
    return images


def image_url(node: Tag, *, fallback_url: str) -> str | None:
    for attribute in (
        "data-src",
        "data-original",
        "data-original-src",
        "data-lazy-src",
        "data-zoom-src",
        "data-thumb",
        "src",
        "data-srcset",
        "srcset",
    ):
        value = node.get(attribute)
        if not value:
            continue
        candidate = srcset_first_url(str(value)) if "srcset" in attribute else str(value)
        normalized = normalized_image_url(candidate, fallback_url=fallback_url)
        if normalized:
            return normalized
    return None


def srcset_first_url(value: str) -> str:
    first_candidate = value.split(",", maxsplit=1)[0].strip()
    return first_candidate.split(maxsplit=1)[0] if first_candidate else ""


def normalized_image_url(value: str, *, fallback_url: str) -> str | None:
    candidate = html.unescape(value).strip()
    if not candidate or candidate.startswith(("data:", "javascript:", "about:")):
        return None
    resolved = urljoin(fallback_url, candidate)
    parsed = urlsplit(resolved)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        return None
    return resolved


def image_caption(node: Tag) -> str | None:
    for ancestor in image_contexts(node):
        for selector in (
            "figcaption",
            ".PhotoCMS_Caption",
            ".caption",
            ".image_desc",
            ".figcaption",
            ".detail__caption",
            ".expEdit",
            ".cms-desc",
        ):
            caption_node = ancestor.select_one(selector)
            if caption_node and isinstance(caption_node, Tag):
                caption = normalized_text(caption_node.get_text(" ", strip=True))
                if valid_caption(caption):
                    return caption
        caption = normalized_text(ancestor.get_text(" ", strip=True))
        if valid_caption(caption):
            return caption
    return None


def image_contexts(node: Tag) -> list[Tag]:
    contexts: list[Tag] = []
    for ancestor in node.parents:
        if not isinstance(ancestor, Tag):
            continue
        classes = " ".join(ancestor.get("class", [])).lower()
        if ancestor.name == "figure" or any(
            marker in classes for marker in ("image", "photo", "picture", "caption", "sortable")
        ):
            contexts.append(ancestor)
        if len(contexts) >= 4:
            break
    return contexts


def valid_caption(value: str) -> bool:
    if len(value) < 5 or len(value) > 500:
        return False
    return not any(pattern.search(value) for pattern in BOILERPLATE_PATTERNS)


def remove_unwanted_nodes(soup: BeautifulSoup, config: dict[str, Any]) -> None:
    unwrap_selectors = [*DEFAULT_UNWRAP_SELECTORS, *string_list(config.get("unwrap_selectors"))]
    for selector in unwrap_selectors:
        for node in soup.select(selector):
            if should_unwrap_node(node):
                node.unwrap()

    selectors = [*DEFAULT_EXCLUDE_SELECTORS, *string_list(config.get("exclude_selectors"))]
    for selector in selectors:
        for node in soup.select(selector):
            node.decompose()


def should_unwrap_node(node: Tag) -> bool:
    return node.find(["img", "picture", "video", "iframe"]) is None


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
