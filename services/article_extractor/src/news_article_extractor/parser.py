from __future__ import annotations

import html
import re
from datetime import datetime
from email.utils import parsedate_to_datetime
from html.parser import HTMLParser

from news_article_extractor.models import ExtractedArticle


class ArticleHTMLParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.in_title = False
        self.in_paragraph = False
        self.skip_depth = 0
        self.title_parts: list[str] = []
        self.paragraph_parts: list[str] = []
        self.current_paragraph: list[str] = []
        self.meta: dict[str, str] = {}
        self.canonical_url: str | None = None

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attrs_dict = {key.lower(): value or "" for key, value in attrs}
        if tag in {"script", "style", "noscript"}:
            self.skip_depth += 1
            return
        if self.skip_depth:
            return
        if tag == "title":
            self.in_title = True
        elif tag == "p":
            self.in_paragraph = True
            self.current_paragraph = []
        elif tag == "br" and self.in_paragraph:
            self.current_paragraph.append(" ")
        elif tag == "meta":
            key = attrs_dict.get("property") or attrs_dict.get("name")
            content = attrs_dict.get("content")
            if key and content:
                self.meta[key.lower()] = normalized_text(content)
        elif tag == "link" and "canonical" in attrs_dict.get("rel", "").lower():
            href = attrs_dict.get("href")
            if href:
                self.canonical_url = normalized_text(href)

    def handle_endtag(self, tag: str) -> None:
        if tag in {"script", "style", "noscript"} and self.skip_depth:
            self.skip_depth -= 1
            return
        if self.skip_depth:
            return
        if tag == "title":
            self.in_title = False
        elif tag == "p" and self.in_paragraph:
            paragraph = normalized_text("".join(self.current_paragraph))
            if len(paragraph) >= 20:
                self.paragraph_parts.append(paragraph)
            self.in_paragraph = False
            self.current_paragraph = []

    def handle_data(self, data: str) -> None:
        if self.skip_depth:
            return
        if self.in_title:
            self.title_parts.append(data)
        if self.in_paragraph:
            self.current_paragraph.append(data)


def extract_article(
    payload: bytes,
    *,
    fallback_url: str,
    require_canonical_url: bool = False,
) -> ExtractedArticle:
    parser = ArticleHTMLParser()
    parser.feed(payload.decode("utf-8", errors="replace"))
    title = first_present(
        parser.meta.get("og:title"),
        parser.meta.get("twitter:title"),
        normalized_text("".join(parser.title_parts)),
    )
    summary = first_present(
        parser.meta.get("og:description"),
        parser.meta.get("description"),
        parser.meta.get("twitter:description"),
    )
    author = first_present(
        parser.meta.get("author"),
        parser.meta.get("article:author"),
        parser.meta.get("byl"),
    )
    published_at = first_datetime(
        parser.meta.get("article:published_time"),
        parser.meta.get("pubdate"),
        parser.meta.get("publishdate"),
        parser.meta.get("date"),
    )
    body_text = normalized_text("\n".join(parser.paragraph_parts)) or None
    canonical_candidate = first_present(
        parser.canonical_url,
        parser.meta.get("og:url"),
        parser.meta.get("twitter:url"),
    )
    canonical_url = canonical_candidate or (None if require_canonical_url else fallback_url)
    return ExtractedArticle(
        canonical_url=canonical_url,
        title=clean_title(title),
        summary=summary,
        body_text=body_text,
        author=author,
        published_at=published_at,
    )


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
