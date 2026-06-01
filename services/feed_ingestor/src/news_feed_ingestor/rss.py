from __future__ import annotations

import html
import re
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from html.parser import HTMLParser
from urllib.parse import urlparse

from defusedxml import ElementTree
from news_platform.ids import normalize_article_url

from news_feed_ingestor.models import FeedItem, ParsedFeed


class SummaryTextParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.parts: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag in {"br", "div", "li", "p"}:
            self.parts.append(" ")

    def handle_data(self, data: str) -> None:
        self.parts.append(data)


def parse_rss_items(payload: bytes) -> ParsedFeed:
    root = ElementTree.fromstring(payload)
    if root.tag != "rss" or root.find("./channel") is None:
        raise ValueError("Expected an RSS document with a channel")
    items: dict[str, FeedItem] = {}
    skipped_items = 0
    duplicate_item_count = 0
    for item in root.findall("./channel/item"):
        title = normalized_text(item.findtext("title"))
        article_url = normalized_text(item.findtext("link"))
        if not title or not is_http_url(article_url):
            skipped_items += 1
            continue
        article_url = normalize_article_url(article_url)
        if article_url in items:
            duplicate_item_count += 1
            continue
        items[article_url] = FeedItem(
            article_url=article_url,
            title=title,
            summary=summary_text(item.findtext("description")),
            published_at=parse_rss_datetime(item.findtext("pubDate")),
        )
    if not items:
        raise ValueError("RSS feed contains no valid items")
    return ParsedFeed(
        items=list(items.values()),
        skipped_items=skipped_items,
        duplicate_item_count=duplicate_item_count,
    )


def summary_text(value: str | None) -> str | None:
    if not value:
        return None
    parser = SummaryTextParser()
    parser.feed(value)
    summary = normalized_text(html.unescape("".join(parser.parts)))
    return summary or None


def normalized_text(value: str | None) -> str:
    return re.sub(r"\s+", " ", value or "").strip()


def is_http_url(value: str) -> bool:
    parsed = urlparse(value)
    return parsed.scheme in {"http", "https"} and bool(parsed.netloc)


def parse_rss_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return None
    return parsed.replace(tzinfo=UTC) if parsed.tzinfo is None else parsed
