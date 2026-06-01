from __future__ import annotations

import pytest

from news_feed_ingestor.rss import parse_rss_items

RSS = b"""<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <item>
      <title>Gia vang tang</title>
      <description><![CDATA[
        <a href="https://vnexpress.net/a.html"><img src="a.jpg"></a></br>
        Tom tat bai viet.
      ]]></description>
      <pubDate>Mon, 01 Jun 2026 05:00:00 +0700</pubDate>
      <link>https://vnexpress.net/a.html</link>
    </item>
    <item>
      <title>Missing link</title>
    </item>
  </channel>
</rss>
"""


def test_parse_vnexpress_rss_items() -> None:
    parsed = parse_rss_items(RSS)

    assert len(parsed.items) == 1
    assert parsed.skipped_items == 1
    assert parsed.items[0].article_url == "https://vnexpress.net/a.html"
    assert parsed.items[0].summary == "Tom tat bai viet."
    assert parsed.items[0].published_at.isoformat() == "2026-06-01T05:00:00+07:00"


def test_parse_rejects_non_rss_payload() -> None:
    with pytest.raises(ValueError, match="Expected an RSS document"):
        parse_rss_items(b"<html><body>Blocked</body></html>")


def test_parse_rejects_rss_without_valid_items() -> None:
    with pytest.raises(ValueError, match="RSS feed contains no valid items"):
        parse_rss_items(b"<rss><channel></channel></rss>")


def test_parse_collapses_duplicate_urls() -> None:
    rss_with_duplicate = RSS.replace(
        b"  </channel>",
        b"""    <item>
      <title>Duplicate</title>
      <link>https://vnexpress.net/a.html</link>
    </item>
  </channel>""",
    )

    parsed = parse_rss_items(rss_with_duplicate)

    assert len(parsed.items) == 1
    assert parsed.duplicate_item_count == 1


def test_parse_collapses_tracking_url_variants() -> None:
    rss_with_duplicate = RSS.replace(
        b"  </channel>",
        b"""    <item>
      <title>Duplicate</title>
      <link>https://vnexpress.net/a.html?utm_source=rss#top</link>
    </item>
  </channel>""",
    )

    parsed = parse_rss_items(rss_with_duplicate)

    assert len(parsed.items) == 1
    assert parsed.duplicate_item_count == 1
