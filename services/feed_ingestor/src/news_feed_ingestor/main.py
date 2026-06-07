from __future__ import annotations

import argparse
import time
from dataclasses import asdict

from news_platform.config import load_settings, load_sources
from news_platform.storage import StorageLayout

from news_feed_ingestor.adapters import HttpFeedClient
from news_feed_ingestor.rss import parse_rss_items
from news_feed_ingestor.service import FeedIngestor
from news_service_common.config import select_enabled_source, select_feed
from news_service_common.errors import IngestionError
from news_service_common.events import JsonEventPublisher
from news_service_common.runtime import elapsed_ms, handle_unconsumed_error
from news_service_common.stages import run_stage
from news_service_common.storage import S3PayloadStore
from news_service_common.telemetry import log_event, log_metric
from news_service_common.url_safety import UrlSafetyPolicy

SERVICE_NAME = "feed_ingestor"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Scrape configured RSS feeds into landing.")
    parser.add_argument("--source-id", default="vnexpress")
    parser.add_argument("--feed-id", default="kinh_doanh")
    parser.add_argument("--all-feeds", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def run() -> int:
    args = parse_args()
    started_at = time.perf_counter()
    try:
        return scrape(args)
    except Exception as error:
        return handle_unconsumed_error(
            service_name=SERVICE_NAME,
            failure_event="rss_feed_failed",
            error=error,
            started_at=started_at,
            source_id=args.source_id,
            feed_id=args.feed_id,
            dry_run=args.dry_run,
        )


def scrape(args: argparse.Namespace) -> int:
    config = run_stage("config_load", False, load_settings)
    sources = run_stage("config_load", False, lambda: load_sources(settings=config))
    source = run_stage(
        "source_select", False, lambda: select_enabled_source(sources, args.source_id)
    )
    feeds = (
        source["feed_discovery"]["feeds"]
        if args.all_feeds
        else [run_stage("feed_select", False, lambda: select_feed(source, args.feed_id))]
    )
    retry = config["crawl"]["retry"]
    object_store = None
    publisher = None
    if not args.dry_run:
        object_store = run_stage(
            "storage_connect",
            True,
            lambda: S3PayloadStore(
                endpoint_url=config["storage"]["endpoint_url"],
            ),
        )
        publisher = run_stage("event_bus_connect", True, lambda: JsonEventPublisher(config))
    feed_count = len(feeds)
    log_event(
        SERVICE_NAME,
        "rss_source_started",
        source_id=source["source_id"],
        feed_count=feed_count,
        dry_run=args.dry_run,
    )
    failed_feed_count = 0
    fatal_feed_count = 0
    successful_feed_count = 0
    for index, feed in enumerate(feeds):
        feed_failed, feed_fatal = scrape_feed(
            args=args,
            config=config,
            source=source,
            feed=feed,
            retry=retry,
            object_store=object_store,
            publisher=publisher,
        )
        failed_feed_count += int(feed_failed)
        fatal_feed_count += int(feed_fatal)
        successful_feed_count += int(not feed_failed)
        if index < feed_count - 1:
            time.sleep(source["crawl"]["delay_seconds"])
    log_event(
        SERVICE_NAME,
        "rss_source_completed",
        source_id=source["source_id"],
        feed_count=feed_count,
        successful_feed_count=successful_feed_count,
        failed_feed_count=failed_feed_count,
        fatal_feed_count=fatal_feed_count,
        dry_run=args.dry_run,
    )
    log_metric(
        SERVICE_NAME,
        "rss_source_feeds_total",
        feed_count,
        source_id=source["source_id"],
        successful_feed_count=successful_feed_count,
        failed_feed_count=failed_feed_count,
        fatal_feed_count=fatal_feed_count,
        dry_run=args.dry_run,
    )
    if fatal_feed_count:
        return 1
    return 1 if failed_feed_count and successful_feed_count == 0 else 0


def scrape_feed(
    *,
    args: argparse.Namespace,
    config: dict,
    source: dict,
    feed: dict,
    retry: dict,
    object_store: S3PayloadStore | None,
    publisher: JsonEventPublisher | None,
) -> tuple[bool, bool]:
    started_at = time.perf_counter()
    try:
        _scrape_feed(
            args=args,
            config=config,
            source=source,
            feed=feed,
            retry=retry,
            object_store=object_store,
            publisher=publisher,
            started_at=started_at,
        )
    except Exception as error:
        handle_unconsumed_error(
            service_name=SERVICE_NAME,
            failure_event="rss_feed_failed",
            error=error,
            started_at=started_at,
            source_id=source["source_id"],
            feed_id=feed["feed_id"],
            dry_run=args.dry_run,
        )
        fatal = not isinstance(error, IngestionError) or error.retryable
        return True, fatal
    return False, False


def _scrape_feed(
    *,
    args: argparse.Namespace,
    config: dict,
    source: dict,
    feed: dict,
    retry: dict,
    object_store: S3PayloadStore | None,
    publisher: JsonEventPublisher | None,
    started_at: float,
) -> None:
    http_client = HttpFeedClient(
        user_agent=config["crawl"]["user_agents"][source["crawl"]["user_agent_policy"]],
        timeout_seconds=source["crawl"]["timeout_seconds"],
        max_feed_bytes=config["crawl"]["max_feed_bytes"],
        retry_attempts=retry["attempts"],
        retry_backoff_seconds=retry["backoff_seconds"],
        url_policy=UrlSafetyPolicy(source["domain"]),
        on_retry=lambda **fields: log_event(
            SERVICE_NAME,
            "rss_fetch_retry",
            level="warning",
            source_id=source["source_id"],
            feed_id=feed["feed_id"],
            dry_run=args.dry_run,
            **fields,
        ),
    )
    if args.dry_run:
        response = run_stage("feed_fetch", True, lambda: http_client.fetch(feed["url"]))
        parsed_feed = run_stage("feed_parse", False, lambda: parse_rss_items(response.content))
        log_event(
            SERVICE_NAME,
            "rss_feed_validated",
            source_id=source["source_id"],
            feed_id=feed["feed_id"],
            dry_run=True,
            duration_ms=elapsed_ms(started_at),
            parsed_item_count=len(parsed_feed.items),
            skipped_item_count=parsed_feed.skipped_items,
            duplicate_item_count=parsed_feed.duplicate_item_count,
        )
        return

    if object_store is None or publisher is None:
        raise RuntimeError("Storage and event publisher are required outside dry-run mode")
    outcome = FeedIngestor(
        feed_client=http_client,
        object_store=object_store,
        publisher=publisher,
        storage_layout=StorageLayout.from_config(config),
        url_policy=UrlSafetyPolicy(source["domain"]),
    ).scrape(source, feed)
    log_event(
        SERVICE_NAME, "rss_feed_scraped", duration_ms=elapsed_ms(started_at), **asdict(outcome)
    )


def main() -> None:
    raise SystemExit(run())


if __name__ == "__main__":
    main()
