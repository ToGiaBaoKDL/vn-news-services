from __future__ import annotations

import argparse
import time
from dataclasses import asdict
from typing import Any

from news_platform.config import load_settings, load_sources
from news_platform.contracts.events import ArticleFetched

from news_article_extractor.service import ArticleExtractor
from news_service_common.config import select_enabled_source
from news_service_common.events import (
    ConsumedEvent,
    JsonEventConsumer,
    JsonEventPublisher,
)
from news_service_common.runtime import (
    ConsumedRetryBackoff,
    ShutdownSignal,
    elapsed_ms,
    handle_consumed_error,
    handle_unconsumed_error,
    should_stop_after_process,
)
from news_service_common.stages import run_stage
from news_service_common.storage import S3PayloadStore
from news_service_common.telemetry import log_event, log_metric

SERVICE_NAME = "article_extractor"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Extract fetched article payloads from Redpanda.")
    parser.add_argument("--group-id", default="article_extractor")
    parser.add_argument("--poll-timeout-seconds", type=float, default=10.0)
    parser.add_argument("--once", action="store_true")
    return parser.parse_args()


def run() -> int:
    args = parse_args()
    started_at = time.perf_counter()
    shutdown = ShutdownSignal()
    shutdown.install()
    try:
        config = run_stage("config_load", False, load_settings)
        sources = run_stage("config_load", False, lambda: load_sources(settings=config))
        publisher = run_stage("event_bus_connect", True, lambda: JsonEventPublisher(config))
        consumer = run_stage(
            "event_bus_connect",
            True,
            lambda: JsonEventConsumer(config, group_id=args.group_id),
        )
        object_store = run_stage(
            "storage_connect",
            True,
            lambda: S3PayloadStore(
                endpoint_url=config["storage"]["endpoint_url"],
            ),
        )
        retry_backoff = ConsumedRetryBackoff.from_config(config)
    except Exception as error:
        return handle_unconsumed_error(
            service_name=SERVICE_NAME,
            failure_event="article_extract_failed",
            error=error,
            started_at=started_at,
            group_id=args.group_id,
        )
    try:
        while not shutdown.requested:
            result = process_one(
                args,
                config,
                sources,
                publisher,
                consumer,
                object_store,
                retry_backoff,
                shutdown,
            )
            exit_code = should_stop_after_process(result, once=args.once)
            if exit_code is not None:
                return exit_code
        log_event(SERVICE_NAME, "article_extract_stopped", duration_ms=elapsed_ms(started_at))
        return 0
    finally:
        consumer.close()


def process_one(
    args: argparse.Namespace,
    config: dict[str, Any],
    sources: list[dict[str, Any]],
    publisher: JsonEventPublisher,
    consumer: JsonEventConsumer,
    object_store: S3PayloadStore,
    retry_backoff: ConsumedRetryBackoff,
    shutdown: ShutdownSignal,
) -> int:
    started_at = time.perf_counter()
    consumed: ConsumedEvent | None = None
    try:
        consumed = run_stage(
            "event_consume",
            True,
            lambda: consumer.consume_one(
                "article_fetched", timeout_seconds=args.poll_timeout_seconds
            ),
        )
        if consumed is None:
            return 0
        started_at = time.perf_counter()
        event = run_stage(
            "event_decode",
            False,
            lambda: ArticleFetched.model_validate(consumed.decode_value()),
        )
        source = run_stage(
            "source_select",
            False,
            lambda: select_enabled_source(sources, event.source_id),
        )
        extractor = ArticleExtractor(
            object_store=object_store,
            publisher=publisher,
            source=source,
        )
        outcome = extractor.extract(event)
        run_stage("event_commit", True, lambda: consumer.commit(consumed))
        retry_backoff.reset(consumed)
        log_event(
            SERVICE_NAME,
            "article_extract_succeeded",
            duration_ms=elapsed_ms(started_at),
            topic=consumed.topic,
            partition=consumed.partition,
            offset=consumed.offset,
            **asdict(outcome),
        )
        log_metric(
            SERVICE_NAME,
            "article_extract_events_total",
            1,
            source_id=outcome.source_id,
            article_id=outcome.article_id,
            status=outcome.status,
        )
        return 0
    except Exception as error:
        return handle_consumed_error(
            service_name=SERVICE_NAME,
            failure_event="article_extract_failed",
            dlq_event="article_extract_dlq",
            config=config,
            publisher=publisher,
            consumer=consumer,
            consumed=consumed,
            error=error,
            started_at=started_at,
            retry_backoff=retry_backoff,
            shutdown=shutdown,
        )


def main() -> None:
    raise SystemExit(run())


if __name__ == "__main__":
    main()
