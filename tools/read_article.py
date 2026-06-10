from __future__ import annotations

import argparse
import json
import os
import shlex
import subprocess
import sys
from collections.abc import Iterable
from typing import Any

from news_platform.config import get_topic_name, load_settings
from news_platform.ids import make_stable_id, normalize_article_url

STAGE_TOPIC_KEYS = {
    "fetched": "article_fetched",
    "extracted": "article_extracted",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Read fetched or extracted article data.")
    identifier = parser.add_mutually_exclusive_group(required=True)
    identifier.add_argument("--article-id")
    identifier.add_argument("--url")
    parser.add_argument("--stage", choices=("all", *STAGE_TOPIC_KEYS), default="all")
    parser.add_argument("--output", choices=("summary", "json", "content"), default="summary")
    parser.add_argument("--all-versions", action="store_true")
    parser.add_argument("--since", default="-24h", help="rpk duration or RFC3339 start time.")
    parser.add_argument(
        "--data-host",
        default=os.environ.get("VN_NEWS_DATA_SSH_HOST", "tgb-data-1"),
    )
    parser.add_argument(
        "--storage-host",
        default=os.environ.get("VN_NEWS_STORAGE_SSH_HOST", "tgb-processing-1"),
    )
    parser.add_argument(
        "--storage-container",
        default=os.environ.get(
            "VN_NEWS_STORAGE_CONTAINER",
            "vn-news-processing-article-fetcher-1",
        ),
    )
    parser.add_argument(
        "--redpanda-container",
        default=os.environ.get("VN_NEWS_REDPANDA_CONTAINER", "vn-news-data-redpanda-1"),
    )
    return parser.parse_args()


def resolve_article_id(article_id: str | None, url: str | None) -> str:
    if article_id:
        return article_id
    if not url:
        raise ValueError("article_id or url is required")
    return make_stable_id("article", normalize_article_url(url))


def selected_stages(stage: str) -> tuple[str, ...]:
    return tuple(STAGE_TOPIC_KEYS) if stage == "all" else (stage,)


def consume_stage(
    *,
    stage: str,
    topic: str,
    article_id: str,
    since: str,
    data_host: str,
    redpanda_container: str,
) -> list[dict[str, Any]]:
    offset = f"@{since}:end"
    command = [
        "ssh",
        data_host,
        "docker",
        "exec",
        redpanda_container,
        "rpk",
        "topic",
        "consume",
        topic,
        "--offset",
        offset,
        "--use-schema-registry=value",
        "--format",
        "json",
        "--pretty-print=false",
    ]
    result = subprocess.run(command, check=True, capture_output=True, text=True)
    matches: list[dict[str, Any]] = []
    for line in result.stdout.splitlines():
        record = json.loads(line)
        event = json.loads(record["value"])
        if event.get("article_id") != article_id:
            continue
        matches.append(
            {
                "stage": stage,
                "topic": record["topic"],
                "partition": record["partition"],
                "offset": record["offset"],
                "timestamp": record["timestamp"],
                "event": event,
            }
        )
    return matches


def latest_records(records: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    latest: dict[str, dict[str, Any]] = {}
    for record in records:
        stage = record["stage"]
        if stage not in latest or record["timestamp"] > latest[stage]["timestamp"]:
            latest[stage] = record
    return [latest[stage] for stage in STAGE_TOPIC_KEYS if stage in latest]


def record_summary(record: dict[str, Any]) -> dict[str, Any]:
    event = record["event"]
    keys = (
        "event_id",
        "event_time",
        "source_id",
        "article_id",
        "requested_url",
        "canonical_url",
        "title",
        "published_at",
        "source_document_id",
        "source_payload_uri",
        "content_hash",
        "payload_uri",
        "extractor_version",
    )
    summary = {
        "stage": record["stage"],
        "topic": record["topic"],
        "partition": record["partition"],
        "offset": record["offset"],
        **{key: event[key] for key in keys if key in event},
    }
    if "content_blocks" in event:
        summary["block_count"] = len(event["content_blocks"])
    if "images" in event:
        summary["image_count"] = len(event["images"])
    return summary


def fetched_html(
    *,
    payload_uri: str,
    storage_host: str,
    storage_container: str,
) -> str:
    python_code = (
        "import os,sys;"
        "from news_service_common.storage import S3PayloadStore;"
        "store=S3PayloadStore(endpoint_url=os.environ['VN_NEWS_STORAGE_ENDPOINT_URL']);"
        "sys.stdout.buffer.write(store.read_compressed(sys.argv[1]))"
    )
    remote_command = shlex.join(
        [
            "docker",
            "exec",
            storage_container,
            "/app/.venv/bin/python",
            "-c",
            python_code,
            payload_uri,
        ]
    )
    result = subprocess.run(
        ["ssh", storage_host, remote_command],
        check=True,
        capture_output=True,
    )
    return result.stdout.decode("utf-8", errors="replace")


def content_for_record(record: dict[str, Any], args: argparse.Namespace) -> str:
    event = record["event"]
    if record["stage"] == "extracted":
        return event["body_text"]
    return fetched_html(
        payload_uri=event["payload_uri"],
        storage_host=args.storage_host,
        storage_container=args.storage_container,
    )


def main() -> int:
    args = parse_args()
    article_id = resolve_article_id(args.article_id, args.url)
    settings = load_settings()
    records = [
        record
        for stage in selected_stages(args.stage)
        for record in consume_stage(
            stage=stage,
            topic=get_topic_name(settings, STAGE_TOPIC_KEYS[stage]),
            article_id=article_id,
            since=args.since,
            data_host=args.data_host,
            redpanda_container=args.redpanda_container,
        )
    ]
    if not records:
        print(f"No matching events found for {article_id}.", file=sys.stderr)
        return 1

    selected = records if args.all_versions else latest_records(records)
    if args.output == "content":
        preferred = next(
            (record for record in reversed(selected) if record["stage"] == "extracted"),
            selected[-1],
        )
        try:
            print(content_for_record(preferred, args))
        except BrokenPipeError:
            return 0
        return 0

    payload = selected if args.output == "json" else [record_summary(record) for record in selected]
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
