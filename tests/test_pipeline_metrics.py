from __future__ import annotations

from datetime import UTC, date, datetime

from news_platform.contracts.events import (
    ArticleExtracted,
    ArticleFetched,
    NewsDlq,
    PipelineMetricObserved,
)

from news_pipeline_metrics.main import (
    MetricPoint,
    PipelineMetricAccumulator,
    collect_and_publish,
    dlq_source_id,
    extraction_failure_sources,
)


def test_accumulator_builds_source_level_metric_points() -> None:
    accumulator = PipelineMetricAccumulator()

    accumulator.add("article_fetched", article_fetched("vnexpress").model_dump(mode="json"))
    accumulator.add("article_extracted", article_extracted("vnexpress").model_dump(mode="json"))
    accumulator.add(
        "pipeline_metric",
        PipelineMetricObserved(
            schema_version="pipeline.metric_observed.v1",
            event_id="event_metric_1",
            event_time=datetime(2026, 6, 11, tzinfo=UTC),
            service="article_fetcher",
            metric_name="article_fetch_retry_count",
            metric_value=2,
            dimensions={"source_id": "vnexpress", "error_class": "ReadTimeout"},
        ).model_dump(mode="json"),
    )

    points = accumulator.drain()

    values = {
        (point.name, tuple(sorted(point.dimensions.items()))): point.value for point in points
    }
    assert values[("ArticleFetchSuccessCount", (("sourceId", "vnexpress"),))] == 1
    assert values[("ArticleExtractionSuccessCount", (("sourceId", "vnexpress"),))] == 1
    assert (
        values[
            (
                "ArticleFetchRetryCount",
                (
                    ("errorClass", "ReadTimeout"),
                    ("service", "article_fetcher"),
                    ("sourceId", "vnexpress"),
                ),
            )
        ]
        == 2
    )


def test_accumulator_counts_dlq_rejections_and_failure_source() -> None:
    accumulator = PipelineMetricAccumulator()
    for _ in range(3):
        accumulator.add("dlq", extraction_dlq("dantri").model_dump(mode="json"))

    points = accumulator.drain()

    metric_names = [point.name for point in points]
    assert "DlqEventCount" in metric_names
    assert "ArticleRejectedCount" in metric_names
    assert any(
        point.name == "SourceExtractionFailure" and point.dimensions == {"sourceId": "dantri"}
        for point in points
    )


def test_dlq_source_id_returns_unknown_when_payload_is_not_decoded() -> None:
    event = extraction_dlq("dantri")
    event.payload["failed_event"] = {"encoding": "base64", "value": "abc"}

    assert dlq_source_id(event) == "unknown"


def test_extraction_failure_sources_require_no_successes() -> None:
    failed = extraction_failure_sources(
        extraction_success={"dantri": 1},
        rejected={("dantri", "article_extract", "BoilerplateOnlyError"): 1},
        min_rejections=1,
    )

    assert failed == set()


def test_extraction_failure_sources_require_minimum_rejections() -> None:
    failed = extraction_failure_sources(
        extraction_success={},
        rejected={("dantri", "article_extract", "BoilerplateOnlyError"): 2},
        min_rejections=3,
    )

    assert failed == set()


def test_collect_and_publish_commits_consumed_metrics_after_publish_failure() -> None:
    consumer = FakeMetricsConsumer(
        [("article_fetched", article_fetched("vnexpress").model_dump(mode="json"))]
    )
    publisher = FakeMetricPublisher(fail=True)

    result = collect_and_publish(
        consumer=consumer,
        accumulator=PipelineMetricAccumulator(),
        lag_reader=FakeLagReader(),
        publisher=publisher,
        poll_timeout_seconds=0.01,
        flush_interval_seconds=0.01,
        consumer_lag_groups=["article_fetcher"],
    )

    assert result == 0
    assert consumer.committed is True
    assert publisher.published


def test_collect_and_publish_does_not_commit_when_nothing_was_consumed() -> None:
    consumer = FakeMetricsConsumer([])

    collect_and_publish(
        consumer=consumer,
        accumulator=PipelineMetricAccumulator(),
        lag_reader=FakeLagReader(),
        publisher=FakeMetricPublisher(),
        poll_timeout_seconds=0.01,
        flush_interval_seconds=0.01,
        consumer_lag_groups=["article_fetcher"],
    )

    assert consumer.committed is False


class FakeMetricsConsumer:
    def __init__(self, events: list[tuple[str, dict]]) -> None:
        self.events = events
        self.committed = False

    def consume_one(self, timeout_seconds: float) -> tuple[str, dict] | None:
        if not self.events:
            return None
        return self.events.pop(0)

    def commit(self) -> None:
        self.committed = True


class FakeLagReader:
    def read(self, group_ids: list[str]) -> list[MetricPoint]:
        return [
            MetricPoint(
                "ConsumerGroupLagTotal",
                0,
                "count",
                {"consumerGroup": group_ids[0]},
            )
        ]


class FakeMetricPublisher:
    def __init__(self, *, fail: bool = False) -> None:
        self.fail = fail
        self.published: list[MetricPoint] = []

    def publish(self, points: list[MetricPoint]) -> None:
        self.published = points
        if self.fail:
            raise RuntimeError("metric sink unavailable")


def article_fetched(source_id: str) -> ArticleFetched:
    return ArticleFetched(
        schema_version="article.fetched.v2",
        event_id="event_fetched_1",
        event_time=datetime(2026, 6, 11, tzinfo=UTC),
        run_id="run_1",
        source_id=source_id,
        ingest_date=date(2026, 6, 11),
        article_id="article_1",
        requested_url="https://vnexpress.net/a.html",
        source_document_id="source_document_1",
        fetched_at=datetime(2026, 6, 11, tzinfo=UTC),
        status_code=200,
        content_length_bytes=1000,
        payload_uri="s3://landing/payload.html.zst",
        content_hash="hash_1",
        fetch_status="success",
    )


def article_extracted(source_id: str) -> ArticleExtracted:
    return ArticleExtracted(
        schema_version="article.extracted.v3",
        event_id="event_extracted_1",
        event_time=datetime(2026, 6, 11, tzinfo=UTC),
        run_id="run_1",
        source_id=source_id,
        ingest_date=date(2026, 6, 11),
        article_id="article_1",
        requested_url="https://vnexpress.net/a.html",
        canonical_url="https://vnexpress.net/a.html",
        title="Title",
        body_text="Clean article body",
        content_blocks=[{"type": "paragraph", "text": "Clean article body", "ordinal": 1}],
        content_hash="hash_2",
        source_document_id="source_document_1",
        source_payload_uri="s3://landing/payload.html.zst",
        extractor_version="1",
        extraction_status="success",
    )


def extraction_dlq(source_id: str) -> NewsDlq:
    return NewsDlq(
        schema_version="news.dlq.v1",
        event_id="event_dlq_1",
        event_time=datetime(2026, 6, 11, tzinfo=UTC),
        source_topic="news.article.fetched.v2",
        source_partition=0,
        source_offset=1,
        error_class="BoilerplateOnlyError",
        error_message="empty article",
        payload={
            "stage": "article_extract",
            "failed_event": {
                "encoding": "json",
                "value": article_fetched(source_id).model_dump(mode="json"),
            },
        },
    )
