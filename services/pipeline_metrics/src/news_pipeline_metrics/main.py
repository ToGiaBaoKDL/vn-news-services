from __future__ import annotations

import argparse
import time
import warnings
from collections import Counter
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Protocol

import httpx
from confluent_kafka import Consumer, ConsumerGroupTopicPartitions, Message, TopicPartition
from confluent_kafka.admin import AdminClient, OffsetSpec
from news_platform.config import get_topic_key, get_topic_name, load_settings
from news_platform.contracts.events import (
    ArticleExtracted,
    ArticleFetched,
    NewsDlq,
    PipelineMetricObserved,
)

from news_service_common.events import decode_schema_registry_json
from news_service_common.runtime import ShutdownSignal, elapsed_ms, handle_unconsumed_error
from news_service_common.stages import run_stage
from news_service_common.telemetry import log_event

SERVICE_NAME = "pipeline_metrics"
DEFAULT_TOPIC_KEYS = ("article_fetched", "article_extracted", "pipeline_metric", "dlq")
OCI_METRIC_BATCH_SIZE = 50
METRIC_NAME_MAP = {
    "article_fetch_retry_count": "ArticleFetchRetryCount",
    "consumer_retry_count": "ConsumerRetryCount",
}

warnings.filterwarnings(
    "ignore",
    message=r"The 'strict' parameter is no longer needed on Python 3\+.*",
    category=FutureWarning,
    module=r"urllib3\.poolmanager",
)


@dataclass(frozen=True)
class MetricPoint:
    name: str
    value: float
    unit: str
    dimensions: dict[str, str]


class MetricPublisher(Protocol):
    def publish(self, points: list[MetricPoint]) -> None: ...


class JsonMetricPublisher:
    def publish(self, points: list[MetricPoint]) -> None:
        log_event(
            SERVICE_NAME,
            "pipeline_metrics_dry_run",
            metrics=[point.__dict__ for point in points],
        )


class OciMetricPublisher:
    def __init__(
        self,
        *,
        client: Any,
        compartment_id: str,
        namespace: str,
        resource_id: str,
    ) -> None:
        self.client = client
        self.compartment_id = compartment_id
        self.namespace = namespace
        self.resource_id = resource_id

    @classmethod
    def from_instance_principal(cls, *, namespace: str) -> OciMetricPublisher:
        metadata = instance_metadata()
        import oci
        from oci.monitoring import MonitoringClient

        signer = oci.auth.signers.InstancePrincipalsSecurityTokenSigner()
        client = MonitoringClient(
            {},
            signer=signer,
            service_endpoint=f"https://telemetry-ingestion.{metadata['region']}.oraclecloud.com",
        )
        return cls(
            client=client,
            compartment_id=metadata["compartmentId"],
            namespace=namespace,
            resource_id=metadata["id"],
        )

    def publish(self, points: list[MetricPoint]) -> None:
        if not points:
            return
        from oci.monitoring.models import Datapoint, MetricDataDetails, PostMetricDataDetails

        timestamp = datetime.now(UTC)
        for chunk in chunks(points, OCI_METRIC_BATCH_SIZE):
            metric_data = [
                MetricDataDetails(
                    compartment_id=self.compartment_id,
                    name=point.name,
                    namespace=self.namespace,
                    dimensions={
                        "resourceId": self.resource_id,
                        **point.dimensions,
                    },
                    metadata={"unit": point.unit},
                    datapoints=[
                        Datapoint(
                            timestamp=timestamp,
                            value=point.value,
                            count=1,
                        )
                    ],
                )
                for point in chunk
            ]
            self.client.post_metric_data(
                post_metric_data_details=PostMetricDataDetails(metric_data=metric_data)
            )


def instance_metadata() -> dict[str, str]:
    response = httpx.get(
        "http://169.254.169.254/opc/v2/instance/",
        headers={"Authorization": "Bearer Oracle"},
        timeout=10,
    )
    response.raise_for_status()
    metadata = response.json()
    required = {"compartmentId", "id", "region"}
    missing = required - set(metadata)
    if missing:
        raise ValueError(f"OCI instance metadata is missing: {sorted(missing)}")
    return metadata


class PipelineEventConsumer:
    def __init__(self, config: dict[str, Any], *, group_id: str) -> None:
        self.config = config
        self.consumer = Consumer(
            {
                "bootstrap.servers": config["event_bus"]["bootstrap_servers"],
                "group.id": group_id,
                "enable.auto.commit": False,
                "auto.offset.reset": "latest",
            }
        )
        self.consumer.subscribe(
            [get_topic_name(config, topic_key) for topic_key in DEFAULT_TOPIC_KEYS]
        )

    def consume_one(self, timeout_seconds: float) -> tuple[str, dict[str, Any]] | None:
        message = self.consumer.poll(timeout_seconds)
        if message is None:
            return None
        if message.error():
            raise RuntimeError(str(message.error()))
        return self.topic_key(message), decode_schema_registry_json(message.value())

    def topic_key(self, message: Message) -> str:
        return get_topic_key(self.config, message.topic())

    def commit(self) -> None:
        self.consumer.commit(asynchronous=False)

    def close(self) -> None:
        self.consumer.close()


class PipelineMetricAccumulator:
    def __init__(self, *, source_failure_min_rejections: int = 3) -> None:
        self.source_failure_min_rejections = source_failure_min_rejections
        self.fetch_success: Counter[str] = Counter()
        self.extraction_success: Counter[str] = Counter()
        self.rejected: Counter[tuple[str, str, str]] = Counter()
        self.dlq: Counter[tuple[str, str, str]] = Counter()
        self.observed: Counter[tuple[str, str, tuple[tuple[str, str], ...]]] = Counter()

    def add(self, topic_key: str, payload: dict[str, Any]) -> None:
        if topic_key == "article_fetched":
            event = ArticleFetched.model_validate(payload)
            self.fetch_success[event.source_id] += 1
            return
        if topic_key == "article_extracted":
            event = ArticleExtracted.model_validate(payload)
            self.extraction_success[event.source_id] += 1
            return
        if topic_key == "dlq":
            event = NewsDlq.model_validate(payload)
            source_id = dlq_source_id(event)
            stage = dlq_stage(event)
            error_class = clean_dimension(event.error_class)
            self.dlq[(source_id, stage, error_class)] += 1
            self.rejected[(source_id, stage, error_class)] += 1
            return
        if topic_key == "pipeline_metric":
            event = PipelineMetricObserved.model_validate(payload)
            metric_name = METRIC_NAME_MAP.get(event.metric_name)
            if metric_name:
                dimensions = {"service": event.service, **event.dimensions}
                key = (
                    metric_name,
                    event.metric_unit,
                    tuple(sorted(clean_dimensions(dimensions).items())),
                )
                self.observed[key] += event.metric_value

    def drain(self) -> list[MetricPoint]:
        points: list[MetricPoint] = []
        for source_id, count in sorted(self.fetch_success.items()):
            points.append(
                MetricPoint(
                    "ArticleFetchSuccessCount",
                    float(count),
                    "count",
                    {"sourceId": source_id},
                )
            )
        for source_id, count in sorted(self.extraction_success.items()):
            points.append(
                MetricPoint(
                    "ArticleExtractionSuccessCount",
                    float(count),
                    "count",
                    {"sourceId": source_id},
                )
            )
        for (source_id, stage, error_class), count in sorted(self.rejected.items()):
            points.append(
                MetricPoint(
                    "ArticleRejectedCount",
                    float(count),
                    "count",
                    {"sourceId": source_id, "stage": stage, "errorClass": error_class},
                )
            )
        for (source_id, stage, error_class), count in sorted(self.dlq.items()):
            points.append(
                MetricPoint(
                    "DlqEventCount",
                    float(count),
                    "count",
                    {"sourceId": source_id, "stage": stage, "errorClass": error_class},
                )
            )
        for (metric_name, unit, dimensions), value in sorted(self.observed.items()):
            points.append(MetricPoint(metric_name, float(value), unit, dict(dimensions)))

        for source_id in sorted(
            extraction_failure_sources(
                self.extraction_success,
                self.rejected,
                min_rejections=self.source_failure_min_rejections,
            )
        ):
            points.append(
                MetricPoint(
                    "SourceExtractionFailure",
                    1,
                    "count",
                    {"sourceId": source_id},
                )
            )

        self.fetch_success.clear()
        self.extraction_success.clear()
        self.rejected.clear()
        self.dlq.clear()
        self.observed.clear()
        return points


class ConsumerLagReader:
    def __init__(self, config: dict[str, Any]) -> None:
        self.admin = AdminClient({"bootstrap.servers": config["event_bus"]["bootstrap_servers"]})

    def read(self, group_ids: list[str]) -> list[MetricPoint]:
        points: list[MetricPoint] = []
        for group_id in group_ids:
            points.extend(self._read_group(group_id))
        return points

    def _read_group(self, group_id: str) -> list[MetricPoint]:
        future_by_group = self.admin.list_consumer_group_offsets(
            [ConsumerGroupTopicPartitions(group_id)]
        )
        offsets = future_by_group[group_id].result(timeout=10)
        partitions = [
            partition
            for partition in offsets.topic_partitions or []
            if partition.offset is not None and partition.offset >= 0
        ]
        latest_offsets = self._latest_offsets(partitions)

        points: list[MetricPoint] = []
        total_lag = 0
        for partition in partitions:
            latest = latest_offsets.get((partition.topic, partition.partition))
            if latest is None:
                continue
            lag = max(0, latest - partition.offset)
            total_lag += lag
            points.append(
                MetricPoint(
                    "ConsumerGroupLag",
                    float(lag),
                    "count",
                    {
                        "consumerGroup": group_id,
                        "topic": partition.topic,
                        "partition": str(partition.partition),
                    },
                )
            )
        points.append(
            MetricPoint(
                "ConsumerGroupLagTotal",
                float(total_lag),
                "count",
                {"consumerGroup": group_id},
            )
        )
        return points

    def _latest_offsets(self, partitions: list[TopicPartition]) -> dict[tuple[str, int], int]:
        if not partitions:
            return {}
        futures = self.admin.list_offsets(
            {
                TopicPartition(partition.topic, partition.partition): OffsetSpec.latest()
                for partition in partitions
            }
        )
        latest_offsets: dict[tuple[str, int], int] = {}
        for partition, future in futures.items():
            latest_offsets[(partition.topic, partition.partition)] = future.result(
                timeout=10
            ).offset
        return latest_offsets


def clean_dimensions(dimensions: dict[str, Any]) -> dict[str, str]:
    return {
        clean_dimension_name(key): clean_dimension(value)
        for key, value in dimensions.items()
        if value is not None and clean_dimension(value)
    }


def clean_dimension(value: Any) -> str:
    return str(value).strip()[:128] if value is not None else ""


def clean_dimension_name(value: str) -> str:
    parts = value.strip().split("_")
    return parts[0] + "".join(part.capitalize() for part in parts[1:])


def chunks[T](values: list[T], size: int) -> list[list[T]]:
    return [values[index : index + size] for index in range(0, len(values), size)]


def dlq_source_id(event: NewsDlq) -> str:
    failed_event = event.payload.get("failed_event")
    if isinstance(failed_event, dict) and failed_event.get("encoding") == "json":
        value = failed_event.get("value")
        if isinstance(value, dict) and isinstance(value.get("source_id"), str):
            return value["source_id"]
    return "unknown"


def dlq_stage(event: NewsDlq) -> str:
    stage = event.payload.get("stage")
    return stage if isinstance(stage, str) and stage else "unknown"


def extraction_failure_sources(
    extraction_success: Mapping[str, int],
    rejected: Counter[tuple[str, str, str]],
    *,
    min_rejections: int,
) -> set[str]:
    failed_sources: Counter[str] = Counter()
    for source_id, stage, _error_class in rejected:
        if source_id != "unknown" and stage.startswith("article_extract"):
            failed_sources[source_id] += rejected[(source_id, stage, _error_class)]
    return {
        source_id
        for source_id, reject_count in failed_sources.items()
        if reject_count >= min_rejections and extraction_success.get(source_id, 0) == 0
    }


def collect_and_publish(
    *,
    consumer: PipelineEventConsumer,
    accumulator: PipelineMetricAccumulator,
    lag_reader: ConsumerLagReader,
    publisher: MetricPublisher,
    poll_timeout_seconds: float,
    flush_interval_seconds: float,
    consumer_lag_groups: list[str],
) -> int:
    deadline = time.monotonic() + flush_interval_seconds
    consumed_count = 0
    while time.monotonic() < deadline:
        consumed = consumer.consume_one(
            min(poll_timeout_seconds, max(0.1, deadline - time.monotonic()))
        )
        if consumed is None:
            continue
        topic_key, payload = consumed
        try:
            accumulator.add(topic_key, payload)
        except Exception as error:
            log_event(
                SERVICE_NAME,
                "pipeline_metric_event_skipped",
                level="warning",
                topic_key=topic_key,
                error_class=type(error).__name__,
                error_message=str(error),
            )
        consumed_count += 1

    points = accumulator.drain()
    try:
        points.extend(lag_reader.read(consumer_lag_groups))
    except Exception as error:
        log_event(
            SERVICE_NAME,
            "consumer_lag_collection_failed",
            level="warning",
            error_class=type(error).__name__,
            error_message=str(error),
        )
    publish_status = "success"
    try:
        publisher.publish(points)
    except Exception as error:
        publish_status = "failed"
        log_event(
            SERVICE_NAME,
            "pipeline_metrics_publish_failed",
            level="error",
            metric_points=len(points),
            error_class=type(error).__name__,
            error_message=str(error),
        )
    if consumed_count:
        consumer.commit()
    log_event(
        SERVICE_NAME,
        "pipeline_metrics_published",
        consumed_events=consumed_count,
        metric_points=len(points),
        publish_status=publish_status,
    )
    return 0 if publish_status == "failed" else len(points)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Publish aggregated VN News pipeline metrics.")
    parser.add_argument("--group-id")
    parser.add_argument("--flush-interval-seconds", type=float)
    parser.add_argument("--poll-timeout-seconds", type=float)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--once", action="store_true")
    return parser.parse_args()


def run() -> int:
    args = parse_args()
    started_at = time.perf_counter()
    shutdown = ShutdownSignal()
    shutdown.install()
    try:
        config = run_stage("config_load", False, load_settings)
        metric_config = config["event_bus"]["metrics"]
        group_id = args.group_id or metric_config["collector_group_id"]
        flush_interval_seconds = (
            args.flush_interval_seconds or metric_config["flush_interval_seconds"]
        )
        poll_timeout_seconds = args.poll_timeout_seconds or metric_config["poll_timeout_seconds"]
        consumer = run_stage(
            "event_bus_connect",
            True,
            lambda: PipelineEventConsumer(config, group_id=group_id),
        )
        lag_reader = run_stage("event_bus_connect", True, lambda: ConsumerLagReader(config))
        publisher: MetricPublisher
        if args.dry_run:
            publisher = JsonMetricPublisher()
        else:
            publisher = run_stage(
                "oci_metric_connect",
                True,
                lambda: OciMetricPublisher.from_instance_principal(
                    namespace=metric_config["namespace"]
                ),
            )
        accumulator = PipelineMetricAccumulator(
            source_failure_min_rejections=metric_config["source_failure_min_rejections"]
        )
    except Exception as error:
        return handle_unconsumed_error(
            service_name=SERVICE_NAME,
            failure_event="pipeline_metrics_failed",
            error=error,
            started_at=started_at,
        )

    try:
        while not shutdown.requested:
            try:
                collect_and_publish(
                    consumer=consumer,
                    accumulator=accumulator,
                    lag_reader=lag_reader,
                    publisher=publisher,
                    poll_timeout_seconds=poll_timeout_seconds,
                    flush_interval_seconds=flush_interval_seconds,
                    consumer_lag_groups=metric_config["consumer_lag_groups"],
                )
            except Exception as error:
                log_event(
                    SERVICE_NAME,
                    "pipeline_metrics_failed",
                    level="error",
                    error_class=type(error).__name__,
                    error_message=str(error),
                )
                if args.once:
                    return 1
                time.sleep(min(30, flush_interval_seconds))
                continue
            if args.once:
                return 0
        log_event(SERVICE_NAME, "pipeline_metrics_stopped", duration_ms=elapsed_ms(started_at))
        return 0
    finally:
        consumer.close()


def main() -> None:
    raise SystemExit(run())


if __name__ == "__main__":
    main()
