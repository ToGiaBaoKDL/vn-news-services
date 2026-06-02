from __future__ import annotations

import httpx
from confluent_kafka.admin import AdminClient
from news_platform.config import load_settings

from news_service_common.storage import S3PayloadStore
from news_service_common.telemetry import log_event

SERVICE_NAME = "dependency_health"


def check_dependencies() -> None:
    config = load_settings()
    AdminClient({"bootstrap.servers": config["event_bus"]["bootstrap_servers"]}).list_topics(
        timeout=5
    )
    response = httpx.get(
        f"{config['event_bus']['schema_registry_url'].rstrip('/')}/subjects",
        timeout=5,
    )
    response.raise_for_status()
    S3PayloadStore(endpoint_url=config["storage"]["endpoint_url"]).check_bucket(
        config["storage"]["buckets"]["landing"]
    )


def main() -> None:
    try:
        check_dependencies()
    except Exception as error:
        log_event(
            SERVICE_NAME,
            "dependency_health_failed",
            level="error",
            error_class=type(error).__name__,
            error_message=str(error),
        )
        raise SystemExit(1) from error
    log_event(SERVICE_NAME, "dependency_health_succeeded")


if __name__ == "__main__":
    main()
