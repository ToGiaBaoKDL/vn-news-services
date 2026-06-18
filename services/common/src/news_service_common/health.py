from __future__ import annotations

import argparse
from collections.abc import Iterable

import httpx
from confluent_kafka.admin import AdminClient
from news_platform.config import load_settings

from news_service_common.storage import S3PayloadStore
from news_service_common.telemetry import log_event

SERVICE_NAME = "dependency_health"
DEFAULT_CHECKS = ("event_bus", "schema_registry", "landing_storage")
VALID_CHECKS = frozenset(DEFAULT_CHECKS)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Check required service dependencies.")
    parser.add_argument(
        "--checks",
        default=",".join(DEFAULT_CHECKS),
        help="Comma-separated checks: event_bus,schema_registry,landing_storage.",
    )
    return parser.parse_args()


def parse_checks(value: str) -> tuple[str, ...]:
    checks = tuple(check.strip() for check in value.split(",") if check.strip())
    unknown = sorted(set(checks) - VALID_CHECKS)
    if unknown:
        raise ValueError(f"Unknown dependency health checks: {unknown}")
    if not checks:
        raise ValueError("At least one dependency health check is required")
    return checks


def check_dependencies(checks: Iterable[str] = DEFAULT_CHECKS) -> None:
    selected_checks = set(checks)
    config = load_settings()
    if "event_bus" in selected_checks:
        AdminClient({"bootstrap.servers": config["event_bus"]["bootstrap_servers"]}).list_topics(
            timeout=5
        )
    if "schema_registry" in selected_checks:
        response = httpx.get(
            f"{config['event_bus']['schema_registry_url'].rstrip('/')}/subjects",
            timeout=5,
        )
        response.raise_for_status()
    if "landing_storage" in selected_checks:
        S3PayloadStore(endpoint_url=config["storage"]["endpoint_url"]).check_bucket(
            config["storage"]["buckets"]["landing"]
        )


def main() -> None:
    try:
        args = parse_args()
        check_dependencies(parse_checks(args.checks))
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
