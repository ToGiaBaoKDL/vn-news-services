from __future__ import annotations

from news_service_common import health


def test_dependency_health_checks_event_bus_registry_and_landing_bucket(monkeypatch) -> None:
    calls: list[tuple[str, str]] = []

    class FakeAdminClient:
        def __init__(self, config: dict[str, str]) -> None:
            calls.append(("redpanda", config["bootstrap.servers"]))

        def list_topics(self, *, timeout: int) -> None:
            assert timeout == 5

    class FakeResponse:
        def raise_for_status(self) -> None:
            calls.append(("schema_registry", "ok"))

    class FakeStore:
        def __init__(self, *, endpoint_url: str) -> None:
            calls.append(("storage", endpoint_url))

        def check_bucket(self, bucket: str) -> None:
            calls.append(("bucket", bucket))

    monkeypatch.setattr(
        health,
        "load_settings",
        lambda: {
            "event_bus": {
                "bootstrap_servers": "redpanda:9092",
                "schema_registry_url": "http://redpanda:8081",
            },
            "storage": {
                "endpoint_url": "http://seaweedfs-s3:8333",
                "buckets": {"landing": "tgb-prod-landing-a7k3p9"},
            },
        },
    )
    monkeypatch.setattr(health, "AdminClient", FakeAdminClient)
    monkeypatch.setattr(health.httpx, "get", lambda url, timeout: FakeResponse())
    monkeypatch.setattr(health, "S3PayloadStore", FakeStore)

    health.check_dependencies()

    assert calls == [
        ("redpanda", "redpanda:9092"),
        ("schema_registry", "ok"),
        ("storage", "http://seaweedfs-s3:8333"),
        ("bucket", "tgb-prod-landing-a7k3p9"),
    ]


def test_dependency_health_can_skip_storage(monkeypatch) -> None:
    calls: list[tuple[str, str]] = []

    class FakeAdminClient:
        def __init__(self, config: dict[str, str]) -> None:
            calls.append(("redpanda", config["bootstrap.servers"]))

        def list_topics(self, *, timeout: int) -> None:
            assert timeout == 5

    class FakeResponse:
        def raise_for_status(self) -> None:
            calls.append(("schema_registry", "ok"))

    def fail_storage(*args: object, **kwargs: object) -> None:
        raise AssertionError("storage health should not run")

    monkeypatch.setattr(
        health,
        "load_settings",
        lambda: {
            "event_bus": {
                "bootstrap_servers": "redpanda:9092",
                "schema_registry_url": "http://redpanda:8081",
            },
            "storage": {},
        },
    )
    monkeypatch.setattr(health, "AdminClient", FakeAdminClient)
    monkeypatch.setattr(health.httpx, "get", lambda url, timeout: FakeResponse())
    monkeypatch.setattr(health, "S3PayloadStore", fail_storage)

    health.check_dependencies(("event_bus", "schema_registry"))

    assert calls == [
        ("redpanda", "redpanda:9092"),
        ("schema_registry", "ok"),
    ]


def test_parse_checks_rejects_unknown_dependency() -> None:
    try:
        health.parse_checks("event_bus,unknown")
    except ValueError as error:
        assert "unknown" in str(error)
    else:
        raise AssertionError("expected unknown health check to fail")
