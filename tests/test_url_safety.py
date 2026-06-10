from __future__ import annotations

import ipaddress
import socket

import pytest

from news_service_common.errors import IngestionError
from news_service_common.url_safety import UrlSafetyPolicy


def test_url_safety_allows_source_domain_and_subdomain() -> None:
    policy = UrlSafetyPolicy(
        "vnexpress.net",
        resolver=lambda host, port: [ipaddress.ip_address("8.8.8.8")],
    )

    assert (
        policy.validate_url("https://vnexpress.net/a.html", stage="article_fetch", resolve=True)
        == "https://vnexpress.net/a.html"
    )
    assert policy.validate_url(
        "https://rss.vnexpress.net/a.html",
        stage="article_fetch",
        resolve=False,
    )


def test_url_safety_rejects_http_and_external_domain() -> None:
    policy = UrlSafetyPolicy("vnexpress.net")

    with pytest.raises(IngestionError, match="https"):
        policy.validate_url("http://vnexpress.net/a.html", stage="article_fetch", resolve=False)

    with pytest.raises(IngestionError, match="outside allowed domain"):
        policy.validate_url("https://evil.example/a.html", stage="article_fetch", resolve=False)


def test_url_safety_allows_redirect_domain_only_for_redirect_target() -> None:
    policy = UrlSafetyPolicy(
        "baochinhphu.vn",
        allowed_redirect_domains=("thanglong.chinhphu.vn",),
        resolver=lambda host, port: [ipaddress.ip_address("8.8.8.8")],
    )

    with pytest.raises(IngestionError, match="outside allowed domain"):
        policy.validate_url(
            "https://thanglong.chinhphu.vn/a.html",
            stage="article_fetch",
            resolve=False,
        )

    assert (
        policy.redirect_target(
            "https://baochinhphu.vn/a.html",
            "https://thanglong.chinhphu.vn/b.html",
            stage="article_fetch",
        )
        == "https://thanglong.chinhphu.vn/b.html"
    )


def test_url_safety_rejects_private_dns_targets() -> None:
    policy = UrlSafetyPolicy(
        "vnexpress.net",
        resolver=lambda host, port: [ipaddress.ip_address("10.0.0.5")],
    )

    with pytest.raises(IngestionError, match="non-public"):
        policy.validate_url("https://vnexpress.net/a.html", stage="article_fetch", resolve=True)


def test_url_safety_treats_dns_failure_as_retryable() -> None:
    def fail_dns(host: str, port: int):
        raise socket.gaierror("temporary failure")

    policy = UrlSafetyPolicy("vnexpress.net", resolver=fail_dns)

    with pytest.raises(IngestionError) as raised:
        policy.validate_url("https://vnexpress.net/a.html", stage="article_fetch", resolve=True)

    assert raised.value.retryable is True
