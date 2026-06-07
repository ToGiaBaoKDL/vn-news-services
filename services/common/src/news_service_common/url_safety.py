from __future__ import annotations

import ipaddress
import socket
from collections.abc import Callable
from dataclasses import dataclass
from urllib.parse import urljoin, urlparse

from news_service_common.errors import IngestionError

Resolver = Callable[[str, int], list[ipaddress.IPv4Address | ipaddress.IPv6Address]]


def default_resolver(host: str, port: int) -> list[ipaddress.IPv4Address | ipaddress.IPv6Address]:
    addresses = []
    for result in socket.getaddrinfo(host, port, type=socket.SOCK_STREAM):
        address = result[4][0]
        addresses.append(ipaddress.ip_address(address))
    return sorted(set(addresses), key=str)


@dataclass(frozen=True)
class UrlSafetyPolicy:
    allowed_domain: str
    resolver: Resolver = default_resolver
    max_redirects: int = 5

    def validate_url(
        self,
        url: str,
        *,
        stage: str,
        resolve: bool,
        retryable_dns: bool = True,
    ) -> str:
        parsed = urlparse(url)
        if parsed.scheme.lower() != "https":
            raise self._error(stage, f"URL must use https: {url}", retryable=False)
        host = normalize_host(parsed.hostname)
        if not host:
            raise self._error(stage, f"URL host is missing: {url}", retryable=False)
        if not host_matches_domain(host, self.allowed_domain):
            raise self._error(
                stage,
                f"URL host {host} is outside allowed domain {self.allowed_domain}",
                retryable=False,
            )
        if resolve:
            self.validate_dns(host, parsed.port or 443, stage=stage, retryable_dns=retryable_dns)
        return url

    def redirect_target(self, current_url: str, location: str, *, stage: str) -> str:
        if not location:
            raise self._error(
                stage,
                "Redirect response is missing Location header",
                retryable=False,
            )
        return self.validate_url(
            urljoin(current_url, location),
            stage=stage,
            resolve=True,
            retryable_dns=True,
        )

    def validate_dns(
        self,
        host: str,
        port: int,
        *,
        stage: str,
        retryable_dns: bool,
    ) -> None:
        try:
            addresses = self.resolver(host, port)
        except OSError as error:
            raise self._error(
                stage,
                f"DNS resolution failed for {host}: {error}",
                retryable=retryable_dns,
                error_class=type(error).__name__,
            ) from error
        if not addresses:
            raise self._error(
                stage,
                f"DNS returned no addresses for {host}",
                retryable=retryable_dns,
            )
        blocked = [str(address) for address in addresses if not address.is_global]
        if blocked:
            raise self._error(
                stage,
                f"DNS returned non-public addresses for {host}: {blocked}",
                retryable=False,
            )

    @staticmethod
    def _error(
        stage: str,
        message: str,
        *,
        retryable: bool,
        error_class: str | None = None,
    ) -> IngestionError:
        return IngestionError(
            stage=stage,
            retryable=retryable,
            message=message,
            error_class=error_class or "UrlSafetyError",
        )


def normalize_host(host: str | None) -> str | None:
    if not host:
        return None
    return host.rstrip(".").lower().encode("idna").decode("ascii")


def host_matches_domain(host: str, domain: str) -> bool:
    normalized_domain = normalize_host(domain)
    return bool(normalized_domain) and (
        host == normalized_domain or host.endswith(f".{normalized_domain}")
    )
