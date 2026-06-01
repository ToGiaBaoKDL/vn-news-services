from __future__ import annotations

from collections.abc import Callable

import httpx

from news_service_common.errors import IngestionError

ErrorFactory = Callable[[str, int | None], IngestionError]


def is_retryable_http_status(status_code: int) -> bool:
    return status_code in {408, 425, 429} or status_code >= 500


def read_limited_content(
    response: httpx.Response,
    max_bytes: int,
    *,
    error_factory: ErrorFactory,
    payload_name: str,
) -> bytes:
    content_length = response.headers.get("content-length")
    if content_length:
        try:
            declared_bytes = int(content_length)
        except ValueError:
            pass
        else:
            if declared_bytes > max_bytes:
                raise error_factory(
                    f"{payload_name} response exceeds {max_bytes} byte limit",
                    response.status_code,
                )

    chunks: list[bytes] = []
    received_bytes = 0
    for chunk in response.iter_bytes():
        received_bytes += len(chunk)
        if received_bytes > max_bytes:
            raise error_factory(
                f"{payload_name} response exceeds {max_bytes} byte limit",
                response.status_code,
            )
        chunks.append(chunk)
    return b"".join(chunks)
