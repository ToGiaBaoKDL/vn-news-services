from __future__ import annotations

import time
from collections.abc import Callable

import httpx

from news_feed_ingestor.models import FeedCheckpoint, FeedResponse
from news_service_common.errors import FeedFetchError
from news_service_common.http import is_retryable_http_status, read_limited_content
from news_service_common.url_safety import UrlSafetyPolicy


class HttpFeedClient:
    def __init__(
        self,
        *,
        user_agent: str,
        timeout_seconds: int,
        max_feed_bytes: int,
        retry_attempts: int,
        retry_backoff_seconds: int,
        sleep: Callable[[float], None] = time.sleep,
        client_factory: Callable[[], httpx.Client] | None = None,
        on_retry: Callable[..., None] | None = None,
        url_policy: UrlSafetyPolicy | None = None,
    ) -> None:
        self.user_agent = user_agent
        self.timeout_seconds = timeout_seconds
        self.max_feed_bytes = max_feed_bytes
        self.retry_attempts = retry_attempts
        self.retry_backoff_seconds = retry_backoff_seconds
        self.sleep = sleep
        self.client_factory = client_factory or (
            lambda: httpx.Client(follow_redirects=False, timeout=self.timeout_seconds)
        )
        self.on_retry = on_retry
        self.url_policy = url_policy

    def fetch(self, url: str, checkpoint: FeedCheckpoint | None = None) -> FeedResponse:
        if self.url_policy:
            url = self.url_policy.validate_url(url, stage="feed_fetch", resolve=True)
        headers = {
            "Accept": "application/rss+xml, application/xml;q=0.9, */*;q=0.8",
            "User-Agent": self.user_agent,
        }
        if checkpoint and checkpoint.events_published:
            if checkpoint.etag:
                headers["If-None-Match"] = checkpoint.etag
            if checkpoint.last_modified:
                headers["If-Modified-Since"] = checkpoint.last_modified

        with self.client_factory() as client:
            for attempt in range(1, self.retry_attempts + 1):
                try:
                    return self._fetch_once(client, url, headers, checkpoint)
                except httpx.HTTPStatusError as error:
                    status_code = error.response.status_code
                    retryable = is_retryable_http_status(status_code)
                    if not retryable or attempt == self.retry_attempts:
                        raise FeedFetchError(
                            str(error),
                            retryable=retryable,
                            status_code=status_code,
                            error_class=type(error).__name__,
                        ) from error
                    self._sleep_before_retry(attempt, error, status_code=status_code)
                except httpx.TransportError as error:
                    if attempt == self.retry_attempts:
                        raise FeedFetchError(
                            str(error),
                            retryable=True,
                            error_class=type(error).__name__,
                        ) from error
                    self._sleep_before_retry(attempt, error)
                except FeedFetchError as error:
                    if not error.retryable or attempt == self.retry_attempts:
                        raise
                    self._sleep_before_retry(attempt, error, status_code=error.status_code)
        raise RuntimeError("RSS fetch loop exited unexpectedly")

    def _fetch_once(
        self,
        client: httpx.Client,
        url: str,
        headers: dict[str, str],
        checkpoint: FeedCheckpoint | None,
    ) -> FeedResponse:
        current_url = url
        for redirect_count in range(self._max_redirects() + 1):
            with client.stream("GET", current_url, headers=headers) as response:
                if response.status_code == 304:
                    if not checkpoint or not checkpoint.events_published:
                        raise FeedFetchError(
                            "Received HTTP 304 without a published checkpoint",
                            retryable=False,
                            status_code=304,
                        )
                    return FeedResponse(304, b"", checkpoint.etag, checkpoint.last_modified)
                if response.is_redirect:
                    if redirect_count == self._max_redirects():
                        raise FeedFetchError(
                            "RSS redirect limit exceeded",
                            retryable=False,
                            status_code=response.status_code,
                        )
                    location = response.headers.get("location", "")
                    if not location:
                        raise FeedFetchError(
                            "RSS redirect response is missing Location header",
                            retryable=True,
                            status_code=response.status_code,
                            error_class="MalformedRedirect",
                        )
                    if not self.url_policy:
                        next_url = str(response.next_request.url) if response.next_request else ""
                    else:
                        next_url = self.url_policy.redirect_target(
                            str(response.url),
                            location,
                            stage="feed_fetch",
                        )
                    current_url = next_url
                    continue
                if is_retryable_http_status(response.status_code):
                    response.raise_for_status()
                response.raise_for_status()
                return FeedResponse(
                    status_code=response.status_code,
                    content=read_limited_content(
                        response,
                        self.max_feed_bytes,
                        error_factory=lambda message, status_code: FeedFetchError(
                            message,
                            retryable=False,
                            status_code=status_code,
                        ),
                        payload_name="RSS",
                    ),
                    etag=response.headers.get("etag"),
                    last_modified=response.headers.get("last-modified"),
                )
        raise RuntimeError("RSS redirect loop exited unexpectedly")

    def _max_redirects(self) -> int:
        return self.url_policy.max_redirects if self.url_policy else 5

    def _sleep_before_retry(
        self,
        attempt: int,
        error: Exception,
        *,
        status_code: int | None = None,
    ) -> None:
        delay_seconds = self.retry_backoff_seconds * attempt
        if self.on_retry:
            self.on_retry(
                attempt=attempt,
                max_attempts=self.retry_attempts,
                delay_seconds=delay_seconds,
                error_class=getattr(error, "error_class", type(error).__name__),
                error_message=str(error),
                status_code=status_code,
            )
        self.sleep(delay_seconds)
