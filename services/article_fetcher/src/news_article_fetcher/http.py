from __future__ import annotations

import time
from collections.abc import Callable

import httpx

from news_article_fetcher.models import ArticleHttpResponse
from news_service_common.errors import HttpFetchError
from news_service_common.http import is_retryable_http_status, read_limited_content


class ArticleHttpClient:
    def __init__(
        self,
        *,
        user_agent: str,
        timeout_seconds: int,
        max_article_bytes: int,
        retry_attempts: int,
        retry_backoff_seconds: int,
        sleep: Callable[[float], None] = time.sleep,
        client_factory: Callable[[], httpx.Client] | None = None,
        on_retry: Callable[..., None] | None = None,
    ) -> None:
        self.user_agent = user_agent
        self.timeout_seconds = timeout_seconds
        self.max_article_bytes = max_article_bytes
        self.retry_attempts = retry_attempts
        self.retry_backoff_seconds = retry_backoff_seconds
        self.sleep = sleep
        self.client_factory = client_factory or (
            lambda: httpx.Client(follow_redirects=True, timeout=self.timeout_seconds)
        )
        self.on_retry = on_retry

    def fetch(self, url: str) -> ArticleHttpResponse:
        headers = {
            "Accept": "text/html,application/xhtml+xml;q=0.9,*/*;q=0.8",
            "User-Agent": self.user_agent,
        }
        with self.client_factory() as client:
            for attempt in range(1, self.retry_attempts + 1):
                try:
                    with client.stream("GET", url, headers=headers) as response:
                        if is_retryable_http_status(response.status_code):
                            response.raise_for_status()
                        response.raise_for_status()
                        content = read_limited_content(
                            response,
                            self.max_article_bytes,
                            error_factory=lambda message, status_code: HttpFetchError(
                                message,
                                stage="article_fetch",
                                retryable=False,
                                status_code=status_code,
                            ),
                            payload_name="Article",
                        )
                        if not content:
                            raise HttpFetchError(
                                "Article response is empty",
                                stage="article_fetch",
                                retryable=False,
                                status_code=response.status_code,
                            )
                        return ArticleHttpResponse(
                            status_code=response.status_code,
                            content=content,
                            content_type=response.headers.get("content-type"),
                        )
                except httpx.HTTPStatusError as error:
                    status_code = error.response.status_code
                    retryable = is_retryable_http_status(status_code)
                    if not retryable or attempt == self.retry_attempts:
                        raise HttpFetchError(
                            str(error),
                            stage="article_fetch",
                            retryable=retryable,
                            status_code=status_code,
                            error_class=type(error).__name__,
                        ) from error
                    self._sleep_before_retry(attempt, error, status_code=status_code)
                except httpx.TransportError as error:
                    if attempt == self.retry_attempts:
                        raise HttpFetchError(
                            str(error),
                            stage="article_fetch",
                            retryable=True,
                            error_class=type(error).__name__,
                        ) from error
                    self._sleep_before_retry(attempt, error)
        raise RuntimeError("Article fetch loop exited unexpectedly")

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
                error_class=type(error).__name__,
                error_message=str(error),
                status_code=status_code,
            )
        self.sleep(delay_seconds)
