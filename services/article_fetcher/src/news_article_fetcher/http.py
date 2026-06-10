from __future__ import annotations

import time
from collections.abc import Callable, Sequence

import httpx

from news_article_fetcher.models import ArticleHttpResponse
from news_service_common.errors import HttpFetchError
from news_service_common.http import is_retryable_http_status, read_limited_content
from news_service_common.url_safety import UrlSafetyPolicy

DEFAULT_INVALID_DOCUMENT_MARKERS = (
    "<title>just a moment",
    "checking your browser before accessing",
    "/cdn-cgi/challenge-platform/",
    "cf-chl-",
    "__jsl_clearance",
    "<title>access denied</title>",
    "<title>request rejected</title>",
    "<title>403 forbidden</title>",
    "<title>404 not found</title>",
    "<title>not found</title>",
)

HTML_CONTENT_TYPES = {"text/html", "application/xhtml+xml"}


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
        url_policy: UrlSafetyPolicy | None = None,
        blocked_status_codes: list[int] | set[int] | tuple[int, ...] = (),
        invalid_document_markers: Sequence[str] = (),
    ) -> None:
        self.user_agent = user_agent
        self.timeout_seconds = timeout_seconds
        self.max_article_bytes = max_article_bytes
        self.retry_attempts = retry_attempts
        self.retry_backoff_seconds = retry_backoff_seconds
        self.sleep = sleep
        self.client_factory = client_factory or (
            lambda: httpx.Client(follow_redirects=False, timeout=self.timeout_seconds)
        )
        self.on_retry = on_retry
        self.url_policy = url_policy
        self.blocked_status_codes = set(blocked_status_codes)
        self.invalid_document_markers = tuple(
            marker.strip()
            for marker in (*DEFAULT_INVALID_DOCUMENT_MARKERS, *invalid_document_markers)
            if marker.strip()
        )

    def fetch(self, url: str) -> ArticleHttpResponse:
        if self.url_policy:
            url = self.url_policy.validate_url(url, stage="article_fetch", resolve=True)
        headers = {
            "Accept": "text/html,application/xhtml+xml;q=0.9,*/*;q=0.8",
            "User-Agent": self.user_agent,
        }
        with self.client_factory() as client:
            for attempt in range(1, self.retry_attempts + 1):
                try:
                    return self._fetch_once(client, url, headers)
                except httpx.HTTPStatusError as error:
                    status_code = error.response.status_code
                    if status_code in self.blocked_status_codes:
                        raise HttpFetchError(
                            f"Article fetch blocked by source HTTP policy: status {status_code}",
                            stage="article_fetch",
                            retryable=False,
                            status_code=status_code,
                            error_class="SourceHttpPolicyBlocked",
                        ) from error
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
                except HttpFetchError as error:
                    if not error.retryable or attempt == self.retry_attempts:
                        raise
                    self._sleep_before_retry(attempt, error, status_code=error.status_code)
        raise RuntimeError("Article fetch loop exited unexpectedly")

    def _fetch_once(
        self,
        client: httpx.Client,
        url: str,
        headers: dict[str, str],
    ) -> ArticleHttpResponse:
        current_url = url
        for redirect_count in range(self._max_redirects() + 1):
            with client.stream("GET", current_url, headers=headers) as response:
                if response.is_redirect:
                    if redirect_count == self._max_redirects():
                        raise HttpFetchError(
                            "Article redirect limit exceeded",
                            stage="article_fetch",
                            retryable=False,
                            status_code=response.status_code,
                        )
                    location = response.headers.get("location", "")
                    if not location:
                        raise HttpFetchError(
                            "Article redirect response is missing Location header",
                            stage="article_fetch",
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
                            stage="article_fetch",
                        )
                    current_url = next_url
                    continue
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
                self._validate_article_document(response, content)
                return ArticleHttpResponse(
                    status_code=response.status_code,
                    content=content,
                    content_type=response.headers.get("content-type"),
                )
        raise RuntimeError("Article redirect loop exited unexpectedly")

    def _max_redirects(self) -> int:
        return self.url_policy.max_redirects if self.url_policy else 5

    def _validate_article_document(self, response: httpx.Response, content: bytes) -> None:
        content_type = response.headers.get("content-type")
        if content_type and media_type(content_type) not in HTML_CONTENT_TYPES:
            raise HttpFetchError(
                f"Article response is not HTML: {content_type}",
                stage="article_fetch",
                retryable=False,
                status_code=response.status_code,
                error_class="InvalidArticleDocument",
            )
        marker = matched_invalid_document_marker(content, self.invalid_document_markers)
        if marker:
            raise HttpFetchError(
                f"Article response matches invalid-document marker: {marker}",
                stage="article_fetch",
                retryable=False,
                status_code=response.status_code,
                error_class="InvalidArticleDocument",
            )

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


def media_type(content_type: str) -> str:
    return content_type.split(";", maxsplit=1)[0].strip().lower()


def matched_invalid_document_marker(content: bytes, markers: Sequence[str]) -> str | None:
    sample = content[:65536].decode("utf-8", errors="ignore").lower()
    for marker in markers:
        if marker.lower() in sample:
            return marker
    return None
