# syntax=docker/dockerfile:1.7
FROM python:3.12.13-slim AS builder

ENV UV_PROJECT_ENVIRONMENT=/app/.venv
ENV UV_LINK_MODE=copy

COPY --from=ghcr.io/astral-sh/uv:0.11.17 /uv /usr/local/bin/uv

COPY --from=vn-news-platform-lib / /app/vn-news-platform-lib/
WORKDIR /app/vn-news-services

COPY pyproject.toml uv.lock ./
COPY services/common/pyproject.toml services/common/pyproject.toml
COPY services/feed_ingestor/pyproject.toml services/feed_ingestor/pyproject.toml
COPY services/article_fetcher/pyproject.toml services/article_fetcher/pyproject.toml
COPY services/article_extractor/pyproject.toml services/article_extractor/pyproject.toml

RUN --mount=type=cache,id=uv-feed-ingestor,sharing=locked,target=/root/.cache/uv \
    uv sync --frozen --no-dev --package news-feed-ingestor --no-install-workspace

COPY services/common services/common
COPY services/feed_ingestor services/feed_ingestor

RUN --mount=type=cache,id=uv-feed-ingestor,sharing=locked,target=/root/.cache/uv \
    uv sync --frozen --no-dev --package news-feed-ingestor --no-editable

FROM python:3.12.13-slim

WORKDIR /app

ENV PATH="/app/.venv/bin:$PATH"
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV VN_NEWS_CONFIG_DIR=/app/configs

RUN addgroup --system app && adduser --system --ingroup app app

COPY --from=builder /app/.venv /app/.venv

USER app

ENTRYPOINT ["python", "-m", "news_feed_ingestor.main"]
