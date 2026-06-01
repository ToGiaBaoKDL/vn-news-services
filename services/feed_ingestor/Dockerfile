# syntax=docker/dockerfile:1.7
FROM python:3.12.13-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV VN_NEWS_CONFIG_DIR=/app/configs
ENV UV_PROJECT_ENVIRONMENT=/app/.venv
ENV PATH="/app/.venv/bin:$PATH"

COPY --from=ghcr.io/astral-sh/uv:0.11.17 /uv /usr/local/bin/uv

COPY --from=vn-news-platform-lib / /app/vn-news-platform-lib/
COPY . /app/vn-news-services/

WORKDIR /app/vn-news-services

RUN uv sync --frozen --no-dev --package news-feed-ingestor

ENTRYPOINT ["python", "-m", "news_feed_ingestor.main"]
