from subprocess import CompletedProcess

from tools.read_article import fetched_html, latest_records, resolve_article_id, selected_stages


def test_resolve_article_id_normalizes_tracking_parameters() -> None:
    tracked = resolve_article_id(None, "https://VNEXPRESS.net/a?utm_source=x&id=1")
    clean = resolve_article_id(None, "https://vnexpress.net/a?id=1")

    assert tracked == clean


def test_selected_stages_preserves_pipeline_order() -> None:
    assert selected_stages("all") == ("fetched", "extracted")
    assert selected_stages("fetched") == ("fetched",)


def test_latest_records_returns_latest_record_per_stage() -> None:
    records = [
        {"stage": "fetched", "timestamp": 1},
        {"stage": "extracted", "timestamp": 2},
        {"stage": "fetched", "timestamp": 3},
    ]

    assert latest_records(records) == [records[2], records[1]]


def test_fetched_html_uses_article_fetcher_storage_access(monkeypatch) -> None:
    calls: list[list[str]] = []

    def run(command, **kwargs):
        calls.append(command)
        return CompletedProcess(command, 0, stdout=b"article body")

    monkeypatch.setattr("tools.read_article.subprocess.run", run)

    assert (
        fetched_html(
            payload_uri="s3://landing/article.html.zst",
            storage_host="tgb-processing-1",
            storage_container="vn-news-processing-article-fetcher-1",
        )
        == "article body"
    )
    assert calls[0][0:2] == ["ssh", "tgb-processing-1"]
    assert "vn-news-processing-article-fetcher-1" in calls[0][2]
    assert "S3PayloadStore" in calls[0][2]
