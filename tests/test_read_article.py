from tools.read_article import latest_records, resolve_article_id, selected_stages


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
