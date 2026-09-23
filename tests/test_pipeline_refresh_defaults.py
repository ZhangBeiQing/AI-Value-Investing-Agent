from services.pipeline.steps import refresh_data
from services.pipeline.steps import build_stock_research


def test_run_refresh_data_skips_disclosures_by_default(monkeypatch) -> None:
    captured = {}

    def fake_manage_daily_data(*args, **kwargs) -> None:
        captured["args"] = args
        captured["kwargs"] = kwargs

    monkeypatch.setattr(refresh_data, "run_manage_daily_data", fake_manage_daily_data)

    refresh_data.run_refresh_data("2026-09-01", symbols=["601985.SH"])

    assert captured["args"] == ("2026-09-01",)
    assert captured["kwargs"]["skip_disclosures"] is True


def test_research_artifact_uses_cached_news_by_default(monkeypatch) -> None:
    captured = {}

    monkeypatch.setattr(
        build_stock_research,
        "analyze_stock_dynamics_and_valuation",
        lambda *args, **kwargs: {},
    )
    monkeypatch.setattr(
        build_stock_research,
        "get_financial_report_summary",
        lambda *args, **kwargs: {},
    )

    def fake_search_stock_news(*args, **kwargs) -> str:
        captured["allow_refresh"] = kwargs["allow_refresh"]
        return "{}"

    monkeypatch.setattr(build_stock_research, "search_stock_news", fake_search_stock_news)

    build_stock_research._build_base_artifact(
        "601985.SH",
        "2026-09-01",
        allow_news_refresh=False,
    )

    assert captured["allow_refresh"] is False
