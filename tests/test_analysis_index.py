import json

from services.trading.analysis_index import update_analysis_index


def _write_snapshot(root, run_date, symbol, price):
    path = root / run_date / "fixed_tracked" / "02_basic_snapshot_payload.json"
    path.parent.mkdir(parents=True)
    path.write_text(
        json.dumps({"stocks": {symbol: {"latest_price": price}}}), encoding="utf-8"
    )


def test_update_analysis_index_uses_isolated_dashboard_snapshot(tmp_path):
    official_root = tmp_path / "official"
    isolated_root = tmp_path / "isolated"
    _write_snapshot(isolated_root, "2026-09-21", "01801.HK", 89.35)

    update_analysis_index(
        {
            "stock_decisions": [
                {
                    "symbol": "01801.HK",
                    "price_impression": "合理偏低估",
                    "confidence_score": 0.72,
                    "sizing_reason": "测试",
                }
            ]
        },
        "fixed_tracked",
        "2026-09-21",
        official_root,
        snapshot_root=isolated_root,
    )

    index = json.loads((official_root / "_analysis_index.json").read_text(encoding="utf-8"))
    entry = index["fixed_tracked"]["01801.HK"]
    assert entry["deep_analysis_date"] == "2026-09-21"
    assert entry["last_deep_analysis_price"] == 89.35
    assert entry["price_impression"] == "合理偏低估"


def test_update_analysis_index_does_not_regress_to_older_analysis(tmp_path):
    root = tmp_path / "skill_runs"
    root.mkdir()
    (root / "_analysis_index.json").write_text(
        json.dumps(
            {
                "fixed_tracked": {
                    "01801.HK": {
                        "deep_analysis_date": "2026-09-21",
                        "last_deep_analysis_price": 89.35,
                        "price_impression": "合理",
                    }
                }
            }
        ),
        encoding="utf-8",
    )

    update_analysis_index(
        {"stock_decisions": [{"symbol": "01801.HK", "price_impression": "偏贵"}]},
        "fixed_tracked",
        "2026-09-18",
        root,
    )

    index = json.loads((root / "_analysis_index.json").read_text(encoding="utf-8"))
    assert index["fixed_tracked"]["01801.HK"]["deep_analysis_date"] == "2026-09-21"
    assert index["fixed_tracked"]["01801.HK"]["price_impression"] == "合理"


def test_named_index_migration_update_and_price_report_preserve_stock(tmp_path):
    from services.pipeline.steps.build_agent_input import write_price_delta_report

    root = tmp_path / "skill_runs"
    _write_snapshot(root, "2026-07-01", "512800.SH", 0.737)
    snapshot = {"stocks": {"512800.SH": {"stock_name": "银行ETF", "latest_price": 0.748}}}
    path = root / "2026-07-02" / "fixed_tracked" / "02_basic_snapshot_payload.json"
    path.parent.mkdir(parents=True)
    path.write_text(json.dumps(snapshot, ensure_ascii=False), encoding="utf-8")
    index_path = root / "_analysis_index.json"
    index_path.write_text(json.dumps({"fixed_tracked": {"512800.SH": {
        "deep_analysis_date": "2026-07-01", "last_deep_analysis_price": 0.737,
        "price_impression": "合理偏低估",
    }}}), encoding="utf-8")

    update_analysis_index({}, "fixed_tracked", "2026-07-02", root)
    migrated = json.loads(index_path.read_text(encoding="utf-8"))["fixed_tracked"]
    assert list(migrated) == ["银行ETF_512800.SH"]
    assert migrated["银行ETF_512800.SH"]["deep_analysis_date"] == "2026-07-01"
    assert migrated["银行ETF_512800.SH"]["symbol"] == "512800.SH"
    report = write_price_delta_report(
        "2026-07-02", path.parent, snapshot, analysis_index_ref=str(index_path)
    )
    assert "银行ETF（512800.SH）" in report.read_text(encoding="utf-8")
    assert "+1.5%" in report.read_text(encoding="utf-8")

    update_analysis_index({"stock_decisions": [{
        "symbol": "512800.SH", "stock_name": "银行ETF", "price_impression": "合理",
    }]}, "fixed_tracked", "2026-07-02", root)
    updated = json.loads(index_path.read_text(encoding="utf-8"))["fixed_tracked"]
    assert list(updated) == ["银行ETF_512800.SH"]
    assert updated["银行ETF_512800.SH"]["last_deep_analysis_price"] == 0.748

    update_analysis_index({}, "fixed_tracked", "2026-07-03", root)
    assert list(json.loads(index_path.read_text(encoding="utf-8"))["fixed_tracked"]) == ["银行ETF_512800.SH"]
