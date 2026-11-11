"""Read-only cache preflight; historical input generation never starts a refresh."""

from commons import get_stock_data_dir, is_etf_symbol, parse_symbol
from services.backtest.experiment import BacktestExperiment
from services.backtest.prices import historical_close, quote_date_as_of


def inspect_shared_inputs(experiment: BacktestExperiment, run_date: str, symbols) -> dict:
    issues = []
    for symbol in symbols:
        expected = quote_date_as_of(symbol, run_date)
        quote = historical_close(symbol, run_date, experiment.context.source_data_root)
        if quote is None or quote[0] != expected:
            issues.append({"symbol": symbol, "dataset": "price", "required_date": expected,
                           "actual_date": quote[0] if quote else None})
        info = parse_symbol(symbol)
        root = get_stock_data_dir(info, base_dir=experiment.context.source_data_root)
        if not is_etf_symbol(info) and not (root / "disclosures/index.json").is_file():
            issues.append({"symbol": symbol, "dataset": "disclosures", "required_date": run_date})
    return {"status": "needs_shared_data" if issues else "ready", "issues": issues,
            "note": "仅检查缓存；缺口通过现有数据准备入口补齐，不在 prepare-day 自动刷新。"}
