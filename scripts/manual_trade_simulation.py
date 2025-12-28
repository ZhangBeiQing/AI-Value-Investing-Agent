"""
手动调试 buy/sell MCP 工具的迷你脚本。

运行方式:
    python scripts/manual_trade_simulation.py

脚本会:
1. 在 tmp/manual_trade_env.json 写入 SIGNATURE / TODAY_DATE 等运行环境。
2. 准备 data/agent_data/<signature>/position/position.jsonl，填入初始持仓（默认只有现金）。
3. 如果 data/merged.jsonl 不存在，注入简化的行情 JSONL，供 get_open_prices 使用。
4. 调用 agent_tools.tool_trade 中的 buy / sell 函数，并打印结果，便于逐步调试。
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict

# ---------------------------------------------------------------------------
# 基础配置，可按需修改
# ---------------------------------------------------------------------------
SIGNATURE = "demo-agent"
TODAY_DATE = "2025-11-11"
INITIAL_CASH = 500_000.0
SYMBOLS: Dict[str, float] = {
    "AAPL": 150.0,
    "MSFT": 320.0,
}


def ensure_runtime_env(project_root: Path) -> Path:
    """创建 runtime env json，并告知 general_tools 读取该文件。"""
    runtime_path = project_root / "tmp" / "manual_trade_env.json"
    runtime_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "SIGNATURE": SIGNATURE,
        "TODAY_DATE": TODAY_DATE,
        "IF_TRADE": False,
    }
    runtime_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    os.environ["RUNTIME_ENV_PATH"] = str(runtime_path)
    # 兜底：部分逻辑可能直接读环境变量
    os.environ.setdefault("SIGNATURE", SIGNATURE)
    os.environ.setdefault("TODAY_DATE", TODAY_DATE)
    return runtime_path


def ensure_position_file(project_root: Path) -> Path:
    """如果 position.jsonl 不存在，则写入初始记录。"""
    position_file = (
        project_root
        / "data"
        / "agent_data"
        / SIGNATURE
        / "position"
        / "position.jsonl"
    )
    if position_file.exists():
        return position_file

    position_file.parent.mkdir(parents=True, exist_ok=True)
    record_date = datetime.strptime(TODAY_DATE, "%Y-%m-%d")
    record = {
        "date": (record_date - timedelta(days=1)).strftime("%Y-%m-%d"),
        "id": 0,
        "this_action": {"action": "bootstrap", "trades": {}},
        "positions": {"CASH": INITIAL_CASH},
    }
    position_file.write_text(json.dumps(record, ensure_ascii=False) + "\n", encoding="utf-8")
    return position_file


def ensure_merged_prices(project_root: Path) -> Path:
    """为 get_open_prices 准备基础行情，如果文件已存在则不覆盖。"""
    merged_file = project_root / "data" / "merged.jsonl"
    if merged_file.exists():
        return merged_file

    merged_file.parent.mkdir(parents=True, exist_ok=True)
    lines = []
    for symbol, price in SYMBOLS.items():
        doc = {
            "Meta Data": {"2. Symbol": symbol},
            "Time Series (Daily)": {
                TODAY_DATE: {
                    "1. buy price": f"{price:.2f}",
                }
            },
        }
        lines.append(json.dumps(doc, ensure_ascii=False))
    merged_file.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return merged_file


def main() -> None:
    project_root = Path(__file__).resolve().parents[1]
    runtime_path = ensure_runtime_env(project_root)
    position_file = ensure_position_file(project_root)
    merged_file = ensure_merged_prices(project_root)

    print(f"Runtime env: {runtime_path}")
    print(f"Position file: {position_file}")
    print(f"Merged prices: {merged_file}")

    # 为避免导入时读取到未就绪的 env，需要在准备完成后再导入工具函数
    from agent_tools.tool_trade import buy, sell

    print("\n=== 调用 buy({'AAPL': 10, 'MSFT': 5}) ===")
    buy_result = buy({"AAPL": 10, "MSFT": 5})
    print(json.dumps(buy_result, indent=2, ensure_ascii=False))

    print("\n=== 调用 sell({'AAPL': 3}) ===")
    sell_result = sell({"AAPL": 3})
    print(json.dumps(sell_result, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
