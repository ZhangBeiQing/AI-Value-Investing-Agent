"""Materialize the lightweight ``06_hot_news_digest.json`` from the full state."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from services.selection_system.hot_news_digest import write_hot_news_digest  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(
        description="从 06_hot_news_state.json 生成精简的 06_hot_news_digest.json"
    )
    parser.add_argument("--date", required=True, help="要分析的交易日 YYYY-MM-DD")
    parser.add_argument("--base-dir", default="data", help="数据根目录，默认 data")
    args = parser.parse_args()

    target = write_hot_news_digest(args.date, base_dir=args.base_dir)
    if target is None:
        print(
            f"未找到 {args.base_dir}/selection_runs/{args.date}/06_hot_news_state.json，"
            "请先运行 /gradual-hot-news-summary"
        )
        return 1
    print(f"hot news digest 已写入: {target}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
