"""Deprecated main entry.

The repository has migrated to the local skill-only workflow.
This file is intentionally kept as a short migration notice so old habits fail
fast and point to the correct commands.
"""

from __future__ import annotations

import sys


MESSAGE = """
`main.py` 已下线，仓库当前使用本地 `skill-only` 工作流。

请改用以下命令：

1. 刷新每日数据
   python scripts/manage_daily_data.py

2. 生成每日 skill 输入
   python scripts/run_daily_pipeline.py --date YYYY-MM-DD

3. 让本地 Agent 读取 `data/skill_runs/{date}/` 并生成 `05_decision.json`

4. 执行交易后处理
   python scripts/run_post_trade.py --date YYYY-MM-DD

如需查看重构与清理计划：
- docs/skill_only_refactor_plan.md
- docs/phase4_entry_cleanup_checklist.md
""".strip()


def main() -> int:
    print(MESSAGE)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
