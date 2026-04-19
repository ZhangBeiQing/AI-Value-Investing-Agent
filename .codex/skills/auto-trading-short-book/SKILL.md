---
name: auto-trading-short-book
description: >
  short_book 账本的每日交易 skill。用户说“开始 short_book 交易”、
  “分析短期股票池交易”或要为 short_book 目录生成并确认 `05_decision.json` 时使用。
---

# Auto Trading Short Book

## 1. 账本设置

- `book_type`：`short_book`
- 目录：`data/skill_runs/YYYY-MM-DD/short_book/`
- `signature`：`book-short_book`

## 2. 执行规则

- 只分析 `short_book` 账本，不读取 `fixed_tracked` 或 `long_book`
- 更重近端催化、公告、异动、兑现节奏，但仍必须遵守当前账本 `03_agent_input.md`
- 单账本通用流程见：
  - `../auto-trading-daily-pipeline/references/book-trading-workflow.md`

## 3. 后处理

当 `05_decision.json` 已人工确认后，执行：

```bash
python scripts/run_post_trade.py --date YYYY-MM-DD --book-type short_book --signature book-short_book
```
