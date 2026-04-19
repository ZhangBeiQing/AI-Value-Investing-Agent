---
name: auto-trading-fixed-tracked
description: >
  fixed_tracked 账本的每日交易 skill。用户说“开始 fixed_tracked 交易”、
  “分析固定股票池交易”或要为 fixed_tracked 目录生成并确认 `05_decision.json` 时使用。
---

# Auto Trading Fixed Tracked

## 1. 账本设置

- `book_type`：`fixed_tracked`
- 目录：`data/skill_runs/YYYY-MM-DD/fixed_tracked/`
- `signature`：`book-fixed_tracked`

## 2. 执行规则

- 只分析 `fixed_tracked` 账本，不读取 `short_book` 或 `long_book`
- 单账本通用流程见：
  - `../auto-trading-daily-pipeline/references/book-trading-workflow.md`

## 3. 后处理

当 `05_decision.json` 已人工确认后，执行：

```bash
python scripts/run_post_trade.py --date YYYY-MM-DD --book-type fixed_tracked --signature book-fixed_tracked
```
