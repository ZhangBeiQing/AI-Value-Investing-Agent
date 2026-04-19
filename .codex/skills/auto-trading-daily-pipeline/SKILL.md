---
name: auto-trading-daily-pipeline
description: >
  三账本交易公共模板与调度说明。用于把 fixed_tracked、short_book、long_book
  的每日交易分析拆成独立账本执行，避免把三个股票池混进同一个上下文。用户说“开始今天股票交易”、
  “运行三本交易分析”或要查看三账本交易流程时使用。
---
description: >
  三账本交易公共模板与调度说明。用于把 fixed_tracked、short_book、long_book
  的每日交易分析拆成独立账本执行，避免把三个股票池混进同一个上下文。用户说“开始今天股票交易”、
  “运行三本交易分析”或要查看三账本交易流程时使用。
---

# Auto Trading Multi-Book Template

## 1. 什么时候用

- 用户说“开始今天股票交易”，但没有指定账本
- 用户要一次看完整的三账本交易流程
- 用户要确认 fixed_tracked / short_book / long_book 三个交易 skill 的分工

## 2. 核心原则

- 不要在一个上下文里同时分析三个账本
- 每个账本必须单独读取自己的 `01-04` 产物、单独生成自己的 `05_decision.json`
- 每个账本必须使用自己的 `signature`，避免仓位和历史锚点串账
- 如果用户要求三本都跑，默认按顺序串行执行：
  1. `fixed_tracked`
  2. `short_book`
  3. `long_book`

## 3. 账本映射

- `fixed_tracked`
  - 目录：`data/skill_runs/YYYY-MM-DD/fixed_tracked/`
  - `signature`：`book-fixed_tracked`
  - 对应 skill：`auto-trading-fixed-tracked`
- `short_book`
  - 目录：`data/skill_runs/YYYY-MM-DD/short_book/`
  - `signature`：`book-short_book`
  - 对应 skill：`auto-trading-short-book`
- `long_book`
  - 目录：`data/skill_runs/YYYY-MM-DD/long_book/`
  - `signature`：`book-long_book`
  - 对应 skill：`auto-trading-long-book`

## 4. 公共流程

单账本交易分析的通用步骤写在：

- `references/book-trading-workflow.md`

进入具体账本执行时，不要重复发明流程，直接继承那份公共模板。

## 5. 后处理命令

当某个账本的 `05_decision.json` 已经人工确认可执行后，必须对该账本单独执行：

```bash
python scripts/run_post_trade.py --date YYYY-MM-DD --book-type <book_type> --signature <book_signature>
```

例如：

```bash
python scripts/run_post_trade.py --date 2026-04-19 --book-type short_book --signature book-short_book
```

如需直接指定目录，也可以改用：

```bash
python scripts/run_post_trade.py --date YYYY-MM-DD --output-dir data/skill_runs/YYYY-MM-DD/<book_type> --signature <book_signature>
```

## 6. 执行要求

- 用户只指定某一个账本时，只运行对应账本 skill
- 用户要求“三本都跑”时，按账本顺序分别执行，不要把三本股票合并成一个决策文件
- 若用户没有说明是只分析还是要执行后处理，先完成单账本分析与 `05_decision.json`，再等待人工确认
