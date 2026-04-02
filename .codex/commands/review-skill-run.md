---
name: review-skill-run
description: Review one local skill run by checking outputs, logs, decision artifacts, and post-trade files for completeness and consistency. Invoke with /review-skill-run YYYY-MM-DD [--base-dir data] [--signature xxx] [--post-trade].
---

# Review Skill Run

检查某一天的本地 `skill-only` 运行是否完整、是否产物齐全、是否存在明显的契约错误。

这个 command 面向当前项目，不面向通用仓库。

## Usage

```text
/review-skill-run YYYY-MM-DD [--base-dir data] [--signature SIGNATURE] [--post-trade]
```

## Arguments

- `YYYY-MM-DD`
  - 必填，目标运行日期
- `--base-dir`
  - 可选，默认 `data`
  - 指向 `skill_runs/` 所在根目录
- `--signature`
  - 可选
  - 当需要检查 `data/agent_data/{signature}` 侧的交易汇总文件时使用
- `--post-trade`
  - 可选
  - 表示本次除了检查 `01-05`，还要检查 `06-08` 与交易汇总落地

## Goal

输出一份结构化审查结论，回答以下问题：

1. `01-04` 文件是否齐全，内容是否明显缺失或格式错误
2. 如果存在 `05_decision.json`，其关键字段是否完整、能否解析
3. 如果要求检查 post-trade，`06-08` 是否齐全，交易汇总文件是否同步更新
4. 是否存在明显 warning / failed 痕迹

## Workflow

### Step 1：确定路径

根据参数先解析：

- `run_dir = <base-dir>/skill_runs/YYYY-MM-DD`
- `global_context = <run_dir>/01_global_context.md`
- `snapshot = <run_dir>/02_basic_snapshot_payload.json`
- `agent_input = <run_dir>/03_agent_input.md`
- `stock_research_dir = <run_dir>/04_stock_research/`
- `decision = <run_dir>/05_decision.json`
- `execution_log = <run_dir>/06_execution_log.json`
- `daily_summary = <run_dir>/07_daily_summary.json`
- `history_merge = <run_dir>/08_history_merge.json`

如果 `run_dir` 不存在，直接停止并报告。

### Step 2：检查 `01-04`

#### `01_global_context.md`

- 文件存在
- 非空
- 内容长度不是异常短

#### `02_basic_snapshot_payload.json`

- 文件存在
- JSON 可解析
- 顶层结构是对象或数组
- 至少包含一个股票快照条目

#### `03_agent_input.md`

- 文件存在
- 非空
- 明显包含对 `01`、`02`、`04` 的引用说明或使用说明

#### `04_stock_research/`

- 目录存在
- 至少有一个 `.md` 文件
- 随机抽查一个文件：
  - 非空
  - 结构上应包含研究文本，而不是空模板

### Step 3：检查 `05_decision.json`

如果文件存在，检查：

- JSON 可解析
- 顶层字段是否包含：
  - `summary_date`
  - `stock_decisions`
  - `system_risk_notes`
  - `system_focus_items`
- `stock_decisions` 是否为数组
- 每个操作项是否至少包含：
  - `symbol`
  - `stock_name`
  - `action_type`
  - `recommended_action`

如果不存在，不把它直接判为失败，只说明“本次运行可能还停留在 agent 决策前”。

### Step 4：检查 post-trade

仅当满足以下任一条件时执行：

- 用户显式传入 `--post-trade`
- `06_execution_log.json` 已存在

检查：

- `06_execution_log.json` 是否存在且可解析
- `07_daily_summary.json` 是否存在且可解析
- `08_history_merge.json` 是否存在且可解析

如果提供了 `--signature`，继续检查：

- `data/agent_data/{signature}/stock_decisions.json`
- `data/agent_data/{signature}/portfolio_daily_summary.json`
- `data/agent_data/{signature}/decision_summary.json`

如果这些文件缺失，要指出是“交易汇总未落地”还是“缺少 signature 无法继续判断”。

### Step 5：检查日志与警告

优先寻找：

- `logs/main_scripts/ManageDailyData/latest_status.json`
- `logs/main_scripts/ManageDailyData/`
- 与本次步骤相关的 `logs/` 文件

检查是否有明显：

- `failed`
- `warning`
- traceback
- JSON 校验失败
- 文件缺失

如果无法唯一定位到本次日期对应日志，应明确说明“日志检查不充分”。

## Output Format

最终输出必须包含以下五部分。

```markdown
# Skill Run Review

## Target
- Date: YYYY-MM-DD
- Base Dir: ...
- Run Dir: ...
- Post Trade Checked: yes/no

## Status
- Overall: PASS | WARNING | FAIL
- Decision: ...
- Post Trade: ...

## Findings
1. [Severity] 问题标题
   - Evidence: ...
   - Impact: ...
   - Suggested Next Step: ...

## Checked Files
- path1: ok / missing / invalid
- path2: ok / missing / invalid

## Summary
- 一句话总结这次 run 是否可用于继续分析或交易
```

## Severity Rules

- `FAIL`
  - `01-04` 关键文件缺失
  - `05_decision.json` 存在但不可解析
  - `06-08` 存在但结构损坏
- `WARNING`
  - 存在 warning 步骤但主产物还在
  - 某些辅助文件缺失，但不阻塞主要消费
  - 无法充分定位日志
- `PASS`
  - 主链路产物齐全
  - JSON 可解析
  - 没有明显失败痕迹

## Important Notes

- 这个 command 的目标是“审查现有运行结果”，不是默认去重跑整个流程。
- 除非用户明确要求，不要在审查过程中自动执行 `manage_daily_data`、`run_daily_pipeline` 或 `run_post_trade`。
- 如果发现问题，优先给证据和定位建议，不要立刻大改代码。
