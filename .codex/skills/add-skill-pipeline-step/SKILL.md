---
name: add-skill-pipeline-step
description: Guide for adding or refactoring a step in the local skill-only pipeline. Use when changing data_refresh, daily_pipeline, prompt flow, or 01-08 artifacts.
---

# Add Skill Pipeline Step

为当前项目新增、拆分或重构一个 `skill-only` 流水线步骤。

## When to Use

以下情况适用：

- 用户要求新增 `01-08` 中某一步的产物
- 需要拆分 `services/pipeline/steps/` 中的步骤
- 需要调整 `manage_daily_data`、`run_daily_pipeline`、`run_post_trade` 的职责
- 需要修改 `skill_flow.json` 并同步到本地产物结构

## Step 1：先定义契约

在改代码前先明确：

- 这个步骤属于 `01-04` 还是 `05-08`
- 输入文件是什么
- 输出文件是什么
- 是否要求兼容旧版命名和目录结构
- 失败时是 `warning` 继续还是 `failed` 中止

至少写清楚：

```text
Step Name:
Input:
Output:
Side Effects:
Validation:
Compatibility Notes:
```

## Step 2：决定代码落点

- CLI 改动放 `scripts/`
- 编排放 `services/pipeline/` 或 `services/trading/`
- 单步实现优先放 `services/pipeline/steps/`
- Prompt/flow 配置改动放 `configs/prompt_flow/`

不要把复杂逻辑重新写回脚本入口。

## Step 3：实现最小变更

优先保持原有文件名和消费者不变。

- 扩字段优先于改字段名
- 扩步骤优先于打乱已有顺序
- 修改 `run_manifest.json` 时保持现有风格

如果是交易后处理：

- 优先调用本地 Python service
- 不新增 MCP 依赖

## Step 4：同步文档

至少同步以下其中之一：

- `AGENTS.md`
- `docs/PROJECT_SYSTEM_SUMMARY.md`
- 相关 `docs/` 设计文档

如果改了 `skill_flow.json`，必须说明上游输入和下游 JSON 契约是否变了。

## Step 5：验证

优先用临时目录验证：

```bash
python scripts/run_daily_pipeline.py --date YYYY-MM-DD --base-dir data/tmp_<name>
python scripts/run_post_trade.py --date YYYY-MM-DD
```

至少检查：

- `run_manifest.json`
- 目标文件是否生成
- 关键 JSON 是否可解析

## Common Mistakes

- 只改 prompt，不改产物契约说明
- 把复杂逻辑写进 `scripts/`
- 改文件名导致本地 Agent 读不到
- 忘记同步 `run_manifest.json`
- 验证时污染正式 `data/skill_runs/`
