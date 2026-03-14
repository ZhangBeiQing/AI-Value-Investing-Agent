---
name: debug-skill-run
description: Guide for debugging failures in manage_daily_data, run_daily_pipeline, or run_post_trade using evidence-first and minimal reproduction.
---

# Debug Skill Run

排查 `skill-only` 主链路失败时，先收集 evidence，再做最小复现。

## When to Use

- `python scripts/manage_daily_data.py` 失败
- `python scripts/run_daily_pipeline.py` 某一步失败
- `python scripts/run_post_trade.py` 交易或汇总失败
- 用户只描述“今天跑不通了”，但还没有明确报错定位

## Step 1：先定位失败步骤

优先看：

- `logs/` 组件日志
- `logs/main_scripts/*/latest_status.json`
- `data/skill_runs/{date}/run_manifest.json`
- `06_execution_log.json`

先回答：

- 失败发生在哪一步
- 是输入缺失、代码异常、外部数据异常还是 JSON 契约不一致

## Step 2：收集最小 evidence

至少保留以下一种：

- 失败堆栈
- 失败步骤前后的输入输出文件
- `run_manifest.json` 中的 `failed` / `warning`
- 某个 symbol 的局部数据目录

不要在没有证据时直接大改代码。

## Step 3：缩成最小复现

优先缩小问题：

- 用单股票池
- 用临时 `--base-dir`
- 只重跑失败步骤对应脚本

推荐：

```bash
python scripts/run_daily_pipeline.py --date YYYY-MM-DD --base-dir data/tmp_debug
python scripts/run_post_trade.py --date YYYY-MM-DD
```

## Step 4：修复时保持边界

- 数据问题优先修 `shared_data_access/`
- 编排问题优先修 `services/pipeline/` 或 `services/trading/`
- 日志和 evidence 不足时，先补日志再修逻辑

不要为了绕过问题，把本该本地预生成的数据重新推给在线 agent 临时联网解决。

## Step 5：回归验证

修复后至少复跑导致问题的那条最小链路。

如果影响范围更大，再补主链路验证。

## Common Mistakes

- 一上来全量跑，结果日志被淹没
- 不看 `run_manifest.json` 就猜问题
- 看到异常就直接 try/except 吞掉
- 没有最小复现就开始大重构

