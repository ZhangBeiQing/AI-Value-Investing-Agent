# 回测逐日调度与角色模板

本文件只定义“当天怎样调度”。文件所有权、推进门和隔离边界以 [产物与所有权契约](artifact-contract.md) 为准；历史联网边界以 [联网防穿越规则](guarded-web-policy.md) 为准。

## 1. 日期和执行日

交易日由 `resolve_experiment_trading_dates()` 解析，禁止手工枚举。前 `N-1` 个日期是决策日，最后一日只用于期末收盘估值。

```text
decision_date = D 日收盘后生成研究和 05
execution_date = 实验交易日序列中 D 的紧邻下一日
```

每一天先执行：

```bash
python scripts/manage_fixed_tracked_backtest.py prepare-day \
  --experiment-id {experiment_id} \
  --date {decision_date} \
  --build-missing-inputs \
  --max-workers 6
```

读 `{root}/00_prepare_status.json`。`needs_financial_research` 时严格按主 Skill 的财报门禁处理；任何其他非 `ready` 状态都停止当天。

```text
{root} = data/backtests/fixed_tracked/{experiment_id}/skill_runs/{decision_date}/fixed_tracked
{skill_runs_root} = data/backtests/fixed_tracked/{experiment_id}/skill_runs
```

## 2. P0 与空 P0

主 Agent只读取 `{root}/00`、`01`、`02`、`03`、`{experiment_root}/agent_data/backtest-{experiment_id}/latest_decision_snapshot.json`（存在时）与可选市场级文件，按 `{root}/03_agent_input.md` 选择 P0；不得读 04。快照仅用于上次结论、待验证事实和遗留风险的历史上下文，不能替代当天资料或形成直接交易指令。回测自动接受 P0。

P0 为空时：

```bash
python scripts/manage_fixed_tracked_backtest.py no-trade-day \
  --experiment-id {experiment_id} \
  --date {decision_date} \
  --reason "P0 为空，组合层未发现需要深度辩论的交易机会"
```

然后跳到第 6 节执行 D+1 无交易结算。

## 3. 辩论批次和共同规则

每只 P0 固定使用 6 个逻辑角色：Bull、Bear、Juror 01、Juror 02、Juror 03、finalizer；Bull/Bear rebuttal 必须复用 opening 的原会话。

同一阶段最多 10 个 subagent。超过时只按并发上限拆批；上一批目标文件全部落盘并通过 JSON/schema 校验后才启动下一批。不同股票可并行，同一股票严格按：

```text
prepare → opening → rebuttal → jury → aggregate → finalizer → validate
```

所有 JSON 写入者先完整读取 `auto-trading-fixed-tracked/references/json-writing-guide.md`，写后按其中要求校验。

主 Agent prompt 只能传：角色身份、日期、必读文件清单、唯一输出路径。不得附加任何研究结论、数字、交易数量、规则解释或主观观点。

## 4. 准备辩论目录

对每只 P0：

```bash
python scripts/manage_debate.py prepare \
  --date {decision_date} \
  --book-type fixed_tracked \
  --symbol {symbol} \
  --base-dir {skill_runs_root}
```

`prepare` 只创建目录，不能把它当作研究或投票完成。

## 5. 角色模板

### 5.1 Bull / Bear Opening

Bull：

```text
你担任 {symbol} {stock_name} 在 {decision_date} 的 Bull advocate。

直接完整读取：
1. {root}/00_backtest_context.md
2. .codex/skills/auto-trading-fixed-tracked/references/debate-bull.md
3. .codex/skills/auto-trading-fixed-tracked/references/json-writing-guide.md
4. {root}/03_stock_analysis_input.md
5. {root}/01_global_context.md
6. 与本股直接相关的热点和板块文件（存在时）
7. 当前股票唯一的 {root}/04_stock_research/*_research.md

只允许写入：
{root}/debate/{stock_name}_{symbol}/advocates/bull/opening.json

完成后只回传文件路径和完成状态。
```

Bear 使用同一模板，只替换角色为 `Bear advocate`、reference 为 `debate-bear.md`、输出为 `advocates/bear/opening.json`。

等待两份 opening 合法后才进入 rebuttal。

### 5.2 Rebuttal：必须复用原会话

保存每只股票 Bull 和 Bear opening 的 task ID。Bull follow-up：

```text
继续担任 {symbol} {stock_name} 在 {decision_date} 的原 Bull advocate。

直接完整读取：
1. {root}/00_backtest_context.md
2. .codex/skills/auto-trading-fixed-tracked/references/debate-rebuttal.md
3. .codex/skills/auto-trading-fixed-tracked/references/json-writing-guide.md
4. {root}/debate/{stock_name}_{symbol}/advocates/bull/opening.json
5. {root}/debate/{stock_name}_{symbol}/advocates/bear/opening.json

只允许写入：
{root}/debate/{stock_name}_{symbol}/advocates/bull/rebuttal.json

完成后只回传文件路径和完成状态。
```

Bear 对称执行，输出自己的 `bear/rebuttal.json`。原会话空返回或未落盘时，先用原 task ID 续写；仅平台无法恢复原会话时允许新建，并记录该例外。

### 5.3 三名独立 Juror

`juror_01`、`juror_02`、`juror_03` 必须是互不共享上下文的新会话，不得读取彼此 ballot：

```text
你担任 {symbol} {stock_name} 在 {decision_date} 的独立 {juror_id}。

直接完整读取：
1. {root}/00_backtest_context.md
2. .codex/skills/auto-trading-fixed-tracked/references/debate-juror.md
3. .codex/skills/auto-trading-fixed-tracked/references/json-writing-guide.md
4. {root}/03_stock_analysis_input.md
5. {root}/01_global_context.md
6. 当前股票唯一的 {root}/04_stock_research/*_research.md
7. {root}/debate/{stock_name}_{symbol}/advocates/bull/opening.json
8. {root}/debate/{stock_name}_{symbol}/advocates/bull/rebuttal.json
9. {root}/debate/{stock_name}_{symbol}/advocates/bear/opening.json
10. {root}/debate/{stock_name}_{symbol}/advocates/bear/rebuttal.json

只允许写入：
{root}/debate/{stock_name}_{symbol}/jury/{juror_id}/ballot.json

完成后只回传文件路径和完成状态。
```

### 5.4 本地聚合

三份 ballot 均合法后，读取隔离账本中 D 日实际持股数，执行：

```bash
python scripts/manage_debate.py aggregate \
  --date {decision_date} \
  --book-type fixed_tracked \
  --symbol {symbol} \
  --position-shares {isolated_position_shares} \
  --base-dir {skill_runs_root}
```

Agent 无权写 `vote_summary.json`。若原 ballot 被修订，必须重新 aggregate，不能手工改汇总。

### 5.5 独立 Finalizer

Finalizer 必须不是任何 Juror：

```text
你担任 {symbol} {stock_name} 在 {decision_date} 的唯一 finalizer。

直接完整读取：
1. {root}/00_backtest_context.md
2. .codex/skills/auto-trading-fixed-tracked/references/debate-finalizer.md
3. .codex/skills/auto-trading-fixed-tracked/references/json-writing-guide.md
4. configs/prompt_flow/fixed_tracked/stock_decision.schema.json
5. configs/prompt_flow/fixed_tracked/stock_decision.example.json
6. {root}/03_stock_analysis_input.md
7. {root}/01_global_context.md
8. 当前股票唯一的 {root}/04_stock_research/*_research.md
9. {root}/debate/{stock_name}_{symbol}/advocates/bull/opening.json
10. {root}/debate/{stock_name}_{symbol}/advocates/bull/rebuttal.json
11. {root}/debate/{stock_name}_{symbol}/advocates/bear/opening.json
12. {root}/debate/{stock_name}_{symbol}/advocates/bear/rebuttal.json
13. {root}/debate/{stock_name}_{symbol}/jury/juror_01/ballot.json
14. {root}/debate/{stock_name}_{symbol}/jury/juror_02/ballot.json
15. {root}/debate/{stock_name}_{symbol}/jury/juror_03/ballot.json
16. {root}/debate/{stock_name}_{symbol}/final/vote_summary.json

只允许写入：
{root}/debate/{stock_name}_{symbol}/final/stock_verdict.json

完成后只回传文件路径和完成状态。
```

Finalizer 必须服从 `vote_summary.resolved_action`，不是第四名 Juror。

每只 P0 finalizer 完成后：

```bash
python scripts/manage_debate.py validate \
  --date {decision_date} \
  --book-type fixed_tracked \
  --symbol {symbol} \
  --base-dir {skill_runs_root} \
  --require-verdict
```

## 6. 合并、校验与 D+1 执行

所有 P0 都通过 `--require-verdict` 后：

```bash
python scripts/merge_subagent_decisions.py \
  --date {decision_date} \
  --book-type fixed_tracked \
  --source debate \
  --base-dir {skill_runs_root}
```

补齐并校验 `{root}/05_decision.json` 的 `summary_date`、`system_risk_notes`、`system_focus_items`。这些仅是组合层字段；主 Agent不得改写任何单股 verdict 的研究事实、投票或动作。

然后执行隔离 D+1 模拟：

```bash
python scripts/manage_fixed_tracked_backtest.py execute-day \
  --experiment-id {experiment_id} \
  --date {decision_date} \
  --execution-date {execution_date}
```

该入口会校验 `execution_date` 必须等于下一交易日，并写入 06-08。不得调用没有 `--backtest-root` 的真实后处理路径。

## 7. 长耗时命令

`prepare-day`、公告同步、财报准备、PDF 转换和批量重建可能耗时很长。保持同一进程/会话持续轮询，不因短暂无输出重启命令；累计最多等待一小时。至少每 60 秒检查一次状态并向用户报告。命令退出后必须读状态文件或输出文件确认结果，不能只看最后一行日志。
