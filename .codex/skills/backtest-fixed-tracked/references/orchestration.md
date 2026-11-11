# 回测每周研究、逐日结算与角色模板

本文件只定义“当天怎样调度”。文件所有权、推进门和隔离边界以 [产物与所有权契约](artifact-contract.md) 为准；历史联网边界以 [联网防穿越规则](guarded-web-policy.md) 为准。

## 1. 日期和执行日

交易日由 `resolve_experiment_trading_dates()` 解析，禁止手工枚举。前 `N-1` 个日期是决策日，最后一日只用于期末收盘估值。

```text
decision_date = D 日收盘后生成研究和 05
execution_date = 实验交易日序列中 D 的紧邻下一日
```

研究日才执行以下准备；非研究日不运行此命令：

```bash
python scripts/manage_fixed_tracked_backtest.py prepare-day \
  --experiment-id {experiment_id} \
  --date {decision_date} \
  --build-missing-inputs \
  --max-workers 6
```

读 `{root}/00_prepare_status.json`。`needs_shared_data` 时先通过现有准备入口补指定缓存缺口，不自动刷新全池；`needs_financial_research` 时按主 Skill 处理完整深研与日常摘要补建两类任务；`needs_macro_research` 时派发独立宏观 subagent，主 Agent 不代写。历史准备的角色分工、并行顺序和验收复用主 Skill 6.2，宏观及财报研究未就绪不得生成 01-04 或进入 P0。其他非 `ready` 状态停止当天并处理具体缺口。

```text
{root} = data/backtests/fixed_tracked/{experiment_id}/skill_runs/{decision_date}/fixed_tracked
{skill_runs_root} = data/backtests/fixed_tracked/{experiment_id}/skill_runs
```

### 周期间隔与恢复

研究日按主 Skill 的七个自然日节奏选择，并在实验根目录 `analysis_schedule.json` 记录。用户跳过当天研究也占用本轮窗口。日期从 `resolve_experiment_trading_dates()` 获取，末日不生成决策。

从 `status.progress.next_date` 起，到下一研究日之前的每个未完成交易日，依次执行：

```bash
python scripts/manage_fixed_tracked_backtest.py no-trade-day \
  --experiment-id {experiment_id} --date {interval_date} \
  --reason "每周一次研究：本日为间隔持仓估值日，无新增交易"
python scripts/manage_fixed_tracked_backtest.py execute-day \
  --experiment-id {experiment_id} --date {interval_date}
```

`execute-day` 省略执行日期时由程序解析紧邻下一交易日。已有成功06且07/08齐备的日期直接跳过，已有合法空05则只执行；若已有非空决策、失败06或账本与06不一致，先核对现场，不能覆盖或静默吞掉。每个日期结算成功才推进，不能把中间日期留空后直接 prepare 下一周。间隔内不生成宏观或01-04，不调用研究角色。下一研究日的财报与宏观门禁照常完整执行。

## 2. P0 与空 P0

选择 P0 前按实验当天 02 的股票范围刷新隔离快照：

```python
import json
from pathlib import Path
from services.trading.trade_summary import use_agent_data_root, refresh_latest_decision_snapshot
symbols = json.loads(Path("{root}/02_basic_snapshot_payload.json").read_text())["stocks"]
with use_agent_data_root("{experiment_root}/agent_data"):
    refresh_latest_decision_snapshot("backtest-{experiment_id}", symbols=symbols)
```

主 Agent只读取 `{root}/00`、`01`、`02`、`03`、`p0_event_report.md`、`price_delta_report.md`、`{experiment_root}/agent_data/backtest-{experiment_id}/latest_decision_snapshot.json`（存在时）与可选市场级文件，按 `{root}/03_agent_input.md` 选择 P0；不得读 04。快照中的最近分析日期、价格印象和完整待核验事项用于结合当天变化选择 P0。P0 须经用户确认。

P0 为空时：

```bash
python scripts/manage_fixed_tracked_backtest.py no-trade-day \
  --experiment-id {experiment_id} \
  --date {decision_date} \
  --reason "P0 为空，组合层未发现需要深度辩论的交易机会"
```

然后跳到第 6 节执行 D+1 无交易结算。

## 3. 当前研究与裁决流程

复用 `auto-trading-fixed-tracked/SKILL.md` 的当前单裁决流程：两份独立 opening → 一名独立 Adjudicator → validate。
仅决定性争议时复用原 Advocate 定向回应。
全部角色使用通用 subagent，继承当前模型；并发上限与批次遵循日常交易 skill 和平台实际额度。
派单只传角色、日期、股票、必读文件和授权输出路径，不转述规则或注入个股结论。

## 4. 回测路径与持仓

所有 manage_debate 命令必须传 `--base-dir {skill_runs_root}`，不能使用真实默认目录。
从实验隔离账本核实 D 日实际持股数量，不能取真实持仓或目标仓位。

```bash
python scripts/manage_debate.py prepare --date {decision_date} --symbol {symbol} --base-dir {skill_runs_root} --position-shares {隔离账本实际持股数量}
```

记录当日持仓上下文后，才派发 Bull/Bear。P0 必须先经用户确认，不能自动接受。

## 5. 角色派发

使用日常交易 skill 中的 Bull/Bear 与 Adjudicator 模板，把所有输入/输出路径替换为 `{root}` 和 `{debate_root}`。
每个角色必须额外先读 `{root}/00_backtest_context.md` 和历史联网边界，不能使用 D 之后的资料。
同股双方各用独立会话，裁决者是第三个独立会话，必要的定向质询复用原会话。
裁决者只写最终 verdict 与可选质询/解决记录，不写 05；程序文件和账本仍由程序写。

```bash
python scripts/manage_debate.py validate --date {decision_date} --symbol {symbol} --base-dir {skill_runs_root} --require-verdict
```

## 6. 组合复核、合并、校验与 D+1 执行

合并前按日常交易 skill 阶段 C 完成现金、BUY 总额、币种/价格日期、SELL 持仓上限、交易单位、集中风险、系统风险、跨 verdict 冲突与关键假设复核。现金来自实验隔离账本，不预支尚未成交的卖出收入。写 `{root}/00_portfolio_review.md` 记录依据和处理结果；需要改数量由原裁决者改 verdict 并重新 validate，需要补关键论证则走原会话质询。存在未处理问题时不得合并或执行；通过后自动推进，无需第二次人工确认。

所有 P0 都通过 `--require-verdict` 后：

```bash
python scripts/merge_subagent_decisions.py \
  --date {decision_date} \
  --book-type fixed_tracked \
  --base-dir {skill_runs_root}
```

补齐并校验 `{root}/05_decision.json` 的 `summary_date`、`system_risk_notes`、`system_focus_items`。这些仅是组合层字段；主 Agent不得改写任何单股 verdict 的研究事实或动作。

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
