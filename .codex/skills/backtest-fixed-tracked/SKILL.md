---
name: backtest-fixed-tracked
description: 对指定历史区间运行 fixed_tracked 的隔离多 Agent 回测。按交易日串行生成历史输入、自动完成 P0选股 与 P0股票 辩论、合并 05，并以 D+1 开盘价模拟成交；不触碰真实账本或真实交易。
---

# fixed_tracked 历史回测

## 1. 用途与角色

当用户要求“回测固定股池”“测试某段日期的固定池收益”或“用某金额从某日回测到某日”时使用本 Skill。

主 Agent 是**历史实验调度器**，不是个股研究员。职责只有：

1. 创建或续跑隔离实验；
2. 按真实交易日从早到晚推进；
3. 准备当天历史输入并处理财报门禁；
4. 按 `auto-trading-fixed-tracked` 调度 P0、辩论、投票和 finalizer；
5. 自动合并 `05_decision.json`，以 D+1 开盘价模拟成交；
6. 校验、断点恢复与期末结果检查。

不得代替 Bull、Bear、Juror 或 finalizer 做个股结论；不得把主 Agent 的个股观点写进 subagent prompt。

## 2. 必读文件与唯一规则来源

开始前必须完整读取：

1. [产物与所有权契约](references/artifact-contract.md)
2. [逐日调度与角色模板](references/orchestration.md)
3. [联网防穿越规则](references/guarded-web-policy.md)
4. [结果检查](references/result-review.md)

个股辩论的角色规则、JSON 契约和投票机制只复用：

```text
.codex/skills/auto-trading-fixed-tracked/references/
configs/prompt_flow/fixed_tracked/stock_decision.schema.json
```

不要在本 Skill 或运行时 prompt 中复制、改写这些个股规则。回测只额外增加历史日期和隔离目录约束。

## 3. 绝对约束

- 激活环境：`source /home/zhangbeiqing/venv/ai_stock/bin/activate`。
- `decision_date` 是 D 日收盘后；可用信息截止 D 日 `23:59:59 +08:00`；成交只能发生在紧邻交易日 D+1 的开盘价。
- 交易日只能由实验程序解析。禁止按自然日、周末或工作日自行猜测；禁止把区间最后一个交易日当作决策日。
- 一天必须完成 `准备 → P0 → 辩论/05 → D+1 模拟成交 → 06/07/08`，才可进入下一天。跨日绝不并行。
- 所有运行产物、账本、决策和交易记忆都在 `data/backtests/fixed_tracked/{experiment_id}/`。禁止写真实 `data/agent_data`、正式 `data/skill_runs` 或真实交易账本。
- 共享写入只允许两类例外：按公告日、通过日期因果门禁的 `data/stock_info/*/financial_reports/` 深度基本面总结；以及由正常日期接口按 D 日重建的 `data/stock_info/*/{analysis,pe_pb_analysis}` 派生中间结果。两者都不得包含回测仓位、交易动作或 D 日之后的信息。
- 不得因为共享目录没有现成历史 Markdown 就让 04 输出空估值；必须走正常日期接口重建并在读取时按 D 日截断。
- `12_quant_prefilter_long.csv` 只扩展当日研究范围，不能直接形成交易动作。
- 回测没有日常交易 Skill 的两次人工暂停：P0、05 和模拟成交自动推进；但**绝不**调用真实模式的 `run_post_trade.py`。
- 联网只在实验 `network_mode=guarded_web` 下允许，并严格执行 [联网防穿越规则](references/guarded-web-policy.md)。

## 4. 目录和文件所有权

实验根目录：

```text
data/backtests/fixed_tracked/{experiment_id}/
├── experiment.json                 # 创建后不可改的实验身份与配置；extend 只追加结束日期历史
├── coverage.json                   # 覆盖率审计
├── skill_runs/{D}/fixed_tracked/   # D 日所有 00-08 与 debate 产物
├── agent_data/backtest-{id}/       # 隔离持仓、订单和交易记忆
├── checkpoints/                    # 程序进度
└── results/                        # finalize 后的净值与汇总
```

当天各文件由谁写、何时可推进，完全以 [产物与所有权契约](references/artifact-contract.md) 为准。弱模型遇到任何“该不该写/能不能继续”的问题，先查该表，不能自行推断。

## 5. 创建、检查与扩展实验

### 5.1 新实验

```bash
source /home/zhangbeiqing/venv/ai_stock/bin/activate
python scripts/manage_fixed_tracked_backtest.py prepare \
  --start-date {start_date} \
  --end-date {end_date} \
  --initial-cash {initial_cash} \
  --network-mode guarded_web
```

记录返回的 `experiment_id`，并读取：

```text
data/backtests/fixed_tracked/{experiment_id}/experiment.json
data/backtests/fixed_tracked/{experiment_id}/coverage.json
```

`experiment_id` 的起始日期与创建时刻不可变。需要更早起点时新建实验，不能修改既有实验起点。

默认重建历史 01-04；不得把未来的正式产物复制到历史日期。只有用户明确确认旧正式输入已完成日期因果和 Prompt 兼容审计时，才允许 `prepare-day --reuse-existing-inputs`。

### 5.2 恢复或向后扩展

中断、异常或需要确认当前状态时：

```bash
python scripts/manage_fixed_tracked_backtest.py status \
  --experiment-id {experiment_id}
```

续跑从 `status.progress.next_date` 开始；禁止重复执行已有成功 `06_execution_log.json` 的日期。

用户要求延长结束日期时：

```bash
python scripts/manage_fixed_tracked_backtest.py extend \
  --experiment-id {experiment_id} \
  --end-date {new_end_date}
```

不得新建目录、复制账本或修改 `experiment_id`。扩展后旧结束日自动转为普通决策日；旧 `results/summary.json` 过期，新增区间完成后必须重新 `finalize`。

## 6. 单日状态机

对实验交易日序列中的每一个非最后交易日，严格按下列状态推进。任何状态失败就停止该日；不能跳到后续状态，也不能跳到下一天。

```text
READY
  → PREPARED
  → FINANCIAL_GATE_PASSED
  → P0_READY
  → DEBATE_COMPLETE 或 NO_TRADE_05
  → DECISION_05_VALID
  → EXECUTED_D_PLUS_1
  → DAILY_MEMORY_WRITTEN
```

最后一个交易日只做收盘估值，不创建无法在区间内成交的 05 或订单。

### 6.1 PREPARED：准备当天输入

```bash
python scripts/manage_fixed_tracked_backtest.py prepare-day \
  --experiment-id {experiment_id} \
  --date {decision_date} \
  --build-missing-inputs \
  --max-workers 6
```

读取 `{root}/00_prepare_status.json`，其中 `{root}` 为：

```text
data/backtests/fixed_tracked/{experiment_id}/skill_runs/{decision_date}/fixed_tracked
```

只有 `status == ready` 才可进入 P0。还必须确认以下输入存在：

```text
00_backtest_context.md
01_global_context.md
02_basic_snapshot_payload.json
03_agent_input.md
03_stock_analysis_input.md
04_stock_research/
../run_manifest.json
```

### 6.2 财报门禁

若 `status == needs_financial_research`：

1. 读取 `00_prepare_status.json.financial_research`；只处理 `required_items`。
2. 按返回的 `preparation_command` 准备材料。
3. 使用 `financial-report-summary` Skill 完成其完整多角色闭环；所有角色额外完整读取本股 workdir 的 `00_backtest_context.md`。
4. 使用返回的 `registration_command_template` 注册总结，**必须保留** `--as-of-date {decision_date}`。
5. 运行返回的 `resume_prepare_day_command`，重新生成同一天 04。
6. 再读 `00_prepare_status.json`；只有变为 `ready` 才可继续。

不得把“有财报原文”当成“已有深度总结”；不得用 D 日之后的财报、研报或总结补齐。`unavailable_items` 只表示 D 日前确实无法识别原文，04 必须明确记录该缺口。

旧实验需要修复已生成的空 04 时，财报总结注册完成后重跑：

```bash
python scripts/manage_fixed_tracked_backtest.py prepare-day \
  --experiment-id {experiment_id} \
  --date {decision_date} \
  --build-missing-inputs \
  --force-rebuild-inputs \
  --max-workers 6
```

### 6.3 P0 与空 P0

主 Agent只读 `{root}/00`、`01`、`02`、`03`、`{experiment_root}/agent_data/backtest-{experiment_id}/latest_decision_snapshot.json`（存在时）和存在时的市场级文件；不得打开 `04_stock_research/*`。快照仅用于上次结论、待验证事实和遗留风险的历史上下文，不能替代当天资料或形成直接交易指令。按当天 `03_agent_input.md` 选择 P0。回测自动接受该 P0，不等待用户确认。

若 P0 为空，只生成显式空 05：

```bash
python scripts/manage_fixed_tracked_backtest.py no-trade-day \
  --experiment-id {experiment_id} \
  --date {decision_date} \
  --reason "P0 为空，组合层未发现需要深度辩论的交易机会"
```

随后直接进入 D+1 模拟成交；不要伪造单股 verdict。

### 6.4 辩论、05 和模拟成交

P0 非空时，逐股辩论的精确顺序、唯一输出路径、并发批次、角色 prompt 和校验命令见 [逐日调度与角色模板](references/orchestration.md)。核心顺序不可变：

```text
prepare dirs → Bull/Bear opening → 原会话 rebuttal → 3 Juror
→ aggregate → 独立 finalizer → validate → merge 05 → execute-day
```

所有 P0 完成后，自动合并当日 `05_decision.json`，补齐其顶层 `summary_date`、`system_risk_notes`、`system_focus_items` 并校验。然后只通过隔离入口执行：

```bash
python scripts/manage_fixed_tracked_backtest.py execute-day \
  --experiment-id {experiment_id} \
  --date {decision_date} \
  --execution-date {next_trading_date}
```

成功时必须出现 `{root}/06_execution_log.json`、`07_daily_summary.json`、`08_history_merge.json`；同时隔离交易记忆目录会更新 `latest_decision_snapshot.json`，按股票保留 `decision_summary.json` 中最新的一条完整分析结果，供下一交易日 P0 筛选读取。`06` 是幂等凭证：已有匹配的成功 06 时不得再次成交。

## 7. 结束、校验与汇报

所有决策日完成后：

```bash
python scripts/manage_fixed_tracked_backtest.py finalize \
  --experiment-id {experiment_id}
```

按 [结果检查](references/result-review.md) 核验净值、现金、持仓、订单、D+1 开盘价、最大回撤、重复订单、`lookahead_risk`、`survivorship_bias` 与真实账本未变。

向用户报告：实验 ID、实际交易日期范围、初始/最终资产、累计收益、最大回撤、成交次数、未成交订单原因、数据和前视偏差限制。不得把回测结果描述为无偏 point-in-time 实盘业绩。

## 8. 失败处理速查

| 现场 | 正确处理 | 禁止做法 |
| --- | --- | --- |
| `prepare-day` 非 `ready` | 读 `00_prepare_status.json`，处理财报门禁或缺失输入后重跑 | 跳过 04 或带着缺输入进入 P0 |
| 某角色文件缺失/JSON 无效 | 让该文件唯一所有者修复，再校验 | 主 Agent 手写其研究结论或跳过该股 |
| `vote_summary` 与 ballot 不一致 | 重新运行本地 `aggregate` | Agent 手写或修改 `vote_summary` |
| 05 校验失败 | 修复对应 verdict 或顶层系统字段后再合并 | 用空壳 05 绕过校验 |
| 开盘价缺失 | 保留 `pending` 订单及原因，按执行日志处理 | 用收盘价、未来价格或估算价替代 |
| 中断 | 先 `status`，只从 `next_date` 继续 | 删除 06、重复成交或重新初始化账本 |
