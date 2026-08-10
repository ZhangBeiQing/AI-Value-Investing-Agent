# 回测产物、所有权与推进契约

本文件是 `backtest-fixed-tracked` 的唯一文件分配表。任何角色写文件前先确认：该文件是否属于自己、前置文件是否已完成、写完后由谁校验。

## 1. 路径变量

```text
{experiment_root} = data/backtests/fixed_tracked/{experiment_id}
{skill_runs_root} = {experiment_root}/skill_runs
{day_root} = {skill_runs_root}/{decision_date}/fixed_tracked
{debate_root} = {day_root}/debate/{stock_name}_{symbol}
```

`decision_date` 是 D 日收盘后的决策日期，`execution_date` 必须是实验交易日序列中紧邻 D 的下一交易日。

## 2. 实验级文件

| 文件/目录 | 写入者 | 写入时机 | 约束 |
| --- | --- | --- | --- |
| `{experiment_root}/experiment.json` | `prepare` / `extend` 程序 | 创建、合法延长区间 | 人工或 Agent 不得修改起始日期、初始资金、实验 ID、执行规则；`extend` 只更新结束日期与历史记录 |
| `{experiment_root}/coverage.json` | `prepare` / `extend` 程序 | 创建或扩展后 | 只读审计结果 |
| `{experiment_root}/agent_data/backtest-{id}/` | 隔离账本与后处理程序 | 初始化与每次成功执行后 | 禁止使用真实 `data/agent_data` 替代 |
| `{experiment_root}/checkpoints/` | 程序 | 运行中 | 仅用于恢复，不能手工伪造完成状态 |
| `{experiment_root}/results/` | `finalize` 程序 | 全部日期完成后 | 扩展实验后旧结果过期，必须重建 |

## 3. 单日文件与唯一写入者

| 阶段 | 文件 | 唯一写入者 | 推进前置 | 写后校验 |
| --- | --- | --- | --- | --- |
| 准备 | `00_backtest_context.json/.md` | `prepare-day` | 合法决策日 | 文件存在；所有角色先读 `.md` |
| 准备 | `00_prepare_status.json` | `prepare-day` | 输入/财报门禁检查完成 | `status == ready` 才可 P0 |
| 准备 | `01`、`02`、`03`、`04` | `prepare-day` / 历史流水线 | 财报门禁通过 | 文件齐全；研究包只限 D 日及以前信息 |
| 准备 | `../run_manifest.json` | `prepare-day` | 当日 universe 建立 | 确认固定池 + 当日 12 候选 + 已持仓股 |
| 目录 | `debate/.../` 空目录 | `manage_debate.py prepare` | P0 已确认 | 只建目录，不写角色结果 |
| Opening | `advocates/bull/opening.json` | Bull | 目录存在 | JSON 合法；只读本股研究包 |
| Opening | `advocates/bear/opening.json` | Bear | 目录存在 | JSON 合法；只读本股研究包 |
| Rebuttal | `advocates/bull/rebuttal.json` | 原 Bull 会话 | 双方 opening 合法 | JSON 合法 |
| Rebuttal | `advocates/bear/rebuttal.json` | 原 Bear 会话 | 双方 opening 合法 | JSON 合法 |
| 投票 | `jury/juror_0{1,2,3}/ballot.json` | 对应独立 Juror | 两份 rebuttal 合法 | JSON 合法；Juror 间互不可读 |
| 聚合 | `final/vote_summary.json` | `manage_debate.py aggregate` | 三份 ballot 合法 | 严格匹配 ballot 与当日持仓；Agent 不得写 |
| 裁决 | `final/stock_verdict.json` | 独立 finalizer | `vote_summary` 已生成 | `validate --require-verdict` 通过 |
| 合并 | `05_decision.json` | `merge_subagent_decisions.py` 或 `no-trade-day` | 全部 P0 verdict 合法，或 P0 为空 | 决策 schema 通过；顶层系统字段非空/合理 |
| 执行 | `06_execution_log.json` | `execute-day` | 合法 05 与 D+1 开盘价 | `status=success` 是幂等凭证 |
| 总结 | `07_daily_summary.json` | `execute-day` | 成功 06 | 记录执行日和组合结果 |
| 记忆 | `08_history_merge.json` | `execute-day` | 成功 06 | 下一日读取隔离交易记忆 |
| 决策快照 | `{experiment_root}/agent_data/backtest-{id}/latest_decision_snapshot.json` | 隔离交易记忆服务 | `decision_summary.json` 更新后 | 每只股票仅保留 `end_date` 最新的一条完整分析结果；下一日 P0 可读，不能当作直接交易指令 |

## 4. 状态门与不可跨越顺序

```text
00_prepare_status.ready
  → P0
  → opening × 2
  → rebuttal × 2
  → ballot × 3
  → vote_summary
  → stock_verdict
  → 05_decision
  → 06_execution(D+1 open)
  → 07 + 08
```

- 同一股票不得跨过任何箭头。
- 同一交易日不得在 06 成功前开始下一日。
- 区间最后日没有 D+1，不得生成决策或 06。
- `P0 = 空` 是唯一不走逐股辩论的正常分支：写显式空 05，再走 D+1 的无交易执行与 06-08。

## 5. 目录与信息隔离

允许写入：

```text
data/backtests/fixed_tracked/{experiment_id}/**
data/stock_info/*/financial_reports/**  # 仅通过公告日和 as-of-date 门禁的基本面总结
data/stock_info/*/analysis/**           # 正常日期接口按 D 日重建的派生研究中间结果
data/stock_info/*/pe_pb_analysis/**     # 正常日期接口按 D 日重建的估值中间结果
```

禁止写入：

```text
data/agent_data/**
data/skill_runs/**
data/selection_runs/**
真实交易账本、真实订单或真实 post-trade 路径
```

`analysis` 与 `pe_pb_analysis` 是可覆盖的派生中间结果，不是历史决策或交易产物；缺少现成 Markdown 时必须按 D 日走正常接口重建，不能因此输出空估值。共享公告、行情、财报缓存只能在读取时按 D 日截断。公告准备成功后可由实验 checkpoint 复用，但每日读取仍必须包含 D 日已发布且排除 D 日之后的公告。不得用 D 后新增文件、当前一致预期、事后复盘、修正公告或未来价格补齐历史资料。

## 6. 数量、交易与恢复约束

- `aggregate` 的 `--position-shares` 必须来自实验隔离账本在 D 日的实际持仓，不能使用真实持仓或猜测值。
- 所有 BUY/SELL 的 D+1 成交价必须由程序读取下一交易日精确开盘价；缺价时订单为 `pending`，不得替换价格。
- 成功 `06_execution_log.json` 不能删除、覆盖或重复执行；它是当天订单幂等边界。
- 订单、现金、持仓和交易记忆只由隔离执行程序写入。主 Agent、研究角色和 finalizer 都不能修改。
- 任何异常先运行 `status`，读取 `00_prepare_status.json`、`06_execution_log.json` 与 checkpoint，再决定从哪一状态恢复。
