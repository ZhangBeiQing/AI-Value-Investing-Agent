---
name: backtest-fixed-tracked
description: 对当前 fixed_tracked 固定股池做每周一次的隔离历史回测；复用主干研究与决策规则，研究间隔七个自然日，期间持仓估值，订单仍按下一交易日开盘成交。
---

# fixed_tracked 历史回测

## 规则来源与职责

主 Agent 是实验调度器：检查输入、调度研究、选择 P0、组合复核与程序校验。不得代写个股研究或裁决，不向角色注入投资观点。

- 数据准备分工：`.codex/skills/daily-data-preparation/SKILL.md`。
- 宏观：`.codex/skills/daily-macro-summary/SKILL.md`。
- 财报：`.codex/skills/financial-report-summary/SKILL.md` 和其角色模板。
- P0、Bull/Bear、独立 Adjudicator、质询、数量决策：`.codex/skills/auto-trading-fixed-tracked/SKILL.md` 及其 references。
- 当天策略：历史目录内 `03_agent_input.md` 与 `03_stock_analysis_input.md`；由当前主干 policy 生成。
- 回测额外约束：本 skill 与 `references/artifact-contract.md`、`references/orchestration.md`、`references/guarded-web-policy.md`、`references/result-review.md`，开始前完整读取。

回测不维护第二套选股、投资或仓位规则。个股角色、schema 和共同研究方法直接复用主干，不复制到派单文字中。

## 实验边界

- 日期 D 是分析日，可知信息截止 D 日 23:59:59 北京时间；成交使用紧邻下一实验交易日的精确开盘价。缺价记 pending，不替换价格、不自动跨日重试订单。
- 实验交易日由 CN 市场日历程序解析，禁止按工作日猜测。最后一日只做收盘估值；不生成区间外订单。
- 股池由 `configs.stock_pool.fixed_tracked_symbols()` 冻结，包含当前 A/H 股、ETF 与美股。默认不并入量化候选或其他选股结果。未上市标的按缓存覆盖证据排除；已持仓股须继续覆盖。
- 美股研究、02 与持仓估值只能用 D 日北京时间截止前已收盘的美东行情，显示真实 quote_date；美元资产沿用主干人民币折算。香港及美国休市时不存在该日精确开盘价的订单保留 pending。
- 全部 00-08、账本、交易记忆和缓存写在 `data/backtests/fixed_tracked/{experiment_id}/`。禁止写正式交易目录或调用真实后处理入口。
- 共享财报深研可以通过历史日期门禁后发布；共享派生估值可以按日期接口重建，不得混入实验动作或仓位。已完成的真实交易输入与历史原稿不覆盖。
- 默认每周分析一次。研究日完成准备、P0、辩论、组合复核、05、D+1 执行及 06-08 后，推进至下一研究日；期间交易日仅做显式无交易结算，跨日不并行。
- 使用运行时通用 subagent，继承当前模型；角色并发与批次跟随对应主干 skill，不自行另定研究角色或降级模型。
- P0 仍须用户明确确认。回测在组合复核通过后自动合并和模拟执行，不再增加第二次人工确认。
- 当前实验 schema 为 2。旧实验只保留供审计，拒绝续跑，不能把新股池或新执行规则套在旧账本上。

## 创建与恢复

```bash
source /home/zhangbeiqing/venv/ai_stock/bin/activate
python scripts/manage_fixed_tracked_backtest.py prepare \
  --start-date {start_date} --end-date {end_date} --initial-cash {initial_cash} \
  --model {verified_runtime_model} --reasoning-effort {verified_runtime_effort} \
  --model-provider {provider} --runtime {runtime} --network-mode guarded_web
```

模型参数只记录运行时事实，不切换模型。Codex 可从当前 CODEX_THREAD_ID 会话日志的最新 turn_context 核实模型与推理档位；无法取得时记录 unknown，不猜测。恢复前核对实际模型/档位与 experiment.json，变化时另建实验。

读取 experiment.json 和 coverage.json。历史输入始终按当前规则重建，不复制正式 01-04，不手改实验身份或完成状态。

```bash
python scripts/manage_fixed_tracked_backtest.py status --experiment-id {experiment_id}
python scripts/manage_fixed_tracked_backtest.py extend --experiment-id {experiment_id} --end-date {new_end_date}
```

恢复从 progress.next_date 与 next_stage 继续。成功 06 是幂等凭证，禁止重复成交。extend 只向后延长原实验，新增区间完成后重新 finalize。

## 每周研究节奏

- 首次研究日默认为实验起始交易日；用户指定日期或中途调整节奏时以用户指令为准。以本轮计划研究日 D 为锚，下一计划日期为 D + 7 个自然日，例如 2026-07-03 → 2026-07-10，不按五个交易日替代。
- 若 D+7 休市，按实验 CN 交易日历顺延至第一个交易日；下一轮从实际研究日重新加七天。不超过实验末日，末日仍只做估值。
- 用户跳过本轮或本轮 P0 为空，也消耗本轮研究窗口，下一次仍按本轮日期加七天，不能次日重新选 P0。
- 主 Agent 在实验根目录 `analysis_schedule.json` 记录当前节奏、锚日期、下一研究日及用户中途变更原因；它是调度记录，不修改 experiment.json 的实验身份或已成交历史。中途改频率必须在最终结果中说明分段节奏。
- 研究间隔内不生成 01-04、不更新宏观、不派发个股/财报研究、不做每日 P0；只用现有 `no-trade-day` 和 `execute-day` 按日历串行写空 05 与 06-08，延续持仓和净值，不伪造单股 verdict，也不把无交易日写成新的深研日期。
- 到达下一研究日前，必须结清前面的所有实验交易日。`status.progress.next_date` 是下一待结算日期，不一定是研究日，必须与 `analysis_schedule.json` 一起读取。不得直接跳日期绕过前日完成校验。
- 研究日 P0 扫描至少覆盖上次研究以来的事件，不能只看主干默认的最近三个交易日；使用十四日事件窗口检查整个间隔，对影响盈利、估值、治理或风险的实质变化定向核验。其他选股、仓位和裁决规则仍复用主干，不另设周度交易规则。
- 成交日保持研究日紧邻的下一实验交易日，不推迟到下一周研究日；间隔内 pending 仍不自动重试。

间隔无交易结算的恢复与命令示例见 `references/orchestration.md` 第1节。

## 研究日数据准备

```bash
python scripts/manage_fixed_tracked_backtest.py prepare-day \
  --experiment-id {experiment_id} --date {decision_date} --build-missing-inputs --max-workers 6
```

本命令先读本地缓存，检查历史资料、宏观、财报深研与摘要，再生成隔离 01-04；不会自动刷新全池公告或启动模型。
读取 `{root}/00_prepare_status.json`，`{root}={experiment_root}/skill_runs/{decision_date}/fixed_tracked`。只有 ready 才能进入 P0。

1. **needs_shared_data**：读取 shared_inputs.issues。通过现有数据准备入口补指定股票的缺口，例如 `prepare_single_stock.py --symbol {symbol} --date {decision_date} --only data`；禁止未检查就全池强刷。抓取面向真实时间取足历史，读取时截断。不得以旧交易日价格冒充所需收盘；缺少原文或日历时明确失败。
2. **needs_macro_research**：默认复用 D 日以前宏观稿；本次需要重写时创建实验显式加 `--macro-summary-policy regenerate`。按日常数据准备的模板派发一个独立宏观 subagent，额外传历史截止、00_backtest_context.md 与隔离输出 `{experiment_root}/macro_economy/YYYYMMDD.md`。主 Agent 不代写。首日重建基线、之后逐日更新，遵守当前五节和 3000 非空白字符上限；保留共享原稿，禁止回退现实 latest 面板。
3. **needs_financial_research**：按 financial_research 区分 required_items（缺少最新历史深研或需要更新）和 daily_brief_items（仅缺摘要）。展示 existing_historical_research、rejected_historical_reports 和原因，不能把有旧深研说成没有研究。财报原文只能是输入，不是深研总结。
4. 宏观 subagent 可与纯 Python 财报 prepare 并行。两者结束后由主 Agent 直接调度财报 skill 的研究角色；完整深研与只补摘要分别走对应模板。只补摘要不联网、不改全文、不重做行业和预期研究。所有角色先读历史上下文；缺 PDF Markdown 时先确认 pymupdf4llm 就绪。
5. 新深研注册保留 `--as-of-date {decision_date} --require-deep-research --require-daily-brief`。不以原文存在、文件名公告日期或摘要格式通过代替历史真实性与内容核验。同一报告期的业绩公告与正式报告可复用已覆盖的研究；经营数据公告不按标题一刀切排除，指引与实际结果须明确区分，后续新增实际结果需要对照更新。
6. 宏观和财报通过验收后重跑同日 prepare-day。修复已有输入时加 `--force-rebuild-inputs`，只重建未成交日期。必须检查 01、02、03_agent_input、03_stock_analysis_input、每股非空 04 以及 run_manifest；不得因空目录或缺股而报 ready。

财报选择跨同 symbol 的名称目录查找，跳过误放原文。历史复用核对研究知识截止，不将晚于 D 的研究重标为历史摘要。被拒绝的未来资料不进入角色上下文。

## P0、辩论与组合复核

按日常交易 skill 的阶段 A/B/C 执行。主 Agent只读 00、01、02、03、p0_event_report、price_delta_report、隔离分析索引和最近决策快照，不读 04。
展示 P0 的股票、触发原因、批次和 P1 说明，等待用户明确确认；不预先指定 BUY/SELL 方向。用户明确指定研究范围时服从其授权。

确认后按 references/orchestration.md 替换路径与实际隔离持仓，使用同一 Bull/Bear → 独立 Adjudicator → 必要质询 → validate。主 Agent 不代写 verdict。

合并前按主干阶段 C 复核多股 BUY 总额、现金、币种、价格日期、卖出上限、交易单位、集中风险和关键争议，写 `{root}/00_portfolio_review.md`。调整数量交原裁决者并重新校验，不能靠执行器缩量弥补组合复核遗漏。

P0 为空时通过 no-trade-day 生成显式空 05，仍做 D+1 无交易结算，不伪造单股裁决。

```bash
python scripts/manage_fixed_tracked_backtest.py no-trade-day \
  --experiment-id {experiment_id} --date {decision_date} --reason {reason}
```

## 合并与模拟执行

按 orchestration.md 对所有 P0 validate，通过 merge_subagent_decisions.py 合并 05，补齐顶层系统字段，然后：

```bash
python scripts/manage_fixed_tracked_backtest.py execute-day \
  --experiment-id {experiment_id} --date {decision_date} --execution-date {next_trading_date}
```

执行沿用主干先 SELL 后 BUY 的批次、交易单位和人民币计价。非法数量、卖超或现金不足整批拒绝，不按置信度排序、不向下取整、不自动缩量。不解析自然语言预案的条件订单。
成功后验收 06、07、08 与隔离最近分析快照。账本有执行但缺 06 时停止，核对中断现场，不重复下单。

## 结束与异常

```bash
python scripts/manage_fixed_tracked_backtest.py finalize --experiment-id {experiment_id}
```

所有决策日执行完成后才可 finalize。按 result-review.md 核对净值、币种、现金与持仓、成交价、重复订单、pending/rejected 以及偏差标记。
报告实验 ID、日期范围、资金、收益、回撤、成交次数和数据限制。本回测使用当前股池回看历史，零费用/滑点且允许受约束联网，属于策略模拟，不是无偏实盘业绩。

失败按具体状态修复；不重启仍在运行的长命令，不跳过门禁，不删除成功 06。长命令每 60 秒检查并告知进展，最多等待一小时；停止或恢复时保留现场。
