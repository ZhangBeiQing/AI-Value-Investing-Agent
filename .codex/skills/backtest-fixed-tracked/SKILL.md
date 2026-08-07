---
name: backtest-fixed-tracked
description: 对指定历史日期区间自动运行 fixed_tracked 多 Agent 交易决策回测。当用户说“回测固定股池”“测试某段日期的固定池收益”“用50万从某日回测到某日”时使用。Skill 使用当前固定池加每日 12_quant_prefilter_long 候选，逐日自动完成 P0、辩论、投票、05、D+1 开盘模拟成交和隔离交易记忆，不触碰真实账本。
---

# fixed_tracked 历史回测

## 1. 核心约束

- 激活 `/home/zhangbeiqing/venv/ai_stock`。
- 日期必须按交易日从早到晚串行；同一天内部允许多股票并行。
- 交易日序列由程序从 `000001.IDX` 实际行情日期生成；缓存不能完整覆盖时使用
  `SSE` 交易所日历。不得由 Agent 按工作日猜测，也不得把周末或法定休市日传给
  `prepare-day`。
- `12_quant_prefilter_long.csv` 只扩展研究范围，不直接决定交易。
- 当日 12 不存在但历史因子快照存在时，只读因子快照并在实验目录重建 12；
  不写回正式 `data/selection_runs` 或 `data/factor_store`。
- 所有角色直接读取当天 `00_backtest_context.md`。
- 联网研究服从 [references/guarded-web-policy.md](references/guarded-web-policy.md)。
- 辩论角色继续读取 `auto-trading-fixed-tracked/references/` 下的原规则，不复制规则。
- 不进行日常 Skill 的两次人工暂停；自动接受 P0、自动合并 05、自动后处理。
- 所有可变产物必须位于实验目录；禁止写正式 `data/agent_data`。
- 例外：按财报公告日生成并通过日期因果门禁的季度基本面总结继续写入共享
  `data/stock_info/*/financial_reports`，供其他实验和日常研究复用；不得包含
  回测仓位、交易动作或 D 日之后的信息。
- `data/stock_info/*/analysis` 与 `pe_pb_analysis` 是日常可覆盖的派生中间产物；
  回测应调用正常日期接口按回测日重建它们，并把返回结果直接合入 04。不得仅因
  当前目录没有现成历史 Markdown 就输出空估值。
- 重建 04 前，程序会对当日股票集合增量补齐普通公告摘要与
  `news_audited.json`；同一实验内每只股票成功准备一次后由 checkpoint 复用，
  后续日期不重复联网。每日读取必须只保留发布日期不晚于决策日 D 的公告，
  包括 D 日已经发布的公告，排除 D 日之后的内容。
- 重建 04 前还有强制财报门禁：先按 D 日截断选择每股最新已披露财报；若该财报
  尚无已登记的深度基本面总结，`prepare-day` 必须返回
  `needs_financial_research` 并停止，不得生成缺财报的 04。主 Agent 必须复用
  `financial-report-summary` 完成完整多角色研究与历史日期注册。
- 长耗时命令的等待、轮询和输出可观测性必须服从
  [references/orchestration.md](references/orchestration.md) 的“长耗时命令执行”规则；
  不得因工具短暂 yield 或一段时间无新输出而误判超时。
- 不调用真实模式的 `run_post_trade.py`。

## 2. 建立实验

```bash
source /home/zhangbeiqing/venv/ai_stock/bin/activate
python scripts/manage_fixed_tracked_backtest.py prepare \
  --start-date {start_date} \
  --end-date {end_date} \
  --initial-cash {initial_cash} \
  --network-mode guarded_web
```

记录命令返回的 `experiment_id`。读取：

```text
data/backtests/fixed_tracked/{experiment_id}/experiment.json
data/backtests/fixed_tracked/{experiment_id}/coverage.json
```

默认实验ID只包含固定不变的起始日期和创建时间，不包含可继续向后扩展的结束日期。
起始日期不能修改；如果需要更早的起点，必须新建实验。

不要因为历史 01-04 缺失就使用未来文件冒充；按当日步骤补建。默认用当前代码和
Prompt 在实验目录重建 01-04。只有用户明确确认旧产物已经完成日期因果与 Prompt
兼容审计时，才给 `prepare-day` 增加 `--reuse-existing-inputs`。

## 2.1 向后续跑原实验

用户要求在满意的原结果上继续回测时，不新建目录、不复制账本，也不修改
`experiment_id`。执行：

```bash
python scripts/manage_fixed_tracked_backtest.py extend \
  --experiment-id {experiment_id} \
  --end-date {new_end_date}
```

`new_end_date` 必须晚于当前结束日期，并且区间内至少增加一个交易日。命令保持
原起始日期、目录、仓位账本、订单、投资记忆、01-08 和 checkpoint 不变，只更新
`experiment.json`、追加 `extension_history` 并重建 `coverage.json`。

原结束日此前只用于期末估值；扩展后它会按新的交易日序列自动成为普通决策日。
随后运行 `status`，从 `next_date` 继续逐日回测。旧 `results/summary.json` 在截止
日期不一致时属于过期结果，完成新增区间后必须重新 `finalize`。

## 3. 逐日循环

完整调度见 [references/orchestration.md](references/orchestration.md)。

除区间最后一个交易日外，每个交易日先运行（最后一个交易日只用于持仓收盘估值，
因为区间内没有下一交易日可成交）：

```bash
python scripts/manage_fixed_tracked_backtest.py prepare-day \
  --experiment-id {experiment_id} \
  --date {date} \
  --build-missing-inputs \
  --max-workers 6
```

要求当日目录存在：

```text
00_backtest_context.md
01_global_context.md
02_basic_snapshot_payload.json
03_agent_input.md
03_stock_analysis_input.md
04_stock_research/
```

如果 `00_prepare_status.json.status == needs_financial_research`：

1. 运行 `financial_research.preparation_command`，只准备
   `required_items`，不要扩成全固定池；
2. 完整执行 `financial-report-summary` 的 Industry Researcher、
   Expectation Scout、Financial Author、Research Challenger 和 Author 修订；
3. 所有财报角色额外完整读取逐股 workdir 中的 `00_backtest_context.md`；
4. 按 `registration_command_template` 登记，必须保留 `--as-of-date D`；
5. 全部注册成功后，运行返回的 `resume_prepare_day_command`，强制重建同一天04；
6. 门禁变成 ready、04 已生成后，才能进入 P0。

不得把“财报原文存在”误当成“深度总结已完成”，也不得用 D 日之后发布的财报或
总结填补。`unavailable_items` 表示同步后在 D 日以前确实没有可识别财报原文，
允许04明确记录数据缺口。

修复旧实验中已经生成的空财报04时，在完成深研和历史日期注册后执行：

```bash
python scripts/manage_fixed_tracked_backtest.py prepare-day \
  --experiment-id {experiment_id} \
  --date {date} \
  --build-missing-inputs \
  --force-rebuild-inputs \
  --max-workers 6
```

如果确定性本地构建后仍缺文件，停止该日并记录原因；不得跳过后继续推进仓位。

## 4. P0 与辩论

主 Agent按当天 `03_agent_input.md` 做 P0，不读取个股研究包。回测自动接受 P0。

每只 P0 严格执行：

```text
prepare debate dirs
→ Bull/Bear opening
→ 原 Bull/Bear rebuttal follow-up
→ 3 个独立 Juror
→ manage_debate aggregate
→ 独立 Finalizer
→ validate
```

角色除日常输入外必须先读取当天 `00_backtest_context.md`。文件所有权、JSON 契约和角色规则完全复用 `auto-trading-fixed-tracked`。

所有 P0 完成后直接运行：

```bash
python scripts/merge_subagent_decisions.py \
  --date {date} \
  --book-type fixed_tracked \
  --source debate \
  --base-dir data/backtests/fixed_tracked/{experiment_id}/skill_runs
```

补齐 05 顶层 `system_risk_notes` 和 `system_focus_items`，校验后继续，不等待人工确认。

## 5. 自动后处理

由程序确定并校验下一实际交易日后运行：

```bash
python scripts/run_post_trade.py \
  --date {date} \
  --book-type fixed_tracked \
  --backtest-root data/backtests/fixed_tracked/{experiment_id} \
  --execution-date {next_trading_date} \
  --execution-price open
```

必须产生：

```text
06_execution_log.json
07_daily_summary.json
08_history_merge.json
```

并更新实验目录中的：

```text
agent_data/backtest-{experiment_id}/position/position.jsonl
agent_data/backtest-{experiment_id}/stock_decisions.json
agent_data/backtest-{experiment_id}/decision_summary.json
agent_data/backtest-{experiment_id}/portfolio_daily_summary.json
```

完成后才能进入下一交易日。下一日 04 必须读取这里形成的最后一次投资逻辑和待核验事项。

## 6. 断点与结束

已有成功的 06 时不得重复成交。平台或会话中断后先运行：

```bash
python scripts/manage_fixed_tracked_backtest.py status \
  --experiment-id {experiment_id}
```

从第一个没有成功 06 的交易日继续。

全部日期完成后：

```bash
python scripts/manage_fixed_tracked_backtest.py finalize \
  --experiment-id {experiment_id}
```

按 [references/result-review.md](references/result-review.md) 检查结果。结束日盘后新决策不跨出区间成交。
