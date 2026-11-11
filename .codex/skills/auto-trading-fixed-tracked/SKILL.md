---
name: auto-trading-fixed-tracked
description: 固定股池每日交易分析与人工确认。用户输入"开始固定股票池交易”“开始今天固定股池交易”时使用。默认 Bull/Bear 独立研究后由一名独立裁决者输出最终决策，争议时定向质询；确认后合并05并运行虚拟账本后处理，不连接券商。
---

# fixed_tracked 管理者工作流

## 1. 职责

主 Agent 负责输入检查、组合风险、P0、角色调度、程序校验、组合复核和人工确认；不得代替个股角色研究或指定动作与数量。

**Bull / Bear 独立 opening → 独立 Adjudicator（同时完成最终输出）。**
常规 3 个角色、3 个 turn；有决定性争议时再做定向回应。
事实核验和人工确认不能因角色减少而省略。裁决者独立决定动作与数量，不按票数聚合。

## 2. 输入与规则来源

定位 `data/skill_runs/{date}/fixed_tracked/`，必须已有：

- `01_global_context.md`
- `02_basic_snapshot_payload.json`
- `03_agent_input.md`
- `03_stock_analysis_input.md`
- `p0_event_report.md`
- `04_stock_research/`，当前股票研究包唯一

缺失或不完整时报告并暂停，先问用户是否补数据，不能自动补跑数据链路。
日期为要分析的已收盘交易日，默认 today；周末或节假日显式指定最近交易日。休市日研究仅消费已按该自然日明确准备的输入，不以旧收盘冒充当日成交数据。
美股最新已收盘日可早于 A 股分析日，须显示真实行情日期，禁止冒充实时价格。

规则直接读取，不在派单消息中转述：

- 主 Agent：当天 `03_agent_input.md`。
- 全部个股角色：当天 `03_stock_analysis_input.md`、`01_global_context.md` 和本股完整研究包。
- Bull / Bear：[Bull](references/debate-bull.md)、[Bear](references/debate-bear.md)。
- 独立裁决者：[Adjudicator](references/debate-adjudicator.md)。
- 需要反驳时：[Rebuttal](references/debate-rebuttal.md)。
- JSON：[写入规范](references/json-writing-guide.md)。
- 最终字段：`configs/prompt_flow/fixed_tracked/stock_decision.schema.json` 与对应 example。

Codex + GPT 优先自带搜索，其他组合优先百炼 MCP；遵循 `configs/research/web_research_policy.md` 的证据准入、回溯与失败关闭规则。

所有角色使用运行时通用 subagent，继承当前模型，不自行降级。不同股票可并行；同一股票按阶段先后。
每批不超过运行时并发额度与 20 的较小值，落盘与校验完成再推进。不要求全池每天深研：由 P0 选择决定。

## 3. 阶段 A：P0 与第一次人工确认

选择 P0 前先刷新最近分析快照：

```bash
python -c "from services.trading.trade_summary import refresh_latest_decision_snapshot; refresh_latest_decision_snapshot('book-fixed_tracked')"
```

快照范围为当前股池与实际持仓。每股读取代码、名称、最近分析日期、价格印象和完整待核验事项，用于判断今天是否值得重点研究。

主 Agent 按顺序读：03_agent_input、02、01、p0_event_report、price_delta_report、`data/skill_runs/_analysis_index.json`（若有）、
`data/agent_data/book-fixed_tracked/latest_decision_snapshot.json`（若有），及相关可选市场级文件。
先用事件概览核查全池近期事项；资料缺失或时效不足时定向搜索补证，记录日期和影响对象后再选 P0。此阶段不读 04。

按主 Agent 规则做系统风险判断和 P0 选择，展示 symbol、名称、触发原因、批次与 P1 说明，然后暂停。
用户明确确认 P0 前不得创建研究 Agent；用户已经明确指定只研究某股票时，按该授权范围执行。

## 4. 阶段 B：独立研究与裁决

### B1. 准备

```bash
python scripts/manage_debate.py prepare --date {date} --book-type fixed_tracked --symbol {symbol} --position-shares {当日实际持股数量}
```

创建角色目录与 `review_context.json`，记录本账本当日实际持股数量，不生成任何研究或决策。股数须从当日账本上下文核实，不能取建议目标仓位。

```text
debate/{stock_name}_{symbol}/
├── review_context.json              # 程序：当日实际持股数量
├── advocates/bull/opening.json      # Bull
├── advocates/bear/opening.json      # Bear
├── adjudicator/                     # 裁决者的可选质询与解决记录
└── final/stock_verdict.json          # 独立裁决者
```

### B2. 两方独立 opening

为 Bull、Bear 分别创建独立 subagent，保存原会话 ID。派单只包含身份、日期、股票、必读文件及允许输出路径。
两方开篇独立研究，不读对方论点、历史 verdict 或截止日期之后的资料。

Bull 模板（Bear 对称替换 reference、身份和输出目录）：

```text
你是 {date}、{symbol} {stock_name} 的 Bull advocate。
完整读取：
.codex/skills/auto-trading-fixed-tracked/references/debate-bull.md
.codex/skills/auto-trading-fixed-tracked/references/json-writing-guide.md
{root}/03_stock_analysis_input.md
{root}/01_global_context.md
{当前股票唯一研究包路径}
只写：{root}/debate/{stock_name}_{symbol}/advocates/bull/opening.json
完成后只回传路径。
```

两份 opening 均存在、非空且 JSON 合法后，才进入独立裁决。主 Agent 只做调度和格式检查，不把自己的个股观点传给裁决者。

### B3. 一名独立裁决者

新建独立 subagent，不能复用任一 Advocate 会话：

```text
你是 {date}、{symbol} {stock_name} 的独立 Adjudicator。
完整读取：
.codex/skills/auto-trading-fixed-tracked/references/debate-adjudicator.md
.codex/skills/auto-trading-fixed-tracked/references/json-writing-guide.md
configs/prompt_flow/fixed_tracked/stock_decision.schema.json
configs/prompt_flow/fixed_tracked/stock_decision.example.json
{root}/03_stock_analysis_input.md
{root}/01_global_context.md
{当前股票唯一研究包路径}
{debate_root}/review_context.json
{debate_root}/advocates/bull/opening.json
{debate_root}/advocates/bear/opening.json
只写：{debate_root}/final/stock_verdict.json；需质询时可写 {debate_root}/adjudicator/review_request.json 与 review_resolution.json。
完成后只回传动作和路径，或阻塞问题及路径。
```

裁决者独立决定动作、数量、价格印象和最终字段，综合两方证据但不按论点数量投票。
裁决者同时承担独立审查与最终整理职责；程序校验不证明结论正确。

### B4. 按需质询

不强制每股反驳。决定性数字/口径冲突、重大更正、缺失反面证据或盈利桥接断点无法直接核验，或合理的估值/盈利假设差异足以改变动作且关键论证缺失时：

1. 裁决者写 `adjudicator/review_request.json` 并返回阻塞，不写可执行 verdict。
2. 主 Agent 只转发问题和路径，复用相关原 Advocate 会话做定向核验；可写本方 `rebuttal.json`，不改 opening。
3. 新决定性证据对双方都相关时，同步给双方相同文件及截止日期；裁决者必须读到所有回应和新增证据。
4. 原裁决者续会话逐项核验并写 `review_resolution.json`，逐项完成核验、假设取舍和影响说明后写 verdict。
5. 回应完成后，裁决者依据当前可得信息输出最终动作。无法精确预测盈利、倍数或概率是投资判断的不确定性，应进入假设、风险及数量，不能据此拒绝裁决或自动HOLD。基础输入错误导致无法形成合法预案时，报告具体错误；不得编造输入或以自报置信度替代核验。

新财报、重大风险不会机械触发三个重复裁判；必须逐项核对受影响的原文、分母、指引和情景，需要时进入质询。
没有需要补充的关键论证时直接裁决，不必额外创建“审核是否需要反驳”的 Agent。

### B5. 程序校验

```bash
python scripts/manage_debate.py validate --date {date} --symbol {symbol} --require-verdict
```

校验双方 opening、质询闭环、最终字段、动作/持仓语义、卖出不超持仓，
并复用现有执行器的交易单位规则。它不替代现金与组合复核，也不证明事实正确。
原文发生修订时按实际影响重审，不沿用已失效结论。

## 5. 阶段 C：组合层复核与第二次人工确认

主 Agent 读取每股 verdict，不重新写个股结论。核对：

- 同时 BUY 的总额、现金、币种换算和价格基准；不能默认用尚未成交的 SELL 为 BUY 融资。
- SELL 不超过持股，数量遵循市场现有规则，计划与下个交易日适配。
- 同行业/主题集中风险、系统风险与多个 verdict 的冲突。
- 数据缺口、尚未解决的争议和决定性事实的核验情况。
- 动作所依赖的利润、倍数、兑现时点及合理变化能否改变方向；关键论证缺失时交回原角色质询；回应后由裁决者取舍并输出动作，不要求消除投资不确定性。主 Agent 不代写动作。

展示动作、数量、理由、价格/数据日期、主要风险和组合数量调整建议。
暂停；未经用户明确确认“生成决策”或同义授权，不生成 05。

## 6. 阶段 D：确认后合并 05

用户接受数量调整时，原裁决者更新自己的 verdict 数量及执行说明；不能由主 Agent 静默改研究结论。
更新后重新 validate。若需改方向或新增决定性事实，应重审而非当成数量整理。

```bash
python scripts/merge_subagent_decisions.py --date {date} --book-type fixed_tracked
```

合并程序校验当前链路，缺少任一已准备股票的 verdict 时停止。原有 05 顶层和单股字段契约保持不变。
补齐 summary_date、system_risk_notes、system_focus_items。用户明确要求“开始后处理”时，已授权合并现有决策并继续阶段 E，不重复请求确认。

## 7. 阶段 E：虚拟账本后处理

后处理仅更新本地虚拟账本、交易记录和总结归档，不连接券商，不修改用户手工记录的真实持仓。

用户要求后处理后，主 Agent 直接运行以下命令；05 尚未生成时先执行阶段 D 的合并命令：

```bash
python scripts/run_post_trade.py --date {date} --book-type fixed_tracked --signature book-fixed_tracked
```

- 禁止在后处理阶段重新开展个股、财报、估值、真实持仓或决策影响核对，禁止派发或唤醒 subagent。
- 禁止重写裁决、调整动作数量、重建研究包或增加确认步骤。研究与组合复核在阶段 B/C 完成。
- 字段、交易单位、价格和账本校验由现有程序完成，主 Agent 不额外重复检查。
- 命令成功后直接汇报结果；失败时只处理命令报告的具体运行、文件或格式问题，不启动额外研究。
- 沿用现有价格引用、现金与仓位计算及账本更新规则。下次 P0 读取刷新后的 latest_decision_snapshot。

## 8. 失败与恢复

- 缺文件、空返回或格式错误：找原会话和唯一文件所有者修复，不让主 Agent 代写结论。
- 研究存在未知：定向核验，仍无法取得的资料明确列为未知，由裁决者选择工作假设并完成决策，不伪造事实。
- 输入或决定性事实改变：由原角色按新资料重审，重新 validate，不把旧结论当作新分析。
- 所有角色禁止修改程序文件、其他角色文件或 05。程序校验只管结构和一致性，事实正确性仍由研究与裁决负责。
