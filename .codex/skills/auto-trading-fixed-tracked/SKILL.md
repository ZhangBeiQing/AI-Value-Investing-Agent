---
name: auto-trading-fixed-tracked
description: 固定跟踪池每日交易分析与人工确认工作流。用户要求“开始固定股票池交易”“开始今天固定股池交易”或要求对 fixed_tracked 运行多 Agent 辩论时使用。主 Agent先做组合与 P0 筛选，再为每只 P0 股票协调 Bull、Bear、Rebuttal、三名 Juror 和 finalizer，用户确认后才生成 05_decision.json；不自动执行真实交易。
---

# fixed_tracked 管理者工作流

## 1. 职责

担任中心化管理者。只负责：

- 确认分析日期和输入完整性；
- 完成组合层系统风险判断和 P0 选择；
- 在用户确认后调度逐股辩论；
- 检查每个角色只写自己的文件；
- 聚合三名 Juror 投票；
- 汇总单股 verdict 并做组合层数量复核；
- 在第二次人工确认后生成 `05_decision.json`；
- 只有用户另行明确授权时才进入交易后处理。

不要代替 Bull、Bear 或 Juror 做个股深研，不要把自己的分析加入角色 prompt，不要允许任何 subagent 直接修改 `05_decision.json`。

## 2. 前置输入

定位：

```text
data/skill_runs/{date}/fixed_tracked/
```

要求存在：

```text
01_global_context.md
02_basic_snapshot_payload.json
03_agent_input.md
03_stock_analysis_input.md
04_stock_research/
```

若缺少任一文件，暂停并向用户报告。不要自行运行旧数据准备链补文件。

可选共享输入：

```text
data/selection_runs/{date}/06_hot_news_state.json
data/selection_runs/{date}/05_board_heat_digest.json
data/skill_runs/_analysis_index.json
```

可选文件缺失时记录为无此输入，不因此终止。

`fixed_tracked` 已包含固定股票池、真实持仓和长期候选；不要再单独运行 `long_book` Skill。

## 3. 规则唯一来源

- 主 Agent的组合与 P0 方法：当天 `03_agent_input.md`。
- 个股共同投资、联网搜索和证据准入规则：当天 `03_stock_analysis_input.md`。
- Bull opening：[references/debate-bull.md](references/debate-bull.md)。
- Bear opening：[references/debate-bear.md](references/debate-bear.md)。
- 双方 rebuttal：[references/debate-rebuttal.md](references/debate-rebuttal.md)。
- Juror 投票：[references/debate-juror.md](references/debate-juror.md)。
- 单股 final verdict：[references/debate-finalizer.md](references/debate-finalizer.md)。
- 最终单股字段：`configs/prompt_flow/fixed_tracked/stock_decision.schema.json`。

主 Agent必须让目标角色直接读取对应文件。禁止在运行时 prompt 中复制、概括或改写这些规则。

## 3.1 模型角色路由

固定股池辩论必须按以下 OpenCode subagent profile 派发：

- Bull opening、Bear opening，以及复用原会话的 Bull/Bear rebuttal：`fixed-tracked-advocate-luna`（GPT-5.6 Luna）；
- 三名独立 Juror 与唯一 finalizer：`fixed-tracked-adjudicator-terra`（GPT-5.6 Terra）。
- 如果是回测模式为了降低成本，全部使用更便宜的`fixed-tracked-advocate-luna`（GPT-5.6 Luna）

Juror 属于有投票权的裁判角色，不得改用 Luna。除非专用 profile 不可用且用户明确同意降级，否则不得静默回退到通用 Agent 或其他模型。

**主 Agent 的角色边界：只负责调度与文件路径，不代做任何个股决策，不传递任何客观规则，所有规则都在文件里，主agent只要让subagent看文件就行** 主 Agent 只向 subagent 传递「角色身份、必读文件清单、唯一输出路径」这三类必要信息；所有具体决策——包括价值判断、`action_num` 的数量、分批建仓的规模与条件、价格区间、取整方式、仓位比例——都必须由对应 subagent 在读完其规则与研究包后自行得出。主 Agent 不得在 prompt 中写入任何结论、数字、比例、取整或价格引导或者分析规则、分析方法等，即使是为了「确保结果正确」；正确的产出只能来自 subagent 按规则自主推理，而非主 Agent 的干预。违反时，输出看似正确也属于越界。

## 4. 阶段 A：建立 P0

### A1. 确定日期

用户明确指定 `YYYY-MM-DD` 时使用该日期，否则使用最近一个已经产生收盘数据的交易日。
主agent再给不同subaget比如bull bear juror和finalizer派发任务时，必须显式告诉它当前分析的日期

### A2. 主 Agent读取

按顺序读取：

1. `03_agent_input.md`
2. `02_basic_snapshot_payload.json`
3. `01_global_context.md`
4. `_analysis_index.json`（存在时）
5. 热点主题和板块热度文件（存在时）
6. `data/agent_data/book-fixed_tracked/latest_decision_snapshot.json`（存在时）

不要打开任何 `04_stock_research/*_research.md`。

快照仅用于识别各股票上次结论、待验证事实和遗留风险，不能替代当天资料，也不得把其中历史动作、数量或价格计划当作当天直接指令。

### A3. 输出 P0 并暂停

按 `03_agent_input.md` 完成系统风险判断和 P0 选择，向用户输出：

- 系统风险结论；
- P0 股票 symbol、名称和触发原因；
- P0 数量和建议批次；
- P1 未进入深研的说明。

立即暂停。未得到用户对 P0 的明确确认前，不创建任何辩论 Agent。

## 5. 阶段 B：逐股辩论

对用户确认后的每只 P0 股票执行本节。不同股票可以并行，同一股票内部必须按阶段顺序执行。

### B0. 并发调度（单阶段每批最多 10 个）

主 Agent 在同一阶段每批最多同时启动 **10 个 subagent**。若当天该阶段需要超过 10 个，必须按批次执行：上一批全部结束、目标文件全部落盘并校验通过后，才能启动下一批。不得超过 10 个，以避免平台资源竞争导致任务空返回、写错日期目录或文件缺失。

批次只按平台并发上限拆分，不改变研究逻辑；不得在同一股票内部跨阶段混跑，也不得在前一批产物未核验时启动后一批。

- 阶段一（Opening）：全部 P0 股票 × Bull + Bear = `2 × N` 个 subagent，按每批最多 10 个启动；
- 阶段二（Rebuttal）：复用原 Bull/Bear 会话，全部 P0 × 2 = `2 × N` 个 follow-up，按每批最多 10 个唤醒；
- 阶段三（Juror）：全部 P0 × 3 名独立 Juror = `3 × N` 个 subagent，按每批最多 10 个启动；
- 阶段四（Finalizer）：全部 P0 × 1 = `N` 个 finalizer，按每批最多 10 个启动。

示例：8 只 P0 的 Opening/Rebuttal 各 16 个，拆成 10+6 两批；Juror 为 24 个，拆成 10+10+4 三批；Finalizer 为 8 个，可一批完成。16 只 P0 时，Opening/Rebuttal 各 32 个，拆成 10+10+10+2 四批；Juror 为 48 个，拆成 10+10+10+10+8 五批；Finalizer 为 16 个，拆成 10+6 两批。

每一阶段必须等该阶段全部 P0 的产物落盘并校验通过后，才统一进入下一阶段；阶段之间不得混跑。单只股票的 `opening → rebuttal → jury → aggregate → finalizer` 内部顺序仍必须严格保持，只是多只股票之间全程并行。

**所有写 JSON 的 subagent（Bull/Bear/Rebuttal/Juror/Finalizer）在落盘任何 JSON 前，必须先阅读 `references/json-writing-guide.md` 并按其中「提交前强制自检」用命令校验文件后再回传。**

### B1. 准备目录

```bash
source /home/zhangbeiqing/venv/ai_stock/bin/activate
python scripts/manage_debate.py prepare \
  --date {date} \
  --book-type fixed_tracked \
  --symbol {symbol}
```

该命令只做三件事：

1. 检查 `data/skill_runs/{date}/fixed_tracked/` 已存在；
2. 解析股票名称并生成“名称_标准代码”目录名；
3. 创建各角色互不重叠的空目录。

它不创建 `opening.json`、`rebuttal.json`、`ballot.json`、`vote_summary.json` 或 `stock_verdict.json`，也不会覆盖已有结果。重复执行是安全的。

命令完成后立即存在的目录：

```text
debate/{stock_name}_{symbol}/
├── advocates/
│   ├── bull/
│   └── bear/
├── jury/
│   ├── juror_01/
│   ├── juror_02/
│   └── juror_03/
└── final/
```

后续阶段才分别生成：

- Bull/Bear opening 阶段：各自的 `opening.json`；
- Bull/Bear rebuttal 阶段：各自的 `rebuttal.json`；
- Jury 阶段：各 Juror 的 `ballot.json`；
- `aggregate` 命令：`final/vote_summary.json`；
- finalizer：`final/stock_verdict.json`。

提前创建空目录只是为了让主 Agent 在派单前锁定每个角色的唯一输出位置。每个路径只有一个逻辑写入者，不增加 `{agent_id}` 子目录。

### B2. 并行创建 Bull 和 Bear

各角色把结果写入现有字段，不增加 JSON 字段。Bull/Bear opening 必须创建为 `fixed-tracked-advocate-luna`。

Bull 运行时 prompt：

```text
你担任 {symbol} {stock_name} 在 {date} 的 Bull advocate。

直接完整读取：
1. .codex/skills/auto-trading-fixed-tracked/references/debate-bull.md
2. data/skill_runs/{date}/fixed_tracked/03_stock_analysis_input.md
3. data/skill_runs/{date}/fixed_tracked/01_global_context.md
4. data/selection_runs/{date}/06_hot_news_state.json（存在时）
5. data/selection_runs/{date}/05_board_heat_digest.json（存在时）
6. 当前股票唯一的 04_stock_research 研究包

只允许写入：
data/skill_runs/{date}/fixed_tracked/debate/{stock_name}_{symbol}/advocates/bull/opening.json

opening 必须遵守财报前盈利推演协议；不得用“等待财报”替代可完成的高频经营分析。

禁止读取其他股票研究包，禁止修改其他辩论文件或 05_decision.json。
完成后只回传文件路径。
```

Bear 使用相同模板，但：

- reference 改为 `debate-bear.md`；
- 角色改为 Bear；
- 输出改为 `advocates/bear/opening.json`。

等待两份 opening 都存在且 JSON 可解析后再继续。

### B3. 让原 Bull 和 Bear 完成 Rebuttal

**Bull 和 Bear 是每只 P0 固定的两个 subagent 会话，必须在整个辩论流程中复用同一个会话，不得在任一阶段重新创建全新 Agent。**

- Opening 阶段创建 Bull、Bear 两个 Agent 后，保存其 task_id；
- Rebuttal 阶段必须**唤醒原 Bull / 原 Bear 的同一会话**（复用其 task_id）发送 follow-up：
  - 反驳 Bear opening → 唤醒原 Bull 会话；
  - 反驳 Bull opening → 唤醒原 Bear 会话；
- 这样做的好处是：每个角色复用 opening 阶段已经读取的研究包、共同规则和联网核验上下文，rebuttal 不必重读全部材料，论点更连贯、更省上下文。

Bull follow-up：

```text
继续担任原 Bull。直接读取：
1. .codex/skills/auto-trading-fixed-tracked/references/debate-rebuttal.md
2. 你自己的 bull/opening.json
3. 对方的 bear/opening.json

只允许写入 bull/rebuttal.json。不要修改 opening 或其他文件。
完成后只回传文件路径。
```

Bear follow-up 对称执行（唤醒原 Bear 会话）。

若平台确实无法继续原 Agent 会话，才新建 rebuttal Agent；新 Agent 必须额外读取本方角色 reference、双方 opening、共同个股输入和本股研究包，并在汇报中注明是新建会话。

subagent 返回空结果或未落盘文件时，先检查是否为原会话未完成，用原 task_id 续上该会话补写，而不是立即新起一个 Agent。

等待两份 rebuttal 都存在且 JSON 可解析后再继续。

### B4. 并行创建三个 Juror

创建三个相互独立的 `fixed-tracked-adjudicator-terra` Agent。每个 Juror 使用相同输入，但写入不同目录：

```text
你担任 {symbol} {stock_name} 的独立 {juror_id}。

直接完整读取：
1. .codex/skills/auto-trading-fixed-tracked/references/debate-juror.md
2. data/skill_runs/{date}/fixed_tracked/03_stock_analysis_input.md
3. data/skill_runs/{date}/fixed_tracked/01_global_context.md
4. 当前股票唯一的 04_stock_research 研究包
5. bull/opening.json
6. bull/rebuttal.json
7. bear/opening.json
8. bear/rebuttal.json

不得读取 jury/ 下其他 Juror 的文件。
只允许写入：
data/skill_runs/{date}/fixed_tracked/debate/{stock_name}_{symbol}/jury/{juror_id}/ballot.json

投票前必须审计双方是否完成财报前盈利推演；`HOLD`/`FLAT` 不得仅因财报尚未发布。

禁止修改其他文件或 05_decision.json。完成后只回传文件路径。
```

三个 Juror 的 agent 上下文必须彼此独立。等待三份 ballot 完成。

每份 ballot 同时包含：

- `action_type`：参与多数票聚合；
- `price_impression`：该 Juror 在完整辩论后的独立价格判断；
- `reason`：同时解释动作和价格印象，以及二者看似不一致时的原因。

### B5. 本地聚合投票

```bash
python scripts/manage_debate.py aggregate \
  --date {date} \
  --book-type fixed_tracked \
  --symbol {symbol} \
  --position-shares {当前实际持股数量}
```

聚合规则：

- 任一动作获得至少两票：多数动作；
- 三票分散且无人达到两票：`no_majority`；
- 本地脚本根据多数票和 `position_shares` 写出唯一 `resolved_action`；
- 本地脚本把三名 Juror 的 `price_impression` 原样保存到 `price_impression_votes`，不计算多数、中位数或最终价格标签；
- 不按自报置信度加权；
- Agent无权手写或修改 `vote_summary.json`。

### B6. 生成唯一 Stock Verdict

为当前股票创建一个独立 `fixed-tracked-adjudicator-terra` finalizer。不得复用任一 Juror，避免某名 Juror 在整理最终底稿时放大自己的选票。finalizer 不是第四名裁判，无权改变 `vote_summary.resolved_action`。

```text
担任 {symbol} {stock_name} 的唯一 finalizer。

直接完整读取：
1. .codex/skills/auto-trading-fixed-tracked/references/debate-finalizer.md
2. configs/prompt_flow/fixed_tracked/stock_decision.schema.json
3. configs/prompt_flow/fixed_tracked/stock_decision.example.json
4. data/skill_runs/{date}/fixed_tracked/03_stock_analysis_input.md
5. 当前股票唯一的 04_stock_research 研究包
6. 双方 opening 和 rebuttal
7. 三份 Juror ballot
8. final/vote_summary.json

只允许写入：
data/skill_runs/{date}/fixed_tracked/debate/{stock_name}_{symbol}/final/stock_verdict.json

禁止修改投票、辩论文件或 05_decision.json。完成后只回传动作和文件路径。
```

完成后校验：

```bash
python scripts/manage_debate.py validate \
  --date {date} \
  --book-type fixed_tracked \
  --symbol {symbol} \
  --require-verdict
```

校验失败时先让唯一文件所有者修复格式或缺失内容。不要让主 Agent静默代改研究结论，也不要跳过该股票。

## 6. 阶段 C：组合层复核与第二次暂停

所有 P0 完成后，主 Agent只读取每只股票的：

- `final/vote_summary.json`
- `final/stock_verdict.json`

检查：

- BUY 总额是否超过现金；
- 多只 BUY 是否造成单一行业或主题过度集中；
- SELL 数量是否超过现有持仓；
- 数量是否符合市场交易单位；
- 危机模式约束是否被违反；
- 多个 verdict 的动作和仓位是否互相冲突。

主 Agent可以提出组合层 `action_num` 调整建议，但不得重写双方论点、Juror 票数、最终动作或个股事实。向用户展示：

- 每只股票三票分布；
- 多数状态；
- final action、数量和主要理由；
- `no_majority` 项；
- 建议的组合层数量调整。

立即暂停。此时不得存在新生成的 `05_decision.json`。

## 7. 阶段 D：人工确认后生成 05

只有用户明确回复“OK 生成决策”或同义确认后：

1. 如用户确认了组合层数量调整，优先让原 finalizer 通过 follow-up 只更新对应 `stock_verdict.json` 的 `action_num`、仓位数字和 `recommended_action`；不要修改研究事实、辩论、票数或最终动作。无法继续原 finalizer 时暂停并报告，不由主 Agent静默接管该文件。
2. 运行：

```bash
python scripts/merge_subagent_decisions.py \
  --date {date} \
  --book-type fixed_tracked \
  --source debate
```

3. 更新 `05_decision.json` 顶层：
   - `summary_date`
   - `system_risk_notes`
   - `system_focus_items`
4. 暂停并让用户检查 `05_decision.json`。

不要因为生成了 `05_decision.json` 就自动执行交易。

## 8. 阶段 E：交易后处理

只有用户再次明确确认真实执行后，才按项目现有规则调用：

```bash
python scripts/run_post_trade.py \
  --date {date} \
  --book-type fixed_tracked \
  --signature book-fixed_tracked
```

成功后，除更新 `data/agent_data/book-fixed_tracked/decision_summary.json` 外，还会更新同目录的 `latest_decision_snapshot.json`。该文件按股票保留 `decision_summary.json` 中 `end_date` 最新的一条完整分析结果；下一交易日主 Agent 在阶段 A 选择 P0 前应读取它，不得把其中历史动作或价格计划当作当天直接执行指令。

不得修改真实交易、价格引用、仓位和最小交易单位规则。

## 9. 调度和失败规则

- 每只股票常规使用 6 个逻辑 Agent：Bull、Bear、Juror 01、02、03、finalizer。
- 常规共 8 个 turn：2 opening、2 rebuttal follow-up、3 ballot、1 finalizer。
- **所有 P0 股票必须完成辩论，但单阶段每批并发不得超过 10 个**：Opening、Rebuttal、Juror、Finalizer 分别按第 5 节 B0 的批次规则执行。每批结束后必须检查目标文件数量、JSON 可解析性和 Schema，再进入下一批；每只股票内部的 `opening → rebuttal → jury → aggregate → finalizer` 阶段顺序仍然必须严格保持。
- 同一 symbol 内严格执行 `opening → rebuttal → jury → aggregate → finalizer`。
- 文件不存在、JSON 损坏或校验失败时，不得默默跳过。
- 纯 JSON 格式错误由原文件所有者修复；结论冲突向用户报告。
- 旧 `subagent_result` 流程只作为历史兼容，不在新 fixed_tracked 辩论流程中使用。

## 10. 主Agent绝对不能把任何自己的主观感受传递给subagent!!! 这是被绝对禁止的！！在生成给subagent的prompt的时，主agent只是一个客观的透传工具，绝对不能擅自提醒subagent或者告诉subagent
自己的观点！！！绝对禁止
