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
- 个股共同投资规则：当天 `03_stock_analysis_input.md`。
- Bull opening：[references/debate-bull.md](references/debate-bull.md)。
- Bear opening：[references/debate-bear.md](references/debate-bear.md)。
- 双方 rebuttal：[references/debate-rebuttal.md](references/debate-rebuttal.md)。
- Juror 投票：[references/debate-juror.md](references/debate-juror.md)。
- 单股 final verdict：[references/debate-finalizer.md](references/debate-finalizer.md)。
- 最终单股字段：`configs/prompt_flow/fixed_tracked/stock_decision.schema.json`。

主 Agent必须让目标角色直接读取对应文件。禁止在运行时 prompt 中复制、概括或改写这些规则。

## 4. 阶段 A：建立 P0

### A1. 确定日期

用户明确指定 `YYYY-MM-DD` 时使用该日期，否则使用最近一个已经产生收盘数据的交易日。目录中的日期就是“要分析的交易日”，不是下一交易日。

### A2. 主 Agent读取

按顺序读取：

1. `03_agent_input.md`
2. `02_basic_snapshot_payload.json`
3. `01_global_context.md`
4. `_analysis_index.json`（存在时）
5. 热点主题和板块热度文件（存在时）

不要打开任何 `04_stock_research/*_research.md`。

### A3. 输出 P0 并暂停

按 `03_agent_input.md` 完成系统风险判断和 P0 选择，向用户输出：

- 系统风险结论；
- P0 股票 symbol、名称和触发原因；
- P0 数量和建议批次；
- P1 未进入深研的说明。

立即暂停。未得到用户对 P0 的明确确认前，不创建任何辩论 Agent。

## 5. 阶段 B：逐股辩论

对用户确认后的每只 P0 股票执行本节。不同股票可以并行，同一股票内部必须按阶段顺序执行。

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
2. 校验并标准化 symbol 目录名；
3. 创建各角色互不重叠的空目录。

它不创建 `opening.json`、`rebuttal.json`、`ballot.json`、`vote_summary.json` 或 `stock_verdict.json`，也不会覆盖已有结果。重复执行是安全的。

命令完成后立即存在的目录：

```text
debate/{symbol}/
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
data/skill_runs/{date}/fixed_tracked/debate/{symbol}/advocates/bull/opening.json

禁止读取其他股票研究包，禁止修改其他辩论文件或 05_decision.json。
完成后只回传文件路径。
```

Bear 使用相同模板，但：

- reference 改为 `debate-bear.md`；
- 角色改为 Bear；
- 输出改为 `advocates/bear/opening.json`。

等待两份 opening 都存在且 JSON 可解析后再继续。

### B3. 让原 Bull 和 Bear 完成 Rebuttal

优先向原 Bull 和 Bear 发送 follow-up，不创建新 Agent。

Bull follow-up：

```text
继续担任原 Bull。直接读取：
1. .codex/skills/auto-trading-fixed-tracked/references/debate-rebuttal.md
2. 你自己的 bull/opening.json
3. 对方的 bear/opening.json

只允许写入 bull/rebuttal.json。不要修改 opening 或其他文件。
完成后只回传文件路径。
```

Bear follow-up 对称执行。

若平台无法继续原 Agent，才新建 rebuttal Agent；新 Agent必须额外读取本方角色 reference、双方 opening、共同个股输入和本股研究包。

等待两份 rebuttal 都存在且 JSON 可解析后再继续。

### B4. 并行创建三个 Juror

创建三个相互独立的 Agent。每个 Juror 使用相同输入，但写入不同目录：

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
data/skill_runs/{date}/fixed_tracked/debate/{symbol}/jury/{juror_id}/ballot.json

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

为当前股票创建一个独立 finalizer。不得复用任一 Juror，避免某名 Juror 在整理最终底稿时放大自己的选票。finalizer 不是第四名裁判，无权改变 `vote_summary.resolved_action`。

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
data/skill_runs/{date}/fixed_tracked/debate/{symbol}/final/stock_verdict.json

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

不得修改真实交易、价格引用、仓位和最小交易单位规则。

## 9. 调度和失败规则

- 每只股票常规使用 6 个逻辑 Agent：Bull、Bear、Juror 01、02、03、finalizer。
- 常规共 8 个 turn：2 opening、2 rebuttal follow-up、3 ballot、1 finalizer。
- 根据平台并发上限分批处理股票，不要求 10 只股票同时启动 60 个 Agent。
- 同一 symbol 内严格执行 `opening → rebuttal → jury → aggregate → finalizer`。
- 文件不存在、JSON 损坏或校验失败时，不得默默跳过。
- 纯 JSON 格式错误由原文件所有者修复；结论冲突向用户报告。
- 旧 `subagent_result` 流程只作为历史兼容，不在新 fixed_tracked 辩论流程中使用。
