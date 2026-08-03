# fixed_tracked 辩论与 Prompt 分层详细设计

## 1. 文档目的

本文是 `fixed_tracked` 固定跟踪池从“单个 subagent 直接形成单股决策”迁移到“管理者协调的多 Agent 辩论、投票和组合复核”模式的实现设计。

本文必须能够被一个不了解历史讨论的 AI 或开发者直接使用。实施者应以本文描述的职责边界、文件契约和流程顺序为准，不应自行恢复旧版“所有 Agent 共读一份万能 Prompt”的设计。

## 2. 当前系统背景

日常交易日前置数据由 Python 流水线生成到：

```text
data/skill_runs/{date}/fixed_tracked/
├── 01_global_context.md
├── 02_basic_snapshot_payload.json
├── 03_agent_input.md
└── 04_stock_research/
```

当前交易由 `.codex/skills/auto-trading-fixed-tracked/SKILL.md` 触发。旧版流程由主 Agent 选出 P0 股票，再为每只 P0 股票创建一个 subagent。subagent 读取全部规则和材料后，直接写入：

```text
subagent_result/{stock_name}_{symbol}_{date}_decision.json
```

随后 `scripts/merge_subagent_decisions.py` 将单股结果合并成 `05_decision.json`。

固定跟踪池已经吸收长期候选池；本设计不恢复独立的 `long_book` 交易流程。`short_book` 暂不迁移到本文的辩论模式。

## 3. 现状问题

### 3.1 Prompt 规则有多个所有者

同一条规则目前可能同时出现在：

- `.codex/skills/auto-trading-fixed-tracked/SKILL.md`
- `configs/prompt_flow/skill_flow.json`
- `services/pipeline/steps/build_agent_input.py` 的硬编码 USER_QUERY
- 主 Agent 临时组织的 subagent prompt
- 生成后的 `03_agent_input.md`

这导致字段数量、文件回传方式、联网要求和人工确认时机发生漂移。

### 3.2 `03_agent_input.md` 混合了不同角色的任务

旧版 `03_agent_input.md` 同时包含：

- 主 Agent 的宏观研判、P0 选择和用户暂停点；
- subagent 的个股分析、联网搜索和文件写入规则；
- 完整 `05_decision.json` 字段与示例；
- 当天组合数据和股票池。

Bull、Bear、Juror 等个股角色不应读取 P0 调度和用户交互规则。主 Agent也不应为了调度而加载完整个股底稿规范。

### 3.3 并行写入边界不清晰

若多个 Agent 同时分析同一只股票并直接修改 `05_decision.json` 或同一单股文件，会产生覆盖、截断和非确定性结果。

### 3.4 旧人工确认时机不一致

旧 Skill 先运行合并脚本写入 `05_decision.json`，随后又声明“只有用户确认后才生成 `05_decision.json`”。新流程必须把单股 verdict 与最终交易文件分开：

1. 用户确认前只生成辩论产物和 `stock_verdict.json`。
2. 用户确认后才合并生成 `05_decision.json`。

## 4. 设计目标

### 4.1 必须实现

- 每类规则只有一个唯一来源。
- 保留旧的 `01_global_context.md`、`02_basic_snapshot_payload.json`、`03_agent_input.md` 和 `04_stock_research/` 文件名。
- 新增 `03_stock_analysis_input.md`，隔离主 Agent 和个股角色的 Prompt。
- 每只 P0 股票使用一个 Bull、一个 Bear、三个 Juror。
- Bull 和 Bear 优先复用原 Agent 完成 rebuttal。
- 每个 Agent 只写自己拥有的文件。
- 三个 Juror 独立投票，任何动作获得至少两票即为多数。
- 三票分散、无人达到两票时，不由 finalizer 自由选择激进行为；有持仓回退为 `HOLD`，无持仓回退为 `FLAT`。
- 最终单股 decision 由唯一 finalizer 写入。
- `05_decision.json` 只在用户明确确认后生成。
- 保留旧 `subagent_result` 合并方式作为兼容入口。

### 4.2 暂不实现

- 不引入 LangGraph。
- 不新增第三方依赖。
- 不迁移 `short_book`。
- 不恢复独立 `long_book`。
- 不修改真实交易执行、仓位计算、成交价格和最小交易单位规则。
- 不让投票按模型自报置信度加权。
- 不建立 `manifest.json` 或动态 Prompt 插件系统。

## 5. 核心设计原则

### 5.1 按职责分层，不按文件数量追求“单 Prompt”

存在多份 Prompt 本身没有问题。问题是同一条规则出现在多份 Prompt 中。

规则归属如下：

| 规则类型 | 唯一来源 | 读取者 |
| --- | --- | --- |
| 仓库操作和安全约束 | `AGENTS.md` | 全部 Agent |
| 状态机、暂停点、创建 Agent、文件所有权、失败处理 | fixed Skill | 主 Agent |
| 核心角色定位、投资哲学、风险偏好、仓位与交易纪律 | `investment_policy.md`，同时编译进两份 03 | 主 Agent、Bull、Bear、Juror、finalizer |
| 宏观与 P0 筛选方法 | `main_policy.md` 编译后的 `03_agent_input.md` | 主 Agent |
| 通用联网搜索、来源分级和证据准入 | `configs/research/web_research_policy.md`，编译进 `03_stock_analysis_input.md` | Bull、Bear、Juror、finalizer |
| 通用个股研究、联网硬触发条件、历史记忆投影、动作语义 | `stock_analysis_policy.md`，与共享策略共同编译进 `03_stock_analysis_input.md` | Bull、Bear、Juror、finalizer |
| Bull/Bear/Rebuttal/Juror/finalizer 的角色差异 | Skill `references/` 对应文件 | 对应角色 |
| 单股最终 JSON 结构和样例 | `stock_decision.schema.json` + `stock_decision.example.json` | finalizer、校验器 |
| 当天事实 | `01`、`02`、`04`、热点和板块文件 | 按角色读取 |
| symbol、日期、输入路径、输出路径 | 主 Agent 的运行时派单 prompt | 单个目标 Agent |

### 5.2 主 Agent 不复述角色规则

主 Agent 的运行时 prompt 只能包含：

- 角色；
- 日期；
- symbol 和股票名；
- 必须读取的文件路径；
- 唯一允许写入的文件路径；
- 完成后回传的短确认格式。

角色规则由 subagent 直接读取固定 reference。主 Agent 不总结或复制 reference 内容，避免较弱模型转述时发生语义漂移。

### 5.3 生成文件不是维护源

以下是生成产物，不允许手工维护：

- `03_agent_input.md`
- `03_stock_analysis_input.md`

静态规则只能修改：

- `configs/prompt_flow/fixed_tracked/investment_policy.md`
- `configs/prompt_flow/fixed_tracked/main_policy.md`
- `configs/prompt_flow/fixed_tracked/stock_analysis_policy.md`
- `configs/research/web_research_policy.md`
- `.codex/skills/auto-trading-fixed-tracked/SKILL.md`
- `.codex/skills/auto-trading-fixed-tracked/references/*.md`
- `configs/prompt_flow/fixed_tracked/stock_decision.schema.json`
- `configs/prompt_flow/fixed_tracked/stock_decision.example.json`

## 6. Prompt 文件设计

### 6.1 `investment_policy.md`

这是 fixed_tracked 核心投资策略的单一来源，同时编译进主 Agent 的 `03_agent_input.md` 和个股角色的 `03_stock_analysis_input.md`。它定义：

- “稳如老苟，动若脱兔”的买卖非对称纪律；
- 事实先于叙事、一致预期只作参考；
- 价值投资、安全边际和盈利预测可靠度；
- 集中但分批、单股目标仓位约 20% 仅作参考；
- 有效首批通常不低于总资产约 2%，但不能倒逼买入；
- 卖出主动裁量、交易单位和黄金 ETF 例外。

它不得包含 P0、角色立场、文件路径、JSON 格式或 Agent 调度规则。修改上述投资哲学时只改这一份文件，不能分别在主 Prompt 和角色 Prompt 中维护两套版本。

### 6.2 `main_policy.md`

只定义主 Agent 如何：

- 理解当天账本和组合；
- 检查 S 级系统性风险；
- 使用 `02_basic_snapshot_payload.json` 做全池快扫；
- 使用分析索引防止股票长期得不到深研；
- 决定 P0；
- 向用户说明 P0 触发原因。

不得包含：

- Bull/Bear/Juror 的工作方法；
- rebuttal 格式；
- 单股 verdict 字段列表；
- subagent 写入路径；
- 如何聚合票数；
- `05_decision.json` 示例。

### 6.3 `stock_analysis_policy.md`

与 `investment_policy.md`、`web_research_policy.md` 共同生成个股输入；本文件只定义所有个股角色共同遵守的研究方法：

- 事实与推断分离；
- 完整历史永久保存，但研究包只读取不含过期执行计划的投资记忆投影；
- 财报披露窗口和时效约束；
- 本地材料优先、必要时联网补证；
- fixed_tracked 联网硬触发条件和上一轮待核验事项闭环；
- 价值、盈利可靠度、周期性、估值和量价的综合判断；
- `BUY`、`SELL`、`HOLD`、`FLAT` 的共同语义；
- `HOLD/FLAT` 不应携带未来价格买入指令；
- `price_impression` 只能由 finalizer 在辩论和投票之后形成。

不得包含：

- P0 选择；
- 主 Agent 暂停点；
- 创建 Agent 的方式；
- 任何角色的立场；
- 辩论目录；
- 完整单股 JSON 示例。

### 6.3.1 `web_research_policy.md`

这是财报研究、产业研究和 fixed_tracked 共用的联网搜索与证据准入单一来源。它定义百炼只负责语义召回、来源等级、百家号等低等级来源的硬性禁用、同源转载识别和原始来源追溯。低等级来源只能提供线索或市场情绪，不能进入事实、预测、估值假设或交易结论。

### 6.4 Role References

Role reference 只描述该角色相对共同规则的差异。

#### Bull

- 从“维持或增加风险敞口”的角度找出全部重要理由。
- 每条理由作为 `arguments` 数组的一行字符串。
- 理由中直接写清事实、推断和来源，不增加额外结构字段。

输出：

```json
{
  "arguments": [
    "理由一",
    "理由二"
  ]
}
```

#### Bear

- 从“减少或不建立风险敞口”的角度找出全部重要理由。
- 使用与 Bull 相同的 opening 格式。

#### Rebuttal

- 阅读对方全部 opening。
- 逐条审核，只反驳确实有异议的论点。
- `original_argument` 必须原样复制对方论点，建立明确对应关系。
- 没有异议时输出空数组，不为凑数量制造反驳。

输出：

```json
{
  "rebuttals": [
    {
      "original_argument": "对方原论点",
      "rebuttal": "审核与反驳"
    }
  ]
}
```

#### Juror

- 三名 Juror 相互独立，不读取其他 Juror 的 ballot。
- 必须阅读原始证据和完整辩论，不能只看摘要。
- 对关键时效冲突可以进行少量定向联网，不重复 Bull/Bear 的广泛搜索。
- 输出一个动作、一个独立价格印象和一个共同解释理由。

输出：

```json
{
  "action_type": "BUY",
  "price_impression": "偏低估",
  "reason": "支持该动作和价格印象的核心权衡，并解释二者之间的关系"
}
```

#### Finalizer

- 每只股票使用一个独立 finalizer，不复用任一 Juror。
- 读取投票汇总、三份 ballot、双方 opening/rebuttal、共同个股规则、原始研究包、JSON Schema 和完整样例。
- 不得改变多数票动作。
- 无多数票时按持仓状态回退为 `HOLD` 或 `FLAT`。
- 负责形成最终 `price_impression`、执行建议和完整单股 decision。
- 审查三名 Juror 的价格印象和理由；不按多数或中位数机械聚合，而是比较事实、盈利假设、估值口径、周期判断和来源质量。
- `court.verdict` 只记录投资判断、证据权衡、核心假设和失效条件，不写价格、数量或分批执行计划。
- `recommended_action` 可以详细规定下一交易日如何执行，但只对下一个交易日有效，不进入下一轮研究 Prompt。

## 7. 生成产物

### 7.1 保留产物

```text
01_global_context.md
02_basic_snapshot_payload.json
03_agent_input.md
04_stock_research/
```

### 7.2 新增产物

```text
03_stock_analysis_input.md
```

`03_agent_input.md` 只服务主 Agent；`03_stock_analysis_input.md` 只服务个股角色。

### 7.3 辩论目录

每只 P0 股票使用标准 symbol 作为目录名：

```text
data/skill_runs/{date}/fixed_tracked/debate/
└── {symbol}/
    ├── advocates/
    │   ├── bull/
    │   │   ├── opening.json
    │   │   └── rebuttal.json
    │   └── bear/
    │       ├── opening.json
    │       └── rebuttal.json
    ├── jury/
    │   ├── juror_01/
    │   │   └── ballot.json
    │   ├── juror_02/
    │   │   └── ballot.json
    │   └── juror_03/
    │       └── ballot.json
    └── final/
        ├── vote_summary.json
        └── stock_verdict.json
```

不使用 `{agent_id}` 子目录，原因是每个角色每只股票只有一个逻辑写入者。固定路径更容易校验，也不会出现 Bull 与 Bear 的错误配对。

## 8. 文件所有权和并行安全

| 文件 | 唯一写入者 |
| --- | --- |
| `bull/opening.json` | Bull |
| `bull/rebuttal.json` | Bull 或 Bull 的 follow-up |
| `bear/opening.json` | Bear |
| `bear/rebuttal.json` | Bear 或 Bear 的 follow-up |
| `juror_01/ballot.json` | Juror 01 |
| `juror_02/ballot.json` | Juror 02 |
| `juror_03/ballot.json` | Juror 03 |
| `final/vote_summary.json` | 本地确定性聚合脚本 |
| `final/stock_verdict.json` | 唯一 finalizer |
| `05_decision.json` | 用户确认后的合并脚本 |

所有 Agent 都禁止直接修改：

- 对方的文件；
- 其他 Juror 的 ballot；
- `vote_summary.json`；
- `05_decision.json`。

本地脚本写 JSON 时使用“临时文件 + 原子替换”，避免读到半写入文件。

## 9. 单股工作流

### Phase 0：准备目录

主 Agent 对用户确认后的每只 P0 股票运行本地准备命令，只创建目录，不创建或覆盖任何 Agent 结果。

### Phase 1：Opening

并行创建：

- Bull；
- Bear。

两者独立读取共同规则和原始材料，分别写 opening。

### Phase 2：Rebuttal

等两份 opening 都存在后：

- 把 Bear opening 路径发送给原 Bull；
- 把 Bull opening 路径发送给原 Bear。

优先继续原 Agent，因为它保留自己的研究过程。若平台不支持 follow-up，才创建新的 rebuttal Agent，并要求其同时读取本方 opening、对方 opening 和对应角色 reference。

### Phase 3：Jury

等两份 rebuttal 完成后，并行创建三个 Juror。三个 Juror：

- 使用相同输入；
- 不共享上下文；
- 不查看其他 ballot；
- 各自写唯一 ballot。

### Phase 4：Vote Aggregation

本地脚本读取三个 ballot：

- 某动作获得至少两票：`status=majority`，记录 `majority_action`。
- 三票分别投给三个不同动作：`status=no_majority`，`majority_action=null`。
- 主 Agent把本账本当前实际持股数量作为 `position_shares` 传给聚合命令。
- 聚合脚本生成唯一 `resolved_action`：有多数时等于多数动作；无多数且有持仓时为 `HOLD`；无持仓时为 `FLAT`。
- 聚合脚本把三名 Juror 的价格印象原样写入 `price_impression_votes`，但不生成 `resolved_price_impression`，也不计算多数或中位数。

投票不使用 `confidence_score` 加权。每名 Juror 一票，避免模型通过自报高置信度获得额外权力。

### Phase 5：Finalization

finalizer 生成 `stock_verdict.json`：

- `action_type` 必须等于本地聚合脚本生成的 `resolved_action`，finalizer 不再自行解释持仓状态或覆盖动作。
- finalizer 比较三名 Juror 的价格印象及其依据后形成最终 `price_impression`；有分歧时按证据质量处理，不机械按票数或标签顺序聚合。
- finalizer 可以决定 `action_num`、执行节奏和风险条件，但不能改变上述动作。

### Phase 6：组合层复核

所有 P0 股票完成后，主 Agent只读取：

- 每只股票的 `vote_summary.json`；
- 每只股票的 `stock_verdict.json`；
- `03_agent_input.md` 中的组合数据。

主 Agent检查：

- 多只 BUY 是否导致现金不足；
- 单一行业或主题是否过度集中；
- SELL 数量是否超过现有持仓；
- BUY/SELL 数量是否符合市场交易单位；
- S 级风险约束是否被违反；
- 多只股票的动作是否彼此矛盾。

主 Agent不直接覆盖 finalizer 的研究结论。如果只需调整执行数量，应在向用户汇报时明确提出“组合层数量调整建议”。用户确认后，优先让原 finalizer 通过 follow-up 按确认结果更新相应 `stock_verdict.json` 的 `action_num` 和 `recommended_action`，不得改写辩论、事实和投票内容。若无法继续原 finalizer，应暂停并向用户报告，不由主 Agent静默接管单股文件。

### Phase 7：人工确认

主 Agent向用户展示：

- P0 股票列表；
- 每只股票的 3 票分布；
- 最终动作；
- 主要理由；
- 组合层数量调整建议；
- 无多数票或低证据质量项目。

到此暂停。不得生成或修改 `05_decision.json`。

### Phase 8：生成 `05_decision.json`

只有用户明确回复“OK 生成决策”或同义确认后，才运行合并脚本，从：

```text
debate/*/final/stock_verdict.json
```

生成 `05_decision.json`，并补充：

- `summary_date`
- `system_risk_notes`
- `system_focus_items`

之后再次暂停，等待用户检查交易文件。真实交易仍由后续人工确认和 `run_post_trade.py` 负责。

## 10. 投票规则

### 10.1 合法动作

```text
BUY / SELL / HOLD / FLAT
```

### 10.2 多数定义

三个 Juror 中任意动作获得两票或三票，即为多数。

示例：

```text
BUY, BUY, HOLD -> BUY
SELL, SELL, HOLD -> SELL
HOLD, HOLD, BUY -> HOLD
BUY, SELL, HOLD -> 无多数
BUY, SELL, FLAT -> 无多数
```

### 10.3 无多数处理

无多数代表证据和动作存在实质分歧，不应让 finalizer 任意挑选激进动作：

- 已持仓：`HOLD`
- 未持仓：`FLAT`

聚合命令必须显式接收本账本当前 `position_shares`，并把上述结果写为 `resolved_action`。后续 Schema 校验、finalizer 和合并脚本共同检查该字段。

人工可以在最终确认阶段否决或要求重审，但系统默认不自动扩大风险敞口。

### 10.4 为什么不按置信度加权

模型自报置信度缺乏跨 Agent 校准。使用加权票会鼓励模型通过提高自报分数扩大影响力，且难以解释。因此：

- 投票是一人一票；
- `confidence_score` 只属于最终研究底稿；
- 置信度不参与多数计算。

## 11. Agent 数量与调度

每只股票的常规逻辑 Agent 数：

- Bull：1
- Bear：1
- Juror：3
- Finalizer：1

合计 6 个逻辑 Agent。

调用轮次：

- Opening：2 次；
- Rebuttal follow-up：2 次；
- Jury：3 次；
- Finalizer：1 次。

常规合计 8 个 Agent turn、6 个逻辑 Agent。若平台不支持 follow-up，rebuttal 最多再增加两个逻辑 Agent。

处理 10 只股票时，不要求同时启动 50 个 Agent。主 Agent按平台并发上限分批调度：

1. 先对一批股票并行完成 Opening；
2. 完成该批 Rebuttal；
3. 并行完成 Jury；
4. 生成 verdict；
5. 再处理下一批。

每个 symbol 的阶段顺序固定，不同 symbol 之间可以并行。

## 12. 单股 Decision Schema

`stock_decision.schema.json` 是新辩论流程中单股 final verdict 的唯一字段契约。

必填字段：

1. `symbol`
2. `stock_name`
3. `scan`
4. `delta_summary`
5. `key_facts`
6. `inferences`
7. `court`
8. `price_impression`
9. `recommended_action`
10. `action_type`
11. `action_num`
12. `key_risks`
13. `next_day_watchlist`
14. `confidence_score`

删除的旧字段：

- `deep_analysis_date`：与顶层日期和目录日期重复；
- `history_anchor`：会递归复制上一轮判断并放大历史锚；
- `search_brief`：搜索证据应直接进入事实、推断和双方论点；
- `motion`：投票架构下与最终 `action_type` 重复；
- `price_target`：与当日 `recommended_action` 重复且容易成为过期价格锚。

字段中的数组不设置固定项目数量。`stock_decision.example.json` 提供完整样例，并通过单元测试验证始终符合 Schema。

## 12.1 历史记忆与过期执行计划

`stock_decisions.json` 继续完整保存所有历史决策，不删除 `recommended_action` 或旧字段。生成下一轮 `04_stock_research` 时，Python 使用 `get_stock_memory_context()` 构建研究记忆视图：

- 保留所有已确认 BUY/SELL 仓位变化及其 `delta_summary`、事实、推断、正反方理由和风险；
- 保留最近一次非仓位变化的投资逻辑复核；
- 保留上轮待核验事项；
- 排除历史 `recommended_action`、`price_target` 和其他过期执行计划；
- 旧格式 `court.verdict` 可能混有价格和加仓指令，因此只保留其 `court.pro/con`；新格式 verdict 按新职责可进入长期记忆。

这样既保留长期投资所需的“为什么买卖”记忆，又避免较弱模型把一周前的价格条件机械当成今日纪律。

## 13. 代码修改范围

### 13.1 Prompt 生成

修改：

- `prompts/agent_prompt.py`
- `services/prompting/system_prompt.py`
- `services/pipeline/steps/build_agent_input.py`
- `services/pipeline/daily_pipeline.py`

要求：

- Prompt 配置路径使用函数显式参数，不再通过进程级环境变量切换。
- fixed_tracked 默认从 Markdown policy 生成。
- 每日同时写出主 Agent 输入和个股共同输入。
- short_book 和旧 JSON prompt 配置继续可用。

### 13.2 Debate 管理

新增：

- `services/trading/debate_pipeline.py`
- `scripts/manage_debate.py`

提供：

- `prepare`：只校验账本目录和 symbol，并创建 Bull、Bear、三名 Juror、final 的空目录；不创建或覆盖任何 JSON；
- `aggregate`
- `validate`

### 13.3 合并

扩展：

- `scripts/merge_subagent_decisions.py`

保留默认旧模式，并新增从 debate final verdict 合并的显式选项。

### 13.4 Skill

重写：

- `.codex/skills/auto-trading-fixed-tracked/SKILL.md`

新增固定角色 references。Skill 只保留管理者状态机和必要的调用模板，不复制共同分析规则和 Schema。

## 14. 兼容策略

### 14.1 保持不变

- 现有 `01-04` 主文件名。
- `05_decision.json` 顶层格式。
- 真实交易脚本消费字段。
- `short_book` 旧流程。
- `merge_subagent_decisions.py` 默认读取 `subagent_result`。

### 14.2 新增

- `03_stock_analysis_input.md`
- `debate/` 目录
- debate 管理命令
- debate verdict 合并模式

### 14.3 旧数据

历史运行目录不迁移。旧的 `subagent_result` 可以继续使用原合并模式。只有新辩论运行使用 `debate/`。

## 15. 验证要求

### 15.1 静态验证

- fixed Skill frontmatter 和目录通过 `quick_validate.py`。
- Markdown policy 中不存在未解析占位符。
- 两份 03 都只包含一份渲染后的 `investment_policy.md`。
- JSON Schema 可解析。
- 新 Python 文件通过编译。

### 15.2 单元测试

至少覆盖：

- 三票中两票相同得到多数；
- 三票不同得到无多数；
- ballot 非法动作被拒绝；
- Agent 写入目录互不重叠；
- debate verdict 缺字段时校验失败；
- 旧 `subagent_result` 合并方式仍可用；
- debate verdict 可以在临时目录合并成 `05_decision.json`。

### 15.3 产物验证

在临时目录或最小输入上检查：

- `03_agent_input.md` 包含共享投资策略和主 Agent 政策，不包含个股角色规则；
- `03_stock_analysis_input.md` 不包含 P0、用户暂停或创建 subagent 指令；
- `prepare` 创建正确目录；
- `aggregate` 原子写入合法 `vote_summary.json`；
- 用户确认前不会生成 `05_decision.json`。

## 16. 评审重点

用户评审时重点检查：

1. `investment_policy.md` 是否完整保留原角色定位、投资风格和仓位纪律。
2. `main_policy.md` 的 P0 规则是否符合实际偏好。
3. `stock_analysis_policy.md` 的个股研究方法是否完整且没有重复定义投资风格。
4. Bull 与 Bear 的立场是否清楚但不过度诱导。
5. Rebuttal 是否逐条对应且没有多余字段。
6. Juror 是否真正独立，投票字段是否足够简洁。
7. finalizer 是否被严格限制为执行多数票，而不是重新做一次自由裁决。
8. 无多数回退 `HOLD/FLAT` 是否符合风险偏好。
9. 组合层是否需要在下一轮进一步拆成独立 portfolio allocator。
