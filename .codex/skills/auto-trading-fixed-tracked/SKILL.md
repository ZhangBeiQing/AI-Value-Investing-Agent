---
name: auto-trading-fixed-tracked
description: >
  固定股票池每日股票自动交易程序。Agent 读取用户已经准备好的固定股票池中的每日最新股票数据，根据用户规则详细分析股票后，生成每日交易决策 JSON，经人工确认后，再执行交易、然后生成每日操盘总结，最后合并到历史操盘总结中。
  当用户要求“开始今天固定股票池交易”时触发。
---

# 1. 适用场景 / 触发说明
- 用户说“开始今天固定长票池交易”。
- 目标：通过固定流程生成决策文件，人工确认后执行交易与总结归档。

# 2. Python 环境要求（应遵守）
本技能的所有脚本应在指定的 Python 虚拟环境中运行：

- **虚拟环境路径**：`/home/zhangbeiqing/venv/ai_stock`
- **Python 解释器**：`/home/zhangbeiqing/venv/ai_stock/bin/python`

**激活环境方式**：
```bash
source /home/zhangbeiqing/venv/ai_stock/bin/activate
```

# 3. 输出目录规范（应遵守）
- 统一输出到：`data/skill_runs/YYYY-MM-DD/fixed_tracked`

每日股票资料产物结构介绍：
- `01_global_context.md`（宏观/大盘/渐进式新闻总结，中间产物；请在阅读 03 中规则后再打开）
- `02_basic_snapshot_payload.json`（《基本面数据概览》：basic_stock_info 生成的 basic snapshot；用于 Step 0 快速扫描与定价基准）
- `03_agent_input.md`（最终 user_query 输入；应先读它并完成建议输出，再读其他文件）
- `04_stock_research/`（个股研究包文件夹：新闻/公告/估值/财报等详细信息；每个文件按 symbol 命名）
- `05_decision.json`（Agent 决策输出，由当前 Agent 在对话中生成）

## 买入决策原则（固定池）

固定池采用“严格准入、分批建仓、有效初仓、证伪退出”，不再把“严把买入关”理解为必须等待所有技术和宏观信号同时完美：

1. **标的准入要严**：只有基本面质量、估值或价格印象、核心逻辑与主要风险均通过庭审，才允许进入 `BUY`。热点或怕踏空不能单独构成买入理由。
2. **买点不必完美**：标的已经通过准入，但短期量价或事件时点仍有不确定性时，优先设计分批建仓，不宜无限等待所谓完美买点。
3. **初始仓位必须有意义**：仓位按输入中的真实总资产计算。除非预测可靠度低或处于高风险事件窗口，否则不宜用不足总资产 1% 的象征仓位冒充完成建仓；应明确当前仓位、首批仓位、计划目标仓位及各自占总资产比例。
4. **确认后加仓，证伪则退出**：首批买入后，只有预设的基本面、催化、量价或资金确认出现才继续加仓；若核心逻辑、盈利验证或风险边界被证伪，应减仓或退出，不得用“长期持有”无限容忍错误。
5. **先校验资金基线**：若 `03_agent_input.md` 的总资产、现金或持仓与用户明确说明、真实账本记录不一致，必须先纠正基线再计算 `action_num`，不得沿用模拟成交后的污染仓位。

# 4. 标准工作流（优先级队列 + P0 并行 subagent，半自动确认）

## 日期选择说明
- 若用户触发 Skill 时明确提供了日期（格式：`YYYY-MM-DD`），则使用用户指定日期
- 若用户未提供日期，默认使用最近一个交易日（today）；若今天是周末或节假日非交易日，向前查找最近一个交易日
- 主 agent 据此日期定位到 `data/skill_runs/YYYY-MM-DD/fixed_tracked/` 目录，所有文件读取和决策输出都基于该日期目录

## 执行模式说明
本技能采用"主 agent 建立优先级队列 + 仅对 P0 并行派发 subagent"的方式，以控制 Opus 等高成本模型的 token 消耗并提升逐股深度。

**上下文共享边界**：
- 主 agent 只读取 `SKILL.md`、`03_agent_input.md`、`02_basic_snapshot_payload.json`、`01_global_context.md`、`data/skill_runs/_analysis_index.json`，以及 `data/selection_runs/{run_date}/06_hot_news_state.json`、`data/selection_runs/{run_date}/05_board_heat_digest.json`。**主 agent 不宜读任何股票的 `_research.md` 或上一交易日的 `05_decision.json`**——深度阅读是 subagent 的工作，防止主 agent token上下文爆炸
- 主 agent 基于上述输入建立 P0（并行深度分析），仅对 P0 派发并行 subagent。
- 每个 subagent 读取共享输入顺序固定为：`03_agent_input.md -> 01_global_context.md -> data/selection_runs/{run_date}/06_hot_news_state.json -> data/selection_runs/{run_date}/05_board_heat_digest.json -> 本股 04_stock_research/{symbol}_research.md`。subagent 不读 `02_basic_snapshot_payload.json`。
- subagent 在读完自己那只股的研究包后自行整理自检清单（至少覆盖该股 `next_day_watchlist` 遗留跟踪点、今日异常涨跌/放量待解释问题、需要核验的高时效事实）。
- `03_agent_input.md` 中"建议输出"仍由主 agent 对用户执行；subagent 须完整阅读规则，但不需重复向用户输出同一段。
- subagent 建议只读 `01/03/06/05` + 自己那只 `_research.md`，不能读其他股票研究包；分析完后把完整庭审底稿传回主 agent；主 agent 负责汇总 P0 详细底稿，最终写入 `05_decision.json`。

## 阶段 A（自动，由主 agent 完成统筹）

### 步骤 1：确认日期和定位目录
根据上述日期选择规则确定交易日，定位到 `data/skill_runs/YYYY-MM-DD/fixed_tracked/` 目录。

### 步骤 2：读取分析指引和规则
主 agent 先读取 `03_agent_input.md`：
- 先理解规则与输出格式
- 按照 `03_agent_input.md` 要求输出你对规则的理解和持仓信息到终端
- **⚠️ 特别注意**：`03_agent_input.md` 中包含的【最终总结生成规则】(`final_summary_rules`) 定义了 `05_decision.json` 的输出格式，应牢记此格式

### 步骤 3：读取基本面数据概览
主 agent 阅读 `02_basic_snapshot_payload.json`，用于快速扫描各股最新快照、仓位、初始定价锚点。

### 步骤 4：读取宏观、大盘上下文
主 agent 阅读 `01_global_context.md`，提炼：
- 宏观、指数、流动性、事件风险背景（第 1–2 节）

### 步骤 4.5：读取热点新闻主题状态与板块热度

主 agent 阅读 `data/selection_runs/{run_date}/06_hot_news_state.json` 和 `data/selection_runs/{run_date}/05_board_heat_digest.json`，提炼：

- **热点新闻主题**（`active_themes` / `new_themes`）：当前市场正在交易的宏观/产业/政策叙事线，每条主题的 `linked_symbols_in_universe`、`linked_boards`、当前强化/钝化状态、`scenario_tree` 和 `next_day_watchlist`
- **板块热度概览**：近期全局板块热度分布，哪些板块是当前的资金主战场、哪些在冷却

**用途**：
- 后续在步骤 5 决定 P0 队列时，将热点主题是否直接覆盖某只股票作为加分权重（提升该股 P0 优先级）
- 后续在步骤 6 派发 subagent 时，若某 P0 股票落入热点主题的 `linked_symbols_in_universe` 或 `linked_boards`，将该主题的核心信息提炼后写入 subagent prompt，供其作为上下文辅助分析

**文件不存在时的处理**：
- 若 `06_hot_news_state.json` 或 `05_board_heat_digest.json` 不存在，跳过此步骤，在给 subagent 的 prompt 中注明"今日无热点主题状态数据"；不要因此中止流程

### 步骤 5：读取分析历史索引，决定今日 P0 队列

#### 5.1 读取分析历史索引

主 agent 读取单一索引文件 `data/skill_runs/_analysis_index.json`，其中记录了所有账本中每只股票的最后分析日期：

```json
{
  "fixed_tracked": {
    "600036.SH": {"deep_analysis_date": "2026-05-28", "price_impression": "合理", "confidence_score": 0.75},
    ...
  }
}
```

该索引由 `merge_subagent_decisions.py` 在每次合并后自动更新，涵盖所有历史深度分析过的股票（不只是当日）。

#### 5.2 决定今日 P0 队列

结合当前持仓（`02_basic_snapshot_payload.json`）、今日量价扫描、Step 4 的宏观判定和分析历史索引，筛出今天需要 subagent 深度分析的股票：

**需要深度分析的股票（P0）**：
- 今日跌幅/波动异常的持仓股
- 大涨大跌（>5%）的股票
- 财报危险期的股票
- 上次深度分析距今 > N 天（由主 agent 综合判断，建议 5-10 个交易日）、仍持仓的股票
- 有新的产业/政策催化剂的股票
- 之前从未被深度分析过的股票（防止饥饿）

**不需要深度分析的股票**：不输出到今日的 `05_decision.json`。

**需要深度分析的股票（P0）**：符合条件的都可纳入，无数量限制。

#### 5.3 暂停等用户确认

主 agent 列出：
- P0 深度分析队列：X 只（逐只列出 symbol + 触发原因摘要）
- **⚠️ 应停住**，等待用户回复"OK 继续"或"确认"后才能进入步骤 6

**主 agent 派发 subagent 时的 prompt 不宜事项（建议）**：
- **不宜在 prompt 中添加任何分析、判断、估值、推荐**，subagent 应自己从 `03_agent_input.md` 中读懂规则并独立形成判断
- **不宜在 prompt 中提供热点主题交叉匹配结果或任何辅助分析上下文**——subagent 会自己读 `06_hot_news_state.json` 和 `05_board_heat_digest.json`，主 agent 不需要代劳
- prompt 只需告知：① 股票 symbol 和名称；② 按 `03_agent_input.md` 规定的顺序读取哪些文件；③ 输出文件路径和格式要求。核心原则：**subagent 读了 03 就知道该怎么做，主 agent 不要替它思考**

主 agent 对 P0 队列中的每只股票分配一个独立 subagent 并行执行。每个 subagent 的任务边界：
- 只负责 1 只股票
- 应先按顺序完整读取 `03_agent_input.md` → `01_global_context.md` → `data/selection_runs/{run_date}/06_hot_news_state.json` → `data/selection_runs/{run_date}/05_board_heat_digest.json` → 自己那只 `04_stock_research/{symbol}_research.md`；如文件较长应分段顺序读到末尾
- 读完本股研究包后**自行整理 `search_brief` 自检清单**，至少覆盖：① 研究包中已载明的上一交易日 `next_day_watchlist` 遗留跟踪点；② 今日异常涨跌 / 放量待解释问题；③ 需要核验的高时效事实
- 逐项判断自检清单后决定是否联网补证；若清单中有待核验目标或命中任一硬触发条件，联网是必须步骤
- 不宜读其他股票研究包；不宜改写最终 `05_decision.json`

subagent 联网补证的硬触发条件至少包括：
- "最近一次交易日历史交易总结" `next_day_watchlist` 有今天应跟踪的遗留问题
- 今日或最近一日出现明显大涨大跌、放量异动，但研究包现有新闻 / 公告 / 财报无法解释
- 研究包中的新闻、公告、经营数据存在明显滞后、缺失、未知或相互矛盾
- 你准备提出 `BUY` / `SELL`，但关键论据依赖可能已变化的外部事实
- 你自己在阅读后明确感到"这里如果不联网，我无法区分是正常波动还是新的基本面 / 事件驱动"

### 步骤 7：subagent 逐股输出标准化结果
每个 P0 subagent 完成以下内容后，把结果返回给主 agent：
- 该股票的多维分析（宏观、行业、业务、量价、新闻、财报、催化）
- 今日和昨日比，该股发生了何种变化？这种变化是否足以改变你对该股基本面和近期股价可能波动的看法
- 价格印象（`price_impression`）：明显低估 / 偏低估 / 合理 / 略贵 / 明显高估 / 泡沫
- 综合判断理由（`judgment_rationale`）
- 交易动议初筛
- 正反方辩论
- 法官裁决
- 次日继续跟踪变量
- 若进行了联网补证，需把搜索问题、来源和结论吸收到 `key_facts` / `inferences` / `next_day_watchlist` 中；若命中强制触发条件却最终未能拿到有效外部证据，必须在结论中显式降低置信度

subagent 写入文件的 JSON 应结构化包含以下字段：
1. `symbol`
2. `stock_name`
3. `scan`
4. `deep_analysis_date`（本次分析日期，填当天）
5. `history_anchor`
6. `delta_summary`
7. `price_impression`
8. `key_facts`
9. `inferences`
10. `judgment_rationale`
11. `motion`（**仅写标签**，如 `BUY 候选` / `SELL 候选` / `HOLD 候选` / `FLAT 候选`，不写理由、不写结论）
12. `court`（正反方辩论 + `verdict` 才是最终裁决，`motion` 只是进入庭审的动议标签）
13. `recommended_action`
14. `action_type`
15. `action_num`
16. `price_target`
17. `key_risks`
18. `next_day_watchlist`
19. `confidence_score`

**输出要求**：
- 应写成可复用的"决策记忆锚"，记录当下价格印象、判断理由和动作决策的完整逻辑
- 应区分"已核实事实"和"基于事实的推断"
- 若没有新的事实触发，应明确写出"沿用昨日判断，仅更新验证结果"
- 若裁决为 `BUY`，应明确写出真实总资产口径、当前仓位、首批有效仓位、计划目标仓位、分批触发条件和证伪退出条件；不能只给象征性试探仓，也不能默认一次性买满目标仓位
- subagent 应在写入的文件内容中明确体现自行整理的 `search_brief` 自检清单每一项是否已有新进展；不能跳过不答
- 若今日存在异常涨跌或放量，且研究包本地材料不足以解释，subagent 应先联网补证，再决定是"事件驱动"还是"高波动正常波动"

#### 7.1 subagent 分析完成后：文件落盘（建议）

subagent 完成上述 19 字段分析后，**应将结果写入文件**，而不是通过对话上下文回传完整底稿给主 agent。

**输出目录**：
```bash
mkdir -p data/skill_runs/{run_date}/fixed_tracked/subagent_result
```

**输出文件路径**：
```
data/skill_runs/{run_date}/fixed_tracked/subagent_result/{stock_name}_{symbol}_{run_date}_decision.json
```

例如：
```
data/skill_runs/2026-04-28/fixed_tracked/subagent_result/世运电路_603920.SH_2026-04-28_decision.json
```

**文件内容**：只写【单个 stock entry 对象】（21 字段），不要外层 `summary_date` / `stock_decisions` 包装。格式就是从属于 `stock_decisions` 数组的一个完整 JSON 对象：
```json
{
  "symbol": "603920.SH",
  "stock_name": "世运电路",
  "scan": "...",
  "deep_analysis_date": "...",
  ...
}
```

**写完后向主 agent 回传**：只回传一句简短确认，格式为：
```
{symbol} {stock_name} 分析完成 → {文件名} | action={action_type} | 置信度={confidence_score}
```

例如：
```
603920.SH 世运电路 分析完成 → 世运电路_603920.SH_2026-04-28_decision.json | action=HOLD | 置信度=0.75
```

subagent **不宜**把完整 19 字段底稿塞进回传消息中——主 agent 不需要看到详细底稿，合并脚本会自动处理。

### 步骤 8：主 agent 合并 P0 结果

主 agent 等待所有 P0 subagent 完成并确认文件落盘后，按以下步骤操作：

#### 8.1 确认所有 P0 文件已落盘

检查每个 P0 symbol 对应的 `subagent_result/{stock_name}_{symbol}_{date}_decision.json` 是否已存在：
```bash
ls data/skill_runs/{run_date}/fixed_tracked/subagent_result/
```

若某 P0 symbol 缺失文件，立即追问对应 subagent，不要跳过。

#### 8.2 执行合并脚本，将 P0 结果写入 05_decision.json

```bash
source /home/zhangbeiqing/venv/ai_stock/bin/activate && python scripts/merge_subagent_decisions.py --date {run_date} --book-type fixed_tracked
```

该脚本会：
- 从空 `05_decision.json` 骨架开始
- 遍历 `subagent_result/` 下所有 `*_decision.json` 文件
- 按 symbol 匹配后**整条替换** `stock_decisions` 中的对应 entry（新 symbol 则追加）
- 写入合并后的 `05_decision.json`
- 输出替换/新增摘要


#### 8.3 跨股票冲突裁决

主 agent 在完成合并后，检查不同股票间的逻辑冲突（如两只股票建议同时大幅加仓导致仓位过重、价格印象自相矛盾等），二次裁决并回写。

#### 8.4 向用户展示汇总结果

明确分类呈现：
- **P0 深度分析 X 只**：逐只列出 symbol + action 结论（从 subagent 回传的确认信息汇总即可）
- 逐股给出庭审裁决摘要

- **⚠️ 到此应停住，等待人工确认**

### 步骤 9：人工确认后完成最终决策文件

**⚠️ 只有在用户明确回复"OK 生成决策"或类似确认后，主 agent 才执行此步骤**

此时 `05_decision.json` 中 P0 的 entry 已由合并脚本（步骤 8.2）写入。步骤 9 只需完成收尾工作：

#### 9.1 更新顶层字段（两条路径通用）

用 Edit 工具修改 `05_decision.json` 的顶层字段：
- `summary_date`：改为今日日期
- `system_risk_notes`：写今日宏观风险提示（不继承昨日）
- `system_focus_items`：写今日关注点（不继承昨日）

- **⚠️ 生成完毕后应停住，等待人工检查决策文件**

## 阶段 B（人工确认）

### 用户确认：生成 `05_decision.json` 前需要人工确认
- **时机**：主 agent 完成全部 subagent 结果汇总后
- **内容**：用户检查逐股分析结论和逐股庭审裁决
- **操作**：用户回复“OK 生成决策”或“继续生成 JSON”
- **作用**：只有人工确认后，主 agent 才生成 `05_decision.json`

# 5. subagent 规则（参考，禁止复述进 prompt）

> ⚠️ **以下内容已在 `03_agent_input.md` 和 `skill_flow.json` 中完整定义。subagent 会自行阅读 `03_agent_input.md` 获取全部规则。主 agent 在派发 prompt 中禁止复述本节的任何内容。**

## prompt 模板（唯一允许的格式）

每个 subagent 的 prompt 仅允许包含以下内容：

```
你负责对股票 **{symbol} {stock_name}** 进行今日({date})深度分析。

## 读取顺序（严格按此顺序，完整读完每个文件）
1. data/skill_runs/{date}/fixed_tracked/03_agent_input.md
2. data/skill_runs/{date}/fixed_tracked/01_global_context.md
3. data/selection_runs/{date}/06_hot_news_state.json
4. data/selection_runs/{date}/05_board_heat_digest.json
5. data/skill_runs/{date}/fixed_tracked/04_stock_research/{stock_name}_{symbol}_{date}_research.md

## 联网搜索硬触发条件（以下任一命中，联网是必做步骤，不可跳过）
1. 上一交易日 next_day_watchlist 有今天应跟踪的遗留问题
2. 今日或最近一日出现明显大涨大跌、放量异动，但研究包现有材料无法充分解释
3. 研究包中的新闻、公告、经营数据存在明显滞后、缺失、未知或互相矛盾
4. 你准备提出 BUY 或 SELL，但关键论据依赖可能已变化的外部事实
5. 你自己在阅读后明确感到"这里如果不联网，我无法区分是正常波动还是新的基本面/事件驱动"

## 输出
将结果写入文件：data/skill_runs/{date}/fixed_tracked/subagent_result/{stock_name}_{symbol}_{date}_decision.json
只写单个 stock entry 对象（19 字段），deep_analysis_date 填 "{date}"。

写完后回传一句话确认：
{symbol} {stock_name} 分析完成 → {文件名} | action={action_type} | 置信度={confidence_score}
```

**禁止在 prompt 中添加**：持仓背景、成本、浮亏、基本面指标、热点主题分析、分析规则复述、任何判断或建议。subagent 读完 03 全知道。

## 参考：subagent 工作流（仅供理解，不写入 prompt）

subagent 按以下流程自主工作，所有规则来自 `03_agent_input.md`：

- 按 `03 → 01 → 06 → 05 → 本股04` 顺序完整读取
- 读完后自行整理 `search_brief` 自检清单，命中硬触发条件时必须联网补证
- 形成价格印象（明显低估~泡沫），不做精确估值
- 庭审：motion（仅标签）→ 正方 → 反方 → court.verdict
- 结果写入 `subagent_result/` 目录，回传仅需简短确认
