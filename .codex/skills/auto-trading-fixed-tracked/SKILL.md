---
name: auto-trading-fixed-tracked
description: >
  固定股票池每日股票自动交易程序。Agent 读取用户已经准备好的固定股票池中的每日最新股票数据，根据用户规则详细分析股票后，生成每日交易决策 JSON，经人工确认后，再执行交易、然后生成每日操盘总结，最后合并到历史操盘总结中。
  当用户要求“开始今天固定股票池交易”时触发。
---

# 1. 适用场景 / 触发说明
- 用户说“开始今天固定长票池交易”。
- 目标：通过固定流程生成决策文件，人工确认后执行交易与总结归档。
- 长期池相比固定池的差异：股票池每日由 `09_fixed_tracked_candidates.json` 生成，可能新增或剔除；因此继承上一交易日决策基线时，必须先识别池变化再处理。

# 2. Python 环境要求（必须遵守）
本技能的所有脚本必须在指定的 Python 虚拟环境中运行：

- **虚拟环境路径**：`/home/zhangbeiqing/venv/ai_stock`
- **Python 解释器**：`/home/zhangbeiqing/venv/ai_stock/bin/python`

**激活环境方式**：
```bash
source /home/zhangbeiqing/venv/ai_stock/bin/activate
```

# 3. 输出目录规范（必须遵守）
- 统一输出到：`data/skill_runs/YYYY-MM-DD/fixed_tracked`

每日股票资料产物结构介绍：
- `01_global_context.md`（宏观/大盘/渐进式新闻总结，中间产物；请在阅读 03 中规则后再打开）
- `02_basic_snapshot_payload.json`（《基本面数据概览》：basic_stock_info 生成的 basic snapshot；用于 Step 0 快速扫描与定价基准）
- `03_agent_input.md`（最终 user_query 输入；必须先读它并完成强制输出，再读其他文件）
- `04_stock_research/`（个股研究包文件夹：新闻/公告/估值/财报等详细信息；每个文件按 symbol 命名）
- `05_decision.json`（Agent 决策输出，由当前 Agent 在对话中生成）

# 4. 标准工作流（优先级队列 + P0 并行 subagent，半自动确认）

## 日期选择说明
- 若用户触发 Skill 时明确提供了日期（格式：`YYYY-MM-DD`），则使用用户指定日期
- 若用户未提供日期，默认使用上一个交易日（today - 1）；若昨天是周末或节假日非交易日，向前查找最近一个交易日
- 主 agent 据此日期定位到 `data/skill_runs/YYYY-MM-DD/fixed_tracked/` 目录，所有文件读取和决策输出都基于该日期目录

## 执行模式说明
本技能采用"主 agent 建立优先级队列 + 仅对 P0 并行派发 subagent"的方式，以控制 Opus 等高成本模型的 token 消耗并提升逐股深度。

**上下文共享边界**：
- 主 agent 只读取 `SKILL.md`、`03_agent_input.md`、`02_basic_snapshot_payload.json`、`01_global_context.md`、上一交易日的`05_decision.json`，以及 `data/selection_runs/{run_date}/06_hot_news_state.json`、`data/selection_runs/{run_date}/05_board_heat_digest.json`。**主 agent 禁止读任何股票的 `_research.md`**——深度阅读是 subagent 的工作，防止主 agent token上下文爆炸
- 主 agent 基于上述输入建立 P0（并行深度分析，≤5 只）/ P1（主 agent 快扫结论）仅对 P0 派发并行 subagent。
- 每个 subagent 读取共享输入顺序固定为：`03_agent_input.md -> 01_global_context.md -> 本股 04_stock_research/{symbol}_research.md`。subagent 不读 `02_basic_snapshot_payload.json`。
- subagent 在读完自己那只股的研究包后自行整理自检清单（至少覆盖该股 `next_day_watchlist` 遗留跟踪点、今日异常涨跌/放量待解释问题、需要核验的高时效事实）。
- `03_agent_input.md` 中"强制输出"仍由主 agent 对用户执行；subagent 须完整阅读规则，但不需重复向用户输出同一段。
- subagent 只能读 `01/03` + 自己那只 `_research.md`，不得读其他股票研究包；分析完后把完整庭审底稿传回主 agent；主 agent 负责汇总 P0 详细底稿 + P1 快扫，最终写入 `05_decision.json`。

## 阶段 A（自动，由主 agent 完成统筹）

### 步骤 1：确认日期和定位目录
根据上述日期选择规则确定交易日，定位到 `data/skill_runs/YYYY-MM-DD/fixed_tracked/` 目录。

### 步骤 2：读取分析指引和规则
主 agent 先读取 `03_agent_input.md`：
- 先理解规则与输出格式
- 按照 `03_agent_input.md` 要求输出你对规则的理解和持仓信息到终端
- **⚠️ 特别注意**：`03_agent_input.md` 中包含的【最终总结生成规则】(`final_summary_rules`) 定义了 `05_decision.json` 的输出格式，必须牢记此格式

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
- 后续在步骤 6 确定 P0/P1 时，将热点主题是否直接覆盖某只股票作为加分权重（提升该股 P0 优先级）
- 后续在步骤 7 派发 subagent 时，若某 P0 股票落入热点主题的 `linked_symbols_in_universe` 或 `linked_boards`，将该主题的核心信息提炼后写入 subagent prompt，供其作为上下文辅助分析

**文件不存在时的处理**：
- 若 `06_hot_news_state.json` 或 `05_board_heat_digest.json` 不存在，跳过此步骤，在给 subagent 的 prompt 中注明"今日无热点主题状态数据"；不要因此中止流程

### 步骤 5：继承上一交易日决策基线

**目的**：避免主 agent 对 非P0 股票 从头生成底稿，转而继承昨日完整底稿 + 今日局部刷新

#### 5.1 查找并 `cp` 上一交易日的继承基线

查找 `data/skill_runs/<上一交易日>/fixed_tracked/05_decision.json`：

- **存在**：执行 `cp` 到今日目录作为继承基线：

```bash
cp data/skill_runs/<prev_trading_day>/fixed_tracked/05_decision.json data/skill_runs/<today>/fixed_tracked/05_decision.json
```

- **不存在**（首次运行、连续多日缺失、或用户主动删除）：跳过 5.3~5.4，按冷启动执行：

`<上一交易日>` 不是日历昨天，是 run_date 之前最近一个含有 `fixed_tracked/05_decision.json` 的交易日（周一早上通常回看上周五；节假日后类推）。可用 `ls data/skill_runs/` 反向查找。


#### 5.2 顶层字段不继承

顶层 `summary_date`、`system_risk_notes`、`system_focus_items` 必须在步骤 10 重写为今日内容，不得沿用昨日。

### 步骤 6：主 agent 建立 P0/P1 优先级队列
按照03_agent_input.md中规则将待分析股票分为P0(今天需要深度分析) P1(仅概览)：

- **P0 级（并行深度分析）**：今天需要并行subagent深度分析的股票
- **P1 级（继承昨日底稿 + 今日局部刷新，若上一交易日没有该股票的深度分析，则简单生成相关信息，必须注明未深度分析过仅概览）**：股价平稳的持仓股、观察仓。主 agent 不派 subagent，也不读 `_research.md`。

** 主agent选择完P0/P1股票后，需暂停等用户确认**：

主 agent 在完成 P0/P1 分档后，**必须立即停住**，向用户输出以下内容并等待确认：

**输出内容**：
1. **P0 筛选清单**：逐只列出 P0 股票的名称和 symbol，每只附一句话说明为什么被选为 P0（明确写出触发规则：昨日持仓异动 / 今日大涨大跌放量 / 财报危险期 / Step0-Step1 识别机会 / 之前未分析过防止饥饿）
2. **完整分档总览**：
   - P0 深度分析队列：X 只（逐只列出，附触发原因摘要）
   - P1 继承刷新：Y 只（逐只列出 symbol）
3. **暂停等待确认**：说完以上内容后**必须停住**，等待用户回复"OK 继续"、"确认"或类似明确确认后，才能进入步骤 7 派发 subagent。

**禁止**：在没有得到用户对 P0 队列的明确确认前直接派发 subagent。

### 步骤 7：主 agent 对 P0 队列并行派发 subagent

主 agent 在派发每个 P0 subagent 前，**必须执行热点主题交叉匹配**：
1. 检查当前 P0 股票的 symbol 是否出现在 `06_hot_news_state.json` 的任一 `active_themes` 或 `new_themes` 的 `linked_symbols_in_universe` 中
2. 检查当前 P0 股票所在板块是否在 `06_hot_news_state.json` 的任一主题的 `linked_boards` 中，或在 `05_board_heat_digest.json` 中热度排名靠前
3. 若匹配到热点主题，须将以下提炼信息写入该 subagent 的 prompt：
   - 该主题的 `theme_name`、`strength`（强化/稳定/减弱）、`current_state`
   - 该股票/板块为何与该主题关联
   - 该主题的 `scenario_tree` 主要演化路径中与这只股票相关的部分
   - 该主题的 `next_day_watchlist` 中与这只股票相关的跟踪点

**热点主题信息的用途**：让 subagent 在分析时知道"当前宏观/产业叙事正在交易什么"，帮助理解该股的量价异动是否有板块逻辑支撑、催化剂是否在加强或钝化，避免孤立的个股分析漏掉重要的主题驱动因素。

主 agent 对 P0 队列中的每只股票分配一个独立 subagent 并行执行。每个 subagent 的任务边界：
- 只负责 1 只股票
- 必须先按顺序完整读取 `03_agent_input.md` → `01_global_context.md` → 自己那只 `04_stock_research/{symbol}_research.md`；如文件较长必须分段顺序读到末尾
- 读完本股研究包后**自行整理 `search_brief` 自检清单**，至少覆盖：① 研究包中已载明的上一交易日 `next_day_watchlist` 遗留跟踪点；② 今日异常涨跌 / 放量待解释问题；③ 需要核验的高时效事实
- 逐项判断自检清单后决定是否联网补证；若清单中有待核验目标或命中任一硬触发条件，联网是强制步骤
- 不得读其他股票研究包；不得改写最终 `05_decision.json`

subagent 联网补证的硬触发条件至少包括：
- "最近一次交易日历史交易总结" `next_day_watchlist` 有今天应跟踪的遗留问题
- 今日或最近一日出现明显大涨大跌、放量异动，但研究包现有新闻 / 公告 / 财报无法解释
- 研究包中的新闻、公告、经营数据存在明显滞后、缺失、未知或相互矛盾
- 你准备提出 `BUY` / `SELL`，但关键论据依赖可能已变化的外部事实
- 你自己在阅读后明确感到"这里如果不联网，我无法区分是正常波动还是新的基本面 / 事件驱动"

### 步骤 8：subagent 逐股输出标准化结果
每个 P0 subagent 完成以下内容后，把结果返回给主 agent：
- 该股票的五维透视分析
- 今日是否允许重算估值锚，以及触发器是否成立
- 盈利预测可靠度与估值模式
- 估值结论：保守区间、粗略区间，或明确“不做精确估值”
- 交易动议初筛
- 正反方辩论
- 法官裁决
- 次日继续跟踪变量
- 若进行了联网补证，需把搜索问题、来源和结论吸收到 `key_facts` / `inferences` / `next_day_watchlist` 中；若命中强制触发条件却最终未能拿到有效外部证据，必须在结论中显式降低置信度

subagent 写入文件的 JSON 必须结构化包含以下字段：
1. `symbol`
2. `stock_name`
3. `scan`
4. `analysis_type`
5. `history_anchor`
6. `allow_reanchor_today`
7. `forecast_reliability`
8. `valuation_mode`
9. `key_facts`
10. `inferences`
11. `valuation_conclusion`
12. `motion`
13. `court`
14. `recommended_action`
15. `action_type`
16. `action_num`
17. `price_target`
18. `stop_loss`
19. `key_risks`
20. `next_day_watchlist`
21. `confidence_score`

**输出要求**：
- 必须写成可复用的“估值记忆锚”
- 必须区分“已核实事实”和“基于事实的推断”
- 必须显式引用上一交易日的该股估值锚是否变化
- 若没有触发估值锚重算条件，必须明确写出“沿用昨日锚点，仅更新验证结果”，不得因为价格涨跌直接改目标价
- subagent 必须在写入的文件内容中明确体现自行整理的 `search_brief` 自检清单每一项是否已有新进展；不能跳过不答
- 若今日存在异常涨跌或放量，且研究包本地材料不足以解释，subagent 必须先联网补证，再决定是"事件驱动"还是"高波动正常波动"

#### 8.1 subagent 分析完成后：文件落盘（强制）

subagent 完成上述 21 字段分析后，**必须将结果写入文件**，而不是通过对话上下文回传完整底稿给主 agent。

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
  "analysis_type": "...",
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

subagent **禁止**把完整 21 字段底稿塞进回传消息中——主 agent 不需要看到详细底稿，合并脚本会自动处理。

### 步骤 9：主 agent 汇总 P0 底稿 + P1 继承刷新

主 agent 等待所有 P0 subagent 完成并确认文件落盘后，按以下步骤操作：

#### 9.1 确认所有 P0 文件已落盘

检查每个 P0 symbol 对应的 `subagent_result/{stock_name}_{symbol}_{date}_decision.json` 是否已存在：
```bash
ls data/skill_runs/{run_date}/fixed_tracked/subagent_result/
```

若某 P0 symbol 缺失文件，立即追问对应 subagent，不要跳过。

#### 9.2 执行合并脚本，将 P0 结果写入 05_decision.json

```bash
source /home/zhangbeiqing/venv/ai_stock/bin/activate && python scripts/merge_subagent_decisions.py --date {run_date} --book-type fixed_tracked
```

该脚本会：
- 读取已有的 `05_decision.json`（继承基线）
- 遍历 `subagent_result/` 下所有 `*_decision.json` 文件
- 按 symbol 匹配后**整条替换** `stock_decisions` 中的对应 entry（新 symbol 则追加）
- 写入合并后的 `05_decision.json`
- 输出替换/新增/保留的摘要

#### 9.3 P1 股票处理（主 agent 手动完成）

合并脚本**不处理 P1 股票**。主 agent 在合并完成后，用 Edit 工具对 P1 股票的 entry 做局部刷新（与之前流程完全一致）：
- `scan`：更新今日量价、持仓、昨收对比
- `action_type` / `action_num`：更新
- `analysis_type`：改为 `"p1_inherited_from_<prev_date>"`
- `inferences`：在列表末尾追加一行今日确认
- 必要时微调 `confidence_score`
- **其他字段一律不改**
- **`action_type` 继承修正规则（强制）**：继承上日基线时，若上日 `action_type` 为 `BUY` 或 `SELL`，不能机械搬运：
  - 若上日 `action=BUY` 且当前持仓数据已反映买入（股数>0），当日应自动转为 `HOLD`
  - 若上日 `action=SELL` 且当前持仓数据已反映卖出（股数=0），当日应自动转为 `FLAT`
  - 仅当无法从持仓数据确认执行状态时，保留原 `action_type` 并在 `scan` 中注明"待确认执行状态"

若 P1 中有新入池股票（继承基线中不存在），需冷启动生成概览 entry，并注明从未深度分析。

#### 9.4 跨股票冲突裁决

主 agent 在完成合并和 P1 刷新后，检查不同股票间的逻辑冲突（如两股估值方法矛盾、仓位分配冲突等），二次裁决并回写。

#### 9.5 向用户展示汇总结果

明确分类呈现：
- **P0 深度分析 X 只**：逐只列出 symbol + action 结论（从 subagent 回传的确认信息汇总即可）
- **P1 继承刷新 Y 只**：逐只列出 symbol，注明逻辑未变
- 逐股给出庭审裁决摘要

- **⚠️ 到此必须停住，等待人工确认**

### 步骤 10：人工确认后完成最终决策文件

**⚠️ 只有在用户明确回复"OK 生成决策"或类似确认后，主 agent 才执行此步骤**

此时 `05_decision.json` 中 P0 的 entry 已由合并脚本（步骤 9.2）写入，P1 继承刷新也已由主 agent（步骤 9.3）通过 Edit 工具完成。步骤 10 只需完成收尾工作：

#### 10.1 更新顶层字段（两条路径通用）

用 Edit 工具修改 `05_decision.json` 的顶层字段：
- `summary_date`：改为今日日期
- `system_risk_notes`：写今日宏观风险提示（不继承昨日）
- `system_focus_items`：写今日关注点（不继承昨日）

#### 10.2 P1 新入池股票（仅当继承基线中无此 symbol 时）

基线中原本就没有该 symbol 的 P1 股票，此时用 Write 工具**逐只 append** 到 `stock_decisions` 数组末尾，每个字段生成一个概览结果即可，但需注明从未深度分析（`analysis_type` 标记为 `"p1_cold_start"`）。

#### 路径 A / 路径 B 通用

由于合并脚本已在步骤 9.2 统一处理了 P0 条目写入，两条路径的区别已大大缩小：
- **路径 A（有继承基线）**：合并脚本替换了 P0 entry + 主 agent Edit 刷新了 P1 entry → 步骤 10 只需收尾顶层字段
- **路径 B（冷启动）**：合并脚本从空基线写入了 P0 entry → 主 agent 补 P1 冷启动 entry → 步骤 10 收尾顶层字段

#### 通用强制要求

- **P0 entry 是由 subagent 直接写入的完整底稿，合并脚本原样搬运到 05_decision.json**。主 agent 不得重新压缩、改写、丢弃细节。subagent 的分析质量就是最终质量。
- **P1 entry 的 `key_facts`、`court`、`recommended_action`、`key_risks`、`next_day_watchlist` 等字段必须原样保留自继承基线**，禁止重写成更短版本；主 agent 若发现确有必要修改（如 thesis 明显弱化），说明该股应升格为 P0，不能在 P1 流程里暗改。
- P1 的 `analysis_type` 必须显式包含 `inherited_from_<prev_date>` 标签，让后续复核能一眼看出这只股今日没做深度分析。
- **⚠️ 生成完毕后必须停住，等待人工检查决策文件**

## 阶段 B（人工确认）

### 用户确认：生成 `05_decision.json` 前需要人工确认
- **时机**：主 agent 完成全部 subagent 结果汇总后
- **内容**：用户检查逐股分析结论和逐股庭审裁决
- **操作**：用户回复“OK 生成决策”或“继续生成 JSON”
- **作用**：只有人工确认后，主 agent 才生成 `05_decision.json`

# 5. 单股分析最小规则（强制）

1. **单股负责制**：每个 subagent 只负责 1 只股票；允许读取 `03_agent_input.md`、`01_global_context.md` 这 2 份共享输入，以及自己负责的 `_research.md`；不得读取其他股票研究包。该股票 snapshot 已内嵌在 `_research.md` 中。
2. **完整阅读优先**：读取顺序固定为 `03 -> 01 -> 本股 04`，且这 3 份输入都必须从头到尾完整读完；只有全部读完后，才允许补充搜索。若主 agent 在 prompt 中提供了热点主题上下文（提炼自 `06_hot_news_state.json` / `05_board_heat_digest.json`），应在阅读完本地文件后将该主题信息作为辅助分析背景。
3. **先执行搜索next_day_watchlist，再下结论**：subagent 在完成本地阅读后，必须先核验 next_day_watchlist中列出的遗留跟踪点和疑点；不能直接跳过这些核验进入估值与动作判断。
4. **先判断，再估值**：先回答四个问题，再决定是否需要估值：
   - 今日是否允许重算估值锚
   - 盈利预测可靠度是 `high` / `medium` / `low`
   - 估值模式是 `simple_valuation` / `range_valuation` / `no_precise_valuation`
   - 最终动作候选是什么
5. **预测可靠度低时从严**：若 `low`，需要谨慎考虑是否"buy"或者"加仓"；此时重点是判断是否继续持有、减仓或退出。
6. **SOTP 的优先使用原则**：如果公司业务分布清晰、分部利润和关键假设可验证，且拆分估值会实质影响结论，则应优先使用 SOTP；只有在分部口径不清、关键假设无法验证，或拆分后只会制造伪精确时，才退回整体估值或区间估值。
7. **异常波动必须解释**：若今日或最近一日大涨大跌、放量异动，而研究包没有足够解释，必须联网核验是否存在突发利好、利空、公告、经营数据或行业事件；只有核验后仍无证据时，才可判定为高波动品种的正常波动。
8. **禁止事项**：
   - 不得读取其他股票研究包
   - 不得跳过 `03/01/本股04` 的完整阅读直接联网搜索
   - 不得在命中强制联网触发条件时省略搜索步骤
   - 不得为了显得完整而编造精确目标价
   - 不得直接生成或覆盖最终 `05_decision.json`

9. **估值完成后必须回看历史价格区间（强制自查）**：subagent 在完成三情景估值测算后，**必须立即回看**研究包 `1.2 Valuation Report` 部分中的以下两个关键表格：

   **回看内容**：
   - **"最近三年半股价区间分布"表**：显示该股过去 3.5 年实际交易价格分布（最低价 ~ 最高价，分 10 档及出现概率）
   - **"最近三年半最高/最低股价及对应估值指标"表**：显示历史价格极值及当时的 TTM 净利润、PE、PEG

   **强制自省——逐条回答以下问题并写入 `valuation_conclusion`**：
   1. 我算出的【悲观情景目标价】是否 **高于** 该股的"三年半最低价"？如果高于，原因是什么？
      - 计算"当前 TTM 净利润 ÷ 历史最低价时 TTM 净利润"的倍数
      - 如果该倍数 > 1.5x，说明盈利增长可部分解释价格底部抬升，但仍需确认悲观 PE **≤** 历史最低价时的 PE
      - 如果该倍数 ≤ 1.5x 但悲观目标价仍显著高于历史最低价（>30%），**PE 假设严重偏乐观**，必须下调悲观 PE 至历史低位水平或更低
   2. 我算出的【乐观情景目标价】是否 **大幅高于** 该股的"三年半最高价"？如果高出 >30%，必须反思乐观 PE 或利润假设是否过分激进
   3. 最终在 `valuation_conclusion` 末尾必须写一句 **"历史价格锚点验证："**，明确写出悲观目标与历史最低的关系、乐观目标与历史最高的关系，以及偏差的合理性解释

   **核心原则**：估值测算不能飘在空中。如果三情景价格区间与历史实际交易区间严重偏离（悲观 > 历史最低 +30%、乐观 > 历史最高 +30%），必须找到利润增长、PE中枢变化或其他基本面变化的合理解释。没有充分理由之前，不能直接使用该估值结论。

11. **必须调查历史价格极值成因并对比当下（强制）**：subagent 在回看历史价格区间后，必须对两端极值（三年半最低价和最高价）进行成因调查，判断当下与历史极值时刻的异同。

   **调查步骤**：
   1. **先查本地缓存**：检查研究包"最近一次交易日历史交易总结"中是否已有对该股历史极值的成因分析——若有且至今没有新的事实变化（如新的财报大幅偏离、新的行业政策、新的重大事件），直接复用分析并注明"复用历史交易总结第X条"，**跳过联网**。
   2. **若无缓存则强制联网搜索**，逐条回答：
      - **历史最低价（日期+价格）**：当时发生了什么？业绩暴雷/行业寒冬/宏观危机/监管打击/战争冲击/还是正常的熊市波动？当时的 TTM 净利润和 PE 各是多少(ttm pe pb可以从个股研究包里找到)？多种利空因素中哪些是同时叠加的？
      - **历史最高价（日期+价格）**：当时发生了什么？业绩高增长预期/行业景气顶点/资金抱团炒作/重大题材催化/宏观流动性泛滥？当时的 TTM 净利润和 PE 各是多少(ttm pe pb可以从个股研究包里找到)？
   3. **与当下逐项对比**：将历史最低/最高时的条件与当前条件列成对比表：
      - 当前基本面（利润规模、增速、竞争格局、毛利率） vs 历史最低时：显著更好 / 大致相当 / 更差？
      - 当前估值（PE/PB/PS） vs 历史最低时：更便宜 / 相当 / 更贵？
      - 历史最低时叠加的利空因素中，有哪些在当下重新出现（如宏观危机、行业寒冬）？有哪些已彻底解除？
      - 历史最高时推动上涨的因素中，有哪些在当下重新出现？有哪些已不复存在？

   **估值应用**：这个对比直接决定三情景 PE 假设的合理性：
   - 如果当前基本面**远好于**历史最低时（利润增长了 2-3x 但 PE 反而更低），那么悲观 PE 可以**适度高于**历史最低 PE，因为盈利质量已质变，但有证据支撑的差价必须写明
   - 如果当前条件与历史最低时**高度相似**（利润增长有限、同样面临宏观/行业逆风），但你的悲观 PE 却显著高于历史最低 PE，**必须下调**
   - 如果当前与历史最高时面临的乐观条件相似，但你的乐观 PE 远高于历史最高 PE（>50%），说明假设过于激进

   **输出**：在 `valuation_conclusion` 的"历史价格锚点验证："段落末尾，追加"历史极值成因与当下对比："，包含最低价成因简述、最高价成因简述、以及"对比结论：当前情况与历史[最低/最高]时相比，[更好/相似/更差]，因为..."。

   **联网搜索硬触发**：这是**强制步骤**。除非历史交易总结已缓存，否则 subagent 必须在联网搜索中覆盖此项，不得跳过。如果搜索结果不充分，必须在结论中显式降低置信度。

# 6. 结构化决策最小写法

`05_decision.json` 中每只股票应优先使用结构化字段保留完整底稿，而不是把所有内容塞进 `reason`。

**强制补充要求**：
- `05_decision.json` 的职责不是做“摘要”，而是沉淀可复用的完整结构化分析底稿。
- 若 subagent 已经完成详细分析，主 agent 默认必须把这些详细内容带入 `05_decision.json`，而不是自作主张压缩成更短版本。
- 除非用户明确要求“只保留摘要”，否则不得主动删掉大量 `key_facts`、`inferences`、`court`、`recommended_action`、`key_risks`、`next_day_watchlist` 的细节。
- 主 agent 允许做的事情仅限：补 `action_type`、`action_num`、解决跨股票冲突、统一少量措辞、修正明显重复或格式错误；不允许把完整底稿改写成自己想象中的简版内容。

每只股票至少包含：
1. `symbol`
2. `stock_name`
3. `scan`
4. `analysis_type`
5. `history_anchor`
6. `allow_reanchor_today`
7. `forecast_reliability`
8. `valuation_mode`
9. `key_facts`
10. `inferences`
11. `valuation_conclusion`
12. `motion`
13. `court`
14. `recommended_action`
15. `action_type`
16. `action_num`
17. `price_target`
18. `stop_loss`
19. `key_risks`
20. `next_day_watchlist`
21. `confidence_score`

说明：
- `recommended_action` 保留子agent原始主观建议和底稿口吻
- `action_type`、`action_num` 由主agent在人工确认后补充，用于执行层枚举消费
- `BUY` / `SELL` 如需真实执行，必要的数量与执行价信息必须在最终 JSON 中可读；如果当天不交易，`action_num` 仍应显式给出当前持仓或 `0`

# 7. 庭审最小规则

在输出最终 JSON 前，必须完成最小庭审：
- **动议类型**：买入 / 卖出 / 持有 / 观望
- **正方**：为什么应该这么做
- **反方**：最大的反对理由是什么
- **裁决**：最终决定与理由

如果盈利预测可靠度为 `low`，买入动议需更加谨慎审查。

# 8. subagent 文件输出规范

subagent **不通过对话上下文回传完整分析结果**。分析完成后必须将结果写入文件（见步骤 8.1），回传内容仅需简短确认。

写入文件的 JSON 对象包含以下 21 个字段：
1. `symbol`
2. `stock_name`
3. `scan`
4. `analysis_type`
5. `history_anchor`
6. `allow_reanchor_today`
7. `forecast_reliability`
8. `valuation_mode`
9. `key_facts`
10. `inferences`
11. `valuation_conclusion`
12. `motion`
13. `court`
14. `recommended_action`
15. `action_type`
16. `action_num`
17. `price_target`
18. `stop_loss`
19. `key_risks`
20. `next_day_watchlist`
21. `confidence_score`

**输出文件路径**：
```
data/skill_runs/{run_date}/fixed_tracked/subagent_result/{stock_name}_{symbol}_{run_date}_decision.json
```

字段语义必须完整，可直接被 `merge_subagent_decisions.py` 合并脚本读取并写入最终 `05_decision.json`。

**主 agent 汇总约束（强制）**：
- subagent 写入文件的 JSON 是 `05_decision.json` 各股条目的唯一来源主体正文。
- 合并脚本 (`merge_subagent_decisions.py`) 负责将文件原样搬运到 `05_decision.json` 的 `stock_decisions` 数组中。
- **需要的文件一定要完整读完，不要只读一部分！！金融相关分析完整文件很重要**

**注：非常重要。你可以自由裁定是否卖出，本skill只限制必须卖出的情景。非必须卖出情景，你可以根据当前综合形势，发动主观能动性确定是否卖出。必须卖出是为了兜底止损防止亏太多，至于如何止盈、看情况不妙何时及时跑路，你可以自己决定**
