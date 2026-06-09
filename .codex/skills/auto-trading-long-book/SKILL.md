---
name: auto-trading-long-book
description: >
  动态长期股票池每日股票自动交易程序。Agent 读取用户已经准备好的长期股票池中的每日最新股票数据，根据用户规则详细分析股票后，生成每日交易决策 JSON，经人工确认后，再执行交易、然后生成每日操盘总结，最后合并到历史操盘总结中。长期池与固定池的关键差异：每日池子可能变化（新入池/剔出池），因此继承基线时应识别集合关系。
  当用户要求“开始今天长期股票池交易”时触发。
---

# 1. 适用场景 / 触发说明
- 用户说“开始今天长期股票池交易”。
- 目标：通过固定流程生成决策文件，人工确认后执行交易与总结归档。
- 长期池相比固定池的差异：股票池每日由 `09_long_book_candidates.json` 生成，可能新增或剔除；因此继承上一交易日决策基线时，应先识别池变化再处理。

# 2. Python 环境要求（应遵守）
本技能的所有脚本应在指定的 Python 虚拟环境中运行：

- **虚拟环境路径**：`/home/zhangbeiqing/venv/ai_stock`
- **Python 解释器**：`/home/zhangbeiqing/venv/ai_stock/bin/python`

**激活环境方式**：
```bash
source /home/zhangbeiqing/venv/ai_stock/bin/activate
```

# 3. 输出目录规范（应遵守）
- 统一输出到：`data/skill_runs/YYYY-MM-DD/long_book`

每日股票资料产物结构介绍：
- `01_global_context.md`（宏观/大盘/渐进式新闻总结，中间产物；请在阅读 03 中规则后再打开）
- `02_basic_snapshot_payload.json`（《基本面数据概览》：basic_stock_info 生成的 basic snapshot；用于 Step 0 快速扫描与定价基准）
- `03_agent_input.md`（最终 user_query 输入；应先读它并完成建议输出，再读其他文件）
- `04_stock_research/`（个股研究包文件夹：新闻/公告/估值/财报等详细信息；每个文件按 symbol 命名）
- `05_decision.json`（Agent 决策输出，由当前 Agent 在对话中生成）

# 4. 标准工作流（优先级队列 + P0 并行 subagent，半自动确认）

## 日期选择说明
- 若用户触发 Skill 时明确提供了日期（格式：`YYYY-MM-DD`），则使用用户指定日期
- 若用户未提供日期，默认使用上一个交易日（today - 1）；若昨天是周末或节假日非交易日，向前查找最近一个交易日
- 主 agent 据此日期定位到 `data/skill_runs/YYYY-MM-DD/long_book/` 目录，所有文件读取和决策输出都基于该日期目录

## 执行模式说明
本技能采用"主 agent 建立优先级队列 + 仅对 P0 并行派发 subagent"的方式，以控制 Opus 等高成本模型的 token 消耗并提升逐股深度。

**上下文共享边界**：
- 主 agent 只读取 `SKILL.md`、`03_agent_input.md`、`02_basic_snapshot_payload.json`、`01_global_context.md`、`data/skill_runs/_analysis_index.json`，以及 `data/selection_runs/{run_date}/06_hot_news_state.json`、`data/selection_runs/{run_date}/05_board_heat_digest.json`。**主 agent 不宜读任何股票的 `_research.md` 或上一交易日的 `05_decision.json`**——深度阅读是 subagent 的工作，防止主 agent token上下文爆炸
- 主 agent 基于上述输入建立 P0（并行深度分析，≤5 只）仅对 P0 派发并行 subagent。
- 每个 subagent 读取共享输入顺序固定为：`03_agent_input.md -> 01_global_context.md -> data/selection_runs/{run_date}/06_hot_news_state.json -> data/selection_runs/{run_date}/05_board_heat_digest.json -> 本股 04_stock_research/{symbol}_research.md`。subagent 不读 `02_basic_snapshot_payload.json`。
- subagent 在读完自己那只股的研究包后自行整理自检清单（至少覆盖该股 `next_day_watchlist` 遗留跟踪点、今日异常涨跌/放量待解释问题、需要核验的高时效事实）。
- `03_agent_input.md` 中"建议输出"仍由主 agent 对用户执行；subagent 须完整阅读规则，但不需重复向用户输出同一段。
- subagent 建议只读 `01/03/06/05` + 自己那只 `_research.md`，不宜读其他股票研究包；分析完后把完整庭审底稿传回主 agent；主 agent 负责汇总 P0 详细底稿，最终写入 `05_decision.json`。

## 阶段 A（自动，由主 agent 完成统筹）

### 步骤 1：确认日期和定位目录
根据上述日期选择规则确定交易日，定位到 `data/skill_runs/YYYY-MM-DD/long_book/` 目录。

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

主 agent 读取单一索引文件 `data/skill_runs/_analysis_index.json`，其中记录了所有账本中每只股票的最后分析日期。

#### 5.2 决定今日 P0 队列

结合当前持仓、今日量价扫描、Step 4 的宏观判定和分析历史索引，筛出今天需要 subagent 深度分析的股票：

**需要深度分析的股票（P0）**：
- 今日跌幅/波动异常的持仓股
- 大涨大跌（>5%）的股票
- 财报危险期的股票
- 上次深度分析距今 > N 天（由主 agent 综合判断，建议 5-10 个交易日）、仍持仓的股票
- 有新的产业/政策催化剂的股票
- 之前从未被深度分析过的股票（防止饥饿）

**不需要深度分析的股票**：不输出到今日的 `05_decision.json`。

容量规则：P0 队列软上限为 5 只。确有需要时可放宽。

#### 5.3 暂停等用户确认

主 agent 列出：
- P0 深度分析队列：X 只（逐只列出 symbol + 触发原因摘要）
- **⚠️ 应停住**，等待用户回复"OK 继续"或"确认"后才能进入步骤 6

### 步骤 6：主 agent 对 P0 队列并行派发 subagent

**主 agent 派发 subagent 时的 prompt 不宜事项（建议）**：
- **不宜在 prompt 中添加任何分析、判断、估值、推荐**，subagent 应自己从 `03_agent_input.md` 中读懂规则并独立形成判断
- **不宜在 prompt 中提供热点主题交叉匹配结果或任何辅助分析上下文**——subagent 会自己读 `06_hot_news_state.json` 和 `05_board_heat_digest.json`，主 agent 不需要代劳
- prompt 只需告知：① 股票 symbol 和名称；② 按 `03_agent_input.md` 规定的顺序读取哪些文件；③ 输出文件路径和格式要求。核心原则：**subagent 读了 03 就知道该怎么做，主 agent 不要替它思考**

主 agent 在派发每个 P0 subagent 前，先确认热点主题交叉匹配作为 P0 加分权重（用于步骤 5 分档）：
1. 检查当前 P0 股票的 symbol 是否出现在 `06_hot_news_state.json` 的任一 `active_themes` 或 `new_themes` 的 `linked_symbols_in_universe` 中
2. 检查当前 P0 股票所在板块是否在 `06_hot_news_state.json` 的任一主题的 `linked_boards` 中，或在 `05_board_heat_digest.json` 中热度排名靠前
3. 若匹配到热点主题，须将以下提炼信息写入该 subagent 的 prompt：
   - 该主题的 `theme_name`、`strength`（强化/稳定/减弱）、`current_state`
   - 该股票/板块为何与该主题关联
   - 该主题的 `scenario_tree` 主要演化路径中与这只股票相关的部分
   - 该主题的 `next_day_watchlist` 中与这只股票相关的跟踪点

**热点主题信息的用途**：让 subagent 在长期 thesis 判断时知道"当前宏观叙事正关注什么赛道"，帮助判断该股所处行业是否有景气度支撑、当前价格是否透支了主题预期，避免孤立的基本面分析漏掉宏观产业趋势。

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
- 该股票的多维分析（业务、行业、宏观、量价、新闻、财报、催化）
- 今日是否允许重新形成判断（`delta_summary`），以及触发器是否成立
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
4. `deep_analysis_date`
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
- 应显式引用上一交易日的该股判断是否变化
- 若没有新的事实触发，应明确写出"沿用昨日判断，仅更新验证结果"，不宜因为价格涨跌直接改变动作
- subagent 应在写入的文件内容中明确体现自行整理的 `search_brief` 自检清单每一项是否已有新进展；不能跳过不答
- 若今日存在异常涨跌或放量，且研究包本地材料不足以解释，subagent 应先联网补证，再决定是"事件驱动"还是"高波动正常波动"

#### 7.1 subagent 分析完成后：文件落盘（建议）

subagent 完成上述 19 字段分析后，**应将结果写入文件**，而不是通过对话上下文回传完整底稿给主 agent。

**输出目录**：
```bash
mkdir -p data/skill_runs/{run_date}/long_book/subagent_result
```

**输出文件路径**：
```
data/skill_runs/{run_date}/long_book/subagent_result/{stock_name}_{symbol}_{run_date}_decision.json
```

例如：
```
data/skill_runs/2026-04-28/long_book/subagent_result/世运电路_603920.SH_2026-04-28_decision.json
```

**文件内容**：只写【单个 stock entry 对象】（21 字段），不要外层 `summary_date` / `stock_decisions` 包装。格式就是从属于 `stock_decisions` 数组的一个完整 JSON 对象。

**写完后向主 agent 回传**：只回传一句简短确认，格式为：
```
{symbol} {stock_name} 分析完成 → {文件名} | action={action_type} | 置信度={confidence_score}
```

subagent **不宜**把完整 19 字段底稿塞进回传消息中——主 agent 不需要看到详细底稿，合并脚本会自动处理。

### 步骤 8：主 agent 合并 P0 结果

主 agent 等待所有 P0 subagent 完成并确认文件落盘后，按以下步骤操作：

#### 8.1 确认所有 P0 文件已落盘

检查每个 P0 symbol 对应的 `subagent_result/{stock_name}_{symbol}_{date}_decision.json` 是否已存在：
```bash
ls data/skill_runs/{run_date}/long_book/subagent_result/
```

若某 P0 symbol 缺失文件，立即追问对应 subagent，不要跳过。

#### 8.2 执行合并脚本，将 P0 结果写入 05_decision.json

```bash
source /home/zhangbeiqing/venv/ai_stock/bin/activate && python scripts/merge_subagent_decisions.py --date {run_date} --book-type long_book
```

该脚本会：
- 从空 `05_decision.json` 骨架开始
- 遍历 `subagent_result/` 下所有 `*_decision.json` 文件
- 按 symbol 匹配后**整条替换** `stock_decisions` 中的对应 entry（新 symbol 则追加）
- 写入合并后的 `05_decision.json`
- 输出替换/新增摘要

#### 9.3 跨股票冲突裁决

主 agent 在完成合并后，检查不同股票间的逻辑冲突，二次裁决并回写。

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

#### 通用建议要求

- **P0 entry 是由 subagent 直接写入的完整底稿，合并脚本原样搬运到 05_decision.json**。主 agent 不宜重新压缩、改写、丢弃细节。
- **非本次深度分析的股票（P1）不输出到今日的 `05_decision.json`**.
- **⚠️ 生成完毕后应停住，等待人工检查决策文件**

## 阶段 B（人工确认）

### 用户确认：生成 `05_decision.json` 前需要人工确认
- **时机**：主 agent 完成全部 subagent 结果汇总后
- **内容**：用户检查逐股分析结论和逐股庭审裁决
- **操作**：用户回复“OK 生成决策”或“继续生成 JSON”
- **作用**：只有人工确认后，主 agent 才生成 `05_decision.json`

# 5. 单股分析最小规则（建议）

> **本 skill 的核心转变**：抛弃基于 PE/PB 倍数和精确目标价的"伪精确估值"体系。subagent 应读完所有材料后，凭职业直觉综合判断"当前价格是贵还是便宜"，再给出动作建议。价值投资的常识（买好公司、买得便宜、安全边际、长期视角）是最高指导思想，但不应被拆解成"PE 必须 ≤ X"这种硬规则去束缚 AI。

1. **单股负责制**：每个 subagent 只负责 1 只股票；允许读取 `03_agent_input.md`、`01_global_context.md`、`data/selection_runs/{run_date}/06_hot_news_state.json`、`data/selection_runs/{run_date}/05_board_heat_digest.json` 这 4 份共享输入，以及自己负责的 `_research.md`；不宜读取其他股票研究包。该股票 snapshot 已内嵌在 `_research.md` 中。
2. **完整阅读优先**：读取顺序固定为 `03 -> 01 -> 06 -> 05 -> 本股 04`，且这 5 份输入都应从头到尾完整读完；只有全部读完后，才允许补充搜索。`06_hot_news_state.json` 提供当前市场正在交易的热点叙事线和主题演化路径，`05_board_heat_digest.json` 提供板块资金流向全景——subagent 应结合这些数据理解该股所处板块的强弱和资金的进退方向，避免在孤立个股分析中漏掉系统性风格切换或板块虹吸效应。
3. **先执行搜索 next_day_watchlist，再下结论**：subagent 在完成本地阅读后，应先核验 next_day_watchlist 中列出的遗留跟踪点和疑点；不能直接跳过这些核验进入动作判断。
4. **凭职业直觉做价格印象，不做精确估值**：
   - 读完所有材料后，subagent 应对当前价格形成"价格印象"：`明显低估` / `偏低估` / `合理` / `略贵` / `明显高估` / `泡沫`
   - 形成价格印象的依据是综合性的：过去 3.5 年 PE/PB 中枢、历史价格区间分布、当前盈利水平与增速、行业景气度、宏观环境、新闻催化、量价配合、热点主题驱动——不依赖某个单一指标
   - **禁止**自己拍 PE/PB 倍数算目标价；**禁止**为了"显得完整"编造精确目标价
   - 如要描述价格空间，用模糊区间（如 `27-32元`）或定性表达（如 `30元附近`、`比历史中枢低 10-20%`），不应是 `30.5元` 这种精确值
   - 形成价格印象后再决定动作：`强烈买入` / `买入` / `持有` / `卖出` / `强烈卖出` / `观望`
5. **价值投资常识是最高指导思想，但不写成具体规则**：
   - 买好公司（有护城河、稳定盈利、合理 ROE）
   - 买得便宜（相对历史、相对基本面都便宜）
   - 安全边际（留出容错空间）
   - 长期视角（不被短期波动牵着走）
   - 这些是 AI 在做判断时该默默遵守的常识，不是"PE 必须 ≤ 30"这种硬规则
   - AI 可以自由裁定：在什么价位买、买多少、什么时候卖，所有判断都应隐含地服务于"长期跑赢市场"的目标
6. **预测可靠度作为信心系数，而非门槛**：
   - 高可靠度 = 信心强，动作可以更大胆
   - 低可靠度 = 信心弱，应谨慎，优先观望、减仓或不加仓
   - 但不应作为是否买入/卖出的硬性门槛
7. **决策连续性优先**：继承上一交易日的判断是默认行为。只有当出现新的事实变化（新财报、业绩预告、政策、新闻、量价异动）才重新形成判断。没有新触发器时，应明确写"沿用昨日判断，仅更新验证结果"，不宜因价格涨跌直接改变动作。
8. **异常波动必须解释**：若今日或最近一日大涨大跌、放量异动，而研究包没有足够解释，必须联网核验是否存在突发利好、利空、公告、经营数据或行业事件；只有核验后仍无证据时，才可判定为高波动品种的正常波动。
9. **不宜事项**：
   - 不宜读取其他股票研究包
   - 不得跳过 `03/01/本股04` 的完整阅读直接联网搜索
   - 不得在命中强制联网触发条件时省略搜索步骤
   - 不宜为了显得完整而编造精确目标价、PE 倍数或收益率数字
   - 不宜直接生成或覆盖最终 `05_decision.json`
   - 不宜把"AI 直觉判断"包装成虚假精确的估值结果（不要再写"用 25x PE 算出目标价 X 元"这种话）
   - 不宜无脑看多或看空；价值投资要求"贵了舍得卖，便宜了敢买"，AI 应基于材料独立判断，不应顺从市场情绪
10. **历史价格与历史估值是判断依据，不是估值锚**：subagent 应阅读研究包中"最近三年半股价区间分布"和"最近三年半最高/最低股价及对应估值指标"等内容，**用于形成价格印象**（比如"当前 PE 在历史 30% 分位 = 偏低估"），而不是用作"PE 必须落在历史 band 内"的硬规则。同时也建议调查历史极值成因（最低价时的业绩/宏观背景、最高价时的炒作驱动）作为辅助判断材料，但这是综合判断的素材，不是约束。

# 6. 结构化决策最小写法

`05_decision.json` 中每只股票应优先使用结构化字段保留完整底稿，而不是把所有内容塞进 `reason`。

**建议补充要求**：
- `05_decision.json` 的职责不是做"摘要"，而是沉淀可复用的完整结构化分析底稿。
- 若 subagent 已经完成详细分析，主 agent 默认应把这些详细内容带入 `05_decision.json`，而不是自作主张压缩成更短版本。
- 除非用户明确要求"只保留摘要"，否则不宜主动删掉大量 `key_facts`、`inferences`、`judgment_rationale`、`court`、`recommended_action`、`key_risks`、`next_day_watchlist` 的细节。
- 主 agent 允许做的事情仅限：补 `action_type`、`action_num`、解决跨股票冲突、统一少量措辞、修正明显重复或格式错误；不允许把完整底稿改写成自己想象中的简版内容。

每只股票至少包含：
1. `symbol`
2. `stock_name`
3. `scan`（今日量价扫描）
4. `deep_analysis_date`（本次分析日期 YYYY-MM-DD，填当天）
5. `history_anchor`（上一交易日判断要点，用于决策连续性）
6. `delta_summary`（今天与上次深度分析之间该股发生的变化）
7. `price_impression`（价格印象：`明显低估` / `偏低估` / `合理` / `略贵` / `明显高估` / `泡沫`）
8. `key_facts`
9. `inferences`
10. `judgment_rationale`（综合判断理由：解释为什么形成这样的价格印象、为什么这样建议动作）
11. `motion`（**仅写标签**，如 `BUY 候选` / `SELL 候选` / `HOLD 候选` / `FLAT 候选`，不写理由、不写结论）
12. `court`（正反方辩论 + `verdict` 才是最终裁决，`motion` 只是进入庭审的动议标签）（正反方 + 裁决）
13. `recommended_action`
14. `action_type`（`BUY` / `SELL` / `HOLD` / `FLAT`）
15. `action_num`
16. `price_target`（**模糊区间或定性表达**，如 `30-32元` / `30元附近`；不应是精确到分的价格）
17. `key_risks`
18. `next_day_watchlist`
19. `confidence_score`

说明：
- `recommended_action` 保留子 agent 原始主观建议和底稿口吻
- `action_type`、`action_num` 由主 agent 在人工确认后补充，用于执行层枚举消费
- `BUY` / `SELL` 如需真实执行，必要的数量与执行价信息应在最终 JSON 中可读；如果当天不交易，`action_num` 仍应显式给出当前持仓或 `0`
- **不再有"风险收益比"和"预期收益率"的硬性要求**；AI 凭职业直觉做判断，庭审议程验证判断的合理性
- **不再有"止盈止损"硬性阈值**；AI 可在 `judgment_rationale` / `key_risks` 中提及风险点和卖出时机逻辑，但不强制要求给出具体价格阈值；卖出时机由 AI 自由裁定
- **不再有"远期利润打折"、"SOTP 优先"、"单一标的 40% 上限"、"首次建仓 10% 上限"等仓位/估值硬规则**；AI 凭价值投资常识自由裁定

# 7. 庭审最小规则

在输出最终 JSON 前，应完成最小庭审：
- **motion（庭审动议）**：只写简洁标签（`BUY 候选` / `SELL 候选` / `HOLD 候选` / `FLAT 候选`），**禁止在 motion 中写理由、分析或结论**。motion 只是"申请进入庭审的入场券"，不是判断。
- **正方**：为什么这个动作是对的（基于什么事实、什么直觉）
- **反方**：最大的反对理由是什么，什么情况下这个动作会错
- **court.verdict（最终裁决）**：只有这里才是最终决定与理由。verdict 必须独立于 motion、基于正反方辩论后得出。

庭审议程依然保留，但**不再以"风险收益比 / 预期收益率"为强制门槛**，而是以"AI 直觉 + 价值投资常识 + 庭审议程"为最终判断依据。

如果 AI 对自己判断信心不足，买入动议应更谨慎，但仍由 AI 自由裁定是否执行。不存在"信心低 = 一律不买"的硬规则。

# 8. subagent 文件输出规范

subagent **不通过对话上下文回传完整分析结果**。分析完成后应将结果写入文件（见步骤 7.1），回传内容仅需简短确认。
1. `symbol`
2. `stock_name`
3. `scan`
4. `deep_analysis_date`
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

不要求额外包装复杂 JSON，但字段语义应完整、可直接被 `merge_subagent_decisions.py` 合并脚本读取并写入最终 `05_decision.json`。

**主 agent 汇总约束（建议）**：
- subagent 写入文件的 JSON 是 `05_decision.json` 各股条目的唯一来源主体正文。
- 合并脚本 (`merge_subagent_decisions.py`) 负责将文件原样搬运到 `05_decision.json` 的 `stock_decisions` 数组中。
- 主 agent 不宜因为担心文件太长、担心卡住、想节省篇幅等原因，私自修改 subagent 已写入的文件内容或合并后的 entry。
- **需要的文件可以要完整读完，不要只读一部分！！金融相关分析完整文件很重要**

**注：非常重要。你可以自由裁定是否卖出，本skill只限制应卖出的情景，非应卖出情景，你可以根据当前综合形势，发动主观能动性确定是否卖出。应卖出是为了兜底止损防止亏太多，至于如何止盈、看情况不妙何时及时跑路，你可以自己决定**
