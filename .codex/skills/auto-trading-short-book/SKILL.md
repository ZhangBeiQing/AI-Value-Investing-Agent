---
name: auto-trading-short-book
description: >
  动态短线股票池每日股票自动交易程序。Agent 读取用户已经准备好的短线股票池中的每日最新股票数据，
  根据用户规则详细分析股票后，生成每日交易决策 JSON，经人工确认后，再执行交易、然后生成每日操盘总结，
  最后合并到历史操盘总结中。短线池上限 7 只，最大持仓 20 个交易日，到限不论盈亏应了结。
  当用户要求"开始今天短线股票池交易"时触发。
---

# 1. 适用场景 / 触发说明
- 用户说"开始今天短线股票池交易"。
- 目标：通过固定流程生成决策文件，人工确认后执行交易与总结归档。
- 短线核心约束：池上限 7 只，最大持仓 20 个交易日（到限应了结）
- **股票池来源**：`data/selection_runs/YYYY-MM-DD/12_quant_prefilter_short.csv`（由 `refresh_all_for_date.py --generate-prefilter` 自动生成，量化初筛短期 Top20）。

# 2. Python 环境要求（应遵守）
本技能的所有脚本应在指定的 Python 虚拟环境中运行：

- **虚拟环境路径**：`/home/zhangbeiqing/venv/ai_stock`
- **Python 解释器**：`/home/zhangbeiqing/venv/ai_stock/bin/python`

**激活环境方式**：
```bash
source /home/zhangbeiqing/venv/ai_stock/bin/activate
```

# 3. 输出目录规范（应遵守）
- 统一输出到：`data/skill_runs/YYYY-MM-DD/short_book`

每日股票资料产物结构介绍：
- `01_global_context.md`（宏观/大盘/渐进式新闻总结，中间产物；请在阅读 03 中规则后再打开）
- `02_basic_snapshot_payload.json`（《基本面数据概览》：basic_stock_info 生成的 basic snapshot；用于 Step 0 快速扫描与定价基准）
- `03_agent_input.md`（最终 user_query 输入；应先读它并完成建议输出，再读其他文件）
- `04_stock_research/`（个股研究包文件夹：新闻/公告/估值/财报等详细信息；每个文件按 symbol 命名）
- `05_decision.json`（Agent 决策输出，由当前 Agent 在对话中生成）

**上游产物（由 refresh_all_for_date.py 生成）**：
- `data/selection_runs/YYYY-MM-DD/12_quant_prefilter_short.csv`（量化初筛短期 Top20，本 skill 的股票池来源）
- `data/selection_runs/YYYY-MM-DD/12_factor_snapshot.csv`（因子宽表，辅助参考）
- `data/selection_runs/YYYY-MM-DD/06_hot_news_state.json`（热点主题状态）
- `data/selection_runs/YYYY-MM-DD/05_board_heat_digest.json`（板块热度概览）

# 4. 标准工作流（全池并行 subagent）

## 日期选择说明
- 若用户触发 Skill 时明确提供了日期（格式：`YYYY-MM-DD`），则使用用户指定日期
- 若用户未提供日期，默认使用上一个交易日（today - 1）；若昨天是周末或节假日非交易日，向前查找最近一个交易日
- 主 agent 据此日期定位到 `data/skill_runs/YYYY-MM-DD/short_book/` 目录，所有文件读取和决策输出都基于该日期目录

## 执行模式说明
本技能采用"主 agent 对池中所有股票并行派发 subagent"的方式，每只股票一个独立 subagent，上限 7 个并发。

**上下文共享边界**：
- 主 agent 只读取 `SKILL.md`、`03_agent_input.md`、`02_basic_snapshot_payload.json`、`01_global_context.md`，以及 `data/selection_runs/{run_date}/12_quant_prefilter_short.csv`、`data/selection_runs/{run_date}/06_hot_news_state.json`、`data/selection_runs/{run_date}/05_board_heat_digest.json`。**主 agent 不宜读任何股票的 `_research.md`**——深度阅读是 subagent 的工作。
- 每个 subagent 读取共享输入顺序固定为：`03_agent_input.md -> 01_global_context.md -> data/selection_runs/{run_date}/06_hot_news_state.json -> data/selection_runs/{run_date}/05_board_heat_digest.json -> 本股 04_stock_research/{symbol}_research.md`。该股 snapshot 已写入研究包，subagent 不单独读 `02_basic_snapshot_payload.json`。
- subagent 建议只读 `01/03/06/05` + 自己那只 `_research.md`，不宜读其他股票研究包。
- 所有 subagent 完成后把结果写入文件，主 agent 负责汇总并最终写入 `05_decision.json`。

## 阶段 A（自动，由主 agent 完成统筹）

### 步骤 1：确认日期和定位目录
根据上述日期选择规则确定交易日，定位到 `data/skill_runs/YYYY-MM-DD/short_book/` 目录。

### 步骤 2：读取分析指引和规则
主 agent 先读取 `03_agent_input.md`：
- 先理解规则与输出格式
- 按照 `03_agent_input.md` 要求输出你对规则的理解和持仓信息到终端
- **⚠️ 特别注意**：`03_agent_input.md` 中包含的【最终总结生成规则】(`final_summary_rules`) 定义了 `05_decision.json` 的输出格式，应牢记此格式

### 步骤 3：读取基本面数据概览
主 agent 阅读 `02_basic_snapshot_payload.json`，用于快速扫描各股最新快照、仓位、持仓天数。

说明：`02_basic_snapshot_payload.json` 仅由主 agent 使用；subagent 不读取，每只股票对应的 snapshot 已在研究包中。

### 步骤 4：读取宏观、大盘上下文
主 agent 阅读 `01_global_context.md`，提炼：
- 宏观、指数、流动性、事件风险背景（第 1–2 节）
- 重点排查 S 级系统性风险

### 步骤 4.5：读取热点新闻主题状态与板块热度

主 agent 快速浏览 `data/selection_runs/{run_date}/06_hot_news_state.json` 和 `data/selection_runs/{run_date}/05_board_heat_digest.json`，了解当前市场主线叙事（粗略感知即可，深度分析由 subagent 自行完成）。

**文件不存在时的处理**：
- 若 `06_hot_news_state.json` 或 `05_board_heat_digest.json` 不存在，跳过此步骤；不要因此中止流程

### 步骤 5：读取分析历史索引

**目的**：了解每只股票的最后深度分析日期，帮助判断催化连续性和是否需要重新分析。

读取 `data/skill_runs/_analysis_index.json`，该文件由 `merge_subagent_decisions.py` 在每次合并 P0 结果时自动更新，记录各账本每只股票的最后深度分析日期、价格印象和置信度。

- 若某只股票从未被深度分析过（不在索引中），标记为冷启动。
- 若上次深度分析距今较远（建议超过 5-10 个交易日），且该股为持仓股或出现异常波动，应优先纳入 P0 深度分析。
- **不再需要 `cp` 上一交易日的 `05_decision.json`**——分析历史全部由 `_analysis_index.json` 管理。

### 步骤 6：确认股票池并等待用户确认

主 agent 读取 `data/selection_runs/{run_date}/12_quant_prefilter_short.csv` 获取今日短线候选股票池（Top20），结合当前持仓状态筛选出当日分析池（上限 7 只），向用户输出：
- 全量 Top20 列表（名称 + symbol + short_score + 今日涨幅）
- 当前持仓股标注持仓天数，已满 20 个交易日建议退出
- 当日分析池选出逻辑（优先持仓股、优 high-score + 强催化确认）
- **暂停等待确认**：等待用户回复"OK 继续"或类似确认后，进入步骤 7

### 步骤 7：主 agent 对全池并行派发 subagent

主 agent 对池中每只股票分配一个独立 subagent 并行执行（热点主题和板块热度由 subagent 自行从 `06_hot_news_state.json` 和 `05_board_heat_digest.json` 中读取分析）。每个 subagent 的任务边界：
- 只负责 1 只股票
- 应先按顺序完整读取 `03_agent_input.md` → `01_global_context.md` → `data/selection_runs/{run_date}/06_hot_news_state.json` → `data/selection_runs/{run_date}/05_board_heat_digest.json` → 自己那只 `04_stock_research/{symbol}_research.md`；如文件较长应分段顺序读到末尾
- 读完本股研究包后**自行整理 `search_brief` 自检清单**（主 agent 不会下发 brief），至少覆盖：① 研究包中已载明的上一交易日 `next_day_watchlist` 遗留跟踪点；② 今日异常涨跌 / 放量待解释问题；③ 需要核验的高时效事实
- 逐项判断自检清单后决定是否联网补证；若清单中有待核验目标或命中任一硬触发条件，联网是必须步骤
- 不宜读其他股票研究包；不宜改写最终 `05_decision.json`

subagent 联网补证的硬触发条件至少包括：
- "最近一次交易日历史交易总结" `next_day_watchlist` 有今天应跟踪的遗留问题
- 今日或最近一日出现明显大涨大跌、放量异动，但研究包现有新闻 / 公告 / 财报无法解释
- 研究包中的新闻、公告、经营数据存在明显滞后、缺失、未知或相互矛盾
- 你准备提出 `BUY` / `SELL`，但关键论据依赖可能已变化的外部事实
- 你自己在阅读后明确感到"这里如果不联网，我无法区分是正常波动还是新的基本面 / 事件驱动"

### 步骤 7：subagent 逐股输出标准化结果

每个 subagent 按 skill_flow 中 Step 2 的要求完成分析后，把结果返回给主 agent：
- 当前催化剂与动能评级（`high` / `medium` / `low`），以及催化能否在 1-5 个交易日内兑现
- 今日量价确认信号分析
- 交易定性模式（`event_driven` / `technical_breakout` / `momentum_following` / `oversold_bounce` / `no_clear_catalyst`）
- 价格印象（`price_impression`）：明显低估 / 偏低估 / 合理 / 略贵 / 明显高估 / 泡沫
- 综合判断理由（`judgment_rationale`）
- 交易动议初筛
- 正反方辩论
- 法官裁决
- 明确的最大持仓天数（≤20 个交易日，仍受短线池硬约束）、退出条件、催化证伪条件

subagent 写入文件的 JSON 应结构化包含以下字段：
1. `symbol`
2. `stock_name`
3. `scan`
4. `deep_analysis_date`
5. `history_anchor`
6. `delta_summary`
7. `price_impression`
8. `catalyst_and_momentum`
9. `trading_mode`
10. `key_facts`
11. `inferences`
12. `judgment_rationale`
13. `risk_reward_setup`
14. `motion`（**仅写标签**，如 `BUY 候选` / `SELL 候选` / `HOLD 候选` / `FLAT 候选`，不写理由、不写结论）
15. `court`（正反方辩论 + `verdict` 才是最终裁决，`motion` 只是进入庭审的动议标签）
16. `recommended_action`
17. `action_type`
18. `action_num`
19. `price_target`
20. `max_holding_days`
21. `key_risks`
22. `next_day_watchlist`
23. `confidence_score`

**输出要求**：
- 应写成可复用的"短线交易记忆锚"
- 应区分"已核实事实"和"基于事实的推断"
- 每笔买入应绑定：明确的退出条件（最大持仓天数 ≤20 + 催化证伪条件 + 破位/止损逻辑），具体止损价由 AI 凭职业直觉给定，不强制要求 5-8% 阈值
- subagent 应在写入的文件内容中明确体现自行整理的 `search_brief` 自检清单每一项是否已有新进展；不能跳过不答
- 若今日存在异常涨跌或放量，且研究包本地材料不足以解释，subagent 应先联网补证

#### 7.1 subagent 分析完成后：文件落盘（建议）

subagent 完成上述分析后，**应将结果写入文件**，而不是通过对话上下文回传完整底稿给主 agent。

**输出目录**：
```bash
mkdir -p data/skill_runs/{run_date}/short_book/subagent_result
```

**输出文件路径**：
```
data/skill_runs/{run_date}/short_book/subagent_result/{stock_name}_{symbol}_{run_date}_decision.json
```

**文件内容**：只写【单个 stock entry 对象】（23 字段），不要外层 `summary_date` / `stock_decisions` 包装。

**写完后向主 agent 回传**：只回传一句简短确认，格式为：
```
{symbol} {stock_name} 分析完成 → {文件名} | action={action_type} | 置信度={confidence_score}
```

subagent **不宜**把完整底稿塞进回传消息中——主 agent 不需要看到详细底稿，合并脚本会自动处理。

### 步骤 9：主 agent 汇总全池结果

主 agent 等待所有 subagent 完成并确认文件落盘后，按以下步骤操作：

#### 9.1 确认所有文件已落盘

检查每个 symbol 对应的 `subagent_result/{stock_name}_{symbol}_{date}_decision.json` 是否已存在：
```bash
ls data/skill_runs/{run_date}/short_book/subagent_result/
```

若某 symbol 缺失文件，立即追问对应 subagent，不要跳过。

#### 8.2 执行合并脚本，将结果写入 05_decision.json

```bash
source /home/zhangbeiqing/venv/ai_stock/bin/activate && python scripts/merge_subagent_decisions.py --date {run_date} --book-type short_book
```

该脚本会：
- 从空 `05_decision.json` 骨架开始
- 遍历 `subagent_result/` 下所有 `*_decision.json` 文件
- 按 symbol 匹配后**整条替换** `stock_decisions` 中的对应 entry（新 symbol 则追加）
- 写入合并后的 `05_decision.json`
- 输出替换/新增摘要

#### 9.3 出池股票处理

若基线中存在某只 symbol 但今日已不在股票池，从 `stock_decisions` 中删除该 entry。

#### 8.3 跨股票冲突裁决

主 agent 检查不同股票间的逻辑冲突，二次裁决并回写。

#### 8.4 向用户展示汇总结果

明确分类呈现：
- 逐只列出 symbol + action 结论（从 subagent 回传的确认信息汇总即可）
- 标注持仓满 20 个交易日的股票

- **⚠️ 到此应停住，等待人工确认**

### 步骤 9：人工确认后完成最终决策文件

**⚠️ 只有在用户明确回复"OK 生成决策"或类似确认后，主 agent 才执行此步骤**

此时 `05_decision.json` 中所有 entry 已由合并脚本（步骤 8.2）写入。步骤 9 只需完成收尾工作：

#### 9.1 更新顶层字段

用 Edit 工具修改 `05_decision.json` 的顶层字段：
- `summary_date`：改为今日日期
- `system_risk_notes`：写今日宏观风险提示（不继承昨日）
- `system_focus_items`：写今日关注点（不继承昨日）

#### 通用建议要求

- **P0 entry 是由 subagent 直接写入的完整底稿，合并脚本原样搬运到 05_decision.json**。主 agent 不宜重新压缩、改写、丢弃细节。
- **⚠️ 生成完毕后应停住，等待人工检查决策文件**

## 阶段 B（人工确认）

### 用户确认：生成 `05_decision.json` 前需要人工确认
- **时机**：主 agent 完成全部 subagent 结果汇总后
- **内容**：用户检查逐股分析结论和逐股庭审裁决
- **操作**：用户回复"OK 生成决策"或"继续生成 JSON"
- **作用**：只有人工确认后，主 agent 才最终完成 `05_decision.json`

# 5. 单股分析最小规则（建议）

> **本 skill 的核心转变**：抛弃基于 PE/PB 倍数和精确目标价的"伪精确估值"体系。短线交易的核心是催化剂 + 量价 + 风险收益直觉，subagent 应读完所有材料后，凭职业直觉综合判断"是否值得开仓 / 加仓 / 持有 / 平仓"。价值投资常识仍是指导思想（买好公司、买得便宜、安全边际），但短线更看重"近端催化 + 量价确认 + 风险可控"，不写 PE 目标价。

1. **单股负责制**：每个 subagent 只负责 1 只股票；允许读取 `03_agent_input.md`、`01_global_context.md` 这 2 份共享输入，以及自己负责的 `_research.md`；不宜读取其他股票研究包。该股票 snapshot 已内嵌在 `_research.md` 中。
2. **完整阅读优先**：读取顺序固定为 `03 -> 01 -> 本股 04`，且这 3 份输入都应从头到尾完整读完；只有全部读完后，才允许补充搜索。若主 agent 在 prompt 中提供了热点主题上下文（提炼自 `06_hot_news_state.json` / `05_board_heat_digest.json`），应在阅读完本地文件后将该主题信息作为辅助分析背景。
3. **先执行搜索 next_day_watchlist，再下结论**：subagent 在完成本地阅读后，应先核验 next_day_watchlist 中列出的遗留跟踪点和疑点；不能直接跳过这些核验进入动作判断。
4. **凭职业直觉做交易判断，不做精确估值**：
   - 短线交易的核心抓手是：**催化剂 + 量价 + 风险收益直觉**，不是"PE 多少倍 → 目标价多少"
   - 读完所有材料后，subagent 应回答：
     - 当前催化剂与动能评级（`high` / `medium` / `low`）：是否有明确近端事件催化、市场是否通过量价确认、是否面临近端重大风险
     - 交易定性模式（`event_driven` / `technical_rebound` / `trend_following` / `no_clear_catalyst`）
     - 风险收益直觉：向上赔率 vs 向下风险，是否值得开仓
     - 当前价格印象（`明显低估` / `偏低估` / `合理` / `略贵` / `明显高估` / `泡沫`）——仅作为"贵不贵"的快速判断，不作为单独否决项
     - 最终动作候选与明确的退出逻辑
   - **禁止**自己拍 PE/PB 倍数算目标价；**禁止**为了"显得完整"编造精确目标价
   - 如要描述价格空间，用模糊区间或定性表达（如"短线第一阻力在 32 元附近"），不应是 `32.5元` 这种精确值
5. **价值投资常识是最高指导思想，但短线更看催化**：
   - 短线可以因为催化明确 + 量价确认 + 风险可控而买入"偏贵但有动能"的标的
   - 短线必须明确退出逻辑（破位、催化证伪、达到目标、最大持仓天数到限）
   - 短线不应长期持有逻辑不清晰的票——超过 20 个交易日的票，不论盈亏都应了结
6. **动能与催化低时从严**：若 `catalyst_and_momentum.level` 为 `low`，默认不宜新开仓或加仓；已持仓股票只需要回答逻辑是否证伪、是否触发退出条件、或继续持有等待验证。
7. **异常波动必须解释**：若今日或最近一日大涨大跌、放量异动，而研究包没有足够解释，必须联网核验是否存在突发利好、利空、公告、经营数据或行业事件；只有核验后仍无证据时，才可判定为高波动品种的正常波动。
8. **不宜事项**：
   - 不宜读取其他股票研究包
   - 不得跳过 `03/01/本股04` 的完整阅读直接联网搜索
   - 不得在命中强制联网触发条件时省略搜索步骤
   - 不宜长篇大论计算 PE/PB 倍数而忽略近端催化与量价
   - 不宜为了显得完整而编造精确目标价、PE 倍数或收益率数字
   - 不宜直接生成或覆盖最终 `05_decision.json`
   - 不宜无脑看多或看空；subagent 应基于材料独立判断，不应顺从市场情绪

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
8. `catalyst_and_momentum`（催化剂与动能评级）
9. `trading_mode`（交易定性模式）
10. `key_facts`
11. `inferences`
12. `judgment_rationale`（综合判断理由：解释为什么形成这样的价格印象、催化评估、动作建议）
13. `risk_reward_setup`（风险收益直觉：向上赔率 vs 向下风险，定性描述）
14. `motion`（**仅写标签**，如 `BUY 候选` / `SELL 候选` / `HOLD 候选` / `FLAT 候选`，不写理由、不写结论）
15. `court`（正反方辩论 + `verdict` 才是最终裁决，`motion` 只是进入庭审的动议标签）（正反方 + 裁决）
16. `recommended_action`
17. `action_type`（`BUY` / `SELL` / `HOLD` / `FLAT`）
18. `action_num`
19. `price_target`（**模糊区间或定性表达**，如 `32元附近` / `30-32元`；不应是精确到分的价格）
20. `max_holding_days`（最大持仓天数，**仍受 ≤20 交易日硬约束**——这是短线池核心规则）
21. `key_risks`
22. `next_day_watchlist`
23. `confidence_score`

说明：
- `recommended_action` 保留子 agent 原始主观建议和底稿口吻
- `action_type`、`action_num` 由主 agent 在人工确认后补充，用于执行层枚举消费
- `BUY` 应绑定明确的退出条件（`max_holding_days ≤ 20` + 催化证伪条件 + 破位/止损逻辑），但不强制要求具体止损价
- `SELL` 应写明卖出原因（破位触发 / 催化证伪 / 20 天到限 / 自由裁定）
- 如果当天不交易，`action_num` 仍应显式给出当前持仓或 `0`
- **不再有"风险收益比"和"预期收益率"的硬性要求**；AI 凭职业直觉做判断，庭审议程验证判断的合理性
- **不再有"止盈止损"硬性价格阈值**；AI 可在 `judgment_rationale` / `key_risks` 中提及破位/止损逻辑，但不强制要求给出具体价格；卖出时机由 AI 自由裁定
- **不再有"远期利润打折"、"SOTP 优先"等估值硬规则**；AI 凭价值投资常识自由裁定

# 7. 庭审最小规则

在输出最终 JSON 前，应完成最小庭审：
- **motion（庭审动议）**：只写简洁标签（`BUY 候选` / `SELL 候选` / `HOLD 候选` / `FLAT 候选`），**禁止在 motion 中写理由、分析或结论**。motion 只是"申请进入庭审的入场券"，不是判断。
- **正方**：为什么在当前时间窗内，催化、量价确认和盈亏比值得买入、卖出或继续持有
- **反方**：催化是否伪证/一日游？量价是否背离？3天赔率是否真的划算？近端风险是否排除（财报/解禁/减持/监管）？
- **court.verdict（最终裁决）**：只有这里才是最终决定与理由。最终动作 + 退出条件（最大持仓天数 ≤20 个交易日 + 催化证伪条件 + 破位/止损逻辑）。verdict 必须独立于 motion、基于正反方辩论后得出。

庭审议程依然保留，但**不再以"风险收益比 / 预期收益率"为强制门槛**，而是以"AI 直觉 + 短线交易常识 + 庭审议程"为最终判断依据。

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
8. `catalyst_and_momentum`
9. `trading_mode`
10. `key_facts`
11. `inferences`
12. `judgment_rationale`
13. `risk_reward_setup`
14. `motion`（**仅写标签**，如 `BUY 候选` / `SELL 候选` / `HOLD 候选` / `FLAT 候选`，不写理由、不写结论）
15. `court`（正反方辩论 + `verdict` 才是最终裁决，`motion` 只是进入庭审的动议标签）
16. `recommended_action`
17. `action_type`
18. `action_num`
19. `price_target`
20. `max_holding_days`
21. `key_risks`
22. `next_day_watchlist`
23. `confidence_score`

不要求额外包装复杂 JSON，但字段语义应完整、可直接被 `merge_subagent_decisions.py` 合并脚本读取并写入最终 `05_decision.json`。

**主 agent 汇总约束（建议）**：
- subagent 写入文件的 JSON 是 `05_decision.json` 各股条目的唯一来源主体正文。
- 合并脚本 (`merge_subagent_decisions.py`) 负责将文件原样搬运到 `05_decision.json` 的 `stock_decisions` 数组中。
- 主 agent 不宜因为担心文件太长、担心卡住、想节省篇幅等原因，私自修改 subagent 已写入的文件内容或合并后的 entry。
- **需要的文件可以要完整读完，不要只读一部分！！金融相关分析完整文件很重要**
