---
name: auto-trading-fixed-tracked
description: >
  固定股票池每日股票自动交易程序。Agent 读取用户已经准备好的固定股票池中的每日最新股票数据，根据用户规则详细分析股票后，生成每日交易决策 JSON，经人工确认后，再执行交易、然后生成每日操盘总结，最后合并到历史操盘总结中。
  当用户要求“开始今天固定股票池交易”时触发。
---

# 1. 适用场景 / 触发说明
- 用户说“开始今天固定股票池交易”。
- 目标：通过固定流程生成决策文件，人工确认后执行交易与总结归档。

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
- `06_execution_log.json`（执行器记录买卖）
- `07_daily_summary.json`（每日操盘总结）
- `08_history_merge.json`（合并后的历史操盘总结）

# 4. 标准工作流（优先级队列 + P0 并行 subagent，半自动确认）

## 日期选择说明
- **用户在触发 Skill 时会提供日期**（格式：`YYYY-MM-DD`）
- 主 agent 根据此日期定位到 `data/skill_runs/YYYY-MM-DD/fixed_tracked/` 目录
- 所有文件读取和决策输出都基于该日期目录

## 执行模式说明（重要）
本技能采用"主 agent 建立优先级队列 + 仅对 P0 并行派发 subagent"的方式，以控制 Opus 等高成本模型的 token 消耗并提升逐股深度。

**上下文共享边界（必须理解）**：
- 主 agent 只读取 `SKILL.md`、`03_agent_input.md`、`02_basic_snapshot_payload.json`、`01_global_context.md`（含末尾 `yesterday_digest` 表）。**主 agent 禁止读任何股票的 `_research.md`**——深度阅读是 subagent 的工作。
- 主 agent 基于上述输入建立 P0（并行深度分析，≤5 只）/ P1（主 agent 快扫结论）/ P2（跳过）三档队列，仅对 P0 派发并行 subagent。
- 每个 subagent 读取共享输入顺序固定为：`03_agent_input.md -> 01_global_context.md -> 本股 04_stock_research/{symbol}_research.md`。该股 snapshot 已写入研究包，subagent 不单独读 `02_basic_snapshot_payload.json`。
- **主 agent 派发 P0 subagent 时不要预写 `search_brief`**——subagent 在读完自己那只股的研究包后自行整理自检清单（至少覆盖该股 `next_day_watchlist` 遗留跟踪点、今日异常涨跌/放量待解释问题、需要核验的高时效事实）。
- `03_agent_input.md` 中"强制输出"仍由主 agent 对用户执行；subagent 须完整阅读规则，但不需重复向用户输出同一段。
- subagent 只能读 `01/03` + 自己那只 `_research.md`，不得读其他股票研究包；分析完后把完整庭审底稿传回主 agent；主 agent 负责汇总 P0 详细底稿 + P1 快扫 + P2 跳过记录，最终写入 `05_decision.json`。

## 阶段 A（自动，由主 agent 完成统筹）

### 步骤 1：确认日期和定位目录
根据用户提供的日期（`YYYY-MM-DD`），定位到 `data/skill_runs/YYYY-MM-DD/fixed_tracked/` 目录。

### 步骤 2：读取分析指引和规则
主 agent 先读取 `03_agent_input.md`：
- 先理解规则与输出格式
- 按照 `03_agent_input.md` 要求输出你对规则的理解和持仓信息到终端
- **⚠️ 特别注意**：`03_agent_input.md` 中包含的【最终总结生成规则】(`final_summary_rules`) 定义了 `05_decision.json` 的输出格式，必须牢记此格式

### 步骤 3：读取基本面数据概览
主 agent 阅读 `02_basic_snapshot_payload.json`，用于快速扫描各股最新快照、仓位、初始定价锚点。

说明：`02_basic_snapshot_payload.json` 仅由主 agent 使用；subagent 不读取，每只股票对应的 snapshot 已在研究包中。

### 步骤 4：读取宏观、大盘与昨日账本摘要
主 agent 阅读 `01_global_context.md`，提炼：
- 宏观、指数、流动性、事件风险背景（第 1–2 节）
- 末尾 `yesterday_digest` 表（第 3 节）：每股 `last_deep_scan_date` / `last_action_type` / `price_target` / `stop_loss`，供 Step 2 建立优先级队列与防饥饿机制使用
- 若 yesterday_digest 缺失（首次运行或无历史决策），P0 筛选退化为"仅基于今日量价 + 持仓异动"，防饥饿机制本日不触发

### 步骤 5：主 agent 建立 P0/P1/P2 优先级队列
结合当日持仓、`02_basic_snapshot_payload.json` 的量价扫描、Step 0 + Step 1 的宏观判定、以及 `01_global_context.md` 末尾的 `yesterday_digest`，主 agent 把股票池分成三档：

- **P0 级（并行深度分析，≤ 5 只）**：
  1. 昨日持仓且今日跌幅 / 波动异常
  2. 未持仓但今日波动异常（大涨 >5% / 大跌 >5%，涨停跌停优先）
  3. 处于"财报危险期"的持仓股
  4. Step 0 / Step 1 识别到的极具吸引力潜在买入目标
  5. **【防饥饿机制】** 对照 `yesterday_digest`，若某股 `last_deep_scan_date` 距今 >10 天未深度分析，今日强制升 P0（**一天最多强制一只**）
- **P1 级（主 agent 快扫结论）**：股价平稳的持仓股、观察仓。主 agent 不派 subagent，也不读 `_research.md`；仅基于 snapshot + yesterday_digest + 01 宏观，直接在 05 中给出 HOLD / FLAT 简明底稿（允许沿用上一次锚点并写明"逻辑未变"）。
- **P2 级（记录跳过）**：无持仓且无明显机会，`last_deep_scan_date` 距今 ≤10 天的股票，05 中记一行"已略过，理由：近期已深度分析"。

若 P0 候选过多（>5 只），按"昨日持仓异动 > 财报危险期 > 强催化非持仓 > 防饥饿触发"顺序择优裁剪到 5 只以内。

### 步骤 6：主 agent 对 P0 队列并行派发 subagent
主 agent 对 P0 队列中的每只股票分配一个独立 subagent 并行执行。每个 subagent 的任务边界：
- 只负责 1 只股票
- 必须先按顺序完整读取 `03_agent_input.md` → `01_global_context.md` → 自己那只 `04_stock_research/{symbol}_research.md`；如文件较长必须分段顺序读到末尾
- 读完本股研究包后**自行整理 `search_brief` 自检清单**（主 agent 不会下发 brief），至少覆盖：① 研究包中已载明的上一交易日 `next_day_watchlist` 遗留跟踪点；② 今日异常涨跌 / 放量待解释问题；③ 需要核验的高时效事实
- 逐项判断自检清单后决定是否联网补证；若清单中有待核验目标或命中任一硬触发条件，联网是强制步骤
- 不得读其他股票研究包；不得改写最终 `05_decision.json`

subagent 联网补证的硬触发条件至少包括：
- "最近一次交易日历史交易总结" `next_day_watchlist` 有今天应跟踪的遗留问题
- 今日或最近一日出现明显大涨大跌、放量异动，但研究包现有新闻 / 公告 / 财报无法解释
- 研究包中的新闻、公告、经营数据存在明显滞后、缺失、未知或相互矛盾
- 你准备提出 `BUY` / `SELL`，但关键论据依赖可能已变化的外部事实
- 你自己在阅读后明确感到"这里如果不联网，我无法区分是正常波动还是新的基本面 / 事件驱动"

### 步骤 7：subagent 逐股输出标准化结果
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

subagent 回传结果必须结构化包含以下字段：
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
- subagent 必须在回传内容里明确体现自行整理的 `search_brief` 自检清单每一项是否已有新进展；不能跳过不答
- 若今日存在异常涨跌或放量，且研究包本地材料不足以解释，subagent 必须先联网补证，再决定是"事件驱动"还是"高波动正常波动"

### 步骤 8：主 agent 汇总 P0 底稿 + P1 快扫 + P2 跳过记录
主 agent 收集所有 P0 subagent 返回结果后，再叠加自己对 P1 的快扫结论与 P2 跳过记录，统一完成：
- 检查是否所有股票都覆盖
- 检查是否满足 `03_agent_input.md` 的输出契约
- 发现不同股票间的逻辑冲突时，由主 agent 二次裁决并回写
- **强制要求：主 agent 写入 `05_decision.json` 的结构化结果，必须尽量保留 subagent 的完整底稿，不得为了省 token、图省事或追求简洁而自行压缩、改写、丢弃大量细节**
- **强制要求：`key_facts`、`inferences`、`court`、`recommended_action`、`next_day_watchlist` 等字段应优先沿用 subagent 的详细结果；主 agent 只允许做必要的冲突裁决、字段补齐、少量措辞统一和执行字段补充，不得把完整底稿缩写成几句摘要**
- 向用户展示“逐股分析 + 逐股庭审”的汇总结果
- **⚠️ 到此必须停住，等待人工确认**

### 步骤 9：人工确认后生成决策文件
**⚠️ 只有在用户明确回复"OK 生成决策"或类似确认后，主 agent 才执行此步骤**
- 将所有 subagent 结果整理成 JSON 格式
- **严格遵守** `03_agent_input.md` 中的【最终总结生成规则】格式
- 生成 `data/skill_runs/YYYY-MM-DD/fixed_tracked/05_decision.json`
- `05_decision.json` 中每只股票应以 subagent 风格结构化字段为主；主 agent 仅在人工确认后补充 `action_type`、`action_num` 等执行字段
- **强制要求：写入 `05_decision.json` 时，每只股票的主体内容必须尽量保留 subagent 的完整详细结果，而不是主 agent 自己重新压缩成短摘要。除非用户明确要求缩写，否则不得主动删减大量事实、推断、庭审细节和 watchlist。**
- **强制要求：如果 subagent 已经给出更详细的 `key_facts`、`inferences`、`court`、`recommended_action`、`key_risks`、`next_day_watchlist`，主 agent 默认应原样或近原样写入；禁止把“详细底稿”降级成“简略版底稿”。**
-  `05_decision.json`生成时建议一只一只股票的生成，防止一次性写入太多导致文件生成失败。最终文件生成完成后再次停住，等待人工检查决策文件

## 阶段 B（人工确认）

### 用户确认：生成 `05_decision.json` 前需要人工确认
- **时机**：主 agent 完成全部 subagent 结果汇总后
- **内容**：用户检查逐股分析结论和逐股庭审裁决
- **操作**：用户回复“OK 生成决策”或“继续生成 JSON”
- **作用**：只有人工确认后，主 agent 才生成 `05_decision.json`

# 5. 单股分析最小规则（强制）

1. **单股负责制**：每个 subagent 只负责 1 只股票；允许读取 `03_agent_input.md`、`01_global_context.md` 这 2 份共享输入，以及自己负责的 `_research.md`；不得读取其他股票研究包。该股票 snapshot 已内嵌在 `_research.md` 中。
2. **完整阅读优先**：读取顺序固定为 `03 -> 01 -> 本股 04`，且这 3 份输入都必须从头到尾完整读完；只有全部读完后，才允许补充搜索。
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

如果盈利预测可靠度为 `low`，买入动议通常应被驳回，除非出现极强、可验证的例外证据。

# 8. subagent 回传最小结构

subagent 回传给主 agent 时，至少包含以下 21 个字段：
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

不要求额外包装复杂 JSON，但字段语义必须完整、可直接被主 agent 汇总进入最终 `05_decision.json`。

**主 agent 汇总约束（强制）**：
- subagent 回传给主 agent 的结果默认视为 `05_decision.json` 的主体正文，而不是仅供主 agent 参考的草稿。
- 只要 subagent 输出已经满足字段契约，主 agent 在写 `05_decision.json` 时应尽量保留原始详细内容。
- 不得因为“担心文件太长”“担心卡住”“想节省篇幅”等原因，私自把 subagent 的详细结果压缩成简写版；若担心写入失败，应采用分批/逐只写入，而不是删细节。



##### **必须先建 P0/P1/P2 优先级队列，再仅对 P0 并行派发 subagent（上限 5 只）**
##### **必须先建 P0/P1/P2 优先级队列，再仅对 P0 并行派发 subagent（上限 5 只）**
##### **必须先建 P0/P1/P2 优先级队列，再仅对 P0 并行派发 subagent（上限 5 只）**