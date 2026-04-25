---
name: auto-trading-long-book
description: >
  动态长期股票池每日股票自动交易程序。Agent 读取用户已经准备好的长期股票池中的每日最新股票数据，根据用户规则详细分析股票后，生成每日交易决策 JSON，经人工确认后，再执行交易、然后生成每日操盘总结，最后合并到历史操盘总结中。长期池与固定池的关键差异：每日池子可能变化（新入池/剔出池），因此继承基线时必须识别集合关系。
  当用户要求“开始今天长期股票池交易”时触发。
---

# 1. 适用场景 / 触发说明
- 用户说“开始今天长期股票池交易”。
- 目标：通过固定流程生成决策文件，人工确认后执行交易与总结归档。
- 长期池相比固定池的差异：股票池每日由 `09_long_book_candidates.json` 生成，可能新增或剔除；因此继承上一交易日决策基线时，必须先识别池变化再处理。

# 2. Python 环境要求（必须遵守）
本技能的所有脚本必须在指定的 Python 虚拟环境中运行：

- **虚拟环境路径**：`/home/zhangbeiqing/venv/ai_stock`
- **Python 解释器**：`/home/zhangbeiqing/venv/ai_stock/bin/python`

**激活环境方式**：
```bash
source /home/zhangbeiqing/venv/ai_stock/bin/activate
```

# 3. 输出目录规范（必须遵守）
- 统一输出到：`data/skill_runs/YYYY-MM-DD/long_book`

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
- 主 agent 据此日期定位到 `data/skill_runs/YYYY-MM-DD/long_book/` 目录，所有文件读取和决策输出都基于该日期目录

## 执行模式说明
本技能采用"主 agent 建立优先级队列 + 仅对 P0 并行派发 subagent"的方式，以控制 Opus 等高成本模型的 token 消耗并提升逐股深度。

**上下文共享边界**：
- 主 agent 只读取 `SKILL.md`、`03_agent_input.md`、`02_basic_snapshot_payload.json`、`01_global_context.md`，以及 上一交易日的`05_decision.json`。**主 agent 禁止读任何股票的 `_research.md`**——深度阅读是 subagent 的工作，防止主 agent token上下文爆炸
- 主 agent 基于上述输入建立 P0（并行深度分析，≤5 只）/ P1（主 agent 快扫结论）仅对 P0 派发并行 subagent。
- 每个 subagent 读取共享输入顺序固定为：`03_agent_input.md -> 01_global_context.md -> 本股 04_stock_research/{symbol}_research.md`。subagent 不读 `02_basic_snapshot_payload.json`。
- subagent 在读完自己那只股的研究包后自行整理自检清单（至少覆盖该股 `next_day_watchlist` 遗留跟踪点、今日异常涨跌/放量待解释问题、需要核验的高时效事实）。
- `03_agent_input.md` 中"强制输出"仍由主 agent 对用户执行；subagent 须完整阅读规则，但不需重复向用户输出同一段。
- subagent 只能读 `01/03` + 自己那只 `_research.md`，不得读其他股票研究包；分析完后把完整庭审底稿传回主 agent；主 agent 负责汇总 P0 详细底稿 + P1 快扫，最终写入 `05_decision.json`。

## 阶段 A（自动，由主 agent 完成统筹）

### 步骤 1：确认日期和定位目录
根据上述日期选择规则确定交易日，定位到 `data/skill_runs/YYYY-MM-DD/long_book/` 目录。

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

### 步骤 5：继承上一交易日决策基线

**目的**：避免主 agent 对 非P0 股票 从头生成底稿，转而继承昨日完整底稿 + 今日局部刷新

#### 5.1 查找并 `cp` 上一交易日的继承基线

查找 `data/skill_runs/<上一交易日>/long_book/05_decision.json`：

- **存在**：执行 `cp` 到今日目录作为继承基线：

```bash
cp data/skill_runs/<prev_trading_day>/long_book/05_decision.json data/skill_runs/<today>/long_book/05_decision.json
```

- **不存在**（首次运行、连续多日缺失、或用户主动删除）：跳过 5.3~5.4，按冷启动执行：

`<上一交易日>` 不是日历昨天，是 run_date 之前最近一个含有 `long_book/05_decision.json` 的交易日（周一早上通常回看上周五；节假日后类推）。可用 `ls data/skill_runs/` 反向查找。


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

### 步骤 9：主 agent 汇总 P0 底稿 + P1 继承刷新
主 agent 收集所有 P0 subagent 返回结果后，再叠加 P1 继承刷新计划统一完成：
- 检查是否所有股票都覆盖（含 P0 新底稿、P1 继承刷新）
- 检查是否满足 `03_agent_input.md` 的输出契约
- 发现不同股票间的逻辑冲突时，由主 agent 二次裁决并回写
- **强制要求：P0 股票写入 `05_decision.json` 时必须尽量保留 subagent 的完整底稿，不得为了省 token、图省事或追求简洁而自行压缩、改写、丢弃大量细节**
- **强制要求：`key_facts`、`inferences`、`court`、`recommended_action`、`next_day_watchlist` 等字段应优先沿用 subagent 的详细结果；主 agent 只允许做必要的冲突裁决、字段补齐、少量措辞统一和执行字段补充，不得把完整底稿缩写成几句摘要**
- 向用户展示汇总结果时，必须明确分类呈现：**P0 深度分析 X₁ 只 / P1 继承刷新 Y 只（逻辑未变）/ **，并逐股给出庭审裁决
- **⚠️ 到此必须停住，等待人工确认**

### 步骤 10：人工确认后生成决策文件

**⚠️ 只有在用户明确回复"OK 生成决策"或类似确认后，主 agent 才执行此步骤**

写入策略分两条路径（由步骤 5 的结果决定）：

#### 路径 A：步骤 5 已 `cp` 继承基线（`05_decision.json` 已存在于今日目录）

这是常态路径，主目标是用 Edit 工具做**最小增量修改**，节省 Opus token。

1. **P0 股票（每只）**：用 Edit 工具**整条替换**该 symbol 在 `stock_decisions` 数组中的 entry 为 subagent 回传的完整底稿。整条替换保证 subagent 的详细内容原样进入决策文件。
2. **P1 股票（每只）**：用 Edit 工具**仅修改**以下字段：
   - `scan`（今日量价、持仓、昨收对比）
   - `action_type` / `action_num`（今日执行动作）
   - `analysis_type` 改为 `"p1_inherited_from_<prev_date>"`
   - 在 `inferences` 列表末尾追加一行今日确认
   - 必要时微调 `confidence_score`
   - **其他字段一律不改**
3. **顶层字段**：
   - Edit `summary_date` 为今日日期
   - Edit `system_risk_notes` 为今日宏观风险提示（不继承昨日）
   - Edit `system_focus_items` 为今日关注点（不继承昨日）
6. **P1新入池股票**：基线中原本就没有该 symbol，此时每个字段生成一个概览结果即可，但需注明从未深度分析。

#### 路径 B：步骤 5 无继承基线（冷启动）

1. 用 Write 工具一次性生成 `05_decision.json`，严格遵守 `03_agent_input.md` 中【最终总结生成规则】格式。
2. 建议分几只 / 几只地追加写入（先 Write 基础结构，再用 Edit 追加 `stock_decisions` 数组元素），防止一次性写入过大导致失败。
3. 每只股票按 subagent（P0）或主 agent 快扫（P1 冷启动）底稿填入。

#### 通用强制要求

- **P0 entry 必须原样或近原样保留 subagent 完整底稿**，不得为省 token 压缩成短摘要；主 agent 仅允许做：补 `action_type` / `action_num`、跨股票冲突裁决、少量措辞统一、明显格式错误修正。
- **P1 entry 的 `key_facts`、`court`、`recommended_action`、`key_risks`、`next_day_watchlist` 等字段必须原样保留自继承基线**，禁止重写成更短版本；主 agent 若发现确有必要修改（如 thesis 明显弱化），说明该股应升格为 P0，不能在 P1 流程里暗改。
- P1 / P2 的 `analysis_type` 必须显式包含 `inherited_from_<prev_date>` 标签，让后续复核能一眼看出这只股今日没做深度分析。
- **⚠️ 生成完毕后必须停住，等待人工检查决策文件**

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
9. **加速期修正（强制）**：对高景气 / 高增速行业，若简单套用 2y PE 中位数作锚 + 固定远期打折 + 不加增长期权溢价，会在 S 曲线陡峭段出现系统性低估（典型为 AI 算力链、光模块、AI PCB、AI 应用软件等 2025-2026 爆发赛道）。subagent 在估值时必须先判断本股是否落入加速期，再选择是否启用以下修正：

   **判定条件**：
   - 最近两个季度归母净利润同比增速均 ≥ 50%，且趋势向上；
   - 下游行业 12 个月订单 / 资本开支能见度明确（如北美 CSP CapEx 指引、英伟达 GPU 出货节奏、国内 AI 服务器订单）；

   **修正规则 A（PE 锚切换）**：用"过去 3 个季度滚动 TTM PE 中枢"或"行业资本开支驱动的未来中枢"替代原 2y PE 中位数作主锚；2y 中位数只作交叉参考，不作主锚。这样能在行业估值中枢本身向上台阶的情况下避免用过时锚点低估。

   **修正规则 B（远期打折放宽）**：远期利润基数打折从默认 85-90% 放宽到 **92-95%**，前提是已有连续 2 个季度财报兑现超预期、且最近一次业绩预告落在一致预期上端。未满足上述前提的仍用默认打折。

   **修正规则 C（增长期权溢价）**：若年利润增速 > 80% 且订单能见度 >12 个月，在 SOTP 或整体估值得到市值后额外加 **10-20% 增长期权溢价**（option premium），作为对市场"远期期权价值"定价的补偿。加成比例必须在 `valuation_conclusion` 中显式说明理由。

   **披露要求**：
   - 若启用 A/B/C 任一条，必须在 `valuation_conclusion` 显式写出："本股落入加速期修正，应用规则 [A/B/C]，修正前目标 X 元，修正后目标 Y 元，现价 Z 元对应预期收益率 R%"。
   - 若判定**不**落入加速期，同样需要在 `valuation_conclusion` 一句话说明"本股不落入加速期（理由：...）"，让复核可验证判断。

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