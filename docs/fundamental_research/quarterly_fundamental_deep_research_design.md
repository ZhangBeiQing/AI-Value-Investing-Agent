# 季度财报与全面基本面深度研究系统详细设计

状态：已实施（v1；保留后续 LangGraph/自动批次分组增强空间）
更新日期：2026-08-03
适用范围：`financial-report-summary`、财报研究 workdir、公司触发型产业链研究、财报前预期差研究

## 1. 文档目的

本文档定义如何把当前 `financial-report-summary` 从“单个 Agent 阅读财报并一次性输出 Markdown”，升级为一套可批量运行、可复核、可持续深化的季度基本面研究系统。

本文档应足以让一个不了解历史讨论的开发 Agent 完成后续实施。实施前必须再次核对仓库实时状态，不得仅凭本文档假设代码尚未变化。

本次设计只提出改造方案，不修改：

- `financial_reports/YYYYMMDD.md` 最终文件路径；
- `summary_index.json` 现有字段契约；
- 每日交易 `01-08` 产物契约；
- 真实交易、仓位和价格执行规则；
- `run_daily_pipeline.py` 的现有日常职责。

## 2. 目标

系统最终需要同时回答以下五类问题。

### 2.1 过去发生了什么

- 公司整体营收、归母净利润、扣非净利润、毛利率、费用率、经营现金流和自由现金流如何变化；
- 最近两年分季度同比、环比和季节性趋势如何；
- 各业务、产品、区域、客户、子公司分别贡献了多少；
- 哪些业务增长、哪些业务稳定、哪些业务拖累；
- 利润变化的主要原因和次要原因分别是什么；
- 变化来自主营经营、价格、销量、产品结构、成本、汇率、减值、补助、投资收益还是会计口径。

### 2.2 经营质量如何变化

- 利润是否转化为现金；
- 应收、存货、合同负债、预付款、在建工程和资本开支是否与收入增长匹配；
- 毛利率变化是结构性、周期性还是一次性；
- 公司解释能否被其他财务指标、同行和产业数据验证；
- 是否存在披露口径变化、业务聚合、子公司信息缺失或异常非经常损益；
- 哪些问题仍然无法被公开信息回答。

### 2.3 所在产业链是否值得投资

按程浩然方法研究：

```text
终端需求空间
→ 行业是否爆发式增长
→ 是否存在供需错配
→ 需求如何沿产业链传导
→ 利润最终留在哪个节点
→ 公司是否位于最受益节点
→ 公司是否为该节点的优质龙头
→ 景气度、理论市值空间和证伪条件
```

产业研究不能停留在宽泛行业标签。例如“AI 硬件”必须继续拆分为 PCB、光模块、液冷、存储、电源、服务器等细分产业链；PCB 还需继续拆解上游材料、覆铜板、PCB 制造和下游客户。

### 2.4 财报相对预期如何

- 财报发布前公司公开指引是什么；
- 财报前卖方年度和季度预期是什么；
- 产业经营数据隐含了什么预期；
- 财报前股价和估值已经计价了多少；
- 实际财报在收入、利润、毛利率、分业务、订单、现金流和未来指引上分别是超预期、符合预期还是不及预期；
- 超预期或不及预期的质量如何，是否可以延续；
- 新财报会促使市场上调还是下调未来盈利预测；
- “财报符合预期但预期落地”与“真正改变长期利润中枢”必须区分。

### 2.5 未来可能发生什么

- 下一季度和未来六至十二个月的核心收入、利润和现金流驱动是什么；
- 在手订单、合同负债、库存、产能、利用率、价格、客户验证和技术迭代指向什么；
- 管理层指引、机构一致预期和 Agent 自己的驱动因素预测分别是什么；
- 剩余季度需要实现多少业绩才能达到全年一致预期；
- 基准、向上和向下情景分别依赖什么条件；
- 哪些领先指标可以确认或证伪当前推演；
- 公司类型决定主要收益来源和适用估值方法。

## 3. 非目标

本系统不负责：

- 根据财报直接生成真实买卖指令；
- 替代每日交易阶段的 Bull、Bear、Juror 和 Finalizer；
- 使用投票决定财务事实是否成立；
- 扫描全市场寻找所有潜在新行业；
- 把未经证实的市场传言当作一致预期；
- 用财报后信息反向伪造财报前预期；
- 为了报告完整而虚构公司未披露的分业务数据；
- 自动修改股票池、仓位或交易规则。

## 4. 当前实现与主要问题

### 4.1 当前准备流程

入口：

```bash
python scripts/prepare_financial_report_skill.py \
  --date YYYY-MM-DD \
  --sync-first \
  --json \
  --include-quant-prefilter
```

当前每只股票的 `financial_report_workdir` 包含：

```text
01_latest_report.md
02_previous_report.md
03_report_analysis_prompt.md
04_future_outlook_prompt.md
05_agent_input.md
manifest.json
```

当前由一个 subagent 完成：

```text
阅读原始财报
→ 联网搜索
→ 知识缺口诊断
→ 再次搜索
→ 撰写最终报告
→ 更新 summary_index.json
```

### 4.2 当前已有优点

- 已要求同比、环比和季节性分析；
- 已要求公司整体与分部业务研究；
- 已要求订单、存货、应收、产能和现金流研究；
- 已区分公司公告、券商、媒体和个人来源；
- 已要求定量归因利润变化；
- 已包含未来六至十二个月预测；
- 已把财报总结嵌入后续 `04_stock_research` 研究包。

### 4.3 当前主要问题

1. 同一 Agent 同时负责发现问题、回答问题、评价答案质量，容易产生确认偏差。
2. 缺少独立的“研究质询”角色，无法自动模拟用户看完报告后不断追问的过程。
3. 财报前预期与财报后信息没有形成严格隔离。
4. 当前一致预期缓存可能在财报后被刷新覆盖，无法可靠还原财报前状态。
5. 年度一致预期容易被错误地当成季度一致预期。
6. 当前财报 Prompt 主要依赖联网搜索，没有优先复用项目已经清洗的价格、估值和同花顺预测。
7. 当前 `FUTURE_OUTLOOK_PROMPT_TEMPLATE` 预设“公司是行业龙头且竞争格局稳固”，属于未研究先下结论。
8. 当前未来 Prompt 对指标、催化剂数量存在不必要的固定限制。
9. 当前产业研究与财报研究是两个独立流程，没有公司触发型的季度产业链研究入口。
10. 当前 workdir 只有最终写作过程，没有保存预期快照、产业链研究、质询和修订过程。
11. 当前 subagent 直接更新 `summary_index.json`，多角色化以后应进一步收敛单写者。
12. 当前 skill 中仍有 `.claude/skills/...` 路径，应与仓库当前 `.codex/skills/...` 统一。

## 5. 核心设计原则

### 5.1 本地结构化数据优先

优先级：

```text
项目本地、按日期冻结的清洗数据
→ 公司原始财报和公告
→ 百炼搜索召回的权威原始来源
→ 正式卖方和持牌媒体
→ 低等级来源仅作为线索
```

项目本地已有：

- `analysis/*_股票分析报告_YYYYMMDD.md/json`
- `pe_pb_analysis/*_YYYYMMDD_enhanced_pe_analysis.md/json`
- `profit_forecast/profit_forecast.csv`
- `skill_runs/{date}/{book}/04_stock_research/*_research.md`
- 既有 `financial_reports/*.md`
- `industry_research/cards/`

这些数据应先进入 workdir，再由 Agent 搜索补缺。

### 5.2 财报前与财报后时间隔离

必须同时维护：

- `pre_announcement`：财报发布前市场可见信息；
- `current_as_of`：本次生成财报总结时最新可见信息。

任何“超预期、符合预期、不及预期”和“预期已计价”判断，必须说明使用的是哪个时间切面。

### 5.3 事实不投票

财务数字、公告内容和来源是否支持结论，不能使用多数表决。

只要一个独立 Reviewer 提出有证据的重大问题，就必须调查、修正或明确列为未解决事项。

交易阶段可以对买卖判断投票；基本面研究阶段采用：

```text
提出问题
→ 找证据
→ 解释或反驳
→ 修订
```

### 5.4 Agent 分工必须产生新信息

不能让多个 Agent 阅读同一份材料后重复写相似观点。不同角色必须拥有不同的信息边界和任务目标。

### 5.5 单写者

- 各 Agent 只写自己的文件；
- 只有 Financial Author 写最终财报 Markdown；
- `summary_index.json` 由主 Agent 调用本地注册脚本或服务更新；
- 不允许多个 Agent 同时修改同一文件。

### 5.6 不用固定条目数替代研究深度

不规定：

- 必须提出几个问题；
- 每节最多写几项；
- 必须列几个催化剂；
- 必须列几个风险。

研究深度由重大性、证据缺口和公司复杂度决定。可以设置运行预算或最大循环轮次作为系统保护，但它不是内容配额。

## 6. 时间语义

### 6.1 三个日期

系统必须区分：

| 字段 | 含义 |
| --- | --- |
| `analysis_date` | 用户本次要求生成基本面报告的日期 |
| `announcement_date` | 最新财报公告日期 |
| `pre_announcement_market_date` | 财报发布前最后一个可用市场快照日期 |

如能取得公告时间，增加：

```text
announcement_datetime
```

### 6.2 财报前市场日选择

规则：

1. 公告时间明确且在收盘后：允许使用公告当日收盘快照；
2. 公告时间在盘前或盘中：使用前一交易日；
3. 只有公告日期、没有时间：保守使用前一交易日；
4. 研究包不存在时，使用最近一个早于截止时间的本地市场快照；
5. 不允许自动使用财报发布后的研究包替代财报前快照。

### 6.3 来源时间截断

Expectation Scout 使用的所有外部来源必须满足：

```text
release_datetime < announcement_datetime
```

若只有日期：

```text
release_date < announcement_date
```

同日来源只有在能证明发布时间早于财报公告时才允许进入财报前预期。

## 7. 总体架构

```text
                           ┌────────────────────────┐
                           │ Python确定性准备层     │
                           │ 财报/价格/估值/预测快照│
                           └───────────┬────────────┘
                                       │
                 ┌─────────────────────┴─────────────────────┐
                 │                                           │
      ┌──────────▼──────────┐                    ┌───────────▼─────────┐
      │ Industry Scope      │                    │ Expectation Scout    │
      │ 细分产业链识别/分组 │                    │ 财报前预期重建       │
      └──────────┬──────────┘                    └───────────┬─────────┘
                 │                                           │
      ┌──────────▼──────────┐                                │
      │ Industry Researcher │                                │
      │ 产业链与利润映射     │                                │
      └──────────┬──────────┘                                │
                 └─────────────────────┬─────────────────────┘
                                       │
                            ┌──────────▼──────────┐
                            │ Financial Author    │
                            │ 财报初稿与未来推演   │
                            └──────────┬──────────┘
                                       │
                            ┌──────────▼──────────┐
                            │ Research Challenger │
                            │ 审计、追问、补搜     │
                            └──────────┬──────────┘
                                       │
                            ┌──────────▼──────────┐
                            │ Financial Author    │
                            │ 修订并写最终报告     │
                            └─────────────────────┘
```

## 8. Python 确定性准备层

### 8.1 不调用整个 `run_daily_pipeline.py`

`prepare_financial_report_skill.py` 不应通过子进程调用整个：

```bash
python scripts/run_daily_pipeline.py
```

原因：

- 会顺带生成每日交易 `01-04`；
- 增加不必要的运行时间；
- 让财报研究反向依赖交易编排；
- 可能污染用户已经准备好的运行目录；
- 不符合脚本入口与业务服务分层原则。

正确方式是复用底层服务：

```python
services.research.stock_analysis.analyze_stock_dynamics_and_valuation(
    symbol,
    as_of_date,
)
```

该服务目前已经由 `run_daily_pipeline` 间接使用，并会生成：

- `analysis/*_股票分析报告_YYYYMMDD.md/json`
- `pe_pb_analysis/*_YYYYMMDD_enhanced_pe_analysis.md/json`

准备脚本应调用服务后，把结果复制或渲染为 workdir 的冻结输入。

### 8.2 本地上下文准备

建议新增业务模块：

```text
services/research/financial_report_context.py
```

职责：

- 解析公告日期和可选公告时间；
- 解析前一交易日；
- 查找最近的财报前 `04_stock_research`；
- 读取或生成财报前价格与估值；
- 读取或生成分析日价格与估值；
- 读取财报前冻结的一致预期；
- 检查估值使用的财务基准期；
- 计算新财报口径下的 pro-forma TTM 指标；
- 生成面向 Agent 的 Markdown 输入；
- 不生成投资结论。

脚本 `prepare_financial_report_skill.py` 只负责参数、编排、日志和最终清单。

### 8.3 财报前研究包选择

优先选择：

```text
data/skill_runs/{pre_market_date}/{book_type}/04_stock_research/
{stock_name}_{symbol}_{pre_market_date}_research.md
```

如果同一股票在多个账本存在研究包，价格、估值和一致预期本身不依赖账本，可按以下顺序选择：

1. `fixed_tracked`
2. `long_book`
3. `short_book`

不得读取研究包中的：

- 持仓与交易记忆；
- recommended_action；
- price_target；
- 历史 verdict；
- 旧买卖计划。

只提取：

- `1.1 Price Report JSON`
- `1.2 Valuation Report`
- 财报前新闻和公告中与预期有关的事实；
- 同花顺一致预期；
- 可验证的公司指引。

### 8.4 一致预期快照

当前 A 股一致预期缓存：

```text
data/stock_info/{stock}/profit_forecast/profit_forecast.csv
```

该缓存可能被刷新覆盖，因此不能天然还原财报前状态。

第一阶段兼容方案：

- 优先从财报前不可变的 `04_stock_research` 提取一致预期；
- 当前缓存只作为分析日最新预期；
- 如果没有财报前研究包，明确标记“缺少冻结的一致预期”。

后续增强方案：

```text
data/stock_info/{stock}/profit_forecast/snapshots/YYYYMMDD.csv
```

由 `shared_data_access` 维护带日期的预测快照。实施该增强时必须继续保留现有 `profit_forecast.csv` 兼容入口。

### 8.5 workdir 新增输入

在保留现有 `01-05` 的基础上新增：

```text
financial_report_workdir/
├── 01_latest_report.md
├── 02_previous_report.md
├── 03_report_analysis_prompt.md
├── 04_future_outlook_prompt.md
├── 05_agent_input.md
├── pre_announcement_market_context.md
├── current_market_context.md
├── valuation_framework.md
├── prior_fundamental_memory.md
├── existing_industry_research.md
├── manifest.json
└── research_outputs/
```

说明：

- 不新增第二份 manifest；
- `manifest.json` 暂时保留兼容，但 Agent 不应依赖其中的 `industry_name` 下结论；
- `prior_fundamental_memory.md` 只保存上期基本面假设、管理层指引兑现和未解决问题，不包含旧交易动作；
- `existing_industry_research.md` 是旧卡片的高密度摘要，不是本季度结论。
- `05_agent_input.md` 声明当前公司的 `disclosures/md/` 和 `disclosures/pdfs/` 只读历史原文目录；只有明确历史缺口时才按 Markdown 优先、单份 PDF 兜底的顺序回溯，不批量读取多年报告，也不自动调用 MinerU。

## 9. 财报前与当前市场上下文

### 9.1 `pre_announcement_market_context.md`

内容：

- 财报公告日期和可选公告时间；
- 财报前市场快照日期；
- 财报前收盘价、市值、PE、PB、PS；
- 估值使用的财务基准期；
- 财报前一段时间的绝对收益；
- 相对大盘、行业或可比公司的超额收益；
- 成交额、换手率和波动变化；
- 历史价格和估值位置；
- 财报前年度一致预期；
- 如可得，财报前季度正式卖方预期；
- 管理层正式指引；
- 数据缺失与口径说明。

### 9.2 `current_market_context.md`

内容：

- 分析日价格、市值、PE、PB、PS；
- 当前增强估值报告；
- 当前价格报告；
- 当前估值所使用的财务报告期；
- 财报发布后价格反应，如已产生；
- 财报前与当前估值变化；
- 新财报口径重新计算的 TTM 扣非利润和 PE；
- 基于财报后新预测的 Forward PE；
- 财报后机构预测修正，如已产生；
- 不能判断时的明确说明。

### 9.3 不允许伪精确判断“已计价百分比”

输出使用：

```text
计价较少
部分计价
计价较充分
可能过度计价
无法判断
```

每个标签必须附证据，不输出“已经计价 67%”等无法验证的精确数字。

## 10. 预期差研究

### 10.1 Expectation Scout 的信息隔离

Expectation Scout 禁止读取：

- `01_latest_report.md`
- 财报发布后的卖方点评；
- 财报发布后的价格反应；
- 财报后更新的机构预测；
- 任何含有实际财报数字的搜索摘要。

它只读取：

- `pre_announcement_market_context.md`
- `02_previous_report.md`
- 上一期正式基本面报告中的可验证事实与待验证问题；
- 截止公告前的公司指引；
- 截止公告前的产业链证据。

### 10.2 预期层次

Expectation Scout 分开研究：

1. 公司公开预期；
2. 正式卖方年度预期；
3. 正式卖方季度预期，如可得；
4. 产业经营隐含预期；
5. 股价和估值隐含预期；
6. 市场主要争议和预测分歧。

### 10.3 年度预期不能冒充季度预期

同花顺年度一致预期只用于：

- 判断全年利润中枢；
- 计算当前累计实际完成率；
- 计算剩余季度隐含业绩要求；
- 观察财报后年度预测是否上调或下调；
- 计算 Forward PE。

没有可靠季度一致预期时，必须写：

```text
未取得高可信季度一致预期，不能对本季度给出精确数值型超预期判断。
以下结论基于年度预期、公司指引、产业数据和股价隐含预期。
```

### 10.4 输出

写入：

```text
research_outputs/expectation_snapshot.md
```

至少回答：

- 财报前市场期待什么；
- 哪些结果属于真正超预期；
- 哪些结果只是符合预期；
- 哪些结果属于实质性不及预期；
- 财报前股价是否已经抢跑；
- 哪些预期有高可信来源，哪些只是推断；
- 最大预期分歧是什么。

## 11. 公司触发型产业链研究

### 11.1 不直接调用 `monthly-industry-research` Skill

`monthly-industry-research` 当前存在：

- 人工确认主题；
- 每次最多一个主题；
- 独立行业池和状态机；
- 不为财报批量运行设计。

财报流水线不能直接触发整个 monthly skill，否则几十只股票无法批量运行。

正确方式：

- 复用其程浩然研究方法；
- 复用已有 `industry_research/cards`；
- 复用证据、利润映射和状态迁移思想；
- 新增“公司触发型、季度级、可批量”的研究 profile；
- monthly skill 继续保持独立，不修改交易产物。

### 11.2 建议的单一规则源

新增：

```text
configs/research/industry_chain_research_policy.md
```

该文件由以下双方共同读取：

- `financial-report-summary`
- `monthly-industry-research`

包含：

- 程浩然方法；
- 终端需求和 TAM；
- 供需错配；
- 产业链利润映射；
- 公司产业暴露；
- 龙头验证；
- 技术路线；
- 状态与证伪；
- 天花板估值适用条件。

避免两个 Skill 各自维护一套不同版本的程浩然 Prompt。

现有：

```text
configs/research/company_valuation_framework.md
```

继续作为公司类型、收益来源和估值方法的单一规则源。

### 11.3 产业链层级

统一使用：

```text
terminal_theme
→ subchain
→ value_chain_node
→ company_exposure
```

示例：

```text
AI算力
→ PCB
→ 电子布/铜箔/树脂/覆铜板/PCB制造
→ 胜宏科技、沪电股份等公司的实际业务暴露
```

### 11.4 Industry Scope

主 Agent 在批量开始前，对 ready stocks 进行细分产业链分组。

第一阶段可以由主 Agent 根据：

- 公司主营；
- `industry_name` 标签；
- 旧产业卡片；
- 最新财报业务分部；

形成分组。

无法可靠分组时，不强行复用，按单公司研究。

后续可增加批次级 `Industry Scope Planner`，输出最小结构：

```json
{
  "groups": [
    {
      "chain_key": "ai-pcb",
      "symbols": ["300476.SZ", "002463.SZ"]
    }
  ]
}
```

这是编排映射，不是研究 manifest。

### 11.5 复用规则

产业研究只有同时满足以下条件才可复用：

- `subchain` 相同，而不只是 `terminal_theme` 相同；
- 研究截止日属于同一财报披露窗口；
- 需求、订单、供给、库存、价格、交期、CAPEX 和技术路线已经刷新；
- 公司特有的业务暴露仍单独研究；
- 旧研究中所有可能变化的事实已重新验证。

例如：

- PCB 与光模块不可共享细分产业研究；
- 同属 PCB 的公司可以共享产业链基线；
- 综合公司不能因涉及某主题就被视为纯主题公司。

### 11.6 Industry Researcher 输出

写入共享路径或股票 workdir：

```text
research_outputs/industry_chain_research.md
```

需要回答：

- 终端需求单位和三至五年空间；
- 当前结构增长、普通周期、成熟或不确定；
- 供需错配在哪里；
- 需求如何向公司所在节点传导；
- 节点定价权和供给响应时间；
- 利润池最终流向；
- 公司主题收入和利润暴露；
- 公司是否为最受益龙头；
- 竞争对手、替代技术和风险；
- 当前行业状态；
- 程浩然天花板估值；
- 证伪条件；
- 数据缺口。

公司不一定是龙头。研究结论允许是：

```text
公司虽然处于高景气产业，但主题收入占比低、利润无法留存或竞争地位不足，
不属于最受益公司。
```

## 12. Financial Author

### 12.1 初稿输入

Financial Author 读取：

- `01_latest_report.md`
- `02_previous_report.md`
- `03_report_analysis_prompt.md`
- `04_future_outlook_prompt.md`
- `pre_announcement_market_context.md`
- `current_market_context.md`
- `valuation_framework.md`
- `prior_fundamental_memory.md`
- `research_outputs/expectation_snapshot.md`
- `research_outputs/industry_chain_research.md`
- 相关固定研究政策。

若上述输入不足以补足最近两年季度趋势、解释口径变化或追溯重大异常，Author 可按 `05_agent_input.md` 声明的只读路径定向查找历史原文。先在 `disclosures/md/` 用文件名、报告期和关键词定位；目标报告没有 Markdown 时才读取 `disclosures/pdfs/` 中对应的单份 PDF。不存在固定 N 年全量读取要求。

### 12.2 两遍阅读，降低历史锚定

第一遍：

- 先分析当前财报原文和当前结构化数据；
- 不读取上一期 AI 的最终估值和投资结论；
- 形成独立的当期事实与异常判断。

第二遍：

- 再读取上一期基本面假设；
- 检查旧假设是确认、削弱、证伪还是未决；
- 检查管理层此前指引是否兑现。

### 12.3 财务与分部研究

每个重大指标和主要业务均回答：

```text
发生了什么
→ 发生在哪个业务
→ 主要和次要原因
→ 各原因大致贡献
→ 暂时、周期还是结构
→ 下一步验证指标
→ 对未来业绩和估值的影响
```

必须覆盖：

- 最近两年季度趋势；
- 同比、环比和季节性；
- 分业务营收、毛利率和利润；
- 子公司；
- 订单、存货、应收、合同负债；
- 现金流和资本开支；
- 费用与研发；
- 非经常损益、汇兑、减值、补助和税率；
- 资产负债表质量；
- 管理层解释与外部验证。

公司没有披露分季度分部数据时：

- 不得虚构；
- 可从累计值推导单季度时必须标记“推导值”；
- 无法推导时说明披露限制；
- 使用订单、销量、价格、子公司和同行数据侧面验证。

### 12.4 预期差矩阵

初稿包含：

| 维度 | 财报前预期 | 实际结果 | 预期差 | 质量与持续性 |
| --- | --- | --- | --- | --- |
| 营收 |  |  |  |  |
| 归母净利润 |  |  |  |  |
| 扣非净利润 |  |  |  |  |
| 毛利率 |  |  |  |  |
| 核心业务 |  |  |  |  |
| 订单/合同负债 |  |  |  |  |
| 现金流/存货 |  |  |  |  |
| 管理层未来指引 |  |  |  |  |

不把各维度粗暴合成为单一数值分数。

### 12.5 未来预测

预测必须基于驱动因素，而不是简单线性外推：

```text
收入 = 销量 × 单价 + 产品结构 + 新业务
毛利 = 各业务收入 × 各业务毛利率
经营利润 = 毛利 - 期间费用
自由现金流 = 经营现金流 - 资本开支
```

根据公司类型调整：

- 周期股：价格、成本曲线、库存、供给出清、产能和订单；
- 稳定股：订单、用户、ARPU、历史利润率、资本开支和分红；
- 科技成长股：TAM、渗透率、份额、ASP、产品结构、客户验证和技术路线。

明确区分：

- 公司正式指引；
- 机构一致预期；
- Agent 基于证据的推演。

### 12.6 全年一致预期的剩余业绩要求

必须计算：

```text
剩余期间隐含净利润
= 财报前全年一致预期
- 本年度累计实际净利润
```

并判断：

- 相比去年同期需要增长多少；
- 是否符合历史季节性；
- 订单、产能、销量、价格和毛利率能否支撑；
- 财报后全年预期应上调、维持还是下调；
- 不能判断时明确说明原因。

### 12.7 初稿输出

写入：

```text
research_outputs/draft_v1.md
```

不得直接覆盖最终 `financial_reports/YYYYMMDD.md`。

## 13. Research Challenger

### 13.1 定位

Challenger 同时承担：

- 事实与数字审计；
- 深度研究质询；
- 独立补充搜索；
- 替代解释构造；
- 未来预测压力测试。

它不是 Juror，不投票。

### 13.2 问题生成方法

#### 趋势异常

- 指标突变；
- 趋势反转；
- 与季节性不符；
- 公司整体与分部背离。

#### 指标背离

- 收入增长但应收增长更快；
- 利润增长但现金流恶化；
- 订单增长但合同负债下降；
- 扩产但利用率下降；
- 存货上升但收入不增长；
- 公司称需求旺盛但同行恶化。

#### 披露缺口

- 分部突然合并；
- “其他业务”占比显著上升；
- 子公司不再披露；
- 口径、定义或非 GAAP 指标变化；
- 只披露增速不披露绝对值；
- 只强调收入不解释利润和现金流。

#### 反事实验证

如果管理层解释为真，哪些其他指标应该同步变化；如果没有同步变化，是否存在替代解释。

#### 预期差质询

- 使用的预期是否真的产生于财报前；
- 年度预期是否被冒充季度预期；
- 股价是否已经提前交易；
- 实际超预期是否来自一次性项目；
- 未来指引是否弱于当期数字；
- 财报后盈利预测是否需要继续调整。

#### 产业链质询

- 行业空间是否被重复计算；
- 需求是否真正传导到公司；
- 利润是否留在公司所在节点；
- 公司是否只是平台型或低纯度受益；
- 供给扩张是否将破坏当前利润；
- 技术替代是否改变天花板。

### 13.3 搜索与回答

Challenger 不只提出问题，还要：

- 使用百炼搜索寻找答案；
- 打开被选中的原始网页、公告或 PDF；
- 给出支持证据与反面证据；
- 说明答案是否充分；
- 给出初稿需要怎样修改。

### 13.4 输出格式

写入：

```text
research_outputs/challenge_round_01.md
```

使用 Markdown，不使用复杂 JSON：

```markdown
## 问题：……

为什么产生这个疑问：
……

调查与证据：
……

当前结论：
……

原报告需要怎样修改：
……

仍需验证：
……
```

不限制问题数量。

## 14. 修订与停止条件

### 14.1 Author 修订

复用原 Financial Author 会话，读取：

- `draft_v1.md`
- `challenge_round_01.md`
- Challenger 新增的来源；
- 所有原始输入。

生成：

```text
research_outputs/draft_v2.md
```

### 14.2 Challenger 复核

只有存在重大问题时再运行复核：

```text
research_outputs/closure_review.md
```

### 14.3 停止条件

研究完成需要满足：

- 重大数字已核对；
- 重大结论可追溯到证据；
- 预期差没有时间穿越；
- 公司整体与主要业务已有趋势和归因；
- 产业链研究已经落到公司利润暴露；
- 未来预测有驱动因素和证伪条件；
- Challenger 的高严重度问题已修正或列为未解决；
- 未解决问题已明确影响；
- 没有占位符或伪造来源。

不能因为 Agent 自己声明“已经完整”而停止。

## 15. 最终报告

最终路径保持：

```text
data/stock_info/{stock_name}_{symbol}/financial_reports/YYYYMMDD.md
```

建议正文结构：

1. 核心结论；
2. 公司类型、主要收益来源和本期判断；
3. 公司业务全景；
4. 最近两年季度趋势；
5. 本期财报核心数据；
6. 分业务、分产品、分区域和子公司；
7. 利润变化定量归因；
8. 毛利率、费用、现金流和资产负债表；
9. 订单、存货、产能和领先经营指标；
10. 产业链空间、供需、利润节点和公司地位；
11. 财报前预期、实际结果与预期差；
12. 财报前已计价程度和财报后估值；
13. 下一季度与未来六至十二个月预测；
14. 全年一致预期完成要求与预测修正；
15. 程浩然天花板估值及适用限制；
16. 风险、替代解释和证伪条件；
17. 下一季度验证清单；
18. 未解决问题与披露限制。

不限制每节条目数量。

## 16. `summary_index.json` 写入

最终 Author 只写最终 Markdown，不直接修改 `summary_index.json`。

主 Agent 在验证最终文件存在后，调用：

```text
scripts/register_financial_report_summary.py
```

或对应 service，统一更新现有字段：

- `latest_completed_report`
- `history`
- `announcement_id`
- `report_date`
- `report_type`
- `paired_previous_*`
- `output_path`
- `generated_at`

字段和文件路径保持兼容。

## 17. Prompt 分层

### 17.1 `SKILL.md`

只负责：

- 主 Agent 编排；
- 角色启动顺序；
- 文件写入权限；
- 失败与重试；
- 最终注册；
- 批量并发控制。

不重复完整投资方法。

### 17.2 固定规则

建议新增：

```text
configs/research/
├── company_valuation_framework.md
├── industry_chain_research_policy.md
├── financial_fundamental_research_policy.md
├── expectation_gap_research_policy.md
├── web_research_policy.md
└── financial_report_output_schema.md
```

所有角色直接读取本地规则源。主 Agent 只传路径，不复述规则。

### 17.3 `05_agent_input.md`

只包含本次运行信息：

- 股票和公告；
- 强制阅读路径；
- workdir；
- 输出路径；
- 时间截止；
- 当前角色可读和禁止读取的文件；
- 角色写入路径。

不复制固定研究规则。

### 17.4 角色 Prompt

每个角色的 Prompt 只包含：

- 当前角色；
- 当前股票或产业链；
- 必读规则；
- 必读输入；
- 禁止输入；
- 唯一输出路径；
- 完成条件。

## 18. 百炼搜索政策

### 18.1 强制入口

所有外部搜索首先使用阿里云百炼搜索 MCP。

已知能力名：

```text
bailian_web_search
```

Codex 和 OpenCode 的实际工具前缀可能不同，Skill 应按能力发现，不把某个 MCP 内部全名写死。

### 18.2 搜索与打开网页分工

```text
百炼搜索
→ 互联网语义召回和候选来源发现

打开具体网页/PDF
→ 阅读百炼选中的公司公告、交易所文件、协会资料、研报或原始页面
```

搜索摘要不能单独支持重大数字。

### 18.3 服务失败

必须区分：

- 搜索成功但没有结果；
- MCP 工具不存在；
- 百炼服务传输错误；
- 结果只有低等级来源。

百炼不可用时默认失败关闭：

```text
search_status = bailian_unavailable
```

不得静默切换其他搜索引擎。只有用户明确允许时才能使用替代搜索。

### 18.4 来源层级

优先级：

1. 公司公告、财报、交易所和监管；
2. 公司业绩会、投资者问答、政府和行业协会；
3. 客户、供应商和竞争对手正式披露；
4. 正式卖方研报；
5. 持牌财经媒体；
6. 聚合网站和个人内容只用于发现线索或市场情绪。

## 19. 批量并发与 Agent 数量

设：

- 股票数量为 `N`；
- 可复用的细分产业链数量为 `K`。

完整深度模式的逻辑角色数：

```text
1 个批次级 Industry Scope Planner（后续增强，可选）
+ K 个 Industry Researcher
+ N 个 Expectation Scout
+ N 个 Financial Author
+ N 个 Research Challenger
```

Author 初稿和修订复用同一 Agent，不再单独启动 Finalizer。

实际运行采用分阶段波次：

1. 产业链分组；
2. 产业链研究；
3. 每股 Expectation Scout；
4. 每股 Author 初稿；
5. 每股 Challenger；
6. Author 修订；
7. 主 Agent 注册结果。

Codex 存在并发限制时，只限制同时运行数量，不改变角色和文件契约。OpenCode 可以提高并发，但仍不能让 Agent 共享写文件。

## 20. 文件写入隔离

每个角色只写：

| 角色 | 唯一写入 |
| --- | --- |
| Python 准备层 | workdir 输入文件 |
| Industry Researcher | `industry_chain_research.md` |
| Expectation Scout | `expectation_snapshot.md` |
| Financial Author 初稿 | `draft_v1.md` |
| Research Challenger | `challenge_round_01.md` |
| Financial Author 修订 | `draft_v2.md`、最终财报 Markdown |
| 主 Agent/注册脚本 | `summary_index.json` |

如共享产业链研究，Industry Researcher 先写共享文件；股票 workdir 只复制路径或只读快照，不允许多股票 Agent 回写共享文件。

## 21. 失败处理

| 场景 | 行为 |
| --- | --- |
| 最新财报缺失 | 当前股票失败，不启动 Agent |
| 上一期财报缺失 | warning，允许继续 |
| 财报前价格快照缺失 | warning，预期计价判断降级 |
| 当前估值缺失 | warning，最终报告说明 |
| 财报前一致预期缺失 | 不允许伪造季度预期，改用公司指引和产业隐含预期 |
| 百炼不可用 | 研究角色失败关闭，等待用户处理或明确允许 fallback |
| 产业链无法识别 | 不复用旧研究，按公司独立研究并说明 |
| Challenger 发现重大问题 | 不直接发布，回到 Author 修订 |
| 高严重度问题无法解决 | 最终报告明确列为未解决，不伪造答案 |
| 最终 Markdown 未生成 | 不更新 `summary_index.json` |

## 22. 对 `monthly-industry-research` 的调整

### 22.1 保留

- 半年结构空间扫描；
- 月度领先指标；
- 季度财务扩散验证；
- 独立行业卡片；
- 人工确认和不自动交易边界。

### 22.2 调整

- 读取共享 `industry_chain_research_policy.md`；
- 明确区分 `terminal_theme`、`subchain`、`value_chain_node`；
- 百炼作为强制搜索入口；
- 不再把“最受益公司数量”作为固定内容限制；
- 不把公司预设为龙头；
- 关键结论必须落到利润池和公司业务暴露；
- 保留旧卡片作为历史状态，变化事实季度刷新。

### 22.3 与财报研究的关系

```text
monthly-industry-research
→ 全市场/主题级独立研究

financial-report-summary 的 Industry Researcher
→ 由具体公司财报触发的季度细分产业链研究
```

两者共享方法和可复用卡片，但不互相调用完整 Skill。

## 23. 建议实施文件

### 23.1 新增

```text
services/research/financial_report_context.py
configs/research/industry_chain_research_policy.md
configs/research/financial_fundamental_research_policy.md
configs/research/expectation_gap_research_policy.md
configs/research/web_research_policy.md
configs/research/financial_report_output_schema.md
.codex/skills/financial-report-summary/references/
tests/test_financial_report_context.py
tests/test_financial_report_expectation_cutoff.py
docs/fundamental_research/quarterly_fundamental_deep_research_design.md
```

如果 `company_valuation_framework.md` 尚未实际存在，则按既有设计新增；若已存在，只复用，不创建重复版本。

### 23.2 修改

```text
scripts/prepare_financial_report_skill.py
services/research/financial_report_summary_prompts.py
.codex/skills/financial-report-summary/SKILL.md
.codex/skills/monthly-industry-research/SKILL.md
.codex/skills/monthly-industry-research/references/output-contract.md
docs/fundamental_research/README.md
docs/PROJECT_SYSTEM_SUMMARY.md
```

可能需要增强：

```text
shared_data_access/cache_registry.py
```

仅用于保存带日期的一致预期快照。新增前需保持现有缓存入口兼容。

### 23.3 不修改

```text
scripts/run_daily_pipeline.py
services/pipeline/daily_pipeline.py
05_decision.json
06_execution_log.json
07_daily_summary.json
08_history_merge.json
真实交易规则
```

如果实施过程中发现必须修改这些文件，应停止并向用户说明原因。

## 24. 分阶段实施

### 阶段一：本地上下文和 Prompt 分层

- 新增财报前与当前市场上下文；
- 复用 `analyze_stock_dynamics_and_valuation()`；
- 复制 `valuation_framework.md`；
- 优先读取本地一致预期；
- 修正 `.claude` 路径；
- 删除“假设公司为龙头”；
- 建立固定规则文件；
- 保持单 Author 先可运行。

### 阶段二：Expectation Scout

- 增加财报前信息隔离；
- 生成 `expectation_snapshot.md`；
- 年度预期与季度预期分开；
- 加入股价与估值隐含预期；
- 新增时间截断测试。

### 阶段三：Industry Researcher

- 增加公司触发型产业链研究；
- 复用旧 cards；
- 增加细分产业链分组；
- 输出利润映射、公司暴露和天花板；
- 调整 monthly skill 读取共享方法。

### 阶段四：Challenger 与修订闭环

- Author 先写 `draft_v1.md`；
- Challenger 自动提问、补搜和提出修改；
- Author 生成 `draft_v2.md` 和最终报告；
- 高严重度问题未闭环时不直接注册。

### 阶段五：历史预测快照和自动质量门禁

- 保存 dated profit forecast snapshots；
- 检查财务期间、单位和计算；
- 检查 TTM 及 Forward PE；
- 对比财报前后机构预测修正；
- 增加管理层指引兑现记录。

## 25. 测试设计

### 25.1 单元测试

1. 公告日无时间时选择前一交易日；
2. 盘后公告允许使用同日收盘；
3. 盘前公告禁止使用同日收盘；
4. 财报前研究包选择不越过公告时点；
5. 只提取 `04_stock_research` 所需模块，不带入交易记忆；
6. 当前估值报告标明财务基准期；
7. 新 TTM 计算正确；
8. 年度一致预期不会被标为季度一致预期；
9. 缺少一致预期时输出降级说明；
10. 最终文件缺失时不更新 summary index；
11. 相同细分产业链允许只读复用；
12. 不同细分产业链不得错误复用。

### 25.2 集成测试

使用临时目录或 fixture，不污染正式 `data/skill_runs`：

- 比亚迪：年度一致预期、旧 TTM 与新财报口径；
- 国电南瑞：稳定股和窄估值区间；
- 牧原股份：周期股和商品价格/成本驱动；
- 胜宏科技：AI PCB 产业链与程浩然天花板；
- 综合公司：多产业链暴露；
- 缺少正式季度一致预期的公司；
- 百炼不可用场景。

### 25.3 Skill 人工验收

对一只股票完整运行，检查：

- workdir 输入齐全；
- Expectation Scout 没有读取实际财报；
- 产业链研究不是泛泛行业背景；
- 初稿明确实际与预期差；
- Challenger 提出了内容相关的新问题；
- 修订稿实际吸收了质询；
- 最终报告路径兼容；
- `summary_index.json` 正确更新；
- 没有修改交易 `01-08`。

## 26. 验收标准

设计实施后，一份合格报告应满足：

- 能看到最近两年公司整体与主要业务的季度趋势；
- 每个重大变化有主要和次要原因；
- 能解释经营质量是否改善；
- 能说明产业链空间、供需、利润节点和公司地位；
- 能还原财报前公开预期和隐含预期；
- 能区分年度一致预期和季度预期；
- 能说明财报前股价是否已部分或充分计价；
- 能计算新财报后的 TTM/Forward 估值；
- 能推演剩余季度达到全年预期的难度；
- 能提出下一季度验证指标和证伪条件；
- Challenger 产生了真正的新问题和新证据；
- 未解决问题被明确披露；
- 最终结论可以被后续交易 Agent 直接引用，但不包含真实交易指令。

## 27. 实施时必须特别检查的现有问题

1. `prepare_financial_report_skill.py` 当前命令示例中存在重复 `python python`，实施时修正。
2. Skill 和 subagent Prompt 中存在 `.claude/skills/...`，应改为 `.codex/skills/...`。
3. `FUTURE_OUTLOOK_PROMPT_TEMPLATE` 的“假设该公司为行业龙头”必须删除。
4. 当前比亚迪旧财报总结使用低等级来源构造季度一致预期，实施后不得继续此模式。
5. 当前增强估值可能只更新价格但沿用旧财务基准期，必须在上下文中显式标注。
6. 当前 `profit_forecast.csv` 不是历史快照，财报前一致预期优先从不可变研究包读取。
7. Python 可以生成价格、估值、趋势和确定性计算，但不得替代 LLM 撰写基本面结论。

## 28. 默认决策

若用户未提出不同意见，后续实施采用：

- 保持最终财报和 `summary_index.json` 契约不变；
- 不调用整个 `run_daily_pipeline.py`；
- 复用底层股票价格与估值服务；
- 本地清洗数据优先、百炼搜索补缺；
- 每股一个 Expectation Scout、一个 Author、一个 Challenger；
- Author 复用完成最终修订；
- 产业研究按细分产业链复用；
- monthly skill 不被财报 skill 直接调用，只共享方法和数据；
- 财报前预期严格时间隔离；
- 不设置内容条目上限；
- 不使用投票决定基本面事实；
- 最终报告不生成真实交易动作。
