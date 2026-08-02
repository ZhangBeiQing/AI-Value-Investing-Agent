# 系统原理与日常决策链路详解

更新日期：2026-07-08

## 1. 文档定位

本文用于解释当前项目的完整工作原理：系统如何从固定脚本收集宏观、行情、财报、新闻、公告、板块和因子数据，如何把这些数据逐步加工成每只股票的研究包，最后如何交给 Agent 判断是否买入、卖出、持有或观望。

本文不是单个脚本说明，而是整个链路的“原理图”。如果只想知道今天怎么跑，请看 `README.md`；如果要改代码或排查链路问题，请结合：

- `docs/PROJECT_SYSTEM_SUMMARY.md`
- `docs/share_data_access/README.md`
- `docs/selection_system/因子库与量化初筛系统设计.md`
- `docs/trade_summary/README.md`
- `.codex/rules/skill-pipeline.md`

## 2. 核心思想

这套系统的核心不是让 Agent 临时联网抓一堆资料，然后凭即时上下文做判断。当前架构是一个本地 `skill-only` 的研究与交易流水线：

```text
Python 固定脚本负责：
  数据抓取、缓存、清洗、指标计算、候选池初筛、每日输入产物生成、交易后处理

Agent / skill 负责：
  宏观总结、新闻主题记忆、财报深研、逐股研究吸收、交易决策、组合层判断
```

这种分工有几个目的：

- **可复现**：每天的输入文件落在 `data/skill_runs/YYYY-MM-DD/`，之后复盘能看到当时给 Agent 的材料。
- **可审计**：宏观、新闻、财报、公告、量化因子、交易决策都有明确产物路径。
- **降低上下文损耗**：重要经验沉淀在 `.codex/skills/`、`.codex/rules/` 和历史交易总结中，不依赖单次对话记忆。
- **避免临时抓数失控**：行情、财报、股本、公告都通过 `shared_data_access` 缓存和读取，避免每个工具自己散乱访问外部源。
- **把筛选和解释分开**：量化因子负责从大股票池里做第一轮可复现筛选，Agent 负责对候选做逻辑验证、风险识别和交易动作判断。

## 3. 日常总流程

日常主入口是：

```bash
python scripts/refresh_all_for_date.py --date YYYY-MM-DD
```

这里的 `--date` 表示“要分析的交易日”，即已经产生收盘数据的那一天。早上运行时通常传昨天的交易日；周末或节假日需要手动传最近一个交易日。

完整链路可以概括为：

```text
1. refresh_all_for_date.py
   刷新基础数据、宏观客观面板、行情、财报、新闻、板块、因子、量化初筛

2. daily-macro-summary skill
   生成渐进式宏观总结

3. gradual-hot-news-summary skill
   生成渐进式热点新闻主题总结

4. prepare_financial_report_skill.py + financial-report-summary skill
   准备并生成重点股票财报深研

5. run_daily_pipeline.py
   汇总所有材料，生成 01-04 研究输入包

6. auto-trading-* skill
   让 Agent 按账本分析股票并生成 05_decision.json

7. run_post_trade.py
   人工确认后执行交易后处理，生成 06-08 并写入历史总结
```

## 4. 数据刷新层：固定脚本先把事实准备好

### 4.1 一键刷新编排

入口：

```bash
python scripts/refresh_all_for_date.py --date 2026-07-06
```

核心编排在：

```text
services/data_refresh/refresh_orchestrator.py
```

它会按固定顺序运行多段 Python 链路。日常默认逻辑是先保证所有后续研究要用的本地数据已经准备好，而不是等 Agent 决策时临时去抓。

主要阶段：

```text
manage_daily_data
selection.run-news
selection.build-board-heat-state
selection.build-factor-store
selection.build-factor-scores
selection.build-quant-prefilter
clear_research_artifact_cache
```

### 4.2 manage_daily_data：股票基础数据和宏观客观面板

入口脚本：

```text
scripts/manage_daily_data.py
```

日常不直接从它开始跑，而是由 `refresh_all_for_date.py` 调用。

它负责：

- 刷新宏观客观面板。
- 刷新固定股票池和候选股票池的行情价格。
- 刷新财报结构化缓存。
- 刷新股本、流通股本等估值计算依赖。
- 运行 `basic_stock_info.py` 生成基础快照。
- 根据参数决定是否跳过公告扫描。

典型输入范围：

```text
TRACKED_A_STOCKS
master_universe
默认指数基准 000001.IDX
```

关键产物：

```text
data/stock_info/{stock_name}_{symbol}/prices/price.csv
data/stock_info/{stock_name}_{symbol}/financials_cache/*.csv
data/stock_info/{stock_name}_{symbol}/share_info/*.csv
data/global_cache/macro_objective_panel/
data/basic_info_cache 或相关 basic snapshot 缓存
```

### 4.3 shared_data_access：统一数据访问基座

统一入口：

```text
shared_data_access/data_access.py
SharedDataAccess.prepare_dataset()
```

设计原则：

- 外部行情、财报、股本、公告访问都优先通过 `shared_data_access`。
- 抓取阶段面向真实当前时间拿足历史。
- 回测、复盘、历史分析只在读取阶段按 `as_of_date` 做截断。
- 每类缓存都有登记、TTL、必需文件和刷新元信息。
- ETF / 指数会自动降级为“只需要价格”的模式。

缓存注册位置：

```text
shared_data_access/cache_registry.py
```

典型缓存：

```text
PRICE_SERIES       prices/price.csv
FINANCIALS         financials_cache/*.csv
SHARE_INFO         share_info/*.csv
DISCLOSURES        disclosures/index.json, cninfo_list.csv, pdfs/
CHIP_DISTRIBUTION  chip_distribution/chip_distribution.csv
BOARD_METRICS      data/global_cache/board_metrics_ths/
MACRO_PANEL        data/global_cache/macro_objective_panel/
```

### 4.4 为什么要先固定脚本收集数据

如果让 Agent 每次临时决定查什么，问题会很多：

- 不同天的数据源、时间窗口和字段可能不一致。
- Agent 容易漏掉股本、价格、公告、历史估值这类“基础但关键”的数据。
- 外部 API 临时失败会直接污染决策过程。
- 回测和复盘无法保证时间因果。

现在的做法是：固定脚本先把事实层做好，Agent 只在已准备好的材料上做判断。这样可以把“数据工程失败”和“研究判断失败”分开定位。

## 5. 宏观层：客观数据和渐进式总结分工

### 5.1 宏观客观面板

宏观客观面板由 Python 脚本构建，偏结构化事实：

```text
data/global_cache/macro_objective_panel/
```

它的角色是给 Agent 提供不依赖主观叙述的宏观底座，例如：

- 利率、汇率、商品、指数、风险资产表现。
- 中美宏观指标。
- 央行、通胀、流动性、风险偏好相关数据。
- 可能影响 A 股和港股风险偏好的客观变化。

这部分尽量程序化，减少 Agent 直接凭新闻标题理解宏观的偏差。

### 5.2 daily-macro-summary：渐进式宏观总结

触发 skill：

```text
/daily-macro-summary
```

产物：

```text
data/macro_economy/YYYYMMDD.md
```

它的任务不是每天从零写宏观报告，而是维护一个渐进式宏观记忆：

- 继承前一日仍然有效的宏观判断。
- 加入当天新增的宏观事件和数据变化。
- 对已经失效的风险进行降权或移除。
- 明确哪些宏观变量会影响今日仓位和风险偏好。
- 为后续热点新闻和交易决策提供“市场环境锚点”。

宏观总结通常会影响：

- 总体仓位倾向。
- 是否需要提高防御。
- 成长股、周期股、红利股、港股、黄金、科技方向的相对偏好。
- 是否应该因为外部风险降低短线交易强度。

## 6. 新闻与主题层：从新闻流到渐进式热点状态

### 6.1 run-news：全市场新闻采集和增强

入口由 `refresh_all_for_date.py` 调用：

```text
scripts/manage_selection_system.py run-news
```

产物通常位于：

```text
data/selection_runs/YYYY-MM-DD/03_news_prompt_input.json
```

这一层负责：

- 抓取全市场新闻。
- 去重。
- 结构化。
- 与股票、板块、主题做初步关联。
- 给后续热点总结 skill 提供输入。

### 6.2 build-board-heat-state：板块热度状态

入口由 `refresh_all_for_date.py` 调用：

```text
scripts/manage_selection_system.py build-board-heat-state
```

产物：

```text
data/selection_runs/YYYY-MM-DD/05_board_heat_state.json
data/selection_runs/YYYY-MM-DD/05_board_heat_digest.json
```

这一层把新闻和行情结合起来，判断哪些板块处于升温、扩散、退潮或分歧状态。

典型用途：

- 辅助短线池识别催化方向。
- 辅助长期池判断产业趋势是否仍在强化。
- 给 Agent 提供“今天市场在交易什么”的背景。
- 给量化因子提供 `board_heat_score` 一类横截面特征。

### 6.3 gradual-hot-news-summary：渐进式主题记忆

触发 skill：

```text
/gradual-hot-news-summary
```

核心产物：

```text
data/selection_runs/YYYY-MM-DD/06_hot_news_state.json
```

它的目标不是简单总结当天新闻，而是维护“主题级研究记忆”：

- 哪些主题正在升温。
- 哪些主题已经过热。
- 哪些主题只是一次性新闻。
- 哪些主题有产业链扩散。
- 哪些主题对应的股票需要进一步深研。
- 哪些新闻对已有持仓构成证实或证伪。

这个文件会进入后续 `01_global_context.md` 和 `03_agent_input.md`，让交易 Agent 不只是看单股，而是理解当天主题环境。

## 7. 股票数据层：价格、估值、历史区间和财务指标

### 7.1 每日价格数据

每只股票的价格缓存：

```text
data/stock_info/{stock_name}_{symbol}/prices/price.csv
```

典型字段：

```text
日期, 开盘, 收盘, 最高, 最低, 成交量, 成交额, 振幅, 涨跌幅, 涨跌额, 换手率, 流通股本
```

价格数据用于：

- 当日涨跌幅。
- 近 3/6/12 月收益。
- 波动率、夏普、最大回撤。
- 均线、MACD、RSI 等技术指标。
- 当前价格在历史区间中的位置。
- 与指数或相似股票的相关性。
- 交易执行时的价格参考。

### 7.2 财报结构化数据

缓存位置：

```text
data/stock_info/{stock_name}_{symbol}/financials_cache/
├── profit_sheet.csv
├── balance_sheet.csv
├── cash_flow_sheet.csv
└── financial_abstract.csv
```

它们用于计算：

- 收入、利润、扣非利润增速。
- 毛利率、净利率、ROE。
- 资产负债、现金流质量。
- TTM 利润。
- PE、PB、PS、PEG 等估值指标。

### 7.3 股本数据

缓存位置：

```text
data/stock_info/{stock_name}_{symbol}/share_info/
```

股本数据用于：

- 总市值。
- 流通市值。
- PE / PB / PS 等估值计算。
- 历史估值对比。

如果股本缓存缺失，系统会尝试初始化抓取；对于目标股票，股本缺失通常是关键错误。对于相似股票或辅助对比股票，部分模块会降级跳过，避免辅助数据失败打断主股票研究。

### 7.4 历史 PE / PB / 价格区间

增强估值分析由：

```text
enhanced_pe_pb_analyzer.py
services/research/stock_analysis.py
```

相关产物：

```text
data/stock_info/{stock_name}_{symbol}/pe_pb_analysis/
├── {name}_{symbol}_YYYYMMDD_enhanced_pe_analysis.json
├── {name}_{symbol}_YYYYMMDD_enhanced_pe_analysis.md
└── {name}_{symbol}_YYYYMMDD_pe_comparison_table.csv
```

它会计算和汇总：

- 当前 PE / PB / PS。
- 近 3.5 年 PE / PB 中位数。
- 当前估值在历史区间里的分位。
- 当前价格相对历史高低点的位置。
- 与相似股票的估值对比。
- 估值偏贵、合理、便宜的解释。

这部分是长期买入判断的重要输入，因为长期策略不能只看价格动量，还要看估值和基本面是否匹配。

### 7.5 价格动态研究

价格动态分析由：

```text
stock_price_dynamics_summarizer.py
services/research/stock_analysis.py
```

产物：

```text
data/stock_info/{stock_name}_{symbol}/analysis/
├── close_price_YYYYMMDD.csv
├── technical_indicators_YYYYMMDD.csv
├── {name}_{symbol}_股票分析报告_YYYYMMDD.json
└── {name}_{symbol}_股票分析报告_YYYYMMDD.md
```

它关注：

- 短中期走势。
- 技术指标。
- 量价关系。
- 与指数和相似股票的相对强弱。
- 是否突破、回踩、放量、缩量、背离。

这部分对短线池尤其重要，也会辅助长期池判断是否适合建仓或加仓。

## 8. 量化因子层：从大股票池筛出短期和长期候选

### 8.1 为什么需要量化初筛

Agent 不适合每天从几百只股票里纯靠阅读做初筛。系统先用可复现的因子做第一轮筛选，再把 Top 候选交给 Agent 深研。

分工是：

```text
量化因子：
  快速、稳定、可回测，负责横截面排序和初筛

Agent：
  慢但理解力强，负责解释、排雷、确认逻辑和最终交易动作
```

### 8.2 Factor Store

构建入口：

```text
scripts/manage_selection_system.py build-factor-store
```

由 `refresh_all_for_date.py` 自动调用。

主要产物：

```text
data/factor_store/by_symbol/{symbol}.csv
data/factor_store/by_date/YYYY-MM-DD.csv
data/selection_runs/YYYY-MM-DD/12_factor_snapshot.csv
data/selection_runs/YYYY-MM-DD/12_factor_snapshot.json
```

因子来源包括：

- 价格。
- 估值。
- 财务质量。
- 成长性。
- 技术趋势。
- 波动和流动性。
- 板块热度。
- 新闻主题。
- 公告事件。
- 筹码分布。

### 8.3 因子评分

入口：

```text
scripts/manage_selection_system.py build-factor-scores
```

评分配置：

```text
configs/selection_system/factor_scoring.yaml
```

产物：

```text
data/selection_runs/YYYY-MM-DD/13_factor_scores.csv
data/selection_runs/YYYY-MM-DD/13_factor_scores.json
```

系统会分别计算偏短期和偏长期的分数。

短期更看重：

- 主题热度。
- 板块强度。
- 价格趋势。
- 放量突破。
- 流动性。
- 短期催化。
- 风险事件过滤。

长期更看重：

- 估值分位。
- ROE、毛利率、净利率。
- 收入和利润增长。
- 行业景气。
- 价格是否处于可接受区间。
- 基本面证据的持续性。

### 8.4 量化初筛输出短期池和长期池

入口：

```text
scripts/manage_selection_system.py build-quant-prefilter
```

产物：

```text
data/selection_runs/YYYY-MM-DD/12_quant_prefilter.csv
data/selection_runs/YYYY-MM-DD/12_quant_prefilter_short.csv
data/selection_runs/YYYY-MM-DD/12_quant_prefilter_long.csv
data/selection_runs/YYYY-MM-DD/12_quant_prefilter.json
```

这些 CSV 是日常三账本流程的重要输入。如果没有运行 LLM 自动选股 skill，`run_daily_pipeline --all-books` 会直接回退使用它们：

```text
short_book  <- 12_quant_prefilter_short.csv
long_book   <- 12_quant_prefilter_long.csv
```

这样可以做到：即使不让 LLM 做全市场选股，系统也能每天稳定生成短期和长期候选池。

## 9. 公告和财报深研层：补足重信息

### 9.1 公告缓存

公告缓存位置：

```text
data/stock_info/{stock_name}_{symbol}/disclosures/
├── index.json
├── cninfo_list.csv
├── pdfs/
└── .cache_registry_meta.json
```

公告用于识别：

- 回购。
- 增持。
- 减持。
- 分红。
- 股权激励。
- 重大合同。
- 中标。
- 并购重组。
- 问询函。
- 监管处罚。
- 业绩预告。
- 定增、可转债、质押等资本动作。

公告信息会进入：

- 新闻摘要。
- 因子事件特征。
- 财报深研。
- 每只股票研究包。
- 交易 Agent 的风险判断。

### 9.2 长期池和短期池公告更新

日常数据刷新会处理一部分公告、新闻和板块信息。对于进入短期池和长期池的候选股票，系统还会进一步确保公告和财报材料足够完整，因为它们后续会被 Agent 重点分析。

典型逻辑是：

```text
先通过因子筛出 short_book / long_book 候选
再对候选池补公告、新闻、财报总结
最后把这些材料汇总进 04_stock_research
```

这样避免对全市场所有股票都做昂贵的公告 PDF 和财报深读，同时保证真正进入候选池的股票信息密度足够。

### 9.3 财报总结准备

入口：

```bash
python scripts/prepare_financial_report_skill.py --date YYYY-MM-DD --sync-first --json --include-quant-prefilter
```

作用：

- 同步固定池、短线候选、长期候选相关财报公告。
- 生成待分析清单。
- 给 `/financial-report-summary` skill 提供逐股任务输入。

### 9.4 financial-report-summary skill

触发：

```text
/financial-report-summary
```

产物：

```text
data/stock_info/{stock_name}_{symbol}/financial_reports/*.md
```

每份财报总结通常会覆盖：

- 财报期。
- 收入和利润变化。
- 毛利率、费用率、现金流。
- 业务结构变化。
- 管理层展望。
- 风险点。
- 与市场预期的差异。
- 对长期逻辑的证实或证伪。

这部分是长期池和固定池决策的重要输入，尤其用于判断“价格上涨是否有基本面支撑”和“下跌是否只是情绪扰动”。

## 10. 研究包生成层：把所有材料汇总成 01-04

### 10.1 run_daily_pipeline

入口：

```bash
python scripts/run_daily_pipeline.py --date YYYY-MM-DD --max-workers 6
python scripts/run_daily_pipeline.py --date YYYY-MM-DD --max-workers 6 --all-books
```

核心实现：

```text
services/pipeline/daily_pipeline.py
services/pipeline/steps/
```

它会把前面所有事实和研究结果汇总成 Agent 可读的固定文件。

### 10.2 账本结构

输出目录：

```text
data/skill_runs/YYYY-MM-DD/
├── run_manifest.json
├── fixed_tracked/
├── short_book/
└── long_book/
```

账本含义：

```text
fixed_tracked:
  固定跟踪股票池，来自 configs/stock_pool.py

short_book:
  短线池，来自 LLM 选股产物或量化初筛短期候选

long_book:
  长期池，来自 LLM 选股产物或量化初筛长期候选
```

### 10.3 01_global_context.md

路径：

```text
data/skill_runs/YYYY-MM-DD/{book_type}/01_global_context.md
```

内容来源：

- 宏观总结。
- 大盘和指数状态。
- 热点新闻主题状态。
- 板块热度摘要。
- 组合层历史背景。

作用：

- 给 Agent 今天的市场环境。
- 约束总仓位和风险偏好。
- 提醒今天重点主题和风险事件。

### 10.4 02_basic_snapshot_payload.json

路径：

```text
data/skill_runs/YYYY-MM-DD/{book_type}/02_basic_snapshot_payload.json
```

内容包括每只股票的结构化快照，例如：

- 最新价格。
- 涨跌幅。
- 成交额。
- PE / PB / PS。
- 市值。
- 收益、波动、回撤。
- 财务增长。
- 毛利率、净利率、ROE。
- 流动性。

作用：

- 给 Agent 快速横向比较。
- 给 03_agent_input 提供结构化事实。
- 后续交易执行也可以引用其中的价格基准。

### 10.5 03_agent_input.md

路径：

```text
data/skill_runs/YYYY-MM-DD/{book_type}/03_agent_input.md
```

这是交易 skill 的主要输入之一，通常包括：

- 今日任务。
- 账本类型。
- 股票列表。
- 决策规则。
- 输出 JSON 契约。
- 风险约束。
- 仓位规则。
- 历史组合上下文摘要。

Agent 最终生成 `05_decision.json` 时必须遵守这里的格式要求。

### 10.6 04_stock_research/*.md

路径：

```text
data/skill_runs/YYYY-MM-DD/{book_type}/04_stock_research/{name}_{symbol}_YYYY-MM-DD_research.md
```

每只股票一份研究包，汇总了：

- 价格动态报告。
- 增强 PE/PB/PS 估值报告。
- 新闻和公告摘要。
- 财报总结。
- 机构一致预期或 forecast。
- 最近一次历史交易总结。
- 当前账本下的历史持仓和历史判断。

这是单股 subagent 最重要的输入文件。

## 11. 历史交易总结层：让 Agent 继承过去的判断

### 11.1 为什么需要历史总结

如果每天都让 Agent 从零判断，它容易出现几个问题：

- 昨天刚确认的长期逻辑今天忘了。
- 连续 HOLD 的原因不断重复。
- 已经证伪的逻辑又被重新采纳。
- 买入、加仓、止损缺少连续性。
- 组合层风险提示无法沉淀。

因此系统会把交易结果和每日总结归档为历史记忆。

### 11.2 交易历史位置

按账本归档：

```text
data/agent_data/book-fixed_tracked/
data/agent_data/book-short_book/
data/agent_data/book-long_book/
```

核心文件：

```text
position/position.jsonl
stock_decisions.json
decision_summary.json
portfolio_daily_summary.json
```

### 11.3 decision_summary.json

这个文件会把连续 HOLD / FLAT 等动作合并，形成更紧凑的历史摘要。

它记录：

- 某只股票从什么时候开始进入某个判断区间。
- 持续了多少天。
- 当时的核心逻辑是什么。
- 期间有没有加仓、减仓、证伪、风险变化。
- 最新一次 Agent 的建议和关注点。

`04_stock_research` 会读取最近一次历史交易总结，让单股 subagent 能继承历史锚点。

### 11.4 portfolio_daily_summary.json

组合层每日总结记录：

- 组合风险。
- 系统性风险提示。
- 明日重点观察。
- 资金使用和仓位结构。
- 当天决策的组合层理由。

这些内容会进入下一天的 `01_global_context.md` 或 `03_agent_input.md`，帮助 Agent 不只看单股，还看组合整体。

## 12. 自定义交易策略层

### 12.1 策略不硬编码在一个地方

这套系统的交易策略由多层共同约束：

```text
configs/
  股票池、因子评分、prompt flow

.codex/rules/
  文件契约、代码规范、数据访问规则、测试规则

.codex/skills/
  各账本交易流程、分析方式、仓位和决策输出要求

data/agent_data/
  历史交易总结和历史仓位

data/skill_runs/
  当日事实输入和研究包
```

这样做的好处是：数据计算、交易规则、Agent 执行流程和历史记忆相互分离，但通过文件契约串起来。

### 12.2 fixed_tracked 策略

固定池用于持续跟踪核心股票。它更强调：

- 逻辑是否仍成立。
- 估值是否可接受。
- 当前价格是否适合初仓、加仓或减仓。
- 组合里是否已有过度集中风险。
- 是否出现证伪信号。

典型动作：

```text
BUY
SELL
HOLD
WATCH
FLAT
```

固定池不是每天都要交易。很多时候重点是确认“继续持有”或“继续观察”的理由是否仍有效。

### 12.3 short_book 策略

短线池更强调：

- 新闻催化。
- 板块热度。
- 量价确认。
- 短期资金偏好。
- 持仓天数。
- 止盈止损。
- 事件兑现后的退出。

短线池有更强的时间约束，通常不应该无限期拖成长期仓位。

### 12.4 long_book 策略

长期池更强调：

- 行业长期空间。
- 公司竞争力。
- 财务质量。
- 估值和成长匹配度。
- 下跌是否是长期买点。
- 上涨后是否透支长期收益。

长期池允许股票池动态变化，但已持仓股票不能因为候选池变化就机械删除。Agent 需要区分：

```text
新入池候选
继续跟踪候选
已持仓但不在今日候选池
应退出的逻辑证伪股票
```

## 13. Agent 决策层：从研究包到买卖动作

### 13.1 auto-trading skill

触发：

```text
/auto-trading-fixed-tracked
/auto-trading-short-book
/auto-trading-long-book
```

对应目录：

```text
data/skill_runs/YYYY-MM-DD/fixed_tracked/
data/skill_runs/YYYY-MM-DD/short_book/
data/skill_runs/YYYY-MM-DD/long_book/
```

### 13.2 Agent 看到什么

Agent 不需要自己猜数据在哪里。它会按 skill 规则读取：

```text
01_global_context.md
02_basic_snapshot_payload.json
03_agent_input.md
04_stock_research/*.md
历史交易总结
当前持仓
```

Agent 的分析顺序通常是：

```text
1. 先看宏观和市场状态，决定总体风险偏好
2. 再看账本规则，明确当前是固定池、短线池还是长期池
3. 读取结构化快照，找到异常波动、估值偏离和风险股票
4. 对重点股票派发或执行单股深研
5. 结合历史交易总结，判断逻辑是延续、强化还是证伪
6. 输出每只股票动作和组合层总结
```

### 13.3 单股决策看什么

单股 Agent 通常会综合：

- 今天价格和成交额是否异常。
- 当前价格在历史区间的位置。
- PE / PB 是否处于可接受分位。
- 最近财报是否证实长期逻辑。
- 新闻和公告是利好、利空还是噪音。
- 板块热度是否支持短期交易。
- 历史持仓理由是否还成立。
- 是否触发止损、止盈、加仓、减仓或观望。
- 组合层是否允许继续加仓。

### 13.4 输出 05_decision.json

Agent 最终输出：

```text
data/skill_runs/YYYY-MM-DD/{book_type}/05_decision.json
```

它通常包括：

- `stock_decisions`：逐股决策。
- `portfolio_summary`：组合层总结。
- `system_risk_notes`：系统性风险。
- `system_focus_items`：明日重点观察。
- 交易动作、数量、理由、置信度、止损、目标价、风险点等字段。

这个文件不是自动无条件执行。用户需要人工确认。

## 14. 交易后处理层：05 到 06-08

### 14.1 run_post_trade

人工确认 `05_decision.json` 后运行：

```bash
python scripts/run_post_trade.py --date YYYY-MM-DD --book-type fixed_tracked --signature book-fixed_tracked
python scripts/run_post_trade.py --date YYYY-MM-DD --book-type short_book --signature book-short_book
python scripts/run_post_trade.py --date YYYY-MM-DD --book-type long_book --signature book-long_book
```

### 14.2 06_execution_log.json

记录交易执行结果：

```text
data/skill_runs/YYYY-MM-DD/{book_type}/06_execution_log.json
```

它会把买卖动作落到：

```text
data/agent_data/book-{book_type}/position/position.jsonl
```

如果有人工仓位覆盖，也会通过对应的 position override 机制处理。

### 14.3 07_daily_summary.json

记录当日组合总结：

```text
data/skill_runs/YYYY-MM-DD/{book_type}/07_daily_summary.json
```

它把 `05_decision.json` 中的组合层内容规范化，供后续归档。

### 14.4 08_history_merge.json

记录历史合并结果：

```text
data/skill_runs/YYYY-MM-DD/{book_type}/08_history_merge.json
```

它会更新：

```text
data/agent_data/book-{book_type}/stock_decisions.json
data/agent_data/book-{book_type}/decision_summary.json
data/agent_data/book-{book_type}/portfolio_daily_summary.json
```

下一天的研究包和 Agent prompt 会读取这些历史文件，实现交易记忆的延续。

## 15. 关键产物流转图

```text
外部数据源
  |
  v
shared_data_access 缓存
  |
  |-- prices/price.csv
  |-- financials_cache/*.csv
  |-- share_info/*.csv
  |-- disclosures/*
  |-- macro_objective_panel/*
  v
refresh_all_for_date.py
  |
  |-- 03_news_prompt_input.json
  |-- 05_board_heat_state.json
  |-- 12_factor_snapshot.csv
  |-- 13_factor_scores.csv
  |-- 12_quant_prefilter_short.csv
  |-- 12_quant_prefilter_long.csv
  v
研究 skill
  |
  |-- data/macro_economy/YYYYMMDD.md
  |-- 06_hot_news_state.json
  |-- financial_reports/*.md
  v
run_daily_pipeline.py
  |
  |-- 01_global_context.md
  |-- 02_basic_snapshot_payload.json
  |-- 03_agent_input.md
  |-- 04_stock_research/*.md
  v
auto-trading-* skill
  |
  v
05_decision.json
  |
  v
run_post_trade.py
  |
  |-- 06_execution_log.json
  |-- 07_daily_summary.json
  |-- 08_history_merge.json
  v
历史交易总结和下一日上下文
```

## 16. 时间因果和复盘原则

系统特别强调 `--date` 的语义：

```text
--date = 要分析的交易日
```

例如：

```bash
python scripts/refresh_all_for_date.py --date 2026-07-06
```

表示分析 2026-07-06 收盘后的数据，而不是 2026-07-08 当前时点，也不是未来交易日。

原则：

- 抓取时可以拿足历史数据。
- 读取和计算时必须按 `as_of_date` 截断。
- Agent 只能基于当日之前已经可见的信息做判断。
- 历史复盘时不能把未来公告、未来价格、未来财报泄露进当日研究包。

## 17. 常见失败点和系统设计取舍

### 17.1 外部 API 失败

例如行情、股本、公告接口短暂返回空或非 JSON。系统会尽量：

- 使用已有缓存。
- 对辅助股票降级跳过。
- 对目标股票保留失败，避免生成错误估值。

### 17.2 新上市股票回刷历史日期

如果分析日期早于股票上市日期，价格区间会为空。这不是代码错误，而是当日确实没有可用行情。回刷历史时可能需要：

```bash
python scripts/refresh_all_for_date.py --date YYYY-MM-DD --no-generate-prefilter
```

或在候选池中过滤当日尚未上市的股票。

### 17.3 公告 PDF 太长

公告 PDF 可能超过模型输入限制。系统会跳过无法摘要的公告，不应让单个 PDF 阻断整条每日链路。重要公告可后续人工或单独工具补摘要。

### 17.4 研究缓存过期

`run_daily_pipeline` 会使用：

```text
data/research_artifact_cache/YYYY-MM-DD/
```

为了避免价格和新闻变化后复用旧研究包，`refresh_all_for_date.py` 会清理当天研究缓存，让后续 04 研究包基于最新数据重建。

## 18. 如何理解最终“买入或卖出”的判断

最终交易动作不是单一模型分数决定，而是多层证据合成：

```text
宏观环境
  决定总仓位和风险偏好

新闻主题和板块热度
  决定短期市场关注方向

量化因子
  决定哪些股票值得进入短期池或长期池

价格和估值
  决定是否已经过热、便宜、合理或风险收益不佳

财报和公告
  决定基本面逻辑是否被证实或证伪

历史交易总结
  决定是否延续过去判断，是否需要修正锚点

账本策略
  决定同样一只股票在固定池、短线池、长期池里应采取不同动作

Agent 综合判断
  输出 BUY / SELL / HOLD / WATCH / FLAT 等动作
```

买入通常需要：

- 逻辑成立。
- 风险可控。
- 估值或价格位置可接受。
- 账本策略允许。
- 当前组合有仓位空间。
- 催化或长期证据足够。

卖出通常来自：

- 逻辑证伪。
- 风险事件。
- 估值过热且缺少继续上行证据。
- 短线催化兑现。
- 止损或止盈规则触发。
- 组合需要降风险或释放资金。

持有通常表示：

- 原逻辑仍成立。
- 当前价格没有足够好的加仓点。
- 也没有触发退出信号。

观察通常表示：

- 有潜在线索，但证据不够。
- 等公告、财报、价格确认或板块进一步发酵。

## 19. 一句话总结

这套系统的本质是：用固定 Python 链路把每天的市场事实、宏观背景、新闻主题、财务估值、历史价格、公告财报、量化因子和历史交易记忆都整理成可复现的本地文件，再让 Agent 在明确账本策略和历史上下文下做最终买卖决策。Python 保证数据和流程稳定，Agent 负责解释、权衡和决策。
