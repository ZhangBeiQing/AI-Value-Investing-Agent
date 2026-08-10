更新日期：2026-08-04

# AI-Value-Investing-Agent 项目系统白皮书

## 0. 文档定位

本文是项目的「入口式」总览文档。新 session、大改动前，先读本文建立全局上下文，再按章节末尾的链接进入对应模块的细化设计。

如果旧文档与本文冲突，以本文为准。`docs/` 已经清理过早期未落地的设计稿，目前保留的所有文档都对应当前实现。

---

## 1. 日常主流程（这是「每天真正跑的链路」）

仓库当前的实际日常节奏：早上 7 点起床后，对**昨天收盘**的数据做分析，并为下一交易日生成预案。所有脚本的 `--date` 默认指「要分析的交易日」，默认 `today - 1`；周末/节假日如果只分析最近一次收盘，应手动指定最近一个交易日。`refresh_all_for_date.py` 和 `run_daily_pipeline.py` 都有程序级交易日守卫：误传休市日时以成功状态显示 `SKIPPED`，不刷新数据、不创建该日 `skill_runs`，也不继续打印后续 Skill 清单。若需要在周末或节假日吸收新增宏观与新闻信息，可以对两个入口显式传入 `--allow-non-trading-date`，按该自然日刷新并生成研究预案；这只放宽实时分析日期，不表示休市日可以成交，也不放宽回测交易日约束。

### 1.1 一键刷数据 + 量化初筛

```bash
python scripts/refresh_all_for_date.py --date 2026-06-11
python scripts/refresh_all_for_date.py --date 2026-08-09 --allow-non-trading-date
```

由 `services/data_refresh/refresh_orchestrator.py` 编排，按顺序执行：

1. `manage_daily_data` — 刷新宏观面板、价格、财报、`basic_stock_info` 等基础缓存（`TRACKED_A_STOCKS ∪ master_universe`，约 100+ 只）
2. `selection.run-news` — 全市场新闻采集 / 去重 / 增强 → `03_news_prompt_input.json`
3. `selection.build-board-heat-state` — 板块热度分析 → `05_board_heat_state.json` / `05_board_heat_digest.json`
4. `selection.build-factor-store` → `selection.build-factor-scores` → `selection.build-quant-prefilter` — 生成因子宽表、评分与短/长两本候选 → `12_factor_snapshot.*` / `13_factor_scores.*` / `12_quant_prefilter*.csv`
5. 清理 `data/research_artifact_cache/{run_date}/` — 让下一次 `run_daily_pipeline` 必然基于最新数据重建 04 产物

跑完后脚本会打印「后续 skill 清单」，下面 1.2~1.6 就是按这份清单逐条往下走。

### 1.2 宏观与新闻总结（人工触发，需 LLM / 联网）

```text
1. /daily-macro-summary            → data/macro_economy/YYYYMMDD.md
2. /gradual-hot-news-summary       → data/selection_runs/YYYY-MM-DD/06_hot_news_state.json
```

- 宏观总结是新闻主题状态的上游校准器；渐进式新闻主题状态是后续 03_agent_input 与各 subagent 的核心研究输入。
- 详见 `.codex/skills/daily-macro-summary/SKILL.md` 与 `.codex/skills/gradual-hot-news-summary/SKILL.md`。

### 1.3 财报准备与总结（fixed_tracked + 量化初筛短期股）

```bash
3. python scripts/prepare_financial_report_skill.py --date 2026-06-11 --sync-first --json --include-quant-prefilter
4. /financial-report-summary       → 各股 data/stock_info/{name}_{symbol}/financial_reports/*.md
```

- `prepare_financial_report_skill.py` 会同步公告 PDF、整理待生成清单，并为每股生成相互隔离的财报前市场上下文与分析日价格/增强估值上下文；它复用底层股票研究服务，不调用整个 `run_daily_pipeline.py`。
- `/financial-report-summary` 按细分产业链协调 Industry Researcher，并为每股协调 Expectation Scout、Financial Author 和 Research Challenger；Author 修订后写入 `financial_reports/`，主 agent 通过质量门禁再注册 `summary_index.json`。
- 财报前预期严格按公告时点截断；年度同花顺预测不能冒充季度一致预期。外部搜索强制百炼优先，重大数字回到原始来源核验。
- 详见 `.codex/skills/financial-report-summary/SKILL.md`。

### 1.4 三账本 01-04 产物

```bash
5. python scripts/run_daily_pipeline.py --date 2026-06-11 --max-workers 6 --all-books
   # 周末/节假日补充分析时追加 --allow-non-trading-date
```

由 `services/pipeline/daily_pipeline.py` 编排。当前 `--all-books` 生成 **fixed_tracked + short_book**；长期候选并入 fixed_tracked，不再单独生成 long_book：

- **fixed_tracked** 取自 `configs/stock_pool.py` 的 `TRACKED_A_STOCKS`
- **short_book** 优先取 `08_short_book_candidates.json`；若选股 skill 未跑，则回退到 `12_quant_prefilter_short.csv`
- **长期候选**优先取 `09_long_book_candidates.json`，缺失时回退 `12_quant_prefilter_long.csv`，随后并入 fixed_tracked

输出目录：

```text
data/skill_runs/YYYY-MM-DD/
├── run_manifest.json                     # 本日三账本来源、symbols、capital_budget
├── fixed_tracked/
│   ├── 01_global_context.md              # 宏观/大盘/渐进式新闻总结
│   ├── 02_basic_snapshot_payload.json    # basic_stock_info 快照（用于定价基准）
│   ├── 03_agent_input.md                 # 共享投资策略 + fixed 主 Agent组合研判与 P0 输入
│   ├── 03_stock_analysis_input.md        # 共享投资策略 + fixed 个股辩论角色研究方法
│   ├── 04_stock_research/                # 每只股票的研究包 *.md
│   ├── 05_decision.json                  # 由 skill agent 在对话中生成
│   ├── debate/                           # Bull/Bear/Jury/final 单股辩论产物
│   └── subagent_result/                  # 旧版单 subagent 兼容产物
└── short_book/                           # 继续使用原 01-04 与 subagent_result 流程
```

详见 `.codex/rules/skill-pipeline.md`。

### 1.5 三账本交易 skill（人工触发，生成决策后人工确认）

```text
6. /auto-trading-fixed-tracked     → fixed_tracked/05_decision.json
7. /auto-trading-short-book        → short_book/05_decision.json
```

- fixed_tracked 主 agent 不读取所有研究包，而是先根据今日异常、量价、宏观判定与 `data/skill_runs/_analysis_index.json` 挑出 P0。用户确认后，每只 P0 使用 Bull、Bear、三名 Juror 和唯一 finalizer；辩论产物写入互不冲突的路径，第二次人工确认后通过 `scripts/merge_subagent_decisions.py --source debate` 生成 `05_decision.json`。
- short_book 继续使用原单 subagent 流程、上限 7 只、最大持仓 20 个交易日；长期候选由 fixed_tracked 统一分析。
- fixed_tracked 的买入规则采用“严格准入、分批建仓、有效初仓、证伪退出”：基本面、估值和逻辑先过关；买点不要求完美，时点不确定性通过分批处理；初仓和目标仓位必须按真实总资产计算并具有实际意义；确认后加仓，逻辑证伪后退出。Bull、Bear、Rebuttal、Juror 和 finalizer 必须利用月度经营公告、产销/交付、订单、价格、排产及产业链数据完成下一报告期盈利推演；财报是最终验证而非默认等待点。产业爆发框架 B 默认禁用，仅当最新财报深研同时确认行业总量爆发、供需错配、最受益环节、龙头地位、公司基本面右侧和折价后估值空间时才能启用；普通科技成长不得自动套用。short_book 继续沿用短线催化与量价确认规则。
- 详见 `.codex/skills/auto-trading-fixed-tracked/SKILL.md` / `auto-trading-short-book/SKILL.md`。

### 1.6 人工确认后分别执行后处理

```bash
9.  python scripts/run_post_trade.py --date 2026-06-11 --book-type fixed_tracked --signature book-fixed_tracked
10. python scripts/run_post_trade.py --date 2026-06-11 --book-type short_book  --signature book-short_book
```

由 `services/trading/post_trade_pipeline.py` 编排，串联 `05` → `06-08` 后处理：

- `06_execution_log.json`：交易执行结果（写入 `data/agent_data/{signature}/position/position.jsonl`）
- `07_daily_summary.json`：当日组合级总结（system_risk_notes / system_focus_items / portfolio_overview）
- `08_history_merge.json`：写入 `data/agent_data/{signature}/{stock_decisions.json, decision_summary.json, portfolio_daily_summary.json}` 并合并连续 HOLD/FLAT 序列

历史决策合并逻辑详见 `docs/trade_summary/`。

---

## 2. 仓库主结构

```text
.
├── scripts/                    # CLI 入口，仅做参数解析 + 调用 services
│   ├── refresh_all_for_date.py
│   ├── manage_daily_data.py
│   ├── manage_selection_system.py
│   ├── prepare_financial_report_skill.py
│   ├── run_daily_pipeline.py
│   ├── run_post_trade.py
│   ├── merge_subagent_decisions.py
│   └── ...
├── services/                   # 业务编排层
│   ├── data_refresh/           # 一键刷新编排（refresh_orchestrator.py）
│   ├── pipeline/               # 01-04 产物生成（daily_pipeline.py + steps/）
│   ├── prompting/              # system prompt 组装
│   ├── research/               # 宏观/新闻/财报/个股研究的核心实现
│   ├── industry_research/      # 半年结构扫描、月度领先指标监控、产业链深研
│   ├── selection_system/       # 选股系统：universe / news / board_heat / factor_store / quant_prefilter / candidate_selection
│   ├── snapshot/               # basic_snapshot
│   └── trading/                # 交易执行 + 06-08 后处理（post_trade_pipeline.py / trade_summary.py / trade_executor.py）
├── shared_data_access/         # 统一外部数据访问与缓存
│   ├── data_access.py          # SharedDataAccess.prepare_dataset() 唯一入口
│   ├── cache_registry.py       # 缓存类型 / TTL / 路径登记
│   ├── chip_distribution.py    # 筹码分布抓取/回退计算
│   ├── board_metrics.py        # 板块行情/历史
│   ├── indicator_library.py    # 统一指标库
│   ├── macro_objective_panel.py
│   ├── industry_catalog.py     # 申万行业目录缓存
│   ├── industry_financial_panel.py # 全A行业财务扩散验证
│   ├── models.py / paths.py / exceptions.py / validation.py
├── core/                       # 通用基础设施
│   ├── logging.py              # 统一日志入口
│   ├── llm_output.py
│   └── runtime_state.py
├── configs/
│   ├── stock_pool.py           # TRACKED_A_STOCKS（fixed_tracked 静态池）
│   ├── prompt_flow/            # fixed_tracked Markdown policy + short/legacy JSON flow
│   └── selection_system/       # factor_scoring.yaml 等评分配置
├── data/                       # 运行产物与缓存
├── logs/                       # 组件日志
├── .codex/                     # 项目规则、skills、commands（主维护目录）
│   ├── rules/
│   ├── skills/
│   └── commands/
├── docs/                       # 设计与系统文档（本文所在）
├── agent_tools/, tools/        # 历史兼容层，新代码不再向此处沉淀
└── basic_stock_info.py / shared_financial_utils.py / stock_price_dynamics_summarizer.py / enhanced_pe_pb_analyzer.py
                                # 历史保留的顶层脚本，仍由 daily 链路调用，新逻辑不再继续堆在这里
```

---

## 3. 数据与缓存基座

### 3.1 统一入口 `SharedDataAccess`

- **唯一入口**：`shared_data_access.SharedDataAccess.prepare_dataset(symbolInfo, as_of_date, ...)` 是访问 akshare / 巨潮的唯一路径。
- **交易日入口**：`shared_data_access.market_calendar` 优先使用 `000001.IDX` 实际行情日期，缓存不能完整覆盖请求区间时使用 `pandas_market_calendars` 的 `SSE` 日历；加载失败时直接报错，禁止退化成“周一至周五”近似。
- 它会先调用 `ensure_symbol_data` 刷新价格、财报、股本、公告缓存，再按 `as_of_date` 截断 DataFrame，组装 `FinancialDataBundle` / `PriceDataBundle` / `ShareInfo` / `DisclosureBundle` 返回。
- 当历史请求日期早于价格缓存首个交易日时，统一标记为 `not_listed_as_of_date` 并从当日研究集合排除；缓存缺失、损坏或已上市股票异常缺数据仍按错误处理。
- ETF / 指数会自动降级为「仅价格」模式。
- 设计与调用姿势详见 `docs/share_data_access/README.md`。

### 3.2 缓存注册表

- 所有缓存类型登记在 `shared_data_access/cache_registry.py` 的 `CacheKind` 与 `BASE_REGISTRY`。
- 当前已注册缓存除逐股财报、行情、股本、公告、分析、筹码和一致预期外，还包括板块、宏观、全A行业财务面板与申万行业目录等全局数据集。
- TTL、路径、必需文件、刷新元信息 (`.cache_registry_meta.json`) 等机制详见 `docs/cache/cache_registry_design.md`。

### 3.3 每日刷新策略归属（重要）

`shared_data_access` 只是**机制层**（负责「抓 / 缓存 / 切片」）。

**「今天哪些股票该被强刷」由 `services/data_refresh/refresh_orchestrator.py` 唯一决定**：

- 默认每日刷新范围 = `TRACKED_A_STOCKS ∪ master_universe`（约 100+ 只）
- 价格 / 财报结构化 / basic_info 由 `manage_daily_data` 统一负责（轻量档 `--force-refresh-price`，重量档 `--force-refresh`）
- 公告（disclosures）由 `selection_system.build-announcements` 按 universe 增量负责；`manage_daily_data` 用 `--skip-disclosures` 跳过重复扫描
- **上层模块不得私设 `force_refresh_*=True`**；若读取阶段发现缓存过期，应向 orchestrator 反馈（warning / 异常）由策略层统一修正

### 3.4 因子库 Factor Store

- 路径：`data/factor_store/`
- 结构：`by_symbol/{symbol}.{csv,parquet}` 是单股纵向视图，`by_date/{date}.{csv,parquet}` 是当日横截面视图
- 由 `services/selection_system/factor_store.py + factor_history.py + factor_scoring.py + quant_prefilter.py` 统一管理
- 评分配置：`configs/selection_system/factor_scoring.yaml`（按 `stock_type ∈ {growth, cyclical, special}` 配置门槛与权重）
- 输出 `13_factor_scores.{csv,json}` 与 `12_quant_prefilter*.{csv,json}`，供 `run_daily_pipeline` 在选股 candidate 缺失时直接回退
- 详细设计与历史回测口径详见 `docs/selection_system/因子库与量化初筛系统设计.md`

### 3.5 数据落地约定

- 每只股票一个目录：`data/stock_info/{stock_name}_{symbol}/`，下含 `prices/`、`financials_cache/`、`share_info/`、`disclosures/`、`news/`、`analysis/`、`pe_pb_analysis/`、`chip_distribution/`、`financial_reports/`、`forecast/` 等子目录
- 全局缓存：`data/global_cache/`（板块、宏观、相似股、symbol 映射等）
- 选股运行产物：`data/selection_runs/YYYY-MM-DD/`
- 三账本运行产物：`data/skill_runs/YYYY-MM-DD/{fixed_tracked,short_book,long_book}/`
- 交易归档：`data/agent_data/book-{book_type}/`

---

## 4. 选股系统现状（量化因子初筛主轴）

当前主轴是「量化因子初筛 + 各账本独立深研」。LLM 直接从全宇宙选股的方案（`auto-selection-daily-pipeline` skill）保留为可选路径，日常默认不跑。

独立的 `industry_research` 研究层不属于日常选股主轴。它先半年级扫描申万二级行业的三至五年结构空间，再按月更新已批准主题的需求、订单、供给、库存、价格、交期和资本开支等领先指标，并在财报披露窗口用全A行业财务扩散做季度验证。热点新闻、板块涨幅、资金流和股票动量不参与行业发现。统计宇宙、结构候选宇宙与人工批准的交易研究宇宙相互分离；每月最多深研一个经用户确认的产业主题。该层不修改 `01-08`、长期池或交易仓位。详见 `docs/selection_system/行业景气研究系统设计.md`。

### 4.1 当前在用的链路

1. `master_universe` → `data/universe/master_universe.json`（选股股票宇宙，由 `bootstrap` 子命令初始化）
2. `run-news` → 全市场新闻采集 / 去重 / 增强 → `03_news_prompt_input.json`
3. `build-board-heat-state` → 板块热度研究 → `05_board_heat_state.json` / `05_board_heat_digest.json`
4. `/daily-macro-summary` + `/gradual-hot-news-summary` → 宏观总结 + 渐进式主题状态 → `06_hot_news_state.json`
5. `build-factor-store` → `data/factor_store/by_symbol/*` 与 `by_date/{date}.*` + `data/selection_runs/{date}/12_factor_snapshot.*`
6. `build-factor-scores` → `13_factor_scores.{csv,json}`（按 `factor_scoring.yaml` 配置生成 short_score / long_score）
7. `build-quant-prefilter` → `12_quant_prefilter.csv` + `12_quant_prefilter_short.csv` + `12_quant_prefilter_long.csv`

### 4.2 量化初筛 → 三账本的衔接

`run_daily_pipeline --all-books` 在生成 short_book / long_book 的 01-04 产物时，优先看 `08_short_book_candidates.json` / `09_long_book_candidates.json`；若不存在则回退到 `12_quant_prefilter_short.csv` / `12_quant_prefilter_long.csv`。

日常默认走「量化初筛 → 直接进三账本」，因此 `08/09` 这两个 LLM 选股产物大多不存在，三账本会自动用 prefilter 结果作为股票池。

### 4.3 设计文档索引

- `docs/selection_system/因子库与量化初筛系统设计.md` — 当前主轴
- `docs/selection_system/板块热度摘要与查询设计.md` — `05_board_heat_digest` / `query_board_snapshot.py`
- `docs/selection_system/05_board_heat_state字段说明.md` — `05_board_heat_state.json` 字段字典

---

## 5. 交易后处理与历史决策合并

### 5.1 文件布局（按账本一套）

`data/agent_data/book-{book_type}/`：

- `position/position.jsonl` — 每日仓位记录，由 `tools.price_tools` 写入；包含 `IF_TRADE` 标记
- `position/manual_position_override.json` — 人工干预入口
- `stock_decisions.json` — 原始逐股决策表（追加写入）
- `decision_summary.json` — 合并后的决策摘要（连续 HOLD/FLAT 序列合并为一条）
- `portfolio_daily_summary.json` — 组合级别的 system_risk_notes / system_focus_items / portfolio_overview

### 5.2 三步流程

1. `save_daily_operations(signature, ai_output_json)` 把 `05_decision.json` 的 `stock_decisions` 写入 `stock_decisions.json`（同日唯一）
2. `process_and_merge_operations` 重建 `decision_summary.json`：连续 HOLD/FLAT 序列合并为一条，BUY/SELL 保留为独立记录
3. `get_historical_context` / `get_portfolio_historical_context` / `load_yesterday_daily_summary` 为下一日 prompt 提供历史上下文

### 5.3 设计文档

`docs/trade_summary/README.md` — 完整的 JSON 契约、合并规则、与代码的对照。

---

## 6. 兼容层与运行治理

### 6.1 兼容层现状

- `agent_tools/` 与 `tools/` 仍保留少量历史导入路径兼容包装，真实业务实现已经迁移到 `services/` 与 `core/`
- 旧时代的 MCP 服务脚本（`start_mcp_services.py`、`tool_python.py`、`tool_math.py`）已经清理
- `basic_stock_info.py`、`enhanced_pe_pb_analyzer.py`、`stock_price_dynamics_summarizer.py`、`shared_financial_utils.py`、`tool_financial_report.py` 仍位于仓库根目录，是历史保留的顶层脚本，仍被日常链路调用，但**不再继续在此沉淀新逻辑**

### 6.2 日志规范

- 禁止在 `services/`、`core/`、`shared_data_access/`、`agent_tools/` 等库代码里直接用 `print`
- 统一通过 `core.logging` 入口：`get_logger()` / `init_component_logger()` / `init_tool_logger()`
- Logger 名必须是业务语义明确的 PascalCase（如 `ManageDailyData`、`DailyPipeline`、`TradeSummary`）
- 详见 `.codex/rules/code-style.md`

---

## 7. 规则与 skills 索引

### 7.1 主维护目录：`.codex/`

仓库规则、skills、commands 都以 `.codex/` 为唯一主维护目录（`.claude` 是软链）。

### 7.2 Rules（`.codex/rules/`）

| 文件 | 用途 |
| --- | --- |
| `pre_commit_rule.md` | Angular 风格 + 简体中文的 commit 规范 |
| `code-style.md` | Python 代码风格 + 统一日志规范 |
| `shared-data-access.md` | 缓存 / 时间截断 / SymbolInfo / 数据访问统一入口 |
| `skill-pipeline.md` | 01-08 产物契约、脚本分层、交易后处理约束 |
| `testing.md` | evidence-first 调试、最小复现、主链路验证要求 |

### 7.3 Skills（`.codex/skills/`）

| Skill | 触发场景 |
| --- | --- |
| `daily-macro-summary` | 用户说「更新今天的宏观总结」 → `data/macro_economy/YYYYMMDD.md` |
| `gradual-hot-news-summary` | 用户说「更新今日热点主题总结」 → `06_hot_news_state.json` |
| `monthly-industry-research` | 用户说「开始本月行业研究」 → 月度行业雷达与单主题产业链深研 |
| `auto-selection-daily-pipeline` | 用户说「开始今天自动选股」（实验性，日常通常不跑） |
| `financial-report-summary` | 用户说「生成财报总结」 → 各股 `financial_reports/*.md` |
| `auto-trading-fixed-tracked` | 用户说「开始今天固定股票池交易」 → `fixed_tracked/05_decision.json` |
| `auto-trading-short-book` | 用户说「开始今天短线股票池交易」 → `short_book/05_decision.json` |
| `auto-trading-long-book` | 用户说「开始今天长期股票池交易」 → `long_book/05_decision.json` |
| `backtest-fixed-tracked` | 指定历史区间 → 隔离运行 fixed_tracked 多 Agent 决策、D+1 开盘模拟成交与净值汇总 |
| `add-skill-pipeline-step` | 修改 / 新增 01-08 流水线步骤时使用 |
| `extend-shared-data-access` | 新增数据源、缓存目录、衍生指标时使用 |

### 7.4 Commands（`.codex/commands/`）

| Command | 用途 |
| --- | --- |
| `review-skill-run` | 检查某一天的 skill 运行产物、日志与交易后处理是否完整且一致 |

---

## 8. docs 子目录索引

| 路径 | 用途 |
| --- | --- |
| `docs/PROJECT_SYSTEM_SUMMARY.md` | **当前文档**，项目入口总览 |
| `docs/cache/cache_registry_design.md` | 缓存注册表机制与所有 `CacheKind` 说明 |
| `docs/share_data_access/README.md` | `SharedDataAccess.prepare_dataset()` 调用姿势与策略归属 |
| `docs/manage_data/data_refresh_plan.md` | 一键刷数据流水线设计（`refresh_all_for_date.py` + `refresh_orchestrator.py`） |
| `docs/selection_system/因子库与量化初筛系统设计.md` | 选股主轴：因子库 + 评分配置 + 量化初筛 + 回测 |
| `docs/selection_system/行业景气研究系统设计.md` | 独立月度层：行业雷达 + 单主题深研 + 景气状态与利润映射 |
| `docs/selection_system/板块热度摘要与查询设计.md` | `05_board_heat_digest` + `query_board_snapshot.py` |
| `docs/selection_system/05_board_heat_state字段说明.md` | `05_board_heat_state.json` 字段字典 |
| `docs/news/README.md` | 上市公司公告新闻系统设计 |
| `docs/trade_summary/README.md` | 每日操盘总结 JSON 契约与处理流程 |
| `docs/fundamental_research/README.md` | 财报研究文件约定与 skill 入口 |
| `docs/财报样例` | 财报研究 prompt 样例（人工资料）|

---

## 9. 重大重构时的更新约定

- 任何修改 01-08 文件契约（字段、目录、文件名）、缓存策略、`shared_data_access` 入口、交易执行规则、`factor_scoring.yaml` 评分口径的改动，都应**先在对应 docs 中同步更新**，再提交代码；不要让设计文档与代码脱节。
- 新增 skill / command / rule 时，更新本文「7. 规则与 skills 索引」表格；新增 docs 子目录时，更新「8. docs 子目录索引」表格。
- 重大主流程调整后，更新本文顶部的「更新日期」与「1. 日常主流程」章节。
