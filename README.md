# AI Value Investing Agent

AI驱动的价值投资。核心思路是先用 Python 准备好当日全部数据和输入产物，再由本地 Agent 按照 `.codex/skills/` 中的 skill 依次执行多 Agent 辩论、生成交易决策。

支持 A 股、港股、ETF/指数等不同标的；固定池、短线池、长期池分别作为独立账本运行。

## 系统架构

```
┌─────────────────────────────────────────────────┐
│  Python 数据层（一次性运行）                        │
│  scripts/refresh_all_for_date.py                 │
│  → 行情/财报/股本/公告/新闻/板块/因子/量化初筛      │
├─────────────────────────────────────────────────┤
│  Python 输入产物（一次性运行）                      │
│  scripts/run_daily_pipeline.py                   │
│  → 01_global_context / 02_snapshot /             │
│    03_agent_input / 04_stock_research            │
├─────────────────────────────────────────────────┤
│  LLM Skill 层（逐 skill 人工触发）                 │
│  /daily-macro-summary                            │
│  /gradual-hot-news-summary                       │
│  /financial-report-summary                       │
│  /auto-trading-fixed-tracked ← 多 Agent 辩论      │
│  /auto-trading-short-book                        │
│  /auto-trading-long-book                         │
├─────────────────────────────────────────────────┤
│  Python 后处理（人工确认后运行）                    │
│  scripts/run_post_trade.py                       │
│  → 06_execution_log / 07_daily_summary /         │
│    08_history_merge                              │
└─────────────────────────────────────────────────┘
```

### 多 Agent 辩论架构（fixed_tracked）

fixed_tracked 账本采用中心化管理 + 逐股多 Agent 辩论：

- **主 Agent**：读取宏观/快照/研究包，根据今日异常、量价、宏观判定与历史分析索引挑出 P0 候选
- **Bull（多头）**：从基本面、估值、催化三维度构建做多逻辑
- **Bear（空头）**：挑战关键假设、财务质量、估值合理性
- **Rebuttal（反驳）**：Bull/Bear 交叉质询
- **Juror × 3（陪审员）**：独立投票，给出 action/confidence/price_impression
- **Finalizer（终审）**：聚合投票结果，生成最终裁决

### 三账本体系

| 账本 | 股票来源 | 分析方法 | 持仓周期 |
| --- | --- | --- | --- |
| `fixed_tracked` | 静态池（`configs/stock_pool.py`） | 多 Agent 辩论 | 长期跟踪 |
| `short_book` | 量化初筛 / LLM 选股 | 单 Agent 催化+量价 | ≤20 交易日 |
| `long_book` | 量化初筛 / LLM 选股 | 单 Agent 质量+增长 | 灵活 |

## 当前日常流程

> **运行环境**：建议使用 [opencode](https://github.com/anomalyco/opencode) 运行，它对并发 SubAgent 数量没有限制，且自带搜索工具，无需额外配置。
> 
> **运行原则**：以下全部步骤在一个会话里一次性跑完，不需要等待和中断。Agent 应自行处理超时和并发调度。
> 
> **搜索要求**：使用 opencode 自带的 Search MCP 即可，不需要额外配置阿里云百炼 MCP。
> 
> **网络代理**：执行 `git push` 等需要访问 GitHub 的命令前，先执行 `proxy_on`（定义在 `~/.bashrc` 中）。
> 
> **日期语义**：所有主脚本的 `--date` 都表示"要分析的交易日"，即收盘数据已产生的那一天。日常节奏是第二天早上分析昨天收盘；周末或节假日请手动指定最近一个交易日。非交易日自动 SKIPPED，不刷新数据。

### 1. 激活虚拟环境 + 一键刷新全部数据

```bash
source /home/zhangbeiqing/venv/ai_stock/bin/activate
python scripts/refresh_all_for_date.py --date cur_date
```

> `cur_date` 替换为实际日期，如 `2026-08-06`。

对 `TRACKED_A_STOCKS ∪ master_universe`（约 100+ 只）刷新：
- 价格、财报结构化缓存、宏观客观面板
- 全市场新闻采集/去重/增强
- 板块热度分析
- 因子库构建、因子评分、量化初筛（短/长两本候选）
- 清理旧研究缓存

该命令约需 **15 分钟**，Agent 请设置足够的超时时间，避免中途中断。

### 2. 启动 MinerU（财报 PDF 转 Markdown）

```bash
scripts/start_mineru_api.sh
```

> 先检查 MinerU 是否已经启动（如 `ps aux | grep mineru`），若已运行则跳过此步。

MinerU 用于将财报 PDF 转换为 Agent 更易分析的 Markdown 格式。

### 3. 并发启动 3 个 SubAgent

步骤 1 完成后，**在同一条消息里一次性启动 3 个 SubAgent**，不要逐个串行等待：

**SubAgent A — 宏观总结**（约需 5-10 分钟）

> 执行： "开始 cur_date 的宏观总结"
> 
> 触发 skill：`daily-macro-summary`
> 
> 产出：`data/macro_economy/YYYYMMDD.md`

**SubAgent B — 渐进式新闻总结**（约需 20 分钟）

> 执行： "开始 cur_date 的渐进式新闻总结，没用的已经过时的新闻就删掉，不要让渐进式新闻总结文件太大"
> 
> 触发 skill：`gradual-hot-news-summary`
> 
> 产出：`data/selection_runs/YYYY-MM-DD/06_hot_news_state.json`
> 
> 注意：需定期清理过期新闻主题，控制文件体积。

**SubAgent C — 财报深研**（约需 10-30 分钟）

> 先运行准备脚本：
> ```bash
> python scripts/prepare_financial_report_skill.py --date cur_date --sync-first --json --include-quant-prefilter
> ```
> 
> 该命令可能需要 10-30 分钟（主要是把新发的财报 PDF 转 Markdown），请设置足够的超时时间。
> 
> 然后按 skill 要求，对需要更新财报的公司逐一执行财报深研。
> 
> 触发 skill：`financial-report-summary`
> 
> 产出：`data/stock_info/{name}_{symbol}/financial_reports/*.md`

### 4. 生成全部 01-04 股票研究包

步骤 3 的三个 SubAgent 全部完成后，运行：

```bash
python scripts/run_daily_pipeline.py --date cur_date --max-workers 6 --all-books
```

产出三账本的完整输入产物：

```text
data/skill_runs/YYYY-MM-DD/
├── run_manifest.json
├── fixed_tracked/
│   ├── 01_global_context.md           # 宏观/大盘/新闻
│   ├── 02_basic_snapshot_payload.json # 个股快照
│   ├── 03_agent_input.md             # 投资策略 + P0 筛选输入
│   ├── 03_stock_analysis_input.md    # 个股辩论研究方法
│   └── 04_stock_research/            # 逐股研究包 *.md
├── short_book/
└── long_book/
```

至此，当日全部数据和输入产物准备完毕，后续可按需触发交易 skill。

### 5. 逐账本运行交易 Skill（后续步骤）

```text
/auto-trading-fixed-tracked     → fixed_tracked/05_decision.json
/auto-trading-short-book        → short_book/05_decision.json
/auto-trading-long-book         → long_book/05_decision.json
```

- **fixed_tracked**：主 Agent 先读 01-04 和 `_analysis_index.json` 挑出 P0 → 用户确认 → 全量并发启动 Bull/Bear → Rebuttal → 3 名 Juror → Finalizer → 聚合投票 → 用户二次确认 → 生成 `05_decision.json`
- **short_book**：单 Agent 逐股分析，上限 7 只，最大持仓 20 个交易日
- **long_book**：单 Agent 逐股分析，当前长期候选并入 fixed_tracked 统一分析

真实执行前需要人工确认 `05_decision.json`。

### 6. 交易后处理

人工确认后，按账本分别执行：

```bash
python scripts/run_post_trade.py --date cur_date --book-type fixed_tracked --signature book-fixed_tracked
python scripts/run_post_trade.py --date cur_date --book-type short_book  --signature book-short_book
python scripts/run_post_trade.py --date cur_date --book-type long_book   --signature book-long_book
```

后处理串联 `05 → 06-08`，写入 `data/skill_runs/YYYY-MM-DD/{book_type}/` 和 `data/agent_data/book-{book_type}/`。

## 修改投资策略

如果需要调整投资决策规则，请修改以下文件：

```
configs/prompt_flow/fixed_tracked/investment_policy.md
```

该文件是 `fixed_tracked` 全部 Agent（主 Agent、Bull、Bear、Juror、Finalizer）共用的核心投资策略。修改后将在下一次运行交易 skill 时自动生效。

## 回测系统

对指定历史日期区间，自动运行 fixed_tracked 多 Agent 决策回测：

```bash
python scripts/manage_fixed_tracked_backtest.py prepare --experiment my_test --start 2025-01-01 --end 2025-12-31
python scripts/manage_fixed_tracked_backtest.py status --experiment my_test
```

核心特性：
- **实验隔离**：每个回测实验有独立的 `data/backtest_experiments/` 目录，不触碰真实账本
- **冻结输入**：逐日生成只读模式的 01-04 产物，基于历史日期截断行情和公告，不写共享缓存
- **D+1 开盘模拟成交**：使用次日开盘价作为模拟成交价
- **财经研究集成**：当回测日有新财报公告时，自动触发财报深研（`historical_mode`，跳过联网数据）
- **覆盖校验**：自动检测股票上市日期，跳过尚未上市的标的
- **断点续跑**：支持中断后从最后完成的交易日继续
- **度量汇总**：执行结束后输出收益率、夏普比率、最大回撤等关键指标

详见 `.codex/skills/backtest-fixed-tracked/SKILL.md`。

## 其他核心系统

### 月度行业研究

独立于交易主轴的行业景气度研究系统：

- **半年级结构扫描**：对申万二级行业做三至五年结构空间筛查（structural_growth / cyclical / mature / uncertain）
- **月度领先指标监控**：跟踪需求、订单、供给、库存、价格、交期和资本开支等领先指标
- **季度财务验证**：财报披露窗口用全A行业财务扩散数据做验证
- **单主题深研**：经用户确认后开展产业链深研，每月最多一个主题

该层不修改 `01-08`、长期池或交易仓位。详见 `.codex/skills/monthly-industry-research/SKILL.md`。

### 自动选股流水线（实验性）

LLM 直接从全宇宙选股的方案，日常默认不跑：

```text
/auto-selection-daily-pipeline
```

由 2 个 subagent 分别基于 `08_short_book_input` 和 `09_long_book_input` 生成短期/长期候选池，merge 后产生深研队列。详见 `.codex/skills/auto-selection-daily-pipeline/SKILL.md`。

### 量化因子初筛（默认选股主轴）

日常默认的选股路径是量化因子初筛：`master_universe → factor_store → factor_scoring → quant_prefilter`，直接产出短/长两本候选池进入三账本。评分配置见 `configs/selection_system/factor_scoring.yaml`。

## 目录结构

```text
scripts/                        CLI 入口，仅做参数解析 + 调用 services
├── refresh_all_for_date.py     一键刷数据 + 选股（日常入口）
├── manage_daily_data.py        数据刷新（单步）
├── prepare_financial_report_skill.py  财报深研环境准备
├── run_daily_pipeline.py       01-04 输入产物生成
├── merge_subagent_decisions.py 多 Agent 辩论结果聚合
├── manage_fixed_tracked_backtest.py  回测实验管理
└── run_post_trade.py           交易后处理
services/                       业务编排层
├── data_refresh/               一键刷新编排
├── pipeline/                   01-04 产出（daily_pipeline + steps/）
├── prompting/                  system prompt 组装
├── research/                   宏观、新闻、财报、个股研究
├── selection_system/           选股系统（universe / news / factors / prefilter）
├── snapshot/                   basic_snapshot
├── trading/                    交易执行 + 06-08 后处理
├── industry_research/          月度行业研究
└── backtest/                   回测引擎（实验/输入/账本/执行/度量）
shared_data_access/             统一外部数据访问与缓存
├── data_access.py              SharedDataAccess.prepare_dataset() 唯一入口
├── cache_registry.py           缓存类型 / TTL / 路径登记
├── market_calendar.py          交易日历（基于000001.IDX实际行情 + SSE日历）
├── historical_prices.py        回测历史行情读取与校验
├── macro_objective_panel.py    宏观客观面板
└── ...
core/                           通用基础设施
├── logging.py                  统一日志入口
├── run_context.py              回测运行上下文
└── ...
configs/
├── stock_pool.py               TRACKED_A_STOCKS（固定池16只）
├── prompt_flow/fixed_tracked/  investment_policy / main_policy / stock_analysis_policy
├── prompt_flow/skill_flow*.json  short_book 兼容 Prompt flow
├── research/                   财报输出 schema
└── selection_system/           factor_scoring.yaml
data/                           运行产物与缓存
├── skill_runs/YYYY-MM-DD/      三账本 01-08 产物
├── selection_runs/YYYY-MM-DD/  选股运行产物
├── backtest_experiments/       回测实验目录（隔离）
├── stock_info/{name}_{symbol}/ 逐股缓存（行情/财报/公告/研究）
├── factor_store/               因子库
├── agent_data/book-{type}/     交易归档（隔离账本）
├── research_artifact_cache/    研究产物缓存
└── macro_economy/              宏观总结
logs/                           组件日志
.codex/                         规则、skills、commands（主维护目录）
├── rules/                      pre_commit / code-style / skill-pipeline / testing
├── skills/                     12 个日常运行 skills
└── commands/                   review-skill-run
docs/                           系统设计文档
agent_tools/, tools/            历史兼容层，新代码不再向此处沉淀
```

## 关键约定

- `--date` 统一表示"要分析的交易日"，不是脚本运行当天，不是未来日期。
- 日常入口是 `python scripts/refresh_all_for_date.py`，不要从旧的 `manage_daily_data.py` 开始。
- 外部行情、财报、股本、公告访问必须经 `shared_data_access.SharedDataAccess`。
- `data/skill_runs/YYYY-MM-DD/` 下的 `01-08` 文件是 skill 流水线契约，改字段、文件名或目录结构前要同步规则和文档。
- 运行产物写入 `data/`，日志写入 `logs/`，不要把临时产物塞进源码目录。
- 上层模块不得私设 `force_refresh_*=True`，每日刷新策略由 `refresh_orchestrator.py` 唯一决定。
- Git 提交使用 Angular 格式 + 简体中文，详见 `.codex/rules/pre_commit_rule.md`。

## Skills 索引

| Skill | 触发方式 | 产物 |
| --- | --- | --- |
| `daily-macro-summary` | 说"更新今天宏观总结" | `data/macro_economy/YYYYMMDD.md` |
| `gradual-hot-news-summary` | 说"更新今日热点主题总结" | `06_hot_news_state.json` |
| `financial-report-summary` | 说"生成财报总结" | `financial_reports/*.md` |
| `auto-trading-fixed-tracked` | 说"开始今天固定股票池交易" | `fixed_tracked/05_decision.json` |
| `auto-trading-short-book` | 说"开始今天短线股票池交易" | `short_book/05_decision.json` |
| `auto-trading-long-book` | 说"开始今天长期股票池交易" | `long_book/05_decision.json` |
| `backtest-fixed-tracked` | 说"回测固定股池" | `backtest_experiments/` |
| `auto-selection-daily-pipeline` | 说"开始今天自动选股"（实验性） | `10_candidate_merge.json` |
| `monthly-industry-research` | 说"开始本月行业研究" | `industry_research/` |
| `add-skill-pipeline-step` | 修改 01-08 流水线步骤时 | — |
| `extend-shared-data-access` | 新增数据源 / 缓存时 | — |
| `review-skill-run` | 检查某一天产物完整性 | — |

## 文档索引

| 文档 | 内容 |
| --- | --- |
| `docs/PROJECT_SYSTEM_SUMMARY.md` | 系统白皮书，含完整主流程、缓存策略、选股系统、交易后处理 |
| `docs/share_data_access/README.md` | 统一数据访问层调用姿势与策略归属 |
| `docs/cache/cache_registry_design.md` | 缓存注册表机制 |
| `docs/selection_system/因子库与量化初筛系统设计.md` | 量化因子初筛主轴设计 |
| `docs/selection_system/行业景气研究系统设计.md` | 月度行业研究系统 |
| `docs/trade_summary/README.md` | 交易后处理与历史决策合并 |
| `docs/fundamental_research/README.md` | 财报深研文件约定 |
| `docs/backtest/fixed_tracked_agent_backtest_design.md` | 回测系统设计 |
| `.codex/rules/skill-pipeline.md` | 01-08 文件契约 |

## 安装与配置

```bash
pip install -r requirements.txt
cp .env.example .env
```

### MinerU（财报 PDF 转 Markdown）

本项目使用 [MinerU](https://github.com/opendatalab/MinerU) 将财报 PDF 转为 Markdown，需单独安装：

```bash
git clone https://github.com/opendatalab/MinerU.git
cd MinerU
pip install -e .
```

MinerU 启动方式：

```bash
scripts/start_mineru_api.sh
```

主要配置文件：

- `configs/stock_pool.py` — 固定跟踪池（当前 16 只 A/港股）
- `configs/prompt_flow/fixed_tracked/investment_policy.md` — 核心投资策略
- `configs/prompt_flow/fixed_tracked/main_policy.md` — 主 Agent Prompt
- `configs/prompt_flow/fixed_tracked/stock_analysis_policy.md` — 辩论角色研究方法
- `configs/selection_system/factor_scoring.yaml` — 量化初筛评分规则
- `.env` — 模型 API Key、Base URL 等运行参数

## License

[MIT License](LICENSE)
