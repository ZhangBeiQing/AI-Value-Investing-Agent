# AI Value Investing Agent

这是一个本地 `skill-only` 的 AI 股票研究、选股与交易流水线。当前系统的核心使用方式不是让 Agent 临时在线取数，而是先用 Python 把当天需要的数据、缓存和输入产物准备好，再由 Codex / 本地 Agent 按 `.codex/skills/` 里的 skill 一个一个执行。

当前支持 A 股、港股、ETF/指数等不同标的；固定池、短线池、长期池分别作为独立账本运行。

## 当前日常流程

所有主脚本的 `--date` 都表示“要分析的交易日”，也就是已经产生收盘数据的那一天。日常节奏通常是第二天早上分析昨天收盘；周末或节假日请手动指定最近一个交易日。

### 1. 刷新当天全部数据

```bash
source /home/zhangbeiqing/venv/ai_stock/bin/activate
python scripts/refresh_all_for_date.py --date 2026-07-06
```

这个命令是现在的日常数据入口，会刷新价格、财报结构化缓存、宏观客观面板、新闻、板块热度、因子库、量化初筛等 Python 链路数据。跑完后脚本会打印后续 skill 清单。

常用参数：

```bash
python scripts/refresh_all_for_date.py --date 2026-07-06 --fresh-heavy
python scripts/refresh_all_for_date.py --date 2026-07-06 --no-generate-prefilter
python scripts/refresh_all_for_date.py --date 2026-07-06 --skip-news-boards
```

- `--fresh-heavy`：额外强刷财报结构化数据等重缓存。
- `--no-generate-prefilter`：只刷固定池，不生成量化初筛。
- `--skip-news-boards`：跳过新闻和板块热度，适合调试或已确认缓存可复用时。

### 2. 逐个运行研究类 skill

数据刷新完成后，按需依次触发这些 skill：

```text
/daily-macro-summary
/gradual-hot-news-summary
```

如果当天需要补财报深研，再执行：

```bash
python scripts/prepare_financial_report_skill.py --date 2026-07-06 --sync-first --json --include-quant-prefilter
```

然后触发：

```text
/financial-report-summary
```

这些 skill 会把宏观总结、热点主题状态、逐股财报总结写入 `data/macro_economy/`、`data/selection_runs/`、`data/stock_info/*/financial_reports/` 等目录，供后续交易 skill 继承。

### 3. 生成 01-04 交易输入产物

固定池默认模式：

```bash
python scripts/run_daily_pipeline.py --date 2026-07-06 --max-workers 6
```

三账本模式：

```bash
python scripts/run_daily_pipeline.py --date 2026-07-06 --max-workers 6 --all-books
```

输出位于：

```text
data/skill_runs/YYYY-MM-DD/
├── run_manifest.json
├── fixed_tracked/
│   ├── 01_global_context.md
│   ├── 02_basic_snapshot_payload.json
│   ├── 03_agent_input.md
│   └── 04_stock_research/*.md
├── short_book/
└── long_book/
```

`fixed_tracked` 来自 `configs/stock_pool.py`；`short_book` / `long_book` 优先使用选股 skill 产物，缺失时回退到量化初筛结果。

### 4. 逐个运行交易 skill

生成 01-04 后，按账本逐个触发：

```text
/auto-trading-fixed-tracked
/auto-trading-short-book
/auto-trading-long-book
```

每个交易 skill 会读取对应账本目录下的 `01-04` 输入，派发必要的单股 subagent，最后生成：

```text
data/skill_runs/YYYY-MM-DD/{book_type}/05_decision.json
```

真实执行前需要人工确认 `05_decision.json`。

### 5. 人工确认后运行交易后处理

```bash
python scripts/run_post_trade.py --date 2026-07-06 --book-type fixed_tracked --signature book-fixed_tracked
python scripts/run_post_trade.py --date 2026-07-06 --book-type short_book --signature book-short_book
python scripts/run_post_trade.py --date 2026-07-06 --book-type long_book --signature book-long_book
```

后处理会生成并归档：

- `06_execution_log.json`
- `07_daily_summary.json`
- `08_history_merge.json`
- `data/agent_data/book-{book_type}/stock_decisions.json`
- `data/agent_data/book-{book_type}/decision_summary.json`
- `data/agent_data/book-{book_type}/portfolio_daily_summary.json`

## 目录结构

```text
scripts/                    CLI 入口，只做参数解析和编排调用
services/data_refresh/       一键刷新编排
services/pipeline/           01-04 skill 输入产物生成
services/research/           宏观、新闻、财报、个股研究
services/selection_system/   新闻、板块热度、因子库、量化初筛、候选池
services/trading/            交易执行与 06-08 后处理
shared_data_access/          行情、财报、股本、公告的统一缓存入口
core/                        日志、运行态、通用基础设施
configs/                     股票池、prompt flow、选股评分配置
data/                        运行产物与缓存
logs/                        组件日志
.codex/rules/                项目规则
.codex/skills/               日常运行 skills
.codex/commands/             高频检查命令
docs/                        系统设计文档
agent_tools/, tools/         少量历史兼容层，新代码不再优先落这里
```

## 关键约定

- `--date` 统一表示“要分析的交易日”，不是执行脚本当天，也不是未来日期。
- 日常默认先跑 `python scripts/refresh_all_for_date.py --date YYYY-MM-DD`，不要直接从旧的 `manage_daily_data.py` 开始。
- 外部行情、财报、股本、公告访问必须经 `shared_data_access`。
- `data/skill_runs/YYYY-MM-DD/` 下的 `01-08` 文件是 skill 流水线契约，改字段、文件名或目录结构前要先同步规则和文档。
- 运行产物写入 `data/`，日志写入 `logs/`，不要把临时产物塞进源码目录。

## 文档索引

- `docs/PROJECT_SYSTEM_SUMMARY.md`：系统总览和当前主流程。
- `docs/share_data_access/README.md`：统一数据访问层。
- `docs/cache/cache_registry_design.md`：缓存注册表。
- `docs/selection_system/因子库与量化初筛系统设计.md`：量化初筛主轴。
- `docs/trade_summary/README.md`：交易后处理与历史决策合并。
- `.codex/rules/skill-pipeline.md`：`01-08` 文件契约。

## 安装与配置

```bash
pip install -r requirements.txt
cp .env.example .env
```

主要配置：

- `configs/stock_pool.py`：固定跟踪池 `TRACKED_A_STOCKS`。
- `configs/prompt_flow/skill_flow.json`：默认 prompt flow。
- `configs/selection_system/factor_scoring.yaml`：量化初筛评分配置。
- `.env`：模型 API Key、Base URL 等运行参数。

## License

[MIT License](LICENSE)
