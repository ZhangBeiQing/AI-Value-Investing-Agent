# 数据刷新链路设计

更新日期：2026-06-12

## 1. 设计目标

把「今天哪些数据要刷、谁来决定刷新策略、哪些数据由谁负责」这件事固定下来，让上层模块只关心数据本身，不再各自散发 `force_refresh_*=True` 之类的私下行为。

策略归属：

- **`shared_data_access` 是机制层**：负责「抓 / 缓存 / 切片 / 时间截断」
- **`services/data_refresh/refresh_orchestrator.py` 是策略层**：决定「今天哪些股票该被强刷、强刷哪些档位」
- 所有上层模块必须服从策略层，不允许私下绕过

## 2. 主入口

```bash
# 默认：刷新「要分析的交易日」所需的固定股票池 + master_universe 链路数据
python scripts/refresh_all_for_date.py --date 2026-06-11

# 重量档：额外强刷财报结构化数据等重缓存
python scripts/refresh_all_for_date.py --date 2026-06-11 --fresh-heavy

# 跳过量化初筛（不生成 factor_store / 12_quant_prefilter*）
python scripts/refresh_all_for_date.py --date 2026-06-11 --no-generate-prefilter

# 跳过新闻采集和板块热度
python scripts/refresh_all_for_date.py --date 2026-06-11 --skip-news-boards

# 启用旧口径完整选股管线（生成 07/08/09/10/11，仅在显式启用 AI 选股 skill 时使用）
python scripts/refresh_all_for_date.py --date 2026-06-11 --include-selection-universe

# 周末/节假日吸收新增宏观和新闻；仅放宽分析日期，不表示该日可以交易
python scripts/refresh_all_for_date.py --date 2026-08-09 --allow-non-trading-date
```

`--date` 默认指「要分析的交易日」（即最近一个已收盘的交易日），默认 `today - 1`。周末或节假日只需要最近一次收盘数据时，要手动指定最近一个交易日；确需按休市自然日吸收新增宏观与新闻时，必须显式传入 `--allow-non-trading-date`。该参数不会改变价格数据的最后交易日，也不会放宽回测或真实交易执行的交易日约束。

## 3. 编排顺序

`services/data_refresh/refresh_orchestrator.py` 中 `run_refresh_pipeline` 按固定顺序执行：

| 序号 | 步骤 | 入口 | 用途 |
| --- | --- | --- | --- |
| 1 | `manage_daily_data` | `scripts/manage_daily_data.py` | 刷新宏观面板、价格、财报、`basic_stock_info` 等基础缓存（`TRACKED_A_STOCKS ∪ master_universe`） |
| 2 | `selection.run-news` | `scripts/manage_selection_system.py run-news` | 全市场新闻采集 / 去重 / 增强 → `03_news_prompt_input.json` |
| 3 | `selection.build-board-heat-state` | `scripts/manage_selection_system.py build-board-heat-state` | 板块热度分析 → `05_board_heat_state.json` / `05_board_heat_digest.json` |
| 4 | `selection.build-factor-store` | `scripts/manage_selection_system.py build-factor-store` | 因子宽表 → `data/factor_store/`、`12_factor_snapshot.*` |
| 5 | `selection.build-factor-scores` | `scripts/manage_selection_system.py build-factor-scores` | 因子评分 → `13_factor_scores.*` |
| 6 | `selection.build-quant-prefilter` | `scripts/manage_selection_system.py build-quant-prefilter` | 量化初筛 → `12_quant_prefilter*.csv/json` |
| 7 | `clear_research_artifact_cache` | 内部步骤 | 清理 `data/research_artifact_cache/{run_date}/`，让 `run_daily_pipeline` 必然基于最新数据重建 04 产物 |

当 `--include-selection-universe` 时（旧口径），步骤 4-6 会替换成：

- `selection.run-signals` → 板块变化与个股热度
- `selection.build-announcements` → 公告增量
- `selection.build-shared-context` → 共享上下文
- `selection.build-candidate-pools` → AI 选股候选池 08/09 文件

跑完后脚本会打印「后续 skill 清单」（宏观总结、新闻总结、财报准备与总结、三账本 01-04、三账本交易、三账本后处理），人工按顺序触发。

## 4. 每日刷新策略细则

### 4.1 股票范围

- 每日刷新范围 = `TRACKED_A_STOCKS ∪ master_universe`（约 100+ 只）
- 来源：`configs/stock_pool.py` 与 `data/universe/master_universe.json`
- 由 orchestrator 在启动时合并去重，传给 `manage_daily_data --symbols ...`

### 4.2 价格 / 财报 / basic_info

由 `manage_daily_data` 统一负责：

- 轻量档（默认）：`--force-refresh-price` 强刷价格
- 重量档：`--force-refresh` 同时强刷财报结构化数据等重缓存（由 `refresh_all_for_date.py --fresh-heavy` 启用）

### 4.3 公告（disclosures）

由 `services/selection_system/announcement_summary.py` 的 `build-announcements` 子命令按 universe 增量负责。

因此 orchestrator 调用 `manage_daily_data` 时传入 `--skip-disclosures`，避免两层重复扫描。

### 4.4 新闻 / 板块热度

由 `selection.run-news` 和 `selection.build-board-heat-state` 负责，为渐进式热点新闻总结提供上游输入。

`--skip-news-boards` 可跳过这两个步骤（用于只想刷数据、不跑新闻链路的场景）。

### 4.5 量化初筛

由 `selection.build-factor-store` → `build-factor-scores` → `build-quant-prefilter` 三步负责，输出 `12_quant_prefilter_short.csv` 与 `12_quant_prefilter_long.csv` 直接作为下游 `run_daily_pipeline --all-books` 的 short_book / long_book 输入。

`--no-generate-prefilter` 可跳过。

### 4.6 研究产物缓存清理

每次刷新结束都会无条件清理 `data/research_artifact_cache/{run_date}/`，因为该缓存的 input_fingerprint 不感知 prices 缓存的更新，必须清掉让 `run_daily_pipeline` 重建 04 产物。

## 5. 失败处理

- 单步失败默认会中止后续步骤（`--continue-on-failure` 可强制继续）
- 失败会记录到 logger（`RefreshOrchestrator`），并通过 `summarize_result` 在末尾打印
- 研究产物缓存清理永远会执行，即使前面步骤失败
- 整体退出码非 0 时，应查看上方汇总，处理失败步骤后重跑

## 6. 日志与状态

| 路径 | 内容 |
| --- | --- |
| `logs/main_scripts/RefreshAllForDate/` | `refresh_all_for_date.py` 入口日志 |
| `logs/main_scripts/ManageDailyData/` | `manage_daily_data` 日志 + `latest_status.json` |
| `logs/services/data_refresh/` | `refresh_orchestrator` 编排日志 |
| `logs/selection_system/` | 各子命令日志 |

`latest_status.json` 记录 `manage_daily_data` 单次运行的 `steps` 与 `status`，便于事后排查。

## 7. 实现要点

- `refresh_all_for_date.py` 只解析参数 + 调用 `services.data_refresh.refresh_orchestrator.run_refresh_pipeline()`；不要把策略逻辑放进脚本
- `manage_daily_data.py` 同样只承担参数解析 + `concurrent.futures.ThreadPoolExecutor` 并发刷新；真实抓取逻辑在 `shared_data_access.SharedDataAccess.prepare_dataset()` 中
- 新增数据集时，必须在 `services/data_refresh/refresh_orchestrator.py` 中明确回答「谁负责它的每日 fresh」，不要让消费端私下发起强刷

详见 `.codex/rules/shared-data-access.md` 与 `.codex/rules/skill-pipeline.md`。
