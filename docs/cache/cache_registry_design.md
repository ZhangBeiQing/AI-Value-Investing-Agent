# 缓存注册表设计

更新日期：2026-07-14

本文档说明 `shared_data_access/cache_registry.py` 中的缓存登记与刷新机制，便于扩展数据源或排查缓存异常时快速定位。

## 1. 总览

所有外部数据缓存都通过 `CacheKind`（枚举） + `CacheSpec`（元信息） + `BASE_REGISTRY`（字典）三件套登记。

每条缓存的关键字段：

- `subdir`：目录路径。`per_stock=True` 时相对于 `data/stock_info/<股票名_代码>/`，`per_stock=False` 时相对于 `data/`
- `ttl_days`：数据允许的存活天数；超过即视为过期
- `required_files`：必须存在的文件；缺失任意一个都会触发刷新
- `optional_files`：可选文件；缺失不触发刷新
- `per_stock`：是否按股票分别建目录

`_apply_policy()` 读取 `configs/cache_policy.json`，允许对 TTL / subdir 做运行时覆盖。

## 2. 当前已注册的缓存类型

| `CacheKind` | 路径 | TTL | per_stock | 必需文件 | 说明 |
| --- | --- | --- | --- | --- | --- |
| `FINANCIALS` | `financials_cache/` | 7 天 | ✓ | `profit_sheet.csv` / `balance_sheet.csv` / `cash_flow_sheet.csv` | 利润 / 资产负债 / 现金流 / `financial_abstract.csv`（可选）|
| `PRICE_SERIES` | `prices/` | 1 天 | ✓ | `price.csv` | 行情日线 |
| `SHARE_INFO` | `share_info/` | 7 天 | ✓ | （由接口生成的 CSV） | A 股 `stock_share_change_cninfo.csv` / 港股 `stock_hk_financial_indicator_em.csv` |
| `DISCLOSURES` | `disclosures/` | 1 天 | ✓ | `index.json` / `cninfo_list.csv` | 公告列表 + PDF + Markdown |
| `ANALYSIS` | `analysis/` | 1 天 | ✓ | — | 价格动态 / 技术指标等派生输出 |
| `PE_ANALYSIS` | `pe_pb_analysis/` | 1 天 | ✓ | — | 增强版 PE/PB 分析（JSON/Markdown/CSV） |
| `BASIC_INFO` | `basic_info_cache/` | 0 天（不缓存） | ✗ | — | 每日 `basic_stock_info` 快照（AlphaVantage 风格 JSON） |
| `CHIP_DISTRIBUTION` | `chip_distribution/` | 1 天 | ✓ | `chip_distribution.csv` | 东方财富日 K 筹码分布 / 本地回退计算 |
| `CN_PROFIT_FORECAST` | `profit_forecast/` | 1 天 | ✓ | `profit_forecast.csv` | A 股机构一致预期（同花顺）|
| `HK_PROFIT_FORECAST` | `profit_forecast/` | 1 天 | ✓ | `profit_forecast.csv` | 港股盈利预测（经济通）|
| `BOARD_HISTORY_THS` | `global_cache/board_history_ths/` | 1 天 | ✗ | `universe.csv` | 同花顺行业板块历史指数 + universe |
| `BOARD_METRICS_THS` | `global_cache/board_metrics_ths/` | 1 天 | ✗ | `latest.json` | 同花顺行业板块日度量化指标快照 |
| `MACRO_OBJECTIVE_PANEL` | `global_cache/macro_objective_panel/` | 1 天 | ✗ | `latest.json` | 宏观客观数据面板日度快照 |
| `INDUSTRY_FINANCIAL_PANEL` | `global_cache/industry_financial_panel/` | 30 天 | ✗ | `latest.json` | 全A业绩横截面与行业财务扩散月度快照 |
| `INDUSTRY_CATALOG_SW` | `global_cache/industry_catalog_sw/` | 180 天 | ✗ | `latest.json` | 申万一二三级行业目录与估值快照 |

## 3. 元信息与刷新记录

每个缓存目录都可包含 `.cache_registry_meta.json`，字段 `last_updated` 表示最近一次写入时间。

辅助函数：

- `_load_meta(cache_dir)`：读取时间戳
- `record_cache_refresh(cache_dir)`：刷新完成后写入当前 UTC 时间戳

写完 CSV 必须调用 `record_cache_refresh`，否则系统会误判为过期。

## 4. 缓存状态检查

```python
result = check_cache(cache_dir, kind)
# CacheCheckResult:
#   missing_files: List[str]
#   last_updated: datetime | None
#   stale: bool

should_refresh(cache_dir, kind, force=False)
# True 条件：
#   - 目录不存在
#   - required_files 缺失
#   - stale=True（超过 TTL）
#   - force=True
```

更新函数统一先调用 `should_refresh`，避免重复请求外部 API。

## 5. 更新函数分工

| 更新函数 | 负责的 `CacheKind` | 备注 |
| --- | --- | --- |
| `update_financial_data_cached` | `FINANCIALS` | A 股走 `stock_profit_sheet_by_report_em` 等；港股走 `stock_financial_hk_report_em` + `stock_financial_hk_analysis_indicator_em` |
| `update_price_data_cached` | `PRICE_SERIES` | 根据 `SymbolInfo` 分发到 `fetch_cn_a_daily_with_fallback` / `fetch_cn_index_daily` / `fetch_cn_etf_daily` / `fetch_hk_a_daily_with_fallback`，统一保留「换手率」4 位小数 |
| `update_share_info_cached` | `SHARE_INFO` | A 股 `ak.stock_share_change_cninfo`；港股 `ak.stock_hk_financial_indicator_em`；ETF/指数跳过 |
| `update_disclosures_cached` | `DISCLOSURES` | `ak.stock_zh_a_disclosure_report_cninfo` 拉取后写入 `cninfo_list.csv` |
| `update_chip_distribution_cached` | `CHIP_DISTRIBUTION` | `ak.stock_cyq_em`；失败时调用 `build_chip_distribution_from_price_csv` 本地回退计算 |
| `update_profit_forecast_cached` | `CN_PROFIT_FORECAST` / `HK_PROFIT_FORECAST` | A 股 `stock_profit_forecast_ths`；港股 `stock_hk_profit_forecast_et` |
| `update_board_metrics_cached` | `BOARD_HISTORY_THS` / `BOARD_METRICS_THS` | 由 `shared_data_access/board_metrics.py` 负责 |
| `update_macro_objective_panel_cached` | `MACRO_OBJECTIVE_PANEL` | 由 `shared_data_access/macro_objective_panel.py` 负责 |
| `update_industry_financial_panel_cached` | `INDUSTRY_FINANCIAL_PANEL` | 由 `shared_data_access/industry_financial_panel.py` 负责 |
| `update_industry_catalog_cached` | `INDUSTRY_CATALOG_SW` | 由 `shared_data_access/industry_catalog.py` 负责 |

## 6. `ensure_symbol_data`

`SharedDataAccess.prepare_dataset()` 在加载数据前会调用该函数：

1. 非指数/ETF：依次调用 `update_financial_data_cached` → `update_share_info_cached`
2. 所有标的：调用 `update_price_data_cached`，`lookback_price_days` 由调用方指定
3. 当 `include_disclosures=True` 时：调用 `update_disclosures_cached`
4. 支持 `force_refresh`（全量）/ `force_refresh_price` / `force_refresh_financials` 的细粒度控制

## 7. 扩展指南

- 新增缓存类型时：先在 `CacheKind` 与 `BASE_REGISTRY` 登记，再实现对应 `update_*_cached` 函数，**记得在写完之后调用 `record_cache_refresh`**
- 脚本若只想检测状态，调用 `check_cache` / `should_refresh` 即可
- 批量维护任务应复用这些 API，不要绕过去自己写硬编码路径
- 一切「今天哪些缓存该被强刷」的策略由 `services/data_refresh/refresh_orchestrator.py` 统一决策，上层模块不得私设 `force_refresh_*=True`

## 8. `.cache_registry_meta.json` 的清理约定

写 `analysis/` / `pe_pb_analysis/` 等派生目录前要先清旧文件，但必须**保留** `.cache_registry_meta.json`，否则下一次 `should_refresh` 会立刻判定过期。

详见 `.codex/rules/shared-data-access.md`。
