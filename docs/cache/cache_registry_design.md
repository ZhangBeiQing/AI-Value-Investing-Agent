# 缓存注册表与刷新策略

本文档总结 `shared_data_access/cache_registry.py` 的缓存设计，便于在扩展数据管线或排查缓存问题时快速定位逻辑。

## 1. 缓存类型与目录结构
核心定义位于 `shared_data_access/cache_registry.py:52-112`。通过 `CacheKind` + `CacheSpec` 描述每类缓存：

- **subdir**：对应目录（相对于 `data/stock_info/<股票名_代码>/`，若 `per_stock=False` 则位于数据根目录）。
- **ttl_days**：数据允许的存活天数，超过即视为过期。
- **required_files**：必须存在的文件；缺失任意一个都会触发刷新。
- **per_stock**：是否按股票分别创建目录。

典型配置：

| CacheKind | 目录 | 必需文件 | 说明 |
|-----------|------|----------|------|
| `FINANCIALS` | `financials_cache/` | `profit_sheet.csv`, `balance_sheet.csv`, `cash_flow_sheet.csv` | 财报缓存，TTL=7 天 |
| `PRICE_SERIES` | `prices/` | `price.csv` | 行情缓存，TTL=1 天 |
| `SHARE_INFO` | `share_info/` |（由接口生成的 CSV，如 `stock_share_change_cninfo.csv`）| 股本缓存，TTL=7 天 |
| `PE_ANALYSIS` | `pe_pb_analysis/` | - | 增强 PE/PB 输出 |
| `ANALYSIS` | `analysis/` | - | 价格动态报告输出 |

`_apply_policy()` 会读取 `configs/cache_policy.json`，允许对 TTL/目录做运行时覆盖。

## 2. 元信息与刷新记录
每个缓存目录都可包含 `.cache_registry_meta.json`，字段 `last_updated` 表示最近一次写入时间。辅助函数：

- `_load_meta(cache_dir)`：读取时间戳。
- `record_cache_refresh(cache_dir)`：刷新完成后写入当前 `UTC()`。

确保在写入 CSV 成功后调用 `record_cache_refresh`，否则系统会误判为过期。

## 3. 缓存状态检查

- `check_cache(cache_dir, kind)` 返回 `CacheCheckResult`，其中包含：
  - `missing_files`: 缺失的必需文件列表；
  - `last_updated`: 最近更新时间；
  - `stale`: 是否超过 TTL。
- `should_refresh(cache_dir, kind, force=False)` 基于检查结果决定是否刷新：
  - 目录不存在 / 必需文件缺失 / `stale=True` → 需要刷新；
  - `force=True` 时无条件刷新。

更新函数统一先调用 `should_refresh`，避免重复请求 akshare。

## 4. 更新函数职责

### 4.1 `update_financial_data_cached`
- 仅普通股票执行，指数/ETF 直接返回。
- 根据市场类型选用不同 akshare 接口（A 股 `stock_profit_sheet_by_report_em` 等，港股 `stock_financial_hk_report_em` + `stock_financial_hk_analysis_indicator_em`）。
- 将数据写入 `financials_cache/*.csv`，并记录刷新时间。

### 4.2 `update_price_data_cached`
- 所有标的都会执行，内部根据 `SymbolInfo` 判断调用 `fetch_cn_a_daily_with_fallback`、`fetch_cn_index_daily`、`fetch_cn_etf_daily` 或 `fetch_hk_a_daily_with_fallback`。
- 结果写入 `prices/price.csv`，对“换手率”列统一保留 4 位小数，然后调用 `record_cache_refresh`。

### 4.3 `update_share_info_cached`
- 普通股票使用 `ak.stock_share_change_cninfo` 获取股本变动，写入 `share_info/stock_share_change_cninfo.csv`；
- 港股使用 `ak.stock_hk_financial_indicator_em`，写入 `share_info/stock_hk_financial_indicator_em.csv`；
- ETF/指数跳过。

## 5. `ensure_symbol_data`
`SharedDataAccess.prepare_dataset()` 在加载数据前会调用该函数，确保缓存可用：

1. 非指数/ETF：调用 `update_financial_data_cached` → `update_share_info_cached`；
2. 所有标的：调用 `update_price_data_cached`，`lookback_price_days` 由调用方指定；
3. 支持 `force_refresh`（全量）和 `force_refresh_financials`（仅财报/股本）的细粒度控制。

## 6. 使用建议
- 新增缓存类型时，在 `CacheKind` 和 `BASE_REGISTRY` 登记后，再实现对应更新函数并记得 `record_cache_refresh`。
- 脚本若需只检测缓存状态，可直接调用 `check_cache` / `should_refresh`。
- 批量维护任务（如 `cache_manager.py`）应复用这些 API，保持整仓一致。

通过以上机制，缓存刷新、文件布局与 akshare 调用都集中在 `shared_data_access/cache_registry.py`，避免各脚本重复造轮子，并确保数据质量可追溯。

