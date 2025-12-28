# 数据共享模块使用说明

## 模块使命
`shared_data_access` 统一封装了行情、财报、股本等数据的下载、缓存与读取逻辑。借助该模块：
- 上层智能体与工具只需要面对一份 `PreparedData`，无需关心 akshare 或缓存目录结构；
- 所有外部数据访问都走同一套限流、异常兜底与日志记录策略，避免重复实现；
- 缓存、增量刷新、数据校验等通用逻辑集中维护，减少脏数据导致的隐患。

## 核心入口：SharedDataAccess.prepare_dataset
方法位于 shared_data_access/data_access.py，是所有数据消费方的唯一入口。

```
prepare_dataset(
    *,
    symbolInfo: SymbolInfo,
    as_of_date: str,
    force_refresh: bool = False,
    force_refresh_financials: bool = False,
) -> PreparedData
```

### 主要职责
1. 调用 ensure_symbol_data 触发 akshare 抓取或刷新本地缓存，支持价格/财报/股本按需刷新。
2. 自动识别指数或 ETF，只返回价格数据，防止对无财务数据的标的做无效请求。
3. 组装 PreparedData：
   - financials (FinancialDataBundle): 对应 profit_sheet.csv、balance_sheet.csv、cash_flow_sheet.csv、analysis_indicator.csv、financial_abstract.csv。
   - prices (PriceDataBundle): price.csv 内最近 price_lookback_days（默认 1800 天）的行情，并包含起止日期、源文件。
   - share_info (ShareInfo): 通过 ShareInfoProvider 读取或缓存总股本与流通股本，自动处理 TTL 与数据源优先级。
   - disclosures (DisclosureBundle，可选): 当调用 `prepare_dataset(..., include_disclosures=True)` 时返回，载入近 lookback 天内的公告列表 DataFrame，供公告摘要、监控等模块直接复用。
4. 返回值中包含 symbolInfo 与 as_of（datetime），方便报告注明基准日期。

## 目录与缓存
- 财报缓存：data/stock_info/{股票名_代码}/financials_cache/
- 行情缓存：data/stock_info/{股票名_代码}/price.csv
- 股本缓存：data/stock_info/{股票名_代码}/share_info/stock_share_change_cninfo.csv  
  - A 股通过 `ak.stock_share_change_cninfo` 更新，港股则走 `ak.stock_hk_financial_indicator_em`；抓取后会被写入上述 CSV。  
  - 该 CSV 已纳入 `shared_data_access/cache_registry.py` 的 `CacheKind.SHARE_INFO` 策略中（参见 line 52-112 以及 `update_share_info_cached`），并由 `SharedDataAccess._load_share_info`/`ShareInfoProvider` 负责读取不同日期的股本数据。  
- shared_data_access/cache_registry.py 负责文件完整性、TTL 校验，缺失时抛出 CacheIntegrityError 提醒补全。
- 公告缓存：data/stock_info/{股票名_代码}/disclosures/cninfo_list.csv  
  - `update_disclosures_cached` 统一负责从 `ak.stock_zh_a_disclosure_report_cninfo` 拉取数据并写入 CSV，`CacheKind.DISCLOSURES` 负责 TTL 管理。

## 回测友好的缓存策略
- 所有 `update_*` 函数（价格、财报、股本、公告）始终面向“真实世界的当前时间”抓取足量数据。比如价格默认抓取 1800 个交易日、公告默认抓取近 2 年（≈730 天），这些缓存一旦写入即可供未来任意回测日使用，无需按回测日期重新拉取。  
- `prepare_dataset` 在读取缓存后，才会根据 `as_of_date` 做时间截断，确保回测环境只能看到该日期之前的数据，实现“先全量入库、后按需切片”的设计。  
- 若需要更长窗口，可以通过 `SharedDataAccess` 初始化参数或缓存策略配置（如 LOOKBACK_PRICE_DAYS、disclosure_lookback_days）集中放大抓取范围，而不是在 update 阶段依赖 as_of。  
- 这种分层策略既保证了回测的严格因果性，也避免了每次回测都重新向 akshare 请求历史数据。

## 项目规范
1. 禁止在 Analyzer、Tool、Agent 层直接调用 akshare。凡涉及外部行情、财报、股本、指标的请求，一律走 SharedDataAccess。
2. 若 prepare_dataset 尚无法提供某字段，应：
   1. 在 shared_data_access 内补充数据装载逻辑（如扩展 _load_financial_bundle 或新增缓存类型）。
   2. 在 shared_data_access/models.py 中更新数据类，让新字段成为 PreparedData 的一部分。
   3. 必要时更新 shared_data_access/cache_registry.py 或 paths.py，保证缓存落地有据。
3. 新脚本处理多只股票时，仅需实例化一次 SharedDataAccess，循环调用 prepare_dataset，避免重复初始化。
4. 始终使用 SymbolInfo 作为股票标识，可通过 parse_symbol 从命令行或配置解析用户输入。

## 典型使用流程
1. symbol = parse_symbol("600406.SH")
2. accessor = SharedDataAccess(base_dir=BASE_DIR, logger=LOGGER)
3. dataset = accessor.prepare_dataset(symbolInfo=symbol, as_of_date="2025-11-30")
4. 使用 dataset.prices.frame 计算技术指标，使用 dataset.financials.profit_sheet 计算估值，使用 dataset.share_info 推导市值或股本，必要时使用 dataset.disclosures.frame 获取公告元数据。

## 扩展指南
- 需要新的 akshare 接口：先在 shared_data_access 下完成缓存和读取封装，再向上暴露整洁的 PreparedData 字段，切勿在上层直接发起网络请求。
- 需要新增衍生指标：优先放在 indicator_library 或 shared_financial_utils 中实现，输入数据仍来自 PreparedData。
- 调试刷新：通过 prepare_dataset(..., force_refresh=True) 或 force_refresh_financials=True 触发重新抓取，确保缓存一致。

遵循上述规则，智能体与工具就能共享同一份、可追溯的数据底座。一旦发现某类 akshare 数据尚未被封装，请优先在 shared_data_access 中实现，然后通知其它模块复用。
