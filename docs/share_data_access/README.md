# SharedDataAccess 使用说明

更新日期：2026-08-09

## 1. 模块使命

`shared_data_access/` 是项目里访问 akshare、巨潮、东方财富等外部数据源的**唯一**入口。它统一了下载、缓存、读取、时间截断的全部细节，让上层只面对一份 `PreparedData`，不需要关心：

- akshare 接口差异（A 股 / 港股 / 指数 / ETF）
- 缓存目录布局、TTL、刷新元信息
- 回测因果性（什么时候允许看到什么数据）
- 限流、重试、异常兜底

## 2. 核心入口：`SharedDataAccess.prepare_dataset`

位于 `shared_data_access/data_access.py`。

```python
prepare_dataset(
    *,
    symbolInfo: SymbolInfo,
    as_of_date: str,
    force_refresh: bool = False,
    force_refresh_price: bool = False,
    skip_price_refresh: bool = False,
    force_refresh_financials: bool = False,
    skip_financial_refresh: bool = False,
    include_disclosures: bool = False,
    disclosure_lookback_days: int | None = None,
    force_refresh_disclosures: bool = False,
    disclosure_as_of_date: str | None = None,
    include_chip_distribution: bool = False,
    force_refresh_chip_distribution: bool = False,
    chip_adjust: str = "qfq",
    prefer_local_chip_distribution: bool = False,
    local_full_history_chip_distribution: bool = False,
) -> PreparedData
```

价格抓取窗口 `price_lookback_days`（默认 1800 个交易日）是 `SharedDataAccess` 的初始化参数，不在 `prepare_dataset` 上逐次传入。

主要职责：

1. 调用 `ensure_symbol_data` 触发抓取或刷新本地缓存，支持价格 / 财报 / 股本按需独立刷新
2. 自动识别指数或 ETF，只返回价格数据，防止对无财务数据的标的发起无效请求；港股 ETF/杠杆产品没有稳定代码前缀，识别时必须传递含名称的完整 `SymbolInfo`
3. 组装 `PreparedData`：
   - `financials` (`FinancialDataBundle`)：`profit_sheet` / `balance_sheet` / `cash_flow_sheet` / `financial_abstract`（`financial_abstract` 可选）
   - `prices` (`PriceDataBundle`)：`price.csv` 内最近 `price_lookback_days`（默认 1800 天）的行情，包含起止日期、源文件
   - `share_info` (`ShareInfo`)：通过 `ShareInfoProvider` 读取或缓存总股本与流通股本，自动处理 TTL 与数据源优先级
   - `disclosures` (`DisclosureBundle`，可选)：当 `include_disclosures=True` 时返回，载入近 `disclosure_lookback_days` 天内的公告列表 DataFrame
4. 返回值包含 `symbolInfo` 与 `as_of`（`datetime`），方便报告标注基准日期

## 3. 缓存目录速查

| 缓存类型 | 路径 | 主入口 |
| --- | --- | --- |
| 财报 | `data/stock_info/<股票名_代码>/financials_cache/` | `update_financial_data_cached` |
| 行情 | `data/stock_info/<股票名_代码>/prices/price.csv` | `update_price_data_cached` |
| 股本 | `data/stock_info/<股票名_代码>/share_info/` | `update_share_info_cached` |
| 公告 | `data/stock_info/<股票名_代码>/disclosures/` | `update_disclosures_cached` |
| 筹码分布 | `data/stock_info/<股票名_代码>/chip_distribution/chip_distribution.csv` | `update_chip_distribution_cached`（含本地回退）|
| 一致预期 | `data/stock_info/<股票名_代码>/profit_forecast/profit_forecast.csv` | `update_profit_forecast_cached` |
| 板块 | `data/global_cache/board_history_ths/`、`board_metrics_ths/` | `shared_data_access/board_metrics.py` |
| 宏观 | `data/global_cache/macro_objective_panel/` | `shared_data_access/macro_objective_panel.py` |
| 全A行业财务面板 | `data/global_cache/industry_financial_panel/` | `shared_data_access/industry_financial_panel.py` |
| 申万行业目录 | `data/global_cache/industry_catalog_sw/` | `shared_data_access/industry_catalog.py` |

完整 `CacheKind` 与 TTL 见 `docs/cache/cache_registry_design.md`。

港股正式财报仍由公告同步入口统一落入 `disclosures/pdfs/` 和
`disclosures/md/`。当巨潮提供的港股附件地址不可用时，财报同步会按股票、公告日和标题到
港交所披露易匹配官方中文 PDF；该回退仅用于财报附件，不改变普通公告的抓取逻辑。

## 4. 回测友好的缓存策略

- 所有 `update_*` 函数始终面向「真实世界的当前时间」抓取足量数据
  - 价格默认抓取 1800 个交易日
  - 公告默认抓取近 2 年（约 730 天）
- 价格缓存的每日刷新是**增量**的：若已有缓存覆盖到 lookback 窗口内，`update_price_data_cached`
  只请求「缓存最后一根交易日 − 10 天重叠」到当前的窗口，再按日期去重合并回旧缓存，避免每天重复
  拉取完整 1800 天历史
  - 10 天重叠窗口用于识别前复权（qfq）基准变化：若重叠区间收盘价与旧缓存不一致，说明期间发生
    除权除息，自动回退为全量重抓，防止两段不同复权基准直接拼接
  - `requested_start_date` 元信息保留最早请求起点，保证上市日/覆盖判定不受增量刷新影响
- 所有行情来源（新浪 / 东财 / ETF / 指数）在 `commons/stock_utils.py:_finalize_price_frame` 统一出口归一，
  固定输出 9 列 `日期, 开盘, 最高, 最低, 收盘, 成交量, 成交额, 换手率, 流通股本`，单位统一为
  成交量=股、成交额=元、换手率=小数比例、流通股本=股。东财接口的「手 / 百分数」在出口处换算，
  避免同一 `price.csv` 因回退切换出现列结构或单位（100 倍）不一致。
- 所有 akshare 调用经 `commons/stock_utils.py:api_call_with_delay` 统一节流：进程级锁 + 单调时钟在
  「调用前」预留时间片，保证跨线程的调用起点至少相隔默认 0.25s（聚合约 4 req/s），避免线程池各 worker
  各自 sleep 导致齐发（thundering herd）而触发限流；可用环境变量 `AKSHARE_MIN_INTERVAL_SECONDS`
  调整，应急降速时可调大（如 0.5~1.0）。
- 一旦写入缓存就可供未来任意回测日使用，无需按回测日期重新拉取
- `prepare_dataset` 在读取缓存后才会根据 `as_of_date` 做时间截断，保证回测环境只能看到该日期之前的数据
- 若需要更长窗口，通过 `SharedDataAccess` 初始化参数（`price_lookback_days`）或缓存策略配置统一放大抓取范围，**不要**在 update 阶段依赖 `as_of`

这种「先全量入库、后按需切片」的设计同时保证了：

- 回测的严格因果性
- 不会每次回测都重新向 akshare 请求历史数据

## 5. 每日刷新策略归属（重要）

`shared_data_access` 是**机制层**——只负责「抓 / 缓存 / 切片」，**不**决定「今天哪些股票该被强刷」。

**策略由 `services/data_refresh/refresh_orchestrator.py` 唯一决定**：

- **每日刷新股票范围** = `TRACKED_A_STOCKS ∪ master_universe`（约 100+ 只）
  - 由 orchestrator 启动时读取 `configs/stock_pool.py` 与 `data/universe/master_universe.json`，合并去重
- **价格 / 财报结构化 / basic_info** 由 `manage_daily_data`（接收 orchestrator 传入的扩展 symbol 列表）统一负责
  - 轻量档：`--force-refresh-price`（默认开）
  - 重量档：`--force-refresh`（`--fresh-heavy` 开启，强刷财报结构化）
- **公告（disclosures）** 由 `services/selection_system/announcement_summary.py` 的 `build-announcements` 子命令按 universe 增量负责
  - 因此 orchestrator 调用 `manage_daily_data` 时传入 `--skip-disclosures`，避免两层重复扫描
- **选股系统侧的 `_ensure_universe_snapshot_coverage` 是纯兜底路径**（只在缓存缺失时补抓）
  - 正常情况应全量命中缓存
  - 若触发兜底分支并打印 warning，说明上游 refresh 有遗漏，需要排查而不是默认接受

规则沉淀：

1. 任何新增数据集（新的选股因子、新的快照字段）都要回答「谁负责它的每日 fresh」——答案应当是 `services/data_refresh/`，而不是消费端
2. 上层模块不得私下做 `force_refresh_*=True` 的调用；如果消费路径发现缓存过期，应向 orchestrator 反馈（warning 或异常），由策略层统一修正

## 6. 项目规范

1. **禁止**在 Analyzer / Tool / Agent 层直接调用 akshare。凡涉及外部行情、财报、股本、指标的请求，一律走 `SharedDataAccess`
2. 若 `prepare_dataset` 尚无法提供某字段，应：
   1. 在 `shared_data_access` 内补充数据装载逻辑（如扩展 `_load_financial_bundle` 或新增缓存类型）
   2. 在 `shared_data_access/models.py` 中更新数据类，让新字段成为 `PreparedData` 的一部分
   3. 必要时更新 `shared_data_access/cache_registry.py` 或 `paths.py`，保证缓存落地有据
3. 新脚本处理多只股票时，**只**实例化一次 `SharedDataAccess`，循环调用 `prepare_dataset`，避免重复初始化
4. 始终使用 `SymbolInfo` 作为股票标识，可通过 `parse_symbol` 从命令行或配置解析用户输入

## 7. 典型使用流程

```python
from shared_data_access.data_access import SharedDataAccess
from commons import parse_symbol

symbol = parse_symbol("600406.SH")
accessor = SharedDataAccess(base_dir=None, logger=LOGGER)
dataset = accessor.prepare_dataset(symbolInfo=symbol, as_of_date="2026-06-11")

# 使用
prices_df = dataset.prices.frame                    # 行情
profit_df = dataset.financials.profit_sheet          # 利润表
total_shares = dataset.share_info.total_shares       # 股本
if dataset.disclosures:
    annc_df = dataset.disclosures.frame              # 公告（只在 include_disclosures=True 时存在）
```

## 8. 扩展指南

- **新 akshare 接口**：先在 `shared_data_access` 下完成缓存和读取封装，再向上暴露整洁的 `PreparedData` 字段
- **新衍生指标**：优先放在 `indicator_library` 或 `shared_financial_utils` 中实现，输入数据仍来自 `PreparedData`
- **调试刷新**：通过 `prepare_dataset(..., force_refresh=True)` 或 `force_refresh_financials=True` 触发重新抓取——仅用于调试，**不要**写入业务代码

详见 `.codex/rules/shared-data-access.md`。
