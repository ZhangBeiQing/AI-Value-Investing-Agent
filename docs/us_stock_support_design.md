# 美股支持设计（US Stock Support）

更新日期：2026-11-06

## 1. 背景与目标

现有主链路是 A 股 / 港股口径：`shared_data_access` 抓取并缓存价格、三大报表、股本、公告，`services/research` 基于这些缓存生成 `01-04` 研究包。美股在符号层早有 `US` 后缀占位（`SYMBOL_SUFFIX_INFO`、`to_us_symbol()`、`NYSE` 日历），但数据抓取、财务报表结构、公告来源三处都没有落地，因此 `TSM.US` 这类标的会在 `ensure_symbol_data` 处直接失败。

本设计要解决的问题只有一个：**让美股标的复用同一条 `01-04` 链路，产出与 A 股同结构的研究包**（`04_stock_research/*.md` 的四个章节保持不变），而不是另起一套平行系统。

非目标：

- 不做美股交易执行、不做美股回测（`05-08` 与仓位规则仍只服务现有账本）；
- 不在本轮引入第三方付费数据源；
- 不改 `01-08` 文件名、字段与目录契约。

## 2. 架构落点

| 层 | 职责 | 本轮改动 |
| --- | --- | --- |
| `commons/` | 符号解析、市场日历、目录布局 | 复用 `US` 后缀；新增美股交易日收盘判定 |
| `shared_data_access/` | 抓取、缓存、归一化、时间截断 | 新增 `us_market.py` / `us_financials.py` / `us_disclosures.py`，在 `cache_registry` 按市场分发 |
| `services/research/` | 解释与组装 | 估值/价格报告按市场选择基准与口径；公告章节改读美股披露缓存 |
| `services/pipeline/` | `01-04` 编排 | 复用现有步骤，新增 `us_tracked` 书目录入口 |
| `scripts/` | CLI | 新增 `run_us_research.py`（薄入口） |

原则不变：**抓取只在 `shared_data_access`，研究层只解释；抓取面向真实时间，截断只在读取阶段。**

## 3. 符号与交易日语义

- 美股符号统一为 `TICKER.US`，例如 `TSM.US`、`SPY.US`；`parse_symbol` 已支持，`code` 保留原始大小写前的规范化大写形式。
- 基准指数不引入新市场类型，直接用美股 ETF 作为基准：`SPY.US`（标普 500 ETF）。它是 ETF，走「仅价格」模式，不会触发财务抓取。
- **收盘语义**：A 股在北京时间 15:00 收盘，项目约定「晚间分析当天收盘」；美股收盘是美东 16:00，对应北京时间次日凌晨 4:00/5:00。因此北京时间晚上运行美股研究时，「最近一个已收盘交易日」通常是**前一个美东交易日**。
  - `shared_data_access.market_calendar.resolve_latest_completed_session(market, now)` 统一给出这个日期；
  - 美股研究入口默认把请求日期解析到「已完成收盘的最近交易日」，并在日志与产物头部写清实际分析日；
  - A 股链路不改，仍由调用方显式传「要分析的交易日」。

## 4. 数据源映射（实测于 akshare 1.18.39）

| 数据 | 接口 | 说明 |
| --- | --- | --- |
| 日线行情（主） | `stock_us_hist(symbol="106.TSM", period="daily", start_date, end_date, adjust="qfq")` | 东财；需 `市场前缀.TICKER`，成交量=股、成交额=美元、换手率为百分数 |
| 日线行情（备） | `stock_us_daily(symbol="TSM", adjust="qfq")` | 新浪；只返回全历史 `date/open/high/low/close/volume`，需本地切区间；无成交额/换手率 |
| 三大报表 | `stock_financial_us_report_em(stock="TSM", symbol=..., indicator=...)` | 长表 `ITEM_NAME/AMOUNT/REPORT_DATE`；`综合损益表` 用「单季报」，`资产负债表`/`现金流量表` 需「单季报 + 累计季报 + 年报」拼季度 |
| 财务指标 | `stock_financial_us_analysis_indicator_em(symbol="TSM", indicator=...)` | 宽表，含 `NOTICE_DATE`、营收/利润/毛利率/ROE 等，用于公告日映射与摘要 |
| 股本 / 市值 / PE / PB | 东财单标的行情 `push2.eastmoney.com/api/qt/stock/get?secid=<prefix.TICKER>` | `f116` 总市值（美元）、`f164` PE(TTM)、`f167` PB、`f43` 最新价；`prefix ∈ {105,106,107}` 需探测 |
| 公告披露 | SEC EDGAR `data.sec.gov/submissions/CIK##########.json` | 官方、免费、稳定；含 `form / filingDate / reportDate / accessionNumber / primaryDocument / items` |

实测结论：

- `stock_us_spot_em()`（全市场快照）能拿到 `代码=106.TSM`、总市值、市盈率，但单次约 170 秒，不适合单股链路；只作为「市场前缀 → secid」的兜底。
- `stock_us_hist` 必须用正确的市场前缀：`105.TSM` 返回空，`106.TSM` 正常；因此实现为依次探测 `105/106/107` 并缓存结果。
- `stock_us_valuation_baidu`、`stock_individual_basic_info_us_xq` 当前返回非 JSON / 接口失效，不使用。
- `stock_financial_us_report_em` 的 `symbol` 只接受 `资产负债表 / 综合损益表 / 现金流量表`（传 `利润表` 会抛参数错误）。
- 报表口径：`单季报` 的 `资产负债表` 只覆盖每年 Q1，`现金流量表` 也主要覆盖 Q1；`累计季报` 覆盖 Q2/Q3；`年报` 覆盖 Q4。三者合并后才得到完整季度序列。

## 5. 财务归一化

### 5.1 长表 → A 股宽表

下游（`calculate_rolling_ttm_profit`、`EnhancedPEPBAnalyzer`、`basic_snapshot`）都按 A 股宽表列名读取，因此美股在**缓存层**就归一化成同一套列：

```
REPORT_DATE, NOTICE_DATE, REPORT_TYPE, CURRENCY,
TOTAL_OPERATE_INCOME, OPERATE_INCOME, OPERATE_COST, GROSS_PROFIT,
RESEARCH_EXPENSE, SALE_EXPENSE, MANAGE_EXPENSE, OPERATE_PROFIT,
TOTAL_PROFIT, INCOME_TAX, NETPROFIT, PARENT_NETPROFIT, MINORITY_INTEREST,
BASIC_EPS, DILUTED_EPS, ...
TOTAL_ASSETS, TOTAL_CURRENT_ASSETS, TOTAL_LIABILITIES, TOTAL_CURRENT_LIAB,
TOTAL_PARENT_EQUITY, TOTAL_EQUITY, MONETARYFUNDS, INVENTORY, ACCOUNTS_RECE,
ACCOUNTS_PAYABLE, SHORT_LOAN, LONG_LOAN, CAPITAL_RESERVE, UNASSIGN_RPOFIT, ...
NETCASH_OPERATE, NETCASH_INVEST, NETCASH_FINANCE, CONSTRUCT_LONG_ASSET, CCE_ADD, ...
```

- `PARENT_NETPROFIT` 取「归属于母公司股东净利润」；美股没有扣非口径，`DEDUCT_PARENT_NETPROFIT = PARENT_NETPROFIT`，并在 `us_financial_meta.json` 标注 `deduct_proxy=true`，研究层对美股不使用扣非估值。
- `NOTICE_DATE` 用 `stock_financial_us_analysis_indicator_em` 的 `REPORT_DATE → NOTICE_DATE` 映射补齐；映射缺失时退回 `filingDate`（SEC）或报告期 + 法定滞后天数。

### 5.2 季度序列拼接

对每个报表分别抓 `单季报 / 累计季报 / 年报`，按报告月份取口径：

| 报告月 | 采用口径 | 含义 |
| --- | --- | --- |
| 03 | 单季报 | 一季度累计（= 单季） |
| 06 | 累计季报 | 半年度累计 |
| 09 | 累计季报 | 前三季度累计 |
| 12 | 年报 | 全年 |

这样得到的序列与 A 股「年内累计」语义一致，`calculate_rolling_ttm_profit` 与 `basic_snapshot._ttm_from_cumulative_row` 无需改动。

### 5.3 币种与 ADR

美股报价是美元，但部分 ADR（如台积电）财报以本币（TWD）编制，且 1 份 ADR 对应多份普通股。直接 `美元股价 × 本币净利润` 会同时踩两个坑。

处理方式：

1. 由东财行情快照得到 `market_cap_usd`、`pe_ttm_usd`、`price_usd`；
2. `total_shares = market_cap_usd / price_usd`（ADR 等值股数，已隐含 ADR 比例）；
3. 若 `CURRENCY != USD`，用**隐含汇率**把报表金额换算成美元：
   `fx_local_per_usd = TTM 归母净利润(本币) / (market_cap_usd / pe_ttm_usd)`；
4. 报表金额统一除以 `fx` 后落缓存，`financial_abstract` 同样处理；每股指标直接用 `美元金额 / total_shares` 计算；
5. 汇率与原始币种写入 `financials_cache/us_financial_meta.json`，研究包中注明「金额已折算为美元，1 USD = X TWD」。

若 `pe_ttm` 缺失（亏损或数据缺口）且本币非美元，则不猜汇率：估值章节显式返回「缺少可用汇率，估值不可用」，避免输出错一个数量级的 PE/PB。

## 6. 公告分析：巨潮 → SEC EDGAR
A 股 / 港股公告走巨潮（`cninfo`）。美股没有巨潮口径，替换方案：

- **主源：SEC EDGAR**
  - CIK 由 `https://www.sec.gov/files/company_tickers.json` 缓存映射（TICKER → CIK）；
  - `https://data.sec.gov/submissions/CIK##########.json` 取最近申报（含分页的 `filings.files`）；
  - 只保留有分析价值的表单：`6-K / 8-K / 10-Q / 10-K / 20-F / DEF 14A / SC 13D/G / 424B`；过滤 `3/4/5` 内部人交易噪声（保留但降级为 low）；
  - 输出与 `cninfo_list.csv` 同列的缓存：`代码 / 名称 / 公告标题 / 公告时间 / 公告类型 / 公告链接`，同时写 `disclosures/index.json` 满足缓存契约；
  - 章节 2 的新闻条目由披露缓存直接生成（标题=表单+描述，时间=`filingDate`，类别=`SEC 6-K`，影响度按表单分级），**不下载 PDF、不调用 LLM 摘要**，保证离线可复现。
- **补充源（可选，不阻塞主链）**：台积电等台湾企业可在 `MOPS`（公开资讯观测站）与公司 IR 页面拿到中文版月营收/法说会材料；本轮只登记为人工补充入口，不写进自动链路。
- 为什么不是东财/雪球：两者美股公告字段不稳定、无稳定公告日与原文链接，无法满足「公告时点截断」的因果要求；SEC 的 `filingDate/acceptanceDateTime` 是权威时点。

## 6.1 财报原文：巨潮 → SEC EDGAR

`financial-report-summary` 这条深研链路要求 `disclosures/md/*.md` 里存在财报原文。
A 股 / 港股由巨潮 PDF 转换而来，美股此前完全没有这一层，于是 `prepare_financial_report_skill.py`
会把 `TSM.US` 判成 `latest_report_pdf_missing` 并空转。

替换方案：**`shared_data_access/us_financial_documents.py` 把 SEC 申报渲染成同构 Markdown**，
目录与命名沿用巨潮口径（`{date}__{code}__{accession}__{slug}.md`），因此 skill 流程与角色分工
一个字都不用改。

来源分两档，因为外国私人发行人拿不到"一份完整财报"：

| 发行人类型 | 表单 | 拿到什么 | 完整性 |
| --- | --- | --- | --- |
| 美国本土公司 | `10-Q` / `10-K` | 主文档即完整财报；另用 `FilingSummary.xml` 的 `R*.htm` 渲染三大报表（带上年同期列） | 完整 |
| 外国私人发行人（如台积电） | `6-K` | 只有新闻稿：叙述 + 6 行利润摘要 + 制程结构 + 下季指引 | 缺资产负债表与现金流量表 |

对第二档的处理（已确认按"当天出报告"口径）：

- 6-K 财报稿作为 `01_latest_report.md` 主体；
- 缺失的资产负债表、现金流量表与费用明细由东财结构化财报补足，写入同一文件的
  「补充：结构化财务报表（第三方数据）」小节，**显式标注非公司披露原文、冲突时以原文为准**；
- 完整三表通常要等季末约 45 天后的「Financial Statements」6-K 或 20-F 年报，
  等它会让研究落后一个季度，因此不作为阻塞条件。

识别规则（美股 6-K 与月营收共用同一表单，必须区分）：

1. `10-K / 10-Q / 20-F / 40-F` 直接判为财报；
2. `6-K / 8-K` 要求报告期落在季末（3/6/9/12 月末），否则是股东大会/董事会决议这类事件日；
3. 再读申报正文第一条要点，排除「Revenue Report / Dividend / Shareholding」并保留业绩稿；
4. 季度归类用标题里的「报告期 YYYY-MM-DD」推 `(fiscal_year, quarter)`，不解析英文
   `Third Quarter`，也**不能**套用 A 股「年报次年披露要减一」的财年换算。



## 7. 产物与入口

- 新账本目录：`data/skill_runs/{us_session_date}/us_tracked/`，与 `fixed_tracked/` 平级，互不覆盖；
- 产物与 A 股一致：`01_global_context.md`、`02_basic_snapshot_payload.json`、`03_agent_input.md`、`03_stock_analysis_input.md`、`04_stock_research/{名称}_{代码}.US_{date}_research.md`；
- 入口：`python scripts/run_us_research.py --symbol TSM.US --date 2026-11-05`；不传 `--date` 时自动取最近一个已收盘的美东交易日；
- 单股数据准备仍可复用 `prepare_single_stock.py --symbol TSM.US`（价格/财报/披露缓存），但它不会生成 `01-04`。

## 8. 已知限制

1. 美股没有同花顺/经济通口径的机构一致预期，`03` 的「一致预期」章节对美股为空，不伪造。
2. `04` 的财报章节对美股使用**结构化摘要**（营收/利润/利润率/资产负债/现金流趋势）；真正的季度深研由 `financial-report-summary` 触发，财报原文来自 SEC EDGAR（见 §6.1），流程与 A 股一致。
3. 隐含汇率依赖东财 `pe_ttm`，是近似值；同一标的的历史 PE 序列使用同一汇率，只用于纵向比较，不用于跨币种精确估值。
4. SEC 只覆盖在美国注册/申报的发行人；同时在 A/H 上市的公司仍应走原巨潮链路。
5. 美股财年不一定等于自然年（如 NVIDIA 财年 1 月底结束）；本轮按 `REPORT_DATE` 的日历年拼接累计值，遇到非自然财年会退化，需在后续版本按 `fiscalYearEnd` 校正。
6. **历史回测的汇率与股本**：隐含汇率和 ADR 等值股本取「当前」行情快照，历史回测会带入未来信息；若要把 `us_tracked` 接入回测，需要按日缓存 quote 快照并在读取阶段截断。
7. SEC 文档要点只抓取每份申报的正文/附件前若干行，属于「标题级」信息，不替代对 6-K/20-F 全文的精读。

## 9. 验证方式

- 单测：`tests/test_us_stock_support.py` 覆盖符号解析、报表归一化、季度拼接、SEC 表单分级、隐含汇率；
  `tests/test_us_financial_documents.py` 覆盖财报识别、季末判定、季度归类、XBRL 报表解析与 HTML 转写；
- 联调：`python scripts/run_us_research.py --symbol TSM.US --date 2026-11-05`，检查
  - `data/stock_info/台积电_TSM.US/` 下价格/财报/披露缓存落盘；
  - `04_stock_research/台积电_TSM.US_2026-11-05_research.md` 四个章节齐全，价格与估值数字与东财快照同量级；
- 回归：`python -m pytest tests -q` 全绿。
