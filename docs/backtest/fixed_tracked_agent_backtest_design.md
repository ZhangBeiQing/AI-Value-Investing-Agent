# fixed_tracked 多 Agent 历史回测系统详细设计

> 状态：设计待评审，尚未实现  
> 目标示例：回测 2026-01-01 至 2026-08-03 的 fixed_tracked 策略，初始现金 500,000 元  
> 设计口径：当前固定池基线 + 每日长期候选、允许受约束联网、零费率、完全隔离账本、D 日收盘决策后于 D+1 开盘成交

## 1. 目标与边界

### 1.1 目标

新增一个独立的 `backtest-fixed-tracked` skill。用户可以用自然语言触发：

```text
回测 2026-01-01 到 2026-08-03 的固定股池，初始资金 50 万
```

Skill 负责：

1. 建立回测实验并冻结实验参数；
2. 识别区间内交易日和历史输入覆盖情况；
3. 从当前固定股池和每日长期候选构造当日 fixed_tracked 分析集合；
4. 复用安全的历史产物，补建可按日期重建的输入；
5. 在缺失上下文时允许 Agent 受约束联网研究；
6. 逐日运行 fixed_tracked 的 P0、Bull、Bear、Rebuttal、Juror、Finalizer；
7. 自动合并 `05_decision.json`，不等待人工确认；
8. 自动调用回测模式的 `run_post_trade.py`，在 D+1 开盘模拟成交并生成 `06-08`；
9. 维护独立的 `position.jsonl` 和投资逻辑记忆；
10. 计算最终资产、收益率、最大回撤和交易明细；
11. 支持断点续跑、阶段缓存和失败恢复。

### 1.2 不属于第一版的内容

- 不接入真实券商；
- 不调用或修改真实 fixed_tracked 账本；
- 不回测 short_book；
- 不单独运行 long_book 交易 Skill；
- 不把未来才进入当前股票池这一事实伪装成历史可知；
- 不保证受约束联网能够从理论上彻底消灭未来信息；
- 不在第一版引入手续费、印花税和滑点；
- 不修改现有日常交易的 `01-08` 文件契约。

## 2. 已确认的实验口径

### 2.1 初始状态

```yaml
initial_cash: 500000
initial_positions: {}
commission_rate: 0
stamp_duty_rate: 0
slippage_bps: 0
```

回测从全现金开始，不读取用户备份或删除前的真实历史仓位。

### 2.2 股票集合

基础池使用实验创建时 `configs.stock_pool.TRACKED_A_STOCKS` 中的当前 17 只股票，并把这份列表复制进实验配置，此后即使源码里的固定池变化，本次实验也不随之变化。

当日分析集合：

```text
当日 fixed_tracked
= 冻结的 17 只基础股票
∪ 当日长期候选
∪ 截至当日仍持仓的股票
```

保留持仓股票的原因是：股票即使已退出长期候选，也必须继续被分析，不能因为不再入选而失去卖出机会。

### 2.3 长期量化候选只负责扩展研究范围

回测直接使用当日 `12_quant_prefilter_long.csv` 扩展 fixed_tracked 的研究范围：

1. 当日存在 `12_quant_prefilter_long.csv` 时，读取其中的长期量化候选；
2. 不存在时，先尝试基于历史本地因子库生成；
3. 仍无法生成时，记录当天 `long_candidates_missing=true`，仅使用基础池和持仓股继续运行。

`12_quant_prefilter_long.csv` 只是上游选股服务产物，不具有任何交易决策权：

```text
12_quant_prefilter_long.csv
    ↓ 只扩展当日待研究股票集合
fixed_tracked 主 Agent 做 P0
    ↓
Bull / Bear / Rebuttal / Juror / Finalizer
    ↓
05_decision.json 才是当日最终交易决策
```

回测不复用历史 `09_long_book_candidates.json` 来替代当前 Agent 的判断。实验目录中的 daily pipeline 应显式把当日 `12_quant_prefilter_long.csv` 候选合入 fixed_tracked，然后由当前版本的 Agent 从完整研究集合中作最终选择。

如果正式 `data/selection_runs/{date}/12_quant_prefilter_long.csv` 缺失，但
`data/factor_store/by_date/{date}.csv` 或当日历史因子快照存在，回测应从共享因子
快照只读重建 12，并只写入实验目录。不得为了补 12 回写或刷新正式 factor store。

历史量化初筛应优先使用已有入口一次性批量生成：

```bash
python scripts/manage_selection_system.py \
  --base-dir data \
  build-factor-history \
  --start-date 2026-01-01 \
  --end-date 2026-08-03 \
  --source local_factor_store \
  --build-prefilter
```

最终实现前需要用小区间验证该命令只使用不晚于目标日期的数据。不得直接沿用 `scripts/batch_rebuild_quant_prefilter.py` 的工作日枚举，因为它当前只排除周末，不能准确排除法定休市日。

### 2.4 幸存者偏差声明

当前 17 只固定池被用于整个历史区间，会产生：

- 幸存者偏差；
- 股票池选择的未来信息；
- 当时尚未上市股票无历史行情的问题。

实验结果衡量的是：

> 如果今天选定的这些固定跟踪股票，加上每个历史日期按当时量化因子得到的长期候选，从回测起点开始交给当前交易决策系统管理，会得到怎样的结果。

它不衡量“当时真实股票池选择能力”。最终报告必须保留：

```json
{
  "universe_mode": "frozen_current_tracked_plus_daily_long_candidates",
  "survivorship_bias": true
}
```

## 3. 日期与成交语义

### 3.0 交易日序列不是 Agent 判断

回测主时钟使用 A 股交易日序列。程序调用
`shared_data_access.market_calendar.market_sessions_between()`：

1. 优先读取 `000001.IDX` 的实际行情日期；只有缓存完整覆盖回测区间时才采用；
2. 缓存缺失或覆盖不足时，使用 `pandas_market_calendars` 的 `SSE` 交易所日历；
3. 日历加载失败时停止，不允许退化为“排除周六、周日”的工作日近似；
4. 用户输入的起止日期可以是自然日，程序会自动裁剪为区间内真实交易日；
5. `prepare-day` 只接受序列中前 `N-1` 个交易日，最后一个交易日只做期末收盘估值；
6. `execution_date` 必须是序列中紧邻 `decision_date` 的下一交易日，手工传入其他日期会被拒绝。

因此元旦、春节、国庆等法定休市日以及普通周末不会启动 Agent，也不会生成当天
`01-08`。这套判断属于 Python 编排层，不依赖主 Agent 的常识或提示词遵从。

### 3.1 三种日期

必须区分：

- `decision_date`：D 日，收盘数据已经产生，Agent 在盘后分析；
- `execution_date`：D+1，该标的下一个可交易日；
- `valuation_date`：组合净值计算日。

### 3.2 成交规则

第一版统一使用：

```text
D 日收盘后产生决策
→ D+1 该标的下一可交易日开盘价成交
```

不得用 D 日收盘价成交，也不得用 D 日以前的收盘价成交。

### 3.3 现有实现不能直接复用

当前 `run_post_trade(D)` 会设置：

```text
TODAY_DATE = D
```

`services/trading/trade_executor.py` 随后调用：

```python
get_prev_close_prices(D, symbols)
```

因此当前实际取价是 D 的上一交易日收盘价，同时把仓位记录日期写成 D。这与本回测定义的 D+1 开盘成交不同。

回测不得改变真实交易默认行为。应新增独立的 `BacktestExecutionSimulator`，并让现有 `scripts/run_post_trade.py` 在显式指定回测参数时委托给它；未指定回测参数时仍走原有真实后处理。

目标调用形式：

```bash
python scripts/run_post_trade.py \
  --date {decision_date} \
  --book-type fixed_tracked \
  --signature {backtest_signature} \
  --backtest-root {experiment_root} \
  --execution-date {next_trading_date} \
  --execution-price open
```

该调用仍然生成与日常流程对应的：

```text
06_execution_log.json
07_daily_summary.json
08_history_merge.json
```

同时只向实验目录中的 `position.jsonl` 追加 D+1 成交后的仓位。

### 3.4 精确日期取价

现有 `get_open_prices()` 内部允许“目标日没有记录时回退到更早价格”。回测成交禁止这种行为。

回测价格读取必须新增严格接口：

```python
get_exact_open_price(symbol, execution_date) -> float | None
```

要求行情行的日期必须等于 `execution_date`。没有精确开盘价时不能拿上一天开盘价冒充。

### 3.5 停牌、休市与跨市场

基础池同时包含 A 股、港股和 ETF。订单的 D+1 定义为“该标的在 D 之后的下一可交易日”，而不是简单的下一个自然日。

订单状态：

```text
created -> pending -> filled
                   -> rejected
                   -> expired
```

第一版规则：

- 无精确开盘价但后续存在下一交易日：保持 `pending`，在该标的下一交易日开盘尝试；
- 交易数量不合法、现金不足或卖出超过持仓：`rejected`；
- 超过实验结束日仍未成交：`expired`；
- 回测结束日产生的决策默认不跨出回测区间成交。

### 3.6 每日净值

每日收盘按当日或最近一个可用收盘价计算持仓市值。停牌股允许沿用最近收盘价进行盯市，但必须在净值记录中标记 `stale_price_symbols`。

## 4. 联网模式与防止未来信息

### 4.1 默认模式

按用户确认，默认不是禁止联网的严格模式，而是：

```yaml
network_mode: guarded_web
```

所有参与联网研究的 Agent 都必须知道：

1. 当前任务是历史回测；
2. 当前可知信息截止到 `decision_date` 收盘；
3. 搜索词必须显式包含截止日期；
4. 不能采用发布日期晚于 `decision_date` 的资料；
5. 不能采用后来对历史事件进行回顾、修正或总结的文章；
6. 找不到合格资料时应承认缺失，不能用未来内容填空。

### 4.2 回测上下文文件

每个回测日生成：

```text
skill_runs/{date}/fixed_tracked/00_backtest_context.json
skill_runs/{date}/fixed_tracked/00_backtest_context.md
```

JSON 示例：

```json
{
  "mode": "historical_backtest",
  "decision_date": "2026-04-01",
  "knowledge_cutoff": "2026-04-01T23:59:59+08:00",
  "network_mode": "guarded_web",
  "future_information_forbidden": true,
  "search_query_date_required": true,
  "source_publication_date_must_be_on_or_before": "2026-04-01",
  "retrospective_articles_forbidden": true
}
```

该文件必须由以下角色直接读取：

- 宏观上下文研究 Agent；
- 财报研究相关 Agent；
- fixed_tracked 主 Agent；
- Bull；
- Bear；
- Rebuttal；
- 三名 Juror；
- Finalizer。

主 Agent 不得只在派单 Prompt 中临时复述规则，因为弱模型可能遗漏或改写。

### 4.3 搜索词要求

允许的搜索词示例：

```text
截至2026年4月1日 胜宏科技 PCB订单 产能利用率
2026年1月1日至2026年4月1日 比亚迪 销量 降价
2026年4月1日之前 美联储 利率预期 原始报道
```

不允许：

```text
胜宏科技后来怎么样
胜宏科技2026全年业绩
2026年8月回顾4月PCB行情
```

### 4.4 来源准入

每条通过联网得到并真正影响结论的关键事实，至少记录：

```json
{
  "claim": "需要验证的事实",
  "source_title": "来源标题",
  "source_url": "https://...",
  "published_at": "2026-03-28",
  "checked_against_cutoff": true,
  "retrospective_risk": false
}
```

如果搜索结果没有可靠发布日期：

- 不得用于改变 BUY/SELL 动作；
- 可以作为待核验线索；
- 必须标记 `publication_date_unknown`。

### 4.5 百炼搜索的角色

优先使用阿里云百炼 Search MCP 做互联网召回。百炼返回的是候选证据，不代表已通过日期和来源审查。

只有需要打开某个官方网站、公告原文或具体页面核对时，才使用页面读取能力。

### 4.6 不可消除的限制

百炼没有硬性的 `end_date` 参数。仅依靠搜索词和 Agent 自律无法从理论上保证零穿越。因此最终报告必须包含：

```json
{
  "network_mode": "guarded_web",
  "lookahead_risk": true,
  "lookahead_control": "query_cutoff_plus_source_date_review"
}
```

这不是阻止回测运行，而是避免把近似历史研究误称为完全严格的 point-in-time 回测。

## 5. 历史输入准备策略

### 5.1 不逐日运行 `refresh_all_for_date.py`

`refresh_all_for_date.py --date D` 只编排 D 日的刷新和选股产物，不会自动生成 D 以前所有日期的 `skill_runs`。

它还会：

- 强刷价格缓存；
- 运行新闻采集和板块热度；
- 默认构建量化初筛；
- 清理 D 日研究产物缓存。

因此不能从 1 月到 8 月每天机械执行。回测准备阶段应拆成：

1. 一次性补足全区间原始行情缓存；
2. 一次性构建历史因子横截面和量化初筛；
3. 按财报披露事件准备季度研究；
4. 逐日只构建该日所需的 01-04；
5. 缺少历史软信息时才由受约束联网 Agent 补充。

### 5.2 覆盖率审计

`prepare` 阶段先扫描每个交易日，并对每类输入分类：

```text
reusable
rebuildable_local
requires_guarded_web
missing
unsafe_future_content
```

检查对象至少包括：

- 价格历史；
- 估值所需历史财务和股本；
- 宏观总结；
- 宏观客观面板；
- 个股公告和新闻；
- 财报深研；
- 当日 `12_quant_prefilter_long.csv`；
- 旧 `01-04`；
- 旧 `05_decision.json`。

输出 `coverage.json`，先向用户展示预计：

- 交易日数量；
- 可直接复用天数；
- 需要本地重建天数；
- 需要联网补充天数；
- 财报深研事件数量；
- 预计 LLM 调用量。

开始大规模 LLM 回测前必须有一次显式确认，防止意外产生高额费用。

### 5.3 历史产物复用

旧 `skill_runs` 不能仅因文件存在就直接复用。

允许复用的条件：

- 文件请求日期与回测日一致；
- 行情和财务数据不晚于回测日；
- 新闻项目发布日期不晚于回测日；
- 财报总结没有使用披露日后的搜索证据；
- Prompt 版本与本实验要求兼容；
- 输入指纹一致。

建议：

- `01_global_context.md`：经过日期审查后可复用；
- `02_basic_snapshot_payload.json`：经过字段级 `as_of_date` 审查后可复用；
- `03_agent_input.md`、`03_stock_analysis_input.md`：按当前 Prompt 重新生成，成本低；
- `04_stock_research`：只有通过来源日期审查后才复用；
- `05_decision.json`：默认不作为新回测决策复用，只能用于对照实验。

### 5.3.1 历史日期早于上市日

如果共享价格缓存的首个交易日晚于回测日，应把该股票标记为
`not_listed_as_of_date`，并从当天研究集合、量化横截面和 P0 输入中排除。

这类情况是正常的历史可用性状态，不是数据刷新失败。必须同时保留：

- 股票代码；
- 请求日期；
- 缓存首个交易日。

只有“首个交易日明确晚于请求日期”才能按未上市跳过。缓存目录缺失、CSV 损坏或
正常上市股票在目标区间异常缺数据，仍然属于真实错误，不能一并吞掉。

### 5.4 宏观上下文

优先级：

1. 精确日期的历史宏观总结；
2. 不晚于 D 的最近宏观总结；
3. D 日宏观客观面板；
4. 受约束联网补充。

需要修正当前 `pick_macro_file()` 的危险回退：如果不存在不晚于 D 的文件，不得返回目录中最新文件，因为该文件可能来自未来。

### 5.5 渐进式热点新闻

回测默认不运行 `/gradual-hot-news-summary`。

原因：

- 状态会从上一日继承；
- 缺失中间日期时容易把未来状态倒灌；
- 成本高；
- 它主要服务选股主题记忆，不是 fixed_tracked 决策的硬依赖。

如果当日已经存在且通过日期审计，可作为可选输入读取。缺失时不专门补写，改由受约束联网研究和本地当日新闻提供上下文。

### 5.6 个股新闻和公告

`prepare-day --build-missing-inputs` 在重建 04 前复用日常公告接口，依次执行普通
公告抓取、原子摘要和 `news_audited.json` 增量审计。公告缓存仍写入共享
`data/stock_info`，因为它是日常链路本来就会持续增量维护的公共数据。

为避免按回测交易日重复付费，每个实验使用
`checkpoints/announcement_preparation.json` 记录已成功准备的股票：

- 冻结固定池股票通常在首个决策日准备；
- 后续新出现的 `12_quant_prefilter_long.csv` 候选在首次进入研究范围时准备；
- 成功后本实验不再重复联网，失败项会在后续日期重试；
- `network_mode=disabled` 时不刷新，只读取已有审计公告。

本地 `search_stock_news(symbol, D)` 按公告发布日期过滤：

- 保留最近 95 天内、发布日期 `<= D` 的公告；
- D 日收盘后分析允许使用 D 日已经发布的公告；
- 严禁回填 D 日之后的公告。

仍需注意：

- `news_audited.json` 可能是在未来重新审核生成；
- 审核摘要本身可能夹带后来认知；
- 公告审计模型的联网背景信息仍可能带入后来认知。

回测读取器应保留原始公告发布日期，并把“原文日期安全”和“未来生成的摘要可能污染”分开标记。

### 5.7 财报深研

财报研究按“披露事件”生成，不按交易日生成。

对每只股票：

1. 找到不晚于 D 的最新财报披露；
2. 若该财报已有合格的 point-in-time 研究，复用；
3. 否则只使用截至披露窗口可知的信息重新研究；
4. 生成后在后续日期持续沿用；
5. 新财报披露后再更新。

严格设置：

```python
report_release_slack_days = 0
```

不能沿用 fixed_tracked 日常研究包当前的一天宽限。

财报 PDF 到 Markdown 的 MinerU 转换是内容格式转换，可以跨实验复用，不需要每个交易日重复转换。

回测 `prepare-day` 必须在构建04以前执行财报门禁：

1. 每个实验、每只股票增量同步一次当前可取得的财报原文缓存；
2. 读取阶段按 D 截断，选择 D 日以前最新正式财报；
3. 检查 `summary_index.json` 是否已登记该公告对应且实际存在的深度总结；
4. 缺少时返回 `needs_financial_research`，不生成04；
5. 主 Agent 复用完整 `financial-report-summary` 多角色流程；
6. 使用 `--as-of-date D --require-deep-research` 注册后重跑当日输入准备。

财报识别必须排除募集说明书中的“半年报更新稿”和“披露提示性公告”，并在匹配
“年度报告”前优先识别“半年度报告”，避免子串误判。历史财报研究不得读取未来
基本面总结、未来产业 card 或当前一致预期缓存；没有分析日不可变预测快照时应
明确缺失。

### 5.8 估值与价格研究

价格和估值计算必须在读取阶段截断到 D。原始价格缓存可以包含 D 之后的数据，但任何滚动窗口、相似行情、PE/PB 分位和技术指标都只能使用 `date <= D` 的行。

生成文件时不得因为当前缓存更完整，就把未来财报数据用于 D 日的 TTM 或 Forward 指标。

## 6. 回测目录与隔离

### 6.1 目录

```text
data/backtests/fixed_tracked/{experiment_id}/
├── experiment.json
├── coverage.json
├── source_audit.jsonl
├── checkpoints/
│   └── state.json
├── selection_runs/
│   └── {date}/
├── skill_runs/
│   └── {date}/
│       ├── run_manifest.json
│       └── fixed_tracked/
│           ├── 00_backtest_context.json
│           ├── 00_backtest_context.md
│           ├── 01_global_context.md
│           ├── 02_basic_snapshot_payload.json
│           ├── 03_agent_input.md
│           ├── 03_stock_analysis_input.md
│           ├── 04_stock_research/
│           ├── debate/
│           ├── 05_decision.json
│           └── 06_execution_log.json
├── agent_data/
│   ├── position/
│   │   └── position.jsonl
│   ├── stock_decisions.json
│   ├── decision_summary.json
│   └── portfolio_daily_summary.json
└── results/
    ├── summary.json
    ├── summary.md
    ├── equity_curve.csv
    ├── trades.csv
    ├── orders.csv
    ├── daily_universe.csv
    └── missing_context.json
```

### 6.2 `experiment.json`

该文件不是宽泛的业务 manifest，只记录保证实验可复现所必需的参数：

```json
{
  "experiment_id": "fixed_20260101_20260803_001",
  "start_date": "2026-01-01",
  "end_date": "2026-08-03",
  "initial_cash": 500000,
  "execution_rule": "next_market_trading_day_open",
  "network_mode": "guarded_web",
  "commission_rate": 0,
  "stamp_duty_rate": 0,
  "slippage_bps": 0,
  "base_universe": ["..."],
  "long_candidate_policy": "daily_12_quant_prefilter_long",
  "survivorship_bias": true,
  "git_commit": "...",
  "working_tree_dirty": true,
  "model_config": {},
  "prompt_hashes": {}
}
```

实验目录名是稳定身份，不是必须随截止日期变化的展示名称。默认命名采用：

```text
fixed_{start_date}_{created_at}
```

不再把 `end_date` 写进默认实验ID。`start_date` 一经创建不可修改；改变起点意味着
重新回测，必须建立新实验。`end_date` 可通过 `extend` 向后扩展，并在
`extension_history` 记录每次旧、新截止日期。

### 6.3 路径隔离要求

回测代码不得通过普通 `signature` 拼接回正式：

```text
data/agent_data/
```

所有仓位、交易总结和运行态函数都必须显式接收：

```python
backtest_root: Path
```

不允许依赖全局环境变量把路径“碰巧”切换到回测目录。

任何回测测试都应断言正式目录的文件哈希在运行前后没有变化。

### 6.4 一套业务规则，两套运行上下文

隔离不通过复制整套 Skill 实现，而通过显式运行上下文实现：

```python
@dataclass(frozen=True)
class RunContext:
    mode: Literal["live", "backtest"]
    source_data_root: Path
    run_output_root: Path
    skill_runs_root: Path
    selection_runs_root: Path
    agent_data_root: Path
    research_cache_root: Path
    macro_output_root: Path
    experiment_id: str | None
    knowledge_cutoff: str | None
```

日常模式：

```text
mode = live
source_data_root = data
run_output_root = data
skill_runs_root = data/skill_runs
selection_runs_root = data/selection_runs
agent_data_root = data/agent_data
```

回测模式：

```text
mode = backtest
source_data_root = data
run_output_root = data/backtests/fixed_tracked/{experiment_id}
skill_runs_root = {experiment_root}/skill_runs
selection_runs_root = {experiment_root}/selection_runs
agent_data_root = {experiment_root}/agent_data
```

`source_data_root` 和 `run_output_root` 必须分开。回测需要读取正式原始数据缓存；
01-08、账本、辩论和回测缓存不能写回正式目录。唯一例外是
`data/stock_info/*/{analysis,pe_pb_analysis}`：它们本来就是日常流水线按日期覆盖的
派生中间产物，回测复用正常分析接口时允许按回测日覆盖。

现有日常命令不传 `RunContext` 时使用 `RunContext.live()`，保持原行为。回测入口必须显式构造 `RunContext.backtest(experiment_root)`，不得依赖 Agent 自己拼路径。

### 6.5 共享只读数据与必须隔离的数据

可以共享但在回测中只读：

```text
data/stock_info/*/prices/
data/stock_info/*/disclosures/
data/stock_info/*/news/
data/stock_info/*/financials_cache/
data/stock_info/*/share_info/
data/stock_info/*/disclosures/pdfs/
data/stock_info/*/disclosures/md/
data/universe/master_universe.json
data/factor_store/（经过日期因果审计后）
已有且通过日期审计的宏观、财报和研究文件
```

这些文件体积大，没必要为每次回测复制。行情、财报和股本等原始缓存由读取器使用
`knowledge_cutoff` 截断。普通公告缓存是例外：回测可复用日常公告接口增量更新
`disclosures` 与 `news`，但装配 04 时仍必须按公告发布日期截断。

以下两个目录不属于上述只读原始缓存：

```text
data/stock_info/*/analysis/
data/stock_info/*/pe_pb_analysis/
```

`prepare-day` 应调用与日常 `run_daily_pipeline` 相同的
`analyze_stock_dynamics_and_valuation(symbol, run_date)`，按回测日重新生成这两个
目录，并将接口返回的完整 Price Report 与 Valuation Report 直接合入 04。

必须写入实验目录：

```text
当日 selection_runs
当日 skill_runs/01-08
历史回测新生成的宏观总结
受约束联网生成的历史新闻/证据摘要
回测专用财报深研
research_artifact_cache
debate 目录
runtime_env.json
position.jsonl
stock_decisions.json
decision_summary.json
portfolio_daily_summary.json
source_audit.jsonl
checkpoints
results
```

原因是这些内容会随 Prompt、模型、联网结果或前一日仓位变化，写回正式目录会污染用户每天手动运行的真实上下文。

其中公告准备 checkpoint 只记录“本实验哪些股票已完成共享公告缓存准备”，不复制
公告内容，也不参与投资决策。

### 6.6 Skill 与 Prompt 的复用方式

不复制 Bull、Bear、Rebuttal、Juror、Finalizer 和财报研究 Prompt。

`backtest-fixed-tracked` 只是一层新的自动化编排 Skill，直接复用：

```text
.codex/skills/auto-trading-fixed-tracked/references/debate-bull.md
.codex/skills/auto-trading-fixed-tracked/references/debate-bear.md
.codex/skills/auto-trading-fixed-tracked/references/debate-rebuttal.md
.codex/skills/auto-trading-fixed-tracked/references/debate-juror.md
.codex/skills/auto-trading-fixed-tracked/references/debate-finalizer.md
.codex/skills/financial-report-summary/references/*
configs/prompt_flow/fixed_tracked/*
```

新 Skill 只增加以下差异：

- 实验路径；
- `00_backtest_context`；
- 取消人工暂停；
- 自动接受 P0；
- 自动执行组合层数量保护；
- 自动生成 05；
- 自动调用回测模式 `run_post_trade.py`；
- 按 checkpoint 继续下一交易日。

角色 Prompt 中不硬编码 `data/skill_runs/...`，而是由编排器传入当天唯一的 `book_dir`。角色仍直接读取规则文件，主 Agent 不复述规则。

### 6.7 需要改造的路径边界

当前并不是所有 `--base-dir` 都真正传到底层。需要集中改造以下边界：

1. `daily_pipeline`：01-04 输出根目录和研究缓存根目录；
2. `manage_debate`：辩论目录根路径；
3. `merge_subagent_decisions`：05 输出和分析索引路径；
4. `run_post_trade`：06-08、运行态和账本根路径；
5. `trade_summary`：决策历史和组合历史根路径；
6. `price_tools`：仓位文件根路径与回测精确开盘价读取；
7. `financial-report-summary`：回测新生成总结的输出 workdir；
8. 宏观和联网研究：回测补写内容的输出目录。

这些模块都应接收同一个 `RunContext` 或由它派生的 `RunPaths`。不要在每个函数里增加互不一致的 `if backtest`。

### 6.8 双重防污染

除路径注入外，再增加硬保护：

1. 回测模式的 `run_output_root.resolve()` 必须位于 `data/backtests/fixed_tracked/`；
2. 回测模式拒绝把 `agent_data_root` 解析到正式 `data/agent_data/`；
3. 回测模式禁止任何 `force_refresh`；
4. 回测模式禁止删除或清理正式 `data/skill_runs/{date}`；
5. 每个实验使用唯一文件锁，防止两个会话同时推进同一账本；
6. 每个订单使用稳定 `order_id`，防止重试重复成交；
7. 集成测试记录正式账本和正式运行目录的运行前后哈希，必须完全一致。

## 7. 目标代码结构

### 7.1 CLI

新增：

```text
scripts/manage_fixed_tracked_backtest.py
```

CLI 只负责解析参数和调用 service，不承载业务逻辑。

建议子命令：

```bash
# 建立实验、扫描覆盖率，不调用 LLM
python scripts/manage_fixed_tracked_backtest.py prepare \
  --start-date 2026-01-01 \
  --end-date 2026-08-03 \
  --initial-cash 500000 \
  --network-mode guarded_web

# 查看进度和待处理日期
python scripts/manage_fixed_tracked_backtest.py status \
  --experiment-id fixed_20260101_20260803_001

# 保留原目录、账本和历史记忆，向后扩展截止日期
python scripts/manage_fixed_tracked_backtest.py extend \
  --experiment-id fixed_20260101_20260803_001 \
  --end-date 2026-09-30

# 为某日准备确定性输入
python scripts/manage_fixed_tracked_backtest.py prepare-day \
  --experiment-id fixed_20260101_20260803_001 \
  --date 2026-04-01

# 在 05 已生成后，通过回测模式 run_post_trade 模拟挂单/成交并推进账本
python scripts/manage_fixed_tracked_backtest.py execute-day \
  --experiment-id fixed_20260101_20260803_001 \
  --date 2026-04-01

# 生成最终报告
python scripts/manage_fixed_tracked_backtest.py finalize \
  --experiment-id fixed_20260101_20260803_001
```

### 7.2 Service

新增：

```text
services/backtest/
├── experiment.py
├── trading_calendar.py
├── coverage_audit.py
├── universe_builder.py
├── historical_input_builder.py
├── source_date_guard.py
├── ledger.py
├── execution_simulator.py
├── portfolio_guard.py
├── metrics.py
└── orchestrator.py
```

职责：

| 模块 | 职责 |
| --- | --- |
| `experiment.py` | 创建和读取实验配置，冻结股票池、Prompt 和 Git 信息 |
| `trading_calendar.py` | 构造真实交易日期，不用工作日近似 |
| `coverage_audit.py` | 扫描历史输入并分类 |
| `universe_builder.py` | 合并固定池、长期候选、持仓股 |
| `historical_input_builder.py` | 构建回测目录中的 00-04 |
| `source_date_guard.py` | 校验联网证据发布日期和回顾性污染 |
| `ledger.py` | 只读写实验目录下的 position 和订单 |
| `execution_simulator.py` | 由回测模式 `run_post_trade.py` 调用，完成 D+1 精确开盘价模拟成交 |
| `portfolio_guard.py` | 自动处理现金、持仓、交易单位和多单冲突 |
| `metrics.py` | 净值、回撤、收益和交易统计 |
| `orchestrator.py` | 状态机、断点续跑和阶段调用 |

### 7.3 Skill

新增：

```text
.codex/skills/backtest-fixed-tracked/
├── SKILL.md
└── references/
    ├── guarded-web-policy.md
    ├── orchestration.md
    └── result-review.md
```

`SKILL.md` 只保留核心步骤，详细防穿越规则和调度规则放入 references，避免每次触发都占用过多上下文。

## 8. 单日多 Agent 流程

### 8.1 自动化差异

日常 `auto-trading-fixed-tracked` 有两次人工暂停：

1. 用户确认 P0；
2. 用户确认组合交易数量和是否生成 `05`。

回测中取消这两次暂停，但不取消原有角色分工。

### 8.2 单日状态机

```text
inputs_ready
  -> p0_selected
  -> debates_running
  -> verdicts_ready
  -> portfolio_checked
  -> decision_written
  -> orders_created
  -> next_open_executed
  -> ledger_updated
  -> day_completed
```

每次状态转换都落盘。中断后从最近完成状态继续，不重复调用已经成功的 Agent。

### 8.3 P0

主 Agent读取：

1. `00_backtest_context.md`
2. `03_agent_input.md`
3. `02_basic_snapshot_payload.json`
4. `01_global_context.md`
5. 已审计通过的可选热点和板块文件

自动接受 P0，不向用户逐日确认。

未进入 P0 的股票视为当日无新交易指令。它们不需要逐股启动六个辩论 Agent。

### 8.4 辩论

继续复用现有角色规则：

- Bull；
- Bear；
- 原 Bull/Bear follow-up Rebuttal；
- Juror 01/02/03；
- 本地聚合；
- Finalizer。

每个角色额外强制读取 `00_backtest_context.md` 和回测联网规则。

文件所有权保持不变，任何 subagent 都不能直接写 `05_decision.json`。

### 8.5 自动组合复核

真实流程中的人工组合复核在回测中改为确定性 `PortfolioGuard`：

1. 先处理 SELL，再处理 BUY；
2. SELL 数量上限为执行前实际持仓；
3. A 股数量必须满足 100 股交易单位；
4. BUY 总额不得超过成交时可用现金；
5. 多个 BUY 现金冲突时，按以下顺序处理：
   - `confidence_score` 从高到低；
   - 同分时优先持仓补仓；
   - 再按 symbol 排序保证可复现；
6. 剩余现金不足时，把最后一个订单缩减到可负担的合法交易单位；
7. 不足一个交易单位时拒绝该订单；
8. 不允许 PortfolioGuard 改变 Juror 多数动作，只能调整或拒绝数量；
9. 所有调整写入 `portfolio_adjustments`。

该策略需要在实现评审时再次确认，因为它代替了日常流程中的人工判断。

### 8.6 05 生成

继续通过：

```bash
python scripts/merge_subagent_decisions.py \
  --date {date} \
  --book-type fixed_tracked \
  --source debate
```

但 `--base-dir` 必须指向实验自己的 `skill_runs` 根目录，或者扩展脚本支持明确的 `--experiment-root`。不得读取正式 `data/skill_runs` 中同日期的辩论结果。

### 8.7 自动后处理并进入下一日

`05_decision.json` 校验通过后，主 Agent自动调用回测模式的：

```bash
python scripts/run_post_trade.py \
  --date {D} \
  --book-type fixed_tracked \
  --signature {backtest_signature} \
  --backtest-root {experiment_root} \
  --execution-date {D+1} \
  --execution-price open
```

处理顺序：

1. 读取 D 日 `05_decision.json`；
2. 将 BUY/SELL 转成回测订单；
3. 在 D+1 对应标的下一可交易日开盘成交；
4. 写 D 日目录中的 `06_execution_log.json`；
5. 写 `07_daily_summary.json` 和 `08_history_merge.json`；
6. 把成交后仓位追加到实验 `position.jsonl`，记录日期为实际成交日；
7. 把 D 日最终投资逻辑写入实验交易总结；
8. 构建 D+1 收盘研究包时，只读取截至 D+1 已经形成的实验仓位和历史交易总结；
9. 再运行 D+1 的完整 Agent 决策链。

因此循环的主体是：

```text
D 日 01-04
→ Agent 最终选择
→ D 日 05_decision.json
→ 回测 run_post_trade
→ D+1 开盘成交
→ position.jsonl + D 日 06-08
→ D+1 收盘 01-04 继承上述历史
→ D+1 Agent 最终选择
```

## 9. 回测账本

### 9.1 `position.jsonl`

仍保留追加写格式，但每条记录应能区分决策日和成交日：

```json
{
  "id": 12,
  "decision_date": "2026-04-01",
  "date": "2026-04-02",
  "execution_date": "2026-04-02",
  "positions": {
    "300476.SZ": 500,
    "CASH": 450000
  },
  "this_action": {
    "action": "buy",
    "trades": {
      "300476.SZ": 500
    },
    "execution_prices": {
      "300476.SZ": 100
    }
  },
  "total_value": 500000
}
```

### 9.2 订单与仓位分离

`05_decision.json` 是决策，不等于成交。

应单独保存：

- 决策动作；
- 原始订单数量；
- 组合层调整后数量；
- 计划成交日；
- 实际成交日；
- 实际成交价；
- 未成交或拒绝原因。

### 9.3 投资逻辑记忆

回测不读取真实 `book-fixed_tracked` 历史。

每一天完成后，把本实验的决策和成交结果写入实验自己的：

```text
agent_data/stock_decisions.json
agent_data/decision_summary.json
agent_data/portfolio_daily_summary.json
```

下一交易日的 `04_stock_research` 只从本实验历史生成：

- 最近一次投资逻辑总结；
- 上轮遗留待核验事项。

不得读取实验日期之后的回测结果。

## 10. 成本与缓存

### 10.1 不重复生成季度研究

财报深研缓存键：

```text
symbol
+ report_period
+ report_release_date
+ knowledge_cutoff
+ prompt_hash
+ model
```

同一财报在后续交易日直接复用。

### 10.2 Agent 阶段缓存

每个输出保存输入指纹：

```text
role prompt hash
+ 00 backtest context hash
+ 01-04 relevant input hash
+ upstream debate hash
+ model identity
```

例如只修改 Juror Prompt 时：

- Bull/Bear opening 可复用；
- Rebuttal 可复用；
- 三份 ballot 和 Finalizer 重跑。

### 10.3 分批运行

Skill 默认不承诺在一个模型 turn 内跑完八个月。

建议：

```yaml
dates_per_batch: 5
max_concurrent_stocks: platform_limit
```

每批完成后更新 checkpoint。用户再次说“继续回测”时从下一日期继续。

### 10.4 试运行

正式全区间前：

1. 先跑 3 个连续交易日，验证记账和成交日；
2. 再跑 10 个交易日，评估费用和 Agent 稳定性；
3. 最后放开完整区间。

## 11. 最终结果

### 11.1 `summary.json`

至少包含：

```json
{
  "initial_value": 500000,
  "final_value": 0,
  "cash": 0,
  "market_value": 0,
  "total_return": 0,
  "annualized_return": 0,
  "max_drawdown": 0,
  "trade_count": 0,
  "buy_count": 0,
  "sell_count": 0,
  "turnover": 0,
  "completed_decision_days": 0,
  "missing_context_days": 0,
  "guarded_web_source_count": 0,
  "rejected_future_source_count": 0,
  "pending_orders_at_end": 0,
  "lookahead_risk": true,
  "survivorship_bias": true
}
```

### 11.2 资产曲线

`equity_curve.csv`：

```text
date,cash,market_value,total_value,daily_return,drawdown,stale_price_symbols
```

### 11.3 对照基准

至少生成两个对照：

1. 上证指数或沪深 300 的同期收益；
2. 冻结的 17 只基础股票等权买入持有。

如果个别股票在起点尚未上市，等权基准必须说明加入规则，不能提前使用上市后的价格。

### 11.4 最终资产日期

实验结束日以该日收盘盯市：

```text
final_value = end_date 收盘后的现金 + 持仓市值
```

结束日盘后新产生的 BUY/SELL 因为需要下一交易日开盘才能成交，默认不计入本区间成交，只列为期末未执行决策。

## 12. 失败与恢复

### 12.1 可继续的 warning

- 当日热点文件缺失；
- 某个联网事实找不到合格历史来源；
- 某只未上市股票没有行情；
- 长期候选文件缺失且无法重建；
- 某个非关键估值字段不可用。

### 12.2 必须停止当日

- 当日基础价格快照包含 D 之后的数据；
- 账本路径解析到正式 `data/agent_data`；
- 同一订单重复成交；
- `05_decision.json` 无法通过契约校验；
- 仓位出现负数；
- 现金低于允许的浮点误差；
- 成交价不是标的 D 之后下一交易日的精确开盘价；
- Agent 使用了已确认晚于 D 发布的关键来源且未修正。

### 12.3 幂等性

- `prepare` 重跑不得清空已有实验；
- `prepare-day` 输入未变化时直接复用；
- `execute-day` 对同一决策只能成交一次；
- `finalize` 可重复生成；
- 任何重试都不能向 `position.jsonl` 重复追加相同 `order_id`。

## 13. 验证方案

### 13.1 单元测试

至少覆盖：

1. 当前 17 只基础池被正确冻结；
2. 当日 `12_quant_prefilter_long.csv` 只扩展研究范围，不直接产生交易；
3. 缺失时可以基于历史因子安全重建 `12_quant_prefilter_long.csv`；
4. 持仓股退出长期候选后仍在分析集合；
5. 因子读取不使用 D 以后数据；
6. 来源发布日期晚于 D 时被拒绝；
7. 未知发布日期不会成为关键交易依据；
8. D 决策只使用 D+1 精确开盘价；
9. 节假日不被当作交易日；
10. 停牌时不拿上一日开盘价冒充成交价；
11. SELL 不超过持仓；
12. BUY 不超过现金；
13. 回测账本路径完全隔离；
14. 相同 `order_id` 不重复执行；
15. 结束日决策不越界成交；
16. 最大回撤计算正确；
17. 回测模式 `run_post_trade.py` 正确生成 06-08；
18. D 日交易总结会进入 D+1 研究包，但 D+1 之后的总结不会倒灌。

### 13.2 集成测试

构造三天临时数据：

```text
D1 收盘产生 BUY
D2 开盘成交
D2 收盘产生 SELL
D3 开盘成交
```

验证：

- `05_decision.json` 的日期；
- 两次实际成交价格；
- `position.jsonl` 的现金和股数；
- 最终资产；
- 日常正式账本没有变化。

### 13.3 小规模真实验证

选择已有完整 `01-04` 的连续 3 至 5 个交易日：

- 运行完整多 Agent 回测；
- 人工抽查所有联网关键来源日期；
- 检查同一日所有角色都读取了 `00_backtest_context`；
- 检查期末 `position.jsonl` 与 `trades.csv` 一致。

## 14. 实施顺序

### 阶段一：纯 Python 骨架

1. 建立 `services/backtest/`；
2. 实现实验配置、目录隔离和交易日；
3. 实现覆盖率审计；
4. 实现每日 universe；
5. 实现独立账本和 D+1 开盘模拟器；
6. 使用手写测试 `05_decision.json` 验证成交闭环。

这一阶段不调用 LLM，先证明不会污染真实账本。

### 阶段二：历史输入构建

1. 接入历史因子和 `12_quant_prefilter_long.csv`；
2. 为 daily pipeline 增加回测输出根目录；
3. 增加 `00_backtest_context`；
4. 修复宏观未来文件回退；
5. 财报选择在回测模式强制 slack 为 0；
6. 增加来源日期审计。

### 阶段三：回测 Skill

1. 创建 `.codex/skills/backtest-fixed-tracked`；
2. 复用 fixed_tracked 辩论角色文件；
3. 去掉两次人工暂停，改为 checkpoint；
4. 接入 PortfolioGuard；
5. 支持批量、暂停、继续。

### 阶段四：结果与优化

1. 净值、回撤和基准；
2. 阶段缓存和 Prompt hash；
3. 成本统计；
4. 3 日、10 日、完整区间验证。

## 15. 预计修改范围

新增：

```text
scripts/manage_fixed_tracked_backtest.py
services/backtest/*
.codex/skills/backtest-fixed-tracked/*
docs/backtest/fixed_tracked_agent_backtest_design.md
```

可能需要以兼容方式扩展：

```text
scripts/run_post_trade.py
services/pipeline/daily_pipeline.py
services/pipeline/steps/build_global_context.py
services/pipeline/steps/build_stock_research.py
services/research/macro_summary.py
services/research/financial_report.py
scripts/manage_debate.py
scripts/merge_subagent_decisions.py
```

原则：

- 只增加可选参数或新入口；
- 日常交易默认行为不变；
- 不把回测分支散落到真实交易执行器；
- 不改变现有 fixed_tracked 的真实交易价格与仓位规则；
- 回测目录之外无写副作用。

## 16. 实现前仍需在代码评审中确认的细节

以下不阻塞设计，但实现时需要展示给用户审核：

1. 多个 BUY 导致现金不足时的自动排序和缩量规则；
2. 港股每手股数和多市场交易单位来源；
3. 停牌订单是持续等待还是次日失效；
4. 回测结束日之后的 D+1 决策是否完全忽略；
5. 第一版比较沪深 300、上证指数还是同时比较。
