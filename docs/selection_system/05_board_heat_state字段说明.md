# 05_board_heat_state 字段说明

更新日期：2026-06-12

说明：

1. 本文档解释 `data/selection_runs/YYYY-MM-DD/05_board_heat_state.json` 的字段含义。
2. 该文件由 `services/selection_system/board_heat.py` 在 `python scripts/manage_selection_system.py build-board-heat-state --date YYYY-MM-DD` 时生成，是渐进式热点新闻总结、AI 选股、auto-trading 三本账本的共同板块输入之一。
3. 本文档只解释字段语义，不讨论策略规则或最终买卖结论。
4. 文件级导航摘要见 `05_board_heat_digest.json`（见 `板块热度摘要与查询设计.md`）。

## 1. 文件层字段

### `schema_version`

- 当前 JSON 结构版本号。

### `run_date`

- 本次选股链路的运行日期。

### `updated_at`

- 本文件最终生成时间。

### `model`

- 生成研究结论时使用的模型名称。

### `summary`

- 文件级摘要。
- 当前包括：
  - `top_up_count`：涨幅榜入选板块数。
  - `top_down_count`：跌幅榜入选板块数。
  - `researched_count`：最终写入 `boards` 的板块研究条数。

### `boards`

- 板块研究结果列表。
- 每个元素代表一个板块。

### `source_status`

- 上游数据源和研究步骤的状态记录。
- 用于排查是板块历史、市场快照还是模型研究出的问题。

## 2. 单个板块对象字段

### `board_name`

- 板块名称，例如 `保险`、`化学纤维`。

### `board_code`

- 同花顺行业板块代码。

### `direction`

- 该板块是从哪个方向入选的。
- `up` 表示涨幅榜。
- `down` 表示跌幅榜。

### `rank`

- 在对应方向榜单中的名次。

### `change_pct`

- 板块当日涨跌幅，单位 `%`。

### `up_count`

- 板块当日上涨股票家数。

### `down_count`

- 板块当日下跌股票家数。

### `total_turnover`

- 板块当日总成交额。
- 当前单位为 `元`。

### `leading_stock_name`

- 板块当日领涨股名称。

### `leading_stock_change_pct`

- 板块当日领涨股涨跌幅，单位 `%`。

### `price_as_of_date`

- 量化指标对应的历史价格序列截止日期。
- 注意：
  - 它可能早于 `run_date`
  - 原因通常是板块历史指数接口尚未更新到当天收盘

## 3. `related_stock_hints`

这是板块内的辅助股票线索，不等于最终推荐买入股票。

### `symbol`

- 股票代码。

### `name`

- 股票名称。

### `turnover`

- 原始展示格式的成交额，例如 `55.09亿`。

### `turnover_value`

- 数值化成交额，单位 `元`。

### `latest_price`

- 最新价。

### `change_pct`

- 当日涨跌幅，单位 `%`。

### `role_hint`

- 该股票被纳入辅助线索的原因。
- 当前默认多为 `成交额前排`。

## 4. `quant_metrics`

这一层是板块量化快照，主要用来回答：

1. 板块最近一段时间强不强。
2. 是持续走强、反弹、还是高位回落。
3. 板块内部是普涨还是龙头集中。

### 4.1 `interval_returns`

区间收益率，单位 `%`。

- `return_3d_pct`
- `return_5d_pct`
- `return_10d_pct`
- `return_20d_pct`
- `return_60d_pct`
- `return_120d_pct`
- `return_180d_pct`

含义：

- 例如 `return_20d_pct = -10.3015`
- 表示最近 `20` 个交易日累计收益率约为 `-10.3015%`

### 4.2 `interval_rankings`

区间收益率在全体 THS 行业板块中的横截面排名。

- `rank_3d`
- `rank_5d`
- `rank_10d`
- `rank_20d`
- `rank_60d`
- `rank_120d`
- `rank_180d`

含义：

- 数值越小，说明该窗口下相对越强。
- 例如 `rank_20d = 88`
- 表示近 `20` 个交易日表现接近全市场行业板块倒数。

### 4.3 `volatility`

年化波动率，单位 `%`。

- `volatility_20d_pct`
- `volatility_60d_pct`

含义：

- 越大表示短中期波动越大。
- 可理解为“板块最近的价格振幅有多剧烈”。

### 4.4 `max_drawdown`

窗口内最大回撤，单位 `%`。

- `max_drawdown_20d_pct`
- `max_drawdown_60d_pct`
- `max_drawdown_120d_pct`

含义：

- 在对应窗口里，从阶段高点往下最多跌了多少。
- 数值为负，绝对值越大表示回撤越深。

### 4.5 `sharpe`

夏普比率。

- `sharpe_20d`
- `sharpe_60d`
- `sharpe_120d`

含义：

- 衡量风险调整后的收益质量。
- 一般来说：
  - 越高越好
  - 负值表示这段时间收益质量差

### 4.6 `continuity`

连续性和强势频次。

#### `up_days_5d` / `up_days_10d` / `up_days_20d`

- 最近 `5/10/20` 个交易日里，上涨天数有多少。

#### `consecutive_up_days`

- 截至最新一天，已经连续上涨了多少天。

#### `consecutive_down_days`

- 截至最新一天，已经连续下跌了多少天。

#### `top10_hits_10d` / `top10_hits_20d`

- 最近 `10/20` 个交易日里，有多少次进入“全板块单日涨幅前十”。

### 4.7 `breadth`

板块宽度和内部结构。

#### `up_ratio_pct`

- 当日上涨家数占比，单位 `%`。
- 当前口径：
  - `up_count / (up_count + down_count)`

#### `top3_turnover_share_pct`

- 板块内成交额前三股票合计，占整个板块成交额的比例，单位 `%`。
- 越高说明成交越集中在少数龙头。

#### `leader_vs_board_deviation_pct`

- 领涨股涨幅减去板块涨幅，单位 `%`。
- 越大说明龙头明显强于板块平均。
- 可用于判断是“龙头单点拉升”还是“板块整体普涨”。

### 4.8 `phase`

板块所处阶段的辅助量化特征。

#### `drawdown_from_20d_high_pct`

- 相对最近 `20` 日高点的回撤，单位 `%`。

#### `drawdown_from_60d_high_pct`

- 相对最近 `60` 日高点的回撤，单位 `%`。

#### `breakout_20d_high`

- 是否刚突破近 `20` 日新高。

#### `breakout_60d_high`

- 是否刚突破近 `60` 日新高。

#### `return_acceleration_5d_minus_20d_pct`

- `5` 日收益率减去 `20` 日收益率，单位 `%`。
- 这个值越大，通常表示短期强于中期，偏加速。
- 这个值越小，通常表示短期弱于中期，偏衰减。

## 5. 模型研究字段

以下字段是模型基于板块事实、量化快照和相关新闻写出的解释层。

### `research_summary`

- 一句话总结板块今天为什么强或弱。

### `driver_analysis`

- 对板块直接催化和更深层逻辑的解释。

### `persistence_analysis`

- 对“这波逻辑已经持续多久、还可能持续多久”的文字判断。

### `structure_view`

- 对板块内部结构的判断。
- 例如：
  - 普涨
  - 龙头集中
  - 分化

### `recommended_watch_stocks`

- 建议重点跟踪的股票列表。
- 它们是观察标的，不等于最终买入指令。

字段包括：

- `symbol`：股票代码。
- `name`：股票名称。
- `reason`：建议跟踪的原因。

### `risk_points`

- 当前板块最值得注意的风险点列表。

## 6. 阅读顺序建议

如果是人工或模型第一次读取 `05_board_heat_state.json`，建议按这个顺序看：

1. 先看 `direction`、`rank`、`change_pct`，知道它为什么入选。
2. 再看 `interval_returns` 和 `interval_rankings`，判断短中期强弱。
3. 再看 `continuity` 和 `phase`，判断它是在加速、震荡还是回落。
4. 再看 `breadth`，判断是板块普涨还是少数龙头主导。
5. 最后看 `driver_analysis`、`persistence_analysis` 和 `risk_points`，把量化证据翻译成研究结论。

## 7. 当前用途建议

这份字段说明文档当前适合两种用途：

1. 作为人工阅读 `05_board_heat_state.json` 的字段字典。
2. 在后续构建 `selection_bundle` 或模型输入时，作为附带说明材料一并提供给模型。
