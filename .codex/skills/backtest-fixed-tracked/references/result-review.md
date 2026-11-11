# 回测结果检查

读取：

```text
results/summary.json
results/equity_curve.csv
agent_data/backtest-{experiment_id}/position/position.jsonl
agent_data/backtest-{experiment_id}/orders.jsonl
```

至少检查：

- 初始资产是否为用户指定金额；
- 最终现金、持仓市值和总资产能否相加；
- BUY/SELL 是否使用决策日之后的精确开盘价；
- 美元成交、成本、持仓市值是否按主干口径折算人民币；港股和美股休市日是否保留 pending；
- 美股收盘是否来自北京时间截止前已结束的交易日，港股休市是否正确使用最近收盘日期；
- 是否没有置信度排序、自动缩量、数量取整或卖超截断；
- 是否存在负现金或负持仓；
- 是否存在重复 order_id；
- pending/rejected 订单原因；
- 最大回撤和累计收益；
- `lookahead_risk` 与 `survivorship_bias` 是否保留；
- 对照 `analysis_schedule.json` 核对研究间隔为七个自然日、休市按日历顺延，中间仅持仓估值；中途改频率的实验按分段说明，不能将此前每日研究历史宣称为全程周度；
- 正式 `data/agent_data` 是否未发生变化。

本回测使用当前固定池回看历史并允许受约束联网，因此结果是策略模拟，不是零偏差的严格 point-in-time 业绩。
