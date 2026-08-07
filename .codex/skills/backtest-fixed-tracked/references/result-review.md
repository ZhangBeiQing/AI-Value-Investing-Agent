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
- 是否存在负现金或负持仓；
- 是否存在重复 order_id；
- pending/rejected 订单原因；
- 最大回撤和累计收益；
- `lookahead_risk` 与 `survivorship_bias` 是否保留；
- 正式 `data/agent_data` 是否未发生变化。

本回测使用当前固定池回看历史并允许受约束联网，因此结果是策略模拟，不是零偏差的严格 point-in-time 业绩。
