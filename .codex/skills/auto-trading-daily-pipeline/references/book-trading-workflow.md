# Single Book Trading Workflow

这份文档是 `fixed_tracked`、`short_book`、`long_book` 三个交易 skill 共用的单账本模板。

## 1. 输入前提

- 当天 `python scripts/run_daily_pipeline.py --date YYYY-MM-DD` 已经完成
- 当前账本目录下已经存在：
  - `01_global_context.md`
  - `02_basic_snapshot_payload.json`
  - `03_agent_input.md`
  - `04_stock_research/`

## 2. 账本边界

- 只能读取当前账本目录
- 只能使用当前账本的 `signature`
- 不得把其他账本的持仓、交易历史、目标价、止损价混入当前账本

## 3. 主 agent 固定步骤

1. 定位当前账本目录
2. 先完整阅读 `03_agent_input.md`
3. 再读取 `02_basic_snapshot_payload.json`
4. 再读取 `01_global_context.md`
5. 基于当前账本股票清单建立逐股 subagent

## 4. subagent 固定步骤

每个 subagent 只负责 1 只股票，并且只能读取：

1. `03_agent_input.md`
2. `01_global_context.md`
3. 当前股票自己的 `04_stock_research/*_research.md`

禁止读取其他股票研究包。

## 5. 汇总与确认

- 主 agent 汇总逐股分析、庭审和执行建议
- 向用户展示当前账本的完整结果
- 等待用户确认后再生成当前账本的 `05_decision.json`

## 6. 当前账本输出

- 决策文件：`data/skill_runs/YYYY-MM-DD/<book_type>/05_decision.json`
- 执行日志：`data/skill_runs/YYYY-MM-DD/<book_type>/06_execution_log.json`
- 每日总结：`data/skill_runs/YYYY-MM-DD/<book_type>/07_daily_summary.json`
- 历史合并：`data/skill_runs/YYYY-MM-DD/<book_type>/08_history_merge.json`

## 7. 当前账本后处理

人工确认后，必须按当前账本单独执行：

```bash
python scripts/run_post_trade.py --date YYYY-MM-DD --book-type <book_type> --signature <signature>
```

## 8. 决策文件要求

- `05_decision.json` 只包含当前账本股票
- 逐股结构必须完整，不能压缩成只剩摘要
- `action_type` / `action_num` 仍然由主 agent 在人工确认后补齐
