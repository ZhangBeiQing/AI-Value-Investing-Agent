# 每日操盘总结系统设计

更新日期：2026-08-03

## 1. 背景与目标

AI 在每日做决策时容易丢掉历史上下文，会出现：

- 周期股频繁反转（看到低 PE 就买，忽略行业下行）
- 把同一只股票的连续 HOLD 信息全量塞进 prompt，token 浪费严重
- 决策逻辑碎片化，无法回溯审计

本系统通过：

- **结构化记录每日逐股决策**（`stock_decisions.json`）
- **智能合并连续 HOLD/FLAT 序列**（`decision_summary.json`）
- **维护组合级别总结**（`portfolio_daily_summary.json`）

为下一个交易日的 AI prompt 提供高密度、低冗余的「历史交易记忆」。

## 2. 文件布局

每个账本一套，路径形如 `data/agent_data/book-{book_type}/`（`book_type ∈ {fixed_tracked, short_book, long_book}`）。

```text
data/agent_data/book-fixed_tracked/
├── position/
│   ├── position.jsonl                # 每日仓位记录（追加写入），由 tools.price_tools 维护
│   └── manual_position_override.json # 人工干预入口
├── stock_decisions.json              # 原始逐股决策表
├── decision_summary.json             # 合并后的决策摘要
└── portfolio_daily_summary.json      # 组合级日总结
```

实现：`services/trading/trade_summary.py`。文件读写函数：

- `_stock_operations_file(signature)` → `stock_decisions.json`
- `_operation_summary_file(signature)` → `decision_summary.json`
- `_portfolio_summary_file(signature)` → `portfolio_daily_summary.json`

`signature` 与 `book_type` 的映射：`signature = f"book-{book_type}"`。

## 3. 数据契约（与实现一致）

### 3.1 `stock_decisions.json` — 原始决策表

存储 AI 每日对每只股票的原始决策，append-only。

每个条目至少包含：

- `symbol` / `stock_code`（任一即可，`stock_code` 是早期字段，新代码统一传 `symbol`）
- `stock_name`
- `operation_date`（保存时由 `save_daily_operations` 从 `summary_date` 写入）
- `action_type`：`BUY` / `SELL` / `HOLD` / `FLAT`
- `action_num`：操作数量（整数）
- `action_price`：操作价格（可选）
- 决策细节字段。新 fixed_tracked 辩论契约使用 14 字段；`SUMMARY_DETAIL_FIELDS` 仍保留以下部分旧字段，用于历史兼容和完整审计：
  - `scan`
  - `analysis_type`
  - `history_anchor`
  - `allow_reanchor_today`
  - `forecast_reliability`
  - `valuation_mode`
  - `catalyst_and_momentum`
  - `trading_mode`
  - `key_facts`
  - `inferences`
  - `valuation_conclusion`
  - `risk_reward_setup`
  - `motion`
  - `court`（含 `pro` / `con` / `verdict`）
  - `recommended_action`
  - `price_target`
  - `stop_loss`
  - `key_risks`
  - `next_day_watchlist`
  - `confidence_score`

允许同一天同一只股票有多条记录（追加模式），支持 Force Run 的重新决策。

### 3.2 `decision_summary.json` — 合并后的决策摘要

`process_and_merge_operations` 会按以下规则把 `stock_decisions.json` 重建为 `decision_summary.json`：

1. 按 `symbol` 分组、按 `operation_date` 升序排列
2. `BUY` / `SELL` 保留为独立条目：`start_date = end_date = operation_date`，`duration_days = 1`
3. **连续 `HOLD` 或连续 `FLAT` 序列合并**：
   - 序列条件：`action_type` 相同 且 日期连续（包含交易日跳变，由 `get_last_trading_day` 判定）
   - 合并条目 `start_date` 取序列第一天、`end_date` 取最后一天、`duration_days` 自动计算
   - 其他详情字段（`reason` / `key_facts` / `court` 等）**全部取序列最后一天的数据**
4. 触发增量更新失败时会自动 fallback 到「全量重建」，保证 `decision_summary.json` 始终一致

合并后条目的关键字段：

```text
stock_code / stock_name
start_date / end_date / duration_days / action_type
+ SUMMARY_DETAIL_FIELDS（继承自序列最后一天）
```

### 3.3 `portfolio_daily_summary.json` — 组合级别日总结

每日组合级别一行：

```json
{
  "summary_date": "2026-06-10",
  "system_risk_notes": [...],
  "system_focus_items": [...]
}
```

同日重复保存会覆盖；不同日追加。

> 历史上设计稿曾包含 `portfolio_overview` / `total_positions` / `total_value` / `cash_position` / `overall_risk_level` 等字段，**当前实现暂未启用**；如需扩展可在 `save_daily_operations` 中追加。

## 4. 处理流程

### 4.1 步骤 1：保存当日原始决策

```python
save_daily_operations(signature: str, ai_output_json: dict) -> List[dict]
```

输入是 `05_decision.json` 的内容，要求包含：

```json
{
  "summary_date": "YYYY-MM-DD",
  "stock_decisions": [...],
  "system_risk_notes": [...],
  "system_focus_items": [...]
}
```

行为：

- 把 `stock_decisions` 每条加上 `operation_date = summary_date` 后 append 到 `stock_decisions.json`
- 把 `system_*` 写入 `portfolio_daily_summary.json`（同日覆盖）

### 4.2 步骤 2：合并与重建

```python
process_and_merge_operations(signature: str, new_operations: list | None = None)
```

行为：

- 若提供 `new_operations`：尝试增量合并到现有 `decision_summary.json`
- 若增量未产生变化或合并冲突：触发全量重建 `_rebuild_operation_summary`
- 全量重建后会对每个条目做日期相对词归一化（`今日 / 昨日` → 绝对日期）

### 4.3 步骤 3：为下一日生成历史上下文

```python
get_historical_context(signature: str, stock_code: str, n: int) -> list[dict]
# 单只股票最近 N 条记录（按 end_date 降序）

get_stock_memory_context(signature: str, stock_code: str) -> dict
# fixed_tracked 个股研究记忆视图；保留仓位变化和投资理由，
# 排除历史 recommended_action、price_target 和过期执行计划

get_portfolio_historical_context(signature: str, stock_codes: list, n: int = 3) -> dict
# 股票池中每只股票最近 N 条；输出按 end_date 正序（旧→新）便于阅读时间线

load_yesterday_daily_summary(signature: str) -> dict | None
# 加载昨天的 portfolio_daily_summary 条目
```

`build_stock_research` 使用 `get_stock_memory_context` 生成 `04_stock_research` 的“持仓与投资逻辑记忆”。完整历史仍保存在 `stock_decisions.json`；下一轮 Agent只看到最后一次投资逻辑总结及其待核验事项，不再注入所有历史 BUY/SELL 全文，也不会读取上一轮完整执行计划。其他历史函数继续服务组合上下文和旧流程。

## 5. AI 输出格式（输入 → 系统）

`05_decision.json` 中的 `stock_decisions` 数组里每个元素是一个完整 stock entry，由三本账本的 auto-trading skill 在 subagent 阶段生成并由 `merge_subagent_decisions.py` 合并而成。

skill 端要求 entry 字段集见各账本 SKILL.md：

- `configs/prompt_flow/fixed_tracked/stock_decision.schema.json`（fixed_tracked 当前 14 字段）
- `.codex/skills/auto-trading-short-book/SKILL.md`
- `.codex/skills/auto-trading-long-book/SKILL.md`

写入时机：人工确认 `05_decision.json` 后执行 `python scripts/run_post_trade.py --date YYYY-MM-DD --book-type {book_type} --signature book-{book_type}`，由 `services/trading/post_trade_pipeline.py` 串联：

1. 读取 `05_decision.json`
2. 写 `06_execution_log.json` + 更新 `position/position.jsonl`
3. 写 `07_daily_summary.json`
4. 调用 `save_daily_operations` + `process_and_merge_operations` 写 `08_history_merge.json`，同步更新 `data/agent_data/book-{book_type}/` 下的三个 JSON 文件

## 6. 当前阶段实现说明

- 当前以「**本地 JSON 文件**」实现，未迁移到数据库
- `read_json_file` / `write_json_file` 是统一读写入口
- `initialize_data_files` 用于第一次为某个 signature 建立空文件
- 全量重建的代价随历史增长，目前 `fixed_tracked` 已积累约 250 条 summary 条目、6MB 原始 stock_decisions，性能仍然可控
- 未来若迁移到 SQL/NoSQL，只需替换 `read_json_file` / `write_json_file` + 重建逻辑，业务接口（`save_daily_operations` / `process_and_merge_operations` / `get_*_context`）保持不变

## 7. 设计原则

- **信息密度优先**：通过合并连续 HOLD/FLAT 提升每 token 的信息价值密度
- **渐进式上下文管理**：近期细节丰富（保留每天的 `BUY/SELL` 与最近一次 HOLD 详情），远期通过 `duration_days` 概括
- **决策一致性保障**：原始决策表与合并表分离，原始表 append-only 可审计，合并表可重建

## 8. 相关文档

- `docs/PROJECT_SYSTEM_SUMMARY.md` § 5「交易后处理与历史决策合并」
- `.codex/rules/skill-pipeline.md` § 「交易后处理」
- `services/trading/trade_summary.py`（实现）
- `services/trading/post_trade_pipeline.py`（编排）
- `services/trading/trade_executor.py`（交易执行）
