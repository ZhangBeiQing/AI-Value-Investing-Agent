---
paths:
  - scripts/manage_daily_data.py
  - scripts/run_daily_pipeline.py
  - scripts/run_post_trade.py
  - scripts/execute_trade_from_decision.py
  - scripts/merge_trade_summary.py
  - scripts/step*.py
  - services/pipeline/**
  - services/data_refresh/**
  - services/trading/**
  - prompts/**
  - configs/prompt_flow/**
---

# Skill Pipeline Rules

本规则约束 `skill-only` 主链路的编排、产物契约和交易后处理。

## 架构边界

- `scripts/` 只负责 CLI 参数解析、调用服务入口，不承载复杂业务逻辑。
- 真实编排逻辑放在 `services/data_refresh/`、`services/pipeline/`、`services/trading/`。
- 改动主链路时，优先维护服务层，再让脚本入口薄调用。

## 产物契约

- `run_daily_pipeline` 负责 `01-04`：
  - `01_global_context.md`
  - `02_basic_snapshot_payload.json`
  - `03_agent_input.md`
  - `04_stock_research/*.md`
- `run_post_trade` 负责 `05-08` 的后处理串联。
- 新增字段可以扩展，但不要随意重命名或删除既有关键文件、字段、目录结构，除非用户明确同意。
- 如果调整 `skill_flow.json`、prompt 文案或产物结构，必须同步更新说明文档和调用链。

## 状态记录

- 主链路失败时不要吞异常，要留下可复现的错误文本。
- 运行证据优先看目标输出文件和 `logs/` 中的日志。

## 交易后处理

- 交易执行优先通过本地 Python 函数完成，不再新增对 MCP trade 的依赖。
- 任何交易改动都要保持与 `position.jsonl`、`stock_decisions.json`、`decision_summary.json`、`portfolio_daily_summary.json` 的一致性。
- 价格引用规则、最小交易单位、无交易日补记逻辑属于高风险区域，修改前必须先确认。

## Prompt / Flow 修改原则

- 优先保持和旧版 `skill` 流程的文件命名与消费方式兼容。
- 改 prompt 时先确认上游输入和下游 JSON 契约，不要只改文案不看产物。
- 如果 agent 侧无法稳定联网，主流程仍应以本地预生成数据为主，而不是把关键步骤重新推回在线工具。

## 修改后验证

- 改 `01-04` 主链路，至少运行：
  - `python scripts/run_daily_pipeline.py --date YYYY-MM-DD --base-dir data/tmp_<name>`
- 改交易后处理，至少验证：
  - `python scripts/run_post_trade.py --date YYYY-MM-DD`
- 验证时优先使用临时 `--base-dir` 或单股票池，避免污染正式数据。
