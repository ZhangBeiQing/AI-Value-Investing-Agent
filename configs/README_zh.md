# 配置文件

此目录现在只服务于本地 `skill-only` 工作流。历史上的 `default_config.json` 已随旧 MCP / Agent 运行时一并移除。

## 当前有效配置

### `stock_pool.py`

定义每日数据刷新与研究流水线使用的股票池。

### `prompt_flow/skill_flow.json`

定义本地桌面 Agent 使用的主提示词流程。`scripts/run_daily_pipeline.py` 会基于该配置生成 `03_agent_input.md` 等输入产物。

### `.env`

运行时默认值现在主要来自环境变量，而不是一个总控 JSON 配置。常见变量包括：

- `SIGNATURE` 或 `DEFAULT_SIGNATURE`
- `INITIAL_CASH` 或 `INIT_CASH`
- 各模型 API Key 与 Base URL

## 运行约定

当前稳定主流程为：

```bash
python scripts/manage_daily_data.py
python scripts/run_daily_pipeline.py --date YYYY-MM-DD
python scripts/run_post_trade.py --date YYYY-MM-DD
```

每日输入包统一生成在 `data/skill_runs/{date}/`。

## 说明

- 新的项目默认值优先使用环境变量，或拆分为职责单一的小配置文件。
- 除非有明确运维需求，不再恢复“一个 JSON 管全部运行时参数”的旧模式。
- 当前唯一有效的 prompt flow 配置是 `skill_flow.json`。
