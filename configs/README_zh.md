# 配置文件

此目录现在只服务于本地 `skill-only` 工作流。历史上的 `default_config.json` 已随旧 MCP / Agent 运行时一并移除。

## 当前有效配置

### `stock_pool.py`

定义每日数据刷新与研究流水线使用的股票池。

### `prompt_flow/fixed_tracked/`

定义 fixed_tracked 当前 Prompt 源：

- `investment_policy.md` 是全部 fixed_tracked Agent 共用的核心投资策略单一来源
- `investment_policy.md` + `main_policy.md` 生成主 Agent 的 `03_agent_input.md`
- `investment_policy.md` + `configs/research/web_research_policy.md` + `stock_analysis_policy.md` 生成个股辩论角色的 `03_stock_analysis_input.md`
- `stock_decision.schema.json` 定义单股最终 verdict
- `stock_decision.example.json` 为 finalizer 提供一份通过 Schema 校验的完整输出样例

`prompt_flow/skill_flow_short_book.json` 继续服务 short_book。根目录下旧 `skill_flow.json` 与 `skill_flow_long_book.json` 只保留兼容和历史参考。

### `.env`

运行时默认值现在主要来自环境变量，而不是一个总控 JSON 配置。常见变量包括：

- `SIGNATURE` 或 `DEFAULT_SIGNATURE`
- `INITIAL_CASH` 或 `INIT_CASH`
- `ALLOW_BUY_EXECUTION`：默认关闭。只有显式设为 `true/1/yes/on` 时，执行层才会真实执行 `BUY` 指令；否则会自动把买入动作降级为仅保留研究结论、不自动下单。
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
- 不使用额外 `manifest.json`；Prompt 文件与生成关系由流水线代码显式指定。
