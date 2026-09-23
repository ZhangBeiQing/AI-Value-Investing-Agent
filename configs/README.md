# 配置文件

此目录服务于本地 `skill-only` 工作流。历史上的 `default_config.json` 已随旧 MCP / Agent 运行时一并移除。

## 当前有效配置

### `stock_pool.py`

定义每日数据刷新与研究流水线使用的股票池（`TRACKED_A_STOCKS` 等）。

### `prompt_flow/`

- `fixed_tracked/investment_policy.md`：全部 fixed_tracked Agent 共用的核心投资策略单一来源
- `fixed_tracked/main_policy.md`：主 Agent Prompt；`investment_policy.md` + `main_policy.md` 生成 `03_agent_input.md`
- `fixed_tracked/stock_analysis_policy.md`：个股辩论角色研究方法；`investment_policy.md` + `configs/research/web_research_policy.md` + 本文件 生成 `03_stock_analysis_input.md`
- `fixed_tracked/stock_decision.schema.json` / `stock_decision.example.json`：单股最终 verdict 的 schema 与完整样例
- `skill_flow.json`：默认 Prompt flow，是 `services/prompting/agent_prompt.py` 的 `DEFAULT_PROMPT_CONFIG`，可用环境变量 `PROMPT_FLOW_CONFIG` 覆盖

### `research/`

财报 / 产业 / 估值相关固定研究规则与输出 schema（`web_research_policy.md`、`financial_report_output_schema.md`、`company_valuation_framework.md` 等），供财报深研 skill 引用。

### `selection_system/factor_scoring.yaml`

量化初筛评分规则（按 `stock_type ∈ {growth, cyclical, special}` 配置门槛与权重）。

### `industry_research/theme_registry.yaml`

月度行业研究的已批准主题登记。

### `cache_policy.json`

缓存 TTL 等策略，由 `shared_data_access/cache_registry.py` 读取。

### 根目录 `.env`

运行时默认值来自环境变量，而非总控 JSON：

- `SIGNATURE` 或 `DEFAULT_SIGNATURE`
- `INITIAL_CASH` 或 `INIT_CASH`
- `ALLOW_BUY_EXECUTION`：默认关闭。只有显式设为 `true/1/yes/on` 时，执行层才会真实执行 `BUY`；否则降级为仅保留研究结论、不自动下单。
- 各模型 API Key 与 Base URL

## 运行约定

日常主流程（详见根 `README.md`）：

1. 数据准备：crontab 周一至周五 21:03 自动运行，或说"开始今天的数据准备" → 产出 `data/skill_runs/{date}/` 下的 `01-04`
2. 固定股池交易 skill（新开 opencode 会话）→ `05_decision.json`
3. 人工确认后 `python scripts/run_post_trade.py --date YYYY-MM-DD` → `06-08`

## 说明

- 新的项目默认值优先使用环境变量，或拆分为职责单一的小配置文件。
- 除非有明确运维需求，不再恢复"一个 JSON 管全部运行时参数"的旧模式。
- 不使用额外 `manifest.json`；Prompt 文件与生成关系由流水线代码显式指定。
