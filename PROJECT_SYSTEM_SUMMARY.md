更新日期：2026-03-14

# AI-Trader 项目系统白皮书

## 1. 顶层流程与运行方式
- **当前主入口**：项目当前主流程已经切换为 `skill-only`。日常运行顺序为：`scripts/manage_daily_data.py` → `scripts/run_daily_pipeline.py --date YYYY-MM-DD` → 本地 Agent 读取 `data/skill_runs/{date}/` → `scripts/run_post_trade.py --date YYYY-MM-DD`。
- **旧入口状态**：`main.py` 现已改为迁移提示入口；`main.sh` 与 `agent/base_agent/base_agent.py` 已从主流程中清理，不再保留实际运行能力。
- **交易结果落地**：`tools.price_tools` 提供 `get_latest_position`、`get_open_prices`、`add_no_trade_record`、`compute_total_value` 等函数，所有买卖最终写入 `data/agent_data/{signature}/position/position.jsonl` 并更新 `IF_TRADE` 标记。
- **运行前置与依赖**：`pip install -r requirements.txt` 安装依赖，`cp .env.example .env` 并填写密钥；当前主流程默认不再依赖启动 MCP 服务。

## 2. Agent 提示词、策略与上下文
- **提示词生成**（`prompts/agent_prompt.py`）：当前默认 prompt flow 已切换到 `configs/prompt_flow/skill_flow.json`，角色设定、流程、决策约束等由该文件驱动，并自动注入 `{date}`、`{date_1}`、`{positions}`、`{today_buy_price}`、`{position_costs}`、`{position_profit}` 等上下文。
- **历史总结注入**：`trade_summary.get_portfolio_historical_context` 会把 `operation_summary.json` 与最新 `portfolio_daily_summary.json` 中的要点合并成 JSON 块，作为 prompt 的“历史交易总结”输入，解决大模型“记忆断层”问题（详见 `docs/trade_summary/` 下的设计文档）。
- **投资理念文件**：`AI agent的投资理念.md` 记录了深度投资策略、10 只固定股票池、变化响应机制等文字提示，可作为 prompt flow 的补充。
- **停止信号与 JSON 提交**：所有 agent 回答必须输出指定结构的 JSON（包含 `stock_operations`、`system_risk_notes` 等字段），`prompts/agent_prompt.extract_json_from_ai_output` 用于在日志中稳健抽取 JSON。

## 3. 数据与缓存基座
- **统一入口**：`shared_data_access.SharedDataAccess.prepare_dataset(symbolInfo, as_of_date, …)` 是**唯一**被允许访问 AkShare/巨潮的路径，负责：① 调用 `ensure_symbol_data` 刷新价格、财报、股本、公告缓存；② 按 `as_of_date` 对 DataFrame 截断；③ 汇总 `FinancialDataBundle`、`PriceDataBundle`、`ShareInfo`、`DisclosureBundle`，并对 ETF/指数自动降级为“仅价格”模式。`docs/share_data_access/README.md` 详细说明了调用姿势、回测因果性与 `include_disclosures` 用法。
- **缓存注册表**：`shared_data_access/cache_registry.py` 定义 `CacheKind`（financials/prices/share_info/analysis/pe_pb_analysis/basic_info/disclosures）以及 TTL、目录结构和 `.cache_registry_meta.json` 元数据；`docs/cache/cache_registry_design.md` 给出设计原理。所有 `update_*_cached` 函数都需调用 `should_refresh` 和 `record_cache_refresh`，而 `tools` 目录下包含缓存迁移与巡检工具。
- **股本与时间守卫**：`shared_financial_utils.ShareInfoProvider` 将股本缓存放到 `data/global_cache/share_info_cache.json` 并提供 TTL、`apply_dataframe_cutoff`、`filter_financial_abstract_by_cutoff` 等时间截断工具，确保任何回测都遵守因果性。
- **Indicator Library**：`indicator_library/`（已被 `shared_data_access/indicator_library.py` 复用）统一了技术指标、收益风险、流动性、TTM 计算，包含 `schemas.py`（Pydantic 请求/响应）、`gateways.py`（DataFrame gateway）、`calculators/*`（momentum/risk/liquidity/fundamental/trend）。`IndicatorLibrary.calculate()` + `IndicatorSpec` 支持批量指标请求，所有新指标需注册在 `_build_registry` 内。
- **数据落地**：每只股票的数据均存放于 `data/{stock_name}_{symbol}/`（财经缓存、价格、analysis、pe_pb_analysis、news/announcements等），MCP 工具运行产生的日志放在 `logs/{model}/{tool}/`。

## 4. 核心分析与研究模块
- **基础指标批处理**（`basic_stock_info.py`）：`BasicStockInfoService` 会调用 `SharedDataAccess.prepare_dataset` + `IndicatorLibrary`，输出估值、财报增速、风险、流动性等字段并写入 `data/basic_info_cache/basic_info_{symbol}.json`（含历史快照）；CLI 支持 `--symbols`/`--history-days`。
- **增强估值分析**（`enhanced_pe_pb_analyzer.py`）：以 `SymbolInfo` 为核心，串联财报/股本/价格缓存、TTM EPS、PEG、相似股比较、Markdown/CSV/JSON 报告写入。重构后通用指标计算迁移至 `indicator_library.calculators`，并通过 `cache_registry` 管理输出目录。
- **股价动态总结**（`stock_price_dynamics_summarizer.py`）：围绕 `IndicatorLibrary` + `IndicatorBatchRequest` 计算 3/6/12 个月收益、夏普、相关性矩阵、MACD/RSI/MA、行业对比等信息，生成 Markdown + JSON 报告，是 `summarize_stock_price_dynamics` MCP 工具的底层引擎。
- **公告与新闻**：`news/disclosures_builder.py` 把 `SharedDataAccess` 的公告索引下载到本地 PDF/Markdown，并通过 OpenAI/Qwen 模型提取结构化 `raw_facts`、`quantitative_data`、`category` 等字段；`news/progressive_news_summarizer.py` 负责多源新闻/公告/研报收集。`docs/news/` 下的三篇设计文档详细定义了系统目标与规格。
- **财报深度研究**（`fundamental/fundamental_research.py`）：以 `SharedDataAccess` + `disclosures_builder` 提供的公告 Markdown 为输入，`FinancialReportExtractor` 下载/提取要点，再由 `FundamentalResearchAgent` 按 `DOC_EXTRACTION_PROMPT` 与 `REPORT_ANALYSIS_AGENT_PROMPT` 生成结构化研究结果，落地到 `fundamental_reports/`。`docs/fundamental_research/README.md` 描述端到端流程。

## 5. 兼容层与运行治理
- **兼容层状态**：`agent_tools/` 现在只保留少量历史导入路径兼容包装层，真实业务实现已经迁移到 `services/` 与 `core/`。项目当前不再保留 `start_mcp_services.py`、`tool_python.py`、`tool_math.py` 等旧 MCP 服务脚本。
- **工具输出**：
  - `TradeTools`：提供 `buy`/`sell`，校验输入、读取仓位、调用 `price_tools`, 并把成功交易写入 position 日志。
- `StockAnalysis`：默认开放 `analyze_stock_dynamics_and_valuation`（整合价格+估值）以及 `get_basic_stock_info`（需单测时恢复装饰器），原先的 `run_enhanced_pe_pb_analysis` / `summarize_stock_price_dynamics` 逻辑仍保留为内部函数。
  - `tool_stock_news_search.py`/`tool_macro_summary.py`/`tool_financial_report.py` 仅作为历史兼容入口，真实实现分别位于 `services/research/`。
- **统一日志**：兼容包装层和脚本入口统一通过 `core/logging.py` 与 `agent_tools/logging_utils.init_tool_logger()` 获取 `logs/{model}/{tool}/{timestamp}.log` 的结构化日志，满足“工具级独立日志 + logging 分级”规范。

## 6. 交易总结数据库与上下文
- **数据文件布局**：`trade_summary.py` 以 `data/agent_data/{signature}` 为根，维护 `stock_operations.json`（每日原始 JSON）、`operation_summary.json`（合并后的持有/买卖记录）与 `portfolio_daily_summary.json`（组合级别风险/焦点）。
- **三步流程**：
  1. `save_daily_operations(signature, ai_output_json)` 在 agent 产生最终 JSON 后写入原始表，并保证同日唯一。
  2. `process_and_merge_operations` 以股票为单位合并连续 HOLD/FLAT 序列（考虑交易日跳变），买卖则逐条保留。
  3. `get_historical_context` / `get_portfolio_historical_context` / `load_yesterday_daily_summary` 为 prompt 或风控调用提供最近 N 次操作、系统级风险提示。
- **设计文档**：`docs/trade_summary/` 下的背景需求、详细设计与数据库设计文档详细描述了“记忆压缩、token 成本控制、表结构”。

## 7. 文档、测试与开发规范
- **项目说明**：`README.md`、`AGENTS.md`（仓库指南、缓存/回测/指令/语言要求）、`PROJECT_SYSTEM_SUMMARY.md`（本文）作为快速入门材料。
- **设计文档**：`docs/cache/`、`docs/share_data_access/`、`docs/trade_summary/`、`docs/news/`、`docs/fundamental_research/`、`docs/v0.1版本总结/` 等提供各模块架构；在阅读、开发和评审时优先参考对应文档。
- **测试**：`test/test_deepseek_wrapper.py` 验证 Deepseek reasoner wrapper 会把 `reasoning_content` 正确回放；`test/test_prompt.py` 用于手动调试 prompt 输出（未来需替换为自动断言）。根据仓库指南，测试 MCP 工具函数前需移除 `@mcp.tool()` 装饰器，测试完再恢复。
- **运行规范**：所有股票标识必须使用 `SymbolInfo` + `代码.后缀` 格式，数据抓取一律通过 `SharedDataAccess`；更新分析目录前需保留 `.cache_registry_meta.json` 并清理旧输出；日志需通过统一 logger；所有脚本/工具在写 `analysis/`、`pe_pb_analysis/` 等目录前需清扫旧文件。

以上内容覆盖了 2026 年 03 月 14 日最新的代码与文档结构，后续如有重大重构，请同步更新本文件以保持团队对系统的一致认知。
