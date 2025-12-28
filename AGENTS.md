# 仓库指南

# environment
we are 
we are in windows WSL, so any resources that paste into claude code chatbox, we transform its path to WSL path
ex, for "C:\temp\a.jpg", it will be transformed to "/nt/c/temp/a.jpg"

## 项目结构与模块组织
交易入口位于 `main.py`，具体的智能体实现保存在 `agent/` 目录。共享工具和 MCP 集成分别放在 `agent_tools/` 与 `tools/`。提示词和竞赛配置位于 `prompts/` 与 `configs/`，默认配置文件是 `configs/default_config.json`。历史行情数据和生成的智能体日志分别写入 `data/` 与 `logs/`，而仪表盘文档存放在 `docs/`。自动化检查在 `test/` 目录下，遵循 `test_*.py` 的文件命名约定。`tmp_stock_analy_code/` 目录包含已迁移的 AI LLM 股票分析工具包，后续将重构为 MCP 工具，详情请参见该目录下的 `README.md`。

### 数据访问/缓存规范（必须遵守）
- **统一入口**：任何需要调用 akshare 或其他外部行情、财报、股本接口的逻辑，都必须通过 `shared_data_access` 提供的 API（核心为 `SharedDataAccess.prepare_dataset()` / `ensure_symbol_data()`）。禁止在 analyzer、agent 或工具中直接访问 akshare。
- **缓存体系**：`shared_data_access/cache_registry.py` 已定义全部缓存目录与 TTL；若缺少某类数据，应先在 `shared_data_access` 中新增加载逻辑（例如扩展 `update_financial_data_cached`、`update_share_info_cached`），再由上层复用，避免重复代码和脏数据。
- **回测兼容抓取**：所有 `update_*` 函数（价格、财报、股本、公告等）永远面向“当前真实时间”抓取足够长的历史窗口（例如价格默认 1800 天、公告默认 2 年≈730 天），不得根据回测 `as_of_date` 裁剪抓取范围。回测时只能在 `prepare_dataset` 读取阶段按 `as_of_date` 做时间截断，确保既拥有完整缓存又严格遵守因果性。
- **SymbolInfo 传递**：涉及股票标识的函数和类，除非纯字符串处理，否则一律传递 `SymbolInfo`（通过 `parse_symbol` 解析）。这样可确保市场、代码、名称一致，并复用 `SymbolInfo` 内置的格式化与属性方法。
- **输出/分析目录**：脚本在写入 `analysis/`、`pe_pb_analysis/` 等结果目录前应先清理旧文件，仅保留 `.cache_registry_meta.json`，防止缓存越堆越多（参考 `stock_price_dynamics_summarizer.py` 与 `enhanced_pe_pb_analyzer.py` 的实现）。

## 构建、测试与开发命令
- `pip install -r requirements.txt` — 安装智能体及工具所需的全部 Python 依赖。
- `cp .env.example .env` 并填写密钥 — 在任何运行前完成，确保不要将密钥提交到仓库。
- `python main.py` 或 `python main.py configs/sample.json` — 使用默认或自定义场景启动对战。
- `python main.sh` — 执行完整流水线（数据刷新、MCP 服务、智能体、文档服务）。

## 代码风格与命名规范
Python 代码统一使用 4 个空格缩进，变量与函数采用具描述性的 `snake_case`，类使用 `CapWords`。每个模块应暴露一个清晰的入口函数或类。当行为复杂时为函数/类添加文档字符串和类型注解，尤其是跨智能体接口或工具适配器的场景。优先使用显式导入，并将配置默认值保存在 JSON 或 `.env` 中，而不是硬编码常量。

## 测试指南
对于mcp tool函数的测试，测试前请去掉@mcp.tool()装饰器，然后可以直接调用这个函数测试功能是否生效，测试完后再加把@mcp.tool()装饰器加回来。每次重大修改修改后都应该进行
充分测试，确保修改完全准确符合预期才算结束

## 提交与合并请求规范
采用改进的 Conventional Commit 风格（如 `feat`, `fix`, `chore`, `docs`），示例：`feat(Trading Tool): ...`。提交信息保持祈使语气，并聚焦单一变更。Pull Request 需概述行为变化、注明受影响的配置或密钥、关联追踪 issue，并在修改仪表盘图表（`docs/`）时附带日志或截图。若需要更新运行环境，也请在 PR 中说明，方便审阅者复现。

## 智能体与服务运维
保持 `.env` 中的 API 凭据和运行路径（如 `RUNTIME_ENV_PATH`）同步。调用智能体前先运行 `python agent_tools/start_mcp_services.py` 启动 MCP 服务；新增工具后请重启服务以加载最新变更。临时输出请存放在 `logs/` 或 `data/tmp/`，避免污染源码目录。

## 语言偏好
请始终使用简体中文回复用户的所有问题和请求。
