# 数据定时更新管理设计

## 目标
每天 20:00 统一运行一条数据刷新流水线，确保 AI agent 依赖的以下数据始终为最新：

1. **行情/财务/公告基础数据**：通过 `shared_data_access.data_access.prepare_dataset()` 的 `ensure_symbol_data()` 链路刷新（包含价格、财报、公告链接等缓存，遵循统一 TTL 与缓存策略）。
2. **公告新闻审计库**：通过 `news/disclosures_builder.py` 抓取 PDF 并调用 `qwen-doc-turbo` 提取正文，再使用 `AUDIT_MODEL_BASE_URL`/`AUDIT_MODEL_API_KEY` 配置的审计模型对摘要进行复核，更新 `news/news_audited.json`。
3. **基础面快照**：运行 `basic_stock_info.py` 生成/刷新每日 `basic_info_*.json`，供 `02_basic_snapshot_payload.json` 使用。

## 设计思路

### 1. 统一入口脚本
新增 `scripts/manage_daily_data.py`，按以下步骤顺序执行：

1. **环境准备**：优先读取 CLI 参数与环境变量，以确定股票池、日期和签名（默认 `deepseek-reasoner`）。脚本允许覆盖参数（如 `--date 2025-11-03`、`--signature foo`）。
2. **刷新行情/财务缓存**：
   - 读取 `configs/stock_pool.py` 的 `TRACKED_A_STOCKS` 列表。
   - 对每个 symbol 调用 `prepare_dataset(symbolInfo, as_of_date=target_date)`，以确保当天 20:00 前的最新缓存可供 AI 使用。
3. **执行公告新闻构建**：调用 `python news/disclosures_builder.py --all --model qwen-doc-turbo`（或逐股 `--symbol`）并通过环境变量传入审计模型配置。
4. **生成基础信息**：运行 `python basic_stock_info.py --symbols ... --today-time <target_date> --get-look-back-days 0 --max-workers <n>`（使用多线程参数）。
5. **结果汇总**：将每个子流程的输出汇总到 `logs/main_scripts/ManageDailyData/` 下的日志文件，并同步写状态报告到 `latest_status.json`。

### 2. 任务编排

- **cron**：如需定时任务，直接调用 `python scripts/manage_daily_data.py --date $(date +%F)`，并让系统层收集 stdout/stderr。
- **可选手动触发**：脚本支持 `--force-refresh`，用于开发调试时跳过缓存 TTL。

### 3. 错误处理

- 子流程采用统一封装的子进程执行；若失败，立即记录失败状态并退出，返回非零码。
- 每个步骤写入 `status['steps']`（成功/失败时间、耗时、日志路径），便于 UI 或后续排查。

### 4. 目录约定

- `logs/main_scripts/ManageDailyData/`：写组件日志与 `merged.log`，并附加 `latest_status.json`。
- `data/cache_registry/` 原有结构不变，由 `prepare_dataset` 管理。

### 5. 配置/扩展

- `scripts/manage_daily_data.py` 当前以股票池配置、CLI 参数和环境变量为主；可通过 CLI 覆盖 `--signature`、`--symbols-file`、`--date`、`--force-refresh`。
- 对于公告审计模型，优先使用 `.env` 中的 `AUDIT_MODEL_*`，若缺失则抛错提醒配置。

### 6. 实现要点

1. `argparse`：解析日期（默认 `datetime.now().date()`）、signature、symbols 文件、强制刷新选项。
2. `run_step(name, cmd, env=None)`：复用封装的子进程执行工具，记录开始/结束时间与日志路径。
3. `prepare_dataset` 入口：遍历 `TRACKED_A_STOCKS` 并调用 `SharedDataAccess.prepare_dataset(symbolInfo, as_of_date=date_str, force_refresh=force_flag)`。
4. 公告与基础信息流程均通过现有脚本 CLI 完成，降低耦合。
5. 运行结束写入状态 JSON（含成功步骤列表、失败原因、耗时统计）。

该方案通过一个脚本串联所有数据刷新链路，便于定时调度与日志管理，同时保留原有模块的分工，降低改动风险。
