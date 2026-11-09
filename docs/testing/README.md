# 测试说明（`tests/`）

本文件说明 `tests/` 的定位、运行方式与维护约定。核心结论：**`tests/` 是长期维护的回归测试网，不是一次性调试脚本，也不是"用完即丢"的产物。**

## 为什么保留

- 每次修改代码后重复运行，用来确认没有破坏既有行为（回归检测）。
- 重构时代码结构会变，但测试固化的"行为契约"不变，是最好的安全网。
- 修 bug 时先写一条能复现的测试，再修代码，可避免同样的问题再次出现。

判断标准：一个文件只要**未来还会被重复运行来验证行为**，它就属于 `tests/`；只在当时跑一次、拿到结论就作废的，属于临时脚本，不应放进 `tests/` 或提交。

## 运行

```bash
source /home/zhangbeiqing/venv/ai_stock/bin/activate
python -m pytest tests -q
python -m pytest tests -q -k price        # 只跑名字含 price 的用例
```

- 全量（`python -m pytest tests -q`）在 2026-11-08 清理后共 **269 项，应全部通过**；耗时取决于环境。
- 测试必须可离线运行，**不允许依赖实时联网或真实交易数据**；外部依赖用 `monkeypatch`、`tmp_path` 隔离。

## 覆盖范围

| 文件 | 覆盖 |
| --- | --- |
| `test_price_normalization.py` | 多来源行情在 `_finalize_price_frame` 的 canonical 列/单位归一 |
| `test_price_incremental_refresh.py` | 价格缓存增量刷新（重叠窗口、去重合并） |
| `test_api_throttle.py` | `api_call_with_delay` 的跨线程最小间隔节流 |
| `test_etf_data_access.py` | ETF/港股杠杆产品识别与"仅刷价格"路径 |
| `test_logging_run_dir.py` | 运行日志按「日期 + 流程」聚合与保留策略 |
| `test_financial_report_context.py` / `test_financial_report_skill.py` | 财报深度研究的上下文隔离与 skill 编排 |
| `test_debate_pipeline.py` | fixed_tracked 多 Agent 辩论编排 |
| `test_weekly_scoring.py` | 周度固定池评分：派单、日期隔离、JSON 契约校验与等权排名 |
| `test_fixed_tracked_backtest.py` | 隔离回测：universe、账本、D+1 成交、净值 |
| `test_industry_research.py` | 月度产业雷达与主题深研 |
| `test_hot_news_digest.py` | 渐进式新闻总结的轻量 digest 派生 |
| `test_trade_memory_context.py` | 交易历史记忆投影与 prompt 上下文 |
| `test_analysis_index.py` / `post_trade_pipeline_test.py` / `cache_initialization_test.py` | 分析索引、交易后处理串联、缓存初始化 |
| `test_recommendation_dashboard.py` | 推荐看板：BUY 信号表现、持仓聚合、研究包选择（`unittest` 风格） |
| `test_document_conversion.py` / `test_network_timeouts.py` | PDF→Markdown 转换、网络超时设置 |
| `test_pipeline_refresh_defaults.py` / `test_prompt_profiles.py` | 刷新默认参数、prompt 分层职责边界 |
| `test_price_cache_freshness.py` | 价格缓存 TTL、收盘屏障与过期行情源回退 |
| `test_us_stock_support.py` / `test_us_financial_documents.py` | 美股财年、币种、公告归一、原文转换预算与旧截断缓存重建 |
| `test_disclosure_cutoff.py` / `test_ensure_financial_reports.py` | 公告可见性截断、预告与正式财报区分、研究前财报完整性检查 |
| `test_position_symbols.py` / `test_prepare_single_stock.py` / `test_tracked_only_scope.py` | 持仓股票识别、单股命令参数与固定池刷新范围 |
| `test_enhanced_pe_pb_analyzer.py` / `test_magic_formula.py` | PB 权益口径、历史估值截断、EBIT 与资本回报率排序 |
| `test_bailian_search_bridge.py` | 百炼搜索桥接配置、响应文本与空查询校验 |
| `test_news_board_heat_paused.py` | 暂停开关阻止新闻和板块研究、下游不再要求已暂停产物 |

## 2026-11-08 清理结论

- 检查了 `tests/` 下全部 33 个 Python 文件：都有仍存在的业务目标和可重复执行的断言，未发现应整文件删除的一次性调试脚本。
- 删除 `test_us_financial_documents.py` 中重复的美股 market 识别用例；`test_us_stock_support.py::test_parse_us_symbol_resolves_metadata` 已覆盖相同行为。
- 修复价格缓存测试使用固定缓存日期、却使用真实当前时间判断 TTL 的问题；缓存测试统一固定时钟，并确认收盘屏障测试是在 TTL 尚未过期时触发刷新。
- 节流测试显式隔离最小间隔环境变量，避免开发机配置影响默认值与并发间隔断言。
- 缓存初始化测试补齐 `REPORT_DATE`、固定期望报告期，并拒绝意外财报抓取，避免只验证股本初始化时触发真实网络请求。
- 新闻主题与板块热度目前只是暂停，功能入口与恢复开关仍保留，因此继续保留恢复态与暂停态测试；回测、交易后处理及看板也仍有可执行入口，不因不属于默认日常链路而删除其测试。

## 约定

- 文件名 `test_*.py`；函数名 `test_<what>_<condition>_<expected>`。
- 结构遵循 Arrange → Act → Assert。
- 断言应针对**稳定契约**（行为、字段、单位），不要硬编码随时会改的文案口号；确需断言文案时，尽量选 policy 中稳定的锚点句。
- 优先使用 `tmp_path`、`monkeypatch` 构造隔离环境，不写真实 `data/`、`logs/`。

## 新增测试

1. 复现问题：先写一条会失败的测试（红）。
2. 修代码：让测试通过（绿）。
3. 保持小步：一次只验证一个行为，必要时拆分用例。

## 维护说明

- `tests/` 已纳入 git 跟踪（`.gitignore` 不再忽略 `/tests/`）；`__pycache__/`、`.pytest_cache/` 仍被忽略。
- 调整测试契约（产物结构、评分口径、字段）时，应在同一提交内同步更新测试，避免测试与代码脱节。
