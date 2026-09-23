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
python -m pytest tests -q                 # 约 30 秒
python -m pytest tests -q -k price        # 只跑名字含 price 的用例
```

- 全量（`python -m pytest tests -q`）当前约 **135 项，应全部通过**。
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
| `test_fixed_tracked_backtest.py` | 隔离回测：universe、账本、D+1 成交、净值 |
| `test_industry_research.py` | 月度产业雷达与主题深研 |
| `test_hot_news_digest.py` | 渐进式新闻总结的轻量 digest 派生 |
| `test_trade_memory_context.py` | 交易历史记忆投影与 prompt 上下文 |
| `test_analysis_index.py` / `post_trade_pipeline_test.py` / `cache_initialization_test.py` | 分析索引、交易后处理串联、缓存初始化 |
| `test_recommendation_dashboard.py` | 推荐看板：BUY 信号表现、持仓聚合、研究包选择（`unittest` 风格） |
| `test_document_conversion.py` / `test_network_timeouts.py` | PDF→Markdown 转换、网络超时设置 |
| `test_pipeline_refresh_defaults.py` / `test_prompt_profiles.py` | 刷新默认参数、prompt 分层职责边界 |

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
