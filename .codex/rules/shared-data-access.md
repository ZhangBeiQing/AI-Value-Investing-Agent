---
paths:
  - shared_data_access/**
  - services/research/**
  - services/snapshot/**
  - basic_stock_info.py
  - enhanced_pe_pb_analyzer.py
  - stock_price_dynamics_summarizer.py
  - shared_financial_utils.py
---

# Shared Data Access Rules

本规则约束所有行情、财报、股本、公告、派生指标相关代码。

## 统一入口

- 任何需要访问 akshare、公告索引、股本、财报缓存的逻辑，都必须优先通过 `shared_data_access` 暴露的能力接入。
- 禁止在上层分析模块里临时直连 akshare，再自己落缓存。
- 如果现有 `SharedDataAccess` 不够用，先扩展 `shared_data_access/`，再让上层调用。

## 缓存与目录

- 新缓存类型必须先在 `shared_data_access/cache_registry.py` 注册，再写读逻辑。
- 不要在业务模块里散落硬编码缓存目录。
- 写 `analysis/`、`pe_pb_analysis/` 等派生目录前，应先清理旧文件，仅保留 `.cache_registry_meta.json`。

## 时间因果与回测一致性

- `update_*` 抓取函数永远面向真实当前时间抓足够长的历史窗口，不得按 `as_of_date` 缩短抓取范围。
- 复盘、回测、历史观察只允许在 `prepare_dataset` 返回结果或上层读取阶段做时间截断。
- 凡是涉及“昨日”“最近一份财报”“截至某天”的逻辑，必须明确使用绝对日期，不要混淆抓取时间与分析时间。

## SymbolInfo

- 除纯字符串格式转换外，优先传递 `SymbolInfo`。
- 新函数如果同时需要市场、代码、名称，不要拆成多个字符串参数。
- 对外暴露的入口如果先收字符串，应尽早 `parse_symbol()`。

## 设计偏好

- 指标计算尽量下沉到公共层，避免在多个 research/snapshot 模块复制同一套逻辑。
- 缓存层负责“数据准备”和“时间守卫”，研究层负责“解释”和“组装输出”。
- 如果某段逻辑已经稳定且被多个模块复用，优先抽到 `shared_data_access` 或 `indicator_library`，不要继续堆在脚本里。

## 修改后验证

- 至少验证一个真实 symbol 能走通目标路径。
- 如果改动影响主链路输入，至少再验证一次：
  - `python scripts/manage_daily_data.py`
  - 或 `python scripts/run_daily_pipeline.py --date YYYY-MM-DD --base-dir data/tmp_<name>`

