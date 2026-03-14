---
name: extend-shared-data-access
description: Guide for adding a new cached dataset, external data source, or derived indicator through shared_data_access.
---

# Extend Shared Data Access

通过 `shared_data_access` 为项目新增一个统一的数据能力，而不是在上层模块里临时抓数据。

## When to Use

- 新增行情、财报、股本、公告、预测、一致预期等缓存
- 某个研究模块缺数据，准备扩展 `SharedDataAccess`
- 需要让多个模块共享同一份抓取和时间截断逻辑

## Step 1：确认是否真的要扩缓存层

先回答三个问题：

1. 这个数据是否会被两个以上模块复用？
2. 这个数据是否需要 TTL、目录结构、历史保留？
3. 这个数据是否受 `as_of_date` / 回测因果性约束？

如果答案大多是“是”，应下沉到 `shared_data_access`。

## Step 2：定义缓存契约

先定义：

- 缓存目录
- 文件格式
- TTL
- 必需文件 / 可选文件
- 对应 symbol 维度还是全局维度

优先在 `shared_data_access/cache_registry.py` 里注册，而不是先写散乱路径。

## Step 3：实现抓取与读取分离

- 抓取阶段：面向真实当前时间取足历史窗口
- 读取阶段：按 `as_of_date` 或 `today_time` 做时间截断
- 不要把这两层混在一起

如果涉及股票标识：

- 入口尽早转成 `SymbolInfo`

## Step 4：把能力接到上层

新增能力后，再让上层模块复用：

- `services/research/`
- `services/snapshot/`
- `basic_stock_info.py`
- 兼容层 `agent_tools/`

不要跳过共享层，直接在上层复制一套抓取逻辑。

## Step 5：验证

最少做两类验证：

1. 目标 symbol 能成功落缓存
2. 上层至少一个消费者能读到正确数据

建议命令：

```bash
python scripts/manage_daily_data.py
python scripts/run_daily_pipeline.py --date YYYY-MM-DD --base-dir data/tmp_<name>
```

## Common Mistakes

- 直接在 research 层调用 akshare
- 用 `as_of_date` 限制抓取窗口
- 新目录没进 `cache_registry`
- 同一数据被多个模块重复抓取
- 忽略 `SymbolInfo`，导致市场后缀丢失

