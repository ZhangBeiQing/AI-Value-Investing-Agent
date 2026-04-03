---
name: gradual-hot-news-summary
description: >
  用于生成渐进式热点新闻主题总结。
  当用户说“更新今天的渐进式新闻总结”、“更新今日热点主题总结”或类似请求时使用。用于生成可被后续本地选股 agent 直接继承的主题级研究记忆。
---

# Gradual Hot News Summary

## 何时使用

- 用户要更新当天的渐进式新闻总结
- 用户要把“今日新闻 + 最近一天主题状态 + 最近一天宏观总结 + 最近一天板块热点状态”合并成新的主题级研究记忆

## 输出路径

- 主输出：`data/selection_runs/YYYY-MM-DD/06_hot_news_state.json`
- 操作日志：`data/selection_runs/YYYY-MM-DD/06_hot_news_state_ops.json`
- 最新镜像：`data/market_state/hot_news_state/latest.json`
- 历史镜像：`data/market_state/hot_news_state/YYYY-MM-DD.json`

## 先读什么

1. 读 [概览](references/overview.md)
   - 确认这个能力在整个选股系统中的职责边界
2. 读 [输入约定](references/input-contract.md)
   - 开始执行前必读
3. 读 [输出契约](references/output-contract.md)
   - 开始生成 `06_hot_news_state.json` 前必读

## 固定流程

### 1. 准备输入

先激活虚拟环境：

```bash
source /home/zhangbeiqing/venv/ai_stock/bin/activate
```

运行新闻链：

```bash
python scripts/manage_selection_system.py --base-dir data run-news --date YYYY-MM-DD
```

运行当天板块热点状态：

```bash
python scripts/manage_selection_system.py --base-dir data build-board-heat-state --date YYYY-MM-DD
```

### 2. 读取四层主输入

严格按 [输入约定](references/input-contract.md) 读取：

1. 今日 `03_news_prompt_input.json`
2. 最近一天 `06_hot_news_state.json`
3. 最近一天宏观总结
4. 最近一天板块热点状态

### 3. 生成主题级研究记忆

不是做新闻摘要，而是更新主题。

每个主题至少要回答：

1. 为什么进入主上下文
2. 过去几天怎么演变到今天
3. 今天新增了什么
4. 当前市场在交易什么，不交易什么
5. 接下来可能怎么发展
6. 有哪些证伪或反转风险
7. 明天应该跟踪什么

### 4. 必要时联网补证

如果四层主输入仍不足以支撑某个高权重主题判断，可以联网补证。

具体规则见 [事件链与风险规则](references/output-contract.md)。

### 5. 按输出契约落盘

严格按 [输出契约](references/output-contract.md) 生成：

- `06_hot_news_state.json`
- `06_hot_news_state_ops.json`

## 强制要求

- 不要重写宏观总结
- 不要替代板块热度层
- 不要直接做最终选股结论
- 不要把全部新闻直接挂到股票宇宙上
- 不要省略 `history_anchor / today_update / current_state / expected_duration / forward_paths / scenario_tree / key_risks / next_day_watchlist`

