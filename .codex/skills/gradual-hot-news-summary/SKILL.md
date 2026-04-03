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

当前 skill 按 file-only 方式工作：

1. 读取本地输入文件
2. 读取最近一天 `06_hot_news_state.json`
3. 直接生成今天新的 `06_hot_news_state.json` 和 `06_hot_news_state_ops.json`

默认不把 `06_hot_news_state.json` 额外镜像到 `data/market_state/hot_news_state/`。

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

### 2. 读取主输入

严格按 [输入约定](references/input-contract.md) 读取：

1. 今日 `03_news_prompt_input.json`
2. 最近一天 `06_hot_news_state.json`
3. 最近一天宏观总结
4. 最近一天板块热点状态
5. 今日板块热度摘要 `05_board_heat_digest.json`

板块层规则：

1. 先读 `05_board_heat_digest.json`
2. 用其中的 `standard_board_names` 约束 `linked_boards`
3. 不要直接整份读取 `daily_snapshots` 或 `market_snapshots`
4. 若某个主题需要更细板块证据，再运行：

```bash
python scripts/query_board_snapshot.py --date YYYY-MM-DD --board-name "板块A" --board-name "板块B"
```

`linked_boards` 只能从 [输入约定](references/input-contract.md) 里的标准板块名清单中选。

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

同时必须判断：

1. 哪些旧主题今天仍应保留在主上下文
2. 哪些旧主题只应降级到 `cooling_themes`
3. 哪些旧主题应从今天的 `06_hot_news_state.json` 中移出

注意：

1. 被移出的主题不再出现在今天新的 `06_hot_news_state.json`
2. 但必须在 `06_hot_news_state_ops.json` 中留下完整轨迹
3. 若未来又被重新激活，可以作为新一轮主上下文主题重新进入

### 4. 必要时联网补证

如果上述主输入仍不足以支撑某个高权重主题判断，可以联网补证。

具体规则见 [事件链与风险规则](references/output-contract.md)。

对于“是否应彻底移出今天主上下文”这个判断：

1. 普通低权重主题可直接基于现有主输入判断
2. 高权重旧主题在准备移出前，建议联网补证
3. 若最近几天已无新增事实、无板块确认、无扩散影响，可移出今天主上下文

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
- `linked_boards` 必须优先使用 `05_board_heat_digest.json` 中的标准板块名
- 不要把昨天出现过的主题机械地全部延续到今天
- 不要把已结束、已证伪、已完全失去交易性的主题继续保留在今天的 `06_hot_news_state.json`
- 任何被移出今天主上下文的主题，都必须在 `06_hot_news_state_ops.json` 中写明移出日期、原因、最后一次保留日期、是否做过联网复核
