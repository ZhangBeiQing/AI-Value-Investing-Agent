# 输入约定

## 默认主输入

这个 skill 默认有四个主输入，缺一不可；如果缺失，必须在输出里明确记录缺失项。

1. 今日 `03_news_prompt_input.json`
2. 昨天或之前最近一天的 `06_hot_news_state.json`
3. 最近一天宏观总结
4. 最近一天板块热点状态

## 读取顺序

### 1. 今日新闻输入

读取：

- `data/selection_runs/YYYY-MM-DD/03_news_prompt_input.json`

### 2. 最近一天主题状态

优先读取：

- 最近一日 `data/selection_runs/YYYY-MM-DD/06_hot_news_state.json`

若不存在，再退回：

- `data/market_state/hot_news_state/latest.json`

### 3. 最近一天宏观总结

读取：

- `data/macro_economy/` 下日期早于等于今日的最近一份文件

宏观总结不是补充材料，而是默认校准器。

### 4. 今天板块热点状态

优先读取：

- `data/selection_runs/YYYY-MM-DD/05_board_heat_state.json`

若今日不存在，再退回：

- `data/market_state/board_heat_state/latest.json`

板块热点状态不是可选附件，而是主题是否被市场确认的重要证据。

## 何时允许联网补证

如果以上四层输入仍不足以支撑主题判断，可以联网补证。

适合联网补证的主题：

- 战争、关税、制裁、央行、重大政策
- 对板块方向影响大的产业事件
- 可能驱动宇宙外扩的高价值主题

不适合联网补证的内容：

- 普通低价值公司公告
- 仅影响单一个股、没有外溢性的日常事项
