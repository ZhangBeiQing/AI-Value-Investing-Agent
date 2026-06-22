# 输入约定

## 默认主输入

这个 skill 默认有四个主输入加一个股票宇宙用于背景填充，
1. 今日 `03_news_prompt_input.json`(这个文件很长，有今天所有的重要消息，你必须完整读完；若文件过长，必须分段顺序读到末尾)
2. 昨天或之前最近一天的 `06_hot_news_state.json`
3. 最近一天宏观总结
4. 今日板块信息层
5. 当前股票宇宙 `data/universe/master_universe.json`

这些输入均由 `python scripts/refresh_all_for_date.py` 预先生成，skill 本体只读、不再触发抓取。
若发现任一文件缺失，直接提示用户回去跑 `refresh_all_for_date.py`，不要自己跑补齐脚本。

## 读取顺序

### 1. 今日新闻输入

读取：

- `data/selection_runs/YYYY-MM-DD/03_news_prompt_input.json`(这个文件很长，有今天所有的重要消息，你必须完整读完；若文件过长，必须分段顺序读到末尾)

### 2. 最近一天主题状态

优先读取：

- `run_date` 之前最近一日的 `data/selection_runs/<date>/06_hot_news_state.json`

若不存在：

- 视为首次运行或缺少历史主题状态
- 允许从空历史状态启动
- 必须在 `source_status` 中记录 `missing_previous_state_bootstrap`

### 3. 最近一天宏观总结

读取：

- `data/macro_economy/` 下日期早于等于今日的最近一份文件

宏观总结不是补充材料，而是默认校准器。

### 4. 今日板块信息层

这一层由两份文件加一个接口组成，应合并理解，不要当成并列系统：

1. `data/selection_runs/YYYY-MM-DD/05_board_heat_state.json`
2. `data/selection_runs/YYYY-MM-DD/05_board_heat_digest.json`
3. `query_board_snapshot.py` 接口

各自职责：

1. `05_board_heat_state.json`
   - 今天最热板块的研究结果
   - 适合快速看“今天市场重点在交易哪些板块”
   - 不覆盖全部板块
2. `05_board_heat_digest.json`
   - 近期板块热点的超级简单概览
   - 给 AI 提供全局板块导航
3、`query_board_snapshot.py`
   - 负责根据需要，如历史主题总结里跟踪的板块，宏观新闻下可能受影响的版本，感兴趣的板块等不在
     不在05_board_heat_state的板块细节中，则可以调用该接口，查看需要跟踪的板块的详细信息

推荐读取顺序：

1. 先读 `05_board_heat_digest.json`
2. 再看 `05_board_heat_state.json` 里的当日热点板块
3. 按需调用query_board_snapshot接口查询板块详细信息

query_board_snapshot 使用方法：

```bash
python scripts/query_board_snapshot.py --date YYYY-MM-DD --board-name "板块A" --board-name "板块B"
```

### 5. 当前股票宇宙

读取：

- `data/universe/master_universe.json`

用途：

1. 判断哪些候选可以保留在 `outside_universe_names_to_check`
2. 决定 `universe_expansion_hints` 里哪些名字仍需要提示后续补充

要求：

1. 不要凭印象假设股票已经在宇宙里，必须显式对照这个文件
2. 若主题里明显值得跟踪的核心股票当前不在宇宙里，可以在总结中保留到 `outside_universe_names_to_check` 或 `universe_expansion_hints`
3. 若用户本轮已经先更新了 `master_universe.json`，生成当天 `06_hot_news_state.json` 时应优先使用更新后的宇宙，而不是沿用旧 run 的股票映射

## 何时允许联网补证

如果以上主输入仍不足以支撑主题判断，可以联网补证。

适合联网补证的主题：

- 战争、关税、制裁、央行、重大政策
- 对板块方向影响大的产业事件
- 可能驱动宇宙外扩的高价值主题

不适合联网补证的内容：

- 普通低价值公司公告
- 仅影响单一个股、没有外溢性的日常事项
