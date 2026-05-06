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

1. 判断某个主题里的股票应写入 `linked_symbols_in_universe`
2. 判断哪些候选只能保留在 `outside_universe_names_to_check`
3. 决定 `universe_expansion_hints` 里哪些名字仍需要提示后续补充

要求：

1. 不要凭印象假设股票已经在宇宙里，必须显式对照这个文件
2. `linked_symbols_in_universe` 只能写当前 `master_universe.json` 中已经存在的 symbol
3. 若主题里明显值得跟踪的核心股票当前不在宇宙里，可以在总结中保留到 `outside_universe_names_to_check` 或 `universe_expansion_hints`
4. 若用户本轮已经先更新了 `master_universe.json`，生成当天 `06_hot_news_state.json` 时应优先使用更新后的宇宙，而不是沿用旧 run 的股票映射

## `linked_boards` 标准板块名清单

`linked_boards` 只能从下面清单中选择，不要自由发挥，不要写别名。

以下清单来自 akshare 的 `stock_board_industry_name_ths()`，当前共 `90` 个标准板块名：

`IT服务`、`专用设备`、`中药`、`互联网电商`、`保险`、`元件`、`光伏设备`、`光学光电子`、`公路铁路运输`、`其他电子`、`其他电源设备`、`其他社会服务`、`养殖业`、`军工电子`、`军工装备`、`农产品加工`、`农化制品`、`包装印刷`、`化学制品`、`化学制药`、`化学原料`、`化学纤维`、`医疗器械`、`医疗服务`、`医药商业`、`半导体`、`厨卫电器`、`塑料制品`、`多元金融`、`家居用品`、`小家电`、`小金属`、`工业金属`、`工程机械`、`建筑材料`、`建筑装饰`、`影视院线`、`房地产`、`教育`、`文化传媒`、`旅游及酒店`、`服装家纺`、`机场航运`、`橡胶制品`、`汽车整车`、`汽车服务及其他`、`汽车零部件`、`油气开采及服务`、`消费电子`、`港口航运`、`游戏`、`煤炭开采加工`、`燃气`、`物流`、`环保设备`、`环境治理`、`生物制品`、`电力`、`电子化学品`、`电机`、`电池`、`电网设备`、`白色家电`、`白酒`、`石油加工贸易`、`种植业与林业`、`纺织制造`、`综合`、`美容护理`、`能源金属`、`自动化设备`、`计算机设备`、`证券`、`贵金属`、`贸易`、`轨交设备`、`软件开发`、`通信服务`、`通信设备`、`通用设备`、`造纸`、`金属新材料`、`钢铁`、`银行`、`零售`、`非金属材料`、`风电设备`、`食品加工制造`、`饮料制造`、`黑色家电`

## 何时允许联网补证

如果以上主输入仍不足以支撑主题判断，可以联网补证。

适合联网补证的主题：

- 战争、关税、制裁、央行、重大政策
- 对板块方向影响大的产业事件
- 可能驱动宇宙外扩的高价值主题

不适合联网补证的内容：

- 普通低价值公司公告
- 仅影响单一个股、没有外溢性的日常事项
