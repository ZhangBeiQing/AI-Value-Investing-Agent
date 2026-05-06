# 输出契约

## 1. 输出目标

输出不是“新闻列表”，而是“主题级研究记忆”。

一个明天才上线的全新 agent，只读今天的 `06_hot_news_state.json`，也应该能够理解：

1. 这个主题为什么进入主上下文
2. 它过去几天怎么发展到今天
3. 今天发生了什么新增变化
4. 当前市场到底在交易这个主题的什么部分
5. 明天还要继续跟踪什么
6. 它关联哪些宏观主题、板块和股票
7. 是否值得扩展股票宇宙

## 2. 顶层结构

```json
{
  "run_date": "2026-04-03",
  "market_regime_bridge": "一句话说明今天宏观变化如何映射到可交易叙事",
  "active_themes": [],
  "cooling_themes": [],
  "new_themes": [],
  "universe_expansion_hints": [],
  "summary": {}
}
```
## 3. 输出的JSON结构介绍

```json
{
  "run_date": "2026-04-03",
  "market_regime_bridge": "一句话说明今天宏观变化如何映射到可交易叙事",
  "active_themes": [
    {
      "theme_id": "theme_oil_hormuz",
      "theme_name": "油价上行与霍尔木兹风险",
      "status": "active",
      "strength": "strengthening",
      "history_anchor": "这个主题进入主上下文的历史锚点与过去几天的核心演变线。",
      "today_update": "今天新增了哪些关键新闻、谁说了什么、什么关键事件发生了、相比昨天哪里变了。",
      "current_state": "截至今天收盘/盘后，这个主题目前是什么状态，市场在交易什么，不交易什么。",
      "expected_duration": {
        "already_running_for": "已持续4个交易日",
        "base_case": "若无超预期停火，未来1-3个交易日仍可能维持高敏感度",
        "decay_signals": ["霍尔木兹恢复通行", "能源设施未再遇袭", "油价高位回落"]
      },
      "forward_paths": [
        "若霍尔木兹恢复通行，主题可能快速降温，油运和油气溢价回吐。",
        "若能源设施继续受袭，油气/航运/军工可能继续强化。",
        "若冲突维持僵持但不扩大，主题可能仍在，但板块内部会先分化。"
      ],
      "scenario_tree": [
        {
          "scenario": "冲突继续升级",
          "probability_band": "medium_to_high",
          "trigger_signals": ["霍尔木兹仍未恢复通行", "能源设施继续遭袭", "美方继续强硬表态"],
          "market_impact": "油气、航运、军工继续强化"
        },
        {
          "scenario": "冲突高位僵持",
          "probability_band": "medium",
          "trigger_signals": ["双方继续表态强硬但无新增升级", "油价维持高位震荡"],
          "market_impact": "主题延续但边际钝化，板块内部先分化"
        },
        {
          "scenario": "快速降温或停火",
          "probability_band": "low_to_medium",
          "trigger_signals": ["停火谈判确认", "霍尔木兹恢复通行", "特朗普或伊朗明显降温表态"],
          "market_impact": "油运、油气、军工溢价快速回吐"
        }
      ],
      "key_risks": [
        {
          "risk": "特朗普或伊朗在政治压力下快速转向谈判",
          "probability_band": "low_to_medium",
          "why_it_matters": "会直接削弱油价和航运风险溢价"
        },
        {
          "risk": "市场已提前过度交易战争风险",
          "probability_band": "medium",
          "why_it_matters": "即便事件未结束，相关高弹性股票也可能先回撤"
        }
      ],
      "why_it_matters": "影响油气、航运、军工、输入性通胀",
      "linked_macro_topics": ["中东冲突", "油价", "Fed更难转鸽"],
      "linked_boards": ["油气开采及服务", "港口航运", "军工装备"],
      "linked_symbols_in_universe": ["600000.SH", "000001.SZ"],
      "outside_universe_names_to_check": ["某油运股", "某军工股"],
      "search_trigger": "若板块继续强化，应搜索宇宙外龙头/弹性股",
      "evidence_news_ids": ["N001", "N015", "N021"],
      "key_events": [
        {
          "event_date": "2026-04-01",
          "importance": "high",
          "event": "特朗普改口转强硬",
          "impact": "市场重新上修油价和航运风险溢价"
        }
      ],
      "next_day_watchlist": [
        "跟踪霍尔木兹是否恢复通行",
        "跟踪布油是否继续站稳高位",
        "跟踪油运和军工是否继续扩散"
      ]
    }
  ],
  "cooling_themes": [],
  "new_themes": [],
  "universe_expansion_hints": []
}
```

## 4. 主题字段说明

### 4.1 `history_anchor`

用途：

1. 提供一个历史锚点，详细讲述主题演变的开始，发展，截止今天前的最新进展的事件因果链，详细的把前因后果，主要事件链的发展等讲清楚
2. 说明这个主题为什么值得长期保留在上下文里,

应包含：

1. 主题最初进入主上下文的原因
2. 最近几天最关键的变化链
3. 当前判断的历史依据

### 4.2 `today_update`

用途：

1. 只描述今天相对昨天新增了什么

应包含：

1. 关键新闻
2. 新表态
3. 关键事件
4. 资金或板块层面的新确认

### 4.3 `current_state`

用途：

1. 把“历史锚点 + 今日变化”压缩成当前状态结论

应回答：

1. 市场今天到底在交易这个主题的什么部分
2. 这条线是强化、分化、衰减还是等待确认

### 4.4 `forward_paths`

用途：

1. 描述未来 1-5 个交易日可能出现的主要演化路径
2. 帮助后续 agent 理解“这个主题不是确定性的，而是有条件分支的”

注意：

1. 不是宏观长周期预测
2. 只写和选股、板块、市场风格相关的近端路径
3. 不能只写单向乐观路径，必须同时覆盖延续、钝化、反转等主要分支

### 4.5 `expected_duration`

用途：

1. 提醒 agent 这条主题已经持续了多久
2. 帮助判断它目前处于“刚启动 / 中段强化 / 高位拥挤 / 退潮前夜”的哪个阶段

建议包含：

1. `already_running_for`
2. `base_case`
3. `decay_signals`

### 4.6 `scenario_tree`

用途：

1. 把主题未来的主要情景分支结构化
2. 防止 agent 把 `active_theme` 错当成确定性结论

建议每个情景包含：

1. `scenario`
2. `probability_band`
3. `trigger_signals`
4. `market_impact`

概率写法建议：

1. 使用 `high / medium / low / low_to_medium / medium_to_high`
2. 或使用粗区间，如 `20-40%`
3. 不建议写过度精确的点概率

### 4.7 `key_risks`

用途：

1. 显式提醒后续 agent：这个主题可能如何被证伪、降温或反转
2. 防止 agent 因为看到 `active_theme` 就过度确信

建议保留：

1. 会直接改变主题方向的风险
2. 会导致板块先于新闻反转的交易风险
3. 会导致股票选择发生明显切换的结构风险

### 4.8 `next_day_watchlist`

用途：

1. 为明天的新 agent 提供明确跟踪清单

要求：

1. 尽量具体
2. 可执行
3. 可被搜索或核验

## 5. `key_events` 设计

你提出的担心是正确的：

如果一个主题持续一个月，相关事件可能几百条，不能全部挂进主题里。

因此建议：

1. `key_events` 不是全量新闻列表
2. 它是 agent 选出来的“重要节点链”
3. 应兼顾重要程度和时间顺序

### 5.1 设计原则

1. 每个主题只保留少量高价值事件节点
2. 优先保留改变主题方向的节点
3. 优先保留第一次进入主上下文的起点事件
4. 优先保留从“强化 -> 分化 -> 衰减 -> 再强化”的拐点事件
5. 若某个主题已持续很久，优先保留能解释阶段切换的重要节点，而不是平均抽样

### 5.2 建议上限

每个主题最多挂靠：

1. `30-50` 个 key events

进一步建议：

1. `active` 主题默认保留最近 `20-30` 个最重要节点
2. 只有极少数超长主线主题才允许扩到 `50`

### 5.3 选择规则

应由 agent 自动选择，而不是机械保留每天全部新闻。

优先级建议：

1. `critical`
   - 方向性切换节点
2. `high`
   - 强化或证伪的重要确认
3. `medium`
   - 补足事件链连续性的关键背景

不应保留：

1. 重复表态
2. 无新增事实的跟风报道
3. 不改变叙事方向的细碎噪声

### 5.4 `key_events` 与高层摘要的关系

分工应是：

1. `key_events`
   - 保留原始事件链骨架
2. `history_anchor`
   - 对历史主线做压缩总结
3. `today_update`
   - 对当日增量做压缩总结
4. `current_state`
   - 给出今天的高层状态结论
5. `scenario_tree`
   - 给出未来的主要分支推演
6. `key_risks`
   - 给出主题失效或反转的提醒

也就是说：

1. `key_events` 是原始证据骨架
2. 上层字段是面向新 agent 的解释层
3. `scenario_tree` 和 `key_risks` 用来抑制单向叙事偏见

## 7. 哪些新闻应该进入这个系统

应进入主主题系统的内容：

1. 会改变市场叙事的宏观、政策、产业、地缘新闻
2. 会驱动板块或赛道重估的行业级事件
3. 少量对板块或市场有外溢性的公司事件

不应进入主主题系统的内容：

1. 普通财报
2. 小额担保
3. 常规减持、增持
4. 对板块和市场没有扩散意义的日常公告

对于这类单公司信息，更适合进入：

1. 个股 snapshot
2. 股票侧观察清单
3. 股票侧决策日志

## 8. 主题如何映射到股票

主题层不直接负责最终选股，但必须提供股票映射线索。

建议至少输出：

1. `linked_symbols_in_universe`
2. `outside_universe_names_to_check`
3. `search_trigger`

作用：

1. 帮助 agent 在宇宙内优先看哪些股票
2. 触发 agent 在宇宙外搜索新的龙头或弹性股

因此它是：

1. 主题 -> 板块 -> 股票
2. 的桥梁层
