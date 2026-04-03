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
  "archived_themes": [],
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
      "status": "strengthening",
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
      "linked_boards": ["油气开采", "航运", "军工"],
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
  "archived_themes": [],
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

## 6. `06_hot_news_state_ops.json`

### 6.1 它是干嘛用的

`06_hot_news_state_ops.json` 不是给后续选股 agent 直接消费的主结果文件。

它的定位是：

1. 当天主题更新过程的审计日志
2. 供人工复核、回放、调试、排障使用
3. 让人或系统能够追溯：
   - 今天从多少新闻里抽出了哪些候选
   - 每个候选为什么被新增、更新或丢弃
   - 哪些主题发生了 merge、archive、cooling
   - 哪些输入缺失、fallback、失败或降级执行

所以：

1. `06_hot_news_state.json` 是结果文件
2. `06_hot_news_state_ops.json` 是过程文件

### 6.2 设计原则

1. `ops` 是过程审计日志，不是第二份 `06_hot_news_state.json`
2. 顶层按“一次运行”组织，不按“一个主题一个顶层项目”组织
3. 候选判断按 `candidate` 粒度记录，真实写库动作按 `operation` 粒度记录
4. 不要把 `03_news_prompt_input.json` 的全量新闻正文再复制一遍
5. 证据以 `evidence_news_ids` 为主，如需增强可读性，只补轻量摘要，不补全文
6. 缺失、fallback、报错和跳过必须能在 `source_status` 中被审计出来

不建议把 `ops` 设计成“一个主题一个顶层项目”，原因是：

1. `DROP_CANDIDATE` 在被丢弃时还不是主题
2. 同一主题同一天可能出现多个动作
3. 过程文件的原子单位是“判断”和“动作”，不是最终静态主题

### 6.3 顶层结构

```json
{
  "schema_version": 1,
  "run_date": "2026-04-03",
  "updated_at": "2026-04-03T21:15:00+08:00",
  "model": "deepseek-v3.2-exp",
  "embedding_model": "text-embedding-v4",
  "input_summary": {},
  "extracted_candidates": [],
  "retrievals": [],
  "planned_candidate_actions": [],
  "planned_archive_actions": [],
  "planned_merge_actions": [],
  "applied_operations": [],
  "source_status": []
}
```

### 6.4 字段说明

#### `input_summary`

记录输入规模，而不是复制原始输入内容。

建议包含：

1. `news_count`
2. `prompt_news_count`
3. `board_context_count`
4. `existing_theme_count`

#### `source_status`

记录输入与外部步骤是否完整可用。

用途：

1. 让后续排障时区分“今天没有这个信号”还是“今天输入缺失/失败”
2. 记录是否使用了 `latest.json` 或其他 fallback
3. 记录模型调用、检索、merge 等步骤是 `ok / skipped / error`

#### `extracted_candidates`

记录当天从新闻与板块线索中抽出来的候选。

用途：

1. 回看今天本来想追踪什么
2. 对比哪些候选最终被升级、哪些被丢弃

每个 candidate 建议包含：

1. `candidate_id`
2. `theme_name`
3. `summary`
4. `today_delta`
5. `strength`
6. `linked_boards`
   - 必须优先使用 `05_board_heat_digest.json` 里的 `standard_board_names`
7. `linked_symbols`
8. `evidence_news_ids`

#### `retrievals`

记录每个候选回溯匹配到了哪些历史主题。

用途：

1. 解释为什么某候选最终是 `UPDATE_THEME` 而不是 `ADD_THEME`
2. 让人工快速检查主题归并是否合理

#### `planned_candidate_actions`

记录每个候选最终如何决策。

建议动作：

1. `ADD_THEME`
2. `UPDATE_THEME`
3. `DROP_CANDIDATE`

每条 action 建议包含：

1. `candidate_id`
2. `action`
3. `target_theme_id`
4. `reason`
5. `canonical_theme_name`
6. `evidence_news_ids`

#### `planned_archive_actions` / `planned_merge_actions`

记录归档与合并的计划动作，用于解释当天主题库为什么发生结构变化。

#### `applied_operations`

记录真正写入状态库的操作。

它和 `planned_candidate_actions` 的区别：

1. `planned_candidate_actions` 是候选层决策
2. `applied_operations` 是最终落盘动作

建议每条 operation 包含：

1. `op_type`
2. `theme_id`
3. `candidate_id`
4. `target_theme_id`
5. `reason`
6. `payload`

### 6.5 证据记录原则

每条候选或操作可以引用新闻证据，但应尽量轻量：

1. 必须保留 `evidence_news_ids`
2. 如需增加可读性，可补 `news_id / title / source / published_at`
3. 不要在 `ops` 中复制全部 `content`
4. `03_news_prompt_input.json` 仍是原始全文新闻的唯一主来源

### 6.6 主题遗忘与移出主上下文规则

这里的“遗忘”不是物理删除历史，而是：

1. 某主题不再出现在今天新的 `06_hot_news_state.json`
2. 但它的移出轨迹必须保留在今天的 `06_hot_news_state_ops.json`

换句话说：

1. `06_hot_news_state.json` 只保留今天还值得保留在主上下文里的主题
2. 被移出的主题只留在 `ops` 中，供后续人工追溯

#### 6.6.1 何时仍应保留主题

满足以下任一条件，主题通常仍应保留在今天的 `06_hot_news_state.json`：

1. 今天有新增高质量事实、政策、产业或地缘增量
2. 板块和资金仍在持续交易这条叙事
3. 即使新闻增量不大，但主题对明天决策仍有明显影响
4. 主题尚未结束，只是进入分化、钝化或等待确认阶段

#### 6.6.2 何时可以降级到 `cooling_themes`

满足以下特征时，可从 `active_themes` 降到 `cooling_themes`：

1. 最近 1-3 个交易日没有明显新增事实
2. 板块热度和资金确认开始减弱
3. 主题尚未被证伪，也未完全结束
4. 仍存在短期回流或二次强化可能

#### 6.6.3 何时可以从今天主上下文中移出

只有当主题已经明显失去继续占据主上下文的必要性时，才可以移出今天的 `06_hot_news_state.json`。

至少应满足以下大部分条件：

1. 核心事件已经兑现、结束、落地或被证伪
2. 最近几天没有新的高质量增量
3. 板块热度和资金确认已经明显消失
4. 该主题不再对明天的选股或市场理解产生明显影响
5. 继续保留它只会增加噪声，而不会提高决策质量

高权重旧主题在移出前，建议额外联网复核：

1. 搜索最近几天是否还有新发展
2. 搜索是否还有政策、产业、地缘层面的延续影响
3. 若最近已无明显新风声、无扩散影响，可移出今天主上下文

#### 6.6.4 不应轻易移出的情形

以下情况不应直接遗忘：

1. 主题只是暂时缺少新闻，但板块仍在交易
2. 主题虽然降温，但仍可能在 1-2 个交易日内回流
3. 主题影响虽然减弱，但仍是理解其他热点的背景前提
4. 当前证据不足以判断它已经彻底结束

### 6.7 `ops` 中必须记录的移出轨迹

任何被移出今天主上下文的主题，都必须在 `06_hot_news_state_ops.json` 中留下可追溯记录。

建议在 `applied_operations` 中至少写明：

1. `op_type`
   - 固定写 `REMOVE_FROM_MAIN_CONTEXT`
2. `theme_id`
3. `theme_name`
4. `removed_on`
   - 即今天的 `run_date`
5. `last_seen_on`
   - 即它最后一次出现在 `06_hot_news_state.json` 的日期
6. `reason`
   - 为什么移出
7. `evidence_news_ids`
   - 支撑移出判断的新闻证据
8. `search_checked`
   - 是否做过联网复核
9. `search_verdict`
   - 若做过联网复核，简述最近几天是否仍有延续影响

### 6.8 推荐示例

```json
{
  "schema_version": 1,
  "run_date": "2026-04-03",
  "updated_at": "2026-04-03T21:15:00+08:00",
  "model": "deepseek-v3.2-exp",
  "embedding_model": "text-embedding-v4",
  "input_summary": {
    "news_count": 320,
    "prompt_news_count": 40,
    "board_context_count": 6,
    "existing_theme_count": 14
  },
  "extracted_candidates": [
    {
      "candidate_id": "cand_01",
      "theme_name": "油价上行与霍尔木兹风险",
      "summary": "中东冲突继续强化，油运与能源风险溢价抬升",
      "linked_boards": ["油气开采", "航运", "军工"],
      "evidence_news_ids": ["N001", "N015", "N021"]
    }
  ],
  "retrievals": [
    {
      "candidate_id": "cand_01",
      "matches": [
        {
          "theme_id": "theme_oil_hormuz",
          "score": 0.92
        }
      ]
    }
  ],
  "planned_candidate_actions": [
    {
      "candidate_id": "cand_01",
      "action": "UPDATE_THEME",
      "target_theme_id": "theme_oil_hormuz",
      "reason": "与昨日主题属于同一条主线，今天是强化而非新主题",
      "canonical_theme_name": "油价上行与霍尔木兹风险",
      "evidence_news_ids": ["N001", "N015", "N021"]
    }
  ],
  "planned_archive_actions": [],
  "planned_merge_actions": [],
  "applied_operations": [
    {
      "op_type": "UPDATE_THEME",
      "candidate_id": "cand_01",
      "theme_id": "theme_oil_hormuz",
      "reason": "与昨日主题属于同一条主线，今天是强化而非新主题"
    },
    {
      "op_type": "DROP_CANDIDATE",
      "candidate_id": "cand_05",
      "reason": "单公司公告，无板块外溢性"
    },
    {
      "op_type": "REMOVE_FROM_MAIN_CONTEXT",
      "theme_id": "theme_shipping_rate_bounce",
      "theme_name": "航运运价反弹",
      "removed_on": "2026-04-03",
      "last_seen_on": "2026-04-02",
      "reason": "最近几天无新增事实，板块确认消失，继续保留只会增加噪声",
      "evidence_news_ids": [],
      "search_checked": true,
      "search_verdict": "近几天无新的产业或政策催化，主题延续性不足"
    }
  ],
  "source_status": [
    {
      "source": "03_news_prompt_input",
      "status": "ok",
      "rows": 320
    },
    {
      "source": "previous_hot_news_state",
      "status": "fallback",
      "reason": "selection_runs_missing_use_latest"
    }
  ]
}
```

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
