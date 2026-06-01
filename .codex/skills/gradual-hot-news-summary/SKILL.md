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

## 日期语义（必读）

- 本 skill 所有路径、命令、`run_date` 字段中的 `YYYY-MM-DD` 与 `AGENTS.md` 的 `--date` 口径完全一致，指**要分析的交易日**
- **日期选择规则**：
  - 用户显式指定了日期 → 使用用户指定的日期
  - 用户未指定日期 → 使用 `data/selection_runs/` 下最新的已有 `03_news_prompt_input.json` 对应的日期，或当目录为空时默认今天
- 文中出现的「今日」「当天」「今天」在本 skill 上下文里都指这个「要分析的交易日」，不是日历上的 `today`

## 输出路径

- 主输出：`data/selection_runs/YYYY-MM-DD/06_hot_news_state.json`（`YYYY-MM-DD` = 要分析的交易日）

当前 skill 按 "上游准备步骤 + file-only 的总结步骤" 方式工作：

1. 读取本地输入文件
2. 读取最近一天 `06_hot_news_state.json`
3. 直接生成今天新的 `06_hot_news_state.json`

## 先读什么

1. 读 [概览](references/overview.md)
   - 确认这个能力在整个选股系统中的职责边界
2. 读 [输入约定](references/input-contract.md)
   - 开始执行前必读
3. 读 [输出契约](references/output-contract.md)
   - 开始生成 `06_hot_news_state.json` 前必读

## 固定流程

> **前置约定**：本 skill 默认用户已经运行过 `python scripts/refresh_all_for_date.py`，
> `data/selection_runs/YYYY-MM-DD/` 下的 `01-05` 文件与板块层数据已就绪。
> Skill 本体只读文件、跑 LLM，不应触发任何中国 API 调用。
> 若发现输入文件缺失，直接提示用户回去跑 `refresh_all_for_date.py`，不要自己跑补齐脚本。

### 1. 读取主输入

严格按 [输入约定](references/input-contract.md) 读取：

1. 今日 `03_news_prompt_input.json`
2. 最近一天 `06_hot_news_state.json`
3. 最近一天宏观总结
4. 今日板块信息层
5. 当前股票宇宙 `data/universe/master_universe.json`

### 2. 生成主题级研究记忆

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
2. 若未来又被重新激活，可以作为新一轮主上下文主题重新进入

### 3. 必要时联网补证

如果上述主输入仍不足以支撑某个高权重主题判断，可以联网补证。

具体规则见 [事件链与风险规则](references/output-contract.md)。

对于“是否应彻底移出今天主上下文”这个判断：

1. 普通低权重主题可直接基于现有主输入判断
2. 高权重旧主题在准备移出前，建议联网补证
3. 若最近几天已无新增事实、无板块确认、无扩散影响，可移出今天主上下文

### 4. 按输出契约落盘

严格按 [输出契约](references/output-contract.md) 生成：

- `06_hot_news_state.json`

**逐主题生成并写入文件**：不要试图一次性把所有主题写入文件。06_hot_news_state.json 文件通常 6000+ 行、数十万字符，单次 Write 工具调用或单次 bash heredoc 必定被截断导致 JSON 损坏。

正确的落盘流程：

1. 先用 Python 创建包含 `run_date`、`market_regime_bridge` 字段的基础 JSON 骨架，写入文件
2. 每分析完一个主题（active/cooling），立即用 Python 读取现有文件 → `data["active_themes"].append(theme_dict)` → 写回文件
3. 重复步骤 2，直到所有主题全部追加完成
4. 最后追加 `cooling_themes`、`summary` 等剩余字段，同样逐块写入
5. 全部完成后用 `python3 -c "import json; json.load(open('...'))"` 验证 JSON 有效性

禁止的做法：
- 禁止在一条 Write 工具调用中写入完整 JSON（必定截断）
- 禁止在 bash heredoc 中拼装完整 JSON（必定截断）
- 禁止在 Write 的 content 参数中堆砌超过 3000 行的 JSON 文本

## 强制要求

- 不要重写宏观总结
- 不要替代板块热度层
- 不要直接做最终选股结论
- 不要把全部新闻直接挂到股票宇宙上
- `linked_symbols_in_universe` 必须显式对照 `data/universe/master_universe.json`
- 不要省略 `history_anchor / today_update / current_state / expected_duration / forward_paths / scenario_tree / key_risks / next_day_watchlist`
- `linked_boards` 必须使用 [输入约定](references/input-contract.md) 中的标准板块名清单
- 不要把昨天出现过的主题机械地全部延续到今天
- 不要把已结束、已证伪、已完全失去交易性的主题继续保留在今天的 `06_hot_news_state.json`
- **必须逐主题生成并写入文件**，严禁一次性输出全部主题（JSON 体量过大，单次写入必定截断导致文件损坏）

