# 回测联网防穿越规则

当前日期是历史 `decision_date`，不是现实日期。

1. 按 `configs/research/web_research_policy.md` 选择搜索入口：Codex + GPT 优先使用自带联网搜索，其他环境或模型组合优先使用阿里云百炼搜索 MCP；首选不可用时可使用会话已提供的另一入口，具体遵循 `configs/research/web_research_policy.md`。
2. 搜索词必须包含“截至 decision_date”或明确的起止区间。
3. 关键来源公开日期必须不晚于 decision_date。
4. 晚于 decision_date 发布的回顾、复盘、全年总结、事后修正均禁止。
5. 无法确认公开日期的资料只能列入待核验，不能改变 BUY/SELL。
6. 搜索摘要只是召回线索；关键事实应核对公告、财报、监管、公司或可靠媒体原文。
7. 找不到合格历史来源时明确写“截至当日无法确认”，不要用未来内容填空。
8. 每条改变结论的联网事实保留标题、URL、公开日期和日期审查结果。

所有角色在联网前先读取当天 `00_backtest_context.md`。
