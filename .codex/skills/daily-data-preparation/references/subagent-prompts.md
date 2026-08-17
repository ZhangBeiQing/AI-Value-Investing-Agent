# Subagent 派发模板

Step 3 只派发**两个** subagent（A 宏观、B 新闻），和财报 prepare 脚本一起在**同一条消息**里发出。

派发工具随运行时不同：Claude Code 是 `Agent`（`subagent_type: claude`），opencode 是 `task`（用默认 agent，不要指定 `fixed-tracked-*` 那些专用角色）。

**财报总结不在这里派发。** 它由主 agent 在 Step 4 亲自执行——因为 `financial-report-summary` 自己要为每只股票再派发 4 个角色，塞进 subagent 会让并发账算不清，而超限的失败方式是静默空返回。详见 SKILL.md「阶段总览」下的说明。

把 `<cur_date>` 替换成 Step 0 定下来的实际日期（`YYYY-MM-DD`），其余文字**逐字照抄**。

## 为什么不能改写

每段提示词的第一行就是对应子 skill 的**触发语**。改写、缩写或"帮它说清楚一点"会导致：

- subagent 匹配不到 skill，退化成通用 agent 自由发挥；
- 规则被主 Agent 二次转述后失真，而子 skill 的日期语义、文件操作策略、输出契约都写在它自己的 SKILL.md 里。

主 Agent 只传"日期 + 任务句子 + 搜索要求 + 完成后回报什么"。不要额外塞投资观点、股票清单或结论倾向。

---

## Subagent A — 宏观总结

- `description`: `宏观总结 <cur_date>`
- 触发 skill：`daily-macro-summary`
- 约耗时：5-10 分钟
- 主产出：`data/macro_economy/<YYYYMMDD>.md`

```text
开始 <cur_date> 的宏观总结。

先执行 source /home/zhangbeiqing/venv/ai_stock/bin/activate 激活虚拟环境。
需要联网搜索时优先使用阿里云百炼的 bailian_web_search MCP 工具。
<cur_date> 是"要分析的交易日"，即收盘数据已经产生的那一天。请严格用这个日期，不要自己重新推导，也不要做任何减一。

完成后回报：输出文件的绝对路径、本轮相对上一版新增或修正了哪些内容、以及任何数据缺口。
```

---

## Subagent B — 渐进式新闻总结

- `description`: `渐进式新闻总结 <cur_date>`
- 触发 skill：`gradual-hot-news-summary`
- 约耗时：约 20 分钟（本轮最慢的一路，耐心等，不要中断）
- 主产出：`data/selection_runs/<cur_date>/06_hot_news_state.json`

```text
开始 <cur_date> 的渐进式新闻总结，没用的已经过时的新闻就删掉，不要让渐进式新闻总结文件太大。

先执行 source /home/zhangbeiqing/venv/ai_stock/bin/activate 激活虚拟环境。
需要联网搜索时优先使用阿里云百炼的 bailian_web_search MCP 工具。
<cur_date> 是"要分析的交易日"，输出文件的 run_date 必须等于这个日期。

完成后回报：输出文件的绝对路径、本轮主题数量、新增/合并/淘汰了哪些主题、文件体积。
```

---

## 第三件事 — 财报 prepare 脚本（主 agent 自己跑，不是 subagent）

和上面两个 subagent 派发放在**同一条消息**里，按 SKILL.md「超时铁律」的**长命令方式**启动：

```bash
source /home/zhangbeiqing/venv/ai_stock/bin/activate
python scripts/prepare_financial_report_skill.py \
  --date <cur_date> --sync-first --json --include-quant-prefilter
```

- 约 10-30 分钟，主要耗时是把新发布的财报 PDF 转 Markdown。**不能天真地前台跑**：opencode 下要把 `timeout` 设到 ≥ 3600000ms，Claude Code 下要用 `run_in_background: true`。
- 硬前置：MinerU `/health` 已 `healthy`（Step 2）。中途若报连不上，先 `curl /health` 确认，**不要**重启正在转换的 MinerU。
- 组件日志：`logs/research/PrepareFinancialReportSkill/`，看进度时 `tail` 最新一份。
- 正式研究**不得**加 `--skip-market-context`。
- 它是纯 Python，不派发任何 subagent，所以和 A/B 并行不占并发额度。

## Step 4 的财报角色派发（主 agent 亲自做）

A/B 两个 subagent 退出、prepare 脚本也结束之后，主 agent 才开始这一步。**不要**为此再起一个"财报总管" subagent。

派发前完整读取 `.codex/skills/financial-report-summary/SKILL.md` 与 `references/role-prompts.md`，角色提示词用那份模板，本文件不重复。

调度上只有三条硬约束：

- 只处理 prepare 输出里的 `ready_items`；`skipped_items` 不启动任何角色。
- **每批最多 10 个 subagent。** 5 只股票 × 4 个角色 = 20 个，必须拆批：上一批全部结束、文件全部落盘并校验通过，才能起下一批。
  - 这条**优先于** `~/.config/opencode/AGENTS.md` 规则三的"禁止分批"。超限的失败方式是静默空返回，比慢危险。
- 同一细分产业链共用一个 Industry Researcher；不同产业链各自独立，不得互相借用。

> 当天没有公司发新财报时，`ready_items` 为空是**合法结果**：跳过本步，在最终报告里写明"无新增财报"，不要硬造一份研究。
