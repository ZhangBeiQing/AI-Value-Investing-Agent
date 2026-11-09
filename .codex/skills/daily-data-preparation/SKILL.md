---
name: daily-data-preparation
description: >
  交易 skill 之前的每日数据准备总调度，设计运行时点是当天晚上 9 点、分析当天收盘、为下一交易日
  出预案（日期默认 today，不做减一）。当用户说“开始今天的数据准备”、“准备今天的交易数据”、
  “跑今天的盘前数据链路”、“刷新今天全部数据并生成研究包”或类似请求时使用。串联
  refresh_all_for_date → pymupdf4llm 就绪 → 宏观总结 subagent 与财报 prepare 脚本并行
  → 主 agent 亲自做逐股财报研究 → run_daily_pipeline，在一个会话里一次性跑完约 50-90 分钟
  的长链路，产出 01-04 研究包，供 auto-trading-fixed-tracked 直接消费。
---

# Daily Data Preparation

## 边界

- 本 skill **只准备数据与输入产物**，不生成任何交易决策、仓位、目标价，不写 `05_decision.json`。
- 完成后**不自动触发** `auto-trading-fixed-tracked`；只向用户报告"数据已就绪"，由用户决定何时开始交易。
- 不修改 `configs/stock_pool.py`、投资策略文件、账本或历史持仓。
- 两个子 skill（`daily-macro-summary`、`financial-report-summary`）的内部规则由**各自的 SKILL.md 定义**，本 skill 只负责调度、门禁和验收，**不得转述、压缩或改写它们的规则**。
  - `daily-macro-summary` 派给 subagent 执行，派发时只传日期与任务句子。
  - `financial-report-summary` 由**主 agent 直接执行**（原因见「阶段总览」下的说明），执行前要完整读它自己的 SKILL.md 和角色模板。
- **已暂停 `gradual-hot-news-summary`（渐进式新闻总结）**：token 成本过高，`configs/feature_flags.py` 的 `ENABLE_GRADUAL_NEWS_SUMMARY=False`。本 skill 不再派发该 subagent，Step 1 也不再采集新闻、不生成 `05_board_heat_*`（`ENABLE_BOARD_HEAT_LAYER=False`）。不要把它当成"漏了一步"去补。
- 不得为了"跑快一点"而缩小股票范围、跳过步骤或改小并发；范围变更必须由用户明确要求。

## 日期语义（必读）

本 skill 全程只有一个日期变量 `cur_date` = **要分析的交易日**，即收盘数据已经产生的那一天。

**本 skill 的设计运行时点是当天 21:00**（A 股 15:00 收盘后 6 小时），目的是分析**今天**的收盘、为**明天**的买卖给建议。因此：

- 用户显式给了日期 → 用用户的日期。
- 用户没给日期 → **默认 `today`**，与 `AGENTS.md` 的统一口径一致。
- **不做减一。** 「默认昨天 / `today - 1`」是已废弃的旧设计，不要照着任何残留的旧表述推导日期。
- 若 `today` 不是交易日（周末、节假日）→ 改为**最近一个交易日**，并先告知用户「当天无新收盘数据，该交易日可能已经跑过」，确认后再继续，不要闷头重跑一遍。
- 只有一种例外：用户明确说是在**次日补跑**昨天漏掉的那一轮，才显式传那一天的日期。
- **禁止传未来日期。**
- 若用户确实要在休市日吸收盘外宏观/新闻，`refresh_all_for_date.py` 与 `run_daily_pipeline.py` **两处都要**加 `--allow-non-trading-date`；该参数不表示当日可成交。

**当天夜间运行的额外要求**：`akshare` 的当日行情/新闻偶有延迟。Step 1 的门禁必须逐项确认 `<cur_date>` 对应的文件**真的落盘**；缺了就报告失败现场，**绝不允许拿前一交易日的数据顶替**——那会让明天的建议建立在过期收盘价上。

**财报公告的日期口径（`<cur_date>` 与 `<cur_date>+1` 并存）**：公司在交易日 D 的**晚间**发财报，巨潮/交易所把这类盘后公告的日期登记为 **D+1**。因此 Step 3 的 `prepare_financial_report_skill.py` 默认把**公告可见性**截断到 D+1，而价格、估值、市场上下文仍以 D 为准（`--disclosure-cutoff next-day`，默认值）。这**不是**时间穿越：D 日收盘价不含当晚披露的信息，D+1 开盘交易时理应用到它。历史回测必须传 `--backtest-context`，此时脚本强制 `analysis-day` 截断。2026-10-27 真实事故：按 D 截断导致赛腾股份、星宸科技、长川科技、瑞芯微当晚披露的四份三季报整批漏抓。

确定 `cur_date` 后把它固定下来，后续所有命令、路径、subagent 提示词都用同一个值，不要中途改。

## 环境前置

```bash
source /home/zhangbeiqing/venv/ai_stock/bin/activate
```

- 工作目录固定为仓库根目录，所有命令用相对路径 `scripts/xxx.py`。
- 联网搜索按运行环境选择：Codex + GPT 优先使用自带联网搜索，其他环境或模型组合优先使用阿里云百炼搜索 MCP；首选不可用时可使用会话已提供的另一入口，具体遵循 `configs/research/web_research_policy.md`；所有可用搜索入口均不可用时失败关闭并记录 `search_status = web_search_unavailable`，不自行拼接第三方检索 API 或爬虫替代。
- 用户粘贴的 Windows 路径要转成 WSL 路径。

## 超时铁律（本 skill 最容易踩的坑）

本流程有三条长命令，分别约 **15 / 10-30 / 5-15 分钟**。天真地前台跑必然被截断。**怎么跑取决于运行时**，先认清你自己在哪个运行时里，下文统一称为「**按长命令方式启动**」：

| 运行时 | Bash 工具能力 | 长命令怎么启动 |
| --- | --- | --- |
| **opencode**（cron 默认） | 无后台参数，但 `timeout` 上限几乎无限 | 前台跑，`timeout` 设 **≥ 3600000ms（1 小时）**。这也是 `~/.config/opencode/AGENTS.md` 规则四的硬要求 |
| **Claude Code** | `timeout` 上限 600000ms（10 分钟），有 `run_in_background` | 用 `run_in_background: true`，**不要**试图加大 `timeout` |

判断方法：看你的 bash 工具 schema 里有没有 `run_in_background` 参数——有就是 Claude Code，没有就是 opencode。

两个运行时共同的铁律：

- **绝不**把长命令的输出接进 `| tail` / `| head` / `> /dev/null`：这些只在进程退出后才吐字，中途卡住时
  一行 log 都看不到，无法判断是在跑还是已死。启动方式只能是「后台 + 落盘日志」或
  `set -o pipefail; ... 2>&1 | tee -a logs/xxx.log`，启动后立刻把日志路径告诉用户。
- 一个命令**只启动一次**。看不到输出 ≠ 卡死；重复启动会造成缓存写入竞争。
- 想看中途进度时，`tail` 对应的组件日志，**绝不重启命令**。
- 不要用 `sleep` 循环轮询。Claude Code 下后台任务退出时 harness 会通知你；opencode 下前台等待本身就是在等它返回。
- **绝不因为"等太久"就中断、跳过步骤、缩小范围或提前进入下一阶段。**

派发 subagent 的工具名也随运行时不同：Claude Code 叫 `Agent`，opencode 叫 `task`。下文统称「派发 subagent」。

## 阶段总览

| 阶段 | 内容 | 约耗时 | 阻塞关系 |
| --- | --- | --- | --- |
| Step 1 | `refresh_all_for_date.py` | ~15 min | 全流程根依赖 |
| Step 2 | `pymupdf4llm` 依赖就绪 | <1 min | 可与 Step 1 **并行**；Step 3 的硬前置 |
| Step 3 | 宏观 subagent **+** 后台跑财报 prepare 脚本（并行） | 10-30 min | 必须等 Step 1 成功 |
| Step 4 | **主 agent 亲自**做逐股财报研究 | 10-40 min | 必须等 Step 3 三项全部结束 |
| Step 5 | `run_daily_pipeline.py` | 5-15 min | 必须等 Step 4 完成 |

Step 1 与 Step 2 在同一条消息里一起发出；Step 3 的宏观 subagent 和 prepare 脚本也在同一条消息里一起发出。

### 为什么财报不派给 subagent（重要，不要"优化"回去）

`financial-report-summary` 自己就要为**每只股票**派发 Industry Researcher / Expectation Scout / Financial Author 三个角色（同链行业研究可复用）。5 只股票最多需要 15 个 subagent。

嵌套派发在技术上是能跑通的（已实测，含无头模式），但**并发账会算不清**：财报最多 15 个 + 宏观同时在跑，会超过平台每批的并发上限，而超限的失败方式是**静默空返回**——subagent 报告"完成"，文件却是空的或没写。`auto-trading-fixed-tracked` 里那条「每批最多 10 个，不得超过，以避免任务空返回、写错日期目录或文件缺失」就是同一个坑。

所以规则是：**需要再派发一层 subagent 的环节，只能由主 agent 直接做。** 财报研究必须留在主 agent 手里，且必须等宏观一路结束、并发腾空之后才开始。

反过来，`prepare_financial_report_skill.py` 是**纯 Python，自己不派发任何 subagent**，所以它可以和宏观 subagent 并行跑，不占 subagent 并发额度。

## 一、Step 1：一键刷新全部 Python 链路数据

```bash
python scripts/refresh_all_for_date.py --date <cur_date>
```

- 按「超时铁律」的**长命令方式**启动。
- 休市日补充分析时追加 `--allow-non-trading-date`。
- 用户明确要求强刷财报结构化等重缓存时才追加 `--fresh-heavy`。
- **不要**加 `--continue-on-failure`：默认失败即停，才能暴露真实断点。
- 组件日志：`logs/main_scripts/RefreshAllForDate/refresh_all_for_date_*.log` 与同目录 `merged.log`。

### 美股标的已并入同一条链路（不要再单独跑 `run_us_research.py`）

- 固定股池包含美股：`configs/stock_pool.py` 的 `FIXED_TRACKED_US_SYMBOLS`（当前 `TSM.US`）。它们与 A 股/港股**同批刷新、同批进入 `fixed_tracked` 的 `01-04`**，辩论与 `05_decision.json` 也完全一致，不再单独走 `us_tracked` 账本。
- **日期语义差异（预期行为，不是数据顶替）**：`--date D` 对 A 股/港股是 D 日收盘；对美股，北京时间 D 日晚运行时 D 日美股尚未收盘，因此取「最近一个已收盘美东交易日」（通常 D-1）的收盘价与财务口径。研究包里美股标的的报价即为该日收盘价。
- **币种口径**：美元持仓按 `configs/stock_pool.py` 的 `USD_CNY_FX_RATE`（默认 6.65）折算为人民币，计入真实总资产、浮动盈亏与仓位比例；`03_agent_input.md` 的「持仓币种明细」保留原始币种报价与折算后人民币口径两份数字。
- 只有需要**单独**给某只美股补研究包（不跑 A 股链路）时，才用 `python scripts/run_us_research.py --symbol <TICKER.US>`。


### 进入 Step 3 的硬门禁

必须**同时**满足：退出码为 0，且下列文件全部存在（`<cur_date>` 为实际日期）：

```text
data/global_cache/macro_objective_panel/daily_snapshots/<cur_date>.json
```

任一缺失就不要进入 Step 3：先读日志定位失败步骤，向用户报告失败现场（日志路径 + 报错原文），询问是补数据还是跳过。**不要**在缺输入的情况下让 subagent 空跑——它们会基于过期数据产出看起来正常的错误结论。

> 校验用 `find` / `ls`，**不要用 Glob 搜 `data/`**（`data/` 下有 `.git` 子目录，Glob 会整体返回空）。

## 二、Step 2：确认 PDF 转 Markdown 依赖就绪（与 Step 1 并行）

财报 PDF 转 Markdown 由 `pymupdf4llm` 在**本项目虚拟环境内本地**完成，不需要任何常驻服务、健康检查或 GPU。

在项目虚拟环境下执行：

```bash
python -c "import pymupdf4llm; print(pymupdf4llm.VERSION)"
```

- 能打印版本号 → 已就绪，直接进入 Step 3。
- 报 `ModuleNotFoundError` → 先 `pip install pymupdf4llm`（已列入 `requirements.txt`），装完再验一次。
- 这一步通常几秒结束，**不需要**后台运行，也不要去 curl 任何 `/health`。

## 三、Step 3：宏观 subagent + 并行跑财报 prepare

Step 1 门禁通过后，**在同一条消息里一次性发出两件事**：宏观 subagent 派发 + 一个长命令 Bash。不要串行等待第一件结束再发第二件。

- 用运行时的通用 subagent 类型（Claude Code 下 `subagent_type: claude`；opencode 下用默认 agent，不要指定 `fixed-tracked-*` 那些专用角色）。
- 派发提示词逐字使用 [subagent 派发模板](references/subagent-prompts.md)，把 `<cur_date>` 替换成实际日期。**不要**自己改写任务句子：那些句子就是子 skill 的触发语。
- **只派发这一个 subagent。** 不要在这里派发做财报的 subagent（原因见「阶段总览」下的说明），也**不要**派发渐进式新闻总结（该 skill 已暂停）。

| Subagent | 触发 skill | 约耗时 | 主产出 |
| --- | --- | --- | --- |
| A 宏观总结 | `daily-macro-summary` | 5-10 min | `data/macro_economy/<YYYYMMDD>.md` |

同时，主 agent 自己按「超时铁律」的**长命令方式**启动财报准备脚本：

```bash
python scripts/prepare_financial_report_skill.py \
  --date <cur_date> --sync-first --json
```

- 约 10-30 分钟，主要耗时是把新发布的财报 PDF 转 Markdown（本地 `pymupdf4llm`，数百页年报约 1-2 分钟，所以 Step 2 必须已就绪）。
- 它是纯 Python，**不派发任何 subagent**，所以和宏观 subagent 并行不占并发额度。
- 组件日志：`logs/research/PrepareFinancialReportSkill/`。
- 正式研究**不得**加 `--skip-market-context`。

### 两项都结束后

先逐个核对产出文件是否真的落盘，**不要只看 subagent 的自述文字**。

- 宏观 subagent 失败 → 重新派发（幂等）。
- 重试仍失败 → 报告失败现场，并说明"若带着这个缺口继续，`01_global_context.md` 会缺哪一块"。
- prepare 脚本失败 → 读组件日志定位；若是 PDF 转换报错，先看是不是 PDF 无文本层或已损坏（缺文本层时 `pymupdf4llm` 提不出内容），把该文件记为数据缺口。

两项齐了才进入 Step 4。此时宏观 subagent 已经退出，subagent 并发额度腾空，才能开始派发财报角色。

## 四、Step 4：主 agent 亲自做逐股财报研究

**这一步由主 agent 直接执行 `financial-report-summary`，不要整体丢给一个 subagent。**

开始前完整读取 `.codex/skills/financial-report-summary/SKILL.md` 及其 `references/role-prompts.md`，然后按那份 skill 的规则执行。本 skill 不重复它的内部规则，只约束调度方式：

- 从 Step 3 prepare 脚本的 JSON 输出里取 `ready_items`，**只处理 ready 的股票**；`skipped_items`（已总结或无新财报）不启动任何角色。
- 当天没有公司发新财报时，`ready_items` 为空是**合法结果**：跳过本步，在最终报告里写明"无新增财报"，不要硬造研究。
- 逐股派发角色时，**每批最多 10 个 subagent**。比如 5 只股票 × 4 个角色 = 20 个，必须拆成至少 2 批：上一批全部结束、目标文件全部落盘并校验通过后，才能起下一批。
  - ⚠️ **这条优先于 `~/.config/opencode/AGENTS.md` 规则三**（那条要求"Subagent 必须全量并发、禁止分批"）。在本 skill 的 Step 4 里，以"每批最多 10 个"为准。
  - 理由：`auto-trading-fixed-tracked` 记录了超限的真实后果——**任务空返回、写错日期目录、文件缺失**。这类失败是静默的，跑完看起来正常，文件却是空的，比慢一点危险得多。
- 同一细分产业链的多只股票共用一个 Industry Researcher；不同产业链各自独立，不得互相借用。
- 派发任何需要联网的角色前，先按 `configs/research/web_research_policy.md` 确认当前环境的搜索入口可用（Codex + GPT 优先自带搜索，其他组合优先百炼 MCP）。
- 最终报告只由 Financial Author 写，`summary_index.json` 只由主 agent 通过注册脚本写。

主产出：`data/stock_info/{name}_{symbol}/financial_reports/<YYYYMMDD>.md` 与 `summary_index.json`。

## 五、Step 5：生成 01-04 股票研究包

Step 4 完成后：

```bash
python scripts/run_daily_pipeline.py --date <cur_date> --max-workers 6 --all-books
```

- 按「超时铁律」的**长命令方式**启动。
- `--date` 必传：虽然该脚本默认值也是 `today`，但显式传 `cur_date` 才能保证与 Step 1/Step 3 用的是同一天（尤其是跨过午夜的长任务）。
- `--all-books` 是**兼容参数**（自动 manifest 当前只生成综合 `fixed_tracked`），保留传入以对齐历史命令，不影响结果。
- 此入口默认跳过公告刷新：公告已由 Step 1 的 `refresh_all_for_date.py` 负责；不要额外传 `--refresh-disclosures`，否则会重复扫描公告缓存并延迟 01-04 产物。
- 休市日补充分析时追加 `--allow-non-trading-date`。

期望产出：

```text
data/skill_runs/<cur_date>/
├── run_manifest.json
└── fixed_tracked/
    ├── 01_global_context.md            # 宏观 / 大盘
    ├── 02_basic_snapshot_payload.json  # 个股快照
    ├── 03_agent_input.md               # 投资策略 + P0 筛选输入
    ├── 03_stock_analysis_input.md      # 个股辩论研究方法
    └── 04_stock_research/              # 只覆盖 TRACKED_A_STOCKS 固定池的逐股研究包
```

## 六、验收清单

五步跑完后逐项确认（用 `find` / `ls`，不用 Glob）：

- [ ] `data/global_cache/macro_objective_panel/daily_snapshots/<cur_date>.json` 存在
- [ ] `data/macro_economy/<YYYYMMDD>.md` 的正文日期确实是 `cur_date`
- [ ] 财报 `summary_index.json` 已登记本轮新增股票（无新财报时"零新增"也是合法结果）
- [ ] Step 4 的 `ready_items` 逐只都有对应的 `financial_reports/<YYYYMMDD>.md` 落盘；**空文件视为失败**（并发超限的静默空返回就长这样）
- [ ] `data/skill_runs/<cur_date>/run_manifest.json` 存在
- [ ] `data/skill_runs/<cur_date>/fixed_tracked/` 下 `01`、`02`、`03`、`03_stock_analysis_input`、`04_stock_research/` 齐全
- [ ] `04_stock_research/` 的逐股目录数量与 `TRACKED_A_STOCKS` 数量相符,没有大面积缺失

宏观总结的日期核对不能只看文件名：`daily-macro-summary` 允许以前一日文件为模板增量更新，要确认正文头部的数据口径日期也改过来了。

## 七、定时触发（已配置）

本 skill 已挂到系统 crontab，**周一至周五 21:03 自动运行**，无需人工触发：

```text
3 21 * * 1-5 /home/zhangbeiqing/programer/AI-Value-Investing-Agent/scripts/cron_daily_data_prep.sh
```

- 包装脚本：`scripts/cron_daily_data_prep.sh`，默认用 `opencode run --auto --model deepseek/deepseek-flash`（DeepSeek-V4.1-Flash）无头运行（换回 Claude：`RUNTIME=claude`；换模型：`OPENCODE_MODEL=openai/gpt-5.6`）。
  - **不要把默认模型换成 `openai/gpt-5.6`。** 它在无头模式下有 50% 概率把首条回复发到 final 频道、导致 turn 立刻结束零产物（见下），不适合无人值守主路径。要用就手动指定。
- 脚本内有**交易日历门禁**：非交易日（含节假日）直接 SKIP，不会拉起 agent 空跑，所以 crontab 的 `1-5` 只是少几次无用唤醒。
- 还有**收盘门禁**：`RUN_DATE` 是当天且当前不到 16:00 时直接 SKIP，避免基于还没产生的收盘数据跑一轮垃圾。
- 运行状态：`logs/cron_daily_prep/latest_status.json`（`running` / `success` / `skipped` / `timeout` / `failed`）。整轮开始时就会写 `running`，所以看到 `running` 说明正在跑，不是"没触发"。
- 当轮完整日志：`logs/cron_daily_prep/<cur_date>.log`。
- 整体上限 9000 秒（2.5 小时），超时会被 `timeout` 终止并记 `timeout` 状态。

### 退出码 0 不等于跑完（产物验收）

脚本在写 `success` 之前会先做一遍产物验收，**验收不过就写 `failed` 并以 exit 2 结束**，理由是 2026-08-21 与 2026-08-24 的真实事故：

> gpt-5.6 把首条状态更新错发到 **final 频道**，turn 被提前结束，之后所有工具调用被静默丢弃。整轮 13-38 秒结束、`opencode` 正常 exit 0，wrapper 于是写了 `success`——而当天一个产物都没生成。

这种「秒退」会被自动重试一次：`exit 0` + 产物为空 + 耗时 < `FAST_FAIL_SECONDS`（默认 300 秒）时，换 `FALLBACK_MODEL`（默认 `openai/gpt-5.6`，与主模型不同 provider）重跑。**只对秒退重试**——跑了半小时才失败的是真实链路问题（数据源、PDF 转换、余额），从头重来又慢又贵，按「失败与重跑」补那一段更合适。

验收内容 = 「验收清单」里可机器判定的那几项（Step 1 门禁 + 宏观 + `run_manifest.json` + `01/02/03/03_stock_analysis_input` + `04_stock_research/` 非空）。额外还检查 `run_manifest.json` 与 `01_global_context.md` 的 **mtime 晚于本轮启动时间**——否则幂等重跑时，上一轮留下的旧文件会让一个空转轮蒙混过关。

排查某一天到底缺什么，不必重跑整轮：

```bash
VERIFY_ONLY=1 START_EPOCH_OVERRIDE=0 scripts/cron_daily_data_prep.sh 2026-08-21
```

（`START_EPOCH_OVERRIDE=0` 关掉新鲜度检查，回放历史日期时必须加，否则一定误报。）

无人值守运行时**没有人能实时回答提问**：遇到需要确认的地方按默认路径继续，把待确认事项写进最终报告，不要停下来等人。仍然不得触发交易 skill、不得生成或修改 `05_decision.json`。

排查 cron 没跑的顺序：先看 `latest_status.json` 的 `run_date` 是不是当天 → 再 `systemctl status cron` 确认服务在跑 → 最后确认 WSL 当时没被关掉（`wsl --shutdown` 或 Windows 关机期间 cron 不会触发，且**不会补跑**，需要手动传日期补一轮）。

`node` 升级后 `claude` 的绝对路径会变，需要同步改 `scripts/cron_daily_data_prep.sh` 里的 `NODE_BIN`。

## 八、失败与重跑

五个阶段各自幂等，失败后只重跑失败的那一段，不要从头重来：

```bash
# Step 1
python scripts/refresh_all_for_date.py --date <cur_date>

# Step 3 的财报准备层
python scripts/prepare_financial_report_skill.py --date <cur_date> --sync-first --json

# Step 5
python scripts/run_daily_pipeline.py --date <cur_date> --max-workers 6 --all-books
```

- Step 3 的宏观一路：重新派发同一个 subagent 提示词。
- Step 4：只对失败的那几只股票重新派发角色，已经落盘并注册的股票不要重做。

排查顺序：先读 `logs/runs/<日期>/<流程>/` 下对应组件日志和 `merged.log`，再看 `logs/cron_daily_prep/latest_status.json`，最后才动代码。遵守 evidence-first 原则：**先拿到失败现场，再改东西**。

## 九、完成后的报告

向用户输出一个简表：`cur_date`、五个阶段各自的状态与实际耗时、Step 4 实际研究了哪几只股票（或"无新增财报"）、验收清单结果、任何缺口与其影响面。

然后只提示一句："当日数据与 01-04 输入产物已就绪，可以开始运行 `/auto-trading-fixed-tracked`。"

**不要**自动开始交易分析。
