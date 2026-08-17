---
name: daily-data-preparation
description: >
  交易 skill 之前的每日数据准备总调度，设计运行时点是当天晚上 9 点、分析当天收盘、为下一交易日
  出预案（日期默认 today，不做减一）。当用户说“开始今天的数据准备”、“准备今天的交易数据”、
  “跑今天的盘前数据链路”、“刷新今天全部数据并生成研究包”或类似请求时使用。串联
  refresh_all_for_date → MinerU 就绪 → 并发 2 个 subagent（宏观总结 / 渐进式新闻总结）
  与财报 prepare 脚本 → 主 agent 亲自做逐股财报研究 → run_daily_pipeline，在一个会话里
  一次性跑完约 50-90 分钟的长链路，产出 01-04 研究包，供 auto-trading-fixed-tracked 直接消费。
---

# Daily Data Preparation

## 边界

- 本 skill **只准备数据与输入产物**，不生成任何交易决策、仓位、目标价，不写 `05_decision.json`。
- 完成后**不自动触发** `auto-trading-fixed-tracked`；只向用户报告"数据已就绪"，由用户决定何时开始交易。
- 不修改 `configs/stock_pool.py`、投资策略文件、账本或历史持仓。
- 三个子 skill（`daily-macro-summary`、`gradual-hot-news-summary`、`financial-report-summary`）的内部规则由**各自的 SKILL.md 定义**，本 skill 只负责调度、门禁和验收，**不得转述、压缩或改写它们的规则**。
  - 前两个派给 subagent 执行，派发时只传日期与任务句子。
  - `financial-report-summary` 由**主 agent 直接执行**（原因见「阶段总览」下的说明），执行前要完整读它自己的 SKILL.md 和角色模板。
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

确定 `cur_date` 后把它固定下来，后续所有命令、路径、subagent 提示词都用同一个值，不要中途改。

## 环境前置

```bash
source /home/zhangbeiqing/venv/ai_stock/bin/activate
```

- 工作目录固定为仓库根目录，所有命令用相对路径 `scripts/xxx.py`。
- 联网搜索优先使用阿里云百炼 `bailian_web_search` MCP；实际 MCP 前缀可能不同，先确认能力存在。不得静默换成别的搜索引擎。
- 用户粘贴的 Windows 路径要转成 WSL 路径。

## 超时铁律（本 skill 最容易踩的坑）

本流程有三条长命令，分别约 **15 / 10-30 / 5-15 分钟**。天真地前台跑必然被截断。**怎么跑取决于运行时**，先认清你自己在哪个运行时里，下文统一称为「**按长命令方式启动**」：

| 运行时 | Bash 工具能力 | 长命令怎么启动 |
| --- | --- | --- |
| **opencode**（cron 默认） | 无后台参数，但 `timeout` 上限几乎无限 | 前台跑，`timeout` 设 **≥ 3600000ms（1 小时）**。这也是 `~/.config/opencode/AGENTS.md` 规则四的硬要求 |
| **Claude Code** | `timeout` 上限 600000ms（10 分钟），有 `run_in_background` | 用 `run_in_background: true`，**不要**试图加大 `timeout` |

判断方法：看你的 bash 工具 schema 里有没有 `run_in_background` 参数——有就是 Claude Code，没有就是 opencode。

两个运行时共同的铁律：

- 一个命令**只启动一次**。看不到输出 ≠ 卡死；重复启动会造成缓存写入竞争。
- 想看中途进度时，`tail` 对应的组件日志，**绝不重启命令**。
- 不要用 `sleep` 循环轮询。Claude Code 下后台任务退出时 harness 会通知你；opencode 下前台等待本身就是在等它返回。
- **绝不因为"等太久"就中断、跳过步骤、缩小范围或提前进入下一阶段。**

派发 subagent 的工具名也随运行时不同：Claude Code 叫 `Agent`，opencode 叫 `task`。下文统称「派发 subagent」。

## 阶段总览

| 阶段 | 内容 | 约耗时 | 阻塞关系 |
| --- | --- | --- | --- |
| Step 1 | `refresh_all_for_date.py` | ~15 min | 全流程根依赖 |
| Step 2 | MinerU `/health` 就绪 | 0-5 min | 可与 Step 1 **并行**；Step 4 的硬前置 |
| Step 3 | 并发 2 个 subagent（宏观 / 新闻）**+** 后台跑财报 prepare 脚本 | 10-30 min | 必须等 Step 1 成功 |
| Step 4 | **主 agent 亲自**做逐股财报研究 | 10-40 min | 必须等 Step 3 三项全部结束 |
| Step 5 | `run_daily_pipeline.py` | 5-15 min | 必须等 Step 4 完成 |

Step 1 与 Step 2 在同一条消息里一起发出；Step 3 的两个 subagent 和 prepare 脚本也在同一条消息里一起发出。

### 为什么财报不派给 subagent（重要，不要"优化"回去）

`financial-report-summary` 自己就要为**每只股票**派发 Industry Researcher / Expectation Scout / Financial Author / Research Challenger 四个角色。5 只股票就是 20 个 subagent。

嵌套派发在技术上是能跑通的（已实测，含无头模式），但**并发账会算不清**：财报那 20 个 + 宏观 + 新闻同时在跑，会超过平台每批的并发上限，而超限的失败方式是**静默空返回**——subagent 报告"完成"，文件却是空的或没写。`auto-trading-fixed-tracked` 里那条「每批最多 10 个，不得超过，以避免任务空返回、写错日期目录或文件缺失」就是同一个坑。

所以规则是：**需要再派发一层 subagent 的环节，只能由主 agent 直接做。** 财报研究必须留在主 agent 手里，且必须等 A/B 两路结束、并发腾空之后才开始。

反过来，`prepare_financial_report_skill.py` 是**纯 Python，自己不派发任何 subagent**，所以它可以和 A/B 并行跑，不占 subagent 并发额度。

## 一、Step 1：一键刷新全部 Python 链路数据

```bash
python scripts/refresh_all_for_date.py --date <cur_date>
```

- 按「超时铁律」的**长命令方式**启动。
- 休市日补充分析时追加 `--allow-non-trading-date`。
- 用户明确要求强刷财报结构化等重缓存时才追加 `--fresh-heavy`。
- **不要**加 `--continue-on-failure`：默认失败即停，才能暴露真实断点。
- 组件日志：`logs/main_scripts/RefreshAllForDate/refresh_all_for_date_*.log` 与同目录 `merged.log`。

覆盖范围（`TRACKED_A_STOCKS ∪ master_universe`）：价格、财报结构化缓存、宏观客观面板、全市场新闻采集/去重/增强、板块热度、因子库与因子评分、量化初筛、清理旧研究缓存。

### 进入 Step 3 的硬门禁

必须**同时**满足：退出码为 0，且下列文件全部存在（`<cur_date>` 为实际日期）：

```text
data/global_cache/macro_objective_panel/daily_snapshots/<cur_date>.json
data/selection_runs/<cur_date>/03_news_prompt_input.json
data/selection_runs/<cur_date>/05_board_heat_state.json
data/selection_runs/<cur_date>/12_quant_prefilter_short.csv
```

任一缺失就不要进入 Step 3：先读日志定位失败步骤，向用户报告失败现场（日志路径 + 报错原文），询问是补数据还是跳过。**不要**在缺输入的情况下让 subagent 空跑——它们会基于过期数据产出看起来正常的错误结论。

> 校验用 `find` / `ls`，**不要用 Glob 搜 `data/`**（`data/` 下有 `.git` 子目录，Glob 会整体返回空）。

## 二、Step 2：MinerU 就绪（与 Step 1 并行）

MinerU 负责把财报 PDF 转成 Markdown，是 Step 3 里 prepare 脚本的硬前置。

先检查健康状态：

```bash
curl -s -m 5 http://127.0.0.1:8000/health
```

- 返回 `"status": "healthy"` 且 `protocol_version >= 2` → 已就绪，跳过启动。
- 否则按「超时铁律」的**长命令方式**启动，并把输出重定向到日志（MinerU 是常驻进程，opencode 下要放在后台，不能占死会话）：

```bash
scripts/start_mineru_api.sh >> logs/mineru_start.log 2>&1
```

- 启动后每 30-60 秒 `curl` 一次 `/health` 直到 `healthy`。首次加载 VLM 模型可能要几分钟，属正常。
- 启动脚本是 `exec` 前台常驻进程，**必须**后台启动，否则会占死会话。
- 若脚本自身报错退出（`mineru-api` 不存在、CUDA 13 缺失），读 `logs/mineru_start.log` 拿到原文，报告给用户并询问怎么办。**不要**因此跳过财报链路，也不要自己改 venv / CUDA 路径。
- 也可用 `ps aux | grep mineru-api` 辅助确认进程，但**以 `/health` 为准**。

## 三、Step 3：2 个 subagent + 并行跑财报 prepare

Step 1 门禁通过后，**在同一条消息里一次性发出三件事**：两个 subagent 派发 + 一个长命令 Bash。不要逐个串行等待。

- 用运行时的通用 subagent 类型（Claude Code 下 `subagent_type: claude`；opencode 下用默认 agent，不要指定 `fixed-tracked-*` 那些专用角色）。
- 两个 subagent 之间没有依赖，允许任意顺序完成。
- 派发提示词逐字使用 [subagent 派发模板](references/subagent-prompts.md)，把 `<cur_date>` 替换成实际日期。**不要**自己改写任务句子：那些句子就是子 skill 的触发语。
- **只派发这两个。** 不要在这里派发第三个做财报的 subagent（原因见「阶段总览」下的说明）。

| Subagent | 触发 skill | 约耗时 | 主产出 |
| --- | --- | --- | --- |
| A 宏观总结 | `daily-macro-summary` | 5-10 min | `data/macro_economy/<YYYYMMDD>.md` |
| B 渐进式新闻总结 | `gradual-hot-news-summary` | ~20 min | `data/selection_runs/<cur_date>/06_hot_news_state.json` |

同时，主 agent 自己按「超时铁律」的**长命令方式**启动财报准备脚本：

```bash
python scripts/prepare_financial_report_skill.py \
  --date <cur_date> --sync-first --json --include-quant-prefilter
```

- 约 10-30 分钟，主要耗时是把新发布的财报 PDF 转 Markdown（走 MinerU，所以 Step 2 必须已就绪）。
- 它是纯 Python，**不派发任何 subagent**，所以和 A/B 并行不占并发额度。
- 组件日志：`logs/research/PrepareFinancialReportSkill/`。
- 正式研究**不得**加 `--skip-market-context`。

### 三项都结束后

先逐个核对产出文件是否真的落盘，**不要只看 subagent 的自述文字**。

- A/B 有任一路失败 → 重新派发那一路（两个任务彼此独立且幂等）。
- 重试仍失败 → 报告失败现场，并说明"若带着这个缺口继续，`01_global_context.md` 会缺哪一块"。
- prepare 脚本失败 → 读组件日志定位；若是 MinerU 连不上，先 `curl /health` 确认，不要重启正在转换的 MinerU。

三项齐了才进入 Step 4。此时 A/B 的 subagent 已经退出，subagent 并发额度腾空，才能开始派发财报角色。

## 四、Step 4：主 agent 亲自做逐股财报研究

**这一步由主 agent 直接执行 `financial-report-summary`，不要整体丢给一个 subagent。**

开始前完整读取 `.codex/skills/financial-report-summary/SKILL.md` 及其 `references/role-prompts.md`，然后按那份 skill 的规则执行。本 skill 不重复它的内部规则，只约束调度方式：

- 从 Step 3 prepare 脚本的 JSON 输出里取 `ready_items`，**只处理 ready 的股票**；`skipped_items`（已总结或无新财报）不启动任何角色。
- 当天没有公司发新财报时，`ready_items` 为空是**合法结果**：跳过本步，在最终报告里写明"无新增财报"，不要硬造研究。
- 逐股派发角色时，**每批最多 10 个 subagent**。比如 5 只股票 × 4 个角色 = 20 个，必须拆成至少 2 批：上一批全部结束、目标文件全部落盘并校验通过后，才能起下一批。
  - ⚠️ **这条优先于 `~/.config/opencode/AGENTS.md` 规则三**（那条要求"Subagent 必须全量并发、禁止分批"）。在本 skill 的 Step 4 里，以"每批最多 10 个"为准。
  - 理由：`auto-trading-fixed-tracked` 记录了超限的真实后果——**任务空返回、写错日期目录、文件缺失**。这类失败是静默的，跑完看起来正常，文件却是空的，比慢一点危险得多。
- 同一细分产业链的多只股票共用一个 Industry Researcher；不同产业链各自独立，不得互相借用。
- 派发任何需要联网的角色前，先确认 `bailian_web_search` 能力存在。
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
- 休市日补充分析时追加 `--allow-non-trading-date`。

期望产出：

```text
data/skill_runs/<cur_date>/
├── run_manifest.json
└── fixed_tracked/
    ├── 01_global_context.md            # 宏观 / 大盘 / 新闻
    ├── 02_basic_snapshot_payload.json  # 个股快照
    ├── 03_agent_input.md               # 投资策略 + P0 筛选输入
    ├── 03_stock_analysis_input.md      # 个股辩论研究方法
    └── 04_stock_research/              # 固定池 / 持仓 / 短期量化股 / 长期候选的逐股研究包
```

## 六、验收清单

五步跑完后逐项确认（用 `find` / `ls`，不用 Glob）：

- [ ] `data/global_cache/macro_objective_panel/daily_snapshots/<cur_date>.json` 存在
- [ ] `data/selection_runs/<cur_date>/12_quant_prefilter_short.csv` 存在且非空
- [ ] `data/macro_economy/<YYYYMMDD>.md` 的正文日期确实是 `cur_date`
- [ ] `data/selection_runs/<cur_date>/06_hot_news_state.json` 存在，且 `run_date` == `cur_date`
- [ ] 财报 `summary_index.json` 已登记本轮新增股票（无新财报时"零新增"也是合法结果）
- [ ] Step 4 的 `ready_items` 逐只都有对应的 `financial_reports/<YYYYMMDD>.md` 落盘；**空文件视为失败**（并发超限的静默空返回就长这样）
- [ ] `data/skill_runs/<cur_date>/run_manifest.json` 存在
- [ ] `data/skill_runs/<cur_date>/fixed_tracked/` 下 `01`、`02`、`03`、`03_stock_analysis_input`、`04_stock_research/` 齐全
- [ ] `04_stock_research/` 的逐股目录数量与量化初筛 + 固定池 + 持仓的并集量级相符，没有大面积缺失

宏观总结的日期核对不能只看文件名：`daily-macro-summary` 允许以前一日文件为模板增量更新，要确认正文头部的数据口径日期也改过来了。

## 七、定时触发（已配置）

本 skill 已挂到系统 crontab，**周一至周五 21:03 自动运行**，无需人工触发：

```text
3 21 * * 1-5 /home/zhangbeiqing/programer/AI-Value-Investing-Agent/scripts/cron_daily_data_prep.sh
```

- 包装脚本：`scripts/cron_daily_data_prep.sh`，用 `claude -p --dangerously-skip-permissions` 无头运行。
- 脚本内有**交易日历门禁**：非交易日（含节假日）直接 SKIP，不会拉起 agent 空跑，所以 crontab 的 `1-5` 只是少几次无用唤醒。
- 运行状态：`logs/cron_daily_prep/latest_status.json`（`success` / `skipped` / `timeout` / `failed`）。
- 当轮完整日志：`logs/cron_daily_prep/<cur_date>.log`。
- 整体上限 9000 秒（2.5 小时），超时会被 `timeout` 终止并记 `timeout` 状态。

无人值守运行时**没有人能实时回答提问**：遇到需要确认的地方按默认路径继续，把待确认事项写进最终报告，不要停下来等人。仍然不得触发交易 skill、不得生成或修改 `05_decision.json`。

排查 cron 没跑的顺序：先看 `latest_status.json` 的 `run_date` 是不是当天 → 再 `systemctl status cron` 确认服务在跑 → 最后确认 WSL 当时没被关掉（`wsl --shutdown` 或 Windows 关机期间 cron 不会触发，且**不会补跑**，需要手动传日期补一轮）。

`node` 升级后 `claude` 的绝对路径会变，需要同步改 `scripts/cron_daily_data_prep.sh` 里的 `NODE_BIN`。

## 八、失败与重跑

五个阶段各自幂等，失败后只重跑失败的那一段，不要从头重来：

```bash
# Step 1
python scripts/refresh_all_for_date.py --date <cur_date>

# Step 3 的财报准备层
python scripts/prepare_financial_report_skill.py --date <cur_date> --sync-first --json --include-quant-prefilter

# Step 5
python scripts/run_daily_pipeline.py --date <cur_date> --max-workers 6 --all-books
```

- Step 3 的 A / B 两路：重新派发同一个 subagent 提示词。
- Step 4：只对失败的那几只股票重新派发角色，已经落盘并注册的股票不要重做。

排查顺序：先读 `logs/` 下对应组件日志的**最新一份**和 `merged.log`，再看 `latest_status.json`，最后才动代码。遵守 `.codex/rules/testing.md` 的 evidence-first 原则：**先拿到失败现场，再改东西**。

## 九、完成后的报告

向用户输出一个简表：`cur_date`、五个阶段各自的状态与实际耗时、Step 4 实际研究了哪几只股票（或"无新增财报"）、验收清单结果、任何缺口与其影响面。

然后只提示一句："当日数据与 01-04 输入产物已就绪，可以开始运行 `/auto-trading-fixed-tracked`。"

**不要**自动开始交易分析。
