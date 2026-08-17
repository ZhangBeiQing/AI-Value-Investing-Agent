# 仓库指南

## WHAT：项目概览

本项目是一个本地 `skill-only` 的 AI 股票研究与交易流水线。

- 当前日常交易口径：用户手动准备 `data/skill_runs/{date}/` 下的 `01-04` 输入产物，Agent 负责读取资料、完成逐股分析、生成 `05_decision.json`，并在人工确认后继续后续交易执行与总结归档
- `manage_daily_data`、`run_daily_pipeline`、`run_post_trade` 仍是仓库内保留的脚本入口，但不再是“开始今天股票交易”这个 skill 的默认自动执行步骤
- 当前主代码放在 `scripts/`、`services/`、`shared_data_access/`、`core/`
- `agent_tools/`、`tools/` 仍保留少量兼容层，但不再是新代码主落点
- 运行产物与缓存写入 `data/`，日志写入 `logs/`，规范与说明写入 `.codex/`、`docs/`

## WHY：设计目标

- 保持和旧版 `skill` 工作流兼容，稳定产出 `01-08` 文件
- 把外部数据访问统一收敛到 `shared_data_access`
- 把脚本入口与业务实现分离，便于本地 Agent、人工脚本和后续重构复用
- 把经验沉淀进 `rules/` 与 `skills/`，降低每次新 session 的上下文损耗

## 环境约束

- 当前环境是 Windows WSL，用户粘贴的 Windows 路径要转成 WSL 路径
  - 例如 `C:\temp\a.jpg` -> `/nt/c/temp/a.jpg`
- Python 虚拟环境：`source /home/zhangbeiqing/venv/ai_stock/bin/activate`
- 默认语言：始终使用简体中文回复用户

## 核心目录

- `scripts/`：CLI 入口
- `services/data_refresh/`：日常数据刷新编排
- `services/pipeline/`：`01-04` skill 输入产物生成
- `services/research/`：宏观、个股研究、财报、新闻整合
- `services/trading/`：交易执行与 `06-08` 汇总
- `shared_data_access/`：行情、财报、股本、公告的统一缓存入口
- `core/`：日志、运行态、通用基础设施
- `configs/prompt_flow/fixed_tracked/investment_policy.md`：fixed_tracked 全部 Agent 共用的核心投资策略
- `configs/prompt_flow/fixed_tracked/main_policy.md`：fixed_tracked 主 Agent 当前 Prompt 源
- `configs/prompt_flow/fixed_tracked/stock_analysis_policy.md`：fixed_tracked 个股辩论角色共同研究方法
- `configs/prompt_flow/skill_flow*.json`：short_book 与旧版兼容 Prompt flow
- `.codex/rules/`：当前主维护的规则目录，供 Codex 场景优先使用
- `.codex/skills/`：当前主维护的项目技能文档
- `.codex/commands/`：当前主维护的固定动作文档

## 核心命令

```bash
pip install -r requirements.txt
cp .env.example .env

# 日常入口：一键刷新「要分析的交易日」所需的全部 Python 链路数据，并打印后续 skill 清单
# --date 语义统一为「要分析的交易日」（默认 today）；周末/节假日请手动指定最近一个交易日
python scripts/refresh_all_for_date.py
python scripts/refresh_all_for_date.py --date 2026-04-21
python scripts/refresh_all_for_date.py --date 2026-08-09 --allow-non-trading-date  # 休市日补充宏观/新闻分析
python scripts/refresh_all_for_date.py --fresh-heavy   # 额外强刷财报结构化数据等重缓存

# 仅在需要手动准备或调试单步链路时使用
python scripts/manage_daily_data.py
python scripts/run_daily_pipeline.py --date YYYY-MM-DD
python scripts/run_post_trade.py --date YYYY-MM-DD
```

## 日期语义（统一口径）

- `--date` 在本项目主脚本中默认指「要分析的交易日」，即**收盘数据已经产生的那一天**。
- **日常节奏：当天晚上 9 点（A 股 15:00 收盘后）分析当天收盘，为下一交易日出预案。所以默认值就是 `today`。**
- **不要再出现 `today - 1` / 「默认昨天」这种口径，那是已废弃的旧设计。** 看到任何地方还写着减一，按本节口径改掉，不要照着它推导日期。
- 周末或节假日「今天」不是交易日时，需要手动指定最近一个交易日，例如周六补跑时传 `--date <上周五>`。
- 若确需在周末或节假日吸收休市期间新增的宏观与新闻信息，`refresh_all_for_date.py` 和 `run_daily_pipeline.py` 可显式传 `--allow-non-trading-date`，按该自然日生成研究与下一交易日预案；该参数不表示休市日可以成交，也不得用于放宽回测交易日约束。
- 不要出现「传明天的日期」这种用法。
- 因为是当天夜间运行，`akshare` 的当日行情/新闻偶有延迟：每轮必须校验该 `--date` 对应的产物真的落盘，**不得拿前一交易日的数据顶替**，否则次日预案会建立在过期收盘价上。

## Boundaries

### Always Do

- 改代码前先读相关文件，不要凭印象改结构
- 新业务逻辑优先写到 `services/`、`shared_data_access/`、`core/`
- 任何外部行情、财报、股本、公告抓取都优先走 `shared_data_access`
- 用户说"开始今天股票交易"时，优先按 `.codex/skills/auto-trading-daily-pipeline/SKILL.md` 执行，默认假设 `data/skill_runs/YYYY-MM-DD/` 的 `01-04` 已由用户手动准备完成
- 改主链路后至少给出对应验证证据：日志、输出文件或失败现场
- 新增或修改核心组件时使用统一日志入口，不要直接散落 `print`
- 需要联网搜索时优先使用 `WebSearch` 工具，不要使用 `WebFetch` 或其他工具

### Ask First

- 删除或重命名会影响 `01-08` 文件契约的字段、文件名、目录结构
- 修改 `configs/prompt_flow/skill_flow.json` 的语义而不保持旧产物兼容
- 新增第三方依赖、外部 API、系统级运行前提
- 大规模删除历史兼容层，尤其是 `agent_tools/`、`tools/` 中仍被调用的部分
- 修改真实交易落地规则、仓位计算规则、价格引用规则
- 在“开始今天股票交易”场景下，如 `01-04` 产物缺失或不完整，先和用户确认是否要补数据，不要直接代跑旧脚本链路

### Never Do

- 不要在 `services/`、`shared_data_access/`、`core/` 中直接访问 akshare 之外的散乱数据源而不经统一封装
- 不要用 `as_of_date` 裁剪抓取窗口，只能在读取阶段做时间截断
- 不要把运行产物、临时调试文件、日志直接塞进源码目录
- 不要用 `from x import *`
- 用户未明确要求时，不要因为“开始今天股票交易”自动执行 `manage_daily_data`、`run_daily_pipeline`、`run_post_trade`
- **禁止使用 Glob 工具搜索 `data/` 目录下的文件**。Glob 工具有 Bug：`data/` 下存在 `.git` 子目录会导致 Glob 对整个 `data/` 目录返回空结果。改用 `find` 或 `ls` 替代，例如 `find data/skill_runs -name "05_decision.json"` 或 `ls data/skill_runs/*/long_book/05_decision.json`

## Progressive Disclosure

| 任务 | 首选参考 |
| --- | --- |
| 开始今天股票交易 | `data/skill_runs/YYYY-MM-DD/`, `.codex/skills/auto-trading-daily-pipeline/SKILL.md` |
| 交易前准备当日全部数据 | `.codex/skills/daily-data-preparation/SKILL.md` |
| 早上一键刷数据 | `scripts/refresh_all_for_date.py`, `services/data_refresh/refresh_orchestrator.py` |
| 刷新每日数据（单步） | `scripts/manage_daily_data.py`, `services/data_refresh/`, `.codex/skills/extend-shared-data-access/SKILL.md` |
| 调整 `01-04` 产物 | `scripts/run_daily_pipeline.py`, `services/pipeline/`, `.codex/rules/skill-pipeline.md`, `.codex/skills/add-skill-pipeline-step/SKILL.md` |
| 增加研究/快照字段 | `services/research/`, `services/snapshot/`, `.codex/rules/shared-data-access.md` |
| 增加外部数据缓存 | `shared_data_access/`, `shared_financial_utils.py`, `.codex/skills/extend-shared-data-access/SKILL.md` |
| 调整交易后处理 | `scripts/run_post_trade.py`, `services/trading/`, `.codex/rules/skill-pipeline.md` |
| 排查主链路失败 | `logs/`, `latest_status.json`, `.codex/rules/testing.md` |
| 统一日志接入 | `core/logging.py`, `.codex/rules/code-style.md` |

## 数据与缓存规则

- 统一入口：外部数据访问必须通过 `SharedDataAccess.prepare_dataset()` / `ensure_symbol_data()`
- 缓存注册：新增缓存类型先改 `shared_data_access/cache_registry.py`
- 时间因果：抓取时面向真实时间拿足历史，回测或复盘只在读取阶段裁剪
- Symbol 传递：除纯字符串处理外，优先传 `SymbolInfo`
- 输出目录：写 `analysis/`、`pe_pb_analysis/` 等目录前要清旧文件，仅保留 `.cache_registry_meta.json`
- 每日 fresh 策略：由 `services/data_refresh/refresh_orchestrator.py` 唯一决定；每日刷新股票范围 = `TRACKED_A_STOCKS` ∪ `master_universe`；价格/财报/basic_info 归 `manage_daily_data`，公告归 `build-announcements`。上层模块不得私设 `force_refresh_*=True`，发现缓存过期时应向 orchestrator 反馈。详见 `docs/share_data_access/README.md`。

## 日志规则

- 新代码禁止在库代码里直接用 `print` 做运行日志
- 统一使用 `core.logging`：`get_logger()`、`init_component_logger()`、`init_tool_logger()`
- Logger 名称必须是业务语义明确的 PascalCase，如 `ManageDailyData`、`DailyPipeline`、`TradeSummary`
- 详细规范见 `.codex/rules/code-style.md`

## Rules

- `pre_commit_rule.md`：git 提交规则
- `code-style.md`：代码风格与统一日志规范
- `shared-data-access.md`：缓存、时间截断、SymbolInfo、数据访问统一入口
- `skill-pipeline.md`：`01-08` 产物契约、脚本分层与交易后处理约束
- `testing.md`：evidence-first 调试、最小复现、主链路验证要求

上述规则以 `.codex/rules/` 为主维护目录。

## Docs

- `docs/PROJECT_SYSTEM_SUMMARY.md`：系统整体架构、日常主流程、关键模块、缓存布局与交易汇总链路总览，适合新 session 或大改动前快速建立全局上下文

## Skills

- `daily-data-preparation`：交易 skill 之前的每日数据准备总调度，串联 `refresh_all_for_date` → MinerU 就绪 → 并发 2 个 subagent（宏观 / 新闻）与财报 prepare → 主 agent 亲自做逐股财报研究 → `run_daily_pipeline`，一次性产出 `01-04` 研究包。**已挂 crontab，周一至周五 21:03 自动运行**（`scripts/cron_daily_data_prep.sh`）
- `auto-trading-daily-pipeline`：三账本交易公共模板与调度说明，负责定义 fixed_tracked / short_book / long_book 的共用流程与串行执行原则
- `auto-trading-fixed-tracked`：固定股票池 `fixed_tracked` 的单账本交易分析与后处理 skill
- `auto-trading-short-book`：短期股票池 `short_book` 的单账本交易分析与后处理 skill
- `auto-trading-long-book`：长期股票池 `long_book` 的单账本交易分析与后处理 skill
- `backtest-fixed-tracked`：按历史交易日串行回放固定池多 Agent 决策，使用隔离账本和 D+1 开盘模拟成交
- `add-skill-pipeline-step`：新增或重构 `skill` 流水线步骤时使用
- `extend-shared-data-access`：新增数据源、缓存目录或指标依赖时使用
以上 skill 位于 `.codex/skills/`。

## Commands

- `review-skill-run`：检查某一天的 `skill` 运行产物、日志与交易后处理是否完整且一致

Commands 位于 `.codex/commands/`。它不替代 `rules` 或 `skills`，而是把高频动作写成统一入口。
