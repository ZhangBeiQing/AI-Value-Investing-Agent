# 仓库指南

## WHAT：项目概览

本项目是一个本地 `skill-only` 的 AI 股票研究与交易流水线。

- 日常流程：`manage_daily_data` 刷新数据 -> `run_daily_pipeline` 生成 `01-04` 输入产物 -> 本地 Agent 读取 `data/skill_runs/{date}/` -> `run_post_trade` 执行 `05-08`
- 当前主代码放在 `scripts/`、`services/`、`shared_data_access/`、`core/`
- `agent_tools/`、`tools/` 仍保留少量兼容层，但不再是新代码主落点
- 运行产物与缓存写入 `data/`，日志写入 `logs/`，规范与说明写入 `.codex/`、`.claude/`、`docs/`

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
- `configs/prompt_flow/skill_flow.json`：当前主 flow
- `.codex/rules/`：当前主维护的规则目录，供 Codex 场景优先使用
- `.codex/skills/`：当前主维护的项目技能文档
- `.codex/commands/`：当前主维护的固定动作文档
- `.claude/`：兼容 Claude Code 的镜像目录，默认通过软链接指向 `.codex/`

## 核心命令

```bash
pip install -r requirements.txt
cp .env.example .env

python scripts/manage_daily_data.py
python scripts/run_daily_pipeline.py --date YYYY-MM-DD
python scripts/run_post_trade.py --date YYYY-MM-DD
```

## Boundaries

### Always Do

- 改代码前先读相关文件，不要凭印象改结构
- 新业务逻辑优先写到 `services/`、`shared_data_access/`、`core/`
- 任何外部行情、财报、股本、公告抓取都优先走 `shared_data_access`
- 改主链路后至少给出对应验证证据：日志、输出文件或失败现场
- 新增或修改核心组件时使用统一日志入口，不要直接散落 `print`

### Ask First

- 删除或重命名会影响 `01-08` 文件契约的字段、文件名、目录结构
- 修改 `configs/prompt_flow/skill_flow.json` 的语义而不保持旧产物兼容
- 新增第三方依赖、外部 API、系统级运行前提
- 大规模删除历史兼容层，尤其是 `agent_tools/`、`tools/` 中仍被调用的部分
- 修改真实交易落地规则、仓位计算规则、价格引用规则

### Never Do

- 不要在 `services/`、`shared_data_access/`、`core/` 中直接访问 akshare 之外的散乱数据源而不经统一封装
- 不要用 `as_of_date` 裁剪抓取窗口，只能在读取阶段做时间截断
- 不要把运行产物、临时调试文件、日志直接塞进源码目录
- 不要用 `from x import *`

## Progressive Disclosure

| 任务 | 首选参考 |
| --- | --- |
| 刷新每日数据 | `scripts/manage_daily_data.py`, `services/data_refresh/`, `.codex/skills/extend-shared-data-access/SKILL.md` |
| 调整 `01-04` 产物 | `scripts/run_daily_pipeline.py`, `services/pipeline/`, `.codex/rules/skill-pipeline.md`, `.codex/skills/add-skill-pipeline-step/SKILL.md` |
| 增加研究/快照字段 | `services/research/`, `services/snapshot/`, `.codex/rules/shared-data-access.md` |
| 增加外部数据缓存 | `shared_data_access/`, `shared_financial_utils.py`, `.codex/skills/extend-shared-data-access/SKILL.md` |
| 调整交易后处理 | `scripts/run_post_trade.py`, `services/trading/`, `.codex/rules/skill-pipeline.md` |
| 排查主链路失败 | `logs/`, `latest_status.json`, `.codex/rules/testing.md`, `.codex/skills/debug-skill-run/SKILL.md` |
| 统一日志接入 | `core/logging.py`, `.codex/rules/code-style.md` |

## 数据与缓存规则

- 统一入口：外部数据访问必须通过 `SharedDataAccess.prepare_dataset()` / `ensure_symbol_data()`
- 缓存注册：新增缓存类型先改 `shared_data_access/cache_registry.py`
- 时间因果：抓取时面向真实时间拿足历史，回测或复盘只在读取阶段裁剪
- Symbol 传递：除纯字符串处理外，优先传 `SymbolInfo`
- 输出目录：写 `analysis/`、`pe_pb_analysis/` 等目录前要清旧文件，仅保留 `.cache_registry_meta.json`

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

上述规则以 `.codex/rules/` 为主维护目录；`.claude/rules/` 默认是兼容镜像。

## Docs

- `docs/PROJECT_SYSTEM_SUMMARY.md`：系统整体架构、日常主流程、关键模块、缓存布局与交易汇总链路总览，适合新 session 或大改动前快速建立全局上下文

## Skills

- `add-skill-pipeline-step`：新增或重构 `skill` 流水线步骤时使用
- `extend-shared-data-access`：新增数据源、缓存目录或指标依赖时使用
- `debug-skill-run`：`manage_daily_data` / `run_daily_pipeline` / `run_post_trade` 失败时使用

以上三类 skill 位于 `.codex/skills/`，`.claude/skills/` 默认通过软链接复用它们。

## Commands

- `review-skill-run`：检查某一天的 `skill` 运行产物、日志与交易后处理是否完整且一致

Commands 位于 `.codex/commands/`，`.claude/commands/` 默认通过软链接复用它们。它不替代 `rules` 或 `skills`，而是把高频动作写成统一入口。
