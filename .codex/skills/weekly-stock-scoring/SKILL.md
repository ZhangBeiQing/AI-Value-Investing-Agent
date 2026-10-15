---
name: weekly-stock-scoring
description: 每周末对全池 fixed_tracked 股票进行轻量评分排名，当用户要求“每周股票打分”“运行周度评分”或“生成固定池评分排名”时使用，用于生成投资吸引力排名供挑选使用。agent 基于结构化数据包 + 联网搜索对 5 个维度打分，代码按权重组合综合分并生成排名表。
---

# 每周全池轻量评分工作流

## 1. 职责

对股票池做一次性轻量评分，产出全局投资吸引力排名。只用于周末全池概览和发现需要关注的标的。支持两个池：

- `fixed_tracked`（默认）：当日 `run_manifest.json` 的固定跟踪池。
- `universe`：`data/universe/master_universe.json` 的全宇宙股票。

每只股票只消耗 1 个 general subagent 调用。按评分规则执行“核对事实与时点 → 逐维比较正负净影响 → 先选档再选分 → 检查理由与分数一致”四步；不增加反驳角色或额外模型轮次。

## 2. 前置输入

**该日期必须已生成 `01_global_context.md`、`02_basic_snapshot_payload.json` 与 `04_stock_research/*_research.md`**。快照日期必须与分析日一致。`fixed_tracked` 池输入由 `run_daily_pipeline.py` 生成，默认范围为当日 `run_manifest.json` 的池，没有 manifest 时使用当日快照的股票。`universe` 池输入位于 `data/skill_runs/{date}/universe/`，由 `scripts/prepare_universe_scoring_inputs.py` 独立准备（快照 + 研究包），宏观上下文复用当日 `fixed_tracked/01_global_context.md`；该脚本只写 universe 目录，不触碰 fixed_tracked 交易产物。输入缺失时报告失败，请用户先准备，不自动刷新或代跑 pipeline。universe 池准备前还有**财报前置门禁**：目标股票缺对应日期财报时脚本硬失败，须先按 `.codex/skills/financial-report-summary` 补财报（详见「第一步」下的说明）。周评分直接复用全量研究包（§1 估值 + §2 新闻 + §3 财报/一致预期/基本面深研 + §4 持仓记忆）与宏观上下文，不再单独生成轻量研究包。

评分规则文件（subagent 必读）：

```text
configs/prompt_flow/fixed_tracked/weekly_scoring_policy.md
configs/prompt_flow/fixed_tracked/weekly_score.schema.json
```

## 3. 流程

### 第一步：生成 subagent prompt

```bash
python scripts/run_weekly_scoring.py --date {date}
```

脚本会：

1. 校验 `01_global_context.md` 存在，否则提示先跑 `run_daily_pipeline.py`
2. 为每只股票定位全量研究包 `04_stock_research/*{symbol}*{date}*_research.md`，缺失则跳过并 warning
3. 生成 subagent 派单 prompt（含研究包路径 + 宏观上下文路径 + 评分规则路径 + JSON schema + 输出路径），持久化 `scoring_manifest.json`
4. 打印所有 prompt 供主 Agent 派单

可选参数：

- `--symbols 300476.SZ 600276.SH`：只评当日池内的指定股票（测试或分批用）
- `--pool-source fixed_tracked|universe`：评分股票池来源，默认 `fixed_tracked`；`universe` 走 master_universe 全宇宙
- `--base-dir data/tmp_weekly_scoring`：数据根目录，默认同时从这里读取 `skill_runs` 并写入 `weekly_scoring`
- `--input-base-dir data`：单独指定输入数据根目录，可读取正式研究包并将结果写入隔离的 `--base-dir`
- `--generate`：显式生成派单，与不传模式参数相同
- `--json`：输出结构化派单结果

全宇宙评分前先准备当日输入（`--limit` 可只准备前 N 只，分批时按同一批 `--symbols` 准备）：

```bash
python scripts/prepare_universe_scoring_inputs.py --date {date}              # master_universe 全部
python scripts/prepare_universe_scoring_inputs.py --date {date} --symbols 300502.SZ ...  # 指定分批
```

**财报前置门禁（universe 池）**：该脚本在生成研究包前会校验目标股票在 `{date}` 之前是否已有可用财报（`financial_reports/*.md`，口径与研究包一致，ETF 不要求）。

- 有缺失时脚本默认先自动运行财报准备层（`python scripts/prepare_financial_report_skill.py --date {date} --symbols <缺失股> --sync-first --json`，纯 Python，只生成 workdir），写出 `data/skill_runs/{date}/universe/_missing_financial_reports.json`，并以**退出码 2 硬失败**，此时不得继续派发评分。
- 缺失必须由**主 Agent 亲自**补齐：完整读取 `.codex/skills/financial-report-summary/SKILL.md`，对 ready 股票派发 Industry Researcher / Expectation Scout / Financial Author 撰写 `data/stock_info/{name}_{symbol}/financial_reports/<YYYYMMDD>.md`，再用 `scripts/register_financial_report_summary.py` 注册 `summary_index.json`（每批最多 10 个 subagent）。
- 补齐并落盘校验通过后，重跑 `prepare_universe_scoring_inputs.py` 生成研究包，再进入第二步。
- 关闭门禁用 `--no-ensure-financial-reports`，只跳过自动 prepare 用 `--no-prepare-missing-reports`；正式流程不要关。

### 第二步：主 Agent 并发派发 subagent

主 Agent 为每只股票并发发一个 `general` subagent，prompt 由第一步脚本生成。主 Agent 原样转交脚本生成的 prompt（已持久化到 `scoring_manifest.json`），包含以下路径：

- 评分规则文件路径：`configs/prompt_flow/fixed_tracked/weekly_scoring_policy.md`
- 该全量研究包路径：fixed_tracked 池为 `data/skill_runs/{date}/fixed_tracked/04_stock_research/...`，universe 池为 `data/skill_runs/{date}/universe/04_stock_research/...`
- 宏观上下文路径：`data/skill_runs/{date}/fixed_tracked/01_global_context.md`（两个池共用）
- 唯一输出路径：fixed_tracked 池为 `data/weekly_scoring/{date}/{stock_name}_{symbol}_score.json`，universe 池为 `data/weekly_scoring/{date}/universe/{stock_name}_{symbol}_score.json`

**并发限制**：同一批最多 10 个 subagent，且不得超过当前平台可用并发额度。

估值直接完整读取研究包 `### 1.2 Valuation Report (Markdown)`，按规则解释各张表；不另加Python计算或抓取层。必须比较当前PE/PB与历史同口径序列或百分位，以及历史价格高于/低于现价的累计占比。单档概率不作判断依据。已有数据必须用于具体判断，不能因为缺DCF或精确日频分位就退回“估值待核实”；真实缺口指出具体字段。旧报告与新披露冲突时先更新事实，再打分。

实际 prompt 由 `build_subagent_prompt()` 生成，包含分析日、股票身份、规则、全量研究包、宏观上下文、schema 和唯一输出路径。它要求完整阅读、按分析日截止搜索、不输出综合分与交易动作；主 Agent 直接原样派发，不另写一套评分方法。

每个 subagent 输出到（universe 池多一层 `universe/` 子目录）：

```text
data/weekly_scoring/{date}/{stock_name}_{symbol}_score.json
data/weekly_scoring/{date}/universe/{stock_name}_{symbol}_score.json
```

### 第三步：合并排名

所有 subagent 完成后（universe 池同样加 `--pool-source universe`）：

```bash
python scripts/run_weekly_scoring.py --date {date} --merge-only
python scripts/run_weekly_scoring.py --date {date} --pool-source universe --merge-only
```

脚本会：

1. 读取所有 `*_score.json` 文件
2. 按 `weekly_score.schema.json` 校验五维字段、整数范围、非空理由和股票代码；拒绝多余字段、自填综合分、重复 JSON 键与最终理由中的引用链接
3. 按等权 (各 20%) 计算综合分 — 5 维算术平均，机械计算
4. 按综合分降序排名，同分按标准股票代码升序，确保重复运行结果稳定
5. 输出 `weekly_scoring.json`（程序消费）和 `weekly_ranking.md`（人读），同时列出缺失、错误和缺输入跳过的股票

可先仅检查格式，不改写排名文件：

```bash
python scripts/run_weekly_scoring.py --date {date} --validate-only
```

合并只读取当日 `scoring_manifest.json` 中已派单的股票；清单外旧文件不进入排名。部分合法结果允许合并；没有任何合法结果时写出空榜并返回非零退出码，防止保留旧榜冒充当前结果。只校验模式发现错误、缺失或跳过时返回非零退出码。

## 4. 评分维度与权重

| 维度 | 权重 | 数据来源 |
| --- | --- | --- |
| 个股宏观市场情绪 | 20% | `01_global_context.md`（宏观/利率/流动性/政策信号/战争）+ 联网搜索 |
| 估值 | 20% | 研究包 §1.2（当前PE/PB与历史PE/PB对比、价格高于/低于现价占比，结合盈利变化按八档判断）+ 联网搜索 |
| 量价 | 20% | 研究包 §1.1（量价/回撤/波动/K线）+ 联网搜索（股东数/筹码/机构加减仓） |
| 新闻与未来预期 | 20% | 研究包 §2 + §3 一致预期/Forecast + 联网搜索 |
| 基本面 | 20% | 研究包 §3 基本面深研（完整阅读，按行业特性判断，不套 ROE 阈值）+ 联网搜索 |

权重定义在 `services/ranking/weekly_scoring.py` 的 `REFERENCE_WEIGHTS` 中（等权各 0.20），综合分机械等权平均。

研究包复用 `run_daily_pipeline.py` 生成的全量 `04_stock_research/*_research.md`，包含：

- §1 股票指标与估值（Price Report JSON + PE/PB/PS 估值报告、历史分位、三年半净利润增速、最高/最低股价、区间分布）
- §2 新闻与公告
- §3 财报摘要、机构一致预期与 Forecast + 基本面深研（17 章：核心结论/业务全景/利润归因/现金流/产业链地位/证伪条件等）
- §4 持仓与投资逻辑记忆

宏观上下文 `01_global_context.md`：宏观总结/利率面板/央行流动性/政策信号/持续性风险跟踪/资产映射。

## 5. 产物

```text
data/weekly_scoring/{date}/
├── scoring_manifest.json    # 当日股票范围、输入输出路径与派单 prompt
├── {stock_name}_{symbol}_score.json       # 每个 subagent 的原始评分
├── weekly_scoring.json       # 合并后的排名数据（程序消费）
├── weekly_ranking.md         # Markdown 排名表（人读）
└── universe/                 # 仅 --pool-source universe 时使用
    ├── scoring_manifest.json
    ├── {stock_name}_{symbol}_score.json
    ├── weekly_scoring.json
    └── weekly_ranking.md
```

`weekly_ranking.md` 包含：

- 排名表（综合分 + 5 维度分数）
- 各维度理由明细

## 6. 与日常辩论的关系

|  | 每周轻量评分 | 日常多 Agent 辩论 |
| --- | --- | --- |
| 触发 | 每周末手动 | 每个交易日 |
| Agent 数/股 | 1 个 general | 6 个 (Bull/Bear/3Juror/finalizer) |
| 数据来源 | 全量研究包 + 01宏观 + 联网搜索 | 完整研究包 + 联网搜索 |
| 输出 | 5 维度分数 + 综合排名 | BUY/SELL/HOLD/FLAT + 完整决策底稿 |
| 用途 | 全池概览、发现关注标的 | 精确交易决策 |

每周评分的排名可以辅助主 Agent 在下一周选择 P0 时关注排名靠前的股票，但不替代独立的证据审查和辩论。

## 7. 失败处理

- **subagent 输出 JSON 格式错误**：`--merge-only` 会报告错误并跳过该文件
- **subagent 未完成**：缺失的文件不会阻断其他股票的排名生成
- **数据包缺失**：`--generate` 阶段跳过该股票并 warning
- **快照或宏观上下文不存在、日期不匹配**：退出，提示用户准备指定日期的 pipeline 产物；不得使用其他日期数据代替
- **重复研究包**：跳过该股票并 warning，不任意挑选一份
- **评分清单不存在**：校验或合并阶段退出，提示先生成派单
- **universe 池缺少当日快照记录或研究包**：该股票被跳过并 warning；universe 快照只覆盖已准备的股票，分批时按同一批 `--symbols` 准备
