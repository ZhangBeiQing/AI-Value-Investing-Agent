# 周度评分（fixed_tracked / universe）

这是独立的观察排名链路，读取既有当日研究包，五维各占 20%，不修改 `01-08` 或交易账本。支持 `fixed_tracked`（默认）与 `universe`（`data/universe/master_universe.json`）两个股票池。

## 输入与职责

- CLI：`scripts/run_weekly_scoring.py`。
- 派单、合并与排名：`services/ranking/weekly_scoring.py`。
- 逐股格式校验：`services/ranking/scoring_contract.py`。
- universe 池输入准备：`scripts/prepare_universe_scoring_inputs.py`（写 `skill_runs/{date}/universe/`，复用 artifact 缓存，不触碰 fixed_tracked 产物）。
- universe 财报前置门禁：`prepare_universe_scoring_inputs.py` 生成研究包前校验目标股票是否已有对应日期财报，缺失则按财报 skill 准备并硬失败；检测逻辑见 `services/research/financial_report.py::find_symbols_missing_financial_reports`。
- 评分规则：`configs/prompt_flow/fixed_tracked/weekly_scoring_policy.md`。
- 原始评分 JSON schema：`configs/prompt_flow/fixed_tracked/weekly_score.schema.json`。
- Agent 调度：`.codex/skills/weekly-stock-scoring/SKILL.md`。

指定 `--date` 表示已产生收盘数据的分析日，必须与输入快照日期相同。周末运行时应指定已有研究包对应的交易日，不自动减一天或使用上次成功日期。

输入位于 `{input_base_dir}/skill_runs/{date}/{pool_source}/`：快照 `02_basic_snapshot_payload.json`、逐股 `04_stock_research/*_{symbol}_{date}_research.md`。`fixed_tracked` 的宏观上下文取同目录 `01_global_context.md`，有 `run_manifest.json` 时按其中的 fixed_tracked 股票池生成，否则按当日快照股票集合。`universe` 的股票池来自 `{input_base_dir}/universe/master_universe.json`，其宏观上下文复用当日 `fixed_tracked/01_global_context.md`，可用 `prepare_universe_scoring_inputs.py` 独立准备输入。研究包缺失、为空或重复时逐股跳过，绝不回退到前一天。

## 财报前置门禁（universe 池）

`prepare_universe_scoring_inputs.py` 生成研究包前会先校验目标股票**在 `--date` 之前是否已有可用财报**（口径与 `get_financial_report_summary()` 一致：`financial_reports/*.md` 中能否选出一份 release_date ≤ cutoff；cutoff 含 1 天宽限，与研究包取财报的口径一致；ETF/基金无季度财报，不视为缺失）。

- 无缺失：正常生成快照与研究包。
- 有缺失：默认先自动运行财报准备层（`scripts/prepare_financial_report_skill.py --date … --symbols <缺失股> --sync-first`，纯 Python，只生成 workdir），写出缺失清单 `skill_runs/{date}/universe/_missing_financial_reports.json`，并以退出码 2 **硬失败**，阻止在缺财报的条件下生成研究包。
- 补齐缺失只能由**主 agent** 执行：按 `.codex/skills/financial-report-summary/SKILL.md` 为 `ready_items` 逐股撰写 `financial_reports/<YYYYMMDD>.md` 并注册 `summary_index.json`，然后重跑本脚本。
- 关闭门禁（不推荐）：`--no-ensure-financial-reports`；只跳过自动 prepare：`--no-prepare-missing-reports`。

说明：财报"生成"是 Agent 活（Industry Researcher / Expectation Scout / Financial Author 三角色），纯 Python 只能检测并准备 workdir，所以门禁在缺失时硬失败并要求主 agent 补齐，而不是脚本自己写出财报。

## 执行

```bash
source /home/zhangbeiqing/venv/ai_stock/bin/activate

# 生成派单，不启动 Agent，也不刷新数据
python scripts/run_weekly_scoring.py --date YYYY-MM-DD

# 按 skill 逐股派发 Agent；它们写入各自的 _score.json

# 格式校验，不改写排名
python scripts/run_weekly_scoring.py --date YYYY-MM-DD --validate-only

# 校验、计算综合分并排名
python scripts/run_weekly_scoring.py --date YYYY-MM-DD --merge-only

# 全宇宙池：先准备输入，再全程加 --pool-source universe
python scripts/prepare_universe_scoring_inputs.py --date YYYY-MM-DD --symbols 300502.SZ ...
python scripts/run_weekly_scoring.py --date YYYY-MM-DD --pool-source universe --symbols 300502.SZ --json
python scripts/run_weekly_scoring.py --date YYYY-MM-DD --pool-source universe --merge-only
```

默认 `--base-dir data` 同时决定输入与输出数据根目录。测试时可用独立输入根目录：

```bash
python scripts/run_weekly_scoring.py --date YYYY-MM-DD \
  --symbols 300476.SZ 600276.SH \
  --input-base-dir data --base-dir data/tmp_weekly_scoring --json
python scripts/run_weekly_scoring.py --date YYYY-MM-DD \
  --base-dir data/tmp_weekly_scoring --merge-only
```

上述日期和股票必须实际存在于当日对应池（fixed_tracked 或 master_universe）。输出位于 `{base_dir}/weekly_scoring/{date}/`，`--pool-source universe` 时位于 `{base_dir}/weekly_scoring/{date}/universe/`，二者互不覆盖。

## 校验与输出

原始评分顶层只能有 `symbol`、`stock_name`、`scores` 和 `composite_reason`。`scores` 只能有五组 `_score` 与 `_reason`；分数必须是 0—100 的整数，布尔值、浮点值、数字字符串和越界值均不接受。理由必须非空；不能包含 URL、Markdown 链接或脚注。禁止 Agent 输出 `composite_score`。重复 JSON 字段不会被静默覆盖。

校验不判断研究结论正确性，也不从自然语言理由自动推断估值档位。八档估值与行业研究质量仍由 Agent 按评分规则检查。

生成阶段保存 `scoring_manifest.json`，记录分析日、股票池、可派单任务、输入输出路径与 prompt，以及缺输入跳过项。合并只读此清单冻结的股票和当日 `{stock_name}_{symbol}_score.json`，按文件名核验股票身份，忽略清单外遗留文件。

合并输出：

- `weekly_scoring.json`：`items` 排名列表，每项包含原始五维分数与理由、代码计算的 `composite_score` 和 `rank`；以及权重、状态、股票覆盖、缺失和错误信息。
- `weekly_ranking.md`：综合排名、五维分数、逐股理由和未完成股票。

等权综合分保留一位小数；降序排序，同分按股票代码升序。原始逐股 JSON 不改写。状态为 `complete`、`partial` 或 `empty`。

格式错误或单股缺失不阻断其他股票排名。没有任何合法结果时仍写出明确的空榜以替换旧榜，CLI 返回 1；部分结果可用时合并返回 0，但会在结果与日志中列明缺项。只校验模式存在缺失、错误或跳过项时返回 1。缺少整个宏观/快照输入、日期错配或清单缺失则直接失败，不自动运行上游脚本。

重新生成派单会覆盖该日清单，但不会删除已有逐股结果；输入内容或规则发生变化后应重新派发对应股票，避免复用本日旧评分。清单不包含评分的输入内容哈希，不能自动判断同一天的内容修订是否需要重评。

## 验证

`tests/test_weekly_scoring.py` 覆盖当日池解析、隔离目录、缺输入、旧快照、重复或过期研究包、格式校验、身份检查、重复键、等权分数、确定性排序、部分成功和旧榜清理。

逐股评分文件采用“股票名称_代码_score.json”，如 `中兴通讯_000063.SZ_score.json`。名称中的文件系统保留字符替换为下划线。合并兼容旧的纯代码文件名，按文件名末尾代码核验 JSON 身份；同一代码出现多个文件时报告错误，不任意挑选或重复计分。

## 评分校准

评分规则改为单Agent四步：核对事实与披露时点、逐维比较净影响、先选档再选整数、复核分数与理由一致性。估值直接读取研究包§1.2，比较当前PE/PB与历史同口径PE/PB及分位，并分析历史价格高于/低于现价的累计占比；单档占比不作判断依据；不增加Python估值计算链。重大经营事件须进入未来盈利情景，高增长须拆分价格、销量、并表、基数和一次性损益；关键字段实际缺失时具体标注；已有估值数据必须用于判断，不以缺少DCF、分部利润或精确日频分位为由统一退回50分。每维输出事实、正面、负面、净判断与待核实摘要，最终JSON字段和五维等权保持不变。

这些约束旨在减少常见研究错误，并非已验证弱模型与强模型评分相同。需用同一批股票、同一知识截止、冻结输入和规则做盲测，比较事实错误、理由分数矛盾、重大风险遗漏和排名稳定性，不只看分数是否接近某个模型。
