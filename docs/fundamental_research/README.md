# 季度基本面深度研究系统

更新日期：2026-11-09

## 1. 定位

`financial-report-summary` 当前执行流程为：

```text
Python 时间隔离准备
→ 细分产业链研究
→ 财报前预期重建
→ Financial Author 一次撰写（初稿即最终报告）
→ 主 Agent 质量门禁与注册
```

最终报告仍写入：

```text
data/stock_info/{stock_name}_{symbol}/financial_reports/YYYYMMDD.md
```

`summary_index.json` 字段契约不变。系统不生成真实交易动作，也不修改交易 `01-08`。

同一 Financial Author 另写同名 `YYYYMMDD.daily.json`，目标 3000–5000 非空白字符。
每日研究包默认使用通过原文哈希、股票身份、知识日期与正文结构校验的摘要，
缺少或失效时明确回退完整报告。完整报告保留；决定性交易数字按需回查原文。
新研究注册增加 `--require-daily-brief`，详细契约见 `configs/research/financial_daily_brief_contract.md`。

历史设计（质询与修订流程已停用，当前执行以 Skill 为准）见 [季度财报与全面基本面深度研究系统详细设计](quarterly_fundamental_deep_research_design.md)。

## 2. 准备命令

```bash
source /home/zhangbeiqing/venv/ai_stock/bin/activate
python scripts/prepare_financial_report_skill.py \
  --date YYYY-MM-DD \
  --sync-first \
  --json \
  --include-quant-prefilter
```

`--date` 是分析日。正式研究不要使用只供调试的 `--skip-market-context`。

如果最新财报已经登记、只想重建 workdir 评审新 Prompt，可增加：

```text
--force-reprepare
```

它不会覆盖最终财报，也不会修改 `summary_index.json`。

准备脚本不会调用整个 `run_daily_pipeline.py`，而是复用底层股票价格与估值服务。每只 ready 股票的 workdir 包含：

```text
01_latest_report.md
02_previous_report.md
03_report_analysis_prompt.md
04_future_outlook_prompt.md
05_agent_input.md
pre_announcement_market_context.md
current_market_context.md
valuation_framework.md
prior_fundamental_memory.md
existing_industry_research.md
manifest.json
research_outputs/
```

`05_agent_input.md` 同时声明当前公司的 `disclosures/md/` 与 `disclosures/pdfs/` 只读目录。正常研究不批量加载多年报告；只有最近两年趋势缺少可比期、口径变化、重大异常追溯或 Author 发现明确历史缺口时才回溯。先用 `find`/`rg` 在 Markdown 中定位，目标报告没有 Markdown 时再读取对应 PDF；本流程不会为历史回溯自动调用 PDF 转换（pymupdf4llm）。

## 3. 两个时间切面

- `pre_announcement_market_context.md`：只从财报前冻结的 `04_stock_research` 提取价格、估值、公告和年度一致预期，不读取交易记忆。
- `current_market_context.md`：分析日价格、增强估值和最新年度一致预期，并显式提醒估值可能仍使用旧财务基准期。

只有公告日期、没有时间时，保守使用前一市场日。财报前季度预期缺失时不得用年度同花顺预测替代。

一致预期继续兼容：

```text
profit_forecast/profit_forecast.csv
```

每次刷新同时保存不可变日快照：

```text
profit_forecast/snapshots/YYYYMMDD.csv
```

## 4. 固定研究规则

单一规则源位于：

```text
configs/research/
├── company_valuation_framework.md
├── industry_chain_research_policy.md
├── financial_fundamental_research_policy.md
├── expectation_gap_research_policy.md
├── web_research_policy.md
└── financial_report_output_schema.md
```

`05_agent_input.md` 只保存本次股票、日期、输入路径、角色边界和输出路径。主 Agent 派发角色时只传路径，不复述固定规则。

## 天花板情景与价格比较

Industry Researcher 先独立形成经营情景和兑现阶段倍数，隔离价格上下文与历史估值结论；
Author 再结合本期财报核验并比较当前市值。倍数需有增长、资本回报、现金质量、竞争及资本成本依据，
并给出固定利润的倍数敏感性。业务扩展、兑现年份、稀释和分红口径在 §15 写清。
Bull、Bear 与 Adjudicator 对有深研的公司股票完整读取 §15，独立复核适用性和参数；经营变化可触发季度之间的重估。

产业研究与 `monthly-industry-research` 共享 `industry_chain_research_policy.md`，但财报流程不调用完整月度 skill。

## 5. 搜索规则

Codex + GPT 优先使用自带联网搜索，其他环境或模型组合优先使用阿里云百炼搜索 MCP；首选不可用时可使用会话已提供的另一入口，具体遵循 `configs/research/web_research_policy.md`。搜索结果只用于语义召回，重大数字仍需打开公司公告、交易所文件、协会材料或其他原始页面核对。

所有可用搜索入口均不可用或被禁用时默认失败关闭，不自行拼接爬虫或第三方检索 API 替代。

## 6. 研究产物与单写者

```text
research_outputs/
├── industry_chain_research.md
└── pre_announcement_expectations.md
```

- Industry Researcher 写产业链研究；
- Expectation Scout 写财报前预期；
- Financial Author 一次撰写直接写最终报告；
- 主 Agent 通过脚本更新 `summary_index.json`。

## 7. 注册

```bash
python scripts/register_financial_report_summary.py \
  --symbol SYMBOL \
  --path data/stock_info/{stock_name}_{symbol}/financial_reports/YYYYMMDD.md \
  --require-deep-research
```

`--require-deep-research` 会检查初稿、质询、最终文件和 high 问题闭环。门禁失败时不会更新 `summary_index.json`。

## 8. 下游使用

- `services/pipeline/steps/build_stock_research.py` 把最近完成的基本面报告嵌入 `04_stock_research`；
- 每日 Bull、Bear、Juror 与 Finalizer 通过研究包读取基本面结论；
- 每日交易角色仍必须结合当日 `Valuation Report`、最新价格与联网信息独立决策。

关键计算由 Author 使用 Python/Decimal 实算并核对期间和单位；混合公司检查分部与集团边界，防止集团利润估值后重复加入分部价值。全年FCF、季度桥接与情景表的正文继承同步核对。修订原文后重新绑定摘要；算术修正不表示公司经营新增恶化，也不表示未经核验的其他假设获得事实认可。
