---
name: financial-report-summary
description: 对固定跟踪池、长期候选与短期候选股票生成季度全面基本面深度研究。用户说“生成财报总结”“分析最新财报”“做季度基本面研究”或要求批量研究财报时使用。主 Agent 先准备财报前/当前市场上下文，再按细分产业链协调 Industry Researcher，并为每股启动 Expectation Scout、Financial Author、Research Challenger；Author 修订后写入兼容的 financial_reports/YYYYMMDD.md，最后由主 Agent 注册 summary_index.json。
---

# Quarterly Fundamental Research

## 边界

- 最终报告是基本面研究，不生成真实交易动作、仓位、目标价或止损。
- 财务事实不投票；使用“提出问题 → 找证据 → 修订”的闭环。
- 保持 `financial_reports/YYYYMMDD.md` 与 `summary_index.json` 现有契约。
- 各角色只写自己的文件；最终报告只有 Financial Author 写，`summary_index.json` 只有主 Agent 通过注册脚本写。

开始前完整读取 [角色派发模板](references/role-prompts.md)。派发 subagent 时只传角色、股票/产业链、workdir、模板路径和唯一输出路径；不得转述或压缩固定规则。

## 一、准备输入

```bash
source /home/zhangbeiqing/venv/ai_stock/bin/activate
python scripts/prepare_financial_report_skill.py \
  --date YYYY-MM-DD \
  --sync-first \
  --json \
  --include-quant-prefilter
```

`--date` 是本次分析日。准备脚本会为每股生成或更新：

```text
financial_report_workdir/
├── 01_latest_report.md
├── 02_previous_report.md
├── 03_report_analysis_prompt.md
├── 04_future_outlook_prompt.md
├── 05_agent_input.md
├── pre_announcement_market_context.md
├── current_market_context.md
├── valuation_framework.md
├── prior_fundamental_memory.md
├── existing_industry_research.md
├── manifest.json
└── research_outputs/
```

`05_agent_input.md` 还会声明当前公司的只读历史披露目录：

```text
data/stock_info/{stock_name}_{symbol}/disclosures/md/
data/stock_info/{stock_name}_{symbol}/disclosures/pdfs/
```

仅在固定基本面规则定义的明确历史缺口出现时按需读取：先 Markdown，缺失时再定位单份 PDF。不要批量读取多年报告，也不要因本轮研究自动调用 MinerU。

仅处理 `ready_items`。`skipped_items` 中已总结或缺财报的股票不启动角色。正式研究不得使用 `--skip-market-context`。

## 二、验证百炼搜索

在派发任何需要联网的角色前，发现并验证 `bailian_web_search` 能力。实际 MCP 前缀可以不同。

- 能力不存在或传输失败：停止研究，报告 `bailian_unavailable`。
- 搜索成功但无结果：允许角色继续换查询词或记录数据缺口。
- 不得静默切换其他搜索引擎；只有用户明确允许 fallback 才能使用替代搜索。

## 三、产业链分组

**每家公司必须有行业研究**：每只 ready 股票都要由 Industry Researcher 完成其所在细分产业链的行业研究，没有例外，不能因为单公司或无法归组就跳过行业研究。

读取所有 ready item 的 `05_agent_input.md`、财报业务描述和 `existing_industry_research.md`，按以下层级识别每家公司的细分产业链：

```text
terminal_theme → subchain → value_chain_node → company_exposure
```

**同一产业链可复用，不同产业链各自分析**：

- 当同一天选中的多只股票属于同一细分产业链（`subchain` 相同）时，只启动一个 Industry Researcher，用一份共享行业研究覆盖该组所有公司，但必须分别研究每家公司对公司自身的业务暴露；
- 不同细分产业链的股票必须各自启动自己的 Industry Researcher，独立完成各自的行业研究，不得相互借用；
- “AI 硬件”不是可复用细分链；PCB、光模块、液冷、存储、电源和服务器分别研究；
- 即使当天只有一只股票，也必须为它启动 Industry Researcher，完成该股所在细分产业链的行业研究。

共享文件先由该 Agent 单独写完，再作为只读输入给组内股票；其他角色不得回写。旧 card 只作线索，所有可变事实必须按本次披露窗口刷新。

## 四、分波次执行

平台并发额度只决定每波同时启动多少 Agent，不改变逻辑角色：

1. 每个细分产业链启动一个 Industry Researcher；
2. 每股启动一个 Expectation Scout；
3. 等产业链研究与预期快照完成后，每股启动一个 Financial Author 写 `draft_v1.md`；
4. 每股启动一个 Research Challenger 写 `challenge_round_01.md`；
5. 复用原 Financial Author 会话，读取质询并直接写最终报告；
6. 主 Agent 通过质量门禁后逐股注册。

不得为了省并发让同一个 Agent 同时担任 Author 和 Challenger。Author 初稿与修订必须复用同一会话，不额外启动 Finalizer。

## 五、角色派发

所有角色必须完整读取：

- `05_agent_input.md` 中声明的本次路径与权限；
- 自己在 `references/role-prompts.md` 中的精确角色模板；
- 模板列出的 `configs/research/` 固定规则。

主 Agent 给 subagent 的消息采用最小形式：

```text
你是 <ROLE>。处理 <股票或细分产业链>。
完整读取 <role-prompts.md> 中的 <ROLE> 章节，以及 <05_agent_input.md>。
严格遵守其中的可读/禁读边界，只写 <唯一输出路径>。
固定规则以本地文件为准；本消息不补充或改写研究方法。
```

若主消息与本地规则冲突，以本地规则和 `05_agent_input.md` 的更严格限制为准。

## 六、停止与质量门禁

Author 完成修订后，主 Agent 检查：

- `draft_v1.md`、`challenge_round_01.md` 和最终报告均存在且非占位；
- 财报前预期没有时间穿越；
- 年度一致预期没有冒充季度一致预期；
- 最新增强估值的财务基准期已说明；
- 新财报口径 TTM/Forward 估值已重算，或明确说明无法计算；
- 最近两年整体与重大业务趋势、同比、环比、季节性和归因已覆盖；
- 产业链结论落到公司收入/利润暴露，没有预设龙头；
- Challenger 的高严重度问题已由 Author 解决，或按影响融入最终报告 §16/§17；
- 最终报告不含“未解决问题与披露限制”或“证据与来源”独立章节；
- 最终报告不含占位符、伪造来源或交易指令。

高严重度问题未闭环时不注册。真正无法由公开信息回答的问题必须融入 §16/§17，
说明其对判断的影响和后续验证方式，不得输出研究过程附录。

## 七、注册最终报告

最终报告存在并通过门禁后，由主 Agent 逐股执行：

```bash
python scripts/register_financial_report_summary.py \
  --symbol SYMBOL \
  --path data/stock_info/{stock_name}_{symbol}/financial_reports/YYYYMMDD.md \
  --require-deep-research
```

Financial Author 不得自行写 `summary_index.json`。注册失败时保留研究文件并报告失败，不伪造完成状态。

## 八、批量汇报

向用户汇报：

- 成功、跳过和失败股票；
- 每股最终报告路径；
- 细分产业链复用关系；
- 未解决的高严重度问题；
- 百炼搜索状态；
- `summary_index.json` 是否注册成功。

不在汇报中代替报告生成买卖建议。

## 九、历史回测调用

当 `backtest-fixed-tracked` 因 `needs_financial_research` 调用本 Skill 时：

- 使用回测状态返回的 `preparation_command`，只处理 `required_items`；
- 每股 `05_agent_input.md` 会声明历史回测模式，全体角色必须先完整读取同
  workdir 的 `00_backtest_context.md`；
- Industry Researcher、Author 与 Challenger 的联网资料不得晚于回测决策日；
  Expectation Scout 继续使用更严格的财报公告前截止时间；
- 历史模式找不到分析日一致预期快照时禁止回退到当前预测缓存；
- 上期基本面记忆只能选当前财报公告日以前的已登记报告，不能读取后来财报；
- 产业研究候选只能列出文件名可确认不晚于回测日的历史 card；
- 注册必须使用返回模板并保留
  `--as-of-date {decision_date} --require-deep-research`。
