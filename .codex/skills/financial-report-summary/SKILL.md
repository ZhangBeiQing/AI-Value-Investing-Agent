---
name: financial-report-summary
description: >
  用于对最近一天选股系统最终深研队列中的股票生成财报分析文档。主 agent 先运行脚本为每只股票准备
  `financial_report_workdir/` 输入文件，再并行启动多个 subagent（每只股票一个 subagent）读取 workdir，
  每个subagent按 prompt 深度联网分析后输出最终财报总结 markdown。用户说“生成财报总结”时使用
---

# Financial Report Summary

## 1. 什么时候用

- 用户要“生成财报总结”
- 用户要“调用subagent多智能体生成deep research queue中的股票的财报总结”
- 用户要对 short/long 深研队列股票逐股生成最终财报分析 markdown

## 2. 股票从哪里来

- 默认读取最近一个存在的：

```text
data/selection_runs/YYYY-MM-DD/11_deep_research_queue.json
```

- 若用户只要短期或长期股票，则按 `final_mandate` 过滤：
  - `short_book`
  - `long_book`

## 3. 先运行什么脚本

主 agent 必须先运行准备脚本：

```bash
source /home/zhangbeiqing/venv/ai_stock/bin/activate
python scripts/prepare_financial_report_skill.py --date YYYY-MM-DD --mandate all --sync-first --json
```

这个脚本会：

1. 读取最近一天深研队列中的股票
2. 从公告缓存中定位最近两份关键财报
3. 必要时把财报 PDF 转成 markdown
4. 比较 `financial_reports/summary_index.json`
5. 如果最新财报已经分析过，则跳过该股票
6. 如果发现新财报，则为该股票生成固定的 `financial_report_workdir/`
注意：prepare_financial_report_skill脚本会批量生成需要分析股票的financial_report_workdir文件夹，
位于各自股票的data/stock_info/{stock_name}_{symbol}/financial_report_workdir/下面。你需要调用
subAgent读取每只股票下面的financial_report_workdir文件夹，然后严格按照financial_report_workdir下的文件的要求生成输出

## 4. `financial_report_workdir/` 里有什么

每只需要分析的股票都会生成：

```text
data/stock_info/{stock_name}_{symbol}/financial_report_workdir/
  01_latest_report.md
  02_previous_report.md
  03_report_analysis_prompt.md
  04_future_outlook_prompt.md
  05_agent_input.md
  manifest.json
```

文件作用：

- `01_latest_report.md`
  - 最新财报原文 markdown
- `02_previous_report.md`
  - 上一期关键财报原文 markdown
- `03_report_analysis_prompt.md`
  - 历史与当前财报分析 prompt
- `04_future_outlook_prompt.md`
  - 未来 6-12 个月行业与经营前瞻 prompt
- `05_agent_input.md`
  - subagent 必须先读的统一任务说明
- `manifest.json`
  - 当前股票本轮分析的元信息、输入路径和最终输出路径

## 5. 主 agent 怎么做

主 agent 必须：

1. 先运行准备脚本
2. 看脚本输出哪些股票是 `ready`，哪些是 `skipped`
3. 对 `skipped` 股票直接跳过
4. 对每只 `ready` 股票启动 1 个 subagent

**强制要求：必须并行启动多个 subagent，每只股票一个 subagent。**

## 6. subagent 怎么做

每个 subagent 只负责 1 只股票，只允许读取该股票自己的：

```text
financial_report_workdir/
```

固定阅读顺序：

1. `05_agent_input.md`
2. `01_latest_report.md`
3. `02_previous_report.md`（若存在）
4. `03_report_analysis_prompt.md`
5. `04_future_outlook_prompt.md`
6. `manifest.json`

然后：

- subagent 必须根据 `03_report_analysis_prompt.md` 和 `04_future_outlook_prompt.md` 的要求，自主进行深度联网搜索
- 不要由主 agent 预先限制搜索问题列表
- subagent 应该自己判断：要回答这两个 prompt，还缺哪些一致预期、券商观点、行业数据、公司前瞻、风险信息，并主动搜索补齐

换句话说：

- `03_report_analysis_prompt.md` 和 `04_future_outlook_prompt.md` 本身就是 subagent 的核心研究任务定义
- subagent 必须围绕这两个 prompt 自主搜索，而不是只搜索主 agent 额外指定的几个点

## 7. 输出到哪里

每个 subagent 最终把结果写到：

```text
data/stock_info/{stock_name}_{symbol}/financial_reports/YYYYMMDD.md
```

其中 `YYYYMMDD` 必须使用“最新财报公告日”。

此外，subagent 在成功写完该股票最新财报总结后，必须同步更新：

```text
data/stock_info/{stock_name}_{symbol}/financial_reports/summary_index.json
```

更新要求：

- 必须按照已有`summary_index.json`格式，把本次更新的最新财报对应的 `announcement_id`、`report_date`、输出 markdown 路径写入 `summary_index.json`

## 8. 跳过规则

- 是否需要重新分析，只看：

```text
data/stock_info/{stock_name}_{symbol}/financial_reports/summary_index.json
```

- 若 `summary_index.json` 中的最新已分析 `announcement_id` 与当前最新财报 `announcement_id` 相同，则该股票本轮直接跳过，不需要重新分析

## 注：至关重要：仓库里没有现成的“最终生成财报总结”脚本！！！这个skill是要你调用subagent子智能体来完成
## 财报分析工作，不是让你调用狗屁脚本来通过代码生成财报总结！！！是调用subagent子智能体来对每只股票进行分析！！！！完全禁止任何试图生成某个python代码脚本来生成财报的想法
