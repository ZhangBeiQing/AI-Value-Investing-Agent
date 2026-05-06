---
name: financial-report-summary
description: >
  用于对最近一天选股系统最终深研队列中的股票生成财报分析文档。主 agent 并行启动多个 subagent（每只股票一个 subagent）读取 
  data/stock_info/{stock_name}_{symbol}/financial_report_workdir
  每个subagent按 prompt 深度联网分析后输出最终财报总结 markdown。用户说”生成财报总结”时使用
---

# Financial Report Summary

## 1. 什么时候用

- 用户要”生成财报总结”
- 用户要”调用subagent多智能体生成deep research queue中的股票的财报总结”
- 用户要对 short/long 深研队列股票逐股生成最终财报分析 markdown

## 2. 股票从哪里来

- 默认同时覆盖两类股票来源：

1. 最近一个存在的选股深研队列：
```text
data/selection_runs/YYYY-MM-DD/11_deep_research_queue.json
```

2. 固定跟踪股池：`configs/stock_pool.py` 中的 `TRACKED_A_STOCKS`

- 若用户只要特定账本的股票，则按 `final_mandate` 过滤：
  - `short_book`
  - `long_book`
  - `tracked`

- 若用户明确指定只处理深研队列（不要 tracked），则按 `mandate` 传 `all`/`short_book`/`long_book` 并加 `--no-include-tracked`


## 3. `financial_report_workdir/` 里有什么

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

## 4. 主 agent 怎么做

主 agent 必须：

1. 先运行准备脚本（**默认必须带 `--include-tracked`**，确保固定跟踪股池也被覆盖）：
   ```bash
   python scripts/prepare_financial_report_skill.py --json --include-tracked
   ```
   若用户明确只要深研队列不要 tracked，才使用 `--no-include-tracked`。
2. 看脚本输出哪些股票是 `ready`，哪些是 `skipped`
3. 对 `skipped` 股票直接跳过
4. 对每只 `ready` 股票启动 1 个 subagent

**强制要求：必须并行启动多个 subagent，每只股票一个 subagent。**

## 5. subagent 怎么做

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

## 6. 输出到哪里

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

- 禁止“沿用各股票当前已有格式”自由发挥，必须统一写成如下 canonical 结构：

```json
{
  "symbol": "300750.SZ",
  "stock_name": "宁德时代",
  "latest_completed_report": {
    "announcement_id": "1225107946",
    "report_date": "2026-04-16",
    "report_type": "q1",
    "paired_previous_announcement_id": "1225002213",
    "paired_previous_report_date": "2026-03-10",
    "output_path": ".../financial_reports/20260416.md",
    "generated_at": "2026-04-19T12:08:40+08:00"
  },
  "history": [
    {
      "announcement_id": "1225107946",
      "report_date": "2026-04-16",
      "report_type": "q1",
      "paired_previous_announcement_id": "1225002213",
      "paired_previous_report_date": "2026-03-10",
      "output_path": ".../financial_reports/20260416.md",
      "generated_at": "2026-04-19T12:08:40+08:00"
    }
  ]
}
```

- `latest_completed_report` 必须与 `history[0]` 指向同一条最新记录。
- `history` 必须按最新在前排序，最多保留 20 条。
- 输出路径字段统一使用 `output_path`，不要再写 `path`、`reports`、`records`、数组顶层等变体。

## 7. 跳过规则

- 是否需要重新分析，只看：

```text
data/stock_info/{stock_name}_{symbol}/financial_reports/summary_index.json
```

- 若 `summary_index.json` 中的最新已分析 `announcement_id` 与当前最新财报 `announcement_id` 相同，则该股票本轮直接跳过，不需要重新分析

## 注：至关重要：仓库里没有现成的“最终生成财报总结”脚本！！！这个skill是要你调用subagent子智能体来完成
## 财报分析工作，不是让你调用狗屁脚本来通过代码生成财报总结！！！是调用subagent子智能体来对每只股票进行分析！！！！完全禁止任何试图生成某个python代码脚本来生成财报的想法
