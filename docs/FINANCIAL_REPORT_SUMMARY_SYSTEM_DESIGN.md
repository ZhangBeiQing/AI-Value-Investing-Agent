# 财报分析系统设计文档

更新日期：2026-04-17

## 1. 背景

当前公共公告系统 `news/disclosures_builder.py` 会：

- 下载公告 PDF 到 `disclosures/pdfs/`
- 可选转 Markdown 到 `disclosures/md/`
- 对普通公告生成 `news/news.json` 与 `news/news_audited.json`

但财报类公告被显式排除在普通摘要链之外：

- 财报文件太大，尤其年报直接送入普通公告摘要模型容易超过上下文
- 财报需要“读两期原始报告 + 补一致预期 + 做未来经营前瞻”的专门流程
- 因此财报不应走普通 audited 公告分析流程

本设计目标是新增一套独立的“财报分析系统”，但复用现有公告缓存与 shared_data_access 的目录约定。

## 2. 设计目标

1. 财报原始 PDF 和原始 Markdown 复用 `disclosures` 缓存
2. 最终财报总结单独落在 `financial_reports/`
3. 每天可定时运行，但若没有新财报则必须跳过，避免重复下载、重复转换、重复分析
4. 股票范围来自最新 `11_deep_research_queue.json`
5. 短期和长期股票都能进入同一财报分析系统，只是来源过滤不同

## 3. 数据分层

### 3.1 原始缓存层：复用 disclosures

目录：

```text
data/stock_info/{stock_name}_{symbol}/disclosures/
  index.json
  cninfo_list.csv
  pdfs/
  md/
```

说明：

- 财报 PDF 本质上是公告 PDF 的子集，属于公共文件缓存
- 不再把原始财报 PDF/Markdown 混放在 `financial_reports/`
- `disclosures/index.json` 作为公告级真相源，记录财报与非财报公告的元数据

### 3.2 总结产物层：financial_reports

目录：

```text
data/stock_info/{stock_name}_{symbol}/financial_reports/
  summary_index.json
  YYYYMMDD.md
```

说明：

- `financial_reports/` 只存最终总结和状态索引
- 不再存放原始财报 PDF 或财报转出的中间 markdown
- 输出文件名采用“最新财报公告日”

## 4. 为什么不让财报继续走普通公告流程

普通公告流程当前会在 `update_disclosures_for_stock()` 中跳过财报：

```python
if is_financial_report(title):
    _log("跳过财报公告")
    continue
```

保留该行为的原因：

1. 财报体积远大于普通公告
2. 普通公告摘要 prompt 不适合财报分析
3. 财报分析需要跨期对比与外部一致预期补证，不属于普通公告“单条摘要”范式

因此：

- 普通公告系统继续跳过财报摘要
- 财报分析系统单独处理 `is_financial_report=true` 的公告

## 5. 财报缓存与索引设计

### 5.1 disclosures/index.json 作为原始索引

财报系统复用该索引，并补充/依赖以下字段：

- `announcement_id`
- `title`
- `date`
- `is_financial_report`
- `pdf_path`
- `md_path`

建议后续逐步补充：

- `report_type`: `q1/interim/q3/annual`
- `fiscal_year`
- `fiscal_quarter`
- `financial_report_summary_path`
- `financial_report_summarized`

### 5.2 financial_reports/summary_index.json

新增文件：

```text
data/stock_info/{stock_name}_{symbol}/financial_reports/summary_index.json
```

职责：

- 记录最近已完成总结的财报公告 ID
- 记录本次总结所配对的上一期财报
- 记录输出文件路径
- 支持每日重跑时的跳过判断

建议结构：

```json
{
  "symbol": "300750.SZ",
  "stock_name": "宁德时代",
  "latest_completed_report": {
    "announcement_id": "1225107948",
    "report_date": "2026-04-15",
    "report_type": "q1",
    "paired_previous_announcement_id": "1225002247",
    "output_path": "data/stock_info/宁德时代_300750.SZ/financial_reports/20260415.md",
    "generated_at": "2026-04-17T10:20:00"
  },
  "history": []
}
```

## 6. 跳过策略

每日运行时，对每只股票按以下顺序判断：

1. 从最新 `11_deep_research_queue.json` 取股票列表
2. 从 `disclosures/index.json` 中定位最近一份财报公告
3. 读取 `financial_reports/summary_index.json`
4. 若 `latest_completed_report.announcement_id == 当前最新财报announcement_id`，直接跳过
5. 若不同，说明出现了新财报，重新执行下载/转换/分析流程

这样可以确保：

- 无新财报时不重复消耗算力
- 有新财报时自动触发重分析

## 7. 最近两期财报配对规则

默认配对：

- 最新为 `Q1` -> 配对上一份 `年报`
- 最新为 `半年报` -> 配对上一份 `Q1`
- 最新为 `Q3` -> 配对上一份 `半年报`
- 最新为 `年报` -> 配对上一份 `Q3`

若严格配对失败，则退回到时间上次新的财报。

## 8. 股票范围

股票来源固定为：

- `data/selection_runs/YYYY-MM-DD/11_deep_research_queue.json`

支持三种过滤：

- `all`
- `short_book`
- `long_book`

## 9. 当前实现策略

当前正式实现应分两步：

### Phase 1：只处理 disclosures 中已存在的财报

- 从 `disclosures/index.json` 读取财报公告
- 若 `pdf_path/md_path` 已存在，则直接进入准备流程
- 若不存在，则先标记为缺失，不做自动下载

### Phase 2：自动补齐缺失财报原始件

- 针对 `is_financial_report=true` 且没有 `pdf_path` 的公告
- 自动下载 PDF 到 `disclosures/pdfs/`
- 自动转换到 `disclosures/md/`
- 再进入分析流程

## 10. 对 skill 的约束

财报分析 skill 必须：

1. 先读最新财报原始 markdown
2. 再读上一期关键财报原始 markdown
3. 再联网补一致预期、券商快评、行业前瞻
4. 最终将总结写入 `financial_reports/YYYYMMDD.md`
5. 成功后更新 `summary_index.json`

补充约束：

- 主 agent 只负责准备 `financial_report_workdir/` 和分发任务
- 最终财报总结必须由每只股票各自的 subagent 基于其本地 `financial_report_workdir/` 自主联网研究生成
- 不应新增或依赖一个中央 `run_financial_report_summary`/`financial_report_summary_runner` 脚本来替代 subagent 研究流程

## 11. 相关代码改动范围

### 本期核心文件

- `services/research/financial_report_skill.py`
- `scripts/prepare_financial_report_skill.py`
- `.codex/skills/financial-report-summary/SKILL.md`
- `.claude/skills/financial-report-summary/SKILL.md`

### 后续扩展文件

- `news/disclosures_builder.py`
- `shared_data_access/cache_registry.py`

## 12. 总结

最终设计原则：

- 原始财报缓存属于公共公告缓存，放 `disclosures`
- 最终财报分析属于派生产物，放 `financial_reports`
- 普通公告系统继续跳过财报
- 财报分析系统单独处理财报公告
- 通过 `summary_index.json` 实现“有新财报才重跑”
