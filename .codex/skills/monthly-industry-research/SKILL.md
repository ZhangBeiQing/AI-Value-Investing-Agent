---
name: monthly-industry-research
description: 用于半年级结构空间筛查、月度产业领先指标监控、季度行业财务验证，以及对一个经用户确认的结构性产业主题开展深度研究。当用户说“开始本月行业研究”“生成月度行业雷达”“筛选高增长产业”“更新产业指标”“开始半年行业扫描”或“深研某个候选产业链”时使用。该skill只维护独立industry_research产物，不使用热点或股价发现行业，不修改交易01-08文件、股票池或仓位。
---

# Structural Industry Research

## 边界

- 行业发现是半年级，领先指标监控是月度，财务验证是季度级，不每日重选行业。
- 每次最多深研一个经用户确认的产业主题。
- 不使用热点新闻、板块涨幅、资金流或股票动量发现产业机会。
- 不自动修改长期池，不生成交易指令。
- 财报只验证景气扩散，不负责发现“爆发式增长行业”。

## 日期语义

`--date` 是本次研究的绝对截止日，优先使用用户指定日期；未指定时使用当前绝对日期。
外部抓取面向真实当前时间，历史复盘只在读取阶段按 `release_date <= --date` 截断。

## 阶段一：半年结构空间扫描

每半年运行一次，或产业技术和政策约束发生根本变化时重跑：

```bash
source /home/zhangbeiqing/venv/ai_stock/bin/activate
python scripts/manage_industry_research.py refresh-catalog
python scripts/manage_industry_research.py refresh-data --date YYYY-MM-DD
python scripts/manage_industry_research.py build-structural-scan --date YYYY-MM-DD
```

完整读取 `scan_input.json` 与 `scan_input.md`。先对全部申万二级行业做低成本预检，回答终端需求单位、当前市场规模、三至五年终局空间、渗透率与增长驱动。不要因为财务高增长、新闻热度或股价上涨直接入选。

只对预检最可能的约20个方向联网补证，按以下分类填写 `structural_opportunity_pool_template.json`：

- `structural_growth`：三至五年基准收入空间约2倍或以上，且增长来源可持续。
- `cyclical`：主要由价格、库存或短期供给冲击驱动。
- `mature`：终局空间受限或渗透率已高。
- `uncertain`：空间可能很大，但商业化时间或付费能力无法验证。

将候选写入同目录的 `structural_opportunity_pool.json`，保持 `status=draft`、`human_approved=false`，向用户展示10-20个结构候选并暂停。用户确认后才将池标为 `approved` 并将被确认主题设为 `human_approved=true`。

## 阶段二：月度领先指标监控

先基于已有指标历史生成一版监控：

```bash
python scripts/manage_industry_research.py build-radar --date YYYY-MM-DD
```

完整读取 `industry_radar.json` 与 `agent_review_input.md`，只更新已获人工批准主题中缺失或超过45天的领先指标。按主题注册表的 `source_hint` 深搜协会、政府、公司公告、客户或供应商原始材料，不做全市场热点扫描。

每条新增观测追加到 `data/industry_research/cards/{theme_id}/indicator_history.jsonl`，至少包含：

```json
{
  "as_of_date": "YYYY-MM-DD",
  "indicator_id": "",
  "indicator_name": "",
  "category": "demand | order | supply | inventory | price | unit_value | capex",
  "signal": "improving | stable | deteriorating | unknown",
  "value": null,
  "unit": "",
  "period_end": "YYYY-MM-DD或明确期间",
  "release_date": "YYYY-MM-DD",
  "fetched_at": "ISO-8601",
  "source_title": "",
  "source_url": ""
}
```

追加后再次运行 `build-radar`。只有同时满足以下条件，状态才可成为 `fundamental_right_candidate`：

- 主题已获人工批准且结构空间检查通过。
- 至少两项新鲜指标改善。
- 改善指标来自至少两个独立类别。
- 恶化指标不超过一项。

该状态只是基本面右侧候选，不是买入信号。最多建议一个主题进入全产业链深研，然后暂停等待用户确认。

## 阶段三：季度财务验证

在一季报、半年报、三季报或年报进入基本完整披露窗口后运行：

```bash
python scripts/manage_industry_research.py refresh-data --date YYYY-MM-DD
python scripts/manage_industry_research.py build-radar --date YYYY-MM-DD
```

`refresh-data` 通过 `shared_data_access` 获取最近两个已完成报告期的全A业绩横截面，并按公告日期截断。只用行业营收/利润中位数、正增长宽度和连续报告期加速度验证景气是否已扩散；不要用财报排名生成新产业候选，也不要比较“一个月前的同一份财报”。

## 阶段四：准备单主题深研

用户确认主题后运行：

```bash
python scripts/manage_industry_research.py prepare-theme-research \
  --date YYYY-MM-DD \
  --theme-id THEME_ID
```

完整读取该工作目录中的 `00-03` 文件，并在深搜前阅读 [输出与证据契约](references/output-contract.md)。

## 阶段五：深度研究

严格按工作目录 `01_deep_research_prompt.md` 的顺序执行：

1. 验证雷达信号。
2. 建立终端需求与TAM公式。
3. 研究需求、供给、库存、价格、交期、资本开支和扩产周期。
4. 绘制产业链利润映射。
5. 研究最受益节点的前三名公司。
6. 构建悲观、基准、乐观市值情景。
7. 给出状态迁移与证伪条件。

优先使用政府、交易所、行业协会、公司公告、业绩会、客户和供应商原始材料。关键结论需要一个权威原始来源，或两个相互独立的可靠来源。凡是可能变化的产业事实必须联网补证。

默认由当前主agent完成单主题研究，不派发并行subagent，保持单人月度流程可控。

## 阶段六：落盘和人工决策

按工作目录声明的路径生成：

- 深度研究 Markdown
- `industry_card.json`
- `profit_map.json`
- `state_history.jsonl` 的一条新增记录

完成后只汇报研究结论、证据强度和最大不确定性，等待用户决定：

- 归档
- 继续观察
- 纳入长期观察池

即使用户选择纳入观察池，本skill也不直接修改交易候选或持仓文件。
