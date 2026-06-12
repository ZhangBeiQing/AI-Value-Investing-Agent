# 财报研究系统说明

更新日期：2026-06-12

## 1. 当前状态

- 项目不再维护旧的「自动化财报研究流水线」
- 财报研究的有效输入以 `data/stock_info/{stock_name}_{stock_code}/financial_reports/` 下的 Markdown 文件为准
- `services/research/financial_report.py` 会在 `today_time` 之前选取最近的一份财报 Markdown，并结合机构一致预期与 forecast 信息返回给本地 Agent

## 2. 文件约定

- 文件命名以 `YYYYMMDD` 为前缀，必要时追加 `_25一季报`、`_24年报` 等后缀
- 同一天存在多份财报时，遵循「用新不用旧」原则，优先使用代表最新会计期间的文件
- 任何未来重建的自动化流程必须继续兼容该目录与命名约定

## 3. 目录关系

| 目录 / 文件 | 用途 | 维护方式 |
| --- | --- | --- |
| `financial_reports/` | 财报 Markdown | 由 `financial-report-summary` skill 写入；早期为人工维护 |
| `forecast/` | 未来预期 Markdown | 人工维护 |
| `profit_forecast/profit_forecast.csv` | AkShare 拉取的机构一致预期缓存 | 由 `shared_data_access` 的 `update_cn_profit_forecast_cached` / `update_hk_profit_forecast_cached` 自动维护 |
| `services/research/financial_report.py` | 读取最近财报、forecast 与一致预期并组装研究结果 | — |

一致预期数据源：

- A 股：`ak.stock_profit_forecast_ths(..., indicator="业绩预测详表-详细指标预测")`
- 港股：`ak.stock_hk_profit_forecast_et(..., indicator="盈利预测概览")`

## 4. 日常入口

```bash
# 1. 准备财报清单（同步公告 PDF + 计算待生成清单）
python scripts/prepare_financial_report_skill.py --date YYYY-MM-DD \
  --sync-first --json --include-quant-prefilter
#   --sync-first：先同步公告 PDF
#   --json：清单以 JSON 输出，便于 skill 派发 subagent
#   --include-quant-prefilter：除了 fixed_tracked，额外覆盖量化初筛短期池
#   --include-queue：覆盖 selection candidate（08/09 文件存在时使用）

# 2. 触发财报总结 skill
/financial-report-summary
#   主 agent 派发 subagent，每只股票一个，subagent 自主完成
#   阅读→搜索→诊断→验证→撰写，最终写入 financial_reports/*.md
```

skill 详见 `.codex/skills/financial-report-summary/SKILL.md`。

## 5. 研究产物使用方

- `services/pipeline/steps/build_stock_research.py`：在生成 `04_stock_research/*_research.md` 时把最近一份 `financial_reports/` 文件嵌入到逐股研究包
- `services/research/financial_report.py`：组装 prompt 输入时引用最近财报 + forecast + 一致预期
- 三本账本 auto-trading skill 的 subagent：在分析单股时会读取自己那只股的 `04_stock_research/{stock_name}_{symbol}_{date}_research.md`，其中已经包含财报段落
