# 财报研究系统设计说明

## 目标与当前状态
- 当前项目不再维护旧的自动化财报研究流水线。
- 财报研究的有效输入以 `data/stock_info/{stock_name}_{stock_code}/financial_reports/` 下的 Markdown 文件为准。
- `services/research/financial_report.py` 会在 `today_time` 之前选取最近的一份财报 Markdown，并结合机构一致预期与 forecast 信息返回给本地 Agent。

## 当前约定

- 文件命名以 `YYYYMMDD` 为前缀，必要时可追加 `_25一季报`、`_24年报` 等后缀。
- 在同一天存在多份财报时，遵循“用新不用旧”原则，优先使用代表最新会计期间的文件。
- 自动化流程如果未来重建，必须继续兼容该目录与命名约定。

## 目录关系

- `financial_reports/`：人工维护或外部整理后的财报 Markdown。
- `forecast/`：人工维护的未来预期 Markdown。
- `profit_forecast/`：通过 AkShare 拉取的机构一致预期缓存目录，文件名固定为 `profit_forecast.csv`。
  - A 股来源：`ak.stock_profit_forecast_ths(..., indicator="业绩预测详表-详细指标预测")`
  - 港股来源：`ak.stock_hk_profit_forecast_et(..., indicator="盈利预测概览")`
- `services/research/financial_report.py`：读取最近财报、forecast 与一致预期并组装研究结果。
