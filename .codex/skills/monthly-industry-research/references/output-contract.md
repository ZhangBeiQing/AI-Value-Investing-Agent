# 输出与证据契约

## 行业卡片

`industry_card.json` 必须包含：

- `theme_id`、`theme_name`、`as_of_date`
- `classification`
- `current_state`
- `terminal_demand`
- `demand_branches`
- `demand_to_upstream_transmission`
- `tam_scenarios.bear/base/bull`
- `demand_indicators`
- `supply_indicators`
- `price_inventory_lead_time`
- `state_transition_conditions`
- `invalidation_conditions`
- `key_uncertainties`
- `evidence`
- `confidence`
- `human_decision`

状态只能使用配置中声明的枚举：

```text
observation
emerging
fundamental_right
expansion
overheated
downturn
archived
```

## 产业链利润映射

每个重大节点记录：

- 需求驱动
- 供给约束
- 定价权
- 产能响应时间
- 利润池方向
- 上市受益公司
- 支撑结论的证据ID

“最受益”必须解释利润为何留在该节点，不能只依据收入增速或股票涨幅。

## 公司产业暴露

`profit_map.json` 必须包含 `company_exposure_matrix`。综合公司不得因涉及主题就被视为纯主题公司；每家公司至少记录：

- 主题相关收入及占公司总收入比例
- 主题相关毛利、毛利率或无法取得的明确说明
- 数据披露口径是分部还是公司整体
- 其他主要业务及其对公司业绩的影响
- 公司总指标不得替代主题分部指标

无法分拆综合公司的主题利润时，只能将其列为平台型受益者，不能生成伪精确的主题目标市值。

公司列表不设固定数量。纳入标准是其业务暴露、利润留存或竞争地位对结论重大；不得为了凑“前三名”加入低相关公司，也不得默认主题内收入最大的公司就是最受益龙头。

## 证据

每条关键证据至少保存：

```json
{
  "evidence_id": "",
  "fact": "",
  "period_end": "YYYY-MM-DD或明确期间",
  "release_date": "YYYY-MM-DD",
  "fetched_at": "ISO-8601",
  "source_title": "",
  "source_url": "",
  "source_type": "government | exchange | association | company | customer_supplier | research | news",
  "confidence": "low | medium | high"
}
```

回测与历史复盘按 `release_date` 判断当时是否可见，不按 `period_end` 判断。

## 状态历史

`state_history.jsonl` 每行是一个完整JSON对象，至少包含：

- `as_of_date`
- `previous_state`
- `new_state`
- `transition_reason`
- `key_evidence_ids`
- `confidence`
- `recorded_at`

只追加，不覆盖旧记录。状态不变时也可以记录月度复核，但必须说明新增事实。
