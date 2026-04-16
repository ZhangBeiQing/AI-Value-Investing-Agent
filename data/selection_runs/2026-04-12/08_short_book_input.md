# 短期选股头 本地 Agent 输入

- run_date: 2026-04-12
- pool_type: short_book
- required_count: 10
- payload_file: /home/zhangbeiqing/programer/AI-Value-Investing-Agent/data/selection_runs/2026-04-12/08_short_book_input.json
- output_file: /home/zhangbeiqing/programer/AI-Value-Investing-Agent/data/selection_runs/2026-04-12/08_short_book_candidates.json

## 任务定位

你现在不是在做最终交易决策，也不是在做逐股深度研究。
你的任务是从 `master_universe` 中，筛出更值得进入 `short_book` 深研池的股票。

更关注主题强化、板块确认、公告催化、量价与流动性、近端风险。

## 读取顺序

1. 先阅读 `07_shared_selection_context.md`
2. 再阅读 `08_short_book_input.json`
3. 如有需要，可使用本地 snapshot 查询脚本：
   - `python scripts/query_stock_snapshot.py --date 2026-04-12 --symbol 000977.SZ`
   - `python scripts/rank_stock_snapshot.py --date 2026-04-12 --field roe --top 20`
   - `python scripts/filter_stock_snapshot.py --date 2026-04-12 --expr 'liquidity_score >= 0.6 and pe_ttm <= 25'`
4. 若需要补看板块，可使用：
   - `python scripts/query_board_snapshot.py --date 2026-04-12 --board-name "通信设备"`

## 强制约束

1. 只能从 `master_universe` 中选股
2. 必须输出恰好 `10` 只股票
3. 不得重复
4. `priority_rank` 从 `1` 递增
5. `holding_horizon` 必须写 `1-2个月`
6. `main_risks` 必须是字符串数组
7. 若发现某些股票虽然优秀，但不符合 `short_book` 的 mandate，必须舍弃
8. 输出必须是唯一 JSON 对象，顶层结构如下：

```json
{
  "items": [
    {
      "symbol": "",
      "stock_name": ""
    }
  ]
}
```

## 每个 item 必填字段

- `symbol`
- `stock_name`
- `selected_reason`
- `theme_alignment`
- `board_confirmation`
- `announcement_signal`
- `snapshot_highlights`
- `holding_horizon`
- `why_short_book`
- `main_risks`
- `priority_rank`

## 输出位置

将最终 JSON 保存为：

`/home/zhangbeiqing/programer/AI-Value-Investing-Agent/data/selection_runs/2026-04-12/08_short_book_candidates.json`
