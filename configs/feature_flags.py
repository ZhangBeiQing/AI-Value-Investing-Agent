"""临时功能开关：暂停高 token 消耗的研究层。

2026-11-05 用户要求**暂时封禁**`渐进式新闻总结`与 `05_board_heat_*` 板块热度层，
原因是这两层要么是独立 LLM 子任务、要么把大段 JSON 塞进下游 prompt，token 成本过高。

这两层都是"锦上添花"的输入，不是 01-04 产物契约的一部分：
- `渐进式新闻总结` 产出 `06_hot_news_state.json` / `06_hot_news_digest.json`
- 板块热度层产出 `05_board_heat_state.json` / `05_board_heat_digest.json`

当前行为（两个开关均为 False 时）：
- `refresh_all_for_date.py` 不再跑 `selection.run-news` 与 `selection.build-board-heat-state`
- `build_shared_context` 不再把 05/06 写进 shared context
- `build_agent_input` / `main_policy.md` / `skill_flow.json` 不再引用这两份输入
- `daily-data-preparation` skill 只派发宏观一个 subagent

2026-11-05 第二轮（范围收窄）：用户嫌分析股票太多、太慢，要求每日只准备
`TRACKED_A_STOCKS` 固定池的数据。因此新增：

- `ENABLE_FACTOR_PREFILTER_LAYER=False`：每日刷新不再跑因子库/评分/量化初筛，
  也不再为了 prefilter 把刷新范围扩到 `master_universe`
- `ENABLE_SELECTION_UNIVERSE_MERGE=False`：`run_daily_pipeline` 不再把
  长期候选与 `12_quant_prefilter_short.csv` 并入 01-04 研究包

效果：日常刷新的股票范围收敛为 `TRACKED_A_STOCKS`（约 24 只），而不是原来的 234 只。

恢复方式：把对应开关改成 True 即可，代码路径都还在，没有删模块。
"""

from __future__ import annotations

# 渐进式新闻总结（gradual-hot-news-summary → 06_hot_news_state.json）
ENABLE_GRADUAL_NEWS_SUMMARY: bool = False

# 板块热度层（05_board_heat_state.json / 05_board_heat_digest.json）
ENABLE_BOARD_HEAT_LAYER: bool = False

# 量化初筛链：因子库 → 因子评分 → 量化初筛（12_quant_prefilter*、13_factor_scores*）
# 关闭后每日刷新不再覆盖 master_universe，也不再产出这三份文件。
ENABLE_FACTOR_PREFILTER_LAYER: bool = False

# 把「长期候选 / 量化初筛结果」并入每日 01-04 研究包（扩池）
# 关闭后 run_daily_pipeline 只生成 TRACKED_A_STOCKS（+ 手工持仓覆盖）的研究包。
ENABLE_SELECTION_UNIVERSE_MERGE: bool = False


def gradual_news_summary_enabled() -> bool:
    return ENABLE_GRADUAL_NEWS_SUMMARY


def board_heat_layer_enabled() -> bool:
    return ENABLE_BOARD_HEAT_LAYER


def factor_prefilter_layer_enabled() -> bool:
    return ENABLE_FACTOR_PREFILTER_LAYER


def selection_universe_merge_enabled() -> bool:
    return ENABLE_SELECTION_UNIVERSE_MERGE


__all__ = [
    "ENABLE_GRADUAL_NEWS_SUMMARY",
    "ENABLE_BOARD_HEAT_LAYER",
    "ENABLE_FACTOR_PREFILTER_LAYER",
    "ENABLE_SELECTION_UNIVERSE_MERGE",
    "gradual_news_summary_enabled",
    "board_heat_layer_enabled",
    "factor_prefilter_layer_enabled",
    "selection_universe_merge_enabled",
]
