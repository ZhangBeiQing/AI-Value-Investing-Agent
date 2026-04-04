"""Build board-heat research inputs from cached THS board metrics."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
import json
import os
import re
from typing import Any, Dict, List, Mapping, Sequence

from dotenv import load_dotenv
from openai import OpenAI

from core.logging import get_logger
from shared_data_access.board_metrics import (
    build_board_quant_snapshot,
    select_board_candidates_from_snapshot,
)

from .paths import SelectionSystemPaths
from .store import load_json_file, save_json_file


LOGGER = get_logger("SelectionBoardHeat")
DEFAULT_TOP_N = 3
DEFAULT_STOCKS_PER_BOARD = 3
DEFAULT_RESEARCH_MODEL = "deepseek-v3.2-exp"
DEFAULT_DIGEST_TOP_K = 10

load_dotenv(".env")

BOARD_RESEARCH_SYSTEM_PROMPT = """你是一位专门研究A股板块轮动的资深交易研究员。
你的任务是围绕“板块”本身输出结构化研究，不要把任务误解成逐只个股深度分析。
给你的股票信息只是辅助线索，用来帮助你理解板块内部结构、龙头、中军和情绪前排，不是要求你逐条展开公司研究。
请结合板块涨跌幅表现、板块内股票线索、板块量化指标和当天新闻线索，判断板块驱动、持续性、结构分化与风险。
"""

BOARD_RESEARCH_USER_PROMPT = """请基于输入的板块榜单、量化指标与新闻线索，只输出一个 JSON 对象，格式如下：
{
  "boards": [
    {
      "board_name": "",
      "direction": "up 或 down",
      "rank": 1,
      "change_pct": 0.0,
      "research_summary": "详细总结板块今天为什么强/弱",
      "driver_analysis": "板块涨跌的直接原因与更深层驱动",
      "persistence_analysis": "结合输入的时间序列指标，说明这条逻辑已经持续多久、未来可能持续多久",
      "structure_view": "结合板块宽度和龙头偏离，判断板块内部是集中、扩散还是分化",
      "recommended_watch_stocks": [
        {
          "symbol": "",
          "name": "",
          "reason": "为什么值得跟踪"
        }
      ],
      "risk_points": ["风险1", "风险2"]
    }
  ]
}

严格要求：
1. 研究对象是板块，不是逐只股票深挖。
2. 量化指标优先作为判断持续性、强弱和结构的证据，不要忽略它们。
3. 如果输入里没有足够股票线索，你也可以根据板块逻辑给出你认为值得跟踪的股票。
4. 不要输出 markdown，不要输出代码块，不要输出额外解释。
5. 如果无法确认持续时间，请明确写“不确定”，不要编造具体天数。
"""


def build_board_heat_state(
    run_date: str,
    *,
    base_dir: str | Path = "data",
    top_n: int = DEFAULT_TOP_N,
    stocks_per_board: int = DEFAULT_STOCKS_PER_BOARD,
    model: str = DEFAULT_RESEARCH_MODEL,
) -> Dict[str, Path]:
    paths = SelectionSystemPaths.from_base_dir(base_dir)
    paths.ensure_directories()
    paths.ensure_run_dir(run_date)

    LOGGER.info(
        "开始生成板块热度层: run_date=%s top_n=%d stocks_per_board=%d model=%s",
        run_date,
        top_n,
        stocks_per_board,
        model,
    )

    quant_snapshot = build_board_quant_snapshot(
        run_date,
        base_dir=base_dir,
        stocks_per_board=stocks_per_board,
    )
    LOGGER.info(
        "板块量化快照已就绪: run_date=%s price_as_of_date=%s board_count=%s",
        run_date,
        quant_snapshot.get("price_as_of_date"),
        len(quant_snapshot.get("boards", [])) if isinstance(quant_snapshot.get("boards"), list) else 0,
    )
    board_candidates = select_board_candidates_from_snapshot(
        quant_snapshot,
        run_date=run_date,
        top_n=top_n,
    )
    save_json_file(paths.run_board_candidates_path(run_date), board_candidates)
    board_heat_digest = build_board_heat_digest(run_date, quant_snapshot=quant_snapshot)
    save_json_file(paths.run_board_heat_digest_path(run_date), board_heat_digest)

    news_payload = load_json_file(paths.run_news_enriched_path(run_date), default={}) or {}
    board_heat_state = research_board_heat_state(
        run_date,
        board_candidates=board_candidates,
        news_payload=news_payload if isinstance(news_payload, dict) else {},
        model=model,
    )
    save_json_file(paths.run_board_heat_state_path(run_date), board_heat_state)
    save_json_file(paths.board_heat_state_daily_path(run_date), board_heat_state)
    save_json_file(paths.board_heat_state_latest_path, board_heat_state)
    _append_manifest(
        paths.board_heat_state_manifest_path,
        run_date,
        path=paths.board_heat_state_daily_path(run_date),
        item_count=len(board_heat_state.get("boards", [])),
        source_errors=board_heat_state.get("source_status", []),
    )

    LOGGER.info("板块热度层已写入: %s", paths.run_board_heat_state_path(run_date))
    return {
        "board_candidates": paths.run_board_candidates_path(run_date),
        "board_heat_digest": paths.run_board_heat_digest_path(run_date),
        "board_heat_state": paths.run_board_heat_state_path(run_date),
    }


def collect_board_candidates(
    run_date: str,
    *,
    base_dir: str | Path = "data",
    top_n: int = DEFAULT_TOP_N,
    stocks_per_board: int = DEFAULT_STOCKS_PER_BOARD,
) -> Dict[str, Any]:
    quant_snapshot = build_board_quant_snapshot(
        run_date,
        base_dir=base_dir,
        stocks_per_board=stocks_per_board,
    )
    LOGGER.info(
        "板块量化快照已就绪: run_date=%s price_as_of_date=%s board_count=%s",
        run_date,
        quant_snapshot.get("price_as_of_date"),
        len(quant_snapshot.get("boards", [])) if isinstance(quant_snapshot.get("boards"), list) else 0,
    )
    return select_board_candidates_from_snapshot(
        quant_snapshot,
        run_date=run_date,
        top_n=top_n,
    )


def build_board_heat_digest(
    run_date: str,
    *,
    quant_snapshot: Mapping[str, Any],
    top_k: int = DEFAULT_DIGEST_TOP_K,
) -> Dict[str, Any]:
    boards = quant_snapshot.get("boards") if isinstance(quant_snapshot, Mapping) else []
    if not isinstance(boards, list):
        boards = []

    return {
        "schema_version": 1,
        "run_date": run_date,
        "generated_at": datetime.now().isoformat(),
        "summary": {
            "board_count": len(boards),
        },
        "top_boards_today_by_change_pct": _rank_boards_by_metric(boards, metric="change_pct", top_k=top_k),
        "top_boards_today_by_breadth": _rank_boards_by_metric(
            boards,
            metric="up_ratio_pct",
            top_k=top_k,
            nested_path=("quant_metrics", "breadth", "up_ratio_pct"),
        ),
        "top_boards_recent_by_5d_rank": _rank_boards_by_metric(
            boards,
            metric="rank_5d",
            top_k=top_k,
            nested_path=("quant_metrics", "interval_rankings", "rank_5d"),
            ascending=True,
            extra_metric_paths={
                "return_5d_pct": ("quant_metrics", "interval_returns", "return_5d_pct"),
            },
        ),
        "top_boards_recent_by_20d_rank": _rank_boards_by_metric(
            boards,
            metric="rank_20d",
            top_k=top_k,
            nested_path=("quant_metrics", "interval_rankings", "rank_20d"),
            ascending=True,
            extra_metric_paths={
                "return_20d_pct": ("quant_metrics", "interval_returns", "return_20d_pct"),
            },
        ),
    }


def research_board_heat_state(
    run_date: str,
    *,
    board_candidates: Mapping[str, Any],
    news_payload: Mapping[str, Any],
    model: str,
) -> Dict[str, Any]:
    boards = board_candidates.get("boards") if isinstance(board_candidates, Mapping) else []
    source_status = list(board_candidates.get("source_status", [])) if isinstance(board_candidates, Mapping) else []
    if not isinstance(boards, list) or not boards:
        return {
            "schema_version": 1,
            "run_date": run_date,
            "updated_at": datetime.now().isoformat(),
            "model": model,
            "summary": {
                "top_up_count": 0,
                "top_down_count": 0,
                "researched_count": 0,
            },
            "boards": [],
            "source_status": source_status,
        }

    related_news = _select_related_news(
        board_names=[str(item.get("board_name") or "") for item in boards],
        news_payload=news_payload,
    )
    request_payload = {
        "run_date": run_date,
        "board_candidates": boards,
        "related_news": related_news,
    }

    try:
        client = _get_research_client(model)
        completion = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": BOARD_RESEARCH_SYSTEM_PROMPT},
                {"role": "user", "content": json_dumps(request_payload)},
                {"role": "user", "content": BOARD_RESEARCH_USER_PROMPT},
            ],
            extra_body={
                "enable_search": True,
                "search_options": {
                    "forced_search": True,
                    "search_strategy": "max",
                },
            },
        )
        parsed = _parse_json_object(completion.choices[0].message.content)
        researched_boards = parsed.get("boards") if isinstance(parsed, dict) else []
        if not isinstance(researched_boards, list):
            researched_boards = []
        researched_boards = _merge_candidate_context(researched_boards, boards)
        source_status.append(
            {
                "source": "aliyun_deepseek_board_research",
                "status": "ok",
                "model": model,
                "board_count": len(researched_boards),
            }
        )
    except Exception as exc:
        LOGGER.exception("板块热度研究失败，回退为占位结果: %s", exc)
        source_status.append(
            {
                "source": "aliyun_deepseek_board_research",
                "status": "error",
                "model": model,
                "error": str(exc),
            }
        )
        researched_boards = _build_fallback_board_research(boards)

    return {
        "schema_version": 1,
        "run_date": run_date,
        "updated_at": datetime.now().isoformat(),
        "model": model,
        "summary": {
            "top_up_count": sum(1 for item in boards if item.get("direction") == "up"),
            "top_down_count": sum(1 for item in boards if item.get("direction") == "down"),
            "researched_count": len(researched_boards),
        },
        "boards": researched_boards,
        "source_status": source_status,
    }


def _merge_candidate_context(
    researched_boards: Sequence[Mapping[str, Any]],
    candidate_boards: Sequence[Mapping[str, Any]],
) -> List[Dict[str, Any]]:
    context_map = {
        str(item.get("board_name") or "").strip(): dict(item)
        for item in candidate_boards
        if isinstance(item, Mapping) and str(item.get("board_name") or "").strip()
    }

    merged: List[Dict[str, Any]] = []
    for item in researched_boards:
        board_name = str(item.get("board_name") or "").strip()
        context = context_map.get(board_name, {})
        merged_item = dict(context)
        merged_item.update(dict(item))
        if "related_stock_hints" not in merged_item:
            merged_item["related_stock_hints"] = list(context.get("related_stock_hints") or [])
        if "quant_metrics" not in merged_item:
            merged_item["quant_metrics"] = dict(context.get("quant_metrics") or {})
        merged.append(merged_item)
    return merged


def _select_related_news(
    *,
    board_names: Sequence[str],
    news_payload: Mapping[str, Any],
    limit_per_board: int = 3,
) -> List[Dict[str, Any]]:
    items = news_payload.get("items") if isinstance(news_payload, Mapping) else []
    if not isinstance(items, list):
        return []

    selected: List[Dict[str, Any]] = []
    used_ids: set[str] = set()
    for board_name in board_names:
        if not board_name:
            continue
        pattern = re.compile(re.escape(board_name), re.IGNORECASE)
        matched = 0
        for item in items:
            if not isinstance(item, Mapping):
                continue
            title = str(item.get("title") or "")
            content = str(item.get("content") or "")
            if not (pattern.search(title) or pattern.search(content)):
                continue
            news_id = str(item.get("news_id") or f"news_{len(selected)}")
            if news_id in used_ids:
                continue
            selected.append(
                {
                    "news_id": news_id,
                    "title": title,
                    "published_at": str(item.get("published_at") or ""),
                    "source": str(item.get("source") or ""),
                    "content_preview": content[:400],
                    "matched_board": board_name,
                }
            )
            used_ids.add(news_id)
            matched += 1
            if matched >= max(limit_per_board, 0):
                break
    return selected


def _build_fallback_board_research(boards: Sequence[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    fallback = []
    for item in boards:
        fallback.append(
            {
                "board_name": item.get("board_name"),
                "board_code": item.get("board_code"),
                "direction": item.get("direction"),
                "rank": item.get("rank"),
                "change_pct": item.get("change_pct"),
                "up_count": item.get("up_count"),
                "down_count": item.get("down_count"),
                "total_turnover": item.get("total_turnover"),
                "price_as_of_date": item.get("price_as_of_date"),
                "related_stock_hints": list(item.get("related_stock_hints") or []),
                "quant_metrics": dict(item.get("quant_metrics") or {}),
                "research_summary": "模型研究失败，当前仅保留结构化板块榜单、量化指标与辅助股票线索。",
                "driver_analysis": "待后续 DeepSeek 研究补充。",
                "persistence_analysis": "不确定，待后续 DeepSeek 研究补充。",
                "structure_view": "不确定，待后续 DeepSeek 研究补充。",
                "recommended_watch_stocks": [
                    {
                        "symbol": stock.get("symbol"),
                        "name": stock.get("name"),
                        "reason": str(stock.get("role_hint") or "板块辅助线索"),
                    }
                    for stock in list(item.get("related_stock_hints") or [])
                    if isinstance(stock, Mapping)
                ],
                "risk_points": ["当前为降级结果，缺少模型深度研究"],
            }
        )
    return fallback


def _get_research_client(model: str) -> OpenAI:
    api_key = os.getenv("AUDIT_MODEL_API_KEY") or os.getenv("OPENAI_API_KEY")
    base_url = os.getenv("AUDIT_MODEL_BASE_URL") or os.getenv("OPENAI_API_BASE")
    if not api_key or not base_url:
        raise ValueError("缺少 AUDIT_MODEL_API_KEY/AUDIT_MODEL_BASE_URL 或 OPENAI_API_KEY/OPENAI_API_BASE")
    LOGGER.info("初始化板块研究客户端: model=%s base_url=%s", model, base_url)
    return OpenAI(api_key=api_key, base_url=base_url)


def _parse_json_object(text: str) -> Dict[str, Any]:
    content = str(text or "").strip()
    if not content:
        raise ValueError("模型返回为空")
    fenced_match = re.search(r"```json\s*([\s\S]*?)```", content, re.IGNORECASE)
    if fenced_match:
        content = fenced_match.group(1).strip()
    start = content.find("{")
    end = content.rfind("}")
    if start == -1 or end == -1 or end <= start:
        raise ValueError(f"模型返回中未找到 JSON 对象: {text[:500]}")
    parsed = json.loads(content[start : end + 1])
    if not isinstance(parsed, dict):
        raise ValueError("模型返回不是 JSON 对象")
    return parsed


def _append_manifest(
    manifest_path: Path,
    run_date: str,
    *,
    path: Path,
    item_count: int,
    source_errors: Sequence[Mapping[str, Any]],
) -> None:
    payload = load_json_file(
        manifest_path,
        default={
            "schema_version": 1,
            "updated_at": datetime.now().isoformat(),
            "items": [],
        },
    )
    items = payload.get("items", []) if isinstance(payload, dict) else []
    items = [item for item in items if item.get("run_date") != run_date]
    items.append(
        {
            "run_date": run_date,
            "path": str(path),
            "item_count": int(item_count),
            "source_error_count": sum(
                1 for item in source_errors if str(item.get("status") or "").lower() == "error"
            ),
            "updated_at": datetime.now().isoformat(),
        }
    )
    items.sort(key=lambda item: item.get("run_date", ""), reverse=True)
    save_json_file(
        manifest_path,
        {
            "schema_version": 1,
            "updated_at": datetime.now().isoformat(),
            "items": items,
        },
    )


def json_dumps(payload: Mapping[str, Any]) -> str:
    return json.dumps(payload, ensure_ascii=False, indent=2)


def _rank_boards_by_metric(
    boards: Sequence[Mapping[str, Any]],
    *,
    metric: str,
    top_k: int,
    nested_path: Sequence[str] | None = None,
    ascending: bool = False,
    extra_metric_paths: Mapping[str, Sequence[str]] | None = None,
) -> List[Dict[str, Any]]:
    ranked: List[Dict[str, Any]] = []
    for item in boards:
        if not isinstance(item, Mapping):
            continue
        value = _extract_metric_value(item, nested_path or (metric,))
        if value is None:
            continue
        entry: Dict[str, Any] = {
            "board_name": str(item.get("board_name") or "").strip(),
            metric: value,
        }
        for extra_name, extra_path in (extra_metric_paths or {}).items():
            extra_value = _extract_metric_value(item, extra_path)
            if extra_value is not None:
                entry[extra_name] = extra_value
        ranked.append(entry)

    ranked.sort(key=lambda item: float(item.get(metric) or 0.0), reverse=not ascending)
    result: List[Dict[str, Any]] = []
    for rank, item in enumerate(ranked[: max(top_k, 0)], start=1):
        result.append({"rank": rank, **item})
    return result


def _extract_metric_value(item: Mapping[str, Any], path: Sequence[str]) -> float | int | None:
    current: Any = item
    for key in path:
        if not isinstance(current, Mapping):
            return None
        current = current.get(key)
    if isinstance(current, bool) or current is None:
        return None
    if isinstance(current, (int, float)):
        return current
    return None
