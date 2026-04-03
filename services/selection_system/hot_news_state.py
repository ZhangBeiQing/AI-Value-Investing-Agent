"""Incremental hot-news theme state builder for the selection system."""

from __future__ import annotations

import json
import math
import os
import re
from collections import Counter
from datetime import datetime
from hashlib import sha1
from http import HTTPStatus
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Sequence

from dotenv import load_dotenv
from openai import OpenAI

from core.logging import get_logger

from .bootstrap import initialize_selection_system
from .hot_news_store import HotNewsStateStore
from .paths import SelectionSystemPaths
from .store import load_json_file, save_json_file

try:
    import dashscope
except ImportError:  # pragma: no cover - runtime dependency validated during execution
    dashscope = None


LOGGER = get_logger("SelectionHotNews")
load_dotenv(".env")

DEFAULT_THEME_MODEL = "deepseek-v3.2-exp"
DEFAULT_EMBEDDING_MODEL = "text-embedding-v4"
DEFAULT_CANDIDATE_LIMIT = 8
PROMPT_NEWS_LIMIT = 40
MATCH_LIMIT_PER_CANDIDATE = 4
ARCHIVED_THEME_SCAN_LIMIT = 40
MERGE_PAIR_LIMIT = 6
ACTIVE_TO_COOLING_DAYS = 2
COOLING_TO_ARCHIVE_DAYS = 7
MAX_LINKS_PER_THEME = 12
MAX_POINTS_PER_THEME = 6
VALID_STRENGTHS = {"emergence", "strengthening", "mature", "fading"}
VALID_ACTIONS = {"ADD_THEME", "UPDATE_THEME", "DROP_CANDIDATE"}

THEME_EXTRACTION_SYSTEM_PROMPT = """你是A股热点主题状态库的候选抽取器。

你的职责不是写新闻摘要，而是从“今天的新闻 + 今日板块线索”里抽取少量值得进入主题状态机的候选。

必须遵守：
1. 宁缺毋滥，不要因为有新闻就强行建主题。
2. 主题名称要稳定、可复用，避免使用“今日”“消息面”“最新进展”这类临时表述。
3. 单条公司零碎公告、一次性八卦、信息量很低的消息，不应升级为主题。
4. 如果多条新闻描述的是同一条市场主线，要合并成一个候选，不要拆碎。
5. 重点考虑政策、产业催化、地缘风险、商品涨价、宏观交易主线、行业景气、重大监管变化。
6. 只允许依据输入内容判断，不要联网，不要补充外部事实。
7. 只输出一个 JSON 对象，不要输出 markdown，不要输出解释。
"""

THEME_EXTRACTION_USER_PROMPT = """请输出 JSON：
{
  "market_regime_note": "一句话描述今天市场主线环境",
  "candidates": [
    {
      "theme_name": "稳定的主题名",
      "summary": "这个候选主题今天发生了什么",
      "today_delta": "今天相对历史主题新增了什么",
      "strength": "emergence|strengthening|mature|fading",
      "persistence_view": "已持续多久、可能还会持续多久；不确定就写不确定",
      "bull_case": ["看多点1", "看多点2"],
      "bear_case": ["风险点1", "风险点2"],
      "linked_boards": ["板块1", "板块2"],
      "linked_symbols": ["600000.SH", "000001.SZ"],
      "evidence_news_ids": ["B001", "C003"],
      "should_track": true,
      "drop_reason": ""
    }
  ]
}

要求：
1. candidates 最多保留少量高价值候选。
2. 如果某个候选 should_track=false，也保留在 candidates 里，并写 drop_reason，方便下游裁决。
3. evidence_news_ids 必须来自输入。
4. strength 只能是 emergence / strengthening / mature / fading。
5. bull_case、bear_case 没有就给空数组。
"""

THEME_OP_SYSTEM_PROMPT = """你是热点主题状态机的日更调度器。

你的任务不是重写整份状态，而是只对今天的候选做操作判定。

你必须遵守：
1. 默认保守，不要轻易 ADD_THEME。
2. 如果今天候选只是旧主题的延续，应 UPDATE_THEME，而不是新建。
3. 如果候选信息量太弱、或只是一次性噪声，应 DROP_CANDIDATE。
4. 不要每天给同一主题改名；如果旧主题名已经足够稳定，优先沿用旧主题名。
5. 只允许使用三个动作：ADD_THEME / UPDATE_THEME / DROP_CANDIDATE。
6. 输出必须是 JSON，不要输出 markdown，不要输出解释。
"""

THEME_OP_USER_PROMPT = """请输出 JSON：
{
  "market_regime_note": "一句话市场主线说明",
  "candidate_actions": [
    {
      "candidate_id": "cand_01",
      "action": "ADD_THEME|UPDATE_THEME|DROP_CANDIDATE",
      "target_theme_id": "如果是 UPDATE_THEME 则填写，否则留空",
      "reason": "为什么这么判定",
      "canonical_theme_name": "最终主题名；如果是 UPDATE_THEME 优先沿用旧主题",
      "summary": "最终主题摘要",
      "today_delta": "今天新增变化",
      "strength": "emergence|strengthening|mature|fading",
      "persistence_view": "持续性判断",
      "bull_case": ["看多点1", "看多点2"],
      "bear_case": ["风险点1", "风险点2"],
      "linked_boards": ["板块1"],
      "linked_symbols": ["600000.SH"],
      "evidence_news_ids": ["B001", "C003"]
    }
  ],
  "archive_actions": [
    {
      "theme_id": "旧主题ID",
      "reason": "为什么应归档"
    }
  ]
}

要求：
1. candidate_actions 必须覆盖所有输入 candidate_id。
2. target_theme_id 只能引用输入中给出的旧主题 ID。
3. 如果 evidence_news_ids 不足，就保留候选自己的证据，不要编造。
4. archive_actions 只在非常明确退潮或被替代时才输出。
"""

THEME_MERGE_SYSTEM_PROMPT = """你是热点主题库的去重整理器。

你的任务只判断：给定的旧主题对，是否应该 merge。

必须遵守：
1. 只有在两个主题长期看本质上是同一条交易叙事时，才允许 merge。
2. 大板块词与细分催化词，默认不要 merge。
3. 如果只是同属一个行业但驱动不同，也不要 merge。
4. 输出 JSON，不要输出 markdown，不要解释。
"""

THEME_MERGE_USER_PROMPT = """请输出 JSON：
{
  "merge_actions": [
    {
      "source_theme_id": "被并入的主题ID",
      "target_theme_id": "保留的主题ID",
      "reason": "为什么这两个主题应合并"
    }
  ]
}

要求：
1. 只有你确信 mergeable 时才输出。
2. target_theme_id 和 source_theme_id 必须来自输入的 pair。
3. 同一主题最多参与一次 merge。
"""


def build_hot_news_state(
    run_date: str,
    *,
    base_dir: str | Path = "data",
    model: str = DEFAULT_THEME_MODEL,
    embedding_model: str = DEFAULT_EMBEDDING_MODEL,
    candidate_limit: int = DEFAULT_CANDIDATE_LIMIT,
    force_rebuild: bool = False,
) -> Dict[str, Path]:
    paths = initialize_selection_system(SelectionSystemPaths.from_base_dir(base_dir))
    paths.ensure_run_dir(run_date)
    store = HotNewsStateStore(paths.hot_news_state_db_path)

    if store.has_future_snapshots(run_date):
        latest_run_date = store.latest_snapshot_run_date()
        raise ValueError(
            f"存在晚于 {run_date} 的 hot_news_state 快照，当前不允许直接重建历史日期。"
            f" latest_snapshot_run_date={latest_run_date}"
        )

    if store.has_snapshot_for_run(run_date):
        if force_rebuild:
            LOGGER.info("检测到同日快照，开始回滚后重建: run_date=%s", run_date)
            store.rollback_run_date(run_date)
        elif paths.run_hot_news_state_path(run_date).exists() and paths.run_hot_news_ops_path(run_date).exists():
            LOGGER.info("run_date=%s 已存在 hot_news_state，直接复用现有产物", run_date)
            return {
                "hot_news_state": paths.run_hot_news_state_path(run_date),
                "hot_news_ops": paths.run_hot_news_ops_path(run_date),
                "hot_news_state_db": paths.hot_news_state_db_path,
            }

    LOGGER.info(
        "开始构建 hot_news_state: run_date=%s model=%s embedding_model=%s candidate_limit=%d",
        run_date,
        model,
        embedding_model,
        candidate_limit,
    )

    source_status: List[Dict[str, Any]] = []
    news_items = _load_news_items(paths, run_date, source_status=source_status)
    board_context = _load_board_context(paths, run_date, source_status=source_status)
    prompt_news = _prepare_prompt_news(news_items, limit=PROMPT_NEWS_LIMIT)
    news_by_id = {str(item.get("news_id") or ""): item for item in news_items if str(item.get("news_id") or "").strip()}

    candidates, extraction_note = _extract_theme_candidates(
        run_date,
        prompt_news=prompt_news,
        board_context=board_context,
        model=model,
        candidate_limit=candidate_limit,
        source_status=source_status,
    )

    existing_themes = store.load_themes_before(run_date)
    retrievals = _retrieve_relevant_themes(
        store,
        candidates=candidates,
        existing_themes=existing_themes,
        embedding_model=embedding_model,
        source_status=source_status,
    )

    if existing_themes:
        candidate_actions, planner_note, archive_actions = _plan_candidate_actions(
            run_date,
            candidates=candidates,
            retrievals=retrievals,
            model=model,
            source_status=source_status,
        )
    else:
        candidate_actions = _build_bootstrap_candidate_actions(candidates)
        planner_note = extraction_note
        archive_actions = []
        source_status.append(
            {
                "source": "hot_news_theme_ops",
                "status": "skipped",
                "model": model,
                "reason": "bootstrap_without_previous_state",
            }
        )

    candidate_by_id = {str(item.get("candidate_id")): item for item in candidates}
    themes_by_id = {str(theme.get("theme_id")): dict(theme) for theme in existing_themes}
    applied_operations: List[Dict[str, Any]] = []

    touched_theme_ids = _apply_candidate_actions(
        store,
        run_date=run_date,
        candidate_actions=candidate_actions,
        candidate_by_id=candidate_by_id,
        themes_by_id=themes_by_id,
        news_by_id=news_by_id,
        applied_operations=applied_operations,
    )

    merge_actions = _plan_theme_merges(
        store,
        themes=list(themes_by_id.values()),
        model=model,
        embedding_model=embedding_model,
        source_status=source_status,
    )
    touched_theme_ids.update(
        _apply_merge_actions(
            store,
            run_date=run_date,
            merge_actions=merge_actions,
            themes_by_id=themes_by_id,
            applied_operations=applied_operations,
        )
    )

    touched_theme_ids.update(
        _apply_archive_actions(
            store,
            run_date=run_date,
            archive_actions=archive_actions,
            themes_by_id=themes_by_id,
            applied_operations=applied_operations,
        )
    )
    touched_theme_ids.update(
        _apply_automatic_aging(
            store,
            run_date=run_date,
            themes_by_id=themes_by_id,
            touched_theme_ids=touched_theme_ids,
            applied_operations=applied_operations,
        )
    )

    all_themes = sorted(
        (dict(theme) for theme in themes_by_id.values()),
        key=lambda item: (
            0 if item.get("status") == "active" else 1 if item.get("status") == "cooling" else 2,
            str(item.get("last_seen_at") or ""),
        ),
        reverse=False,
    )
    visible_themes = [theme for theme in all_themes if theme.get("status") in {"active", "cooling"}]

    for theme in all_themes:
        store.write_theme(theme)
    store.persist_daily_snapshot(run_date, all_themes)

    market_regime_note = planner_note or extraction_note
    state_payload = _build_state_payload(
        run_date=run_date,
        model=model,
        embedding_model=embedding_model,
        themes=visible_themes,
        all_themes=all_themes,
        candidates=candidates,
        operations=applied_operations,
        market_regime_note=market_regime_note,
        source_status=source_status,
    )
    ops_payload = {
        "schema_version": 1,
        "run_date": run_date,
        "updated_at": datetime.now().isoformat(),
        "model": model,
        "embedding_model": embedding_model,
        "input_summary": {
            "news_count": len(news_items),
            "prompt_news_count": len(prompt_news),
            "board_context_count": len(board_context),
            "existing_theme_count": len(existing_themes),
        },
        "extracted_candidates": candidates,
        "retrievals": retrievals,
        "planned_candidate_actions": candidate_actions,
        "planned_archive_actions": archive_actions,
        "planned_merge_actions": merge_actions,
        "applied_operations": applied_operations,
        "source_status": source_status,
    }

    save_json_file(paths.run_hot_news_state_path(run_date), state_payload)
    save_json_file(paths.run_hot_news_ops_path(run_date), ops_payload)
    save_json_file(paths.hot_news_state_daily_path(run_date), state_payload)
    save_json_file(paths.hot_news_state_daily_ops_path(run_date), ops_payload)
    save_json_file(paths.hot_news_state_latest_path, state_payload)
    _append_manifest(
        paths.hot_news_state_manifest_path,
        run_date=run_date,
        path=paths.hot_news_state_daily_path(run_date),
        theme_count=len(visible_themes),
        source_status=source_status,
    )

    LOGGER.info(
        "hot_news_state 已写入: state=%s ops=%s themes=%d",
        paths.run_hot_news_state_path(run_date),
        paths.run_hot_news_ops_path(run_date),
        len(visible_themes),
    )
    return {
        "hot_news_state": paths.run_hot_news_state_path(run_date),
        "hot_news_ops": paths.run_hot_news_ops_path(run_date),
        "hot_news_state_db": paths.hot_news_state_db_path,
    }


class _DashScopeEmbeddingClient:
    def __init__(self, model: str) -> None:
        if dashscope is None:
            raise ValueError("未安装 dashscope，无法使用阿里云 embedding")
        self.model = model
        self.api_key = os.getenv("AUDIT_MODEL_API_KEY") or os.getenv("OPENAI_API_KEY")
        if not self.api_key:
            raise ValueError("缺少 AUDIT_MODEL_API_KEY 或 OPENAI_API_KEY")

    def embed_texts(self, texts: Sequence[str]) -> List[List[float]]:
        cleaned = [str(text or "").strip() for text in texts]
        if not cleaned:
            return []

        response = dashscope.TextEmbedding.call(
            model=self.model,
            input=cleaned if len(cleaned) > 1 else cleaned[0],
            api_key=self.api_key,
        )

        status_code = getattr(response, "status_code", None)
        if status_code not in (None, HTTPStatus.OK, 200):
            message = getattr(response, "message", "") or getattr(response, "code", "") or str(response)
            raise ValueError(f"DashScope embedding 调用失败: status_code={status_code} message={message}")

        payload = _dashscope_response_to_dict(response)
        output = payload.get("output") if isinstance(payload, dict) else {}
        embeddings = output.get("embeddings") or output.get("embedding") or []
        if isinstance(embeddings, dict):
            embeddings = [embeddings]
        vectors: List[List[float]] = []
        for item in embeddings:
            if not isinstance(item, Mapping):
                continue
            vector = item.get("embedding") or item.get("vector")
            if isinstance(vector, list):
                try:
                    vectors.append([float(value) for value in vector])
                except (TypeError, ValueError):
                    continue

        if len(cleaned) == 1 and not vectors:
            single = output.get("embedding")
            if isinstance(single, list):
                vectors = [[float(value) for value in single]]

        if len(vectors) != len(cleaned):
            raise ValueError(f"DashScope embedding 返回数量异常: expected={len(cleaned)} actual={len(vectors)}")
        return vectors


def _dashscope_response_to_dict(response: Any) -> Dict[str, Any]:
    if isinstance(response, dict):
        return dict(response)
    if hasattr(response, "to_dict"):
        try:
            payload = response.to_dict()
            if isinstance(payload, dict):
                return payload
        except Exception:
            pass
    data = getattr(response, "__dict__", None)
    if isinstance(data, dict):
        return data
    return {}


def _get_theme_llm_client(model: str) -> OpenAI:
    api_key = os.getenv("AUDIT_MODEL_API_KEY") or os.getenv("OPENAI_API_KEY")
    base_url = os.getenv("AUDIT_MODEL_BASE_URL") or os.getenv("OPENAI_API_BASE")
    if not api_key or not base_url:
        raise ValueError("缺少 AUDIT_MODEL_API_KEY/AUDIT_MODEL_BASE_URL 或 OPENAI_API_KEY/OPENAI_API_BASE")
    LOGGER.info("初始化 hot news LLM 客户端: model=%s base_url=%s", model, base_url)
    return OpenAI(api_key=api_key, base_url=base_url)


def _load_news_items(
    paths: SelectionSystemPaths,
    run_date: str,
    *,
    source_status: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    payload = load_json_file(paths.run_news_enriched_path(run_date), default=None)
    if not isinstance(payload, dict):
        source_status.append(
            {
                "source": "03_news_enriched.json",
                "status": "warning",
                "path": str(paths.run_news_enriched_path(run_date)),
                "error": "missing_or_invalid",
            }
        )
        return []

    items = [dict(item) for item in payload.get("items", []) if isinstance(item, dict)]
    source_status.append(
        {
            "source": "03_news_enriched.json",
            "status": "ok",
            "path": str(paths.run_news_enriched_path(run_date)),
            "rows": len(items),
        }
    )
    return items


def _load_board_context(
    paths: SelectionSystemPaths,
    run_date: str,
    *,
    source_status: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    candidate_paths = (
        paths.run_board_heat_state_path(run_date),
        paths.board_heat_state_daily_path(run_date),
        paths.board_heat_state_latest_path,
    )
    for path in candidate_paths:
        payload = load_json_file(path, default=None)
        if not isinstance(payload, dict):
            continue
        payload_run_date = str(payload.get("run_date") or "").strip()[:10]
        if payload_run_date and payload_run_date > str(run_date).strip()[:10]:
            continue
        boards = payload.get("boards")
        if not isinstance(boards, list) or not boards:
            continue
        context = []
        for item in boards[:8]:
            if not isinstance(item, dict):
                continue
            context.append(
                {
                    "board_name": str(item.get("board_name") or "").strip(),
                    "direction": str(item.get("direction") or "").strip(),
                    "change_pct": item.get("change_pct"),
                    "research_summary": _trim_text(item.get("research_summary"), 120),
                    "driver_analysis": _trim_text(item.get("driver_analysis"), 120),
                    "persistence_analysis": _trim_text(item.get("persistence_analysis"), 120),
                }
            )
        source_status.append(
            {
                "source": "board_heat_state",
                "status": "ok",
                "path": str(path),
                "rows": len(context),
            }
        )
        return context

    source_status.append(
        {
            "source": "board_heat_state",
            "status": "warning",
            "error": "missing_or_invalid",
        }
    )
    return []


def _prepare_prompt_news(items: Sequence[Mapping[str, Any]], *, limit: int) -> List[Dict[str, Any]]:
    sorted_items = sorted(
        (dict(item) for item in items),
        key=lambda item: str(item.get("published_at") or ""),
        reverse=True,
    )
    prepared: List[Dict[str, Any]] = []
    for item in sorted_items[: max(limit, 0)]:
        prepared.append(
            {
                "news_id": str(item.get("news_id") or "").strip(),
                "title": _trim_text(item.get("title"), 80),
                "published_at": str(item.get("published_at") or ""),
                "source": str(item.get("source") or ""),
                "content": _trim_text(item.get("content") or item.get("preview") or item.get("title"), 260),
            }
        )
    return prepared


def _extract_theme_candidates(
    run_date: str,
    *,
    prompt_news: Sequence[Mapping[str, Any]],
    board_context: Sequence[Mapping[str, Any]],
    model: str,
    candidate_limit: int,
    source_status: List[Dict[str, Any]],
) -> tuple[List[Dict[str, Any]], str]:
    if not prompt_news:
        source_status.append(
            {
                "source": "hot_news_candidate_extraction",
                "status": "warning",
                "model": model,
                "error": "no_news_input",
            }
        )
        return [], ""

    request_payload = {
        "run_date": run_date,
        "news_items": list(prompt_news),
        "board_context": list(board_context),
        "candidate_limit": max(int(candidate_limit), 1),
    }
    try:
        client = _get_theme_llm_client(model)
        completion = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": THEME_EXTRACTION_SYSTEM_PROMPT},
                {"role": "user", "content": json.dumps(request_payload, ensure_ascii=False, indent=2)},
                {"role": "user", "content": THEME_EXTRACTION_USER_PROMPT},
            ],
        )
        parsed = _parse_json_object(completion.choices[0].message.content)
        candidates, market_regime_note = _normalize_candidates(parsed, prompt_news, candidate_limit=candidate_limit)
        source_status.append(
            {
                "source": "hot_news_candidate_extraction",
                "status": "ok",
                "model": model,
                "candidate_count": len(candidates),
            }
        )
        return candidates, market_regime_note
    except Exception as exc:
        LOGGER.exception("主题候选抽取失败，回退到规则降级: %s", exc)
        source_status.append(
            {
                "source": "hot_news_candidate_extraction",
                "status": "error",
                "model": model,
                "error": str(exc),
            }
        )
        return _build_fallback_candidates(prompt_news, candidate_limit=candidate_limit), ""


def _normalize_candidates(
    payload: Mapping[str, Any],
    prompt_news: Sequence[Mapping[str, Any]],
    *,
    candidate_limit: int,
) -> tuple[List[Dict[str, Any]], str]:
    valid_news_ids = {str(item.get("news_id") or "") for item in prompt_news}
    market_regime_note = str(payload.get("market_regime_note") or "").strip()
    raw_candidates = payload.get("candidates")
    if not isinstance(raw_candidates, list):
        return [], market_regime_note

    normalized: List[Dict[str, Any]] = []
    for index, item in enumerate(raw_candidates[: max(candidate_limit, 0)], start=1):
        if not isinstance(item, Mapping):
            continue
        theme_name = str(item.get("theme_name") or "").strip()
        if not theme_name:
            continue
        evidence_news_ids = [news_id for news_id in _limit_list(item.get("evidence_news_ids") or [], MAX_LINKS_PER_THEME) if news_id in valid_news_ids]
        normalized.append(
            {
                "candidate_id": f"cand_{index:02d}",
                "theme_name": theme_name,
                "summary": str(item.get("summary") or "").strip() or theme_name,
                "today_delta": str(item.get("today_delta") or "").strip(),
                "strength": _normalize_strength(item.get("strength")),
                "persistence_view": str(item.get("persistence_view") or "").strip(),
                "bull_case": _limit_list(item.get("bull_case") or [], MAX_POINTS_PER_THEME),
                "bear_case": _limit_list(item.get("bear_case") or [], MAX_POINTS_PER_THEME),
                "linked_boards": _limit_list(item.get("linked_boards") or [], MAX_LINKS_PER_THEME),
                "linked_symbols": _limit_list(item.get("linked_symbols") or [], MAX_LINKS_PER_THEME),
                "evidence_news_ids": evidence_news_ids,
                "should_track": bool(item.get("should_track", True)),
                "drop_reason": str(item.get("drop_reason") or "").strip(),
            }
        )
    return normalized, market_regime_note


def _build_fallback_candidates(
    prompt_news: Sequence[Mapping[str, Any]],
    *,
    candidate_limit: int,
) -> List[Dict[str, Any]]:
    fallback: List[Dict[str, Any]] = []
    for index, item in enumerate(prompt_news[: max(candidate_limit, 0)], start=1):
        title = str(item.get("title") or "").strip()
        news_id = str(item.get("news_id") or "").strip()
        if not title or not news_id:
            continue
        fallback.append(
            {
                "candidate_id": f"cand_{index:02d}",
                "theme_name": _fallback_theme_name(title),
                "summary": _trim_text(item.get("content") or title, 120),
                "today_delta": title,
                "strength": "emergence",
                "persistence_view": "不确定",
                "bull_case": [],
                "bear_case": [],
                "linked_boards": [],
                "linked_symbols": [],
                "evidence_news_ids": [news_id],
                "should_track": True,
                "drop_reason": "",
            }
        )
    return fallback


def _retrieve_relevant_themes(
    store: HotNewsStateStore,
    *,
    candidates: Sequence[Mapping[str, Any]],
    existing_themes: Sequence[Mapping[str, Any]],
    embedding_model: str,
    source_status: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    if not candidates or not existing_themes:
        source_status.append(
            {
                "source": "dashscope_text_embedding",
                "status": "skipped",
                "model": embedding_model,
                "reason": "no_candidates_or_themes",
            }
        )
        return [
            {
                "candidate_id": str(candidate.get("candidate_id") or ""),
                "matches": [],
            }
            for candidate in candidates
        ]

    candidate_texts = {str(candidate.get("candidate_id")): _build_candidate_embedding_text(candidate) for candidate in candidates}
    theme_texts = {str(theme.get("theme_id")): _build_theme_embedding_text(theme) for theme in existing_themes}

    embeddings: Dict[str, List[float]] = {}
    try:
        embedding_client = _DashScopeEmbeddingClient(embedding_model)
        embeddings = _load_embeddings_with_cache(
            store,
            model=embedding_model,
            embedding_client=embedding_client,
            texts={**candidate_texts, **theme_texts},
        )
        source_status.append(
            {
                "source": "dashscope_text_embedding",
                "status": "ok",
                "model": embedding_model,
                "text_count": len(candidate_texts) + len(theme_texts),
            }
        )
    except Exception as exc:
        LOGGER.exception("embedding 召回失败，将仅使用规则匹配: %s", exc)
        source_status.append(
            {
                "source": "dashscope_text_embedding",
                "status": "error",
                "model": embedding_model,
                "error": str(exc),
            }
        )

    retrievals: List[Dict[str, Any]] = []
    for candidate in candidates:
        candidate_id = str(candidate.get("candidate_id") or "")
        candidate_vector = embeddings.get(candidate_id)
        scored_matches: List[Dict[str, Any]] = []
        for theme in existing_themes:
            theme_id = str(theme.get("theme_id") or "")
            theme_vector = embeddings.get(theme_id)
            similarity = _cosine_similarity(candidate_vector, theme_vector)
            rule_score = _rule_match_score(candidate, theme)
            combined_score = rule_score + (max(similarity, 0.0) * 0.85 if similarity is not None else 0.0)
            if not _should_keep_match(theme, rule_score=rule_score, similarity=similarity, combined_score=combined_score):
                continue
            scored_matches.append(
                {
                    "theme_id": theme_id,
                    "theme_name": theme.get("theme_name"),
                    "status": theme.get("status"),
                    "score": combined_score,
                    "rule_score": rule_score,
                    "embedding_similarity": similarity,
                    "summary": _trim_text(theme.get("summary"), 100),
                    "linked_boards": list(theme.get("linked_boards") or []),
                    "linked_symbols": list(theme.get("linked_symbols") or []),
                }
            )

        scored_matches.sort(key=lambda item: float(item.get("score") or 0.0), reverse=True)
        retrievals.append(
            {
                "candidate_id": candidate_id,
                "matches": scored_matches[:MATCH_LIMIT_PER_CANDIDATE],
            }
        )
    return retrievals


def _load_embeddings_with_cache(
    store: HotNewsStateStore,
    *,
    model: str,
    embedding_client: _DashScopeEmbeddingClient,
    texts: Mapping[str, str],
) -> Dict[str, List[float]]:
    resolved: Dict[str, List[float]] = {}
    uncached_keys: List[str] = []
    uncached_texts: List[str] = []

    for key, text in texts.items():
        text_hash = _text_hash(text)
        cached = store.load_cached_embedding(text_hash=text_hash, model=model)
        if cached is not None:
            resolved[key] = cached
            continue
        uncached_keys.append(key)
        uncached_texts.append(text)

    if uncached_texts:
        batch_size = 8
        for start in range(0, len(uncached_texts), batch_size):
            batch_texts = uncached_texts[start : start + batch_size]
            batch_keys = uncached_keys[start : start + batch_size]
            vectors = embedding_client.embed_texts(batch_texts)
            for key, text, vector in zip(batch_keys, batch_texts, vectors):
                resolved[key] = vector
                store.save_cached_embedding(
                    text_hash=_text_hash(text),
                    model=model,
                    text=text,
                    vector=vector,
                )
    return resolved


def _plan_candidate_actions(
    run_date: str,
    *,
    candidates: Sequence[Mapping[str, Any]],
    retrievals: Sequence[Mapping[str, Any]],
    model: str,
    source_status: List[Dict[str, Any]],
) -> tuple[List[Dict[str, Any]], str, List[Dict[str, Any]]]:
    if not candidates:
        source_status.append(
            {
                "source": "hot_news_theme_ops",
                "status": "skipped",
                "model": model,
                "reason": "no_candidates",
            }
        )
        return [], "", []

    request_payload = {
        "run_date": run_date,
        "candidates": list(candidates),
        "candidate_retrievals": list(retrievals),
    }
    try:
        client = _get_theme_llm_client(model)
        completion = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": THEME_OP_SYSTEM_PROMPT},
                {"role": "user", "content": json.dumps(request_payload, ensure_ascii=False, indent=2)},
                {"role": "user", "content": THEME_OP_USER_PROMPT},
            ],
        )
        parsed = _parse_json_object(completion.choices[0].message.content)
        source_status.append(
            {
                "source": "hot_news_theme_ops",
                "status": "ok",
                "model": model,
                "action_count": len(parsed.get("candidate_actions", [])) if isinstance(parsed.get("candidate_actions"), list) else 0,
            }
        )
        return _normalize_candidate_actions(parsed, candidates=candidates, retrievals=retrievals)
    except Exception as exc:
        LOGGER.exception("主题操作规划失败，回退为保守判定: %s", exc)
        source_status.append(
            {
                "source": "hot_news_theme_ops",
                "status": "error",
                "model": model,
                "error": str(exc),
            }
        )
        return _build_fallback_candidate_actions(candidates, retrievals), "", []


def _build_bootstrap_candidate_actions(candidates: Sequence[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    bootstrap_actions: List[Dict[str, Any]] = []
    for candidate in candidates:
        should_track = bool(candidate.get("should_track", True))
        bootstrap_actions.append(
            {
                "candidate_id": str(candidate.get("candidate_id") or ""),
                "action": "ADD_THEME" if should_track else "DROP_CANDIDATE",
                "target_theme_id": "",
                "reason": "bootstrap_add_theme" if should_track else str(candidate.get("drop_reason") or "bootstrap_drop_candidate"),
                "canonical_theme_name": str(candidate.get("theme_name") or "").strip(),
                "summary": str(candidate.get("summary") or "").strip(),
                "today_delta": str(candidate.get("today_delta") or "").strip(),
                "strength": _normalize_strength(candidate.get("strength")),
                "persistence_view": str(candidate.get("persistence_view") or "").strip(),
                "bull_case": _limit_list(candidate.get("bull_case") or [], MAX_POINTS_PER_THEME),
                "bear_case": _limit_list(candidate.get("bear_case") or [], MAX_POINTS_PER_THEME),
                "linked_boards": _limit_list(candidate.get("linked_boards") or [], MAX_LINKS_PER_THEME),
                "linked_symbols": _limit_list(candidate.get("linked_symbols") or [], MAX_LINKS_PER_THEME),
                "evidence_news_ids": _limit_list(candidate.get("evidence_news_ids") or [], MAX_LINKS_PER_THEME),
            }
        )
    return bootstrap_actions


def _normalize_candidate_actions(
    payload: Mapping[str, Any],
    *,
    candidates: Sequence[Mapping[str, Any]],
    retrievals: Sequence[Mapping[str, Any]],
) -> tuple[List[Dict[str, Any]], str, List[Dict[str, Any]]]:
    retrieval_map = {str(item.get("candidate_id") or ""): item for item in retrievals}
    candidate_map = {str(item.get("candidate_id") or ""): item for item in candidates}
    raw_actions = payload.get("candidate_actions")
    market_regime_note = str(payload.get("market_regime_note") or "").strip()
    raw_archives = payload.get("archive_actions")

    actions_by_candidate: Dict[str, Dict[str, Any]] = {}
    for item in raw_actions if isinstance(raw_actions, list) else []:
        if not isinstance(item, Mapping):
            continue
        candidate_id = str(item.get("candidate_id") or "").strip()
        if candidate_id not in candidate_map:
            continue
        retrieval_matches = retrieval_map.get(candidate_id, {}).get("matches", [])
        valid_theme_ids = {str(match.get("theme_id") or "") for match in retrieval_matches if str(match.get("theme_id") or "").strip()}
        action = str(item.get("action") or "").strip().upper()
        if action not in VALID_ACTIONS:
            action = "DROP_CANDIDATE"
        target_theme_id = str(item.get("target_theme_id") or "").strip()
        if action == "UPDATE_THEME" and target_theme_id not in valid_theme_ids:
            action = "DROP_CANDIDATE"
            target_theme_id = ""
        candidate = candidate_map[candidate_id]
        if not valid_theme_ids and action == "DROP_CANDIDATE" and bool(candidate.get("should_track", True)):
            action = "ADD_THEME"
            target_theme_id = ""
        actions_by_candidate[candidate_id] = {
            "candidate_id": candidate_id,
            "action": action,
            "target_theme_id": target_theme_id,
            "reason": str(item.get("reason") or "").strip(),
            "canonical_theme_name": str(item.get("canonical_theme_name") or candidate.get("theme_name") or "").strip(),
            "summary": str(item.get("summary") or candidate.get("summary") or "").strip(),
            "today_delta": str(item.get("today_delta") or candidate.get("today_delta") or "").strip(),
            "strength": _normalize_strength(item.get("strength") or candidate.get("strength")),
            "persistence_view": str(item.get("persistence_view") or candidate.get("persistence_view") or "").strip(),
            "bull_case": _limit_list(item.get("bull_case") or candidate.get("bull_case") or [], MAX_POINTS_PER_THEME),
            "bear_case": _limit_list(item.get("bear_case") or candidate.get("bear_case") or [], MAX_POINTS_PER_THEME),
            "linked_boards": _limit_list(item.get("linked_boards") or candidate.get("linked_boards") or [], MAX_LINKS_PER_THEME),
            "linked_symbols": _limit_list(item.get("linked_symbols") or candidate.get("linked_symbols") or [], MAX_LINKS_PER_THEME),
            "evidence_news_ids": _limit_list(item.get("evidence_news_ids") or candidate.get("evidence_news_ids") or [], MAX_LINKS_PER_THEME),
        }

    normalized_actions: List[Dict[str, Any]] = []
    for candidate in candidates:
        candidate_id = str(candidate.get("candidate_id") or "")
        action = actions_by_candidate.get(candidate_id)
        if action is None:
            action = {
                "candidate_id": candidate_id,
                "action": "DROP_CANDIDATE",
                "target_theme_id": "",
                "reason": "planner_missing_action",
                "canonical_theme_name": str(candidate.get("theme_name") or ""),
                "summary": str(candidate.get("summary") or ""),
                "today_delta": str(candidate.get("today_delta") or ""),
                "strength": _normalize_strength(candidate.get("strength")),
                "persistence_view": str(candidate.get("persistence_view") or ""),
                "bull_case": _limit_list(candidate.get("bull_case") or [], MAX_POINTS_PER_THEME),
                "bear_case": _limit_list(candidate.get("bear_case") or [], MAX_POINTS_PER_THEME),
                "linked_boards": _limit_list(candidate.get("linked_boards") or [], MAX_LINKS_PER_THEME),
                "linked_symbols": _limit_list(candidate.get("linked_symbols") or [], MAX_LINKS_PER_THEME),
                "evidence_news_ids": _limit_list(candidate.get("evidence_news_ids") or [], MAX_LINKS_PER_THEME),
            }
        normalized_actions.append(action)

    archives: List[Dict[str, Any]] = []
    for item in raw_archives if isinstance(raw_archives, list) else []:
        if not isinstance(item, Mapping):
            continue
        theme_id = str(item.get("theme_id") or "").strip()
        if not theme_id:
            continue
        archives.append(
            {
                "theme_id": theme_id,
                "reason": str(item.get("reason") or "").strip(),
            }
        )
    return normalized_actions, market_regime_note, archives


def _build_fallback_candidate_actions(
    candidates: Sequence[Mapping[str, Any]],
    retrievals: Sequence[Mapping[str, Any]],
) -> List[Dict[str, Any]]:
    retrieval_map = {str(item.get("candidate_id") or ""): item for item in retrievals}
    fallback: List[Dict[str, Any]] = []
    for candidate in candidates:
        candidate_id = str(candidate.get("candidate_id") or "")
        matches = retrieval_map.get(candidate_id, {}).get("matches", [])
        if matches:
            best_match = matches[0]
            fallback.append(
                {
                    "candidate_id": candidate_id,
                    "action": "UPDATE_THEME",
                    "target_theme_id": str(best_match.get("theme_id") or ""),
                    "reason": "fallback_best_match",
                    "canonical_theme_name": str(best_match.get("theme_name") or candidate.get("theme_name") or ""),
                    "summary": str(candidate.get("summary") or ""),
                    "today_delta": str(candidate.get("today_delta") or ""),
                    "strength": _normalize_strength(candidate.get("strength")),
                    "persistence_view": str(candidate.get("persistence_view") or ""),
                    "bull_case": _limit_list(candidate.get("bull_case") or [], MAX_POINTS_PER_THEME),
                    "bear_case": _limit_list(candidate.get("bear_case") or [], MAX_POINTS_PER_THEME),
                    "linked_boards": _limit_list(candidate.get("linked_boards") or [], MAX_LINKS_PER_THEME),
                    "linked_symbols": _limit_list(candidate.get("linked_symbols") or [], MAX_LINKS_PER_THEME),
                    "evidence_news_ids": _limit_list(candidate.get("evidence_news_ids") or [], MAX_LINKS_PER_THEME),
                }
            )
            continue
        fallback.append(
            {
                "candidate_id": candidate_id,
                "action": "ADD_THEME" if candidate.get("should_track", True) else "DROP_CANDIDATE",
                "target_theme_id": "",
                "reason": "fallback_add_or_drop",
                "canonical_theme_name": str(candidate.get("theme_name") or ""),
                "summary": str(candidate.get("summary") or ""),
                "today_delta": str(candidate.get("today_delta") or ""),
                "strength": _normalize_strength(candidate.get("strength")),
                "persistence_view": str(candidate.get("persistence_view") or ""),
                "bull_case": _limit_list(candidate.get("bull_case") or [], MAX_POINTS_PER_THEME),
                "bear_case": _limit_list(candidate.get("bear_case") or [], MAX_POINTS_PER_THEME),
                "linked_boards": _limit_list(candidate.get("linked_boards") or [], MAX_LINKS_PER_THEME),
                "linked_symbols": _limit_list(candidate.get("linked_symbols") or [], MAX_LINKS_PER_THEME),
                "evidence_news_ids": _limit_list(candidate.get("evidence_news_ids") or [], MAX_LINKS_PER_THEME),
            }
        )
    return fallback


def _apply_candidate_actions(
    store: HotNewsStateStore,
    *,
    run_date: str,
    candidate_actions: Sequence[Mapping[str, Any]],
    candidate_by_id: Mapping[str, Mapping[str, Any]],
    themes_by_id: Dict[str, Dict[str, Any]],
    news_by_id: Mapping[str, Mapping[str, Any]],
    applied_operations: List[Dict[str, Any]],
) -> set[str]:
    touched_theme_ids: set[str] = set()
    run_ts = f"{run_date}T21:00:00"

    for action in candidate_actions:
        candidate_id = str(action.get("candidate_id") or "")
        candidate = candidate_by_id.get(candidate_id, {})
        op_type = str(action.get("action") or "").strip().upper()
        evidence_news_ids = _limit_list(action.get("evidence_news_ids") or candidate.get("evidence_news_ids") or [], MAX_LINKS_PER_THEME)
        evidence_items = [
            {
                "news_id": news_id,
                "title": news_by_id.get(news_id, {}).get("title"),
                "source": news_by_id.get(news_id, {}).get("source"),
                "published_at": news_by_id.get(news_id, {}).get("published_at"),
                "content": news_by_id.get(news_id, {}).get("content"),
                "metadata": {"candidate_id": candidate_id},
            }
            for news_id in evidence_news_ids
            if news_id in news_by_id
        ]

        if op_type == "DROP_CANDIDATE":
            operation = {
                "op_type": op_type,
                "candidate_id": candidate_id,
                "theme_id": None,
                "reason": str(action.get("reason") or candidate.get("drop_reason") or "").strip(),
                "payload": dict(action),
            }
            applied_operations.append(operation)
            store.append_operation(
                run_date=run_date,
                op_type=op_type,
                candidate_id=candidate_id,
                reason=operation["reason"],
                payload=operation["payload"],
            )
            continue

        target_theme_id = str(action.get("target_theme_id") or "").strip()
        if op_type == "UPDATE_THEME" and target_theme_id not in themes_by_id:
            op_type = "ADD_THEME"
            target_theme_id = ""

        theme_name = str(action.get("canonical_theme_name") or candidate.get("theme_name") or "").strip()
        theme_id = target_theme_id or _generate_theme_id(theme_name, evidence_news_ids)
        existing = themes_by_id.get(theme_id)

        merged_theme = _merge_theme_payload(
            existing,
            action=action,
            candidate=candidate,
            run_ts=run_ts,
            theme_id=theme_id,
        )
        themes_by_id[theme_id] = merged_theme
        touched_theme_ids.add(theme_id)
        store.write_theme(merged_theme)
        store.replace_theme_evidence(theme_id, run_date, evidence_items)

        operation = {
            "op_type": op_type,
            "candidate_id": candidate_id,
            "theme_id": theme_id,
            "reason": str(action.get("reason") or "").strip(),
            "payload": dict(action),
        }
        applied_operations.append(operation)
        store.append_operation(
            run_date=run_date,
            op_type=op_type,
            theme_id=theme_id,
            candidate_id=candidate_id,
            reason=operation["reason"],
            payload=operation["payload"],
        )

    return touched_theme_ids


def _plan_theme_merges(
    store: HotNewsStateStore,
    *,
    themes: Sequence[Mapping[str, Any]],
    model: str,
    embedding_model: str,
    source_status: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    working_themes = [theme for theme in themes if theme.get("status") in {"active", "cooling"}]
    if len(working_themes) < 2:
        source_status.append(
            {
                "source": "hot_news_theme_merge",
                "status": "skipped",
                "model": model,
                "reason": "insufficient_themes",
            }
        )
        return []

    candidate_pairs = _build_merge_candidate_pairs(
        store,
        themes=working_themes,
        embedding_model=embedding_model,
        source_status=source_status,
    )
    if not candidate_pairs:
        source_status.append(
            {
                "source": "hot_news_theme_merge",
                "status": "skipped",
                "model": model,
                "reason": "no_merge_pairs",
            }
        )
        return []

    request_payload = {"theme_pairs": candidate_pairs}
    try:
        client = _get_theme_llm_client(model)
        completion = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": THEME_MERGE_SYSTEM_PROMPT},
                {"role": "user", "content": json.dumps(request_payload, ensure_ascii=False, indent=2)},
                {"role": "user", "content": THEME_MERGE_USER_PROMPT},
            ],
        )
        parsed = _parse_json_object(completion.choices[0].message.content)
        merge_actions = _normalize_merge_actions(parsed, candidate_pairs)
        source_status.append(
            {
                "source": "hot_news_theme_merge",
                "status": "ok",
                "model": model,
                "pair_count": len(candidate_pairs),
                "merge_count": len(merge_actions),
            }
        )
        return merge_actions
    except Exception as exc:
        LOGGER.exception("主题 merge 规划失败，将跳过 merge: %s", exc)
        source_status.append(
            {
                "source": "hot_news_theme_merge",
                "status": "error",
                "model": model,
                "error": str(exc),
            }
        )
        return []


def _build_merge_candidate_pairs(
    store: HotNewsStateStore,
    *,
    themes: Sequence[Mapping[str, Any]],
    embedding_model: str,
    source_status: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    if len(themes) < 2:
        return []

    theme_texts = {str(theme.get("theme_id")): _build_theme_embedding_text(theme) for theme in themes}
    embeddings: Dict[str, List[float]] = {}
    try:
        embedding_client = _DashScopeEmbeddingClient(embedding_model)
        embeddings = _load_embeddings_with_cache(
            store,
            model=embedding_model,
            embedding_client=embedding_client,
            texts=theme_texts,
        )
    except Exception as exc:
        LOGGER.warning("merge pair embedding 失败，将仅用规则对: %s", exc)
        source_status.append(
            {
                "source": "dashscope_text_embedding_merge_pairs",
                "status": "error",
                "model": embedding_model,
                "error": str(exc),
            }
        )

    pairs: List[Dict[str, Any]] = []
    for left_index, left in enumerate(themes):
        for right in themes[left_index + 1 :]:
            similarity = _cosine_similarity(
                embeddings.get(str(left.get("theme_id") or "")),
                embeddings.get(str(right.get("theme_id") or "")),
            )
            rule_score = _theme_pair_rule_score(left, right)
            score = rule_score + (max(similarity, 0.0) * 0.8 if similarity is not None else 0.0)
            if score < 1.1 and (similarity is None or similarity < 0.82):
                continue
            pairs.append(
                {
                    "theme_a": _compact_theme_for_merge(left),
                    "theme_b": _compact_theme_for_merge(right),
                    "score": score,
                    "rule_score": rule_score,
                    "embedding_similarity": similarity,
                }
            )
    pairs.sort(key=lambda item: float(item.get("score") or 0.0), reverse=True)
    return pairs[:MERGE_PAIR_LIMIT]


def _normalize_merge_actions(
    payload: Mapping[str, Any],
    candidate_pairs: Sequence[Mapping[str, Any]],
) -> List[Dict[str, Any]]:
    pair_lookup = {
        frozenset(
            {
                str(pair.get("theme_a", {}).get("theme_id") or ""),
                str(pair.get("theme_b", {}).get("theme_id") or ""),
            }
        ): pair
        for pair in candidate_pairs
    }
    used_theme_ids: set[str] = set()
    normalized: List[Dict[str, Any]] = []
    for item in payload.get("merge_actions", []) if isinstance(payload.get("merge_actions"), list) else []:
        if not isinstance(item, Mapping):
            continue
        source_theme_id = str(item.get("source_theme_id") or "").strip()
        target_theme_id = str(item.get("target_theme_id") or "").strip()
        pair_key = frozenset({source_theme_id, target_theme_id})
        if (
            not source_theme_id
            or not target_theme_id
            or source_theme_id == target_theme_id
            or pair_key not in pair_lookup
            or source_theme_id in used_theme_ids
            or target_theme_id in used_theme_ids
        ):
            continue
        normalized.append(
            {
                "source_theme_id": source_theme_id,
                "target_theme_id": target_theme_id,
                "reason": str(item.get("reason") or "").strip(),
            }
        )
        used_theme_ids.add(source_theme_id)
        used_theme_ids.add(target_theme_id)
    return normalized


def _apply_merge_actions(
    store: HotNewsStateStore,
    *,
    run_date: str,
    merge_actions: Sequence[Mapping[str, Any]],
    themes_by_id: Dict[str, Dict[str, Any]],
    applied_operations: List[Dict[str, Any]],
) -> set[str]:
    touched_theme_ids: set[str] = set()
    for action in merge_actions:
        source_theme_id = str(action.get("source_theme_id") or "").strip()
        target_theme_id = str(action.get("target_theme_id") or "").strip()
        source_theme = themes_by_id.get(source_theme_id)
        target_theme = themes_by_id.get(target_theme_id)
        if source_theme is None or target_theme is None or source_theme_id == target_theme_id:
            continue

        merged_target = dict(target_theme)
        merged_target["first_seen_at"] = min(
            str(source_theme.get("first_seen_at") or ""),
            str(target_theme.get("first_seen_at") or ""),
        )
        merged_target["last_seen_at"] = max(
            str(source_theme.get("last_seen_at") or ""),
            str(target_theme.get("last_seen_at") or ""),
        )
        merged_target["status"] = "active" if "active" in {source_theme.get("status"), target_theme.get("status")} else "cooling"
        merged_target["linked_boards"] = _limit_list(
            list(target_theme.get("linked_boards") or []) + list(source_theme.get("linked_boards") or []),
            MAX_LINKS_PER_THEME,
        )
        merged_target["linked_symbols"] = _limit_list(
            list(target_theme.get("linked_symbols") or []) + list(source_theme.get("linked_symbols") or []),
            MAX_LINKS_PER_THEME,
        )
        merged_target["key_evidence_news_ids"] = _limit_list(
            list(target_theme.get("key_evidence_news_ids") or []) + list(source_theme.get("key_evidence_news_ids") or []),
            MAX_LINKS_PER_THEME,
        )
        merged_target["bull_case"] = _limit_list(
            list(target_theme.get("bull_case") or []) + list(source_theme.get("bull_case") or []),
            MAX_POINTS_PER_THEME,
        )
        merged_target["bear_case"] = _limit_list(
            list(target_theme.get("bear_case") or []) + list(source_theme.get("bear_case") or []),
            MAX_POINTS_PER_THEME,
        )
        merged_target["aliases"] = _limit_list(
            list(target_theme.get("aliases") or [])
            + list(source_theme.get("aliases") or [])
            + [source_theme.get("theme_name"), target_theme.get("theme_name")],
            MAX_LINKS_PER_THEME,
        )
        merged_target["updated_at"] = datetime.now().isoformat()

        archived_source = dict(source_theme)
        archived_source["status"] = "archived"
        archived_source["updated_at"] = datetime.now().isoformat()
        metadata = dict(archived_source.get("metadata") or {})
        metadata["merged_into"] = target_theme_id
        archived_source["metadata"] = metadata

        themes_by_id[target_theme_id] = merged_target
        themes_by_id[source_theme_id] = archived_source
        store.write_theme(merged_target)
        store.write_theme(archived_source)

        operation = {
            "op_type": "MERGE_THEME",
            "theme_id": source_theme_id,
            "target_theme_id": target_theme_id,
            "reason": str(action.get("reason") or "").strip(),
            "payload": dict(action),
        }
        applied_operations.append(operation)
        store.append_operation(
            run_date=run_date,
            op_type="MERGE_THEME",
            theme_id=source_theme_id,
            target_theme_id=target_theme_id,
            reason=operation["reason"],
            payload=operation["payload"],
        )
        touched_theme_ids.add(source_theme_id)
        touched_theme_ids.add(target_theme_id)
    return touched_theme_ids


def _apply_archive_actions(
    store: HotNewsStateStore,
    *,
    run_date: str,
    archive_actions: Sequence[Mapping[str, Any]],
    themes_by_id: Dict[str, Dict[str, Any]],
    applied_operations: List[Dict[str, Any]],
) -> set[str]:
    touched_theme_ids: set[str] = set()
    for action in archive_actions:
        theme_id = str(action.get("theme_id") or "").strip()
        theme = themes_by_id.get(theme_id)
        if theme is None or theme.get("status") == "archived":
            continue
        updated = dict(theme)
        updated["status"] = "archived"
        updated["updated_at"] = datetime.now().isoformat()
        themes_by_id[theme_id] = updated
        store.write_theme(updated)
        operation = {
            "op_type": "ARCHIVE_THEME",
            "theme_id": theme_id,
            "reason": str(action.get("reason") or "").strip(),
            "payload": dict(action),
        }
        applied_operations.append(operation)
        store.append_operation(
            run_date=run_date,
            op_type="ARCHIVE_THEME",
            theme_id=theme_id,
            reason=operation["reason"],
            payload=operation["payload"],
        )
        touched_theme_ids.add(theme_id)
    return touched_theme_ids


def _apply_automatic_aging(
    store: HotNewsStateStore,
    *,
    run_date: str,
    themes_by_id: Dict[str, Dict[str, Any]],
    touched_theme_ids: set[str],
    applied_operations: List[Dict[str, Any]],
) -> set[str]:
    aged_ids: set[str] = set()
    for theme_id, theme in list(themes_by_id.items()):
        if theme_id in touched_theme_ids:
            continue
        status = str(theme.get("status") or "")
        if status == "archived":
            continue
        days_since_seen = _days_since(str(theme.get("last_seen_at") or ""), run_date)
        if days_since_seen >= COOLING_TO_ARCHIVE_DAYS:
            new_status = "archived"
            reason = f"auto_archive_after_{days_since_seen}d"
        elif status == "active" and days_since_seen >= ACTIVE_TO_COOLING_DAYS:
            new_status = "cooling"
            reason = f"auto_cooling_after_{days_since_seen}d"
        else:
            continue

        updated = dict(theme)
        updated["status"] = new_status
        updated["updated_at"] = datetime.now().isoformat()
        themes_by_id[theme_id] = updated
        store.write_theme(updated)
        operation = {
            "op_type": "ARCHIVE_THEME" if new_status == "archived" else "COOL_THEME",
            "theme_id": theme_id,
            "reason": reason,
            "payload": {"days_since_seen": days_since_seen},
        }
        applied_operations.append(operation)
        store.append_operation(
            run_date=run_date,
            op_type=operation["op_type"],
            theme_id=theme_id,
            reason=reason,
            payload=operation["payload"],
        )
        aged_ids.add(theme_id)
    return aged_ids


def _build_state_payload(
    *,
    run_date: str,
    model: str,
    embedding_model: str,
    themes: Sequence[Mapping[str, Any]],
    all_themes: Sequence[Mapping[str, Any]],
    candidates: Sequence[Mapping[str, Any]],
    operations: Sequence[Mapping[str, Any]],
    market_regime_note: str,
    source_status: Sequence[Mapping[str, Any]],
) -> Dict[str, Any]:
    op_counts = Counter(str(item.get("op_type") or "") for item in operations)
    status_counts = Counter(str(item.get("status") or "") for item in all_themes)
    output_themes = [_theme_to_output(item, run_date=run_date) for item in themes]
    output_themes.sort(
        key=lambda item: (
            0 if item.get("status") == "active" else 1,
            str(item.get("last_seen_at") or ""),
        ),
        reverse=False,
    )
    archived_themes = [
        _theme_to_output(item, run_date=run_date)
        for item in all_themes
        if str(item.get("status") or "") == "archived"
    ]
    new_themes = [
        item
        for item in output_themes
        if str(item.get("first_seen_at") or "").strip()[:10] == str(run_date).strip()[:10]
    ]
    active_themes = [item for item in output_themes if str(item.get("status") or "") == "active"]
    cooling_themes = [item for item in output_themes if str(item.get("status") or "") == "cooling"]
    return {
        "schema_version": 1,
        "run_date": run_date,
        "updated_at": datetime.now().isoformat(),
        "model": model,
        "embedding_model": embedding_model,
        "market_regime_note": market_regime_note,
        "market_regime_bridge": market_regime_note,
        "summary": {
            "theme_count": len(output_themes),
            "active_count": int(status_counts.get("active", 0)),
            "cooling_count": int(status_counts.get("cooling", 0)),
            "archived_count": int(status_counts.get("archived", 0)),
            "candidate_count": len(candidates),
            "added_count": int(op_counts.get("ADD_THEME", 0)),
            "updated_count": int(op_counts.get("UPDATE_THEME", 0)),
            "merged_count": int(op_counts.get("MERGE_THEME", 0)),
            "archived_today_count": int(op_counts.get("ARCHIVE_THEME", 0)),
            "dropped_count": int(op_counts.get("DROP_CANDIDATE", 0)),
        },
        "active_themes": active_themes,
        "cooling_themes": cooling_themes,
        "new_themes": new_themes,
        "archived_themes": archived_themes,
        "universe_expansion_hints": [],
        "themes": output_themes,
        "source_status": list(source_status),
    }


def _theme_to_output(theme: Mapping[str, Any], *, run_date: str) -> Dict[str, Any]:
    theme_name = theme.get("theme_name")
    summary = str(theme.get("summary") or "").strip()
    today_delta = str(theme.get("today_delta") or "").strip()
    persistence_view = str(theme.get("persistence_view") or "").strip()
    strength = theme.get("strength")
    linked_boards = _limit_list(theme.get("linked_boards") or [], MAX_LINKS_PER_THEME)
    linked_symbols = _limit_list(theme.get("linked_symbols") or [], MAX_LINKS_PER_THEME)
    evidence_news_ids = _limit_list(theme.get("key_evidence_news_ids") or [], MAX_LINKS_PER_THEME)
    bear_case = _limit_list(theme.get("bear_case") or [], MAX_POINTS_PER_THEME)
    days_running = _days_since(str(theme.get("first_seen_at") or ""), run_date)
    already_running_for = f"已持续{days_running}个交易日" if days_running < 9999 else "持续时间不确定"
    forward_paths: List[str] = []
    if persistence_view:
        forward_paths.append(persistence_view)
    if strength == "strengthening":
        forward_paths.append("若新增证据继续累积且板块强度同步确认，主题可能继续强化。")
        forward_paths.append("若资金未继续确认或新催化缺位，主题可能从强化转向分化。")
    elif strength == "fading":
        forward_paths.append("若缺少新增验证，主题大概率继续衰减并逐步退出主上下文。")
        forward_paths.append("若出现新的政策、价格或事件催化，主题可能再次回到观察范围。")
    else:
        forward_paths.append("若新的高质量新闻继续补强，主题可继续保留在主上下文。")
        forward_paths.append("若相关板块和风险偏好未跟进，主题可能只停留在新闻层。")
    scenario_tree = _build_default_scenario_tree(theme, forward_paths)
    key_risks = _build_default_key_risks(theme, bear_case)
    next_day_watchlist = _build_default_watchlist(theme, linked_boards, linked_symbols, evidence_news_ids)
    return {
        "theme_id": theme.get("theme_id"),
        "theme_name": theme_name,
        "status": theme.get("status"),
        "first_seen_at": theme.get("first_seen_at"),
        "last_seen_at": theme.get("last_seen_at"),
        "summary": theme.get("summary"),
        "today_delta": theme.get("today_delta"),
        "strength": theme.get("strength"),
        "persistence_view": theme.get("persistence_view"),
        "bull_case": _limit_list(theme.get("bull_case") or [], MAX_POINTS_PER_THEME),
        "bear_case": bear_case,
        "key_evidence_news_ids": evidence_news_ids,
        "linked_boards": linked_boards,
        "linked_symbols": linked_symbols,
        "aliases": _limit_list(theme.get("aliases") or [], MAX_LINKS_PER_THEME),
        "history_anchor": summary or str(theme_name or ""),
        "today_update": today_delta or summary or str(theme_name or ""),
        "current_state": summary or today_delta or str(theme_name or ""),
        "expected_duration": {
            "already_running_for": already_running_for,
            "base_case": persistence_view or "未来持续时间仍需结合新增证据与板块确认继续判断。",
            "decay_signals": bear_case or ["若后续缺少新增验证，主题可能逐步降温。"],
        },
        "forward_paths": forward_paths,
        "scenario_tree": scenario_tree,
        "key_risks": key_risks,
        "why_it_matters": summary or today_delta or str(theme_name or ""),
        "linked_macro_topics": [],
        "linked_symbols_in_universe": linked_symbols,
        "outside_universe_names_to_check": [],
        "search_trigger": "若相关板块继续强化且宇宙内缺少合适标的，应搜索宇宙外龙头或弹性股。",
        "evidence_news_ids": evidence_news_ids,
        "key_events": [],
        "next_day_watchlist": next_day_watchlist,
    }


def _build_default_scenario_tree(theme: Mapping[str, Any], forward_paths: Sequence[str]) -> List[Dict[str, Any]]:
    theme_name = str(theme.get("theme_name") or "").strip() or "当前主题"
    paths = list(forward_paths)
    defaults = [
        {
            "scenario": f"{theme_name}继续强化",
            "probability_band": "medium",
            "trigger_signals": ["新增高质量新闻继续出现", "相关板块得到资金确认"],
            "market_impact": paths[0] if paths else "主题继续强化并扩大影响范围。",
        },
        {
            "scenario": f"{theme_name}高位钝化或分化",
            "probability_band": "medium",
            "trigger_signals": ["新增催化减少", "板块内部开始分化"],
            "market_impact": paths[1] if len(paths) > 1 else "主题仍在，但交易重心可能缩窄到少数代表方向。",
        },
        {
            "scenario": f"{theme_name}明显降温或反转",
            "probability_band": "low_to_medium",
            "trigger_signals": ["关键催化被证伪", "风险偏好转向", "市场主线切换"],
            "market_impact": "相关板块与股票的主题溢价可能快速回吐。",
        },
    ]
    return defaults


def _build_default_key_risks(theme: Mapping[str, Any], bear_case: Sequence[str]) -> List[Dict[str, Any]]:
    risks = [
        {
            "risk": risk,
            "probability_band": "medium",
            "why_it_matters": "若该风险兑现，主题可能弱化、分化或提前反转。",
        }
        for risk in bear_case
    ]
    if risks:
        return risks
    theme_name = str(theme.get("theme_name") or "").strip() or "当前主题"
    return [
        {
            "risk": f"{theme_name}缺少新增高质量验证",
            "probability_band": "medium",
            "why_it_matters": "若后续缺少新增验证，主题热度可能自然衰减。",
        }
    ]


def _build_default_watchlist(
    theme: Mapping[str, Any],
    linked_boards: Sequence[str],
    linked_symbols: Sequence[str],
    evidence_news_ids: Sequence[str],
) -> List[str]:
    theme_name = str(theme.get("theme_name") or "").strip() or "当前主题"
    watchlist = [f"继续跟踪 {theme_name} 是否出现新的高质量证据新闻。"] if evidence_news_ids else []
    if linked_boards:
        watchlist.append(f"观察相关板块是否继续维持强度：{', '.join(linked_boards[:3])}。")
    if linked_symbols:
        watchlist.append(f"观察相关股票是否继续得到市场确认：{', '.join(linked_symbols[:3])}。")
    if not watchlist:
        watchlist.append(f"继续跟踪 {theme_name} 的增量事件与市场确认信号。")
    return watchlist


def _merge_theme_payload(
    existing: Mapping[str, Any] | None,
    *,
    action: Mapping[str, Any],
    candidate: Mapping[str, Any],
    run_ts: str,
    theme_id: str,
) -> Dict[str, Any]:
    base = dict(existing or {})
    old_theme_name = str(base.get("theme_name") or "")
    theme_name = str(action.get("canonical_theme_name") or candidate.get("theme_name") or old_theme_name).strip()
    aliases = _limit_list(
        list(base.get("aliases") or [])
        + [old_theme_name, theme_name, candidate.get("theme_name")],
        MAX_LINKS_PER_THEME,
    )
    return {
        "theme_id": theme_id,
        "theme_name": theme_name,
        "status": "active",
        "first_seen_at": str(base.get("first_seen_at") or run_ts),
        "last_seen_at": run_ts,
        "created_at": str(base.get("created_at") or datetime.now().isoformat()),
        "updated_at": datetime.now().isoformat(),
        "summary": str(action.get("summary") or base.get("summary") or candidate.get("summary") or theme_name).strip(),
        "today_delta": str(action.get("today_delta") or candidate.get("today_delta") or "").strip(),
        "strength": _normalize_strength(action.get("strength") or base.get("strength") or candidate.get("strength")),
        "persistence_view": str(action.get("persistence_view") or base.get("persistence_view") or candidate.get("persistence_view") or "").strip(),
        "bull_case": _limit_list(list(base.get("bull_case") or []) + list(action.get("bull_case") or []), MAX_POINTS_PER_THEME),
        "bear_case": _limit_list(list(base.get("bear_case") or []) + list(action.get("bear_case") or []), MAX_POINTS_PER_THEME),
        "linked_boards": _limit_list(list(base.get("linked_boards") or []) + list(action.get("linked_boards") or candidate.get("linked_boards") or []), MAX_LINKS_PER_THEME),
        "linked_symbols": _limit_list(list(base.get("linked_symbols") or []) + list(action.get("linked_symbols") or candidate.get("linked_symbols") or []), MAX_LINKS_PER_THEME),
        "key_evidence_news_ids": _limit_list(list(base.get("key_evidence_news_ids") or []) + list(action.get("evidence_news_ids") or candidate.get("evidence_news_ids") or []), MAX_LINKS_PER_THEME),
        "aliases": aliases,
        "metadata": {
            **dict(base.get("metadata") or {}),
            "last_candidate_id": action.get("candidate_id"),
            "last_reason": action.get("reason"),
        },
    }


def _rule_match_score(candidate: Mapping[str, Any], theme: Mapping[str, Any]) -> float:
    score = 0.0
    candidate_name = str(candidate.get("theme_name") or "").strip()
    theme_name = str(theme.get("theme_name") or "").strip()
    aliases = {str(value).strip() for value in list(theme.get("aliases") or []) if str(value).strip()}
    if candidate_name and theme_name:
        if candidate_name == theme_name or candidate_name in aliases:
            score += 1.0
        elif candidate_name in theme_name or theme_name in candidate_name:
            score += 0.65
        score += min(_char_bigram_jaccard(candidate_name, " ".join(sorted(aliases | {theme_name}))) * 0.6, 0.35)

    board_overlap = set(_limit_list(candidate.get("linked_boards") or [], MAX_LINKS_PER_THEME)) & set(
        _limit_list(theme.get("linked_boards") or [], MAX_LINKS_PER_THEME)
    )
    symbol_overlap = set(_limit_list(candidate.get("linked_symbols") or [], MAX_LINKS_PER_THEME)) & set(
        _limit_list(theme.get("linked_symbols") or [], MAX_LINKS_PER_THEME)
    )
    score += min(len(board_overlap) * 0.22, 0.44)
    score += min(len(symbol_overlap) * 0.28, 0.56)
    return score


def _theme_pair_rule_score(left: Mapping[str, Any], right: Mapping[str, Any]) -> float:
    left_name = str(left.get("theme_name") or "").strip()
    right_name = str(right.get("theme_name") or "").strip()
    left_aliases = {str(value).strip() for value in list(left.get("aliases") or []) if str(value).strip()}
    right_aliases = {str(value).strip() for value in list(right.get("aliases") or []) if str(value).strip()}
    score = 0.0
    if left_name and right_name:
        if left_name == right_name or left_name in right_aliases or right_name in left_aliases:
            score += 1.0
        elif left_name in right_name or right_name in left_name:
            score += 0.5
        score += min(_char_bigram_jaccard(left_name, right_name) * 0.55, 0.35)

    board_overlap = set(left.get("linked_boards") or []) & set(right.get("linked_boards") or [])
    symbol_overlap = set(left.get("linked_symbols") or []) & set(right.get("linked_symbols") or [])
    score += min(len(board_overlap) * 0.18, 0.36)
    score += min(len(symbol_overlap) * 0.22, 0.44)
    return score


def _should_keep_match(
    theme: Mapping[str, Any],
    *,
    rule_score: float,
    similarity: float | None,
    combined_score: float,
) -> bool:
    status = str(theme.get("status") or "")
    if status == "archived":
        return combined_score >= 1.05 or (similarity is not None and similarity >= 0.84)
    return combined_score >= 0.72 or (similarity is not None and similarity >= 0.8)


def _compact_theme_for_merge(theme: Mapping[str, Any]) -> Dict[str, Any]:
    return {
        "theme_id": theme.get("theme_id"),
        "theme_name": theme.get("theme_name"),
        "status": theme.get("status"),
        "summary": _trim_text(theme.get("summary"), 100),
        "linked_boards": _limit_list(theme.get("linked_boards") or [], MAX_LINKS_PER_THEME),
        "linked_symbols": _limit_list(theme.get("linked_symbols") or [], MAX_LINKS_PER_THEME),
        "aliases": _limit_list(theme.get("aliases") or [], MAX_LINKS_PER_THEME),
    }


def _build_candidate_embedding_text(candidate: Mapping[str, Any]) -> str:
    return "\n".join(
        part
        for part in (
            str(candidate.get("theme_name") or "").strip(),
            str(candidate.get("summary") or "").strip(),
            "板块:" + ",".join(_limit_list(candidate.get("linked_boards") or [], MAX_LINKS_PER_THEME)),
            "个股:" + ",".join(_limit_list(candidate.get("linked_symbols") or [], MAX_LINKS_PER_THEME)),
            "证据:" + ",".join(_limit_list(candidate.get("evidence_news_ids") or [], MAX_LINKS_PER_THEME)),
        )
        if part and part not in {"板块:", "个股:", "证据:"}
    )


def _build_theme_embedding_text(theme: Mapping[str, Any]) -> str:
    return "\n".join(
        part
        for part in (
            str(theme.get("theme_name") or "").strip(),
            str(theme.get("summary") or "").strip(),
            "别名:" + ",".join(_limit_list(theme.get("aliases") or [], MAX_LINKS_PER_THEME)),
            "板块:" + ",".join(_limit_list(theme.get("linked_boards") or [], MAX_LINKS_PER_THEME)),
            "个股:" + ",".join(_limit_list(theme.get("linked_symbols") or [], MAX_LINKS_PER_THEME)),
        )
        if part and part not in {"别名:", "板块:", "个股:"}
    )


def _generate_theme_id(theme_name: str, evidence_news_ids: Sequence[str]) -> str:
    normalized_name = str(theme_name or "").strip()
    seed = normalized_name or "::".join(_limit_list(evidence_news_ids, MAX_LINKS_PER_THEME))
    return f"theme::{sha1(seed.encode('utf-8')).hexdigest()[:12]}"


def _normalize_strength(value: Any) -> str:
    text = str(value or "").strip().lower()
    if text in VALID_STRENGTHS:
        return text
    mapping = {
        "新出现": "emergence",
        "启动": "emergence",
        "增强": "strengthening",
        "强化": "strengthening",
        "成熟": "mature",
        "高位": "mature",
        "衰减": "fading",
        "退潮": "fading",
    }
    return mapping.get(text, "emergence")


def _limit_list(values: Sequence[Any], limit: int) -> List[str]:
    seen: set[str] = set()
    normalized: List[str] = []
    for value in values:
        text = str(value or "").strip()
        if not text or text in seen:
            continue
        seen.add(text)
        normalized.append(text)
        if len(normalized) >= max(int(limit), 0):
            break
    return normalized


def _trim_text(value: Any, limit: int) -> str:
    text = re.sub(r"\s+", " ", str(value or "")).strip()
    if len(text) <= limit:
        return text
    return text[: max(limit - 3, 0)] + "..."


def _fallback_theme_name(title: str) -> str:
    parts = re.split(r"[：:，,。；;、\\s]+", str(title or "").strip())
    candidate = next((part for part in parts if len(part) >= 2), str(title or "").strip())
    return _trim_text(candidate, 20) or "市场主题"


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
        raise ValueError(f"模型返回中未找到 JSON 对象: {content[:500]}")
    parsed = json.loads(content[start : end + 1])
    if not isinstance(parsed, dict):
        raise ValueError("模型返回不是 JSON 对象")
    return parsed


def _char_bigram_jaccard(left: str, right: str) -> float:
    def _bigrams(text: str) -> set[str]:
        normalized = re.sub(r"\s+", "", text)
        if len(normalized) < 2:
            return {normalized} if normalized else set()
        return {normalized[index : index + 2] for index in range(len(normalized) - 1)}

    left_set = _bigrams(left)
    right_set = _bigrams(right)
    if not left_set or not right_set:
        return 0.0
    union = left_set | right_set
    if not union:
        return 0.0
    return len(left_set & right_set) / len(union)


def _cosine_similarity(left: Sequence[float] | None, right: Sequence[float] | None) -> float | None:
    if not left or not right or len(left) != len(right):
        return None
    numerator = sum(float(a) * float(b) for a, b in zip(left, right))
    left_norm = math.sqrt(sum(float(a) * float(a) for a in left))
    right_norm = math.sqrt(sum(float(b) * float(b) for b in right))
    if left_norm <= 0 or right_norm <= 0:
        return None
    return numerator / (left_norm * right_norm)


def _text_hash(text: str) -> str:
    return sha1(text.encode("utf-8")).hexdigest()


def _days_since(timestamp: str, run_date: str) -> int:
    candidate = str(timestamp or "").strip()
    if not candidate:
        return 9999
    base_day = candidate[:10]
    try:
        left = datetime.strptime(base_day, "%Y-%m-%d").date()
        right = datetime.strptime(run_date[:10], "%Y-%m-%d").date()
    except ValueError:
        return 9999
    return max((right - left).days, 0)


def _append_manifest(
    manifest_path: Path,
    *,
    run_date: str,
    path: Path,
    theme_count: int,
    source_status: Sequence[Mapping[str, Any]],
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
            "theme_count": int(theme_count),
            "source_error_count": sum(
                1 for item in source_status if str(item.get("status") or "").lower() == "error"
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
