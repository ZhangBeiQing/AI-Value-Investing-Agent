"""News acquisition, deduplication and enrichment pipeline for the selection system."""

from __future__ import annotations

import hashlib
import json
import os
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Sequence, Tuple

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from openai import OpenAI

from core.logging import get_logger

from .breakfast_enrichment import fetch_breakfast_article_payload
from .paths import SelectionSystemPaths
from .store import save_json_file


LOGGER = get_logger("SelectionNewsCuration")
load_dotenv(".env")

DEFAULT_DEDUP_MODEL = "deepseek-v3.2-exp"
DEFAULT_BATCH_SIZE = 20
SOURCE_BATCH_LIMITS = {
    "em_breakfast": 80,
    "cls_key": 50,
    "ths_global": 240,
    "futu_global": 200,
}
SOURCE_ID_PREFIX = {
    "em_breakfast": "B",
    "cls_key": "C",
    "ths_global": "T",
    "futu_global": "F",
}
THS_MAX_PAGES = 40
FUTU_MAX_PAGES = 24
MERGE_LLM_MAX_ITEMS = 80
DEDUPE_SYSTEM_PROMPT = """你是一个严谨的财经新闻去重与筛噪助手。

你的主要任务不是改写新闻，也不是总结新闻，而是对输入的候选新闻做判决：
1. 识别同一市场事件的重复播报
2. 标记明显噪声、低价值、对市场参考意义弱的新闻
3. 尽量保留当天对A股、港股、美股中概、宏观、行业、商品、汇率、政策有参考价值的重要新闻

非常重要的规则：
1. 每条新闻都有唯一的 news_id，你绝对不能改写、重命名、删除、虚构 news_id
2. 你不能生成新的新闻对象
3. 你只能对现有 news_id 做判定
4. “重复”指同一市场事件被不同来源重复播报，不是仅仅标题相似
5. 如果两条新闻主题相近但事件不同，不能判成重复
6. 优先保留信息更完整、来源更权威、表述更清晰、时间更早的一条作为 canonical_id
7. 你不能输出 markdown，不能输出解释性正文，只能输出严格 JSON
8. 本任务禁止使用外部搜索或联网补充信息，你只能基于输入的候选新闻进行判断

保留标准：
1. 政策、监管、宏观、地缘、商品、汇率、利率、行业催化、重要公司事件
2. 对市场主线、板块轮动、风险偏好有影响的新闻
3. 对后续热点状态判断有参考价值的新闻

应标记为噪声的内容包括但不限于：
1. 明显重复播报
2. 信息量极低、没有新增事实的短句
3. 对市场整体参考意义很弱的零碎消息
4. 与中国市场、港股、中概、全球风险偏好几乎无关的边缘消息
5. 单纯价格波动描述但没有事件驱动的信息

额外强调：
1. 默认保守保留，不要因为“单一公司新闻”就轻易剔除。
2. 只有在你非常确信该新闻几乎没有研究价值时，才把它放进 noise_items。
3. 你的首要任务是去重，不是大幅裁剪新闻总量。

输出 JSON 格式如下：
{
  "keep_ids": ["news_id_1", "news_id_2"],
  "duplicate_groups": [
    {
      "canonical_id": "news_id_a",
      "duplicate_ids": ["news_id_b", "news_id_c"],
      "reason": "同一事件的多源重复播报"
    }
  ],
  "noise_items": [
    {
      "news_id": "news_id_x",
      "reason": "信息密度过低，对市场参考价值弱"
    }
  ]
}

输出要求：
1. keep_ids 中包含所有应保留的 canonical 新闻 id
2. duplicate_ids 和 noise_items 中出现的 id，不应再重复放入 keep_ids
3. 所有 id 必须来自输入
4. reason 要简短明确
5. 只输出 JSON，不要输出任何其他文字
"""


def run_news_curation_pipeline(
    run_date: str,
    *,
    base_dir: str | Path = "data",
    model: str = DEFAULT_DEDUP_MODEL,
    batch_size: int = DEFAULT_BATCH_SIZE,
    max_items_per_source: Dict[str, int] | None = None,
) -> Dict[str, Path]:
    paths = SelectionSystemPaths.from_base_dir(base_dir)
    paths.ensure_directories()
    paths.ensure_run_dir(run_date)

    LOGGER.info("开始新闻模块: run_date=%s model=%s batch_size=%d", run_date, model, batch_size)
    candidates, candidate_source_status = collect_news_candidates(
        run_date,
        max_items_per_source=max_items_per_source,
    )
    save_json_file(
        paths.run_news_candidates_path(run_date),
        _build_payload(
            run_date,
            candidates,
            source_status=candidate_source_status,
            summary={
                "candidate_count": len(candidates),
                "source_count": len(candidate_source_status),
            },
        ),
    )
    LOGGER.info("候选新闻已写入: %s", paths.run_news_candidates_path(run_date))

    decision_payload = dedupe_news_candidates(
        candidates,
        model=model,
        batch_size=batch_size,
    )
    save_json_file(paths.run_news_dedup_decisions_path(run_date), decision_payload)
    LOGGER.info("去重判决已写入: %s", paths.run_news_dedup_decisions_path(run_date))

    deduped_items = [item for item in candidates if item["news_id"] in set(decision_payload["final_keep_ids"])]
    save_json_file(
        paths.run_news_deduped_path(run_date),
        _build_payload(
            run_date,
            deduped_items,
            source_status=[
                *candidate_source_status,
                {
                    "source": "llm_dedupe",
                    "status": "ok",
                    "model": model,
                    "batch_count": len(decision_payload.get("batch_results", [])),
                    "kept_count": len(deduped_items),
                    "flagged_noise_count": len(decision_payload.get("noise_items", [])),
                },
            ],
            summary={
                "candidate_count": len(candidates),
                "deduped_count": len(deduped_items),
                "flagged_noise_count": len(decision_payload.get("noise_items", [])),
            },
        ),
    )
    LOGGER.info("去重结果已写入: %s", paths.run_news_deduped_path(run_date))

    enriched_items, enrichment_status = enrich_news_items(deduped_items)
    save_json_file(
        paths.run_news_enriched_path(run_date),
        _build_payload(
            run_date,
            enriched_items,
            source_status=[*candidate_source_status, *enrichment_status],
            summary={
                "candidate_count": len(candidates),
                "deduped_count": len(deduped_items),
                "enriched_count": len(enriched_items),
                "fetch_error_count": sum(
                    1
                    for item in enriched_items
                    if str(item.get("content_fetch_status") or "").lower() == "fetch_failed"
                ),
            },
        ),
    )
    LOGGER.info("增强正文已写入: %s", paths.run_news_enriched_path(run_date))

    LOGGER.info(
        "新闻模块完成: candidates=%d deduped=%d enriched=%d",
        len(candidates),
        len(deduped_items),
        len(enriched_items),
    )
    return {
        "candidates": paths.run_news_candidates_path(run_date),
        "decisions": paths.run_news_dedup_decisions_path(run_date),
        "deduped": paths.run_news_deduped_path(run_date),
        "enriched": paths.run_news_enriched_path(run_date),
    }


def collect_news_candidates(
    run_date: str,
    *,
    recent_hours: int = 24,
    max_items_per_source: Dict[str, int] | None = None,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    run_day = str(run_date).strip()[:10]
    run_end = datetime.strptime(run_day, "%Y-%m-%d") + timedelta(days=1) - timedelta(seconds=1)
    window_start = run_end - timedelta(hours=max(int(recent_hours), 1))
    source_limits = dict(SOURCE_BATCH_LIMITS)
    if max_items_per_source:
        source_limits.update(max_items_per_source)

    sources = [
        ("em_breakfast", lambda: _fetch_cjzc_rows(window_start, run_end)),
        ("cls_key", lambda: _fetch_cls_key_rows(window_start, run_end)),
        ("ths_global", lambda: _fetch_ths_rows(window_start, run_end)),
        ("futu_global", lambda: _fetch_futu_rows(window_start, run_end)),
    ]

    collected: List[Dict[str, Any]] = []
    source_status: List[Dict[str, Any]] = []
    for source_name, loader in sources:
        try:
            rows, status = loader()
        except Exception as exc:
            LOGGER.warning("候选新闻源拉取失败: source=%s error=%s", source_name, exc)
            source_status.append(
                {
                    "source": source_name,
                    "status": "error",
                    "error": str(exc),
                }
            )
            continue
        if not rows:
            source_status.append(
                {
                    "source": source_name,
                    "status": "warning",
                    "rows": 0,
                    **status,
                }
            )
            continue
        limit = max(int(source_limits.get(source_name, len(rows))), 0)
        kept_rows = rows[:limit] if limit else []
        appended_count = 0
        for index, row in enumerate(kept_rows, start=1):
            title = str(row.get("title") or "").strip()
            if not title:
                continue
            appended_count += 1
            collected.append(
                {
                    "news_id": f"{SOURCE_ID_PREFIX.get(source_name, 'N')}{index:03d}",
                    "title": title,
                    "published_at": str(row.get("published_at") or ""),
                    "source": source_name,
                    "preview": str(row.get("preview") or title).strip(),
                    "url": str(row.get("url") or ""),
                    "needs_fetch": bool(row.get("needs_fetch")),
                }
            )
        source_status.append(
            {
                "source": source_name,
                "status": "ok",
                "rows": appended_count,
                "raw_rows": len(rows),
                "truncated": len(rows) > len(kept_rows),
                **status,
            }
        )
    for index, item in enumerate(collected, start=1):
        item["llm_id"] = f"N{index:03d}"
    LOGGER.info("新闻候选采集完成: total=%d", len(collected))
    return collected, source_status


def dedupe_news_candidates(
    candidates: Sequence[Dict[str, Any]],
    *,
    model: str = DEFAULT_DEDUP_MODEL,
    batch_size: int = DEFAULT_BATCH_SIZE,
) -> Dict[str, Any]:
    client = _get_dedup_client(model)
    batches = _chunk_list(list(candidates), batch_size)
    batch_results: List[Dict[str, Any]] = []
    survivor_ids: List[str] = []
    llm_to_news = {str(item.get("llm_id") or ""): str(item.get("news_id") or "") for item in candidates}

    LOGGER.info("开始新闻去重: candidates=%d batches=%d", len(candidates), len(batches))

    for batch_index, batch in enumerate(batches, start=1):
        LOGGER.info("去重批次开始: batch=%d size=%d", batch_index, len(batch))
        result = _dedupe_batch(client, batch, model=model)
        normalized_llm = _normalize_dedup_result([item["llm_id"] for item in batch], result)
        normalized = _convert_dedup_result_to_news_ids(normalized_llm, llm_to_news)
        batch_results.append(
            {
                "batch_index": batch_index,
                "input_items": [
                    {"llm_id": item["llm_id"], "news_id": item["news_id"], "title": item["title"]}
                    for item in batch
                ],
                "result": normalized,
            }
        )
        survivor_ids.extend(normalized["keep_ids"])
        LOGGER.info("去重批次完成: batch=%d keep=%d", batch_index, len(normalized["keep_ids"]))

    survivor_items = [item for item in candidates if item["news_id"] in set(survivor_ids)]
    merge_result: Dict[str, Any] | None = None
    final_keep_ids = survivor_ids
    if len(survivor_items) > 1:
        LOGGER.info("开始跨批合并去重: survivors=%d", len(survivor_items))
        merge_result = _rule_merge_survivors(survivor_items)
        merged_survivor_items = [item for item in survivor_items if item["news_id"] in set(merge_result["keep_ids"])]
        if len(merged_survivor_items) <= MERGE_LLM_MAX_ITEMS:
            merge_raw = _dedupe_batch(client, merged_survivor_items, model=model)
            merge_llm = _normalize_dedup_result([item["llm_id"] for item in merged_survivor_items], merge_raw)
            llm_merge_result = _convert_dedup_result_to_news_ids(merge_llm, llm_to_news)
            merge_result = {
                "strategy": "rule_then_llm",
                "input_count": len(survivor_items),
                "rule_keep_count": len(merged_survivor_items),
                "keep_ids": llm_merge_result["keep_ids"],
                "duplicate_groups": merge_result.get("duplicate_groups", []) + llm_merge_result.get("duplicate_groups", []),
                "noise_items": llm_merge_result.get("noise_items", []),
            }
        else:
            LOGGER.warning(
                "跨批幸存新闻过多，跳过大模型全量合并: survivors=%d rule_keep=%d limit=%d",
                len(survivor_items),
                len(merged_survivor_items),
                MERGE_LLM_MAX_ITEMS,
            )
            merge_result = {
                "strategy": "rule_only",
                "input_count": len(survivor_items),
                "rule_keep_count": len(merged_survivor_items),
                **merge_result,
            }
        final_keep_ids = merge_result["keep_ids"]
        LOGGER.info("跨批合并完成: final_keep=%d strategy=%s", len(final_keep_ids), merge_result.get("strategy"))

    final_keep_ids = _sort_keep_ids(candidates, final_keep_ids)
    duplicate_groups = [group for batch in batch_results for group in batch.get("result", {}).get("duplicate_groups", [])]
    noise_items = [item for batch in batch_results for item in batch.get("result", {}).get("noise_items", [])]
    if isinstance(merge_result, dict):
        duplicate_groups.extend(merge_result.get("duplicate_groups", []))
        noise_items.extend(merge_result.get("noise_items", []))
    return {
        "schema_version": 1,
        "updated_at": datetime.now().isoformat(),
        "model": model,
        "batch_size": batch_size,
        "batch_results": batch_results,
        "merge_result": merge_result,
        "final_keep_ids": final_keep_ids,
        "duplicate_groups": duplicate_groups,
        "noise_items": noise_items,
        "summary": {
            "input_count": len(candidates),
            "survivor_count": len(final_keep_ids),
            "duplicate_group_count": len(duplicate_groups),
            "flagged_noise_count": len(noise_items),
        },
    }


def enrich_news_items(
    items: Sequence[Dict[str, Any]],
    *,
    max_workers: int = 6,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    LOGGER.info("开始抓取新闻正文: items=%d workers=%d", len(items), max_workers)
    enriched_by_id: Dict[str, Dict[str, Any]] = {}
    with ThreadPoolExecutor(max_workers=max(1, max_workers)) as executor:
        future_map = {executor.submit(_enrich_single_item, item): item["news_id"] for item in items}
        for future in as_completed(future_map):
            news_id = future_map[future]
            enriched_by_id[news_id] = future.result()
    result = [enriched_by_id[item["news_id"]] for item in items if item["news_id"] in enriched_by_id]
    LOGGER.info("新闻正文抓取完成: enriched=%d", len(result))
    status = [
        {
            "source": "news_enrichment",
            "status": "ok",
            "rows": len(result),
            "fetched_count": sum(
                1
                for item in result
                if str(item.get("content_fetch_status") or "").lower() == "fetched_html"
            ),
            "preview_only_count": sum(
                1
                for item in result
                if str(item.get("content_fetch_status") or "").lower() == "preview_only"
            ),
            "fetch_failed_count": sum(
                1
                for item in result
                if str(item.get("content_fetch_status") or "").lower() == "fetch_failed"
            ),
        }
    ]
    return result, status


def _enrich_single_item(item: Dict[str, Any]) -> Dict[str, Any]:
    enrichment = _build_enriched_content(item)
    return {
        "news_id": item["news_id"],
        "title": item["title"],
        "published_at": item["published_at"],
        "source": item["source"],
        "content": enrichment["content"],
        "content_fetch_status": enrichment["fetch_status"],
        "fetch_error": enrichment.get("fetch_error", ""),
        "url": item.get("url", ""),
    }


def _build_enriched_content(item: Dict[str, Any]) -> Dict[str, str]:
    source = str(item.get("source") or "")
    title = str(item.get("title") or "")
    preview = str(item.get("preview") or title)
    url = str(item.get("url") or "")

    if not item.get("needs_fetch") or not url:
        return {
            "content": preview,
            "fetch_status": "preview_only",
            "fetch_error": "",
        }

    try:
        html = _download_html(url)
        source_text = _extract_article_text(source, html)
        if source_text:
            if preview and preview not in source_text and len(source_text) > len(preview):
                return {
                    "content": source_text,
                    "fetch_status": "fetched_html",
                    "fetch_error": "",
                }
            return {
                "content": source_text or preview,
                "fetch_status": "fetched_html",
                "fetch_error": "",
            }
    except Exception as exc:
        LOGGER.warning("新闻正文抓取失败: source=%s title=%s error=%s", source, title, exc)
        return {
            "content": preview,
            "fetch_status": "fetch_failed",
            "fetch_error": str(exc),
        }
    return {
        "content": preview,
        "fetch_status": "preview_only",
        "fetch_error": "",
    }


def _extract_article_text(source: str, html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    if source == "ths_global":
        return _extract_text_from_html(soup, prefer_meta=False)
    if source == "futu_global":
        return _extract_text_from_html(soup, prefer_meta=True)
    return _extract_text_from_html(soup, prefer_meta=False)


def _extract_text_from_html(soup: BeautifulSoup, *, prefer_meta: bool) -> str:
    meta_description = _extract_meta_description(soup)
    candidates: List[str] = []
    selectors = [
        "[class*=content]",
        "[class*=article]",
        "[class*=detail]",
        "[id*=content]",
        "article",
        "main",
    ]
    for selector in selectors:
        for node in soup.select(selector):
            text = _normalize_space(node.get_text("\n", strip=True))
            if len(text) >= 120:
                candidates.append(text)
    best = max(candidates, key=len, default="")
    if prefer_meta and meta_description:
        return best if len(best) >= len(meta_description) + 40 else meta_description
    if best:
        return best
    return meta_description


def _extract_meta_description(soup: BeautifulSoup) -> str:
    for key in ("description", "og:description"):
        tag = soup.find("meta", attrs={"name": key}) or soup.find("meta", attrs={"property": key})
        if tag and tag.get("content"):
            return _normalize_space(str(tag.get("content")))
    return ""


def _fetch_cjzc_rows(window_start: datetime, run_end: datetime) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    url = "https://np-listapi.eastmoney.com/comm/web/getNewsByColumns"
    params = {
        "client": "web",
        "biz": "web_news_col",
        "column": "1207",
        "order": "1",
        "needInteractData": "0",
        "page_index": "1",
        "page_size": "200",
        "req_trace": "1710314682980",
        "fields": "code,showTime,title,mediaName,summary,image,url,uniqueUrl,Np_dst",
    }
    rows: List[Dict[str, Any]] = []
    for page in (1, 2):
        params["page_index"] = str(page)
        response = requests.get(url, params=params, headers={"User-Agent": "Mozilla/5.0"}, timeout=20)
        response.raise_for_status()
        payload = response.json()
        for item in payload.get("data", {}).get("list", []) or []:
            published_at = _normalize_iso(str(item.get("showTime") or ""))
            published_dt = _parse_iso_datetime(published_at)
            if published_dt is None or not _is_within_window(published_dt, window_start, run_end):
                continue
            rows.extend(
                _expand_breakfast_candidates(
                    title=str(item.get("title") or "").strip(),
                    summary=str(item.get("summary") or item.get("title") or "").strip(),
                    published_at=published_at,
                    url=str(item.get("uniqueUrl") or "").strip(),
                )
            )
    return rows, {"pages_scanned": 2}


def _fetch_cls_key_rows(window_start: datetime, run_end: datetime) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    url = "https://www.cls.cn/nodeapi/telegraphList"
    response = requests.get(url, params={"rn": "50"}, headers={"User-Agent": "Mozilla/5.0"}, timeout=20)
    response.raise_for_status()
    payload = response.json()
    rows: List[Dict[str, Any]] = []
    for item in payload.get("data", {}).get("roll_data", []) or []:
        if int(item.get("is_ad") or 0):
            continue
        published_at = datetime.fromtimestamp(int(item["ctime"])).isoformat() if item.get("ctime") else ""
        published_dt = _parse_iso_datetime(published_at)
        if published_dt is None or not _is_within_window(published_dt, window_start, run_end):
            continue
        rows.append(
            {
                "title": str(item.get("title") or "").strip(),
                "published_at": published_at,
                "preview": str(item.get("content") or item.get("title") or "").strip(),
                "url": "",
                "needs_fetch": False,
                "level": str(item.get("level") or ""),
            }
        )
    rows.sort(key=lambda item: item.get("published_at", ""), reverse=True)
    return rows, {"rows_scanned": len(payload.get("data", {}).get("roll_data", []) or [])}


def _fetch_ths_rows(window_start: datetime, run_end: datetime) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    url = "https://news.10jqka.com.cn/tapp/news/push/stock"
    rows: List[Dict[str, Any]] = []
    pages_scanned = 0
    for page in range(1, THS_MAX_PAGES + 1):
        params = {"page": str(page), "tag": "", "track": "website"}
        response = requests.get(url, params=params, headers={"User-Agent": "Mozilla/5.0"}, timeout=20)
        response.raise_for_status()
        payload = response.json()
        source_items = payload.get("data", {}).get("list", []) or []
        pages_scanned += 1
        if not source_items:
            break
        oldest_timestamp = None
        for item in source_items:
            published_at = ""
            if item.get("rtime"):
                published_at = datetime.fromtimestamp(int(item["rtime"])).isoformat()
            published_dt = _parse_iso_datetime(published_at)
            if published_dt is not None:
                oldest_timestamp = published_dt if oldest_timestamp is None else min(oldest_timestamp, published_dt)
            if published_dt is None or not _is_within_window(published_dt, window_start, run_end):
                continue
            rows.append(
                {
                    "title": str(item.get("title") or "").strip(),
                    "published_at": published_at,
                    "preview": str(item.get("digest") or item.get("title") or "").strip(),
                    "url": str(item.get("url") or "").strip(),
                    "needs_fetch": bool(str(item.get("url") or "").strip()),
                }
            )
        if oldest_timestamp is not None and oldest_timestamp < window_start:
            break
    return rows, {"pages_scanned": pages_scanned}


def _fetch_futu_rows(window_start: datetime, run_end: datetime) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    url = "https://news.futunn.com/news-site-api/main/get-flash-list"
    rows: List[Dict[str, Any]] = []
    seq_mark = ""
    pages_scanned = 0
    for _ in range(FUTU_MAX_PAGES):
        params = {"pageSize": "50"}
        if seq_mark:
            params["seqMark"] = seq_mark
        response = requests.get(
            url,
            params=params,
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=20,
        )
        response.raise_for_status()
        payload = response.json()
        data = payload.get("data", {}).get("data", {}) or {}
        news_items = data.get("news", []) or []
        pages_scanned += 1
        if not news_items:
            break

        oldest_timestamp = None
        for item in news_items:
            published_at = ""
            if item.get("time"):
                published_at = datetime.fromtimestamp(int(item["time"])).isoformat()
            published_dt = _parse_iso_datetime(published_at)
            if published_dt is not None:
                oldest_timestamp = published_dt if oldest_timestamp is None else min(oldest_timestamp, published_dt)
            if published_dt is None or not _is_within_window(published_dt, window_start, run_end):
                continue
            rows.append(
                {
                    "title": str(item.get("title") or "").strip(),
                    "published_at": published_at,
                    "preview": str(item.get("content") or item.get("title") or "").strip(),
                    "url": str(item.get("detailUrl") or "").strip(),
                    "needs_fetch": bool(str(item.get("detailUrl") or "").strip()),
                }
            )

        seq_mark = str(data.get("seqMark") or "").strip()
        has_more = bool(data.get("hasMore"))
        if not has_more or not seq_mark:
            break
        if oldest_timestamp is not None and oldest_timestamp < window_start:
            break
    return rows, {"pages_scanned": pages_scanned}


def _expand_breakfast_candidates(
    *,
    title: str,
    summary: str,
    published_at: str,
    url: str,
) -> List[Dict[str, Any]]:
    if not url:
        return [
            {
                "title": title,
                "published_at": published_at,
                "preview": summary or title,
                "url": url,
                "needs_fetch": False,
            }
        ]

    try:
        html = _download_html(url)
        soup = BeautifulSoup(html, "html.parser")
        content = soup.select_one("div#ContentBody")
        if content is None:
            raise ValueError("早餐正文容器不存在")
        items = _split_breakfast_items(content, published_at=published_at, url=url)
        global_market_text = fetch_breakfast_article_payload(url).get("global_market_text", "").strip()
        if global_market_text:
            items.append(
                {
                    "title": "环球市场",
                    "published_at": published_at,
                    "preview": global_market_text,
                    "url": url,
                    "needs_fetch": False,
                }
            )
        if items:
            return items
    except Exception as exc:
        LOGGER.warning("财经早餐展开失败，降级为单条: url=%s error=%s", url, exc)
    return [
        {
            "title": title,
            "published_at": published_at,
            "preview": summary or title,
            "url": url,
            "needs_fetch": True,
        }
    ]


def _split_breakfast_items(content: Any, *, published_at: str, url: str) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    current_section = ""
    current_title = ""
    current_parts: List[str] = []

    def flush_item() -> None:
        nonlocal current_title, current_parts
        body = "\n".join(part for part in current_parts if part).strip()
        title_text = current_title.strip()
        if title_text and body:
            items.append(
                {
                    "title": title_text,
                    "published_at": published_at,
                    "preview": body,
                    "url": url,
                    "needs_fetch": False,
                }
            )
        current_title = ""
        current_parts = []

    for child in content.find_all(["h3", "p"], recursive=False):
        if child.name == "h3":
            flush_item()
            current_section = _clean_breakfast_text(child.get_text(" ", strip=True))
            continue
        if child.find("img"):
            continue
        text = _clean_breakfast_text(child.get_text(" ", strip=True))
        if not text:
            continue
        if _looks_like_breakfast_title(text):
            flush_item()
            current_title = _strip_trailing_colon(text)
            if current_section and current_section not in {"每日精选", "热点题材", "公司新闻", "环球市场"}:
                current_title = f"{current_section} {current_title}".strip()
            continue
        if not current_title:
            fallback_title = _build_breakfast_fallback_title(current_section, text)
            current_title = fallback_title
        current_parts.append(text)

    flush_item()
    return items


def _clean_breakfast_text(text: str) -> str:
    cleaned = re.sub(r"\s+", " ", str(text or "")).strip()
    cleaned = cleaned.replace(" ：", "：").replace(" :", ":")
    return re.sub(r"(?<=[\u4e00-\u9fff])\s+(?=[\u4e00-\u9fff])", "", cleaned)


def _looks_like_breakfast_title(text: str) -> bool:
    stripped = _clean_breakfast_text(text)
    return bool(stripped) and len(stripped) <= 48 and stripped.endswith(("：", ":"))


def _strip_trailing_colon(text: str) -> str:
    return re.sub(r"[：:]\s*$", "", _clean_breakfast_text(text))


def _build_breakfast_fallback_title(section: str, text: str) -> str:
    prefix = section.strip() if section else "早餐快讯"
    normalized = _clean_breakfast_text(text)
    head = _strip_trailing_colon(normalized.split("：", 1)[0])
    snippet = head if 2 <= len(head) <= 28 else normalized[:24].rstrip("，。；;")
    return f"{prefix} {snippet}".strip()


def _normalize_iso(value: str) -> str:
    if not value:
        return ""
    for fmt in (
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
        "%Y/%m/%d %H:%M:%S",
        "%Y/%m/%d",
    ):
        try:
            return datetime.strptime(value, fmt).isoformat()
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(value).isoformat()
    except ValueError:
        return value


def _parse_iso_datetime(value: str) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def _is_within_window(value: datetime, window_start: datetime, run_end: datetime) -> bool:
    return window_start <= value <= run_end


def _dedupe_batch(client: OpenAI, batch: Sequence[Dict[str, Any]], *, model: str) -> Dict[str, Any]:
    payload = [
        {
            "news_id": item["llm_id"],
            "title": item["title"],
            "published_at": item["published_at"],
            "source": item["source"],
            "preview": item["preview"],
        }
        for item in batch
    ]
    completion = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": DEDUPE_SYSTEM_PROMPT},
            {"role": "user", "content": json.dumps(payload, ensure_ascii=False, indent=2)},
        ],
        temperature=0.1,
    )
    content = completion.choices[0].message.content or ""
    return _parse_json_object(content)


def _normalize_dedup_result(input_ids: Sequence[str], result: Dict[str, Any]) -> Dict[str, Any]:
    valid_ids = set(input_ids)
    keep_ids = [item for item in result.get("keep_ids", []) if item in valid_ids]

    duplicate_groups = []
    mentioned_duplicate_ids = set()
    for group in result.get("duplicate_groups", []) or []:
        if not isinstance(group, dict):
            continue
        canonical_id = str(group.get("canonical_id") or "")
        if canonical_id not in valid_ids:
            continue
        duplicate_ids = [item for item in group.get("duplicate_ids", []) if item in valid_ids and item != canonical_id]
        mentioned_duplicate_ids.update(duplicate_ids)
        duplicate_groups.append(
            {
                "canonical_id": canonical_id,
                "duplicate_ids": duplicate_ids,
                "reason": str(group.get("reason") or "").strip(),
            }
        )

    noise_items = []
    mentioned_noise_ids = set()
    for item in result.get("noise_items", []) or []:
        if not isinstance(item, dict):
            continue
        news_id = str(item.get("news_id") or "")
        if news_id not in valid_ids:
            continue
        mentioned_noise_ids.add(news_id)
        noise_items.append(
            {
                "news_id": news_id,
                "reason": str(item.get("reason") or "").strip(),
            }
        )

    accounted = set(keep_ids) | mentioned_duplicate_ids | mentioned_noise_ids
    missing_ids = [item for item in input_ids if item not in accounted]
    if missing_ids:
        LOGGER.warning("去重结果存在未判决 news_id，按保守策略保留: %s", missing_ids)
        keep_ids.extend(missing_ids)

    keep_ids = [item for item in input_ids if item in set(keep_ids) and item not in mentioned_duplicate_ids]
    return {
        "keep_ids": keep_ids,
        "duplicate_groups": duplicate_groups,
        "noise_items": noise_items,
    }


def _convert_dedup_result_to_news_ids(result: Dict[str, Any], llm_to_news: Dict[str, str]) -> Dict[str, Any]:
    return {
        "keep_ids": [llm_to_news[item] for item in result.get("keep_ids", []) if item in llm_to_news],
        "duplicate_groups": [
            {
                "canonical_id": llm_to_news[group["canonical_id"]],
                "canonical_llm_id": group["canonical_id"],
                "duplicate_ids": [llm_to_news[item] for item in group.get("duplicate_ids", []) if item in llm_to_news],
                "duplicate_llm_ids": [item for item in group.get("duplicate_ids", []) if item in llm_to_news],
                "reason": group.get("reason", ""),
            }
            for group in result.get("duplicate_groups", [])
            if group.get("canonical_id") in llm_to_news
        ],
        "noise_items": [
            {
                "news_id": llm_to_news[item["news_id"]],
                "llm_id": item["news_id"],
                "reason": item.get("reason", ""),
            }
            for item in result.get("noise_items", [])
            if item.get("news_id") in llm_to_news
        ],
    }


def _sort_keep_ids(candidates: Sequence[Dict[str, Any]], keep_ids: Iterable[str]) -> List[str]:
    keep_set = set(keep_ids)
    return [item["news_id"] for item in candidates if item["news_id"] in keep_set]


def _rule_merge_survivors(items: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for item in items:
        key = _normalize_news_title_key(item.get("title") or item.get("preview") or "")
        grouped.setdefault(key, []).append(item)

    keep_ids: List[str] = []
    duplicate_groups: List[Dict[str, Any]] = []
    noise_items: List[Dict[str, Any]] = []

    for group in grouped.values():
        ordered = sorted(
            group,
            key=lambda item: (
                str(item.get("published_at") or ""),
                str(item.get("news_id") or ""),
            ),
        )
        canonical = ordered[0]
        keep_ids.append(str(canonical.get("news_id") or ""))
        duplicate_ids = [str(item.get("news_id") or "") for item in ordered[1:] if str(item.get("news_id") or "")]
        if duplicate_ids:
            duplicate_groups.append(
                {
                    "canonical_id": str(canonical.get("news_id") or ""),
                    "duplicate_ids": duplicate_ids,
                    "reason": "跨批规则合并：标题标准化后完全一致",
                }
            )

    return {
        "keep_ids": [item for item in keep_ids if item],
        "duplicate_groups": duplicate_groups,
        "noise_items": noise_items,
    }


def _normalize_news_title_key(text: Any) -> str:
    normalized = str(text or "").strip().lower()
    normalized = re.sub(r"\s+", "", normalized)
    normalized = re.sub(r"[：:;；,，。、“”\"'‘’（）()\\-—_·\[\]【】<>《》!！?？]", "", normalized)
    return normalized


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


def _get_dedup_client(model: str) -> OpenAI:
    api_key = os.getenv("AUDIT_MODEL_API_KEY") or os.getenv("OPENAI_API_KEY")
    base_url = os.getenv("AUDIT_MODEL_BASE_URL") or os.getenv("OPENAI_API_BASE")
    if not api_key or not base_url:
        raise ValueError("缺少 AUDIT_MODEL_API_KEY/AUDIT_MODEL_BASE_URL 或 OPENAI_API_KEY/OPENAI_API_BASE")
    LOGGER.info("初始化新闻去重客户端: model=%s base_url=%s", model, base_url)
    return OpenAI(api_key=api_key, base_url=base_url)


def _download_html(url: str) -> str:
    response = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=30)
    response.raise_for_status()
    return response.text


def _build_payload(
    run_date: str,
    items: Sequence[Dict[str, Any]],
    *,
    source_status: Sequence[Dict[str, Any]] | None = None,
    summary: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    payload = {
        "schema_version": 1,
        "run_date": run_date,
        "updated_at": datetime.now().isoformat(),
        "items": list(items),
    }
    if source_status is not None:
        payload["source_status"] = list(source_status)
    if summary is not None:
        payload["summary"] = dict(summary)
    return payload


def _chunk_list(items: Sequence[Dict[str, Any]], chunk_size: int) -> List[List[Dict[str, Any]]]:
    size = max(int(chunk_size), 1)
    return [list(items[index : index + size]) for index in range(0, len(items), size)]


def _sha1(text: str) -> str:
    return hashlib.sha1(text.encode("utf-8")).hexdigest()[:10]


def _normalize_space(text: str) -> str:
    return re.sub(r"\s+", " ", str(text or "")).strip()
