"""EastMoney breakfast article enrichment helpers.

This module is shared by the "run-news" curation pipeline. It was previously
implemented inside the deprecated stage-1 run-daily pipeline module.
"""

from __future__ import annotations

import json
import os
import re
from hashlib import sha1
from pathlib import Path
from typing import Any, Dict, List

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from openai import OpenAI

from core.logging import get_logger


LOGGER = get_logger("SelectionBreakfast")
load_dotenv(".env")


def fetch_breakfast_article_payload(url: str) -> Dict[str, str]:
    """Download and parse a single EastMoney breakfast article.

    Returns:
        {"body_text": "...", "global_market_text": "..."}
    """

    article_id = _extract_article_id(url)
    cache_dir = Path("data/market_state/raw_news_assets/breakfast")
    cache_dir.mkdir(parents=True, exist_ok=True)
    html_path = cache_dir / f"{article_id}.html"
    image_path = cache_dir / f"{article_id}_global_market.png"
    image_json_path = cache_dir / f"{article_id}_global_market.json"

    html = _download_breakfast_html(url)
    html_path.write_text(html, encoding="utf-8")
    soup = BeautifulSoup(html, "html.parser")
    content = soup.select_one("div#ContentBody")
    if content is None:
        raise ValueError("未找到财经早餐正文容器 div#ContentBody")

    body_text = _extract_breakfast_body_text(content)
    global_market_img_url = _extract_global_market_image_url(content)
    global_market_text = ""
    if global_market_img_url:
        _download_breakfast_image(global_market_img_url, image_path)
        global_market_payload = _extract_global_market_with_qwen(image_path, image_json_path)
        global_market_text = _format_global_market_payload(global_market_payload)

    return {
        "body_text": body_text,
        "global_market_text": global_market_text,
    }


def _sha1(text: str) -> str:
    return sha1(text.encode("utf-8")).hexdigest()[:12]


def _extract_article_id(url: str) -> str:
    match = re.search(r"/a/(\\d+)\\.html", url)
    if match:
        return match.group(1)
    return _sha1(url)


def _download_breakfast_html(url: str) -> str:
    response = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=30)
    response.raise_for_status()
    return response.text


def _extract_breakfast_body_text(content: Any) -> str:
    parts: List[str] = []
    for child in content.find_all(["h3", "p"], recursive=False):
        if child.name == "h3":
            heading = child.get_text(" ", strip=True).replace("\u3000", "").strip()
            if heading:
                parts.append(f"## {heading}")
            continue
        if child.find("img") and "环球市场" in " ".join(parts[-1:]):
            continue
        text = child.get_text("\n", strip=True).replace("\u3000", "").strip()
        if text:
            parts.append(text)
    return "\n".join(parts).strip()


def _extract_global_market_image_url(content: Any) -> str:
    for heading in content.find_all("h3", recursive=False):
        heading_text = heading.get_text(" ", strip=True).replace("\u3000", "")
        if "环球市场" not in heading_text:
            continue
        sibling = heading.find_next_sibling()
        while sibling is not None:
            if getattr(sibling, "name", None) == "p" and sibling.find("img"):
                src = sibling.find("img").get("src") or ""
                return src.replace("https://", "http://", 1)
            if getattr(sibling, "name", None) == "h3":
                break
            sibling = sibling.find_next_sibling()
    return ""


def _download_breakfast_image(url: str, image_path: Path) -> None:
    response = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=30)
    response.raise_for_status()
    image_path.write_bytes(response.content)


def _extract_global_market_with_qwen(image_path: Path, image_json_path: Path) -> Dict[str, Any]:
    if image_json_path.exists():
        try:
            return json.loads(image_json_path.read_text(encoding="utf-8"))
        except Exception:
            pass

    api_key = os.getenv("EXTRACTION_MODEL_API_KEY")
    base_url = os.getenv("EXTRACTION_MODEL_BASE_URL")
    if not api_key or not base_url:
        raise ValueError("缺少 EXTRACTION_MODEL_API_KEY 或 EXTRACTION_MODEL_BASE_URL")

    client = OpenAI(api_key=api_key, base_url=base_url)
    file_obj = client.files.create(file=image_path, purpose="file-extract")
    file_id = getattr(file_obj, "id", None)
    prompt = (
        "请读取这张财经图片，并只输出一个JSON对象。"
        "字段要求：capture_time, sections。"
        "sections 是数组，每个 section 包含 section_name, rows。"
        "rows 是数组，每行包含 market, name, latest, change, change_pct。"
        "数字按图片原文输出，不要自行解释，不要输出额外文字。"
    )
    completion = client.chat.completions.create(
        model="qwen-doc-turbo",
        messages=[
            {"role": "system", "content": "你是一个严谨的财经OCR结构化助手。"},
            {"role": "system", "content": f"fileid://{file_id}"},
            {"role": "user", "content": prompt},
        ],
    )
    payload = _parse_json_like(completion.choices[0].message.content)
    image_json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return payload


def _parse_json_like(text: str) -> Dict[str, Any]:
    content = str(text or "").strip()
    match = re.search(r"```json\\s*([\\s\\S]*?)```", content, re.IGNORECASE)
    if match:
        content = match.group(1).strip()
    start = content.find("{")
    end = content.rfind("}")
    if start == -1 or end == -1 or end <= start:
        raise ValueError(f"无法解析图片OCR JSON: {content[:500]}")
    parsed = json.loads(content[start : end + 1])
    if not isinstance(parsed, dict):
        raise ValueError("图片OCR返回不是对象")
    return parsed


def _format_global_market_payload(payload: Dict[str, Any]) -> str:
    lines: List[str] = []
    capture_time = str(payload.get("capture_time") or "").strip()
    if capture_time:
        lines.append(f"截点时间: {capture_time}")
    for section in payload.get("sections", []) or []:
        section_name = str(section.get("section_name") or "").strip()
        if section_name:
            lines.append(f"[{section_name}]")
        for row in section.get("rows", []) or []:
            market = str(row.get("market") or "").strip()
            name = str(row.get("name") or "").strip()
            latest = str(row.get("latest") or "").strip()
            change = str(row.get("change") or "").strip()
            change_pct = str(row.get("change_pct") or "").strip()
            row_text = " | ".join(part for part in [market, name, latest, change, change_pct] if part)
            if row_text:
                lines.append(row_text)
    return "\n".join(lines).strip()

