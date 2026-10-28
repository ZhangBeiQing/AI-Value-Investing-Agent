#!/usr/bin/env python3
"""Cleanup orphaned DashScope uploaded files to free the upload quota."""

from __future__ import annotations

import argparse
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from core.logging import bootstrap_run_logging_from_argv

bootstrap_run_logging_from_argv("dashscope_cleanup")

from dotenv import load_dotenv

load_dotenv(PROJECT_ROOT / ".env")

from core.logging import get_logger
from openai import OpenAI

LOGGER = get_logger("CleanupDashscopeFiles")


def _iterate_files(client: OpenAI) -> list:
    files: list = []
    after: str | None = None
    while True:
        page = client.files.list(after=after) if after else client.files.list()
        files.extend(page.data)
        if not getattr(page, "has_more", False):
            break
        after = page.data[-1].id if page.data else None
        if not after:
            break
    return files


def main() -> int:
    parser = argparse.ArgumentParser(description="删除百炼云盘上遗留的上传文件，解除上传数量上限（默认仅预览）。")
    parser.add_argument("--keep", type=int, default=0, help="保留最近 N 个文件，其余删除（默认 0，全删）。")
    parser.add_argument("--yes", action="store_true", help="确认执行删除；不加该参数仅打印计划。")
    parser.add_argument("--max-workers", type=int, default=8, help="并发删除的线程数（默认 8）。")
    args = parser.parse_args()

    api_key = os.getenv("EXTRACTION_MODEL_API_KEY")
    base_url = os.getenv("EXTRACTION_MODEL_BASE_URL")
    if not api_key or not base_url:
        LOGGER.error("缺少 EXTRACTION_MODEL_API_KEY 或 EXTRACTION_MODEL_BASE_URL")
        return 2

    client = OpenAI(api_key=api_key, base_url=base_url, timeout=120.0)
    files = _iterate_files(client)
    files.sort(key=lambda f: getattr(f, "created_at", 0) or 0, reverse=True)
    LOGGER.info("当前云盘文件总数: %d", len(files))

    keep = max(0, args.keep)
    to_delete = files[keep:]
    LOGGER.info("计划保留 %d 个，删除 %d 个", min(keep, len(files)), len(to_delete))

    if not args.yes:
        for item in to_delete[:20]:
            LOGGER.info("  [dry-run] 待删除 file_id=%s", item.id)
        if len(to_delete) > 20:
            LOGGER.info("  [dry-run] ... 其余 %d 个省略", len(to_delete) - 20)
        LOGGER.info("未执行删除；确认无误后追加 --yes 重跑")
        return 0

    deleted = 0
    failed = 0
    total = len(to_delete)
    LOGGER.info("开始删除 %d 个文件（--max-workers=%d）...", total, max(1, args.max_workers))

    def _delete(item):
        client.files.delete(item.id)

    workers = max(1, args.max_workers)
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(_delete, item): item for item in to_delete}
        for future in as_completed(futures):
            item = futures[future]
            try:
                future.result()
                deleted += 1
            except Exception as exc:
                failed += 1
                LOGGER.warning("删除失败 file_id=%s: %s", item.id, exc)
            done = deleted + failed
            if done % 200 == 0 or done == total:
                LOGGER.info("进度: %d/%d（成功 %d, 失败 %d）", done, total, deleted, failed)

    LOGGER.info("删除完成: 成功 %d, 失败 %d", deleted, failed)
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
