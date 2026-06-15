#!/usr/bin/env python3
"""Convert a local PDF file to Markdown through the configured MinerU API."""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from core.logging import init_component_logger

WINDOWS_PATH_RE = re.compile(r"^([A-Za-z]):[\\/](.*)$")


def _normalize_path(raw_path: str, *, prefer_existing: bool) -> Path:
    match = WINDOWS_PATH_RE.match(raw_path.strip())
    if not match:
        return Path(raw_path).expanduser()

    drive = match.group(1).lower()
    rest = match.group(2).replace("\\", "/").lstrip("/")
    candidates = [Path(f"/nt/{drive}/{rest}"), Path(f"/mnt/{drive}/{rest}")]
    if prefer_existing:
        for candidate in candidates:
            if candidate.exists():
                return candidate
    return candidates[0]


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="将本地 PDF 转换为 Markdown。")
    parser.add_argument("pdf_path", help="输入 PDF 路径，支持 WSL 路径或 Windows 路径。")
    parser.add_argument(
        "--output",
        help="输出 Markdown 文件路径。默认写到 PDF 同目录、同名 .md 文件。",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="若输出文件已存在则覆盖。",
    )
    parser.add_argument(
        "--stdout",
        action="store_true",
        help="同时将转换后的 Markdown 输出到标准输出。",
    )
    parser.add_argument(
        "--profile",
        default="financial_report",
        choices=["financial_report", "general"],
        help="转换策略。默认 financial_report，优先保住财报/公告中的数字与表格文本。",
    )
    return parser


def _resolve_output_path(pdf_path: Path, output_arg: str | None) -> Path:
    if not output_arg:
        if pdf_path.parent.name == "pdfs" and pdf_path.parent.parent.exists():
            return pdf_path.parent.parent / "md" / f"{pdf_path.stem}.md"
        return pdf_path.with_suffix(".md")
    return _normalize_path(output_arg, prefer_existing=False).expanduser()


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()

    pdf_path = _normalize_path(args.pdf_path, prefer_existing=True).expanduser()
    output_path = _resolve_output_path(pdf_path, args.output)

    if not pdf_path.exists():
        parser.error(f"PDF 文件不存在: {pdf_path}")
    if pdf_path.suffix.lower() != ".pdf":
        parser.error(f"输入文件不是 PDF: {pdf_path}")
    if output_path.exists() and not args.force:
        parser.error(f"输出文件已存在，请使用 --force 覆盖: {output_path}")

    logger = init_component_logger(
        "PDFToMarkdown",
        group="tools",
        filename_prefix="pdf_to_markdown",
    )
    from services.document_conversion import PDFMarkdownConverter, write_pdf_conversion_artifacts

    output_path.parent.mkdir(parents=True, exist_ok=True)
    logger.info("开始转换 PDF: %s, profile=%s", pdf_path, args.profile)
    converter = PDFMarkdownConverter()
    result = converter.convert_with_details(str(pdf_path), output_dir=None, profile=args.profile)
    write_pdf_conversion_artifacts(output_path, result, pdf_path)
    logger.info("Markdown 与元数据已写入: %s", output_path)

    if args.stdout:
        sys.stdout.write(result.markdown)
        if not result.markdown.endswith("\n"):
            sys.stdout.write("\n")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
