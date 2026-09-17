"""Convert PDF documents to Markdown with pymupdf4llm.

转换就是一行 `pymupdf4llm.to_markdown(pdf)`，在本进程内完成，不需要任何外部服务。
本模块只在这行调用外面包了缓存元数据：Markdown 与源 PDF 的 size/mtime 一起落盘，
下次调用直接命中缓存，避免重复转换数百页财报。
"""

from __future__ import annotations

import json
import os
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable

from core.logging import get_logger


LOGGER = get_logger("DocumentConversion")

PDF_MARKDOWN_CONVERTER_VERSION = 5
DEFAULT_PDF_CONVERSION_PROFILE = "financial_report"
SUPPORTED_PDF_CONVERSION_PROFILES = {"financial_report", "general"}


def default_conversion_workers() -> int:
    """并发转换的默认进程数。

    pymupdf4llm 单进程转换本身会吃满约 2 个核，所以按 CPU 数的一半取并发，
    上限 6，避免把机器压到过载反而拖慢整体吞吐。
    """
    cpu = os.cpu_count() or 4
    return max(1, min(6, cpu // 2))


class PDFConversionError(RuntimeError):
    """Raised when a PDF cannot be converted to Markdown."""


def _converter_signature(profile: str) -> str:
    return f"pymupdf4llm:{profile}"


@dataclass
class PDFConversionResult:
    markdown: str
    profile: str
    converter_version: int
    converter_signature: str
    engine: str
    engine_version: str | None
    metrics: dict[str, int]
    generated_at: str

    def to_metadata(self, source_pdf: str | Path | None = None) -> dict[str, Any]:
        payload = asdict(self)
        payload.pop("markdown", None)
        if source_pdf is not None:
            source_path = Path(source_pdf)
            payload["source_pdf"] = str(source_path)
            if source_path.exists():
                stat = source_path.stat()
                payload["source_pdf_size"] = stat.st_size
                payload["source_pdf_mtime"] = stat.st_mtime
        return payload


def pdf_markdown_metadata_path(markdown_path: str | Path) -> Path:
    path = Path(markdown_path)
    return path.with_name(f"{path.name}.meta.json")


def is_pdf_markdown_cache_current(
    markdown_path: str | Path,
    source_pdf: str | Path,
    *,
    profile: str = DEFAULT_PDF_CONVERSION_PROFILE,
) -> bool:
    md_path = Path(markdown_path)
    pdf_path = Path(source_pdf)
    meta_path = pdf_markdown_metadata_path(md_path)
    if not md_path.exists() or md_path.stat().st_size <= 0:
        return False
    if not pdf_path.exists() or not meta_path.exists():
        return False

    try:
        metadata = json.loads(meta_path.read_text(encoding="utf-8"))
    except (OSError, ValueError, TypeError):
        return False

    if metadata.get("profile") != profile:
        return False

    # 其余只看源 PDF 有没有被换掉：size + mtime 一致就认为这份 Markdown 还能用。
    # 不按转换器版本/引擎签名失效，否则换一次转换引擎就要把历史 Markdown 全量重转一遍。
    # 确实需要重转某份时，删掉对应的 .md（和 .md.meta.json）即可。
    stat = pdf_path.stat()
    if metadata.get("source_pdf_size") != stat.st_size:
        return False
    if abs(float(metadata.get("source_pdf_mtime") or 0) - stat.st_mtime) > 1:
        return False
    return True


def write_pdf_conversion_artifacts(
    markdown_path: str | Path,
    result: PDFConversionResult,
    source_pdf: str | Path,
) -> None:
    md_path = Path(markdown_path)
    md_path.parent.mkdir(parents=True, exist_ok=True)
    md_path.write_text(result.markdown, encoding="utf-8")
    pdf_markdown_metadata_path(md_path).write_text(
        json.dumps(result.to_metadata(source_pdf), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


class PDFMarkdownConverter:
    """Thin wrapper that adds caching metadata around ``pymupdf4llm.to_markdown``."""

    def convert_with_details(
        self,
        file_path: str | Path,
        output_dir: str | Path | None = None,
        *,
        profile: str = DEFAULT_PDF_CONVERSION_PROFILE,
    ) -> PDFConversionResult:
        if profile not in SUPPORTED_PDF_CONVERSION_PROFILES:
            raise ValueError(
                f"不支持的 PDF 转换 profile: {profile}，"
                f"可选值: {sorted(SUPPORTED_PDF_CONVERSION_PROFILES)}"
            )

        pdf_path = Path(file_path).expanduser().resolve()
        if not pdf_path.exists():
            raise FileNotFoundError(f"PDF 文件不存在: {pdf_path}")
        if pdf_path.suffix.lower() != ".pdf":
            raise ValueError(f"输入文件不是 PDF: {pdf_path}")

        try:
            import pymupdf4llm
        except ImportError as exc:  # pragma: no cover - 环境缺失
            raise PDFConversionError(
                "缺少 pymupdf4llm，无法转换 PDF。请在项目虚拟环境中执行: pip install pymupdf4llm"
            ) from exc

        LOGGER.info("开始转换 PDF: %s", pdf_path)
        start_time = time.monotonic()
        try:
            markdown = pymupdf4llm.to_markdown(str(pdf_path))
        except Exception as exc:
            raise PDFConversionError(f"pymupdf4llm 转换 PDF 失败: {pdf_path}") from exc

        if not isinstance(markdown, str) or not markdown.strip():
            raise PDFConversionError(
                f"pymupdf4llm 没有提取到有效文本（PDF 无文本层或已损坏）: {pdf_path}。"
                "扫描件需要在本机安装 OCR 后端（Tesseract tessdata 或 RapidOCR）后才能识别。"
            )

        elapsed_seconds = time.monotonic() - start_time
        result = PDFConversionResult(
            markdown=markdown,
            profile=profile,
            converter_version=PDF_MARKDOWN_CONVERTER_VERSION,
            converter_signature=_converter_signature(profile),
            engine="pymupdf4llm",
            engine_version=str(pymupdf4llm.VERSION),
            metrics={
                "char_count": len(markdown),
                "line_count": markdown.count("\n") + 1,
            },
            generated_at=datetime.now().isoformat(timespec="seconds"),
        )

        if output_dir:
            output_path = Path(output_dir) / f"{pdf_path.stem}.md"
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_text(markdown, encoding="utf-8")
            LOGGER.info("Markdown 已保存到: %s", output_path)

        LOGGER.info(
            "PDF 转换完成: chars=%s, lines=%s, elapsed=%.2fs",
            result.metrics["char_count"],
            result.metrics["line_count"],
            elapsed_seconds,
        )
        return result

    def convert(
        self,
        file_path: str | Path,
        output_dir: str | Path | None = None,
        *,
        profile: str = DEFAULT_PDF_CONVERSION_PROFILE,
    ) -> str:
        return self.convert_with_details(
            file_path,
            output_dir=output_dir,
            profile=profile,
        ).markdown


def basic_convert(
    file_path: str | Path,
    output_dir: str | Path | None = None,
    use_llm: bool = False,
    *,
    profile: str = DEFAULT_PDF_CONVERSION_PROFILE,
) -> str:
    del use_llm
    return PDFMarkdownConverter().convert(
        file_path,
        output_dir=output_dir,
        profile=profile,
    )


@dataclass
class ParallelConversionOutcome:
    """并发转换中单个 job 的结果。status ∈ {'cached', 'converted', 'error'}。"""

    pdf_path: str
    markdown_path: str
    status: str
    detail: str
    elapsed_seconds: float


def _convert_job(job: tuple[str, str, str]) -> ParallelConversionOutcome:
    pdf_str, md_str, profile = job
    pdf_path = Path(pdf_str)
    md_path = Path(md_str)
    start = time.monotonic()
    try:
        if is_pdf_markdown_cache_current(md_path, pdf_path, profile=profile):
            return ParallelConversionOutcome(pdf_str, md_str, "cached", "", 0.0)
        result = PDFMarkdownConverter().convert_with_details(
            str(pdf_path), output_dir=None, profile=profile
        )
        write_pdf_conversion_artifacts(md_path, result, pdf_path)
        return ParallelConversionOutcome(
            pdf_str,
            md_str,
            "converted",
            f"chars={result.metrics['char_count']}",
            time.monotonic() - start,
        )
    except Exception as exc:  # noqa: BLE001 - 单个文件失败不应中断整批
        return ParallelConversionOutcome(
            pdf_str,
            md_str,
            "error",
            f"{type(exc).__name__}: {exc}",
            time.monotonic() - start,
        )


def convert_pdfs_in_parallel(
    jobs: Iterable[tuple[str | Path, str | Path]],
    *,
    workers: int | None = None,
    profile: str = DEFAULT_PDF_CONVERSION_PROFILE,
) -> list[ParallelConversionOutcome]:
    """并发把 ``(pdf_path, markdown_path)`` 批量转成 Markdown。

    - 命中缓存的 job 在主进程直接判定，不会占用子进程。
    - 返回列表与输入顺序一一对应。
    - 单个文件失败只在该项标记 ``error``，不影响其余文件。
    """
    normalized = [(str(Path(pdf)), str(Path(md))) for pdf, md in jobs]
    if not normalized:
        return []

    if workers is None or workers < 1:
        workers = default_conversion_workers()
    workers = max(1, min(int(workers), len(normalized)))

    outcomes: list[ParallelConversionOutcome | None] = [None] * len(normalized)
    pending: list[tuple[int, tuple[str, str, str]]] = []
    for idx, (pdf_str, md_str) in enumerate(normalized):
        if is_pdf_markdown_cache_current(Path(md_str), Path(pdf_str), profile=profile):
            outcomes[idx] = ParallelConversionOutcome(pdf_str, md_str, "cached", "", 0.0)
        else:
            pending.append((idx, (pdf_str, md_str, profile)))

    if pending:
        LOGGER.info(
            "并发转换 PDF: 待转=%d 已缓存=%d workers=%d",
            len(pending),
            len(normalized) - len(pending),
            workers,
        )
        with ProcessPoolExecutor(max_workers=workers) as pool:
            future_to_idx = {
                pool.submit(_convert_job, job): idx for idx, job in pending
            }
            for future in as_completed(future_to_idx):
                idx = future_to_idx[future]
                try:
                    outcomes[idx] = future.result()
                except Exception as exc:  # noqa: BLE001 - 子进程崩溃时兜底
                    pdf_str, md_str = normalized[idx]
                    outcomes[idx] = ParallelConversionOutcome(
                        pdf_str, md_str, "error", f"{type(exc).__name__}: {exc}", 0.0
                    )

    return [outcome for outcome in outcomes if outcome is not None]
