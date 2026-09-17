from __future__ import annotations

import json
from pathlib import Path

import pytest

from services.document_conversion import (
    PDF_MARKDOWN_CONVERTER_VERSION,
    PDFConversionError,
    PDFMarkdownConverter,
    convert_pdfs_in_parallel,
    is_pdf_markdown_cache_current,
    write_pdf_conversion_artifacts,
)


def _make_pdf(path: Path) -> None:
    """生成一份带文本层与线框表格的最小 PDF，供转换链路使用。"""
    import pymupdf

    document = pymupdf.open()
    page = document.new_page()
    # 内置 Helvetica 无法嵌入中文，测试 PDF 只用 ASCII 文本；真实财报 PDF 自带中文字体。
    page.insert_text((72, 72), "Report Title", fontsize=14)

    left, top, right, bottom = 72, 120, 300, 180
    col_x = (left + right) / 2
    row_ys = [top, (top + bottom) / 2, bottom]
    for y in row_ys:
        page.draw_line(pymupdf.Point(left, y), pymupdf.Point(right, y))
    for x in (left, col_x, right):
        page.draw_line(pymupdf.Point(x, top), pymupdf.Point(x, bottom))
    page.insert_text((left + 8, top + 20), "Item", fontsize=10)
    page.insert_text((col_x + 8, top + 20), "Amount", fontsize=10)
    page.insert_text((left + 8, row_ys[1] - 8), "Revenue", fontsize=10)
    page.insert_text((col_x + 8, row_ys[1] - 8), "100", fontsize=10)

    document.save(str(path))
    document.close()


def test_converter_returns_markdown_and_metadata(tmp_path: Path) -> None:
    pdf_path = tmp_path / "report.pdf"
    _make_pdf(pdf_path)

    result = PDFMarkdownConverter().convert_with_details(pdf_path, profile="financial_report")

    assert result.engine == "pymupdf4llm"
    assert result.engine_version
    assert "Report Title" in result.markdown
    assert "|" in result.markdown
    assert result.metrics["char_count"] == len(result.markdown)
    assert result.converter_version == PDF_MARKDOWN_CONVERTER_VERSION


def test_cache_is_current_until_source_changes(tmp_path: Path) -> None:
    pdf_path = tmp_path / "report.pdf"
    md_path = tmp_path / "report.md"
    _make_pdf(pdf_path)

    result = PDFMarkdownConverter().convert_with_details(pdf_path)
    write_pdf_conversion_artifacts(md_path, result, pdf_path)

    assert is_pdf_markdown_cache_current(md_path, pdf_path)

    pdf_path.write_bytes(pdf_path.read_bytes() + b"% changed")
    assert not is_pdf_markdown_cache_current(md_path, pdf_path)


def test_cache_accepts_legacy_mineru_markdown(tmp_path: Path) -> None:
    """切换转换引擎不该让历史 Markdown 全量重转：源 PDF 没变就继续用。"""
    pdf_path = tmp_path / "report.pdf"
    md_path = tmp_path / "report.md"
    _make_pdf(pdf_path)

    result = PDFMarkdownConverter().convert_with_details(pdf_path)
    write_pdf_conversion_artifacts(md_path, result, pdf_path)

    metadata_path = md_path.with_name(f"{md_path.name}.meta.json")
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    metadata["engine"] = "mineru"
    metadata["converter_version"] = 4
    metadata["converter_signature"] = "mineru:hybrid-engine:medium:auto:financial_report"
    metadata_path.write_text(json.dumps(metadata), encoding="utf-8")

    assert is_pdf_markdown_cache_current(md_path, pdf_path)


def test_conversion_rejects_non_pdf(tmp_path: Path) -> None:
    txt_path = tmp_path / "report.txt"
    txt_path.write_text("not a pdf", encoding="utf-8")

    with pytest.raises(ValueError):
        PDFMarkdownConverter().convert_with_details(txt_path)


def test_conversion_fails_on_unreadable_pdf(tmp_path: Path) -> None:
    pdf_path = tmp_path / "broken.pdf"
    pdf_path.write_bytes(b"%PDF-1.4 broken content")

    with pytest.raises(PDFConversionError):
        PDFMarkdownConverter().convert_with_details(pdf_path)


def test_parallel_conversion_preserves_order_and_uses_cache(tmp_path: Path) -> None:
    jobs = []
    for name in ("first", "second"):
        pdf_path = tmp_path / f"{name}.pdf"
        _make_pdf(pdf_path)
        jobs.append((pdf_path, tmp_path / f"{name}.md"))

    outcomes = convert_pdfs_in_parallel(jobs, workers=2)
    assert [item.status for item in outcomes] == ["converted", "converted"]
    assert [Path(item.markdown_path) for item in outcomes] == [job[1] for job in jobs]
    assert all(is_pdf_markdown_cache_current(md_path, pdf_path) for pdf_path, md_path in jobs)

    cached = convert_pdfs_in_parallel(jobs, workers=2)
    assert [item.status for item in cached] == ["cached", "cached"]
