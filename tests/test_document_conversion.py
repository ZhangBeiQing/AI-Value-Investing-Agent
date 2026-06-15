from __future__ import annotations

import json
from pathlib import Path

from services.document_conversion.mineru import (
    PDF_MARKDOWN_CONVERTER_VERSION,
    MinerUSettings,
    PDFMarkdownConverter,
    is_pdf_markdown_cache_current,
    write_pdf_conversion_artifacts,
)


class FakeResponse:
    def __init__(self, status_code: int, payload: dict) -> None:
        self.status_code = status_code
        self._payload = payload

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise AssertionError(f"unexpected HTTP status: {self.status_code}")

    def json(self) -> dict:
        return self._payload


class FakeSession:
    def __init__(self) -> None:
        self.status_calls = 0

    def get(self, url: str, timeout: tuple[float, float]) -> FakeResponse:
        del timeout
        if url.endswith("/health"):
            return FakeResponse(
                200,
                {"status": "healthy", "version": "3.3.1", "protocol_version": 2},
            )
        if url.endswith("/tasks/task-1/result"):
            return FakeResponse(
                200,
                {
                    "backend": "hybrid-engine",
                    "version": "3.3.1",
                    "results": {"report": {"md_content": "# 财报\n\n|项目|金额|\n|---|---|\n|收入|100|"}},
                },
            )
        if url.endswith("/tasks/task-1"):
            self.status_calls += 1
            status = "processing" if self.status_calls == 1 else "completed"
            return FakeResponse(
                200,
                {
                    "status": status,
                    "created_at": "2026-06-14T00:00:00+00:00",
                    "started_at": "2026-06-14T00:00:01+00:00",
                    "completed_at": "2026-06-14T00:00:02+00:00" if status == "completed" else None,
                },
            )
        raise AssertionError(f"unexpected GET: {url}")

    def post(self, url: str, data: dict, files: dict, timeout: tuple[float, float]) -> FakeResponse:
        del data, files, timeout
        assert url.endswith("/tasks")
        return FakeResponse(202, {"task_id": "task-1"})


def test_mineru_converter_returns_markdown_and_metadata(tmp_path: Path) -> None:
    pdf_path = tmp_path / "report.pdf"
    pdf_path.write_bytes(b"%PDF-1.4 test")
    settings = MinerUSettings(
        api_url="http://mineru.test",
        backend="hybrid-engine",
        effort="medium",
        parse_method="auto",
        poll_interval_seconds=0,
        task_timeout_seconds=10,
        request_timeout_seconds=5,
    )

    result = PDFMarkdownConverter(settings=settings, session=FakeSession()).convert_with_details(
        pdf_path,
        profile="financial_report",
    )

    assert result.engine == "mineru"
    assert result.engine_version == "3.3.1"
    assert "|收入|100|" in result.markdown
    assert result.metrics["table_line_count"] == 3
    assert result.metrics["html_table_count"] == 0
    assert result.converter_version == PDF_MARKDOWN_CONVERTER_VERSION


def test_cache_requires_current_mineru_signature(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("MINERU_API_URL", "http://mineru.test")
    monkeypatch.setenv("MINERU_BACKEND", "hybrid-engine")
    monkeypatch.setenv("MINERU_EFFORT", "medium")
    monkeypatch.setenv("MINERU_PARSE_METHOD", "auto")

    pdf_path = tmp_path / "report.pdf"
    md_path = tmp_path / "report.md"
    pdf_path.write_bytes(b"%PDF-1.4 test")
    settings = MinerUSettings.from_env()
    result = PDFMarkdownConverter(settings=settings, session=FakeSession()).convert_with_details(pdf_path)
    write_pdf_conversion_artifacts(md_path, result, pdf_path)

    assert is_pdf_markdown_cache_current(md_path, pdf_path)

    metadata_path = md_path.with_name(f"{md_path.name}.meta.json")
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    metadata["converter_signature"] = "mineru:pipeline:medium:auto:financial_report"
    metadata_path.write_text(json.dumps(metadata), encoding="utf-8")

    assert not is_pdf_markdown_cache_current(md_path, pdf_path)
