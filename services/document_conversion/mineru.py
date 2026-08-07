"""Convert PDF documents to Markdown through a MinerU HTTP service."""

from __future__ import annotations

import json
import os
import time
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import requests

from core.logging import get_logger


LOGGER = get_logger("DocumentConversion")

PDF_MARKDOWN_CONVERTER_VERSION = 4
DEFAULT_PDF_CONVERSION_PROFILE = "financial_report"
SUPPORTED_PDF_CONVERSION_PROFILES = {"financial_report", "general"}
MINERU_API_PROTOCOL_VERSION = 2


def _format_page_progress(processed_pages: int, total_pages: int, width: int = 20) -> str:
    if total_pages <= 0:
        return ""
    processed = min(max(processed_pages, 0), total_pages)
    ratio = processed / total_pages
    filled = min(width, int(ratio * width))
    bar = "█" * filled + "░" * (width - filled)
    return f"进度 [{bar}] {processed}/{total_pages}页 ({ratio * 100:.1f}%)"


class MinerUConversionError(RuntimeError):
    """Raised when MinerU cannot complete a document conversion."""


@dataclass(frozen=True)
class MinerUSettings:
    api_url: str
    backend: str
    effort: str
    parse_method: str
    poll_interval_seconds: float
    task_timeout_seconds: float
    request_timeout_seconds: float

    @classmethod
    def from_env(cls) -> "MinerUSettings":
        return cls(
            api_url=os.getenv("MINERU_API_URL", "http://127.0.0.1:8000").rstrip("/"),
            backend=os.getenv("MINERU_BACKEND", "hybrid-engine"),
            effort=os.getenv("MINERU_EFFORT", "medium"),
            parse_method=os.getenv("MINERU_PARSE_METHOD", "auto"),
            poll_interval_seconds=float(os.getenv("MINERU_POLL_INTERVAL_SECONDS", "2")),
            task_timeout_seconds=float(os.getenv("MINERU_TASK_TIMEOUT_SECONDS", "3600")),
            request_timeout_seconds=float(os.getenv("MINERU_REQUEST_TIMEOUT_SECONDS", "60")),
        )

    def signature(self, profile: str) -> str:
        return ":".join(
            (
                "mineru",
                self.backend,
                self.effort,
                self.parse_method,
                profile,
            )
        )


@dataclass
class PDFConversionResult:
    markdown: str
    profile: str
    strategy: str
    converter_version: int
    converter_signature: str
    engine: str
    engine_version: str | None
    metrics: dict[str, int]
    engine_metadata: dict[str, Any]
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

    settings = MinerUSettings.from_env()
    if metadata.get("converter_version") != PDF_MARKDOWN_CONVERTER_VERSION:
        return False
    if metadata.get("profile") != profile:
        return False
    if metadata.get("converter_signature") != settings.signature(profile):
        return False

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
    """Synchronous adapter over MinerU's asynchronous task API."""

    def __init__(
        self,
        settings: MinerUSettings | None = None,
        session: requests.Session | None = None,
    ) -> None:
        self.settings = settings or MinerUSettings.from_env()
        self.session = session or requests.Session()

    def _request_timeout(self) -> tuple[float, float]:
        return (10.0, self.settings.request_timeout_seconds)

    @staticmethod
    def _count_pdf_pages(pdf_path: Path) -> int:
        try:
            import pypdf
            reader = pypdf.PdfReader(str(pdf_path))
            return len(reader.pages)
        except Exception:
            return 0

    def _health(self) -> dict[str, Any]:
        try:
            response = self.session.get(
                f"{self.settings.api_url}/health",
                timeout=self._request_timeout(),
            )
            response.raise_for_status()
            payload = response.json()
        except (requests.RequestException, ValueError) as exc:
            raise MinerUConversionError(
                f"无法连接 MinerU API {self.settings.api_url}。"
                "请先运行 scripts/start_mineru_api.sh，并检查 /health。"
            ) from exc

        protocol_version = payload.get("protocol_version")
        if payload.get("status") != "healthy":
            raise MinerUConversionError(f"MinerU API 状态异常: {payload}")
        if not isinstance(protocol_version, int) or protocol_version < MINERU_API_PROTOCOL_VERSION:
            raise MinerUConversionError(
                f"MinerU API 协议版本过低: {protocol_version}，"
                f"最低需要 {MINERU_API_PROTOCOL_VERSION}"
            )
        return payload

    def _submit_task(self, pdf_path: Path) -> dict[str, Any]:
        form_data = {
            "lang_list": "ch",
            "backend": self.settings.backend,
            "effort": self.settings.effort,
            "parse_method": self.settings.parse_method,
            "formula_enable": "true",
            "table_enable": "true",
            "image_analysis": "false",
            "return_md": "true",
            "return_middle_json": "false",
            "return_model_output": "false",
            "return_content_list": "false",
            "return_images": "false",
            "response_format_zip": "false",
            "return_original_file": "false",
        }
        try:
            with pdf_path.open("rb") as pdf_file:
                response = self.session.post(
                    f"{self.settings.api_url}/tasks",
                    data=form_data,
                    files={"files": (pdf_path.name, pdf_file, "application/pdf")},
                    timeout=self._request_timeout(),
                )
            response.raise_for_status()
            payload = response.json()
        except (OSError, requests.RequestException, ValueError) as exc:
            raise MinerUConversionError(f"提交 MinerU 解析任务失败: {pdf_path}") from exc

        if response.status_code != 202 or not payload.get("task_id"):
            raise MinerUConversionError(f"MinerU 返回了无效任务响应: {payload}")
        return payload

    def _wait_for_task(self, task_id: str, total_pages: int = 0) -> dict[str, Any]:
        deadline = time.monotonic() + self.settings.task_timeout_seconds
        status_url = f"{self.settings.api_url}/tasks/{task_id}"
        last_log = 0.0
        last_progress_pages: int | None = None

        while time.monotonic() < deadline:
            now = time.monotonic()
            try:
                response = self.session.get(status_url, timeout=self._request_timeout())
                response.raise_for_status()
                payload = response.json()
            except (requests.RequestException, ValueError) as exc:
                raise MinerUConversionError(f"查询 MinerU 任务失败: {task_id}") from exc

            status = payload.get("status")
            if status == "completed":
                return payload
            if status == "failed":
                raise MinerUConversionError(
                    f"MinerU 任务失败: {payload.get('error') or payload}"
                )
            if status not in {"pending", "processing"}:
                raise MinerUConversionError(f"MinerU 返回未知任务状态: {payload}")

            server_total_pages = payload.get("total_pages")
            server_processed_pages = payload.get("processed_pages")
            effective_total_pages = (
                server_total_pages
                if isinstance(server_total_pages, int) and server_total_pages > 0
                else total_pages
            )
            processed_pages = (
                server_processed_pages
                if isinstance(server_processed_pages, int) and server_processed_pages >= 0
                else None
            )
            progress_changed = (
                processed_pages is not None
                and processed_pages != last_progress_pages
            )
            if progress_changed or now - last_log >= 10:
                started = payload.get("started_at")
                elapsed_str = ""
                if started:
                    try:
                        from datetime import datetime, timezone
                        started_dt = datetime.fromisoformat(started)
                        elapsed = (datetime.now(timezone.utc) - started_dt).total_seconds()
                        elapsed_str = f", 已处理 {elapsed:.0f}s"
                    except Exception:
                        pass
                progress_info = ""
                if processed_pages is not None and effective_total_pages > 0:
                    progress_info = ", " + _format_page_progress(
                        processed_pages,
                        effective_total_pages,
                    )
                elif effective_total_pages > 0:
                    progress_info = f", 共{effective_total_pages}页"
                LOGGER.info(
                    "MinerU 转换进行中: task_id=%s, status=%s%s%s",
                    task_id,
                    status,
                    progress_info,
                    elapsed_str,
                )
                last_log = now
                last_progress_pages = processed_pages

            time.sleep(self.settings.poll_interval_seconds)

        raise MinerUConversionError(
            f"MinerU 任务等待超时（{self.settings.task_timeout_seconds:.0f} 秒）: {task_id}"
        )

    def _fetch_result(self, task_id: str) -> dict[str, Any]:
        try:
            response = self.session.get(
                f"{self.settings.api_url}/tasks/{task_id}/result",
                timeout=self._request_timeout(),
            )
            response.raise_for_status()
            payload = response.json()
        except (requests.RequestException, ValueError) as exc:
            raise MinerUConversionError(f"获取 MinerU 任务结果失败: {task_id}") from exc
        return payload

    @staticmethod
    def _extract_markdown(payload: dict[str, Any]) -> str:
        results = payload.get("results")
        if not isinstance(results, dict) or not results:
            raise MinerUConversionError(f"MinerU 结果缺少 results: {payload}")

        first_result = next(iter(results.values()))
        if not isinstance(first_result, dict):
            raise MinerUConversionError(f"MinerU 文档结果格式异常: {first_result}")
        markdown = first_result.get("md_content")
        if not isinstance(markdown, str) or not markdown.strip():
            raise MinerUConversionError("MinerU 没有返回有效 Markdown")
        return markdown

    @staticmethod
    def _metrics(markdown: str) -> dict[str, int]:
        return {
            "char_count": len(markdown),
            "line_count": markdown.count("\n") + 1,
            "table_line_count": sum(
                1
                for line in markdown.splitlines()
                if line.strip().startswith("|") and line.strip().endswith("|")
            ),
            "html_table_count": markdown.lower().count("<table"),
        }

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

        start_time = time.monotonic()
        total_pages = self._count_pdf_pages(pdf_path)
        page_info = f" ({total_pages}页)" if total_pages > 0 else ""
        health = self._health()
        task = self._submit_task(pdf_path)
        task_id = str(task["task_id"])
        LOGGER.info(
            "已提交 MinerU 任务: task_id=%s, backend=%s, effort=%s, pdf=%s%s",
            task_id,
            self.settings.backend,
            self.settings.effort,
            pdf_path,
            page_info,
        )
        status_payload = self._wait_for_task(task_id, total_pages)
        result_payload = self._fetch_result(task_id)
        markdown = self._extract_markdown(result_payload)
        elapsed_seconds = time.monotonic() - start_time

        result = PDFConversionResult(
            markdown=markdown,
            profile=profile,
            strategy=f"mineru:{self.settings.backend}:{self.settings.effort}",
            converter_version=PDF_MARKDOWN_CONVERTER_VERSION,
            converter_signature=self.settings.signature(profile),
            engine="mineru",
            engine_version=str(result_payload.get("version") or health.get("version") or "") or None,
            metrics=self._metrics(markdown),
            engine_metadata={
                "api_url": self.settings.api_url,
                "backend": result_payload.get("backend") or self.settings.backend,
                "effort": self.settings.effort,
                "parse_method": self.settings.parse_method,
                "task_id": task_id,
                "created_at": status_payload.get("created_at"),
                "started_at": status_payload.get("started_at"),
                "completed_at": status_payload.get("completed_at"),
                "elapsed_seconds": round(elapsed_seconds, 3),
            },
            generated_at=datetime.now().isoformat(timespec="seconds"),
        )

        if output_dir:
            output_path = Path(output_dir) / f"{pdf_path.stem}.md"
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_text(markdown, encoding="utf-8")
            LOGGER.info("Markdown 已保存到: %s", output_path)

        LOGGER.info(
            "MinerU 转换完成: task_id=%s, chars=%s, elapsed=%.2fs%s",
            task_id,
            result.metrics["char_count"],
            elapsed_seconds,
            page_info,
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
