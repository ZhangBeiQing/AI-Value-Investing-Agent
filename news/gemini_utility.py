import json
import os
import re
import filetype
import time
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from statistics import median
from typing import Any

from core.logging import get_logger
from IPython.display import display, HTML, Markdown
from marker.converters.pdf import PdfConverter
from marker.models import create_model_dict
from marker.providers.pdf import PdfProvider
from surya.settings import settings as surya_settings

LOGGER = get_logger("PDFMarkdownConverter")

PDF_MARKDOWN_CONVERTER_VERSION = 3
DEFAULT_PDF_CONVERSION_PROFILE = "financial_report"
SUPPORTED_PDF_CONVERSION_PROFILES = {"financial_report", "general"}
NUMERIC_GROUP_RE = re.compile(r"\d{1,3}(?:,\d{3})+(?:\.\d+)?")
CJK_CHAR_RE = re.compile(r"[\u4e00-\u9fff]")
PURE_NUMERIC_CELL_RE = re.compile(r"^(?:--|-?\d+(?:,\d{3})*(?:\.\d+)?%?)$")
SECTION_HEADER_RE = re.compile(r"^[（(]?[一二三四五六七八九十]+[)）、.]")


@dataclass
class PDFConversionResult:
    markdown: str
    profile: str
    strategy: str
    converter_version: int
    marker_metrics: dict[str, int]
    provider_metrics: dict[str, int] | None
    marker_metadata: dict[str, Any]
    generated_at: str

    def to_metadata(self, source_pdf: str | Path | None = None) -> dict[str, Any]:
        """把本次转换结果整理成 sidecar 元数据。

        目的：
        1. 让后续脚本能知道当前 markdown 是用哪种策略生成的。
        2. 记录源 PDF 的大小和修改时间，用于判断缓存是否过期。
        3. 避免财报总结、公告同步等流程长期复用旧版本的低质量 md。
        """
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
    """返回 markdown 对应的 sidecar 元数据路径。

    目的：
    当前实现会把正文 markdown 和转换元信息分开存储，避免把转换策略、
    源 PDF 指纹等控制信息直接塞进正文里污染后续 prompt。
    """
    path = Path(markdown_path)
    return path.with_name(f"{path.name}.meta.json")


def is_pdf_markdown_cache_current(
    markdown_path: str | Path,
    source_pdf: str | Path,
    *,
    profile: str = DEFAULT_PDF_CONVERSION_PROFILE,
) -> bool:
    """判断已有 markdown 缓存是否仍然可用。

    目的：
    1. 只要转换器版本、profile、源 PDF 文件指纹任一项变化，就触发重建。
    2. 避免“旧逻辑产出的 md”在新逻辑上线后被继续静默复用。
    3. 给财报总结 skill 和 disclosures 缓存一套统一的失效规则。
    """
    md_path = Path(markdown_path)
    pdf_path = Path(source_pdf)
    meta_path = pdf_markdown_metadata_path(md_path)
    if not md_path.exists() or md_path.stat().st_size <= 0:
        return False
    if not pdf_path.exists() or not meta_path.exists():
        return False

    try:
        metadata = json.loads(meta_path.read_text(encoding="utf-8"))
    except Exception:
        return False

    if metadata.get("converter_version") != PDF_MARKDOWN_CONVERTER_VERSION:
        return False
    if metadata.get("profile") != profile:
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
    """落盘 markdown 正文与 sidecar 元数据。

    目的：
    保证所有入口脚本写出的产物格式一致：
    - `xxx.md` 保存正文
    - `xxx.md.meta.json` 保存策略、版本、源文件指纹
    这样后续任何流程都能统一判断缓存质量和新旧版本。
    """
    md_path = Path(markdown_path)
    md_path.parent.mkdir(parents=True, exist_ok=True)
    md_path.write_text(result.markdown, encoding="utf-8")
    meta_path = pdf_markdown_metadata_path(md_path)
    meta_path.write_text(
        json.dumps(result.to_metadata(source_pdf), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

# 创建一个单例模式的PDF转Markdown转换器类
class PDFMarkdownConverter:
    _instance = None
    _models_loaded = False
    _model_dict = None
    
    def __new__(cls):
        if cls._instance is None:
            LOGGER.info("初始化 PDF 转 Markdown 转换器")
            cls._instance = super(PDFMarkdownConverter, cls).__new__(cls)
            cls._instance.initialized = False
        return cls._instance
        
    def __init__(self):
        if not self.initialized:
            LOGGER.info("检查 marker 模型缓存")

            cache_dir = Path(surya_settings.MODEL_CACHE_DIR)
            models_exist = self._check_models_exist(cache_dir)

            if models_exist:
                LOGGER.info("发现已缓存模型，快速加载: %s", cache_dir)
            else:
                LOGGER.info("模型缓存不完整，将按需下载缺失文件: %s", cache_dir)

            LOGGER.info("开始加载模型")
            start_time = time.time()

            if not PDFMarkdownConverter._models_loaded:
                PDFMarkdownConverter._model_dict = create_model_dict()
                PDFMarkdownConverter._models_loaded = True

            self.converter = PdfConverter(
                artifact_dict=PDFMarkdownConverter._model_dict,
                config={"output_format": "markdown"}
            )

            load_time = time.time() - start_time
            LOGGER.info("模型加载完成，耗时 %.2f 秒", load_time)
            self.initialized = True

    def _required_model_dirs(self, cache_dir: Path) -> list[Path]:
        """列出 marker/surya 运行所需的模型目录。

        目的：
        在真正执行转换前，先做一次本地缓存完整性检查。
        这样日志里能直接说明当前是“纯本地加载”还是“可能需要补下载模型”。
        """
        checkpoints = [
            surya_settings.DETECTOR_MODEL_CHECKPOINT,
            surya_settings.RECOGNITION_MODEL_CHECKPOINT,
            surya_settings.LAYOUT_MODEL_CHECKPOINT,
            surya_settings.TABLE_REC_MODEL_CHECKPOINT,
            surya_settings.OCR_ERROR_MODEL_CHECKPOINT,
        ]
        required_dirs: list[Path] = []
        for checkpoint in checkpoints:
            relative = checkpoint.replace("s3://", "", 1).strip("/")
            required_dirs.append(cache_dir / relative)
        return required_dirs

    def _check_models_exist(self, cache_dir: Path) -> bool:
        """检查 marker/surya 所需模型是否已完整缓存。"""
        if not cache_dir.exists():
            return False

        missing_paths: list[Path] = []
        for model_dir in self._required_model_dirs(cache_dir):
            manifest_path = model_dir / "manifest.json"
            if not model_dir.exists() or not manifest_path.exists():
                missing_paths.append(model_dir)
                continue

            try:
                manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
                expected_files = manifest.get("files") or []
                if not expected_files:
                    missing_paths.append(model_dir)
                    continue
                for filename in expected_files:
                    if not (model_dir / filename).exists():
                        missing_paths.append(model_dir / filename)
            except Exception:
                missing_paths.append(manifest_path)

        if missing_paths:
            LOGGER.info("缺失模型文件: %s", ", ".join(str(path) for path in missing_paths))
            return False

        return True

    def _convert_with_marker(self, file_path: str) -> tuple[str, dict]:
        """执行 marker 默认整页转换，并拿到原始 markdown 与 metadata。

        目的：
        这个结果仍然是基线输出，用来和后面的 provider 重建结果做质量对比。
        如果 provider 重建失败，最终也会安全回退到这里。
        """
        rendered = self.converter(file_path)
        markdown = getattr(rendered, "markdown", "")
        metadata = getattr(rendered, "metadata", {}) or {}
        return markdown, metadata

    def _is_numeric_cell(self, text: str) -> bool:
        """判断一个单元片段是否近似“纯数值单元格”。

        目的：
        后续重建 Markdown 表格时，需要区分“项目名/说明文本”和“金额/比例”。
        这里的判断直接影响列锚点估计、表头推断和跨行合并策略。
        """
        compact = text.replace(" ", "")
        return bool(PURE_NUMERIC_CELL_RE.match(compact))

    def _make_provider_cells(self, provider_line, gap_threshold: float = 8.0) -> list[dict[str, Any]]:
        """把一条 provider 文本行切成若干“候选表格单元”。

        目的：
        `PdfProvider` 提供的是带坐标的 spans，不是现成的表格。
        这里先基于横向间距把同一行拆成更接近单元格的片段，为后续表格重建
        提供最小结构单元。
        """
        groups: list[list[tuple[float, float, str]]] = []
        current_group: list[tuple[float, float, str]] = []
        prev_x1: float | None = None

        for span in provider_line.spans:
            text = span.text.replace("\n", "").strip()
            if not text:
                continue
            x0, _, x1, _ = span.polygon.bbox
            if current_group and prev_x1 is not None and x0 - prev_x1 > gap_threshold:
                groups.append(current_group)
                current_group = []
            current_group.append((x0, x1, text))
            prev_x1 = x1

        if current_group:
            groups.append(current_group)

        cells: list[dict[str, Any]] = []
        for group in groups:
            text = " ".join(item[2] for item in group).strip()
            if not text:
                continue
            cells.append(
                {
                    "x0": float(group[0][0]),
                    "x1": float(group[-1][1]),
                    "text": text,
                    "numeric": self._is_numeric_cell(text),
                }
            )
        return cells

    def _cluster_values(self, values: list[float], threshold: float = 35.0) -> list[float]:
        """把一组相近坐标聚成列锚点。

        目的：
        财报表格里的列并不会严格对齐到同一个 x 值，特别是不同位数的金额、
        百分比、换行后的说明文字都会有轻微偏移。这里通过聚类得到更稳的列中心，
        避免把同一列误拆成两列。
        """
        if not values:
            return []

        clusters: list[list[float]] = []
        for value in sorted(values):
            if not clusters or value - clusters[-1][-1] > threshold:
                clusters.append([value])
            else:
                clusters[-1].append(value)
        return [float(median(cluster)) for cluster in clusters]

    def _build_provider_rows(self, provider: PdfProvider, page_id: int) -> list[dict[str, Any]]:
        """把指定页面的 provider 输出整理成统一的“行结构”。

        目的：
        后面的表块检测、竖排公告表重建、跨页合并都基于这个中间结构工作。
        这里统一准备：
        - 原始文本
        - 行坐标
        - 拆分后的 cells
        - 该行包含多少数值单元
        """
        rows: list[dict[str, Any]] = []
        for provider_line in provider.page_lines.get(page_id, []):
            raw_text = "".join(span.text for span in provider_line.spans).replace("\n", "").strip()
            if not raw_text:
                continue

            x0, y0, x1, y1 = provider_line.line.polygon.bbox
            cells = self._make_provider_cells(provider_line)
            rows.append(
                {
                    "text": raw_text,
                    "x0": float(x0),
                    "x1": float(x1),
                    "y0": float(y0),
                    "y1": float(y1),
                    "cells": cells,
                    "group_count": len(cells),
                    "numeric_cell_count": sum(1 for cell in cells if cell["numeric"]),
                }
            )
        return rows

    def _render_plain_rows(self, rows: list[dict[str, Any]]) -> str:
        """把未识别为表格的行按普通正文输出。

        目的：
        不是所有内容都应该被强行表格化。章节标题、说明段落、风险提示等内容
        仍然更适合普通 markdown 文本。这里也顺手过滤单独页码，减少噪音。
        """
        lines: list[str] = []
        previous_blank = False
        for row in rows:
            text = row["text"].strip()
            if re.fullmatch(r"\d{1,3}", text):
                continue
            if not text:
                if lines and not previous_blank:
                    lines.append("")
                previous_blank = True
                continue
            lines.append(text)
            previous_blank = False
        return "\n".join(lines).strip()

    def _looks_like_table_start(self, row: dict[str, Any], following_rows: list[dict[str, Any]]) -> bool:
        """判断某一行是否像一个表格块的起点。

        目的：
        默认 marker 在中文财报里经常识别不出 Table 块，所以这里做二次检测。
        这个函数同时覆盖两类场景：
        1. 正常财务报表的横向多列表。
        2. 业绩预告那种“项目/本报告期/上年同期”的竖排公告表。
        """
        if self._looks_like_vertical_table_header(row):
            return True
        if row["numeric_cell_count"] >= 2 and row["group_count"] >= 3:
            return True
        if row["group_count"] < 2:
            return False
        if SECTION_HEADER_RE.match(row["text"]):
            return False
        return sum(1 for item in following_rows if item["numeric_cell_count"] >= 2) >= 1

    def _looks_like_vertical_table_header(self, row: dict[str, Any]) -> bool:
        """识别“公告竖排表”的表头行。

        目的：
        业绩预告、业绩快报这类公告常见的表格并不是财报正文那种横向密集表，
        而是左边项目名竖排、右边两列数据分多行展开。默认规则很容易漏掉，
        这里单独给这类表一个入口。
        """
        text = row["text"]
        return (
            row["group_count"] >= 3
            and "项目" in text
            and ("本报告期" in text or "本期" in text or "本年" in text)
            and ("上年同期" in text or "期末余额" in text or "年初余额" in text or "上期" in text)
        )

    def _detect_table_blocks(self, rows: list[dict[str, Any]]) -> list[tuple[int, int]]:
        """在页面行序列中切出疑似表格块。

        目的：
        只依赖 provider 行坐标重新识别表块，替代默认 layout 对中文财报表格
        不稳定的 Table 检测。这里还专门放宽了竖排公告表的行间距阈值，避免
        “项目行和数值行之间隔得稍远”就把表切断。
        """
        blocks: list[tuple[int, int]] = []
        idx = 0
        while idx < len(rows):
            row = rows[idx]
            if not self._looks_like_table_start(row, rows[idx + 1 : idx + 5]):
                idx += 1
                continue

            start_idx = idx
            end_idx = idx
            data_seen = row["numeric_cell_count"] >= 2
            empty_non_numeric_streak = 0
            vertical_table = self._looks_like_vertical_table_header(row)

            while end_idx + 1 < len(rows):
                next_row = rows[end_idx + 1]
                vertical_gap = next_row["y0"] - rows[end_idx]["y1"]
                max_vertical_gap = 40 if vertical_table else 24
                if vertical_gap > max_vertical_gap:
                    break
                if data_seen and SECTION_HEADER_RE.match(next_row["text"]):
                    break

                if next_row["numeric_cell_count"] >= 2:
                    data_seen = True
                    empty_non_numeric_streak = 0
                    end_idx += 1
                    continue

                if vertical_table and next_row["group_count"] >= 1:
                    if any(cell["x0"] - row["x0"] > 60 for cell in next_row["cells"]):
                        data_seen = True
                    end_idx += 1
                    continue

                if next_row["group_count"] >= 2 and not data_seen:
                    end_idx += 1
                    continue

                if next_row["group_count"] == 1 and next_row["numeric_cell_count"] == 0:
                    if data_seen:
                        empty_non_numeric_streak += 1
                        if empty_non_numeric_streak >= 2:
                            break
                    end_idx += 1
                    continue

                if next_row["group_count"] >= 1:
                    end_idx += 1
                    continue
                break

            block_rows = rows[start_idx : end_idx + 1]
            numeric_row_count = sum(1 for item in block_rows if item["numeric_cell_count"] >= 2)
            if vertical_table and len(block_rows) >= 5:
                blocks.append((start_idx, end_idx))
                idx = end_idx + 1
                continue

            if numeric_row_count >= 2 and len(block_rows) >= 3:
                last_data_idx = max(
                    index for index, item in enumerate(block_rows) if item["numeric_cell_count"] >= 2
                )
                trimmed_end_idx = start_idx + last_data_idx
                blocks.append((start_idx, trimmed_end_idx))
                idx = trimmed_end_idx + 1
                continue

            idx += 1
        return blocks

    def _render_table_block(self, rows: list[dict[str, Any]]) -> str | None:
        """把一段表块行渲染成 Markdown 表格。

        目的：
        这是普通财报横向表格的主渲染入口。如果检测到其实是公告竖排表，
        会自动切换到 `_render_vertical_table_block()`；否则走通用的列锚点推断、
        续行合并、表头生成逻辑。
        """
        if not rows:
            return None

        if self._looks_like_vertical_table_header(rows[0]):
            vertical_markdown = self._render_vertical_table_block(rows)
            if vertical_markdown:
                return vertical_markdown

        row_cells = [row["cells"] for row in rows]
        numeric_rows = [cells for cells in row_cells if sum(1 for cell in cells if cell["numeric"]) >= 2]
        if len(numeric_rows) < 2:
            return None

        max_groups = max(len(cells) for cells in numeric_rows)
        max_numeric = max(sum(1 for cell in cells if cell["numeric"]) for cells in numeric_rows)
        representative_rows = [
            cells
            for cells in numeric_rows
            if len(cells) == max_groups and sum(1 for cell in cells if cell["numeric"]) >= max_numeric - 1
        ]
        if not representative_rows:
            representative_rows = numeric_rows

        anchor_values: list[float] = []
        for cells in representative_rows:
            for cell in cells:
                anchor_values.append(cell["x1"] if cell["numeric"] else cell["x0"])

        anchors = self._cluster_values(anchor_values)
        if not anchors:
            return None

        additional_text_anchors = self._cluster_values(
            [
                cell["x0"]
                for cells in row_cells
                for cell in cells
                if not cell["numeric"] and cell["x0"] > anchors[-1] - 5
            ]
        )
        for anchor in additional_text_anchors:
            if all(abs(anchor - existing) > 25 for existing in anchors):
                anchors.append(anchor)
        anchors = sorted(anchors)

        mapped_rows: list[list[str]] = []
        for cells in row_cells:
            mapped = ["" for _ in anchors]
            for cell in cells:
                key = cell["x1"] if cell["numeric"] else cell["x0"]
                column_idx = min(range(len(anchors)), key=lambda idx: abs(anchors[idx] - key))
                mapped[column_idx] = (
                    f"{mapped[column_idx]} {cell['text']}".strip() if mapped[column_idx] else cell["text"]
                )
            mapped_rows.append(mapped)

        normalized_rows: list[list[str]] = []
        pending_first_col_lines: list[str] = []
        for mapped in mapped_rows:
            non_empty_cols = [idx for idx, value in enumerate(mapped) if value]
            numeric_value_count = sum(1 for value in mapped if self._is_numeric_cell(value))

            if non_empty_cols == [0] and numeric_value_count == 0:
                pending_first_col_lines.append(mapped[0])
                continue

            if numeric_value_count >= 2:
                if pending_first_col_lines:
                    prefix = "<br>".join(pending_first_col_lines)
                    mapped[0] = f"{prefix}<br>{mapped[0]}".strip("<br>") if mapped[0] else prefix
                    pending_first_col_lines = []
                normalized_rows.append(mapped)
                continue

            if normalized_rows and len(non_empty_cols) == 1 and non_empty_cols[0] != 0:
                col_idx = non_empty_cols[0]
                normalized_rows[-1][col_idx] = (
                    f"{normalized_rows[-1][col_idx]}<br>{mapped[col_idx]}".strip("<br>")
                    if normalized_rows[-1][col_idx]
                    else mapped[col_idx]
                )
                continue

            if pending_first_col_lines:
                pending_row = ["" for _ in anchors]
                pending_row[0] = "<br>".join(pending_first_col_lines)
                normalized_rows.append(pending_row)
                pending_first_col_lines = []
            normalized_rows.append(mapped)

        if pending_first_col_lines:
            pending_row = ["" for _ in anchors]
            pending_row[0] = "<br>".join(pending_first_col_lines)
            normalized_rows.append(pending_row)

        first_data_idx = 0
        for idx, mapped in enumerate(normalized_rows):
            if sum(1 for value in mapped if self._is_numeric_cell(value)) >= 2:
                first_data_idx = idx
                break

        header = ["" for _ in anchors]
        for mapped in normalized_rows[:first_data_idx]:
            for idx, value in enumerate(mapped):
                if not value:
                    continue
                header[idx] = f"{header[idx]}<br>{value}".strip("<br>") if header[idx] else value
        if not any(header):
            header = [f"列{idx + 1}" for idx in range(len(anchors))]
        elif not header[0]:
            header[0] = "项目"

        data_rows = normalized_rows[first_data_idx:]
        if not data_rows:
            return None

        lines = [
            "|" + "|".join(header) + "|",
            "|" + "|".join(["---"] * len(anchors)) + "|",
        ]
        for mapped in data_rows:
            lines.append("|" + "|".join(mapped) + "|")
        return "\n".join(lines)

    def _append_md_cell(self, original: str, addition: str) -> str:
        """把新增文本追加到 markdown 单元格，使用 `<br>` 保留换行语义。

        目的：
        财报表里大量项目名和说明都是跨多行排版的。直接拼空格会丢结构，
        直接换行又会破坏 markdown 表格，所以统一用 `<br>`。
        """
        addition = addition.strip()
        if not addition:
            return original
        return f"{original}<br>{addition}".strip("<br>") if original else addition

    def _render_vertical_table_block(self, rows: list[dict[str, Any]]) -> str | None:
        """把“公告竖排表”重建成三列表 Markdown。

        目的：
        专门解决业绩预告/业绩快报里常见的这种结构：
        - 第一列是项目名，且经常竖着分多行
        - 第二列是本报告期数值和同比
        - 第三列是上年同期数值
        默认 marker 很难直接产出标准表格，这里单独重建。
        """
        if not rows:
            return None

        header_cells = rows[0]["cells"]
        if len(header_cells) < 3:
            return None

        header_cells = sorted(header_cells, key=lambda cell: cell["x0"])[:3]
        header = [cell["text"] for cell in header_cells]

        body_x0_values = [
            cell["x0"]
            for row in rows[1:]
            for cell in row["cells"]
        ]
        body_anchors = self._cluster_values(body_x0_values, threshold=60.0)
        if len(body_anchors) >= 3:
            anchors = body_anchors[:3]
        else:
            anchors = [cell["x0"] for cell in header_cells]

        data_rows: list[list[str]] = []
        pending_label_lines: list[str] = []
        current_row: list[str] | None = None

        def flush_current() -> None:
            nonlocal current_row
            if current_row is None:
                return
            if any(value.strip() for value in current_row):
                data_rows.append(current_row)
            current_row = None

        for row in rows[1:]:
            mapped = ["", "", ""]
            for cell in row["cells"]:
                column_idx = min(range(3), key=lambda idx: abs(anchors[idx] - cell["x0"]))
                mapped[column_idx] = self._append_md_cell(mapped[column_idx], cell["text"])

            non_empty_cols = [idx for idx, value in enumerate(mapped) if value]
            if not non_empty_cols:
                continue

            if non_empty_cols == [0]:
                if current_row is not None:
                    flush_current()
                pending_label_lines.append(mapped[0])
                continue

            if current_row is None:
                label = ""
                if pending_label_lines:
                    label = "<br>".join(pending_label_lines)
                    pending_label_lines = []
                elif mapped[0]:
                    label = mapped[0]
                current_row = [label, "", ""]
            elif mapped[0]:
                flush_current()
                label = mapped[0]
                if pending_label_lines:
                    label = "<br>".join(pending_label_lines + [label])
                    pending_label_lines = []
                current_row = [label, "", ""]

            if mapped[1]:
                current_row[1] = self._append_md_cell(current_row[1], mapped[1])
            if mapped[2]:
                current_row[2] = self._append_md_cell(current_row[2], mapped[2])

        flush_current()
        if not data_rows:
            return None

        lines = [
            "|" + "|".join(header) + "|",
            "|" + "|".join(["---"] * 3) + "|",
        ]
        for row in data_rows:
            lines.append("|" + "|".join(row) + "|")
        return "\n".join(lines)

    def _markdown_table_to_matrix(self, table_markdown: str) -> tuple[list[str], list[list[str]]] | None:
        """把 markdown 表格解析成 header + rows 矩阵。

        目的：
        渲染完成后还需要继续做列裁剪、表头归一化、同页/跨页合并。
        这些操作在矩阵结构上做更简单，所以先把表格文本还原成二维结构。
        """
        lines = [line.strip() for line in table_markdown.splitlines() if line.strip()]
        if len(lines) < 2 or not lines[0].startswith("|") or not lines[1].startswith("|"):
            return None

        def split_row(row: str) -> list[str]:
            return [cell.strip() for cell in row.strip("|").split("|")]

        header = split_row(lines[0])
        rows = [split_row(line) for line in lines[2:] if line.startswith("|")]
        return header, rows

    def _matrix_to_markdown_table(self, header: list[str], rows: list[list[str]]) -> str:
        """把 header + rows 矩阵重新组装回 markdown 表格。

        目的：
        这是表格后处理阶段的统一出口，保证所有归一化和合并逻辑最终都回到
        一致的 markdown 表格格式。
        """
        lines = [
            "|" + "|".join(header) + "|",
            "|" + "|".join(["---"] * len(header)) + "|",
        ]
        for row in rows:
            padded = row + [""] * max(0, len(header) - len(row))
            lines.append("|" + "|".join(padded[: len(header)]) + "|")
        return "\n".join(lines)

    def _is_table_markdown(self, content: str) -> bool:
        """判断一段文本是否已经是 markdown 表格。

        目的：
        在分段合并时快速区分普通文本和表格，避免把正文按表格逻辑处理。
        """
        stripped = content.strip()
        return stripped.startswith("|") and "\n|---" in stripped

    def _is_generic_header(self, header: list[str]) -> bool:
        """判断表头是否是缺乏语义的占位表头。

        目的：
        当表格重建失败一部分时，header 经常会退化成 `列1/列2/列3` 或空列。
        后面会根据财务报表上下文把它替换成更合理的财务语义表头。
        """
        return all((not cell) or cell.startswith("列") for cell in header)

    def _trim_sparse_columns(self, header: list[str], rows: list[list[str]]) -> tuple[list[str], list[list[str]]]:
        """裁掉几乎没有有效内容的稀疏列。

        目的：
        provider 坐标估列时，有时会多估出一些空列或半空列。
        如果不裁掉，最终 markdown 表会出现很多毫无意义的空白列。
        """
        if not header:
            return header, rows

        keep_indices: list[int] = []
        row_count = max(1, len(rows))
        for idx, cell in enumerate(header):
            non_empty = sum(1 for row in rows if idx < len(row) and row[idx].strip())
            if cell.strip() or non_empty / row_count > 0.2:
                keep_indices.append(idx)

        trimmed_header = [header[idx] for idx in keep_indices]
        trimmed_rows = [[row[idx] if idx < len(row) else "" for idx in keep_indices] for row in rows]
        return trimmed_header, trimmed_rows

    def _compact_sparse_table(self, header: list[str], rows: list[list[str]]) -> tuple[list[str], list[list[str]]]:
        """压缩“列数明显偏多但大多数单元格为空”的表格。

        目的：
        这是为财务报表摘要表准备的兜底逻辑。像“项目 / 本期 / 上期 / 增减”这种表，
        如果锚点估得太散，就会虚增到 4-5 列。这里优先按实际非空单元数量压缩。
        """
        if not rows:
            return header, rows

        max_non_empty = max(sum(1 for cell in row if cell.strip()) for row in rows)
        if max_non_empty >= len(header):
            return header, rows

        compacted_rows: list[list[str]] = []
        for row in rows:
            first_cell = row[0] if row else ""
            remaining = [cell for cell in row[1:] if cell.strip()]
            compacted = ([first_cell] if first_cell.strip() else [""]) + remaining
            compacted += [""] * max(0, max_non_empty - len(compacted))
            compacted_rows.append(compacted[:max_non_empty])

        compacted_header = [header[0] if header else ""]
        compacted_header.extend(cell for cell in header[1:] if cell.strip())
        compacted_header += [""] * max(0, max_non_empty - len(compacted_header))
        return compacted_header[:max_non_empty], compacted_rows

    def _normalized_financial_headers(
        self,
        header: list[str],
        rows: list[list[str]],
        *,
        context_before: str,
    ) -> list[str]:
        """根据上下文把表头归一化成更像财务报表的列名。

        目的：
        财务报表里常见的表头语义其实比较固定，比如：
        - 项目 / 期末余额 / 期初余额
        - 项目 / 本期 / 上期
        - 项目 / 本报告期 / 上年同期 / 增减
        当 provider 重建只拿到模糊 header 时，这里尽量恢复成稳定格式。
        """
        normalized = header[:]
        context = context_before.replace("\n", " ")

        if normalized and not normalized[0].strip():
            normalized[0] = "项目"

        if self._is_generic_header(normalized):
            if "项目" in context and "期末余额" in context and "期初余额" in context and len(normalized) == 3:
                return ["项目", "期末余额", "期初余额"]
            if "项目" in context and "2025 年半年度" in context and "2024 年半年度" in context and len(normalized) == 3:
                return ["项目", "2025 年半年度", "2024 年半年度"]
            if "项目" in context and "2025 年半年度" in context and "2024 年半年度" in context and len(normalized) == 4:
                return ["项目", "2025 年半年度", "2024 年半年度", "备注"]
            if len(normalized) == 3:
                return ["项目", "本期", "上期"]

        joined_header = " ".join(normalized)
        if len(normalized) == 4 and "本报告期 上年同期" in joined_header and "增减" in joined_header:
            return ["项目", "本报告期", "上年同期", "本报告期比上年同期增减"]
        if len(normalized) == 4 and "本报告期" in joined_header and "上年同期" in joined_header:
            return ["项目", "本报告期", "上年同期", "本报告期比上年同期增减"]
        if len(normalized) == 3 and "期末余额" in joined_header and "期初余额" in joined_header:
            return ["项目", "期末余额", "期初余额"]
        if len(normalized) == 3 and "2025 年半年度" in context and "2024 年半年度" in context:
            return ["项目", "2025 年半年度", "2024 年半年度"]
        return normalized

    def _normalize_table_markdown(self, table_markdown: str, *, context_before: str = "") -> str:
        """对单张 markdown 表做结构清洗与财务语义归一化。

        目的：
        渲染出来的原始表格并不是最终交付格式。这里负责统一做：
        1. 稀疏列裁剪
        2. generic header 压缩
        3. 财务报表专用 header 归一化
        """
        parsed = self._markdown_table_to_matrix(table_markdown)
        if parsed is None:
            return table_markdown

        header, rows = parsed
        if self._is_generic_header(header) or any("□" in cell or "☑" in cell for cell in header):
            header, rows = self._compact_sparse_table(header, rows)
        header, rows = self._trim_sparse_columns(header, rows)
        header = self._normalized_financial_headers(header, rows, context_before=context_before)
        return self._matrix_to_markdown_table(header, rows)

    def _table_shapes_match(self, left: str, right: str) -> bool:
        """判断两张表是否足够像同一张表的前后半段。

        目的：
        无论是同页分裂表还是跨页续表，都需要一个保守但实用的“同结构判断”。
        这里主要看列数和表头重叠程度，避免把完全不同的表误拼在一起。
        """
        left_matrix = self._markdown_table_to_matrix(left)
        right_matrix = self._markdown_table_to_matrix(right)
        if left_matrix is None or right_matrix is None:
            return False

        left_header, _ = left_matrix
        right_header, _ = right_matrix
        if len(left_header) != len(right_header):
            return False

        normalized_left = [cell if not cell.startswith("列") else "" for cell in left_header]
        normalized_right = [cell if not cell.startswith("列") else "" for cell in right_header]
        overlap = sum(1 for a, b in zip(normalized_left, normalized_right) if a == b or not a or not b)
        return overlap >= len(left_header) - 1

    def _plain_to_first_col_rows(self, content: str, width: int) -> list[list[str]]:
        """把桥接用的纯文本续行转成“只占第一列”的表格行。

        目的：
        有些表在分页处会夹少量说明行、空标题行、页眉干扰行。为了把它们并回
        同一张表，这里把这类文本映射成第一列续行。
        """
        rows: list[list[str]] = []
        for raw_line in content.splitlines():
            line = raw_line.strip()
            if not line:
                continue
            rows.append([line] + [""] * (width - 1))
        return rows

    def _merge_table_markdown(self, first: str, second: str, *, bridging_plain: str = "") -> str:
        """合并两张同结构 markdown 表。

        目的：
        这是“同页分裂表合并”和“跨页同结构表合并”的底层执行函数。
        如果中间存在少量纯文本桥接内容，也会一起插回表格的第一列。
        """
        first_matrix = self._markdown_table_to_matrix(first)
        second_matrix = self._markdown_table_to_matrix(second)
        if first_matrix is None or second_matrix is None:
            return first if len(first) >= len(second) else second

        header, first_rows = first_matrix
        _, second_rows = second_matrix
        merged_rows = first_rows[:]
        if bridging_plain.strip():
            merged_rows.extend(self._plain_to_first_col_rows(bridging_plain, len(header)))
        merged_rows.extend(second_rows)
        return self._matrix_to_markdown_table(header, merged_rows)

    def _is_ignorable_page_header(self, content: str) -> bool:
        """判断一段纯文本是否只是页眉/页码类噪音。

        目的：
        跨页合并时经常会遇到“上一页表格 -> 页眉 -> 下一页续表”。
        这种中间段落不该阻止表格拼接，所以先单独识别出来。
        """
        lines = [line.strip() for line in content.splitlines() if line.strip()]
        if not lines:
            return True
        if len(lines) == 1 and re.fullmatch(r"\d{1,3}", lines[0]):
            return True
        if len(lines) <= 2 and "报告全文" in " ".join(lines):
            return True
        return False

    def _is_short_table_bridge_plain(self, content: str) -> bool:
        """判断一小段纯文本是否适合作为表格桥接内容。

        目的：
        有些跨页表中间夹的不是纯页眉，而是少量空白项、续行标签、占位项。
        这类文本行如果足够短且数值密度不高，可以安全并回表格。
        """
        lines = [line.strip() for line in content.splitlines() if line.strip()]
        if not lines or len(lines) > 12:
            return False
        if any(SECTION_HEADER_RE.match(line) for line in lines):
            return False
        numeric_rich_lines = sum(1 for line in lines if len(NUMERIC_GROUP_RE.findall(line)) >= 2)
        return numeric_rich_lines <= 2

    def _merge_rendered_segments(self, segments: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """合并渲染后的文本段和表格段。

        目的：
        这是这次大改最核心的“后处理层”，负责三件事：
        1. 同页分裂表合并
        2. 跨页同结构表合并
        3. 邻接纯文本段压缩，减少表格之间无意义的碎片
        """
        if not segments:
            return segments

        merged: list[dict[str, Any]] = []
        idx = 0
        while idx < len(segments):
            current = segments[idx]
            if not merged:
                merged.append(current)
                idx += 1
                continue

            previous = merged[-1]
            if current["type"] == "table" and previous["type"] == "table" and self._table_shapes_match(previous["content"], current["content"]):
                previous["content"] = self._merge_table_markdown(previous["content"], current["content"])
                idx += 1
                continue

            if (
                len(merged) >= 2
                and previous["type"] == "plain"
                and current["type"] == "table"
                and merged[-2]["type"] == "table"
                and previous["page_id"] == current["page_id"] == merged[-2]["page_id"]
                and len([line for line in previous["content"].splitlines() if line.strip()]) <= 12
                and self._table_shapes_match(merged[-2]["content"], current["content"])
            ):
                merged[-2]["content"] = self._merge_table_markdown(
                    merged[-2]["content"],
                    current["content"],
                    bridging_plain=previous["content"],
                )
                merged.pop()
                idx += 1
                continue

            if (
                previous["type"] == "plain"
                and current["type"] == "plain"
                and previous["page_id"] == current["page_id"]
            ):
                previous["content"] = (previous["content"].rstrip() + "\n" + current["content"].lstrip()).strip()
                idx += 1
                continue

            merged.append(current)
            idx += 1

        cross_page_merged: list[dict[str, Any]] = []
        idx = 0
        while idx < len(merged):
            current = merged[idx]
            if not cross_page_merged:
                cross_page_merged.append(current)
                idx += 1
                continue

            previous = cross_page_merged[-1]
            if current["type"] == "table" and previous["type"] == "table":
                if current["page_id"] == previous["page_id"] + 1 and self._table_shapes_match(previous["content"], current["content"]):
                    previous["content"] = self._merge_table_markdown(previous["content"], current["content"])
                    idx += 1
                    continue

            if (
                len(cross_page_merged) >= 2
                and previous["type"] == "plain"
                and current["type"] == "table"
                and cross_page_merged[-2]["type"] == "table"
                and previous["page_id"] == cross_page_merged[-2]["page_id"] + 1 == current["page_id"]
                and self._is_ignorable_page_header(previous["content"])
                and self._table_shapes_match(cross_page_merged[-2]["content"], current["content"])
            ):
                cross_page_merged[-2]["content"] = self._merge_table_markdown(
                    cross_page_merged[-2]["content"],
                    current["content"],
                )
                cross_page_merged.pop()
                idx += 1
                continue

            if (
                previous["type"] == "table"
                and current["type"] == "plain"
                and idx + 1 < len(merged)
                and merged[idx + 1]["type"] == "table"
                and merged[idx + 1]["page_id"] in {current["page_id"], current["page_id"] + 1}
                and current["page_id"] in {previous["page_id"], previous["page_id"] + 1}
                and self._table_shapes_match(previous["content"], merged[idx + 1]["content"])
                and (self._is_ignorable_page_header(current["content"]) or self._is_short_table_bridge_plain(current["content"]))
            ):
                previous["content"] = self._merge_table_markdown(
                    previous["content"],
                    merged[idx + 1]["content"],
                    bridging_plain=current["content"],
                )
                idx += 2
                continue

            if (
                previous["type"] == "table"
                and current["type"] == "plain"
                and idx + 2 < len(merged)
                and merged[idx + 1]["type"] == "plain"
                and merged[idx + 2]["type"] == "table"
                and self._table_shapes_match(previous["content"], merged[idx + 2]["content"])
                and current["page_id"] in {previous["page_id"], previous["page_id"] + 1}
                and merged[idx + 1]["page_id"] in {current["page_id"], current["page_id"] + 1}
                and merged[idx + 2]["page_id"] in {merged[idx + 1]["page_id"], merged[idx + 1]["page_id"] + 1}
            ):
                bridge_content = "\n".join([current["content"], merged[idx + 1]["content"]]).strip()
                if self._is_short_table_bridge_plain(bridge_content):
                    previous["content"] = self._merge_table_markdown(
                        previous["content"],
                        merged[idx + 2]["content"],
                        bridging_plain=bridge_content,
                    )
                    idx += 3
                    continue

            cross_page_merged.append(current)
            idx += 1

        return cross_page_merged

    def _convert_with_marker_provider(self, file_path: str) -> str:
        """使用 marker 的 PdfProvider 做整份文档的表格重建。

        目的：
        这是当前 `financial_report` profile 的核心策略。
        不再依赖默认 `PdfConverter` 是否能识别出 Table 块，而是：
        - 自己从 provider 取行和坐标
        - 自己切表块
        - 自己渲染 markdown 表
        - 最后再统一做段落与跨页合并
        """
        provider = PdfProvider(file_path, config={"keep_chars": False})
        rendered_segments: list[dict[str, Any]] = []

        for page_id in provider.page_range:
            rows = self._build_provider_rows(provider, page_id)
            if not rows:
                continue

            table_blocks = self._detect_table_blocks(rows)
            if not table_blocks:
                plain_content = self._render_plain_rows(rows)
                if plain_content:
                    rendered_segments.append({"type": "plain", "content": plain_content, "page_id": page_id})
                continue

            cursor = 0
            for start_idx, end_idx in table_blocks:
                plain_md = self._render_plain_rows(rows[cursor:start_idx])
                if plain_md:
                    rendered_segments.append({"type": "plain", "content": plain_md, "page_id": page_id})

                table_md = self._render_table_block(rows[start_idx : end_idx + 1])
                if table_md:
                    context_before = self._render_plain_rows(rows[max(0, start_idx - 6) : start_idx])
                    normalized_table_md = self._normalize_table_markdown(table_md, context_before=context_before)
                    rendered_segments.append({"type": "table", "content": normalized_table_md, "page_id": page_id})
                else:
                    fallback_md = self._render_plain_rows(rows[start_idx : end_idx + 1])
                    if fallback_md:
                        rendered_segments.append({"type": "plain", "content": fallback_md, "page_id": page_id})

                cursor = end_idx + 1

            tail_md = self._render_plain_rows(rows[cursor:])
            if tail_md:
                rendered_segments.append({"type": "plain", "content": tail_md, "page_id": page_id})

        merged_segments = self._merge_rendered_segments(rendered_segments)
        return "\n\n".join(segment["content"] for segment in merged_segments if segment["content"]).strip()

    def _quality_metrics(self, text: str) -> dict[str, int]:
        """给候选输出做轻量质量评分。

        目的：
        不同策略输出之间需要可解释的比较指标。这里主要关注：
        - 字符量
        - 数字组数量
        - 中文字符数量
        - 替换字符数量
        这些指标足够判断“信息是否明显丢失”。
        """
        compact = re.sub(r"\s+", "", text or "")
        return {
            "char_count": len(compact),
            "numeric_groups": len(NUMERIC_GROUP_RE.findall(text or "")),
            "cjk_chars": len(CJK_CHAR_RE.findall(text or "")),
            "replacement_chars": (text or "").count("\ufffd"),
        }

    def _marker_has_table_blocks(self, metadata: dict) -> bool:
        """检查默认 marker 输出里是否真的识别出了表格块。

        目的：
        这能帮助判断默认 `PdfConverter` 是否已经足够好。
        如果连 Table 块都没识别出来，provider 重建通常更值得优先。
        """
        page_stats = metadata.get("page_stats") if isinstance(metadata, dict) else None
        if not isinstance(page_stats, list):
            return False

        table_like_types = {"Table", "TableGroup", "TableOfContents", "Form"}
        for page in page_stats:
            block_counts = page.get("block_counts") if isinstance(page, dict) else None
            if not isinstance(block_counts, list):
                continue
            for block_type, count in block_counts:
                if block_type in table_like_types and count:
                    return True
        return False

    def _marker_surya_page_count(self, metadata: dict) -> int:
        """统计默认 marker 输出中有多少页回退到了 surya OCR。

        目的：
        一旦 OCR 回退页变多，默认 markdown 往往更容易出现阅读顺序错乱、
        表格打散和数字丢失，这个指标会直接影响策略选择。
        """
        page_stats = metadata.get("page_stats") if isinstance(metadata, dict) else None
        if not isinstance(page_stats, list):
            return 0
        return sum(
            1
            for page in page_stats
            if isinstance(page, dict) and page.get("text_extraction_method") == "surya"
        )

    def _pipe_table_line_count(self, text: str) -> int:
        """统计输出中已经形成 markdown 表格的行数。

        目的：
        对财报 profile 来说，“有没有真正形成管道表格”是很重要的质量信号。
        """
        return sum(1 for line in (text or "").splitlines() if line.strip().startswith("|") and line.strip().endswith("|"))

    def _is_provider_usable(self, provider_text: str) -> bool:
        """判断 provider 重建结果是否达到最低可用标准。

        目的：
        provider 路径并非永远优于默认 marker。只有当它至少包含足够的正文、
        足够的数字，或者已经形成一定数量的表格行时，才值得参与最终比较。
        """
        metrics = self._quality_metrics(provider_text)
        return (
            metrics["char_count"] >= 120
            and (metrics["cjk_chars"] >= 50 or metrics["numeric_groups"] >= 3 or self._pipe_table_line_count(provider_text) >= 2)
        )

    def _should_prefer_provider(
        self,
        marker_text: str,
        marker_metadata: dict,
        provider_text: str,
        *,
        profile: str,
    ) -> bool:
        """决定最终是否采用 provider 重建结果。

        目的：
        当前 `financial_report` profile 的选择目标不是“谁更像原文段落”，
        而是“谁更适合后续财报分析”。
        因此这里优先看：
        - 是否有 markdown 表格
        - 数字是否更多
        - 默认 marker 是否存在 OCR 回退
        """
        if not provider_text.strip():
            return False

        marker_metrics = self._quality_metrics(marker_text)
        provider_metrics = self._quality_metrics(provider_text)
        marker_has_table_blocks = self._marker_has_table_blocks(marker_metadata)
        marker_surya_pages = self._marker_surya_page_count(marker_metadata)
        provider_pipe_lines = self._pipe_table_line_count(provider_text)
        marker_pipe_lines = self._pipe_table_line_count(marker_text)

        if profile == "financial_report" and self._is_provider_usable(provider_text):
            # 财报 profile 以结构化表格 + 数字保真为第一优先级。
            if provider_pipe_lines >= 2 and marker_pipe_lines == 0:
                return True
            if (
                marker_metrics["numeric_groups"] == 0
                and provider_metrics["numeric_groups"] > 0
            ):
                return True
            if (
                marker_surya_pages > 0
                and provider_metrics["numeric_groups"] >= marker_metrics["numeric_groups"]
            ):
                return True
            if (
                provider_pipe_lines >= 2
                and provider_metrics["numeric_groups"] >= int(marker_metrics["numeric_groups"] * 0.8)
            ):
                return True

        if (
            not marker_has_table_blocks
            and provider_metrics["numeric_groups"] >= marker_metrics["numeric_groups"] + 2
            and provider_metrics["char_count"] >= marker_metrics["char_count"] + 80
        ):
            return True

        if (
            provider_metrics["char_count"] >= int(marker_metrics["char_count"] * 1.25)
            and provider_metrics["numeric_groups"] >= marker_metrics["numeric_groups"] + 2
        ):
            return True

        if (
            marker_metrics["replacement_chars"] > provider_metrics["replacement_chars"]
            and provider_metrics["char_count"] >= marker_metrics["char_count"]
        ):
            return True

        return False

    def convert_with_details(
        self,
        file_path,
        output_dir=None,
        *,
        profile: str = DEFAULT_PDF_CONVERSION_PROFILE,
    ) -> PDFConversionResult:
        """执行完整转换并返回正文 + 详细元数据。

        目的：
        这是当前转换器的主入口。它会同时跑：
        - 默认 marker 输出
        - marker provider 重建输出
        然后基于质量指标做策略选择，并把结果封装成 `PDFConversionResult`，
        供 CLI、财报 skill、disclosures 缓存统一使用。
        """
        if profile not in SUPPORTED_PDF_CONVERSION_PROFILES:
            raise ValueError(
                f"不支持的 PDF 转换 profile: {profile}，可选值: {sorted(SUPPORTED_PDF_CONVERSION_PROFILES)}"
            )

        LOGGER.info("处理 PDF: %s, profile=%s", file_path, profile)
        start_time = time.time()
        marker_text, marker_metadata = self._convert_with_marker(file_path)
        markdown = marker_text
        strategy = "marker"

        marker_metrics = self._quality_metrics(marker_text)
        LOGGER.info(
            "marker 输出统计: char_count=%s, numeric_groups=%s, cjk_chars=%s, replacement_chars=%s, surya_pages=%s",
            marker_metrics["char_count"],
            marker_metrics["numeric_groups"],
            marker_metrics["cjk_chars"],
            marker_metrics["replacement_chars"],
            self._marker_surya_page_count(marker_metadata),
        )

        provider_text = ""
        provider_metrics = None
        try:
            provider_text = self._convert_with_marker_provider(file_path)
        except Exception as exc:
            LOGGER.warning("marker provider 表格重建失败，继续使用默认 marker 输出: %s", exc)
        else:
            provider_metrics = self._quality_metrics(provider_text)
            LOGGER.info(
                "marker provider 输出统计: char_count=%s, numeric_groups=%s, cjk_chars=%s, replacement_chars=%s, pipe_table_lines=%s",
                provider_metrics["char_count"],
                provider_metrics["numeric_groups"],
                provider_metrics["cjk_chars"],
                provider_metrics["replacement_chars"],
                self._pipe_table_line_count(provider_text),
            )
            if self._should_prefer_provider(
                marker_text,
                marker_metadata,
                provider_text,
                profile=profile,
            ):
                markdown = provider_text
                strategy = "marker_provider"
                LOGGER.info("检测到 marker provider 输出更适合当前 profile，已优先采用表格重建结果")
            else:
                LOGGER.info("继续使用 marker 输出")

        process_time = time.time() - start_time

        if output_dir:
            os.makedirs(output_dir, exist_ok=True)
            base_filename = Path(file_path).stem
            output_path = os.path.join(output_dir, f"{base_filename}.md")
            with open(output_path, "w", encoding="utf-8") as f:
                f.write(markdown)
            LOGGER.info("Markdown 已保存到: %s", output_path)

        LOGGER.info("处理时间 %.2f 秒，文件大小 %.2f KB, strategy=%s", process_time, len(markdown) / 1024, strategy)

        return PDFConversionResult(
            markdown=markdown,
            profile=profile,
            strategy=strategy,
            converter_version=PDF_MARKDOWN_CONVERTER_VERSION,
            marker_metrics=marker_metrics,
            provider_metrics=provider_metrics,
            marker_metadata=marker_metadata,
            generated_at=datetime.now().isoformat(timespec="seconds"),
        )

    def convert(
        self,
        file_path,
        output_dir=None,
        *,
        profile: str = DEFAULT_PDF_CONVERSION_PROFILE,
    ):
        """
        转换PDF文件到Markdown
        
        Args:
            file_path: PDF文件路径
            output_dir: 输出目录
            
        Returns:
            str: 转换后的Markdown文本
        """
        return self.convert_with_details(file_path, output_dir=output_dir, profile=profile).markdown


def basic_convert(
    file_path,
    output_dir=None,
    use_llm=False,
    *,
    profile: str = DEFAULT_PDF_CONVERSION_PROFILE,
):
    """
    基础转换函数
    
    Args:
        file_path: 输入文件路径
        output_dir: 输出目录
        use_llm: 是否使用LLM（暂未实现）

    目的：
    保留仓库里原有的兼容调用入口，让旧代码不需要改调用方式就能自动享受到
    新的 marker-only 财报表格重建逻辑。
    """
    converter = PDFMarkdownConverter()
    return converter.convert(file_path, output_dir, profile=profile)


def show_json(obj):
    display(HTML(f"<pre>{json.dumps(obj, indent=2)}</pre>"))

def show_parts(r):
    for part in r.parts:
        if part.text:
            display(Markdown(part.text))
        elif part.inline_data:
            if part.inline_data.mime_type.startswith('image/'):
                # For images, you might want to display them
                pass
        elif part.function_call:
            show_json(part.function_call)
        elif part.function_response:
            show_json(part.function_response)
        elif part.executable_code:
            show_json(part.executable_code)
        elif part.code_execution_result:
            show_json(part.code_execution_result)
    
    if hasattr(r, 'candidates') and r.candidates:
        for candidate in r.candidates:
            if hasattr(candidate, 'grounding_metadata') and candidate.grounding_metadata:
                grounding_metadata = candidate.grounding_metadata
                if hasattr(grounding_metadata, 'search_entry_point') and grounding_metadata.search_entry_point:
                    display(HTML(grounding_metadata.search_entry_point.rendered_content))
