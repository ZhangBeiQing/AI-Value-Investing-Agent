# @title Define some helpers (run this cell)
import json
import os
import filetype
import time
from pathlib import Path

from core.logging import get_logger
from IPython.display import display, HTML, Markdown
from marker.converters.pdf import PdfConverter
from marker.models import create_model_dict
from marker.output import save_output, text_from_rendered
from marker.config.parser import ConfigParser
from surya.settings import settings as surya_settings

LOGGER = get_logger("PDFMarkdownConverter")

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
    
    def convert(self, file_path, output_dir=None):
        """
        转换PDF文件到Markdown
        
        Args:
            file_path: PDF文件路径
            output_dir: 输出目录
            
        Returns:
            str: 转换后的Markdown文本
        """
        # 执行转换
        LOGGER.info("处理 PDF: %s", file_path)
        start_time = time.time()
        rendered = self.converter(file_path)
        process_time = time.time() - start_time
        
        # 获取文本内容
        text, metadata, images = text_from_rendered(rendered)
        
        # 保存输出
        if output_dir:
            os.makedirs(output_dir, exist_ok=True)
            base_filename = Path(file_path).stem
            output_path = os.path.join(output_dir, f"{base_filename}.md")
            
            with open(output_path, "w", encoding="utf-8") as f:
                f.write(text)
            
            LOGGER.info("Markdown 已保存到: %s", output_path)
        
        LOGGER.info("处理时间 %.2f 秒，文件大小 %.2f KB", process_time, len(text) / 1024)
        
        return text


def basic_convert(file_path, output_dir=None, use_llm=False):
    """
    基础转换函数
    
    Args:
        file_path: 输入文件路径
        output_dir: 输出目录
        use_llm: 是否使用LLM（暂未实现）
    """
    converter = PDFMarkdownConverter()
    return converter.convert(file_path, output_dir)


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
