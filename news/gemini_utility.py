# @title Define some helpers (run this cell)
import json
import os
import filetype
import time
from pathlib import Path

from IPython.display import display, HTML, Markdown
from marker.converters.pdf import PdfConverter
from marker.models import create_model_dict
from marker.output import save_output, text_from_rendered
from marker.config.parser import ConfigParser

# 创建一个单例模式的PDF转Markdown转换器类
class PDFMarkdownConverter:
    _instance = None
    _models_loaded = False
    _model_dict = None
    
    def __new__(cls):
        if cls._instance is None:
            print("初始化PDF转Markdown转换器...")
            cls._instance = super(PDFMarkdownConverter, cls).__new__(cls)
            cls._instance.initialized = False
        return cls._instance
        
    def __init__(self):
        if not self.initialized:
            print("检查模型缓存...")
            
            # 检查模型是否已经下载到缓存目录
            cache_dir = os.path.join(os.path.expanduser('~'), 'AppData', 'Local', 'datalab', 'datalab', 'Cache', 'models')
            models_exist = self._check_models_exist(cache_dir)
            
            if models_exist:
                print("发现已缓存的模型，快速加载...")
            else:
                print("首次运行，需要下载模型...")
            
            print("加载模型...")
            start_time = time.time()
            
            if not PDFMarkdownConverter._models_loaded:
                PDFMarkdownConverter._model_dict = create_model_dict()
                PDFMarkdownConverter._models_loaded = True
            
            self.converter = PdfConverter(
                artifact_dict=PDFMarkdownConverter._model_dict,
                config={"output_format": "markdown"}
            )
            
            load_time = time.time() - start_time
            print(f"模型加载完成，耗时: {load_time:.2f}秒")
            self.initialized = True
    
    def _check_models_exist(self, cache_dir):
        """检查必要的模型是否已经存在于缓存目录中"""
        if not os.path.exists(cache_dir):
            return False
        
        # 检查必要的模型目录
        required_models = [
            'text_detection',
            'text_recognition', 
            'layout',
            'table_recognition',
            'ocr_error_detection'
        ]
        
        for model in required_models:
            model_path = os.path.join(cache_dir, model)
            if not os.path.exists(model_path):
                return False
            
            # 检查模型目录是否有内容
            try:
                if not os.listdir(model_path):
                    return False
            except:
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
        print(f"处理: {file_path}")
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
            
            print(f"已保存到: {output_path}")
        
        print(f"处理时间: {process_time:.2f}秒")
        print(f"文件大小: {len(text)/1024:.2f} KB")
        
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