# @title Define some helpers (run this cell)
import json
import os
import time
import pandas as pd
from pathlib import Path
from IPython.display import display, HTML, Markdown

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


def read_cache_file(cache_file, force_refresh=False, dtype=None, index_col=None, 
                   start_date=None, end_date=None, log_prefix="", logger=None):
    """
    通用的缓存文件读取函数
    
    Args:
        cache_file (str): 缓存文件路径
        force_refresh (bool): 是否强制刷新，默认False
        dtype (dict): 数据类型字典，用于pd.read_csv
        index_col (int/str): 索引列，用于pd.read_csv
        start_date (str): 开始日期，用于时间范围筛选
        end_date (str): 结束日期，用于时间范围筛选
        log_prefix (str): 日志前缀，用于标识不同的调用场景
        logger: 日志记录器对象（必需参数）
        
    Returns:
        pd.DataFrame or None: 返回读取的数据，如果读取失败或不满足条件则返回None
        
    Raises:
        TypeError: 如果logger参数为None
    """
    if logger is None:
        raise TypeError("logger参数是必需的，不能为None")
        
    # 如果缓存文件存在且不强制刷新，尝试读取缓存
    if os.path.exists(cache_file) and not force_refresh:
        try:
            # 根据参数读取CSV文件
            read_kwargs = {}
            if dtype is not None:
                read_kwargs['dtype'] = dtype
            if index_col is not None:
                read_kwargs['index_col'] = index_col
                
            df = pd.read_csv(cache_file, **read_kwargs)

            # 如果有索引列且需要转换为日期时间
            if index_col is not None:
                df.index = pd.to_datetime(df.index)

            # 确保'代码'列为字符串类型（如果存在）
            if '代码' in df.columns:
                df['代码'] = df['代码'].astype(str).str.zfill(6)  # 补齐前导零
            
            # 输出日志信息
            if log_prefix:
                logger.info(f"{log_prefix}从缓存读取数据，共{len(df)}条记录")
            else:
                logger.info(f"从缓存读取数据，共{len(df)}条记录")
                
            return df
            
        except Exception as e:
            error_msg = f"读取缓存文件时出错: {e}"
            if log_prefix:
                logger.error(f"{log_prefix}{error_msg}")
            else:
                logger.error(error_msg)
            return None
    
    return None

def manage_cache_with_cleanup(cache_file, cache_dir, today_date, start_date, end_date, 
                             force_refresh=False, tolerance_days=3, index_col=0, 
                             dtype_dict=None, log_prefix="", logger=None):
    """
    综合缓存管理函数：读取缓存、验证时间范围、清理旧数据
    
    Args:
        cache_file: 缓存文件路径
        cache_dir: 缓存目录路径
        today_date: 今天的日期字符串（用于识别当天文件）
        start_date: 请求的开始日期
        end_date: 请求的结束日期
        force_refresh: 是否强制刷新缓存
        tolerance_days: 时间范围容错天数
        index_col: 索引列，默认为0
        dtype_dict: 数据类型字典
        log_prefix: 日志前缀
        logger: 日志记录器对象（必需参数）
    
    Returns:
        DataFrame or None: 如果缓存有效则返回筛选后的数据，否则返回None
        
    Raises:
        TypeError: 如果logger参数为None
    """
    if logger is None:
        raise TypeError("logger参数是必需的，不能为None")
        
    # 1. 尝试读取缓存
    cached_data = read_cache_file(
        cache_file=cache_file,
        force_refresh=force_refresh,
        index_col=index_col,
        dtype=dtype_dict,
        start_date=start_date,
        end_date=end_date,
        log_prefix=log_prefix,
        logger=logger
    )
    
    # 2. 如果有缓存数据，验证时间范围
    if cached_data is not None:
        logger.info(f"{log_prefix}检查缓存数据时间范围: 数据最小日期={cached_data.index.min()}, 数据最大日期={cached_data.index.max()}")
        logger.info(f"{log_prefix}请求时间范围: 开始日期={start_date}, 结束日期={end_date}")
        logger.info(f"{log_prefix}容错范围检查: 开始日期容错={pd.to_datetime(start_date) + pd.Timedelta(days=tolerance_days)}, 结束日期容错={pd.to_datetime(end_date) - pd.Timedelta(days=tolerance_days)}")
        
        # 检查数据是否涵盖请求的时间范围（带容错机制）
        if (cached_data.index.min() <= (pd.to_datetime(start_date) + pd.Timedelta(days=tolerance_days))) and \
           (cached_data.index.max() >= (pd.to_datetime(end_date) - pd.Timedelta(days=tolerance_days))):
            logger.info(f"{log_prefix}缓存数据时间范围满足要求，使用缓存数据")
            # 筛选请求的时间范围
            mask = (cached_data.index >= pd.to_datetime(start_date)) & (cached_data.index <= pd.to_datetime(end_date))
            filtered_data = cached_data.loc[mask]
            logger.info(f"{log_prefix}筛选后数据行数: {len(filtered_data)}")
            return filtered_data
        else:
            logger.info(f"{log_prefix}缓存数据时间范围不满足要求，需要重新获取数据")
    elif force_refresh:
        logger.info(f"{log_prefix}强制刷新缓存，需要重新获取数据")
    else:
        logger.info(f"{log_prefix}缓存文件不存在或读取失败，需要重新获取数据")
    
    # 3. 清理旧数据（如果缓存无效或不存在）
    if os.path.exists(cache_dir):
        files = os.listdir(cache_dir)
        
        for file in files:
            # 跳过今天的数据文件
            if today_date in file:
                continue
            
            # 删除其他日期的文件
            file_path = os.path.join(cache_dir, file)
            try:
                if os.path.isfile(file_path):
                    os.remove(file_path)
                    logger.info(f"{log_prefix}已删除过期文件: {file_path}")
            except Exception as e:
                logger.error(f"{log_prefix}删除文件时发生错误: {file_path}, 错误: {e}")
    
    return None

