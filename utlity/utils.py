#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
实用工具模块
包含错误处理和重试逻辑、格式化功能等
"""

import time
import functools
from typing import Callable, TypeVar, Any, Dict, List, Optional, Union
from pathlib import Path
import logging
import re

# 定义类型变量用于泛型函数
T = TypeVar('T')

def retry(max_retries: int = 3, 
          base_wait: int = 5, 
          timeout_wait_multiplier: int = 2,
          error_handler: Optional[Callable] = None,
          logger: logging.Logger = logging.getLogger(__name__)) -> Callable:
    """
    重试装饰器
    
    参数:
        max_retries (int): 最大重试次数
        base_wait (int): 基本等待时间（秒）
        timeout_wait_multiplier (int): 超时错误等待时间乘数
        error_handler (Callable, optional): 错误处理函数
        
    返回:
        Callable: 装饰后的函数
    """
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> T:
            retry_count = 0
            result = None
            last_exception = None
            
            while retry_count < max_retries:
                try:
                    if retry_count > 0:
                        logger.info(f"第 {retry_count}/{max_retries} 次重试 {func.__name__}...")
                    
                    # 调用原始函数
                    result = func(*args, **kwargs)
                    
                    # 检查结果,如果是API调用可能返回空字符串或None
                    if result or not isinstance(result, (str, type(None))):
                        return result
                    
                    # 处理API返回空结果的情况
                    if result is None or (isinstance(result, str) and not result.strip()):
                        logger.warning(f"{func.__name__} 返回空结果")
                        last_exception = ValueError(f"Function {func.__name__} returned empty result")
                    else:
                        return result
                        
                except Exception as e:
                    last_exception = e
                    error_msg = str(e)
                    logger.warning(f"{func.__name__} 执行出错: {error_msg}")
                    
                    # 确定等待时间
                    if "timeout" in error_msg.lower() or "timed out" in error_msg.lower():
                        wait_time = base_wait * timeout_wait_multiplier * (retry_count + 1)
                        logger.info(f"超时错误，等待 {wait_time} 秒后重试...")
                    else:
                        wait_time = base_wait * (retry_count + 1)
                        logger.info(f"错误，等待 {wait_time} 秒后重试...")
                    
                    # 如果提供了错误处理函数，则调用
                    if error_handler:
                        error_handler(e, retry_count, max_retries)
                
                retry_count += 1
                
                # 如果还有重试次数，则等待
                if retry_count < max_retries:
                    time.sleep(wait_time)
            
            # 达到最大重试次数，记录错误
            if last_exception:
                logger.error(f"{func.__name__} 失败，已达最大重试次数: {last_exception}")
                
            # 在这里可以返回默认值或者重新抛出异常
            return result
        
        return wrapper
    return decorator


def filter_reports_by_type(items: List[Dict[str, Any]], key_field: str = 'announcementTitle') -> List[Dict[str, Any]]:
    """
    通用的报告过滤函数，按年份和报告类型进行分组并过滤
    
    参数:
        items (List[Dict]): 报告项列表
        key_field (str): 标题字段名
        
    返回:
        List[Dict]: 过滤后的报告项列表
    """
    from config import PREFER_PATTERNS, EXCLUDE_PATTERNS
    
    # 输出调试信息
    logger.info(f"开始过滤列表，共有{len(items)}个项目")
    
    # 按年份和报告类型进行分组
    report_groups = {}
    
    for item in items:
        title = item.get(key_field, '')
        if not title:
            continue
            
        # 提取年份和报告类型（第几季度或半年度/年度）
        year_match = re.search(r'(\d{4})年', title)
        if not year_match:
            logger.debug(f"跳过无法确定年份的项目: {title}")
            continue
            
        year = year_match.group(1)
        
        # 确定报告类型
        report_type = None
        for t in ['一季度', '半年度', '三季度', '年度']:
            if t in title:
                report_type = t
                break
        
        if not report_type:
            if '第一季度' in title or '第1季度' in title:
                report_type = '一季度'
            elif '第三季度' in title or '第3季度' in title:
                report_type = '三季度'
            else:
                logger.debug(f"跳过无法确定报告类型的项目: {title}")
                continue
        
        # 使用年份+报告类型作为键
        key = f"{year}_{report_type}"
        
        if key not in report_groups:
            report_groups[key] = []
        
        report_groups[key].append(item)
    
    # 对每个分组，选择优先级最高的报告
    filtered_list = []
    logger.info(f"按年份和报告类型分组后，共有{len(report_groups)}个组")
    
    for key, group in report_groups.items():
        if len(group) == 1:
            filtered_list.append(group[0])
            logger.debug(f"组 {key} 只有1个项目，直接选择")
        else:
            # 对多个项目进行排序，优先选择包含"全文"的，其次选择不包含"摘要"等词的
            preferred = None
            
            # 首先查找包含优先词的
            for item in group:
                title = item.get(key_field, '')
                for pattern in PREFER_PATTERNS:
                    if pattern in title:
                        preferred = item
                        logger.debug(f"组 {key} 选择包含'{pattern}'的项目: {title}")
                        break
                if preferred:
                    break
            
            # 如果没有找到优先的，检查是否有不包含排除词的
            if not preferred:
                for item in group:
                    title = item.get(key_field, '')
                    exclude = False
                    for pattern in EXCLUDE_PATTERNS:
                        if pattern in title:
                            exclude = True
                            break
                    if not exclude:
                        preferred = item
                        logger.debug(f"组 {key} 选择不包含排除词的项目: {title}")
                        break
            
            # 如果仍然没有找到优先的，则选择第一个
            if not preferred:
                preferred = group[0]
                logger.debug(f"组 {key} 未找到首选项目，选择第一个: {preferred.get(key_field, '未知')}")
            
            filtered_list.append(preferred)
    
    logger.info(f"过滤后保留{len(filtered_list)}个项目")
    return filtered_list


def abbreviate_number(number: Any, column_name: str = "", currency: str = "CNY", market: str = "A股") -> str:
    """
    根据数据类型格式化数字，区分货币值、百分比和比率
    
    参数:
        number (Any): 要格式化的数字
        column_name (str): 列名，用于判断数据类型
        currency (str): 货币单位，默认"CNY"（人民币）
        market (str): 市场类型，用于确定格式化规则
        
    返回:
        str: 格式化后的数字字符串
    """
    if number is None:
        return "N/A"
    
    try:
        # 判断数据类型
        # 1. 特殊字段（非数值类）- 直接返回字符串
        if ("CODE" in column_name or 
            "TYPE" in column_name or 
            "STATE" in column_name or 
            "STATUS" in column_name or
            "NAME" in column_name or
            "DATE" in column_name or
            'CURRENCY' in column_name):
            return str(number)
            
        # 2. 增长率、同比增长、百分比类型数据 - 需要更精确的匹配模式
        percentage_keywords = [
            "_YOY", "_GROWTH_", "_RATIO_", "_RATE_", "_PCT_", 
            "_PERCENT_", "_MARGIN_", "_ROE_", "_ROA_", "毛利率"
        ]
        
        is_percentage = False
        for keyword in percentage_keywords:
            if keyword in column_name:
                is_percentage = True
                break
                
        # 一些特定的完整字段名匹配
        percentage_full_fields = [
            "YOY", "GROWTH", "RATIO", "RATE", "PERCENT", "MARGIN", "ROE", "ROA"
        ]
        
        # 检查字段名是否完全等于某个百分比关键词
        if column_name.upper() in percentage_full_fields:
            is_percentage = True
            
        if is_percentage:
            try:
                number = float(number)
                sign = "-" if number < 0 else ""
                abs_number = abs(number)
                # 已经是整数形式的百分比，保留2位小数
                return f"{sign}{abs_number:.2f}%"
            except ValueError:
                # 如果转换失败，返回原始值
                return str(number)
        
        # 3. 货币类型数据 (默认情况)
        try:
            number = float(number)
            # 如果是0或非常接近0，直接返回0
            if abs(number) < 0.01:
                return "0.00元"
                
            sign = "-" if number < 0 else ""
            abs_number = abs(number)
            # 根据市场确定基础单位和货币符号
            if market == "A股":
                # akshare返回的数据通常单位是元
                currency_symbol = "元"
                
                # 统一格式化为以下单位
                if abs_number >= 100000000:  # ≥ 1亿元 → X.XX亿元
                    return f"{sign}{abs_number/100000000:.2f}亿{currency_symbol}"
                elif abs_number >= 10000000:  # ≥ 1000万元 → X.XX千万元
                    return f"{sign}{abs_number/10000000:.2f}千万{currency_symbol}"
                elif abs_number >= 10000:  # ≥ 1万元 → X.XX万元
                    return f"{sign}{abs_number/10000:.2f}万{currency_symbol}"
                else:  # 小额数据，不转换单位
                    return f"{sign}{abs_number:.2f}{currency_symbol}"
                    
            elif market == "港股":
                # 港股数据的单位根据数据源确定
                if currency == "HKD":
                    currency_symbol = "港元"
                else:
                    currency_symbol = "元"
                
                # 根据港股单位可能是千港元或百万港元
                base_unit = 1  # 假设基础单位为"千港元"或"百万港元"
                
                # 检查数字范围，决定使用的单位
                if abs_number >= 100000000 * base_unit:  # 亿元 (≥ 1亿)
                    return f"{sign}{abs_number/(100000000*base_unit):.2f}亿{currency_symbol}"
                elif abs_number >= 10000 * base_unit:  # 万元
                    return f"{sign}{abs_number/(10000*base_unit):.2f}万{currency_symbol}"
                else:  # 小额数据
                    return f"{sign}{abs_number:.2f}{currency_symbol}"
            else:
                # 默认格式化
                return f"{sign}{abs_number:.2f}元"
        except ValueError:
            # 如果转换失败，返回原始值
            return str(number)
        
    except Exception as e:
        logger.error(f"格式化数字时出错: {e}")
        return str(number)


def save_to_file(content: str, file_path: Union[str, Path], header: str = "") -> bool:
    """
    保存内容到文件
    
    参数:
        content (str): 要保存的内容
        file_path (Union[str, Path]): 文件路径
        header (str, optional): 文件头部内容
        
    返回:
        bool: 是否保存成功
    """
    try:
        file_path = Path(file_path)
        # 确保父目录存在
        file_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(file_path, 'w', encoding='utf-8') as f:
            if header:
                f.write(header + "\n\n")
            f.write(content)
        
        logger.info(f"内容已保存至 {file_path}")
        return True
    except Exception as e:
        logger.error(f"保存文件时出错: {file_path}, {e}")
        return False 