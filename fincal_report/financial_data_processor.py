#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
财务数据处理模块的第二部分
处理财务数据的提取和表格创建
"""

import pandas as pd
from typing import Dict, List, Any, Optional, Union, Tuple
from pathlib import Path
import time
from datetime import datetime

from config import logger
from utils import abbreviate_number
from financial_data import FinancialDataProcessor

class FinancialDataExtractor:
    """财务数据提取类，用于从原始数据提取结构化财务数据"""
    
    def __init__(self, data_processor: FinancialDataProcessor):
        """
        初始化财务数据提取器
        
        参数:
            data_processor (FinancialDataProcessor): 财务数据处理器实例
        """
        self.processor = data_processor
        self.stock_code = data_processor.stock_code
        self.stock_name = data_processor.stock_name
        self.full_stock_code = data_processor.full_stock_code
        self.market = data_processor.market
        self.quarters = data_processor.quarters
    
    def extract_recent_quarters(self, balancesheet_df: pd.DataFrame, profit_df: pd.DataFrame, 
                               cashflow_df: pd.DataFrame) -> Optional[Dict[str, Dict[str, Any]]]:
        """
        从财务数据中提取指定范围内的财务数据
        如果有指定日期范围，则提取该范围内的所有财务数据
        否则提取最近N个季度的数据
        
        参数:
            balancesheet_df (pd.DataFrame): 资产负债表数据
            profit_df (pd.DataFrame): 利润表数据
            cashflow_df (pd.DataFrame): 现金流量表数据
            
        返回:
            dict: 包含指定日期范围内财务指标的字典
        """
        # 检查是否有足够的数据
        if balancesheet_df is None or profit_df is None or cashflow_df is None:
            return None

        # 获取报告期列
        try:
            # 确保报告期列存在
            date_column = 'REPORT_DATE'
            if date_column not in balancesheet_df.columns:
                logger.error(f"找不到报告期列: {date_column}")
                return None

            balancesheet_df['报告期'] = pd.to_datetime(balancesheet_df[date_column])
            profit_df['报告期'] = pd.to_datetime(profit_df[date_column])
            cashflow_df['报告期'] = pd.to_datetime(cashflow_df[date_column])

            # 按日期排序（降序）
            balancesheet_df = balancesheet_df.sort_values('报告期', ascending=False)
            profit_df = profit_df.sort_values('报告期', ascending=False)
            cashflow_df = cashflow_df.sort_values('报告期', ascending=False)

            # 检查是否有日期范围限制
            has_date_range = self.processor.start_date_obj is not None or self.processor.end_date_obj is not None
            
            if has_date_range:
                # 如果有日期范围，使用过滤后的所有数据
                logger.info(f"使用日期范围内的所有财务数据: {self.processor.start_date or '最早'} 至 {self.processor.end_date or '最新'}")
                
                # 过滤日期范围
                if self.processor.start_date_obj:
                    balancesheet_df = balancesheet_df[balancesheet_df['报告期'] >= self.processor.start_date_obj]
                    profit_df = profit_df[profit_df['报告期'] >= self.processor.start_date_obj]
                    cashflow_df = cashflow_df[cashflow_df['报告期'] >= self.processor.start_date_obj]
                    
                if self.processor.end_date_obj:
                    balancesheet_df = balancesheet_df[balancesheet_df['报告期'] <= self.processor.end_date_obj]
                    profit_df = profit_df[profit_df['报告期'] <= self.processor.end_date_obj]
                    cashflow_df = cashflow_df[cashflow_df['报告期'] <= self.processor.end_date_obj]
                
                # 确保数据不为空
                if balancesheet_df.empty or profit_df.empty or cashflow_df.empty:
                    logger.warning(f"日期范围内没有财务数据")
                    return None
                    
                recent_balancesheet = balancesheet_df
                recent_profit = profit_df
                recent_cashflow = cashflow_df
            else:
                # 如果没有日期范围，使用最近N个季度的数据
                logger.info(f"使用最近 {self.quarters} 个季度的财务数据")
                recent_balancesheet = balancesheet_df.head(self.quarters)
                recent_profit = profit_df.head(self.quarters)
                recent_cashflow = cashflow_df.head(self.quarters)

            # 获取季度日期列表
            quarters = recent_balancesheet['报告期'].dt.strftime('%Y-%m-%d').tolist()
            
            logger.info(f"提取的财务数据包含以下报告期: {quarters}")

            # 创建空的结果字典
            result = {quarter: {} for quarter in quarters}

            # 从所有财务报表提取完整数据
            for quarter in quarters:
                q_date = pd.to_datetime(quarter)

                # 资产负债表数据 - 保留所有非空列
                bs_row = balancesheet_df[balancesheet_df['报告期'] == q_date].iloc[0] if not balancesheet_df[balancesheet_df['报告期'] == q_date].empty else None
                if bs_row is not None:
                    # 添加所有非空列，添加前缀以区分不同报表
                    for col in bs_row.index:
                        if col != '报告期' and col != date_column and pd.notna(bs_row[col]) and bs_row[col] != 0:
                            # 验证值是否有意义（不为0或非常小的值）
                            try:
                                val = float(bs_row[col])
                                if abs(val) < 0.01:  # 忽略非常小的值
                                    continue
                            except (ValueError, TypeError):
                                # 非数值类型或无法转换的值，保留原样
                                pass

                            # 确保值不为0或空
                            result[quarter][f"BS_{col}"] = bs_row[col]

                # 利润表数据 - 保留所有非空列
                income_row = profit_df[profit_df['报告期'] == q_date].iloc[0] if not profit_df[profit_df['报告期'] == q_date].empty else None
                if income_row is not None:
                    # 添加所有非空列，添加前缀以区分不同报表
                    for col in income_row.index:
                        if col != '报告期' and col != date_column and pd.notna(income_row[col]) and income_row[col] != 0:
                            # 验证值是否有意义
                            try:
                                val = float(income_row[col])
                                if abs(val) < 0.01:  # 忽略非常小的值
                                    continue
                            except (ValueError, TypeError):
                                pass

                            # 确保值不为0或空
                            result[quarter][f"IS_{col}"] = income_row[col]

                    # 计算毛利润（如果有营业收入和营业成本）
                    if 'IS_OPERATE_INCOME' in result[quarter] and 'IS_OPERATE_COST' in result[quarter]:
                        gross_profit = result[quarter]['IS_OPERATE_INCOME'] - result[quarter]['IS_OPERATE_COST']
                        if abs(gross_profit) > 0.01:  # 确保毛利润不为0或非常小
                            result[quarter]['IS_毛利润'] = gross_profit

                        # 计算毛利率
                        if result[quarter]['IS_OPERATE_INCOME'] != 0:
                            gross_margin = gross_profit / result[quarter]['IS_OPERATE_INCOME']
                            if abs(gross_margin) > 0.0001:  # 确保毛利率不为0或非常小
                                result[quarter]['IS_毛利率'] = gross_margin*100 #后面要转换成百分号，所以毛利率*100

                    # 计算毛利润同比增速
                    # 我们需要找到去年同期的季度数据
                    current_date = pd.to_datetime(quarter)
                    prev_year_same_quarter = None

                    # 获取去年同期的日期（简单处理：年份-1）
                    prev_year_date = pd.Timestamp(year=current_date.year-1, month=current_date.month, day=current_date.day)
                    prev_year_str = prev_year_date.strftime('%Y-%m-%d')

                    # 检查是否有去年同期的数据
                    if prev_year_str in result:
                        # 如果当前季度和去年同期都有毛利润数据，计算同比增速
                        if 'IS_毛利润' in result[quarter] and 'IS_毛利润' in result[prev_year_str]:
                            current_gross_profit = result[quarter]['IS_毛利润']
                            prev_year_gross_profit = result[prev_year_str]['IS_毛利润']

                            if prev_year_gross_profit != 0 and abs(prev_year_gross_profit) > 0.01:  # 避免除以零或非常小的值
                                yoy_growth = (current_gross_profit - prev_year_gross_profit) / prev_year_gross_profit
                                if abs(yoy_growth) > 0.0001:  # 确保增长率不为0或非常小
                                    result[quarter]['IS_毛利润_YOY'] = yoy_growth

                # 现金流量表数据 - 保留所有非空列
                cf_row = cashflow_df[cashflow_df['报告期'] == q_date].iloc[0] if not cashflow_df[cashflow_df['报告期'] == q_date].empty else None
                if cf_row is not None:
                    # 添加所有非空列，添加前缀以区分不同报表
                    for col in cf_row.index:
                        if col != '报告期' and col != date_column and pd.notna(cf_row[col]) and cf_row[col] != 0:
                            # 验证值是否有意义
                            try:
                                val = float(cf_row[col])
                                if abs(val) < 0.01:  # 忽略非常小的值
                                    continue
                            except (ValueError, TypeError):
                                pass

                            # 确保值不为0或空
                            result[quarter][f"CF_{col}"] = cf_row[col]

            # 添加财务指标中英文映射，帮助模型理解数据
            field_mapping = {
                # 资产负债表常见指标
                'TOT_ASSETS': '总资产',
                'TOT_LIAB': '总负债',
                'TOTAL_EQUITY': '股东权益合计',
                'TOTAL_LIAB_EQUITY': '负债和股东权益总计',
                'MONETARY_FUND': '货币资金',
                'ST_BORROW': '短期借款',
                'LT_BORROW': '长期借款',
                # 利润表常见指标
                'OPERATE_INCOME': '营业收入',
                'OPERATE_COST': '营业成本',
                'OPERATE_PROFIT': '营业利润',
                'NET_PROFIT_EXCL_MIN_INT_INC': '归属于母公司所有者的净利润',
                # 现金流量表常见指标
                'NET_CASH_FLOWS_OPER_ACT': '经营活动现金流量净额',
                'END_BAL_CASH': '期末现金及现金等价物'
            }

            # 将字段映射添加到结果中，方便模型理解
            result['字段映射'] = field_mapping

            # 检查结果中是否有一些列在所有季度都为空或0，如果是则删除
            all_quarters = [q for q in quarters]  # 排除字段映射
            all_indicators = set()

            # 收集所有指标
            for quarter in all_quarters:
                all_indicators.update(result[quarter].keys())

            # 检查每个指标是否在所有季度都有非空非零值，以及是否在所有季度都为0
            valid_indicators = {}
            for indicator in all_indicators:
                # 统计该指标在多少个季度有非空非零值
                valid_count = sum(1 for q in all_quarters if
                                indicator in result[q] and
                                result[q][indicator] is not None and
                                result[q][indicator] != 0 and
                                result[q][indicator] != "0" and
                                result[q][indicator] != "0.0" and
                                result[q][indicator] != "0.00" and
                                result[q][indicator] != "0.00元" and
                                result[q][indicator] != "N/A")
                valid_indicators[indicator] = valid_count

            # 删除在所有季度都为空或都为0的指标
            for quarter in all_quarters:
                # 创建一个要删除的键的列表
                keys_to_delete = []
                for key in result[quarter]:
                    # 删除在所有季度都为空的指标或者在所有季度值都相同的指标
                    if valid_indicators.get(key, 0) == 0:
                        keys_to_delete.append(key)

                # 删除这些键
                for key in keys_to_delete:
                    del result[quarter][key]

            # 最后再做一次清理，查找所有季度里数据全为N/A的指标
            na_indicators = []
            for indicator in all_indicators:
                if indicator not in ['字段映射'] and all(
                    indicator not in result[q] or
                    str(result[q][indicator]).upper() in [
                        "NONE", "N/A", "0", "0.0", "0.00", "0.00元", ""
                    ] or result[q][indicator] in [None, 0, 0.0]
                    for q in all_quarters
                ):
                    na_indicators.append(indicator)

            # 删除全是N/A或0的指标
            for quarter in all_quarters:
                for indicator in na_indicators:
                    if indicator in result[quarter]:
                        del result[quarter][indicator]

            # 最终的有效性检查 - 删除所有季度都是无效值的指标
            final_na_indicators = []
            for indicator in all_indicators:
                if indicator not in ['字段映射'] and indicator not in na_indicators:
                    invalid_in_all_quarters = True
                    for q in all_quarters:
                        if (
                            indicator in result[q] and
                            result[q][indicator] not in [None, "N/A", 0, "0", "0.0", "0.00", "0.00元", ""] and
                            not (isinstance(result[q][indicator], (int, float)) and abs(result[q][indicator]) < 0.01)
                        ):
                            invalid_in_all_quarters = False
                            break

                    if invalid_in_all_quarters:
                        final_na_indicators.append(indicator)

            # 删除这些最终确认为无效的指标
            for quarter in all_quarters:
                for indicator in final_na_indicators:
                    if indicator in result[quarter]:
                        del result[quarter][indicator]

            # 对筛选后的最近季度数据再次进行一次清洗
            result = self.processor.clean_filtered_quarters_data(result, all_quarters)

            return result

        except Exception as e:
            logger.error(f"提取最近季度数据时出错: {e}")
            return None


class FinancialTableCreator:
    """财务表格创建类，用于将财务数据转换为可读的表格格式"""
    
    def __init__(self, stock_code: str, stock_name: str, market: str = "A股"):
        """
        初始化财务表格创建器
        
        参数:
            stock_code (str): 股票代码
            stock_name (str): 股票名称
            market (str): 市场类型，默认"A股"
        """
        self.stock_code = stock_code
        self.stock_name = stock_name
        self.market = market
    
    def create_financial_table(self, financial_data: Dict[str, Dict[str, Any]]) -> str:
        """
        创建财务指标表格
        
        参数:
            financial_data (dict): 包含多个季度财务数据的字典
            
        返回:
            str: Markdown格式的表格
        """
        if financial_data is None or len(financial_data) == 0:
            return "无可用财务数据"
        
        # 获取季度列表和关键指标
        quarters = list(financial_data.keys())
        
        # 如果有字段映射，移除它避免显示在表格中
        if '字段映射' in quarters:
            quarters.remove('字段映射')
            
        # 确保季度按时间排序（从旧到新）
        quarters.sort()
        
        # 收集所有指标并按类型分组
        all_indicators = set()
        for quarter in quarters:
            all_indicators.update(financial_data[quarter].keys())
        
        # 第一轮过滤：移除在任何季度都为N/A或None的指标
        indicators_to_remove = []
        for indicator in all_indicators:
            # 检查此指标是否在所有季度中都是N/A、None或0
            all_missing = True
            for quarter in quarters:
                if indicator in financial_data[quarter] and financial_data[quarter][indicator] not in [None, "N/A", 0, "0", "0.0", "0.00", "0.00元"]:
                    all_missing = False
                    break
            
            if all_missing:
                indicators_to_remove.append(indicator)
        
        # 从指标集合中移除这些指标
        all_indicators = all_indicators - set(indicators_to_remove)
        
        # 按报表类型分组
        bs_indicators = sorted([ind for ind in all_indicators if ind.startswith('BS_')])
        is_indicators = sorted([ind for ind in all_indicators if ind.startswith('IS_')])
        cf_indicators = sorted([ind for ind in all_indicators if ind.startswith('CF_')])
        
        # 组合所有指标，确保按类型分组显示
        indicators = bs_indicators + is_indicators + cf_indicators
        
        # 创建表头
        header = "| 指标 | " + " | ".join(quarters) + " |"
        separator = "|---" + "|---" * len(quarters) + "|"
        
        # 创建表格行
        rows = []
        for indicator in indicators:
            # 翻译指标名称
            field_name = indicator
            if '字段映射' in financial_data and indicator.split('_', 1)[1] in financial_data['字段映射']:
                translated = financial_data['字段映射'][indicator.split('_', 1)[1]]
                field_name = f"{indicator} ({translated})"
            
            # 第二轮过滤：再次检查此行是否有有效值
            valid_values_count = 0
            total_na_count = 0
            
            for quarter in quarters:
                if quarter == '字段映射':
                    continue
                    
                # 检查此季度的数据是否有效
                if indicator in financial_data[quarter] and financial_data[quarter][indicator] not in [None, "N/A"]:
                    valid_values_count += 1
                else:
                    total_na_count += 1
            
            # 如果所有季度都是N/A或无效值，跳过此行
            if valid_values_count == 0 or total_na_count == len(quarters):
                continue
                
            row = f"| {field_name} |"
            
            for quarter in quarters:
                if quarter == '字段映射':
                    continue
                    
                if indicator in financial_data[quarter] and financial_data[quarter][indicator] not in [None, "N/A"]:
                    value = abbreviate_number(financial_data[quarter][indicator], column_name=indicator, market=self.market)
                    row += f" {value} |"
                else:
                    row += " N/A |"
            
            # 第三轮过滤：确保不添加全是N/A的行
            na_count = row.count(" N/A |")
            if na_count < len(quarters):  # 如果不是所有值都为N/A
                rows.append(row)
        
        # 组合表格
        table = header + "\n" + separator + "\n" + "\n".join(rows)
        return table
    
    def create_single_financial_table(self, df: pd.DataFrame, title: str, prefix: str) -> str:
        """
        将单个财务报表数据转换为Markdown表格格式
        
        参数:
            df (pd.DataFrame): 财务报表数据
            title (str): 表格标题
            prefix (str): 数据前缀（BS_、IS_、CF_）
            
        返回:
            str: Markdown格式的表格
        """
        if df is None or df.empty:
            return f"## {title}\n\n无可用数据"
        
        # 按报告期降序排序
        if 'REPORT_DATE' in df.columns:
            date_column = 'REPORT_DATE'
        elif '报告期' in df.columns:
            date_column = '报告期'
        else:
            date_column = df.columns[0]
            
        df = df.sort_values(date_column, ascending=False)
        
        # 提取最近几个季度的数据
        quarters = min(len(df), 3)  # 默认取最近3个季度
        recent_df = df.head(quarters).copy()
        
        # 创建表头
        date_rows = []
        for idx, row in recent_df.iterrows():
            date_str = row[date_column].strftime('%Y-%m-%d') if isinstance(row[date_column], pd.Timestamp) else str(row[date_column])
            date_rows.append(date_str)
        
        header = f"## {title}\n\n| 指标 |"
        for date_str in date_rows:
            header += f" {date_str} |"
        header += "\n|---|" + "---|" * len(date_rows)
        
        # 预处理列，删除全为空或0的列
        valid_columns = []
        for col in df.columns:
            if col == date_column:
                continue
                
            # 检查此列在选定的最近几个季度中是否有有效值
            has_valid_value = False
            for idx, row in recent_df.iterrows():
                if pd.notna(row[col]) and row[col] != 0:
                    try:
                        val = float(row[col])
                        if abs(val) >= 0.01:  # 数值足够大，视为有效
                            has_valid_value = True
                            break
                    except (ValueError, TypeError):
                        # 非数值类型但非空，视为有效
                        has_valid_value = True
                        break
            
            if has_valid_value:
                valid_columns.append(col)
        
        # 创建表格内容
        rows = []
        for col in valid_columns:
            # 检查此列是否在所有行都是N/A
            all_na = True
            for idx, row in recent_df.iterrows():
                if pd.notna(row[col]) and row[col] != 0:
                    try:
                        val = float(row[col])
                        if abs(val) >= 0.01:  # 数值足够大，视为非NA
                            all_na = False
                            break
                    except (ValueError, TypeError):
                        # 非数值类型但非空，视为非NA
                        all_na = False
                        break
            
            # 跳过全是N/A的行
            if all_na:
                continue
                
            row_text = f"| {col} |"
            na_count = 0
            
            for idx, data_row in recent_df.iterrows():
                if pd.notna(data_row[col]) and data_row[col] != 0:
                    try:
                        val = float(data_row[col])
                        if abs(val) < 0.01:  # 值太小，视为无效
                            row_text += " N/A |"
                            na_count += 1
                            continue
                    except (ValueError, TypeError):
                        pass  # 非数值类型，保持原值
                        
                    value = abbreviate_number(data_row[col], column_name=f"{prefix}_{col}", market=self.market)
                    row_text += f" {value} |"
                else:
                    row_text += " N/A |"
                    na_count += 1
            
            # 只添加不是全是N/A的行
            if na_count < len(date_rows):
                rows.append(row_text)
        
        # 组合表格
        table = header + "\n" + "\n".join(rows)
        return table 