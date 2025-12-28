#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
财务数据处理模块
处理股票财务数据的获取、清洗和转换
"""

import pandas as pd
import akshare as ak
from typing import Tuple, Dict, List, Any, Optional, Union
from pathlib import Path
import time
from datetime import datetime

from config import logger
from utils import retry, abbreviate_number

class FinancialDataProcessor:
    """财务数据处理类，用于获取、清洗和转换财务数据"""
    
    def __init__(self, stock_code: str, stock_name: str, full_stock_code: str, 
                 market: str = "A股", quarters: int = 2, start_date: Optional[str] = None, 
                 end_date: Optional[str] = None, cache_dir: Optional[Path] = None):
        """
        初始化财务数据处理器
        
        参数:
            stock_code (str): 股票代码
            stock_name (str): 股票名称
            full_stock_code (str): 带交易所前缀的完整股票代码
            market (str): 市场类型，默认"A股"
            quarters (int): 要分析的季度数量，如果未指定日期范围则使用此参数
            start_date (str, optional): 分析起始日期 (YYYY-MM-DD格式)
            end_date (str, optional): 分析结束日期 (YYYY-MM-DD格式)
            cache_dir (Path, optional): 缓存目录
        """
        self.stock_code = stock_code
        self.stock_name = stock_name
        self.full_stock_code = full_stock_code
        self.market = market
        self.quarters = quarters
        self.start_date = start_date
        self.end_date = end_date
        
        # 处理日期格式
        if self.start_date:
            try:
                # 尝试按多种格式解析
                if '-' in self.start_date:
                    self.start_date_obj = datetime.strptime(self.start_date, '%Y-%m-%d')
                else:
                    self.start_date_obj = datetime.strptime(self.start_date, '%Y%m%d')
            except ValueError:
                logger.warning(f"无效的起始日期格式: {self.start_date}，将忽略此参数")
                self.start_date = None
                self.start_date_obj = None
        else:
            self.start_date_obj = None
            
        if self.end_date:
            try:
                # 尝试按多种格式解析
                if '-' in self.end_date:
                    self.end_date_obj = datetime.strptime(self.end_date, '%Y-%m-%d')
                else:
                    self.end_date_obj = datetime.strptime(self.end_date, '%Y%m%d')
                # 如果只提供了结束日期，将默认分析近两个季度
                if not self.start_date:
                    logger.info(f"仅提供了结束日期，将分析近 {self.quarters} 个季度的数据")
            except ValueError:
                logger.warning(f"无效的结束日期格式: {self.end_date}，将忽略此参数")
                self.end_date = None
                self.end_date_obj = None
        else:
            self.end_date_obj = None
        
        # 设置缓存目录
        if cache_dir is None:
            base_dir = Path(f"data/{stock_name}_{stock_code}/fundamentals")
            self.cache_dir = base_dir / "cache"
        else:
            self.cache_dir = cache_dir
            
        # 确保缓存目录存在
        self.cache_dir.mkdir(parents=True, exist_ok=True)
    
    def check_local_data(self) -> Tuple[bool, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """
        检查本地缓存的财务数据是否存在且足够，如果足够则返回数据
        
        返回:
            tuple: (是否有足够数据, 资产负债表, 利润表, 现金流量表)
        """
        # 检查缓存目录是否存在
        if not self.cache_dir.exists():
            logger.warning(f"缓存目录不存在: {self.cache_dir}")
            return False, pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
            
        # 检查三个财务数据文件是否都存在
        bs_path = self.cache_dir / f"balance_sheet_{self.market}.csv"
        is_path = self.cache_dir / f"income_statement_{self.market}.csv"
        cf_path = self.cache_dir / f"cash_flow_{self.market}.csv"
        
        if not (bs_path.exists() and is_path.exists() and cf_path.exists()):
            logger.warning(f"本地缓存的财务数据文件不完整")
            return False, pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
            
        # 读取数据
        try:
            bs_df = pd.read_csv(bs_path)
            is_df = pd.read_csv(is_path)
            cf_df = pd.read_csv(cf_path)
            
            # 检查数据是否为空
            if bs_df.empty or is_df.empty or cf_df.empty:
                logger.warning(f"本地缓存的财务数据为空")
                return False, pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
                
            # 确定日期列
            date_column = 'REPORT_DATE' if 'REPORT_DATE' in bs_df.columns else '报告期'
            
            # 转换日期格式以便排序和过滤
            bs_df[date_column] = pd.to_datetime(bs_df[date_column])
            is_df[date_column] = pd.to_datetime(is_df[date_column])
            cf_df[date_column] = pd.to_datetime(cf_df[date_column])
            
            # 按日期排序（降序）
            bs_df = bs_df.sort_values(date_column, ascending=False)
            is_df = is_df.sort_values(date_column, ascending=False)
            cf_df = cf_df.sort_values(date_column, ascending=False)
            
            # 如果指定了日期范围，则过滤数据
            if self.start_date_obj or self.end_date_obj:
                original_bs_len = len(bs_df)
                original_is_len = len(is_df)
                original_cf_len = len(cf_df)
                
                if self.start_date_obj:
                    bs_df = bs_df[bs_df[date_column] >= self.start_date_obj]
                    is_df = is_df[is_df[date_column] >= self.start_date_obj]
                    cf_df = cf_df[cf_df[date_column] >= self.start_date_obj]
                
                if self.end_date_obj:
                    bs_df = bs_df[bs_df[date_column] <= self.end_date_obj]
                    is_df = is_df[is_df[date_column] <= self.end_date_obj]
                    cf_df = cf_df[cf_df[date_column] <= self.end_date_obj]
                
                # 检查过滤后是否有数据
                if bs_df.empty or is_df.empty or cf_df.empty:
                    logger.warning(f"在指定日期范围内没有本地缓存数据")
                    return False, pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
                    
                # 显示过滤结果
                logger.info(f"使用日期范围过滤本地数据: {self.start_date or '最早'} 至 {self.end_date or '最新'}")
                logger.info(f"资产负债表: {original_bs_len} -> {len(bs_df)}条数据")
                logger.info(f"利润表: {original_is_len} -> {len(is_df)}条数据")
                logger.info(f"现金流量表: {original_cf_len} -> {len(cf_df)}条数据")
                
                # 在日期范围过滤模式下，如果有数据就返回，不考虑季度数量
                return True, bs_df, is_df, cf_df
            else:
                # 检查季度数量是否足够
                min_quarters = min(len(bs_df), len(is_df), len(cf_df))
                
                if min_quarters >= self.quarters:
                    logger.info(f"本地缓存的财务数据足够: {min_quarters} >= {self.quarters} 季度")
                    return True, bs_df.head(self.quarters), is_df.head(self.quarters), cf_df.head(self.quarters)
                else:
                    logger.warning(f"本地缓存的财务数据不足: {min_quarters} < {self.quarters} 季度")
                    return False, pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
                    
        except Exception as e:
            logger.error(f"读取本地缓存的财务数据失败: {str(e)}")
            return False, pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    
    def save_financial_data(self, balancesheet_df: pd.DataFrame, profit_df: pd.DataFrame, 
                           cashflow_df: pd.DataFrame) -> bool:
        """
        保存财务数据到本地缓存
        
        参数:
            balancesheet_df (pd.DataFrame): 资产负债表DataFrame
            profit_df (pd.DataFrame): 利润表DataFrame
            cashflow_df (pd.DataFrame): 现金流量表DataFrame
            
        返回:
            bool: 是否保存成功
        """
        try:
            # 设置保存路径
            bs_file = self.cache_dir / f"balance_sheet_{self.market}.csv"
            is_file = self.cache_dir / f"income_statement_{self.market}.csv"
            cf_file = self.cache_dir / f"cash_flow_{self.market}.csv"
            
            # 保存数据到CSV文件
            balancesheet_df.to_csv(bs_file, index=False)
            profit_df.to_csv(is_file, index=False)
            cashflow_df.to_csv(cf_file, index=False)
            
            logger.info(f"财务数据已保存到本地缓存")
            return True
        except Exception as e:
            logger.error(f"保存财务数据到本地缓存出错: {e}")
            return False
    
    @retry(max_retries=3)
    def get_financial_data(self) -> Tuple[Optional[pd.DataFrame], Optional[pd.DataFrame], Optional[pd.DataFrame]]:
        """
        获取财务数据：资产负债表、利润表和现金流量表
        优先使用本地缓存，如果缓存不足则调用akshare API
        
        返回:
            tuple: (资产负债表DataFrame, 利润表DataFrame, 现金流量表DataFrame)
        """
        logger.info(f"获取 {self.stock_name}({self.full_stock_code}) 的财务数据...")
        
        # 检查本地缓存
        has_enough_data, bs_df, is_df, cf_df = self.check_local_data()
        if has_enough_data:
            # 即使使用缓存，也需要进行严格的数据清洗
            bs_df = self.clean_financial_df(bs_df)
            is_df = self.clean_financial_df(is_df)
            cf_df = self.clean_financial_df(cf_df)
            return bs_df, is_df, cf_df
        
        # 如果缓存不足，调用akshare API获取数据
        try:
            if self.market == "A股":
                # 对于A股，需要使用带交易所前缀的股票代码
                symbol_with_prefix = self.full_stock_code
                logger.info(f"使用代码 {symbol_with_prefix} 获取A股财务指标")
                
                # 获取A股财务指标
                balancesheet_df = ak.stock_balance_sheet_by_report_em(symbol=symbol_with_prefix)
                profit_df = ak.stock_profit_sheet_by_report_em(symbol=symbol_with_prefix)
                cashflow_df = ak.stock_cash_flow_sheet_by_report_em(symbol=symbol_with_prefix)
                
                # 确保数据不为空
                if balancesheet_df.empty or profit_df.empty or cashflow_df.empty:
                    logger.error(f"获取财务数据失败: 部分报表为空")
                    return None, None, None
                
                # 清洗数据 - 严格过滤无效列
                balancesheet_df = self.clean_financial_df(balancesheet_df)
                profit_df = self.clean_financial_df(profit_df)
                cashflow_df = self.clean_financial_df(cashflow_df)
                
                # 保存到本地缓存
                self.save_financial_data(balancesheet_df, profit_df, cashflow_df)
                
                return balancesheet_df, profit_df, cashflow_df
                
            elif self.market == "港股":
                # 对于港股，不需要使用带交易所前缀的股票代码
                logger.info(f"使用代码 {self.stock_code} 获取港股财务指标")
                
                # 获取港股财务指标
                try:
                    balancesheet_df = ak.stock_financial_hk_report_em(stock=self.stock_code, symbol="资产负债表", indicator="报告期")
                    profit_df = ak.stock_financial_hk_report_em(stock=self.stock_code, symbol="利润表", indicator="报告期")
                    cashflow_df = ak.stock_financial_hk_report_em(stock=self.stock_code, symbol="现金流量表", indicator="报告期")
                    
                    # 确保数据不为空
                    if balancesheet_df.empty or profit_df.empty or cashflow_df.empty:
                        logger.error(f"获取港股财务数据失败: 部分报表为空")
                        return None, None, None
                    
                    # 清洗数据 - 严格过滤无效列
                    balancesheet_df = self.clean_financial_df(balancesheet_df)
                    profit_df = self.clean_financial_df(profit_df)
                    cashflow_df = self.clean_financial_df(cashflow_df)
                    
                    # 保存到本地缓存
                    self.save_financial_data(balancesheet_df, profit_df, cashflow_df)
                    
                    logger.info(f"成功获取港股 {self.stock_code} 的财务指标")
                    return balancesheet_df, profit_df, cashflow_df
                except Exception as e:
                    logger.error(f"获取港股 {self.stock_code} 财务数据失败: {e}")
                    return None, None, None
            else:
                logger.error(f"不支持的市场类型: {self.market}")
                return None, None, None
                
        except Exception as e:
            logger.error(f"获取财务数据时出错: {e}")
            return None, None, None
    
    def clean_financial_df(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        清洗财务数据DataFrame，删除全为空、全为0或混合有空值和0的无效列
        
        参数:
            df (pd.DataFrame): 原始财务数据DataFrame
            
        返回:
            pd.DataFrame: 清洗后的DataFrame
        """
        if df is None or df.empty:
            return df
            
        # 获取日期列名，确保不删除日期列
        date_columns = []
        for col in df.columns:
            if any(date_name in col.lower() for date_name in ['date', '日期', '报告期', 'report_date']):
                date_columns.append(col)
        
        # 创建要保留的列清单，初始化为日期列
        columns_to_keep = date_columns.copy()
        
        # 对每一列进行检查
        for col in df.columns:
            # 跳过日期列
            if col in date_columns:
                continue
                
            column_data = df[col]
            
            # 检查列是否全为空
            if column_data.isna().all():
                continue  # 跳过全是空值的列
                
            # 检查是否全部为0或空值
            try:
                # 尝试将列数据转换为数值类型，错误值变为NaN
                numeric_data = pd.to_numeric(column_data, errors='coerce')
                # 检查是否所有非NaN值都是0或接近0的小数
                non_na_values = numeric_data.dropna()
                if len(non_na_values) == 0 or ((non_na_values == 0) | (non_na_values.abs() < 0.01)).all():
                    continue  # 跳过全是0或接近0的列
            except:
                # 如果转换失败，检查是否有非空值
                if not column_data.isna().all():
                    columns_to_keep.append(col)
                continue
                
            # 检查列中是否混合有空值和0
            has_significant_values = False
            for val in column_data:
                # 检查值是否为空
                if pd.isna(val):
                    continue
                    
                # 检查值是否为0或接近0
                try:
                    num_val = float(val)
                    if abs(num_val) >= 0.01:  # 非零且不是非常小的值
                        has_significant_values = True
                        break
                except (ValueError, TypeError):
                    # 非数值且非空，视为有意义的值
                    has_significant_values = True
                    break
            
            # 如果有意义的值，保留这一列
            if has_significant_values:
                columns_to_keep.append(col)
        
        # 创建清洗后的DataFrame
        cleaned_df = df[columns_to_keep].copy()
        
        # 记录删除了多少列
        removed_columns = set(df.columns) - set(columns_to_keep)
        logger.info(f"从财务数据中删除了 {len(removed_columns)} 列无效数据")
        
        return cleaned_df
    
    def clean_filtered_quarters_data(self, data: Dict[str, Dict[str, Any]], 
                                    quarters: List[str]) -> Dict[str, Dict[str, Any]]:
        """
        清洗已筛选出的最近几个季度数据，删除在筛选后全为空值或0的列
        
        参数:
            data (dict): 包含最近几个季度财务数据的字典
            quarters (list): 季度日期列表
            
        返回:
            dict: 清洗后的财务数据字典
        """
        if data is None or not quarters:
            return data
        
        # 复制数据，避免修改原始数据
        result = data.copy()
        
        # 保留字段映射
        field_mapping = result.get('字段映射', {})
        
        # 获取所有指标
        all_indicators = set()
        for quarter in quarters:
            if quarter in result:
                all_indicators.update(result[quarter].keys())
        
        # 检查每个指标在所有季度中是否有有效值
        invalid_indicators = []
        for indicator in all_indicators:
            if indicator == '字段映射':
                continue
                
            # 检查此指标是否在所有季度中都是无效值
            all_invalid = True
            for quarter in quarters:
                if quarter not in result:
                    continue
                
                if indicator in result[quarter]:
                    value = result[quarter][indicator]
                    try:
                        # 尝试将值转换为数值
                        if isinstance(value, (int, float)):
                            num_value = value
                        else:
                            num_value = float(value)
                        
                        # 检查值是否有意义（不为0且不为极小值）
                        if abs(num_value) >= 0.01:
                            all_invalid = False
                            break
                    except (ValueError, TypeError):
                        # 非数值类型，检查是否为有效字符串
                        if value not in [None, "N/A", "0", "0.0", "0.00", "0.00元", ""]:
                            all_invalid = False
                            break
            
            # 如果在所有季度中都是无效值，则添加到要删除的指标列表
            if all_invalid:
                invalid_indicators.append(indicator)
        
        # 删除无效指标
        for quarter in quarters:
            if quarter not in result:
                continue
                
            for indicator in invalid_indicators:
                if indicator in result[quarter]:
                    del result[quarter][indicator]
        
        # 恢复字段映射
        if '字段映射' in data:
            result['字段映射'] = field_mapping
            
        return result 