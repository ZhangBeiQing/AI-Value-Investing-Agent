#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
股票基本面分析总结工具
用于分析A股、港股和美股的财务报表和季报，并生成总结报告
"""

import os
import sys
import time
from pathlib import Path
from typing import Dict, List, Any, Optional, Union, Tuple
from datetime import datetime, date
from google import genai

from config import logger, GOOGLE_API_KEY, GEMINI_MODEL
from utils import save_to_file
from financial_data import FinancialDataProcessor
from financial_data_processor import FinancialDataExtractor, FinancialTableCreator
from pdf_processor import PDFReportDownloader
from pdf_analyzer import PDFAnalyzer

class StockFundamentalsSummarizer:
    """股票基本面分析总结器"""

    def __init__(self, stock_code, stock_name, market="A股", quarters=2, 
                 start_date=None, end_date=None):
        """
        初始化股票基本面分析总结器
        
        参数:
            stock_code (str): 股票代码
            stock_name (str): 股票名称
            market (str): 市场类型，"A股"、"港股"或"美股"
            quarters (int): 要分析的季度数量，如果未指定日期范围则使用此参数
            start_date (str, optional): 分析起始日期 (YYYYMMDD格式，如'20220101')
            end_date (str, optional): 分析结束日期 (YYYYMMDD格式，如'20221231')
        """
        self.stock_code = stock_code
        self.stock_name = stock_name
        self.market = market
        self.quarters = quarters
        self.start_date = start_date
        self.end_date = end_date
        
        # 初始化API配置
        self.setup_gemini_client()
        
        # 处理股票代码格式
        if market == "A股":
            # 对于A股，确定交易所并添加前缀
            if stock_code.startswith('6'):
                self.exchange = 'SH'
                self.full_stock_code = f"sh{stock_code}"
            else:
                self.exchange = 'SZ'
                self.full_stock_code = f"sz{stock_code}"
        else:
            # 对于其他市场，保持原始代码
            self.exchange = None
            self.full_stock_code = stock_code

        # 初始化目录
        self.base_dir = Path(f"data/{stock_name}_{stock_code}/fundamentals")
        self.base_dir.mkdir(parents=True, exist_ok=True)
        
        # 初始化各个功能模块
        self._init_modules()
        
    def _init_modules(self):
        """初始化各个功能模块"""
        # 财务数据处理模块
        self.financial_processor = FinancialDataProcessor(
            self.stock_code, 
            self.stock_name,
            self.full_stock_code,
            self.market,
            self.quarters,
            self.start_date,
            self.end_date
        )
        
        # 财务数据提取模块
        self.data_extractor = FinancialDataExtractor(self.financial_processor)
        
        # 财务表格创建模块
        self.table_creator = FinancialTableCreator(
            self.stock_code, 
            self.stock_name,
            self.market
        )
        
        # PDF报告下载模块
        if self.market == "A股":
            self.pdf_downloader = PDFReportDownloader(
                self.stock_code,
                self.stock_name,
                self.exchange,
                self.full_stock_code,
                self.quarters,
                self.start_date,
                self.end_date
            )
        else:
            self.pdf_downloader = None
            
        # PDF分析模块
        self.pdf_analyzer = PDFAnalyzer(
            self.stock_code,
            self.stock_name,
            self.full_stock_code,
            None,  # 使用默认路径
            self.client
        )
        
    def setup_gemini_client(self):
        """设置Gemini API客户端"""
        try:
            # 配置Gemini API客户端
            self.client = genai.Client(api_key=GOOGLE_API_KEY, http_options={'api_version': 'v1alpha'})
            logger.info("已配置Gemini客户端")
        except Exception as e:
            logger.error(f"配置Gemini客户端时出错: {e}")
            self.client = None
            
    def run(self, custom_pdf_folder=None, batch_mode=True, additional_info=None):
        """
        运行分析流程
        
        参数:
            custom_pdf_folder (str, optional): 自定义PDF文件夹路径
            batch_mode (bool): 是否使用批处理模式
            additional_info (str, optional): 额外的提示信息
            
        返回:
            str: 生成的分析摘要
        """
        logger.info(f"开始分析 {self.stock_name}({self.full_stock_code}) 的基本面数据")
        
        # 1. 获取财务数据
        balancesheet_df, profit_df, cashflow_df = self.financial_processor.get_financial_data()
        
        # 2. 提取最近季度数据
        if balancesheet_df is not None and profit_df is not None and cashflow_df is not None:
            financial_data = self.data_extractor.extract_recent_quarters(
                balancesheet_df, profit_df, cashflow_df
            )
            
            # 3. 创建财务表格
            if financial_data:
                financial_table = self.table_creator.create_financial_table(financial_data)
            else:
                financial_table = None
                logger.warning("无法提取财务数据，将不使用财务表格")
        else:
            financial_data = None
            financial_table = None
            logger.warning("无法获取财务数据，将不使用财务表格")
            
        # 4. 处理财报PDF
        try:
            # 如果提供了自定义PDF文件夹，使用其中的PDF文件
            if custom_pdf_folder:
                summary = self.process_custom_pdfs_with_gemini(
                    custom_pdf_folder, financial_table, batch_mode, additional_info
                )
            else:
                # 否则下载和处理财报PDF
                if self.market == "A股":
                    # 检查是否已有足够的财报文件
                    has_enough_reports = self.pdf_downloader.check_existing_reports()
                    
                    # 如果没有足够的财报，尝试下载
                    if not has_enough_reports:
                        download_success = self.pdf_downloader.download_reports()
                        if not download_success:
                            logger.warning(f"无法下载{self.stock_name}的财报，将尝试使用已有文件")
                    
                    # 获取报告文件列表
                    report_files = self.pdf_downloader.report_files
                    
                    # 如果没有报告文件，返回只有财务数据的分析
                    if not report_files:
                        logger.warning("无可用财报文件，将只使用财务数据")
                        summary = self.create_financial_only_summary(financial_table)
                    else:
                        logger.info(f"使用{len(report_files)}个财报文件进行分析")
                        summary = self.pdf_analyzer.process(
                            report_files, financial_table, batch_mode, additional_info
                        )
                else:
                    logger.warning(f"不支持自动下载{self.market}财报，将只使用财务数据")
                    summary = self.create_financial_only_summary(financial_table)
        except Exception as e:
            logger.error(f"处理财报时出错: {e}")
            summary = self.create_financial_only_summary(financial_table)
            
        # 5. 保存最终摘要
        if summary:
            summary_file = self.base_dir / f"{self.stock_name}_{self.stock_code}_summary.md"
            save_to_file(
                summary, 
                summary_file, 
                header=f"# {self.stock_name}({self.full_stock_code}) 财务分析总结"
            )
            logger.info(f"分析摘要已保存至 {summary_file}")
        
        # 8. 保存结果
        output_file = self.base_dir / f"{self.stock_name}_{self.stock_code}_financial_summary.md"
        try:
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write(f"# {self.stock_name} ({self.full_stock_code}) 财务分析\n\n")
                f.write("## 财务数据\n\n")
                if financial_table:
                    f.write(financial_table)
                else:
                    f.write("无可用财务数据\n")
                f.write("\n\n## 综合财务分析\n\n")
                f.write(summary)

                # 如果有额外信息，添加到文件末尾
                if additional_info:
                    f.write("\n\n## 分析中考虑的额外背景信息\n\n")
                    f.write(additional_info)

            logger.info(f"财务分析和PDF分析已保存至 {output_file}")
        except Exception as e:
            logger.error(f"保存财务分析时出错: {e}")
        
        return summary
    
    def process_custom_pdfs_with_gemini(self, custom_pdf_folder, financial_table=None, 
                                       batch_mode=True, additional_info=None):
        """
        处理自定义文件夹中的PDF文件
        
        参数:
            custom_pdf_folder (str): 自定义PDF文件夹路径
            financial_table (str, optional): 财务表格
            batch_mode (bool): 是否使用批处理模式
            additional_info (str, optional): 额外的提示信息
            
        返回:
            str: 生成的分析摘要
        """
        # 检查文件夹是否存在
        pdf_folder = Path(custom_pdf_folder)
        if not pdf_folder.exists() or not pdf_folder.is_dir():
            logger.error(f"自定义PDF文件夹不存在: {custom_pdf_folder}")
            return self.create_financial_only_summary(financial_table)
        
        # 获取所有PDF文件
        pdf_files = list(pdf_folder.glob("*.pdf"))
        
        if not pdf_files:
            logger.warning(f"自定义文件夹中没有PDF文件: {custom_pdf_folder}")
            return self.create_financial_only_summary(financial_table)
        
        logger.info(f"在自定义文件夹中找到{len(pdf_files)}个PDF文件")
        
        # 将Path对象转换为字符串
        pdf_file_paths = [str(f) for f in pdf_files]
        
        # 使用PDF分析器处理文件
        return self.pdf_analyzer.process(
            pdf_file_paths, financial_table, batch_mode, additional_info
        )
    
    def create_financial_only_summary(self, financial_table=None):
        """
        仅使用财务数据创建摘要
        
        参数:
            financial_table (str, optional): 财务表格
            
        返回:
            str: 生成的分析摘要
        """
        if not financial_table:
            return "无可用财务数据和财报文件，无法生成分析"
        
        prompt = f"""
            # {self.stock_name}({self.full_stock_code}) 财务分析任务

            ## 角色设定
            你是一位资深财务分析师，专注于上市公司财报深度研究，具有敏锐的洞察力和丰富的行业经验。

            ## 分析材料
            我提供了以下{self.stock_name}的财务数据供你分析：

            ```
            {financial_table}
            ```

            ## 分析任务
            请基于上述财务数据，提供一份全面而深入的分析报告，包括：

            1. **关键财务指标分析**：
            - 收入和利润增长趋势
            - 盈利能力指标（毛利率、净利率、ROE等）
            - 运营效率指标
            - 偿债能力和资本结构

            2. **业绩驱动因素**：
            - 收入和利润增长的主要来源
            - 成本结构变化

            3. **风险评估**：
            - 从财务数据中识别的潜在风险
            - 值得关注的异常趋势

            4. **财务健康度评估**：
            - 整体财务健康状况
            - 与行业平均水平比较（如有可能）

            请注意以下几点：
            - 关注季度间的变化趋势，而不仅是单期数据
            - 寻找数据中可能隐含的问题
            - 提供你的专业判断，而不只是描述数据
            - 使用量化数据支持你的观点

            最终报告应当长度适中（1500-2000字），既有深度也有广度，能为投资者提供对{self.stock_name}的全面了解。
        """
        
        try:
            if not self.client:
                logger.error("Gemini客户端未初始化，无法生成分析")
                return "无法连接到Gemini API，请检查API密钥配置"
            
            # 保存提示到文件
            self._save_prompt_to_file(prompt, "financial_only")
            
            # 调用Gemini API生成摘要
            response = self.client.models.generate_content_stream(
                model=GEMINI_MODEL,
                contents=prompt
            )
            
            summary = ""
            # 获取响应文本
            for chunk in response:
                if chunk.candidates and chunk.candidates[0].content and chunk.candidates[0].content.parts:
                    for part in chunk.candidates[0].content.parts:
                        if hasattr(part, 'text') and part.text:
                            summary += part.text
            
            return summary
            
        except Exception as e:
            logger.error(f"生成财务数据摘要时出错: {e}")
            return "生成财务分析时出错，请查看日志了解详情"
    
    def _save_prompt_to_file(self, prompt, prompt_type):
        """
        保存提示到文件
        
        参数:
            prompt (str): 提示文本
            prompt_type (str): 提示类型
        """
        try:
            prompt_dir = self.base_dir / "prompts"
            prompt_dir.mkdir(parents=True, exist_ok=True)
            
            prompt_file = prompt_dir / f"{prompt_type}_prompt.md"
            with open(prompt_file, 'w', encoding='utf-8') as f:
                f.write(prompt)
                
            logger.info(f"提示已保存至 {prompt_file}")
            
        except Exception as e:
            logger.error(f"保存提示到文件时出错: {e}")


def main():
    """
    主函数，处理命令行参数并执行相应的操作
    """
    import argparse
    
    parser = argparse.ArgumentParser(description='股票财务状况总结工具')
    parser.add_argument('--code', type=str, nargs='+', required=True, help='股票代码列表，如600000 00700')
    parser.add_argument('--name', type=str, nargs='+', required=True, help='股票名称列表，如"浦发银行 腾讯控股"，请确保与代码顺序一致')
    parser.add_argument('--market', type=str, nargs='+', help='市场类型列表，A股或港股或美股，默认为A股。如果只提供一个值，将应用于所有公司')
    parser.add_argument('--quarters', type=int, default=2, help='要分析的季度数量，默认为2（如果指定了日期范围，则此参数被忽略）')
    parser.add_argument('--start_date', type=str, help='分析起始日期，格式YYYY-MM-DD，如2023-01-01')
    parser.add_argument('--end_date', type=str, help='分析结束日期，格式YYYY-MM-DD，如2023-12-31')
    parser.add_argument('--custom_pdf', type=str, nargs='+', help='自定义PDF文件夹路径列表，如果提供则使用此文件夹中的PDF而不是自动下载的报告。如果只提供一个值，将作为基础路径应用于所有公司')
    parser.add_argument('--batch_mode', action='store_true', default=True, help='是否使用批处理模式处理PDF，默认为True')
    parser.add_argument('--additional_info', type=str, nargs='+', help='额外的提示信息文件路径列表，包含财报中未提及的重要背景信息。如果只提供一个值，将应用于所有公司')

    # 港股关键字：季度业绩、年度报告、业绩报告(小米)、业绩公告(美团)
    args = parser.parse_args()

    # --code 002714 600598 002311 002236 000792 600989 600426 600019 000708 000932 601899 603993 600111--name 牧原股份 北大荒 海大集团 大华股份 盐湖股份 宝丰能源 华鲁恒升 宝钢股份 中信特钢 华菱钢铁 紫金矿业 洛阳钼业 北方稀土--market A股 A股 A股 A股 A股 A股 A股

    # 验证参数列表长度
    if len(args.code) != len(args.name):
        print("错误：股票代码列表和名称列表的长度必须相同！")
        return
    
    # 处理市场类型参数
    if len(args.market) == 1:
        # 如果只提供了一个市场类型，应用于所有公司
        markets = [args.market[0]] * len(args.code)
    elif len(args.market) != len(args.code):
        print("错误：市场类型列表长度必须等于1或等于股票代码列表长度！")
        return
    else:
        markets = args.market
    
    # 处理自定义PDF文件夹参数
    if args.custom_pdf:
        if len(args.custom_pdf) == 1:
            # 如果只提供了一个文件夹，将其作为基础路径，为每个公司创建子文件夹
            base_path = args.custom_pdf[0]
            custom_pdfs = [os.path.join(base_path, f"{name}_{code}") for code, name in zip(args.code, args.name)]
        elif len(args.custom_pdf) != len(args.code):
            print("错误：自定义PDF文件夹列表长度必须等于1或等于股票代码列表长度！")
            return
        else:
            custom_pdfs = args.custom_pdf
    else:
        custom_pdfs = [None] * len(args.code)
    
    # 处理额外信息文件参数
    additional_infos = [None] * len(args.code)
    if args.additional_info:
        if len(args.additional_info) == 1 and os.path.exists(args.additional_info[0]):
            # 如果只提供了一个文件，读取其内容并应用于所有公司
            try:
                with open(args.additional_info[0], 'r', encoding='utf-8') as f:
                    info_content = f.read()
                additional_infos = [info_content] * len(args.code)
                logger.info(f"已读取额外信息文件: {args.additional_info[0]}")
            except Exception as e:
                logger.error(f"读取额外信息文件出错: {e}")
        elif len(args.additional_info) == len(args.code):
            # 如果为每个公司提供了单独的文件
            for i, info_file in enumerate(args.additional_info):
                if info_file and os.path.exists(info_file):
                    try:
                        with open(info_file, 'r', encoding='utf-8') as f:
                            additional_infos[i] = f.read()
                        logger.info(f"已读取额外信息文件: {info_file}")
                    except Exception as e:
                        logger.error(f"读取额外信息文件出错: {e}")
        else:
            print("错误：额外信息文件列表长度必须等于1或等于股票代码列表长度！")
            return
    
    # 循环处理每个公司
    for i, (code, name) in enumerate(zip(args.code, args.name)):
        print(f"\n开始分析 {name}({code}) 的财务数据...")
        logger.info(f"开始分析 {name}({code}) 的财务数据...")
        
        # 创建股票财务总结器实例
        summarizer = StockFundamentalsSummarizer(
            code, 
            name, 
            markets[i], 
            args.quarters,
            args.start_date,
            args.end_date
        )
        
        # 运行分析流程
        summary = summarizer.run(custom_pdfs[i], args.batch_mode, additional_infos[i])
        
        print(f"{name}({code}) 财务分析已完成，请查看保存的文件。")
    
    print("\n所有公司的财务分析都已完成。")


if __name__ == "__main__":
    main()
