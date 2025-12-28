#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Progressive News Summarizer
该模块用于获取特定股票的新闻、公告、研报并进行总结
"""

import io
import os
import sys
import time
import json
import akshare as ak
import pandas as pd
import requests
from google import genai
from datetime import datetime, timedelta
import logging
import re
import urllib.request
from pathlib import Path
from dotenv import load_dotenv
import argparse
from google.genai.types import Tool, GenerateContentConfig, GoogleSearch
from gemini_utility import basic_convert  # 导入PDF转Markdown函数
from google.genai import errors

# 加载.env文件中的环境变量
load_dotenv()

# 设置日志记录
def setup_logging(stock_code, stock_name, start_date=None, end_date=None):
    """
    设置日志记录器
    
    参数:
        stock_code (str): 股票代码
        stock_name (str): 股票名称
        start_date (str): 开始日期，格式为YYYYMMDD
        end_date (str): 结束日期，格式为YYYYMMDD
    """
    # 创建logs目录
    logs_dir = Path("logs")
    logs_dir.mkdir(exist_ok=True)
    
    # 创建股票特定的日志目录
    stock_logs_dir = logs_dir / f"{stock_name}_{stock_code}"
    stock_logs_dir.mkdir(exist_ok=True)
    
    # 构建日志文件名
    if start_date and end_date:
        log_filename = f"progressive_news_summarizer_{start_date}_{end_date}.log"
    else:
        current_date = datetime.now().strftime("%Y%m%d")
        log_filename = f"progressive_news_summarizer_{current_date}.log"
    
    log_file_path = stock_logs_dir / log_filename
    
    # 获取logger实例 - 使用唯一名称以避免重用
    logger_name = f"{__name__}.{stock_code}.{start_date or 'default'}.{end_date or 'default'}"
    logger = logging.getLogger(logger_name)
    
    # 重置logger，清除已有的处理器
    if logger.handlers:
        for handler in logger.handlers[:]:
            logger.removeHandler(handler)
    
    # 设置日志级别
    logger.setLevel(logging.INFO)
    
    # 创建处理器
    file_handler = logging.FileHandler(log_file_path, encoding='utf-8')
    console_handler = logging.StreamHandler(sys.stdout)
    
    # 设置格式
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)
    
    # 添加处理器
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    # 确保消息传递到父级logger
    logger.propagate = False
    
    logger.info(f"日志文件保存在: {log_file_path}")
    return logger

# 获取Gemini API密钥
GOOGLE_API_KEY = os.environ.get("GOOGLE_API_KEY", "")

# 检查是否为测试模式
is_test_mode = len(sys.argv) > 1 and sys.argv[1] == "test"

# 配置Gemini
if GOOGLE_API_KEY:
    # genai.configure(api_key=GOOGLE_API_KEY)
    pass
elif not is_test_mode:  # 只有在非测试模式下才对API密钥缺失报错
    logger.error("未设置GOOGLE_API_KEY环境变量，请设置后再运行。")
    sys.exit(1)

class ProgressiveNewsSummarizer:
    """Progressive News Summarizer类，用于收集和总结股票相关新闻"""
    
    def __init__(self, stock_code, stock_name, market="A股", days=30, start_date=None, end_date=None):
        """
        初始化Progressive News Summarizer
        
        参数:
            stock_code (str): 股票代码
            stock_name (str): 股票名称
            market (str): 市场类型，默认"A股"，可选["A股", "港股"]
            days (int): 要查询的天数，默认30天
            start_date (str): 开始日期，格式为"YYYYMMDD"，如果指定则优先使用，而不使用days
            end_date (str): 结束日期，格式为"YYYYMMDD"，如果指定则优先使用，而不使用days
        """
        self.stock_code = stock_code
        self.stock_name = stock_name
        self.market = market
        self.days = days
        # 使用环境变量的API密钥，并设置HTTP超时选项
        self.client = genai.Client(
            api_key=GOOGLE_API_KEY,
            http_options={"timeout": 600000}
        )
        
        # 设置日志记录器
        self.logger = setup_logging(stock_code, stock_name, start_date, end_date)
        
        # 创建存储目录
        self.base_dir = Path(f"data/{self.stock_name}_{self.stock_code}")
        self.reports_dir = self.base_dir / "reports"  # 保留目录但已禁用研报功能
        self.news_dir = self.base_dir / "news"
        self.announcements_dir = self.base_dir / "announcements"
        self.short_term_summary_dir = self.base_dir / "short_term_summary"
        self.long_term_summary_dir = self.base_dir / "long_term_summary"
        
        self._create_directories()
        
        # 设置日期范围
        self.end_date = datetime.now()
        self.start_date = self.end_date - timedelta(days=self.days)
        
        # 如果指定了日期范围，则优先使用指定日期
        if start_date and end_date:
            try:
                self.start_date = datetime.strptime(start_date, "%Y%m%d")
                self.end_date = datetime.strptime(end_date, "%Y%m%d")
                self.logger.info(f"使用指定日期范围: {start_date} 至 {end_date}")
            except ValueError as e:
                self.logger.error(f"日期格式错误: {e}，将使用默认日期范围")
                
        self.start_date_str = self.start_date.strftime("%Y%m%d")
        self.end_date_str = self.end_date.strftime("%Y%m%d")
        
        # 标记是否为指定日期范围模式
        self.is_date_range_mode = bool(start_date and end_date)
        # 标记是否为多短期总结合并模式
        self.is_multiple_summary_mode = False
        
    def _create_directories(self):
        """创建存储目录结构"""
        self.announcement_summary_dir = self.base_dir / "announcement_summary"
        for directory in [self.reports_dir, self.news_dir, self.announcements_dir, 
                          self.short_term_summary_dir, self.long_term_summary_dir, 
                          self.announcement_summary_dir]:
            directory.mkdir(parents=True, exist_ok=True)
        self.logger.info(f"为{self.stock_name}({self.stock_code})创建目录结构")

    def collect_stock_research_reports(self):
        """
        收集个股研报 - 该功能已被禁用
        返回空列表
        """
        self.logger.info(f"研报功能已被禁用，不再收集研报")
        return []

    def collect_stock_announcements(self):
        """
        收集股票公告
        使用akshare获取公告列表，并下载对应的PDF文件
        """
        self.logger.info(f"正在获取{self.stock_name}({self.stock_code})的公告...")
        try:
            announcement_files = []
            
            if self.market == "港股":
                market = "港股"
            elif self.market == "A股":
                market = "沪深京"
            else:
                self.logger.warning(f"暂不支持{self.market}的公告获取")
                return []
            
            # 使用巨潮资讯接口获取公告
            start_date = self.start_date.strftime("%Y%m%d")
            end_date = self.end_date.strftime("%Y%m%d")
            
            # 确保存在chromedriver
            chromedriver_path = "C:\\Windows\\System32\\chromedriver.exe"
            if not os.path.exists(chromedriver_path):
                self.logger.error(f"未找到chromedriver，请确保chromedriver.exe位于C:\\Windows\\System32\\目录下")
                return []
            
            # 创建缓存目录
            cache_dir = self.base_dir / "cache" / "announcements"
            cache_dir.mkdir(parents=True, exist_ok=True)

            # 构建缓存文件路径
            cache_file = cache_dir / f"{self.stock_code}_{start_date}_{end_date}_公告.csv"
            
            try:
                # 检查是否存在缓存文件
                if cache_file.exists():
                    self.logger.info(f"使用缓存的公告数据: {cache_file}")
                    try:
                        df = pd.read_csv(cache_file, encoding='utf-8')
                        if df.empty:
                            self.logger.info(f"缓存文件为空，未找到公告")
                    except Exception as cache_err:
                        self.logger.error(f"读取缓存文件出错: {cache_err}，将重新获取数据")
                        # 缓存文件读取失败，重新获取数据
                        df = ak.stock_zh_a_disclosure_report_cninfo(
                            symbol=self.stock_code,
                            market=market,
                            tabList="公告",
                            start_date=start_date,
                            end_date=end_date
                        )
                        
                        # 保存到缓存文件
                        if not df.empty:
                            df.to_csv(cache_file, encoding='utf-8', index=False)
                            self.logger.info(f"已将公告数据保存到缓存: {cache_file}")
                else:
                    # 获取公告数据
                    self.logger.info(f"从API获取公告数据...")
                    df = ak.stock_zh_a_disclosure_report_cninfo(
                        symbol=self.stock_code,
                        market=market,
                        category="",
                        start_date=start_date,
                        end_date=end_date
                    )
                    
                    # 保存到缓存文件
                    if not df.empty:
                        df.to_csv(cache_file, encoding='utf-8', index=False)
                        self.logger.info(f"已将公告数据保存到缓存: {cache_file}")
                    else:
                        # 创建空文件作为标记，避免下次仍然调用API
                        with open(cache_file, 'w', encoding='utf-8') as f:
                            f.write("")
                        self.logger.info(f"未找到公告，创建空缓存文件")
                
                # 处理查询结果
                if df.empty:
                    self.logger.info(f"未找到公告")
                    
                # 处理每条公告
                for _, row in df.iterrows():
                    title = re.sub(r'[\\/:*?"<>|]', '_', row['公告标题'])
                    
                    # 过滤与财报相关的公告
                    # if any(keyword in row['公告标题'] for keyword in [
                    #     "年度报告", "半年度报告", "季度报告", "财务报告", "财务会计报告",
                    #     "财务报表", "审计报告", "年报", "半年报", "季报", "财报",
                    #     "业绩报告", "季度财务","公司资料报表", "翌日披露报表", "申请表格","申请版本", "聆讯後资料集"
                    # ]):
                    #     self.logger.info(f"跳过财报相关公告: {row['公告标题']}")
                    #     continue

                    # 过滤与财报相关的公告
                    if any(keyword in row['公告标题'] for keyword in [
                        "翌日披露报表", "申请表格","申请版本", "聆讯後资料集", "法律意见书", " 核查意见", "股东大会的通知", "股东大会通知"
                    ]):
                        self.logger.info(f"跳过财报相关公告: {row['公告标题']}")
                        continue
                    
                    # 处理日期，移除可能存在的时间部分
                    announcement_date = row['公告时间']
                    if ' ' in announcement_date:  # 检查是否包含时间
                        announcement_date = announcement_date.split(' ')[0]  # 只保留日期部分
                    date = announcement_date.replace('-', '')
                    
                    link = row['公告链接']
                    
                    # 设置文件路径
                    pdf_file_path = self.announcements_dir / f"{date}_{title}.pdf"
                    
                    # 如果PDF已存在，检查其大小并跳过下载
                    if pdf_file_path.exists():
                        # 检查文件大小是否超过3MB
                        file_size = os.path.getsize(pdf_file_path)
                        if file_size > 3 * 1024 * 1024:  # 3MB = 3 * 1024 * 1024 bytes
                            self.logger.info(f"跳过大文件公告(大小: {file_size/1024/1024:.2f}MB): {pdf_file_path}")
                            continue
                        
                        self.logger.info(f"公告PDF已存在: {pdf_file_path}")
                        announcement_files.append(str(pdf_file_path))
                        continue
                    
                    # 先创建临时TXT文件记录公告信息
                    txt_file_path = self.announcements_dir / f"{date}_{title}.txt"
                    try:
                        with open(txt_file_path, 'w', encoding='utf-8') as f:
                            f.write(f"标题: {title}\n")
                            f.write(f"日期: {row['公告时间']}\n")
                            f.write(f"链接: {link}\n\n")
                        self.logger.info(f"已创建临时记录: {txt_file_path}")
                    except Exception as txt_err:
                        self.logger.error(f"创建临时记录失败: {txt_err}")
                    
                    # 下载PDF文件
                    self.logger.info(f"下载公告PDF: {title}")
                    try:
                        # 尝试下载PDF
                        self._download_announcement_pdf(link, pdf_file_path)
                        
                        # 检查下载文件的大小是否超过3MB
                        if os.path.exists(pdf_file_path):
                            file_size = os.path.getsize(pdf_file_path)
                            if file_size > 3 * 1024 * 1024:  # 3MB = 3 * 1024 * 1024 bytes
                                self.logger.info(f"删除大文件公告(大小: {file_size/1024/1024:.2f}MB): {pdf_file_path}")
                                os.remove(pdf_file_path)
                                continue
                        
                        self.logger.info(f"PDF下载成功: {pdf_file_path}")
                        announcement_files.append(str(pdf_file_path))
                        
                        # 删除临时TXT文件
                        if os.path.exists(txt_file_path):
                            try:
                                os.remove(txt_file_path)
                                self.logger.info(f"已删除临时记录: {txt_file_path}")
                            except Exception as rm_err:
                                self.logger.error(f"删除临时记录失败: {rm_err}")
                    except Exception as pdf_err:
                        self.logger.error(f"下载PDF失败: {title}, 错误: {pdf_err}")
                        # 下载失败时，保留TXT文件作为记录
                        if os.path.exists(txt_file_path):
                            announcement_files.append(str(txt_file_path))
                
                # 防止请求过快
                time.sleep(1)
                
            except Exception as cat_err:
                self.logger.error(f"获取公告失败: {cat_err}")
            
            # 清理多余的TXT文件
            self._clean_announcement_txt_files(announcement_files)
            
            # 返回结果
            pdf_files = [f for f in announcement_files if f.lower().endswith('.pdf')]
            self.logger.info(f"共收集{len(pdf_files)}个PDF公告")
            return pdf_files

                
        except Exception as e:
            self.logger.error(f"获取公告时出错: {e}")
            return []

    def _clean_announcement_txt_files(self, announcement_files):
        """
        清理announcements目录中的TXT文件
        只保留没有对应PDF文件的TXT记录
        
        Args:
            announcement_files: 已下载的公告文件列表
        """
        try:
            # 获取所有已下载的PDF文件名
            pdf_filenames = [os.path.basename(f) for f in announcement_files if f.lower().endswith('.pdf')]
            
            # 遍历目录中的所有TXT文件
            for file in os.listdir(self.announcements_dir):
                if not file.lower().endswith('.txt'):
                    continue
                    
                txt_path = os.path.join(self.announcements_dir, file)
                
                # 检查是否有对应的PDF文件
                pdf_name = file.replace('.txt', '.pdf')
                if pdf_name in pdf_filenames:
                    # 如果有对应的PDF，删除TXT
                    try:
                        os.remove(txt_path)
                        self.logger.info(f"清理: 删除临时TXT文件 {txt_path}")
                    except Exception as e:
                        self.logger.error(f"删除TXT文件失败: {e}")
                        
            self.logger.info("临时TXT文件清理完成")
        except Exception as e:
            self.logger.error(f"清理TXT文件时出错: {e}")

    
    def _download_announcement_pdf(self, url, output_path):
        """下载巨潮资讯网的公告PDF
        
        Args:
            url: 公告页面URL
            output_path: PDF保存路径
        """
        import urllib.request
        from bs4 import BeautifulSoup
        import requests
        import time
        import os
        
        # 确保目标目录存在
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        # 尝试直接获取PDF链接
        success = False
        
        try:
            # 1. 尝试通过规则推导PDF URL
            # 示例：从http://www.cninfo.com.cn/new/disclosure/detail?stockCode=002415&announcementId=1221835007
            # 推导为http://static.cninfo.com.cn/finalpage/2024-11-26/1221835007.PDF
            
            # 从URL中提取announcementId
            import re
            announcement_id = re.search(r"announcementId=(\d+)", url)
            announcement_time = re.search(r"announcementTime=(\d{4}-\d{2}-\d{2})", url)
            
            if announcement_id and announcement_time:
                ann_id = announcement_id.group(1)
                ann_date = announcement_time.group(1).replace("-", "")
                
                # 构建静态PDF链接
                pdf_url = f"http://static.cninfo.com.cn/finalpage/{announcement_time.group(1)}/{ann_id}.PDF"
                self.logger.info(f"通过规则推导得到PDF链接: {pdf_url}")
                
                # 设置请求头
                headers = {
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
                    "Referer": url
                }
                
                # 创建请求对象
                req = urllib.request.Request(pdf_url, headers=headers)
                
                try:
                    # 下载文件
                    self.logger.info(f"开始下载PDF: {pdf_url}")
                    with urllib.request.urlopen(req) as response, open(output_path, "wb") as out_file:
                        out_file.write(response.read())
                    
                    # 验证下载结果
                    if os.path.exists(output_path) and os.path.getsize(output_path) > 0:
                        self.logger.info(f"下载成功，文件大小: {os.path.getsize(output_path)} 字节")
                        success = True
                        return True
                except Exception as direct_err:
                    self.logger.error(f"直接下载PDF失败: {direct_err}")
            
            # 2. 如果推导失败，尝试解析页面获取PDF链接
            if not success:
                self.logger.info("尝试解析页面获取PDF链接")
                
                response = requests.get(url, headers={
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
                })
                
                if response.status_code == 200:
                    soup = BeautifulSoup(response.text, "html.parser")
                    
                    # 尝试查找页面中的PDF链接
                    pdf_links = []
                    for a_tag in soup.find_all("a", href=True):
                        if ".pdf" in a_tag["href"].lower():
                            pdf_links.append(a_tag["href"])
                    
                    # 也查找embed标签的src属性
                    for embed_tag in soup.find_all("embed"):
                        src = embed_tag.get("src", "")
                        if ".pdf" in src.lower():
                            pdf_links.append(src)
                    
                    self.logger.info(f"通过解析页面找到{len(pdf_links)}个PDF链接")
                    
                    if pdf_links:
                        # 处理相对URL
                        pdf_url = pdf_links[0]
                        if pdf_url.startswith("/"):
                            pdf_url = f"http://www.cninfo.com.cn{pdf_url}"
                        elif not pdf_url.startswith("http"):
                            pdf_url = f"http://www.cninfo.com.cn/{pdf_url}"
                        
                        self.logger.info(f"尝试下载PDF链接: {pdf_url}")
                        pdf_response = requests.get(pdf_url, headers={
                            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
                            "Referer": url
                        })
                        
                        if pdf_response.status_code == 200:
                            with open(output_path, "wb") as f:
                                f.write(pdf_response.content)
                            self.logger.info(f"通过直接下载链接成功保存PDF: {output_path}")
                            success = True
                            return True
            
            # 清理根目录残留的.crdownload文件
            try:
                root_dir = os.getcwd()
                for file in os.listdir(root_dir):
                    if file.lower().endswith(".crdownload"):
                        file_path = os.path.join(root_dir, file)
                        try:
                            os.remove(file_path)
                            self.logger.info(f"已删除残留的下载文件: {file_path}")
                        except Exception as e:
                            self.logger.error(f"删除残留文件失败: {e}")
            except Exception as e:
                self.logger.error(f"清理残留下载文件出错: {e}")
            
            # 检查文件是否成功下载
            if os.path.exists(output_path) and os.path.getsize(output_path) > 0:
                self.logger.info(f"PDF文件成功下载，大小: {os.path.getsize(output_path)} 字节")
                return True
            else:
                raise Exception("下载PDF失败")
                
        except Exception as e:
            self.logger.error(f"下载PDF出错: {e}")
            raise
    
    def search_stock_news_with_gemini(self):
        """
        使用Gemini的Google搜索功能获取股票相关新闻
        如果已存在相同日期范围的新闻文件，则直接复用
        """

        # 检查是否已存在相同日期范围的新闻文件
        news_file = self.news_dir / f"{self.stock_code}_news_{self.start_date_str}_{self.end_date_str}.md"
        if news_file.exists():
            self.logger.info(f"已存在日期范围内的新闻文件，直接复用: {news_file}")
            return str(news_file)

        MODEL = "gemini-2.5-pro"
        self.logger.info(f"正在使用Gemini搜索{self.stock_name}({self.stock_code})的新闻...")

        google_search_tool = Tool(
            google_search=GoogleSearch()
        )

        # 构建搜索提示
        start_date_formatted = self.start_date.strftime("%Y年%m月%d日")
        end_date_formatted = self.end_date.strftime("%Y年%m月%d日")
        
        prompt = f"""
                    请帮我搜集并汇总关于{self.stock_name}在{start_date_formatted}至{end_date_formatted}期间的所有可能影响公司股价的新闻和传闻，严格按以下要求执行：

                    【数据来源要求】
                    覆盖以下渠道：主流财经媒体、行业垂直平台、社交媒体（股吧/雪球/微博）、监管文件、公司公告、供应链信源。对于社交媒体传闻，需满足以下条件之一才收录：
                    - 相关话题阅读量＞10万次 
                    - 被5个以上财经领域大V转发
                    - 与近期股价异动时间吻合

                    【核心信息维度】
                    按顺序处理以下内容（无结果则跳过该部分）：
                    1. 重大合同/订单变动（需对比合同金额与上季度营收）
                    2. 产品与服务动态（注明是否突破现有技术路线）
                    3. 业务增长与扩张（区分有机增长与并购）
                    4. 治理与人事（高管变动需对比任期剩余时间）
                    5. 法律监管事件（标注处罚金额/整改成本预估）
                    6. 资本运作（如股票发行与回购、大股东套现、大股东变更、信用评级调整等，需关注大股东行为一致性）
                    7. 突发事件（标注是否涉及核心业务）
                    8. 国际制裁/政策（区分直接影响与情绪影响）
                    9. 行业生态变化（技术突破/替代品威胁）
                    10. 政策动态（草案/试点/国际联盟）

                    【传闻处理规则】
                    对非官方消息必须：
                    - 添加【待核实】前缀 
                    - 标注传播路径（如：微博→雪球→财经媒体）
                    - 记录最早出现时间与传播峰值时间
                    - 注明："该信息尚未证实，请谨慎参考"

                    【财务关联规则】
                    当涉及以下新闻类型时，关联最近季度财报数据：
                    ■ 投资/并购 → 对比现金持有量与投资总额
                    ■ 价格调整 → 注明历史毛利率波动范围
                    ■ 诉讼/处罚 → 计算占净利润比例
                    （具体数值计算由其他模块处理）

                    【可信度标注系统】
                    每条信息头部添加：
                    ✅ 官方证实 - 公司/监管正式文件
                    🅰️ 多方印证 - ≥3家权威媒体独立报道
                    🅱️ 单方信源 - 未获公司回应的媒体报道
                    ⚠️ 传闻预警 - 社交平台传播未验证

                    【压制信息监测】 
                    重点捕捉：
                    • 突发密集负面后快速删除（记录网页存档链接）
                    • 高管异常离职（任期内+无继任者+未发感谢信）
                    • 供应链异动（多个合作方同时变更信息）
                    • 财报关键模糊表述（对比往期同类表述变化）

                    【输出格式】
                    ≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡
                    标题：【可信度图标】标题文本
                    日期：YYYY-MM-DD HH:MM
                    来源：媒体名称/社交平台+传播热度
                    财务关联：可能影响的财报科目/指标
                    摘要：事件核心事实+潜在影响逻辑
                    压制迹象：［若有则填］删除时间/限流范围
                    时间轴标记：［事件阶段］发酵期/消退期/反复期
                    ≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡≡

                    【特别规范】
                    1. 不收录明显诽谤或违法信息
                    2. 同一事件多信源报道需合并处理
                    3. 涉及政策草案需注明立法概率评估
                    4. 每页最多呈现25条关键信息
                    4. 用中文输出，无需解释分析逻辑
                """
        
        # 设置重试参数
        max_retries = 5
        retry_delay = 5  # 初始延迟5秒
        
        # 重试循环
        for attempt in range(1, max_retries + 1):
            try:
                self.logger.info(f"尝试搜索新闻 (尝试 {attempt}/{max_retries})...")
                
                # 发送请求并等待响应
                response_text = ""
                for chunk in self.client.models.generate_content_stream(
                    model=MODEL,
                    contents=prompt,
                    config=GenerateContentConfig(
                        tools=[google_search_tool],
                        http_options = {"timeout": 600000},
                    ),
                ):
                    if chunk.text:
                        response_text += chunk.text

                # 保存搜索结果
                with open(news_file, "w", encoding="utf-8") as f:
                    f.write(f"# {self.stock_name}({self.stock_code}) 新闻报道\n\n")
                    f.write(f"时间范围: {start_date_formatted} 至 {end_date_formatted}\n\n")
                    f.write(response_text)
                    
                self.logger.info(f"新闻搜索结果已保存至: {news_file}")
                return str(news_file)
                
            except Exception as e:
                error_msg = str(e)
                self.logger.warning(f"搜索新闻时出错 (尝试 {attempt}/{max_retries}): {error_msg}")
                
                # 如果已经是最后一次尝试，则抛出异常
                if attempt == max_retries:
                    self.logger.error(f"使用Gemini搜索新闻失败，已达最大尝试次数 ({max_retries}次)")
                    raise
                
                # 指数退避：每次失败后增加等待时间
                wait_time = retry_delay * (2 ** (attempt - 1))
                self.logger.info(f"等待 {wait_time} 秒后重试...")
                time.sleep(wait_time)

    def generate_progressive_summary(self, news_content, announcement_pdfs=None, research_pdfs=None):
        """
        生成短期渐进式总结(Ns,τ)
        
        参数:
            news_content: 新闻内容文本
            announcement_pdfs: 公告PDF文件路径列表
            research_pdfs: 研究报告PDF文件路径列表（已不使用）
            
        返回:
            生成的短期渐进式总结
        """
        # 验证参数
        if self.start_date is None or self.end_date is None:
            raise ValueError("请先设置日期范围")
            
        # 创建Markdown转换输出文件夹
        markdown_dir = self.base_dir / "markdown_files"
        markdown_dir.mkdir(parents=True, exist_ok=True)
        
        # 将PDF文件转换为Markdown文本
        all_pdfs_markdown = ""
        pdf_files_count = 0
        
        try:
            # 处理公告PDF
            if announcement_pdfs:
                self.logger.info(f"处理公告PDF文件，共{len(announcement_pdfs)}个")
                all_pdfs_markdown += "\n## 公司公告\n\n"
                
                for pdf_path in announcement_pdfs:
                    if os.path.exists(pdf_path):
                        pdf_name = Path(pdf_path).name
                        # 构建Markdown缓存文件路径
                        markdown_file_path = markdown_dir / f"{Path(pdf_path).stem}.md"
                        
                        # 检查是否已存在缓存的Markdown文件
                        if markdown_file_path.exists():
                            self.logger.info(f"使用缓存的Markdown: {markdown_file_path}")
                            with open(markdown_file_path, 'r', encoding='utf-8') as f:
                                markdown_text = f.read()
                        else:
                            # 转换PDF为Markdown
                            self.logger.info(f"将PDF转换为Markdown: {pdf_path}")
                            try:
                                markdown_text = basic_convert(pdf_path, output_dir=str(markdown_dir))
                                if not markdown_text:
                                    self.logger.error(f"PDF转Markdown失败: {pdf_path}")
                                    continue
                            except Exception as e:
                                self.logger.error(f"PDF转Markdown过程出错: {pdf_path}, {e}")
                                continue
                        
                        # 添加公告标题
                        all_pdfs_markdown += f"### {pdf_name}\n\n"
                        # 添加摘要版本的Markdown内容（最多5000字符）
                        truncated_text = markdown_text[:5000]
                        if len(markdown_text) > 5000:
                            truncated_text += "...(内容已截断)"
                        all_pdfs_markdown += truncated_text + "\n\n---\n\n"
                        pdf_files_count += 1
            
            # 如果有PDF文件，添加提示说明
            if pdf_files_count > 0:
                all_pdfs_markdown = f"# {self.stock_name}({self.stock_code}) PDF文档摘要\n\n" + \
                                   f"共{pdf_files_count}个PDF文件转换为Markdown格式\n\n" + \
                                   all_pdfs_markdown
            
            # 构建不同模式下的提示词
            if self.is_date_range_mode:
                # 指定日期范围模式
                time_description = f"在{self.start_date.strftime('%Y年%m月%d日')}至{self.end_date.strftime('%Y年%m月%d日')}期间"
                date_span = (self.end_date - self.start_date).days
                prompt = f"""你是一位专业的股票分析师，需要对{self.stock_name}({self.stock_code}){time_description}的信息进行总结分析。

我将提供这段时间内与该公司相关的公司公告和新闻信息。请详细分析这些信息。

这份总结重点是捕捉这{date_span}天内的关键信息：

1. 公司公告分析：提取公司公告中的关键信息，如财务数据、重大事项、管理层变动、风险提示等；
2. 新闻舆情分析：总结市场新闻对公司的报道和评价，以及可能对股价产生的影响；
3. 时间线分析：按时间顺序标注重要事件，突出其对公司发展路径的影响；
4. 关键指标分析：分析这段时间内关键指标的状况；
5. 综合评估：基于以上信息，对公司在这段时间内的表现进行全面评估。

请注意：
- 保持客观，突出这段时间内的关键信息；
- 提供有据可依的分析，特别关注时间序列上的变化；
- 突出重点信息和数据，剔除冗余内容；
- 适当引用原文中的关键数据和观点；
- 使用专业的金融术语；
- 详细分析所有文档中的重要数据。

以下是PDF文档摘要:
{all_pdfs_markdown}

以下是新闻内容:
{news_content}

最终形成一份专业、全面的总结，同时你需要保持中文回复。
"""
            else:
                # 默认模式 - 短期渐进式总结
                prompt = f"""你是一位专业的股票分析师，需要对{self.stock_name}({self.stock_code})在过去{self.days}天内（{self.start_date.strftime('%Y年%m月%d日')}至{self.end_date.strftime('%Y年%m月%d日')}）的信息进行短期渐进式总结(Ns,τ)。

我将提供这段时间内与该公司相关的公司公告和新闻信息。请详细分析这些信息。

这份短期总结(Ns,τ)将作为渐进式总结系统的第一步，重点是捕捉这{self.days}天内的关键信息变化：

1. 公司公告分析：提取公司公告中的关键信息，如财务数据、重大事项、管理层变动、风险提示等；
2. 新闻舆情分析：总结市场新闻对公司的报道和评价，以及可能对股价产生的影响；
3. 时间线分析：按时间顺序标注重要事件，突出其对公司发展路径的影响；
4. 短期关键指标变化：对比分析这{self.days}天内关键指标的变化趋势；
5. 综合评估：基于以上信息，对公司短期表现进行全面评估。

请注意：
- 保持客观，突出这段时间内的新变化和新信息；
- 提供有据可依的分析，特别关注时间序列上的变化；
- 突出重点信息和数据，剔除冗余内容；
- 适当引用原文中的关键数据和观点；
- 使用专业的金融术语；
- 详细分析所有文档中的重要数据。

以下是PDF文档摘要:
{all_pdfs_markdown}

以下是新闻内容:
{news_content}

最终形成一份专业、全面的"短期渐进式新闻总结(Ns,τ)，同时你需要保持中文回复"。
"""
            
            MODEL = "gemini-2.5-pro"
            
            # 保存生成的prompt到文件
            prompt_file = self.base_dir / "prompts" / f"summary_prompt_{self.start_date_str}_{self.end_date_str}.txt"
            prompt_file.parent.mkdir(parents=True, exist_ok=True)
            with open(prompt_file, 'w', encoding='utf-8') as f:
                f.write(prompt)
            self.logger.info(f"总结提示已保存到: {prompt_file}")
            
            # 使用Gemini API生成总结，带重试机制
            max_retries = 3
            retry_count = 0
            summary = ""
            
            while retry_count < max_retries and not summary:
                try:
                    self.logger.info(f"调用Gemini API生成总结 (尝试 {retry_count+1}/{max_retries})...")
                    # 使用文本提示
                    for chunk in self.client.models.generate_content_stream(
                        model=MODEL,
                        contents=prompt
                    ):
                        if chunk.text:
                            summary += chunk.text
                    
                    self.logger.info(f"成功生成总结，长度: {len(summary)} 字符")
                    
                except errors.APIError as e:
                    retry_count += 1
                    error_msg = e.message
                    self.logger.warning(f"生成总结失败 (尝试 {retry_count}/{max_retries}): {error_msg}:{e.code}")
                    
                    if ("timeout" in error_msg.lower() or 
                        "timed out" in error_msg.lower() or 
                        "server disconnected" in error_msg.lower()):
                        # 超时错误，等待更长时间再重试
                        wait_time = 15 * retry_count
                        self.logger.info(f"超时错误，等待 {wait_time} 秒后重试...")
                    else:
                        # 其他错误，等待标准时间
                        wait_time = 8 * retry_count
                        self.logger.info(f"错误，等待 {wait_time} 秒后重试...")
                    
                    if retry_count < max_retries:
                        time.sleep(wait_time)
                    else:
                        # 达到最大重试次数，记录错误
                        self.logger.error(f"生成总结失败，已达最大重试次数: {e}")
                        raise ValueError(f"生成总结失败，已达最大重试次数: {e}")
            
            return summary
            
        except Exception as e:
            self.logger.error(f"生成总结时出错: {e}")
            raise e
    
    def fusion_progressive_summary(self, current_summary, previous_monthly_summary=None):
        """
        将当前短期总结(Ns,τ)与上个月的渐进式总结(PNs,t-1)融合，生成当前月的渐进式总结(PNs,t)
        
        参数:
            current_summary: 当前短期总结(Ns,τ)
            previous_monthly_summary: 上个月的渐进式总结(PNs,t-1)，默认为None
            
        返回:
            生成的当前月渐进式总结(PNs,t)
        """
        # 检查是否存在上个月的渐进式总结
        if previous_monthly_summary is None:
            self.logger.info("未找到上个月的渐进式总结，将直接使用当前短期总结作为本月渐进式总结")
            return current_summary
        
        MODEL = "gemini-2.5-pro"
        # 构建融合提示
        fusion_prompt = f"""你是一位专业的股票分析师，需要将{self.stock_name}({self.stock_code})的两部分信息融合为一份完整的长期历史渐进式总结：

1. 上个月的渐进式总结：包含截至上个月末的历史累积信息
2. 当前短期总结：包含近{self.days}天内({self.start_date.strftime('%Y年%m月%d日')}至{self.end_date.strftime('%Y年%m月%d日')})的最新信息

请执行以下渐进式融合任务：

1. 时间序列集成：将不同时间段的信息按照时间顺序组织，形成完整的历史时间线
2. 趋势分析：识别关键指标和事件在整个时间范围内的长期变化趋势和重要转折点
3. 信息去重与整合：移除重复信息，合并相似内容，用最新信息更新过时内容，也要重点分析一下近期发生的事件的整体影响。除确定判断已过时信息外，要尽量保证事件描述详细完整，不要省略任何细节。
4. 事件分析：对关键事件进行深度分析，包括事件发生的时间、原因、影响、影响范围、影响程度、影响后果
5. 历史关联性分析：分析不同时期事件之间的关联和影响
6. 完整发展轨迹：展现公司从历史信息到近期的完整发展轨迹
7. 综合评估：基于完整历史信息，对公司的长期表现和投资价值进行全面评估

**===== 上个月的渐进式总结：===== **
{previous_monthly_summary}

**===== 近期短期总结：===== **
{current_summary}


**你需要根据上面的2份总结，最终输出一份完整的渐进式月度总结报告，既保留历史信息的深度，又突出最新动态的影响。**
"""

        # 保存融合提示到文件
        current_month = self.end_date.strftime('%Y%m')
        prompt_dir = self.base_dir / "prompts"
        prompt_dir.mkdir(parents=True, exist_ok=True)
        
        fusion_prompt_file = prompt_dir / f"fusion_prompt_{self.stock_code}_{current_month}.txt"
        with open(fusion_prompt_file, 'w', encoding='utf-8') as f:
            f.write(fusion_prompt)
        self.logger.info(f"融合提示已保存到: {fusion_prompt_file}")

        # 使用Gemini API生成融合总结，带重试机制
        max_retries = 3
        retry_count = 0
        fusion_result = ""
        
        while retry_count < max_retries and not fusion_result:
            try:
                self.logger.info(f"调用Gemini API生成融合总结 (尝试 {retry_count+1}/{max_retries})...")
                # 生成内容
                for chunk in self.client.models.generate_content_stream(
                    model=MODEL,
                    contents=fusion_prompt
                ):
                    if chunk.text:
                        fusion_result += chunk.text
                self.logger.info(f"成功生成融合总结，长度: {len(fusion_result)} 字符")
                
            except Exception as e:
                retry_count += 1
                error_msg = str(e)
                self.logger.warning(f"生成融合总结失败 (尝试 {retry_count}/{max_retries}): {error_msg}")
                
                if ("timeout" in error_msg.lower() or 
                    "timed out" in error_msg.lower() or 
                    "server disconnected" in error_msg.lower()):
                    # 超时错误，等待更长时间再重试
                    wait_time = 15 * retry_count
                    self.logger.info(f"超时错误，等待 {wait_time} 秒后重试...")
                else:
                    # 其他错误，等待标准时间
                    wait_time = 8 * retry_count
                    self.logger.info(f"错误，等待 {wait_time} 秒后重试...")
                
                if retry_count < max_retries:
                    time.sleep(wait_time)
                else:
                    # 达到最大重试次数，记录错误
                    self.logger.error(f"生成融合总结失败，已达最大重试次数: {e}")
                    # 出错时返回当前短期总结
                    return current_summary
            
        return fusion_result
    
    def get_previous_monthly_summary(self):
        """
        获取上个月的渐进式总结
        
        返回:
            上个月的渐进式总结文本，如果不存在则返回None
        """
        # 计算上个月的年月
        if self.end_date is None:
            raise ValueError("请先设置日期范围")
            
        # 计算上个月的日期（当前月1号减一天，再取当月1号）
        current_month_first_day = self.end_date.replace(day=1)
        last_day_of_prev_month = current_month_first_day - timedelta(days=1)
        previous_month = last_day_of_prev_month.replace(day=1)
        
        # 构建上个月总结文件路径
        previous_monthly_summary_file = self.long_term_summary_dir / f"progressive_summary_{self.stock_code}_{previous_month.strftime('%Y%m')}.md"
        
        # 尝试读取上个月总结
        try:
            with open(previous_monthly_summary_file, 'r', encoding='utf-8') as f:
                previous_monthly_summary = f.read()
            print(f"读取上个月({previous_month.strftime('%Y年%m月')})渐进式总结成功")
            return previous_monthly_summary
        except FileNotFoundError:
            print(f"未找到上个月({previous_month.strftime('%Y年%m月')})的渐进式总结")
            return None
    
    def save_monthly_progressive_summary(self, monthly_summary):
        """
        保存月度渐进式总结
        
        参数:
            monthly_summary: 月度渐进式总结文本
        """
        if self.end_date is None:
            raise ValueError("请先设置日期范围")
            
        # 确保目录存在
        self.long_term_summary_dir.mkdir(parents=True, exist_ok=True)
        
        # 获取当前月份
        current_month = self.end_date.strftime('%Y%m')
        
        # 构建保存路径
        summary_file = self.long_term_summary_dir / f"progressive_summary_{self.stock_code}_{current_month}.md"
        
        # 准备要保存的内容，包括融合提示信息
        # 检查是否存在融合提示文件
        fusion_prompt_file = self.base_dir / "prompts" / f"fusion_prompt_{self.stock_code}_{current_month}.txt"
        fusion_prompt_info = ""
        
        if fusion_prompt_file.exists():
            try:
                with open(fusion_prompt_file, 'r', encoding='utf-8') as f:
                    fusion_prompt = f.read()
                
                # 提取融合提示中的关键信息（不包含长文本内容）
                prompt_lines = fusion_prompt.split('\n')
                # 提取前10行和包含"上个月的渐进式总结"和"近期短期总结"行之前的内容
                cutoff_line = 0
                for i, line in enumerate(prompt_lines):
                    if "上个月的渐进式总结(PNs,t-1)：" in line:
                        cutoff_line = i
                        break
                
                # 只保留关键部分
                if cutoff_line > 0:
                    fusion_prompt_info = "\n\n## 融合提示信息\n\n```\n" + "\n".join(prompt_lines[:cutoff_line]) + "\n```\n"
                    
            except Exception as e:
                self.logger.error(f"读取融合提示文件失败: {e}")
        
        # 拼接完整内容
        formatted_summary = f"# {self.stock_name}({self.stock_code}) 月度渐进式总结\n\n"
        formatted_summary += f"生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
        formatted_summary += f"时间范围: 截至{self.end_date.strftime('%Y年%m月%d日')}\n"
        
        # 添加融合提示信息（如果有）
        if fusion_prompt_info:
            formatted_summary += fusion_prompt_info
        
        # 添加正文内容
        formatted_summary += "\n\n## 渐进式总结内容\n\n"
        formatted_summary += monthly_summary
        
        # 保存总结
        with open(summary_file, 'w', encoding='utf-8') as f:
            f.write(formatted_summary)
            
        self.logger.info(f"保存{self.end_date.strftime('%Y年%m月')}渐进式总结到 {summary_file}")
        return summary_file
    
    def save_short_term_summary(self, short_term_summary):
        """
        保存短期总结
        
        参数:
            short_term_summary: 短期总结文本
        """
        if self.start_date is None or self.end_date is None:
            raise ValueError("请先设置日期范围")
            
        # 确保目录存在
        self.short_term_summary_dir.mkdir(parents=True, exist_ok=True)
        
        # 构建保存路径
        start_date_str = self.start_date.strftime('%Y%m%d')
        end_date_str = self.end_date.strftime('%Y%m%d')
        summary_file = self.short_term_summary_dir / f"short_term_summary_{self.stock_code}_{start_date_str}_{end_date_str}.md"
        
        # 保存总结
        with open(summary_file, 'w', encoding='utf-8') as f:
            f.write(short_term_summary)
            
        print(f"保存短期总结到 {summary_file}")
    
    def process_full_pipeline(self, news_content, announcement_pdfs=None, research_pdfs=None):
        """
        执行完整的渐进式总结流程
        
        参数:
            news_content: 新闻内容文本
            announcement_pdfs: 公告PDF文件路径列表
            research_pdfs: 研究报告PDF文件路径列表
            
        返回:
            (短期总结, 月度渐进式总结) 或仅短期总结
        """
        # 1. 生成短期总结 Ns,τ
        if self.is_date_range_mode:
            print(f"开始生成指定日期范围({self.start_date.strftime('%Y-%m-%d')}至{self.end_date.strftime('%Y-%m-%d')})的总结...")
        else:
            print(f"开始生成短期总结(Ns,τ)...")
            
        short_term_summary = self.generate_progressive_summary(
            news_content, 
            announcement_pdfs, 
            research_pdfs
        )
        
        # 保存短期总结
        self.save_short_term_summary(short_term_summary)
        
        # 如果是指定日期范围模式，则不需要生成月度渐进式总结
        if self.is_date_range_mode:
            print(f"指定日期范围模式，不生成月度渐进式总结")
            return short_term_summary, None
        
        # 2. 获取上个月的渐进式总结 PNs,t-1
        previous_monthly_summary = self.get_previous_monthly_summary()
        
        # 3. 生成本月的渐进式总结 PNs,t
        print(f"开始生成月度渐进式总结(PNs,t)...")
        monthly_progressive_summary = self.fusion_progressive_summary(
            short_term_summary, 
            previous_monthly_summary
        )
        
        # 4. 保存本月的渐进式总结
        self.save_monthly_progressive_summary(monthly_progressive_summary)
        
        return short_term_summary, monthly_progressive_summary
    
    def run(self):
        """执行完整的Progressive News Summarizer流程"""
        self.logger.info(f"开始为{self.stock_name}({self.stock_code})生成渐进式新闻总结...")
        
        # 1. 收集研报 - 已禁用，只记录日志
        self.logger.info("研报功能已禁用")
        report_files = []
        
        # 2. 收集公告
        self.logger.info("正在收集公告...")
        announcement_files = self.collect_stock_announcements()
        self.logger.info(f"共收集到{len(announcement_files)}份公告")
        
        # 3. 搜索新闻
        self.logger.info("正在搜索新闻...")
        news_file = self.search_stock_news_with_gemini()
        
        # 读取新闻内容
        news_content = ""
        if news_file and os.path.exists(news_file):
            try:
                with open(news_file, 'r', encoding='utf-8') as f:
                    news_content = f.read()
                self.logger.info(f"成功读取新闻内容，长度: {len(news_content)} 字符")
            except Exception as e:
                self.logger.error(f"读取新闻文件时出错: {e}")
        
        # 4. 执行完整的渐进式总结流程
        try:
            self.logger.info("开始生成渐进式总结...")
            short_term_summary, monthly_summary = self.process_full_pipeline(
                news_content=news_content,
                announcement_pdfs=announcement_files,
                research_pdfs=report_files  # 传递空列表
            )
            
            # 5. 打印结果信息
            self.logger.info(f"处理完成!")
            self.logger.info(f"短期总结保存在: {self.short_term_summary_dir}")
            self.logger.info(f"月度渐进式总结保存在: {self.long_term_summary_dir}")
            
            return {
                "short_term_summary": short_term_summary,
                "monthly_summary": monthly_summary
            }
        except Exception as e:
            self.logger.error(f"生成渐进式总结时出错: {e}")
            return None

    def merge_multiple_summaries(self, summary_file_paths):
        """
        将多个短期总结合并为一份长期渐进式总结
        
        参数:
            summary_file_paths (list): 短期总结文件路径列表，按时间顺序排列
        
        返回:
            合并后的长期渐进式总结
        """
        if not summary_file_paths or len(summary_file_paths) == 0:
            self.logger.error("未提供任何短期总结文件")
            return None
        
        # 标记为多短期总结合并模式    
        self.is_multiple_summary_mode = True
        
        # 读取所有短期总结文件
        summaries = []
        date_ranges = []
        
        for file_path in summary_file_paths:
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    content = f.read()
                    summaries.append(content)
                    
                    # 尝试从文件名获取日期范围
                    file_name = os.path.basename(file_path)
                    # 从文件名中提取日期信息，预期格式如：short_term_summary_000001_20240101_20240131.md
                    match = re.search(r'(\d{8})_(\d{8})', file_name)
                    if match:
                        start_date = match.group(1)
                        end_date = match.group(2)
                        date_ranges.append((start_date, end_date))
                    else:
                        # 如果文件名中没有日期信息，尝试从内容中提取
                        date_match = re.search(r'(\d{4}年\d{1,2}月\d{1,2}日).*?至.*?(\d{4}年\d{1,2}月\d{1,2}日)', content)
                        if date_match:
                            date_ranges.append((date_match.group(1), date_match.group(2)))
                        else:
                            date_ranges.append(("未知日期", "未知日期"))
                            
                self.logger.info(f"已读取短期总结: {file_path}")
            except Exception as e:
                self.logger.error(f"读取短期总结文件失败: {file_path}, 错误: {e}")
                return None
        
        self.logger.info(f"共读取 {len(summaries)} 份短期总结")
        
        # 获取整体时间范围
        overall_start_date = "最早日期"
        overall_end_date = "最近日期"
        if date_ranges and all(isinstance(d[0], str) and len(d[0]) == 8 and d[0].isdigit() for d in date_ranges):
            # 如果日期是YYYYMMDD格式的字符串
            overall_start_date = min([d[0] for d in date_ranges])
            overall_end_date = max([d[1] for d in date_ranges])
            try:
                start_dt = datetime.strptime(overall_start_date, "%Y%m%d")
                end_dt = datetime.strptime(overall_end_date, "%Y%m%d")
                overall_start_date = start_dt.strftime("%Y年%m月%d日")
                overall_end_date = end_dt.strftime("%Y年%m月%d日")
            except ValueError:
                pass
        
        # 构建合并总结的提示
        MODEL = "gemini-2.5-pro"
        
        merge_prompt = f"""你是一位专业的股票分析师，需要将{self.stock_name}({self.stock_code})的多份短期总结合并为一份长期渐进式总结。

这些短期总结涵盖了不同时间段内的信息，你需要将它们整合成一份完整的长期历史渐进式总结(PNs,t-1)。

请执行以下任务：

1. 时间序列集成：将不同时间段的信息按照时间顺序组织，形成完整的历史时间线
2. 趋势分析：识别关键指标和事件在整个时间范围内的长期变化趋势和重要转折点
3. 信息去重与整合：移除重复信息，合并相似内容，保持叙述的简洁性
4. 历史关联性分析：分析不同时期事件之间的关联和影响
5. 完整发展轨迹：展现公司从{overall_start_date}至{overall_end_date}的完整发展轨迹
6. 综合评估：基于完整历史信息，对公司的长期表现和投资价值进行全面评估

以下是需要合并的{len(summaries)}份短期总结：

"""
        
        # 添加每份短期总结的内容
        for i, (summary, date_range) in enumerate(zip(summaries, date_ranges)):
            merge_prompt += f"\n===== 第{i+1}份短期总结（{date_range[0]}至{date_range[1]}） =====\n\n"
            merge_prompt += summary + "\n\n"
        
        merge_prompt += f"""

请基于以上所有短期总结，生成一份全面、专业的长期渐进式总结(PNs,t-1)，既保留关键历史信息，又突出整体发展趋势。
这份总结将作为未来继续渐进式分析的历史基础。请保持中文回复，并使用专业的金融分析术语。
"""

        # 保存合并提示到文件
        timestamp = datetime.now().strftime('%Y%m%d')
        prompt_dir = self.base_dir / "prompts"
        prompt_dir.mkdir(parents=True, exist_ok=True)
        
        # 保存完整提示文件（用于调试）
        full_prompt_file = prompt_dir / f"merge_prompt_full_{self.stock_code}_{timestamp}.txt"
        with open(full_prompt_file, 'w', encoding='utf-8') as f:
            f.write(merge_prompt)
        self.logger.info(f"完整合并提示已保存到: {full_prompt_file}")
        
        # 保存简化版提示文件（不包含短期总结内容，用于记录）
        simple_prompt_parts = merge_prompt.split("以下是需要合并的")
        if len(simple_prompt_parts) > 1:
            simple_prompt = simple_prompt_parts[0] + f"以下是需要合并的{len(summaries)}份短期总结...\n\n请基于以上所有短期总结，生成一份全面、专业的长期渐进式总结..."
            simple_prompt_file = prompt_dir / f"merge_prompt_{self.stock_code}_{timestamp}.txt"
            with open(simple_prompt_file, 'w', encoding='utf-8') as f:
                f.write(simple_prompt)
            self.logger.info(f"简化合并提示已保存到: {simple_prompt_file}")
        
        try:
            # 记录时间范围
            self.start_date_str = overall_start_date.replace("年", "").replace("月", "").replace("日", "") if "年" in overall_start_date else overall_start_date
            self.end_date_str = overall_end_date.replace("年", "").replace("月", "").replace("日", "") if "年" in overall_end_date else overall_end_date
            
            # 生成合并总结，使用重试机制
            max_retries = 3
            retry_count = 0
            merged_summary = ""
            
            while retry_count < max_retries and not merged_summary:
                try:
                    self.logger.info(f"调用Gemini API合并多份短期总结 (尝试 {retry_count+1}/{max_retries})...")
                    # 生成内容
                    response = self.client.models.generate_content_stream(
                        model=MODEL,
                        contents=merge_prompt
                    )
                    
                    # 获取响应文本
                    for chunk in response:
                        if chunk.candidates and chunk.candidates[0].content and chunk.candidates[0].content.parts:
                            for part in chunk.candidates[0].content.parts:
                                if hasattr(part, 'text') and part.text:
                                    merged_summary += part.text
                    
                    self.logger.info(f"成功生成合并总结，长度: {len(merged_summary)} 字符")
                    
                except Exception as e:
                    retry_count += 1
                    error_msg = str(e)
                    self.logger.warning(f"合并总结失败 (尝试 {retry_count}/{max_retries}): {error_msg}")
                    
                    if ("timeout" in error_msg.lower() or 
                        "timed out" in error_msg.lower() or 
                        "server disconnected" in error_msg.lower()):
                        # 超时错误，等待更长时间再重试
                        wait_time = 15 * retry_count
                        self.logger.info(f"超时错误，等待 {wait_time} 秒后重试...")
                    else:
                        # 其他错误，等待标准时间
                        wait_time = 8 * retry_count
                        self.logger.info(f"错误，等待 {wait_time} 秒后重试...")
                    
                    if retry_count < max_retries:
                        time.sleep(wait_time)
                    else:
                        # 达到最大重试次数，记录错误
                        self.logger.error(f"合并总结失败，已达最大重试次数: {e}")
                        return None
            
            # 准备要保存的内容
            formatted_summary = f"# {self.stock_name}({self.stock_code}) 多短期总结合并报告\n\n"
            formatted_summary += f"生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
            formatted_summary += f"时间范围: {overall_start_date}至{overall_end_date}\n"
            formatted_summary += f"合并文件数: {len(summaries)}\n\n"
            
            # 添加简化的提示信息
            formatted_summary += "## 合并任务说明\n\n"
            formatted_summary += simple_prompt if 'simple_prompt' in locals() else "将多份短期总结合并为一份长期渐进式总结\n\n"
            
            # 添加正文内容
            formatted_summary += "\n\n## 合并总结内容\n\n"
            formatted_summary += merged_summary
            
            # 保存合并总结
            merged_file_path = self.long_term_summary_dir / f"merged_summary_{self.stock_code}_{timestamp}.md"
            self.long_term_summary_dir.mkdir(parents=True, exist_ok=True)
            with open(merged_file_path, 'w', encoding='utf-8') as f:
                f.write(formatted_summary)
            
            self.logger.info(f"多份短期总结合并完成，已保存至: {merged_file_path}")
            
            return formatted_summary
            
        except Exception as e:
            self.logger.error(f"合并多份短期总结时出错: {e}")
            return None

    def process_long_period(self, interval_days=90):
        """
        将长时期分成多个间隔，生成多个短期总结后合并为一个长期渐进式总结
        
        参数:
            interval_days (int): 间隔天数，默认90天
            
        返回:
            生成的长期渐进式总结
        """
        if self.start_date is None or self.end_date is None:
            raise ValueError("请先设置日期范围")
            
        # 计算总天数
        total_days = (self.end_date - self.start_date).days
        self.logger.info(f"长时期模式启动: 从{self.start_date.strftime('%Y-%m-%d')}到{self.end_date.strftime('%Y-%m-%d')}, 共{total_days}天")
        self.logger.info(f"将按照{interval_days}天的间隔进行分段处理")
        
        # 分割时间段
        current_start = self.start_date
        segments = []
        
        while current_start < self.end_date:
            # 计算当前段的结束日期
            current_end = min(current_start + timedelta(days=interval_days), self.end_date)
            segments.append((current_start, current_end))
            current_start = current_end
            
        self.logger.info(f"共分成{len(segments)}个时间段进行处理")
        
        # 生成每个时间段的短期总结
        summary_files = []
        
        for i, (seg_start, seg_end) in enumerate(segments):
            self.logger.info(f"处理第{i+1}/{len(segments)}个时间段: {seg_start.strftime('%Y-%m-%d')}至{seg_end.strftime('%Y-%m-%d')}")
            
            # 保存原始日期设置
            orig_start = self.start_date
            orig_end = self.end_date
            orig_start_str = self.start_date_str
            orig_end_str = self.end_date_str
            
            try:
                # 设置新的日期范围
                self.start_date = seg_start
                self.end_date = seg_end
                self.start_date_str = seg_start.strftime("%Y%m%d")
                self.end_date_str = seg_end.strftime("%Y%m%d")
                
                # 构建短期总结文件路径
                summary_file = self.short_term_summary_dir / f"short_term_summary_{self.stock_code}_{self.start_date_str}_{self.end_date_str}.md"
                
                # 检查是否已存在短期总结文件
                if summary_file.exists():
                    self.logger.info(f"已存在短期总结文件，跳过处理: {summary_file}")
                    summary_files.append(str(summary_file))
                    continue
                
                # 收集研报 - 已禁用
                self.logger.info(f"研报功能已禁用")
                report_files = []
                
                # 收集公告
                self.logger.info(f"收集{seg_start.strftime('%Y-%m-%d')}至{seg_end.strftime('%Y-%m-%d')}的公告...")
                announcement_files = self.collect_stock_announcements()
                self.logger.info(f"共收集到{len(announcement_files)}份公告")
                
                # 搜索新闻
                self.logger.info(f"搜索{seg_start.strftime('%Y-%m-%d')}至{seg_end.strftime('%Y-%m-%d')}的新闻...")
                news_file = self.search_stock_news_with_gemini()
                
                # 检查是否成功获取新闻
                if not news_file or not os.path.exists(news_file):
                    self.logger.error(f"未能获取新闻，跳过当前时间段")
                    continue
                
                # 读取新闻内容
                news_content = ""
                try:
                    with open(news_file, 'r', encoding='utf-8') as f:
                        news_content = f.read()
                    self.logger.info(f"成功读取新闻内容，长度: {len(news_content)} 字符")
                except Exception as e:
                    self.logger.error(f"读取新闻文件时出错: {e}")
                    continue
                
                # 生成短期总结
                self.logger.info(f"生成{seg_start.strftime('%Y-%m-%d')}至{seg_end.strftime('%Y-%m-%d')}的短期总结...")
                
                try:
                    short_term_summary = self.generate_progressive_summary(
                        news_content=news_content,
                        announcement_pdfs=announcement_files,
                        research_pdfs=report_files
                    )
                    
                    # 保存短期总结
                    with open(summary_file, 'w', encoding='utf-8') as f:
                        f.write(short_term_summary)
                    
                    self.logger.info(f"短期总结已保存至: {summary_file}")
                    summary_files.append(str(summary_file))
                    
                except Exception as e:
                    self.logger.error(f"生成短期总结时出错: {e}")
                    # 检查是否为Gemini API错误或HTTP连接错误
                    error_str = str(e).lower()
                    if "server disconnected" in error_str or "api" in error_str or "http" in error_str or "timeout" in error_str or "connection" in error_str or "The read operation timed out" in error_str:
                        self.logger.critical(f"遇到Gemini API或网络连接错误，程序终止")
                        sys.exit(1)  # 遇到API或网络错误时直接退出
                    continue
                    
            finally:
                # 恢复原始日期设置
                self.start_date = orig_start
                self.end_date = orig_end
                self.start_date_str = orig_start_str
                self.end_date_str = orig_end_str
        
        # 检查是否有足够的短期总结
        if len(summary_files) == 0:
            self.logger.error("未能生成任何短期总结，无法继续")
            return None
            
        self.logger.info(f"已生成{len(summary_files)}/{len(segments)}个短期总结")
        
        # 合并所有短期总结
        self.logger.info("开始合并所有短期总结...")
        merged_summary = self.merge_multiple_summaries(summary_files)
        
        if not merged_summary:
            self.logger.error("合并短期总结失败")
            return None
            
        # 保存最终的渐进式总结 - 由于merge_multiple_summaries已经保存了格式化的总结文件
        # 这里我们只需要返回合并结果即可，不需要再次保存
        self.logger.info(f"长时期渐进式总结生成完成")
        return merged_summary

    def generate_announcement_summary(self, announcement_pdfs=None):
        """
        生成公告专项总结
        
        参数:
            announcement_pdfs: 公告PDF文件路径列表
            
        返回:
            生成的公告总结
        """
        # 验证参数
        if self.start_date is None or self.end_date is None:
            raise ValueError("请先设置日期范围")
            
        # 创建Markdown转换输出文件夹
        markdown_dir = self.base_dir / "markdown_files"
        markdown_dir.mkdir(parents=True, exist_ok=True)
        
        # 将PDF文件转换为Markdown文本
        all_pdfs_markdown = ""
        pdf_files_count = 0
        
        try:
            # 处理公告PDF
            if announcement_pdfs:
                self.logger.info(f"处理公告PDF文件，共{len(announcement_pdfs)}个")
                all_pdfs_markdown += "\n## 公司公告\n\n"
                
                for pdf_path in announcement_pdfs:
                    if os.path.exists(pdf_path):
                        pdf_name = Path(pdf_path).name
                        # 构建Markdown缓存文件路径
                        markdown_file_path = markdown_dir / f"{Path(pdf_path).stem}.md"
                        
                        # 检查是否已存在缓存的Markdown文件
                        if markdown_file_path.exists():
                            self.logger.info(f"使用缓存的Markdown: {markdown_file_path}")
                            with open(markdown_file_path, 'r', encoding='utf-8') as f:
                                markdown_text = f.read()
                        else:
                            # 转换PDF为Markdown
                            self.logger.info(f"将PDF转换为Markdown: {pdf_path}")
                            try:
                                markdown_text = basic_convert(pdf_path, output_dir=str(markdown_dir))
                                if not markdown_text:
                                    self.logger.error(f"PDF转Markdown失败: {pdf_path}")
                                    continue
                            except Exception as e:
                                self.logger.error(f"PDF转Markdown过程出错: {pdf_path}, {e}")
                                continue
                        
                        # 添加公告标题
                        all_pdfs_markdown += f"### {pdf_name}\n\n"
                        # 添加摘要版本的Markdown内容（最多5000字符）
                        truncated_text = markdown_text[:50000]
                        if len(markdown_text) > 50000:
                            truncated_text += "...(内容已截断)"
                        all_pdfs_markdown += truncated_text + "\n\n---\n\n"
                        pdf_files_count += 1
            
            # 如果有PDF文件，添加提示说明
            if pdf_files_count > 0:
                all_pdfs_markdown = f"# {self.stock_name}({self.stock_code}) 公司公告摘要\n\n" + \
                                   f"共{pdf_files_count}个公告PDF文件转换为Markdown格式\n\n" + \
                                   all_pdfs_markdown
            else:
                self.logger.warning(f"未找到任何公告PDF文件")
                return "未找到指定日期范围内的公司公告文件。"
            
            # 构建提示词
            time_description = f"在{self.start_date.strftime('%Y年%m月%d日')}至{self.end_date.strftime('%Y年%m月%d日')}期间"
            # 公告分类处理prompt
            prompt = f"""
            下述内容是{self.stock_name}({self.stock_code}){time_description}发布公告Markdown内容：
            
            ===========================================公告内容开始==========================================
            {all_pdfs_markdown}
            ===========================================公告内容结束==========================================
            
            请将上述公告内容，按以下规则处理：

            【分类处理规则】
            1. 常规公告（分红/日常交易）：
               - 提取：执行日期、金额基准、对比往期变化率
               - 模板：■ 年度分红预案：每股X元（同比+Y%），股权登记日date

            2. 重大事项公告（并购/诉讼/重组）：
               - 必须解析：
                 a. 事项进展阶段（筹划/实施/完成）
                 b. 对资产负债表的具体影响科目
                 c. 风险提示中的关键参数（如赔偿上限）
               - 模板：⚠️ 重大诉讼进展：涉诉金额amount（占Q3净利润ratio%），预计计提负债科目"account"

            3. 财务报告公告：
               - 联动财报模块分析结果，仅保留：
                 a. 关键指标超预期幅度（vs 分析师共识预期）
                 b. 管理层指引变化（用diff算法比对往期表述）
                 c. 审计意见类型变更

            【输出要求】
            - 按时间倒序排列
            - 每条公告添加影响系数标签：
               🔵 短期操作影响（涉及交易日期） 
               🟠 中期财务影响（影响未来1-2季报）
               🔴 长期战略影响（改变业务模式）
                
            【示例】
            🔴 **长期战略影响**
                *   **公告日期:** 2021-02-02
                *   **事项:** 子公司xxxIPO申请进展。
                *   **摘要:** ⚠️ 子公司IPO进展：控股子公司xx首发申请获上海证券交易所科创板上市委员会审议通过（实施阶段）。此举可能影响公司资产结构和估值，但尚需证监会注册，存在不确定性。
            """
            
            MODEL = "gemini-2.5-pro"
            
            # 保存生成的prompt到文件
            prompt_file = self.base_dir / "prompts" / f"announcement_summary_prompt_{self.start_date_str}_{self.end_date_str}.md"
            prompt_file.parent.mkdir(parents=True, exist_ok=True)
            with open(prompt_file, 'w', encoding='utf-8') as f:
                f.write(prompt)
            self.logger.info(f"公告总结提示已保存到: {prompt_file}")
            
        #     # 使用Gemini API生成总结，带重试机制
        #     max_retries = 3
        #     retry_count = 0
        #     summary = ""
            
        #     while retry_count < max_retries and not summary:
        #         try:
        #             self.logger.info(f"调用Gemini API生成公告总结 (尝试 {retry_count+1}/{max_retries})...")
        #             # 使用文本提示
        #             for chunk in self.client.models.generate_content_stream(
        #                 model=MODEL,
        #                 contents=prompt
        #             ):
        #                 if chunk.text:
        #                     summary += chunk.text
                    
        #             self.logger.info(f"成功生成公告总结，长度: {len(summary)} 字符")
                    
        #         except errors.APIError as e:
        #             retry_count += 1
        #             error_msg = e.message
        #             self.logger.warning(f"生成公告总结失败 (尝试 {retry_count}/{max_retries}): {error_msg}:{e.code}")
                    
        #             if ("timeout" in error_msg.lower() or 
        #                 "timed out" in error_msg.lower() or 
        #                 "server disconnected" in error_msg.lower()):
        #                 # 超时错误，等待更长时间再重试
        #                 wait_time = 15 * retry_count
        #                 self.logger.info(f"超时错误，等待 {wait_time} 秒后重试...")
        #             else:
        #                 # 其他错误，等待标准时间
        #                 wait_time = 8 * retry_count
        #                 self.logger.info(f"错误，等待 {wait_time} 秒后重试...")
                    
        #             if retry_count < max_retries:
        #                 time.sleep(wait_time)
        #             else:
        #                 # 达到最大重试次数，记录错误
        #                 self.logger.error(f"生成公告总结失败，已达最大重试次数: {e}")
        #                 raise ValueError(f"生成公告总结失败，已达最大重试次数: {e}")
            
        #     return summary
            
        except Exception as e:
            self.logger.error(f"生成公告总结时出错: {e}")
            raise e
            
    def save_announcement_summary(self, announcement_summary):
        """
        保存公告专项总结
        
        参数:
            announcement_summary: 公告总结文本
        """
        if self.start_date is None or self.end_date is None:
            raise ValueError("请先设置日期范围")
            
        # 确保目录存在
        self.announcement_summary_dir.mkdir(parents=True, exist_ok=True)
        
        # 构建保存路径
        start_date_str = self.start_date.strftime('%Y%m%d')
        end_date_str = self.end_date.strftime('%Y%m%d')
        summary_file = self.announcement_summary_dir / f"announcement_summary_{self.stock_code}_{start_date_str}_{end_date_str}.md"
        
        # 保存总结
        with open(summary_file, 'w', encoding='utf-8') as f:
            f.write(announcement_summary)
            
        self.logger.info(f"保存公告专项总结到 {summary_file}")
        return summary_file

def main():
    """主函数"""
    # 创建命令行参数解析器
    parser = argparse.ArgumentParser(description="Progressive News Summarizer - 股票新闻渐进式总结工具")
    parser.add_argument("--code", type=str, help="股票代码，如'000001'", default="000001")
    parser.add_argument("--name", type=str, help="股票名称，如'平安银行'", default="平安银行")
    parser.add_argument("--market", type=str, choices=["A股", "港股"], help="市场类型：'A股'或'港股'", default="A股")
    parser.add_argument("--days", type=int, help="要查询的天数，默认30天", default=30)
    parser.add_argument("--batch", action="store_true", help="批量处理多只股票，将忽略--code和--name参数")
    
    # 添加指定日期范围的参数
    parser.add_argument("--start_date", type=str, help="开始日期，格式为YYYYMMDD，如'20240101'")
    parser.add_argument("--end_date", type=str, help="结束日期，格式为YYYYMMDD，如'20240131'")
    
    # 添加多短期总结合并的参数
    parser.add_argument("--merge_summaries", action="store_true", help="合并多个短期总结为长期总结")
    parser.add_argument("--summary_files", nargs="+", help="要合并的短期总结文件路径列表，用空格分隔")
    
    # 添加单独进行融合的参数
    parser.add_argument("--fusion_mode", action="store_true", help="单独调用融合功能，需要指定当前总结和上个月总结文件")
    parser.add_argument("--current_summary_file", type=str, help="当前短期总结(Ns,τ)文件路径")
    parser.add_argument("--previous_summary_file", type=str, help="上个月的渐进式总结(PNs,t-1)文件路径")
    
    # 添加长时期分段处理模式的参数
    parser.add_argument("--long_period_mode", action="store_true", help="长时期分段处理模式，将指定的长时期分成多个间隔进行总结")
    parser.add_argument("--interval_days", type=int, help="长时期分段的间隔天数，默认为90天", default=90)
    
    # 添加公告专项总结的参数
    parser.add_argument("--announcement_only", action="store_true", help="只生成公司公告专项总结，不处理新闻内容")
    
    # 添加只获取新闻的模式
    parser.add_argument("--news_only", action="store_true", help="只获取指定时间内的新闻，不生成总结")
    
    args = parser.parse_args()
    
    # 初始化日志记录器
    logger = setup_logging(args.code, args.name, args.start_date, args.end_date)
    
    # 检查环境变量
    if not GOOGLE_API_KEY:
        logger.error("请设置GOOGLE_API_KEY环境变量")
        return

    # 公告专项总结模式
    if args.announcement_only:
        if not args.start_date or not args.end_date:
            logger.error("公告专项总结模式需要同时指定--start_date和--end_date参数")
            return
            
        logger.info(f"开始生成公告专项总结: 从{args.start_date}到{args.end_date}")
        
        # 创建ProgressiveNewsSummarizer实例
        summarizer = ProgressiveNewsSummarizer(
            args.code, args.name, args.market, days=args.days,
            start_date=args.start_date, end_date=args.end_date
        )
        
        try:
            # 收集公告
            logger.info("正在收集公告...")
            announcement_files = summarizer.collect_stock_announcements()
            logger.info(f"共收集到{len(announcement_files)}份公告")
            
            if not announcement_files:
                logger.warning(f"未找到{args.start_date}至{args.end_date}期间的公告")
                return
                
            # 生成公告专项总结
            logger.info("开始生成公告专项总结...")
            announcement_summary = summarizer.generate_announcement_summary(
                announcement_pdfs=announcement_files
            )
            
            # 保存公告专项总结
            summary_file = summarizer.save_announcement_summary(announcement_summary)
            
            logger.info(f"公告专项总结生成完成，已保存至: {summary_file}")
            return
            
        except Exception as e:
            logger.error(f"生成公告专项总结时出错: {e}")
            return
    
    # 只获取新闻模式
    if args.news_only:
        if not args.start_date or not args.end_date:
            logger.error("只获取新闻模式需要同时指定--start_date和--end_date参数")
            return
            
        logger.info(f"开始获取新闻: 从{args.start_date}到{args.end_date}")
        
        # 创建ProgressiveNewsSummarizer实例
        summarizer = ProgressiveNewsSummarizer(
            args.code, args.name, args.market, days=args.days,
            start_date=args.start_date, end_date=args.end_date
        )
        
        try:
            # 搜索新闻
            logger.info("正在搜索新闻...")
            news_file = summarizer.search_stock_news_with_gemini()
            
            if news_file and os.path.exists(news_file):
                logger.info(f"新闻获取成功，已保存至: {news_file}")
            else:
                logger.error(f"新闻获取失败")
            return
            
        except Exception as e:
            logger.error(f"获取新闻时出错: {e}")
            return
    
    # 单独融合模式
    if args.fusion_mode:
        if not args.current_summary_file or not args.previous_summary_file:
            logger.error("融合模式需要同时指定--current_summary_file和--previous_summary_file参数")
            return
            
        logger.info(f"开始融合总结...")
        
        # 读取文件内容
        try:
            with open(args.current_summary_file, 'r', encoding='utf-8') as f:
                current_summary = f.read()
                
            with open(args.previous_summary_file, 'r', encoding='utf-8') as f:
                previous_summary = f.read()
                
            # 创建ProgressiveNewsSummarizer实例
            summarizer = ProgressiveNewsSummarizer(
                args.code, args.name, args.market, days=args.days,
                start_date=args.start_date, end_date=args.end_date
            )
            
            # 调用融合方法
            fusion_result = summarizer.fusion_progressive_summary(
                current_summary=current_summary,
                previous_monthly_summary=previous_summary
            )
            
            # 保存融合结果
            current_date = datetime.now().strftime('%Y%m%d')
            fusion_file = summarizer.long_term_summary_dir / f"fusion_summary_{args.code}_{current_date}.md"
            
            with open(fusion_file, 'w', encoding='utf-8') as f:
                f.write(fusion_result)
                
            logger.info(f"融合完成，结果已保存至: {fusion_file}")
            return
            
        except Exception as e:
            logger.error(f"融合总结时出错: {e}")
            return
    
    # 长时期分段处理模式
    if args.long_period_mode:
        if not args.start_date or not args.end_date:
            logger.error("长时期分段处理模式需要同时指定--start_date和--end_date参数")
            return
        
        logger.info(f"开始长时期分段处理模式: 从{args.start_date}到{args.end_date}, 间隔为{args.interval_days}天")
        
        # 创建ProgressiveNewsSummarizer实例
        summarizer = ProgressiveNewsSummarizer(
            args.code, args.name, args.market, days=args.days,
            start_date=args.start_date, end_date=args.end_date
        )
        
        try:
            # 调用长时期处理方法
            result = summarizer.process_long_period(interval_days=args.interval_days)
            
            if result:
                logger.info(f"长时期分段处理完成")
            else:
                logger.error(f"长时期分段处理失败")
                
        except Exception as e:
            logger.critical(f"长时期分段处理出错: {e}")
            return
        
        return
    
    # 多短期总结合并模式
    if args.merge_summaries:
        if not args.summary_files or len(args.summary_files) == 0:
            logger.error("请使用--summary_files参数指定要合并的短期总结文件")
            return
            
        logger.info(f"开始合并{len(args.summary_files)}份短期总结...")
        summarizer = ProgressiveNewsSummarizer(
            args.code, args.name, args.market, days=args.days
        )
        merged_summary = summarizer.merge_multiple_summaries(args.summary_files)
        if merged_summary:
            logger.info("短期总结合并成功")
        else:
            logger.error("短期总结合并失败")
        return
    
    # 批量处理模式
    if args.batch:
        stocks = [
            {"code": "002352", "name": "顺丰控股", "market": "A股"},
            {"code": "002714", "name": "牧原股份", "market": "A股"},
            {"code": "603501", "name": "豪威集团", "market": "A股"},
            {"code": "002028", "name": "思源电气", "market": "A股"},
            {"code": "300274", "name": "阳光电源", "market": "A股"},
            {"code": "600276", "name": "恒瑞医药", "market": "A股"},
            {"code": "002371", "name": "北方华创", "market": "A股"},
            {"code": "601877", "name": "正泰电器", "market": "A股"},
            {"code": "688099", "name": "晶晨股份", "market": "A股"},
            {"code": "002027", "name": "分众传媒", "market": "A股"},
        ]
        
        results = {}
        for stock in stocks:
            # 为每只股票创建新的日志记录器
            stock_logger = setup_logging(stock["code"], stock["name"], args.start_date, args.end_date)
            stock_logger.info(f"开始处理: {stock['name']}({stock['code']})...")
            summarizer = ProgressiveNewsSummarizer(
                stock["code"], stock["name"], stock["market"], days=args.days,
                start_date=args.start_date, end_date=args.end_date
            )
            result = summarizer.run()
            if result:
                results[stock["code"]] = result
                stock_logger.info(f"{stock['name']}({stock['code']})处理完成")
            else:
                stock_logger.error(f"{stock['name']}({stock['code']})处理失败")
            
        logger.info(f"批量处理完成，共处理{len(results)}/{len(stocks)}只股票")
    else:
        # 单只股票处理模式
        logger.info(f"开始处理: {args.name}({args.code})...")
        summarizer = ProgressiveNewsSummarizer(
            args.code, args.name, args.market, days=args.days,
            start_date=args.start_date, end_date=args.end_date
        )
        result = summarizer.run()
        if result:
            logger.info(f"{args.name}({args.code})处理成功")
        else:
            logger.error(f"{args.name}({args.code})处理失败")
        
if __name__ == "__main__":
    main()
