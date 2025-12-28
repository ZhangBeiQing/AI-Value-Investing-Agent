#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
PDF报告处理模块
负责下载和过滤财报PDF文件
"""

import os
import re
import json
import logging
import shutil
import httpx
import time
from pathlib import Path
from typing import List, Dict, Any, Optional, Union, Tuple
from datetime import datetime, timedelta

from config import logger, CNINFO_HEADERS, CNINFO_ORGID_URL, CNINFO_QUERY_URL, CNINFO_DETAIL_URL
from config import ANNOUNCEMENT_TYPES, PREFER_PATTERNS, EXCLUDE_PATTERNS, SHREPORT_COOKIES
from utils import retry, filter_reports_by_type

class PDFReportDownloader:
    """PDF报告下载类，用于下载股票财报PDF文件"""
    
    def __init__(self, stock_code: str, stock_name: str, exchange: str, 
                 full_stock_code: Optional[str] = None, quarters: int = 2,
                 start_date: Optional[str] = None, end_date: Optional[str] = None):
        """
        初始化财报PDF下载器
        
        Args:
            stock_code: 股票代码（不含前缀）
            stock_name: 股票名称
            exchange: 交易所（sh或sz）
            full_stock_code: 完整股票代码（含前缀），如果为None，则根据stock_code和exchange自动生成
            quarters: 要下载的季度报告数量，默认2个季度
            start_date: 开始日期，格式为YYYYMMDD或YYYY-MM-DD
            end_date: 结束日期，格式为YYYYMMDD或YYYY-MM-DD
        """
        self.stock_code = stock_code
        self.stock_name = stock_name
        self.exchange = exchange.lower()
        self.full_stock_code = full_stock_code if full_stock_code else f"{exchange.lower()}{stock_code}"
        self.quarters = quarters
        self.start_date = start_date
        self.end_date = end_date
        
        # 处理日期格式
        if self.start_date:
            try:
                # 尝试按多种格式解析
                if '-' in self.start_date:
                    self.start_date_obj = datetime.strptime(self.start_date, '%Y-%m-%d')
                    self.start_date_api = self.start_date
                else:
                    self.start_date_obj = datetime.strptime(self.start_date, '%Y%m%d')
                    self.start_date_api = f"{self.start_date[:4]}-{self.start_date[4:6]}-{self.start_date[6:8]}"
                logger.info(f"设置报告起始日期: {self.start_date_obj.strftime('%Y-%m-%d')}")
            except ValueError:
                logger.warning(f"无效的起始日期格式: {self.start_date}，将忽略此参数")
                self.start_date = None
                self.start_date_obj = None
                self.start_date_api = None
        else:
            self.start_date_obj = None
            self.start_date_api = None
            
        if self.end_date:
            try:
                # 尝试按多种格式解析
                if '-' in self.end_date:
                    self.end_date_obj = datetime.strptime(self.end_date, '%Y-%m-%d')
                    self.end_date_api = self.end_date
                else:
                    self.end_date_obj = datetime.strptime(self.end_date, '%Y%m%d')
                    self.end_date_api = f"{self.end_date[:4]}-{self.end_date[4:6]}-{self.end_date[6:8]}"
                logger.info(f"设置报告结束日期: {self.end_date_obj.strftime('%Y-%m-%d')}")
            except ValueError:
                logger.warning(f"无效的结束日期格式: {self.end_date}，将忽略此参数")
                self.end_date = None
                self.end_date_obj = None
                self.end_date_api = None
        else:
            self.end_date_obj = None
            self.end_date_api = None
        
        # 创建reports目录
        self.reports_dir = Path(f"data/{stock_name}_{stock_code}/fundamentals/reports")
        self.reports_dir.mkdir(parents=True, exist_ok=True)
        
        # 设置headers
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        }
        
        # 当前获取的财报PDF文件列表
        self.report_files = []
    
    def download_reports(self) -> bool:
        """
        根据交易所下载对应的财报PDF
        
        返回:
            bool: 下载是否成功
        """
        try:
            if self.exchange == 'SH':
                return self.download_sh_reports()
            elif self.exchange == 'SZ':
                return self.download_sz_reports()
            else:
                logger.error(f"不支持的交易所: {self.exchange}")
                return False
        except Exception as e:
            logger.error(f"下载财报时出错: {e}")
            return False
    
    @retry(max_retries=3)
    def get_sz_orgid(self) -> Optional[Dict[str, str]]:
        """
        获取深交所股票的orgid
        
        返回:
            Dict[str, str]: 包含code和orgid的字典
        """
        with httpx.Client(headers=CNINFO_HEADERS, timeout=30) as client:
            response = client.get(CNINFO_ORGID_URL)
            if response.status_code == 200:
                try:
                    orgids = response.json()
                    stock_lists = orgids['stockList']
                    for stock_list in stock_lists:
                        if stock_list['zwjc'] == self.stock_name:
                            return {
                                'code': stock_list['code'],
                                'orgid': stock_list['orgId']
                            }
                    logger.warning(f"无法找到股票 {self.stock_name} 的orgid")
                    return None
                except json.JSONDecodeError:
                    logger.warning("获取股票orgid时返回非JSON内容")
                    return None
            else:
                logger.warning(f"获取股票orgid时返回状态码: {response.status_code}")
                return None
    
    def filter_reports_by_date(self, announcements: List[Dict]) -> List[Dict]:
        """根据日期范围过滤公告
        
        Args:
            announcements: 公告列表
            
        Returns:
            过滤后的公告列表
        """
        if not (self.start_date_obj or self.end_date_obj):
            logger.info("未指定日期范围，不进行日期过滤")
            return announcements
        
        filtered_announcements = []
        
        for announcement in announcements:
            try:
                # 从公告中提取日期字段，格式通常为 YYYY-MM-DD
                if 'announcementTime' in announcement:
                    date_str = announcement['announcementTime'].split(' ')[0]
                    date_obj = datetime.strptime(date_str, '%Y-%m-%d')
                elif 'ANNOUNCEMENTTIME' in announcement:
                    date_str = announcement['ANNOUNCEMENTTIME'].split(' ')[0]
                    date_obj = datetime.strptime(date_str, '%Y-%m-%d')
                else:
                    logger.warning(f"公告缺少日期字段: {announcement}")
                    continue
                
                # 应用日期过滤
                if self.start_date_obj and date_obj < self.start_date_obj:
                    continue
                if self.end_date_obj and date_obj > self.end_date_obj:
                    continue
                    
                filtered_announcements.append(announcement)
            except (ValueError, KeyError) as e:
                logger.error(f"处理公告日期时出错: {e}, 公告: {announcement}")
                continue
        
        logger.info(f"日期过滤: 原始公告数 {len(announcements)} -> 过滤后 {len(filtered_announcements)}")
        return filtered_announcements

    @retry(max_retries=3)
    def get_sz_pdf_url(self, orgId, pageSize=30, pageNum=1):
        """获取深交所公告PDF的下载链接"""
        url = "http://www.cninfo.com.cn/new/hisAnnouncement/query"
        
        params = {
            "stock": self.full_stock_code,
            "tabName": "fulltext",
            "pageSize": pageSize,
            "pageNum": pageNum,
            "category": "category_ndbg_szsh;category_bndbg_szsh;category_yjdbg_szsh;category_sjdbg_szsh;",
            "column": "szse",
            "plate": "sz",
            "orgId": orgId,
        }
        
        # 添加日期范围参数
        if self.end_date:
            if self.start_date:
                # 如果有开始日期，使用完整日期范围
                date_range = f"{self.start_date_api}~{self.end_date_api}"
            else:
                # 如果没有开始日期，使用截止日期前一年的日期范围
                end_date_obj = datetime.strptime(self.end_date, '%Y%m%d')
                start_date_obj = end_date_obj - timedelta(days=365)
                start_date_api = start_date_obj.strftime('%Y-%m-%d')
                date_range = f"{start_date_api}~{self.end_date_api}"
            
            params["seDate"] = date_range
        
        try:
            response = httpx.post(url, data=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            return data
        except Exception as e:
            logging.error(f"获取公告PDF URL失败: {e}")
            return None
    
    @retry(max_retries=3)
    def get_sz_total_pages(self, data: Dict[str, str]) -> int:
        """
        获取深交所公告的总页数
        
        参数:
            data (dict): 包含code和orgid的字典
            
        返回:
            int: 总页数
        """
        code = data.get('code')
        orgid = data.get('orgid')
        
        # 构建日期范围参数
        se_date = ""
        if self.start_date_api and self.end_date_api:
            se_date = f"{self.start_date_api}~{self.end_date_api}"
        elif self.start_date_api:
            se_date = f"{self.start_date_api}~"
        elif self.end_date_api:
            se_date = f"~{self.end_date_api}"
            
        post_data = {
            'stock': f'{code},{orgid}',
            'tabName': 'fulltext',
            'pageSize': 30,
            'pageNum': 1,
            'column': 'szse',
            'category': '',
            'plate': 'sz',
            'seDate': se_date,
            'searchkey': '',
            'secid': '',
            'sortName': '',
            'sortType': '',
            'isHLtitle': 'true'
        }
        
        with httpx.Client(headers=CNINFO_HEADERS, timeout=30) as client:
            res = client.post(CNINFO_QUERY_URL, data=post_data)
            if res.status_code == 200:
                try:
                    an = res.json()
                    totalpages = an.get('totalpages', 0)
                    return totalpages
                except json.JSONDecodeError:
                    logger.warning(f"获取公告总页数时返回非JSON内容")
                    return 0
            else:
                logger.warning(f"获取公告总页数时返回状态码: {res.status_code}")
                return 0
    
    @retry(max_retries=3)
    def save_sz_pdfs(self, pdf_info_list, save_path):
        """保存深交所PDF报告到本地"""
        saved_files = []
        
        if not pdf_info_list:
            logging.warning("没有可下载的PDF信息列表")
            return saved_files
        
        for pdf_info in pdf_info_list:
            try:
                # 解析PDF信息
                announcement_id = pdf_info.get('announcementId')
                title = pdf_info.get('announcementTitle', '')
                time_str = pdf_info.get('announcementTime', '')
                
                # 格式化文件名
                if time_str:
                    try:
                        # 尝试将毫秒时间戳转换为日期字符串
                        if isinstance(time_str, int) or time_str.isdigit():
                            date_obj = datetime.fromtimestamp(int(time_str)/1000)
                            date_str = date_obj.strftime('%Y%m%d')
                        else:
                            # 如果已经是日期字符串，则直接使用
                            date_str = time_str.replace('-', '')
                    except Exception as e:
                        logging.warning(f"日期转换出错 '{time_str}': {e}")
                        date_str = "00000000"
                else:
                    date_str = "00000000"
                
                # 清理文件名
                clean_title = re.sub(r'[\\/:*?"<>|]', '_', title)
                filename = f"{date_str}_{self.stock_code}_{clean_title}.pdf"
                file_path = os.path.join(save_path, filename)
                
                # 检查文件是否已存在
                if os.path.exists(file_path):
                    logging.info(f"文件已存在，跳过下载: {filename}")
                    saved_files.append(file_path)
                    continue
                
                # 构建下载URL
                download_url = f"http://static.cninfo.com.cn/{pdf_info.get('adjunctUrl')}"
                
                # 下载PDF
                response = httpx.get(download_url, timeout=30)
                response.raise_for_status()
                
                # 保存PDF
                with open(file_path, 'wb') as f:
                    f.write(response.content)
                
                logging.info(f"已保存PDF: {filename}")
                saved_files.append(file_path)
                
            except Exception as e:
                logging.error(f"保存PDF时出错: {e}")
        
        return saved_files

    def download_sz_reports(self):
        """下载深交所报告"""
        logging.info(f"开始下载深交所报告: {self.stock_code}")
        
        # 创建保存目录
        os.makedirs(self.reports_dir, exist_ok=True)
        
        # 获取组织ID
        org_id = self.get_sz_orgid()
        if not org_id:
            logging.error(f"无法获取组织ID: {self.stock_code}")
            return []
        
        all_pdfs = []
        page_num = 1
        max_pages = 5  # 最多检查5页
        
        while page_num <= max_pages:
            # 获取PDF信息
            data = self.get_sz_pdf_url(org_id, pageSize=30, pageNum=page_num)
            if not data:
                break
            
            announcements = data.get('announcements', [])
            if not announcements:
                logging.info(f"第 {page_num} 页没有公告")
                break
            
            # 过滤公告类型
            filtered_announcements = []
            for ann in announcements:
                title = ann.get('announcementTitle', '')
                
                # 检查是否符合首选模式
                is_preferred = any(pattern in title for pattern in PREFER_PATTERNS)
                
                # 检查是否符合排除模式
                is_excluded = any(pattern in title for pattern in EXCLUDE_PATTERNS)
                
                # 判断公告类型
                announcement_type = ann.get('announcementType')
                is_valid_type = False
                
                if announcement_type:
                    is_valid_type = any(valid_type in announcement_type for valid_type in ANNOUNCEMENT_TYPES)
                
                # 必须是有效类型，且要么是首选，要么不在排除列表中
                if is_valid_type and (is_preferred or not is_excluded):
                    # 日期过滤
                    if 'announcementTime' in ann:
                        try:
                            time_str = ann.get('announcementTime')
                            if isinstance(time_str, int) or (isinstance(time_str, str) and time_str.isdigit()):
                                date_obj = datetime.fromtimestamp(int(time_str)/1000)
                            else:
                                date_obj = datetime.strptime(time_str, '%Y-%m-%d')
                            
                            # 检查日期是否在范围内
                            is_in_range = True
                            if self.start_date_obj and date_obj < self.start_date_obj:
                                is_in_range = False
                            if self.end_date_obj and date_obj > self.end_date_obj:
                                is_in_range = False
                            
                            if is_in_range:
                                filtered_announcements.append(ann)
                        except Exception as e:
                            logging.warning(f"日期解析出错: {e}")
                            # 无法解析日期，仍然添加
                            filtered_announcements.append(ann)
                    else:
                        # 没有日期，仍然添加
                        filtered_announcements.append(ann)
            
            # 按日期降序排序
            filtered_announcements.sort(key=lambda x: x.get('announcementTime', ''), reverse=True)
            
            # 如果有指定季度数量，检查是否已经有足够的PDF
            if self.quarters and len(all_pdfs) + len(filtered_announcements) >= self.quarters * 2:
                filtered_announcements = filtered_announcements[:max(0, self.quarters * 2 - len(all_pdfs))]
                all_pdfs.extend(filtered_announcements)
                break
            
            all_pdfs.extend(filtered_announcements)
            
            # 检查是否有下一页
            total_announcements = data.get('totalAnnouncement', 0)
            total_pages = (total_announcements + 29) // 30  # 向上取整
            
            if page_num >= total_pages or total_announcements <= page_num * 30:
                break
            
            page_num += 1
        
        # 保存PDF
        saved_files = self.save_sz_pdfs(all_pdfs, self.reports_dir)
        
        logging.info(f"已下载 {len(saved_files)} 个深交所报告")
        return saved_files
    
    def download_sh_reports(self) -> bool:
        """
        下载上交所财报
        
        返回:
            bool: 下载是否成功
        """
        try:
            # 导入上交所报告下载模块
            try:
                from shreport import SH
            except ImportError:
                logger.error("无法导入shreport模块，请确保已安装该模块")
                return False
            
            # 创建SH实例
            sh = SH(SHREPORT_COOKIES)
            
            # 设置日期范围参数
            date_range_params = {}
            if self.start_date_obj:
                date_range_params['start_date'] = self.start_date_obj.strftime('%Y-%m-%d')
            if self.end_date_obj:
                date_range_params['end_date'] = self.end_date_obj.strftime('%Y-%m-%d')
            
            # 如果没有指定日期范围，则获取目标财报类型
            target_types = []
            if not (self.start_date_obj or self.end_date_obj):
                for i in range(min(4, self.quarters)):
                    if i == 0:  # 第一季度
                        target_types.append("第一季度")
                    elif i == 1:  # 半年度
                        target_types.append("半年度")
                    elif i == 2:  # 第三季度
                        target_types.append("第三季度")
                    elif i == 3:  # 年度
                        target_types.append("年度")
            
            # 保存原始下载路径
            temp_dir = Path("./temp_reports")
            temp_dir.mkdir(parents=True, exist_ok=True)
            
            # 下载报告
            success = False
            if date_range_params:
                # 使用日期范围下载
                logger.info(f"使用日期范围下载上交所财报: {date_range_params}")
                success = sh.download(code=self.stock_code, savepath=str(temp_dir), **date_range_params)
            else:
                # 使用默认方式下载
                logger.info(f"下载上交所最近财报")
                success = sh.download(code=self.stock_code, savepath=str(temp_dir))
            
            if not success:
                logger.error(f"下载上交所财报失败: {self.stock_code}")
                return False
            
            # 检查下载的文件并移动到指定目录
            downloaded_files = list(temp_dir.glob("*.pdf"))
            
            if not downloaded_files:
                logger.warning(f"未找到下载的PDF文件: {self.stock_code}")
                return False
            
            # 处理下载的文件
            saved_files = []
            for file in downloaded_files:
                file_name = file.name
                
                # 如果使用日期范围，保留所有PDF，否则按类型过滤
                keep_file = True
                if not (self.start_date_obj or self.end_date_obj) and target_types:
                    # 检查是否是我们需要的财报类型
                    keep_file = False
                    for target in target_types:
                        if target in file_name:
                            keep_file = True
                            break
                
                if not keep_file:
                    continue
                
                # 复制文件到指定目录
                new_file_name = f"{self.stock_name}：{file_name}"
                target_path = self.reports_dir / new_file_name
                
                # 如果文件不存在，则复制
                if not target_path.exists():
                    shutil.copy2(file, target_path)
                    logger.info(f"保存PDF: {new_file_name}")
                
                saved_files.append(str(target_path))
            
            # 清理临时目录
            for file in downloaded_files:
                file.unlink(missing_ok=True)
            
            # 更新报告文件列表
            self.report_files = saved_files
            
            logger.info(f"成功下载 {len(saved_files)} 个财报PDF")
            return len(saved_files) > 0
            
        except Exception as e:
            logger.error(f"下载上交所财报时出错: {e}")
            return False
            
    def clean_reports_folder(self) -> None:
        """清理财报文件夹，删除重复的报告"""
        try:
            # 获取所有PDF文件
            pdf_files = list(self.reports_dir.glob("*.pdf"))
            
            # 按年份和报告类型进行分组
            report_groups = {}
            
            for pdf_path in pdf_files:
                file_name = pdf_path.name
                
                # 提取年份和报告类型
                year_match = re.search(r'(\d{4})年', file_name)
                if not year_match:
                    continue
                    
                year = year_match.group(1)
                
                # 确定报告类型
                report_type = None
                for t in ['一季度', '半年度', '三季度', '年度']:
                    if t in file_name:
                        report_type = t
                        break
                
                if not report_type:
                    if '第一季度' in file_name or '第1季度' in file_name:
                        report_type = '一季度'
                    elif '第三季度' in file_name or '第3季度' in file_name:
                        report_type = '三季度'
                    else:
                        continue
                
                # 使用年份+报告类型作为键
                key = f"{year}_{report_type}"
                
                if key not in report_groups:
                    report_groups[key] = []
                
                report_groups[key].append(pdf_path)
            
            # 对每个分组，保留首选文件，删除其他文件
            for key, group in report_groups.items():
                if len(group) <= 1:
                    continue
                
                # 对多个文件进行排序，优先选择包含"全文"的，其次选择不包含排除词的
                keep_file = None
                
                # 首先查找包含优先词的
                for pdf_path in group:
                    file_name = pdf_path.name
                    for pattern in PREFER_PATTERNS:
                        if pattern in file_name:
                            keep_file = pdf_path
                            break
                    if keep_file:
                        break
                
                # 如果没有找到优先的，则选择第一个不包含排除词的
                if not keep_file:
                    for pdf_path in group:
                        file_name = pdf_path.name
                        exclude = False
                        for pattern in EXCLUDE_PATTERNS:
                            if pattern in file_name:
                                exclude = True
                                break
                        if not exclude:
                            keep_file = pdf_path
                            break
                
                # 如果仍未找到，则保留第一个文件
                if not keep_file:
                    keep_file = group[0]
                
                # 删除其他文件
                for pdf_path in group:
                    if pdf_path != keep_file:
                        try:
                            pdf_path.unlink()
                            logger.info(f"删除重复文件: {pdf_path.name}")
                        except Exception as e:
                            logger.error(f"删除文件时出错: {pdf_path.name}, {e}")
            
            # 更新报告文件列表
            saved_files = list(self.reports_dir.glob("*.pdf"))
            self.report_files = [str(f) for f in saved_files]
            
        except Exception as e:
            logger.error(f"清理财报文件夹时出错: {e}")
    
    def check_existing_reports(self) -> bool:
        """
        检查是否已有足够的财报文件
        
        返回:
            bool: 是否已有足够的财报
        """
        # 获取所有PDF文件
        pdf_files = list(self.reports_dir.glob("*.pdf"))
        
        # 如果使用日期范围，则按日期范围过滤文件
        if self.start_date_obj or self.end_date_obj:
            filtered_files = []
            for pdf_path in pdf_files:
                file_name = pdf_path.name
                
                # 尝试从文件名中提取年份
                year_match = re.search(r'(\d{4})年', file_name)
                if not year_match:
                    continue
                
                year = int(year_match.group(1))
                
                # 检查报告季度/半年度/年度，为其估算一个月份
                month = 12  # 默认为年度报告（12月）
                if '一季度' in file_name or '第一季度' in file_name or '第1季度' in file_name:
                    month = 3
                elif '半年度' in file_name:
                    month = 6
                elif '三季度' in file_name or '第三季度' in file_name or '第3季度' in file_name:
                    month = 9
                
                # 创建大致的报告日期（仅用于粗略过滤）
                report_date = datetime(year, month, 1)
                
                # 按日期范围过滤
                if self.start_date_obj and report_date < self.start_date_obj:
                    continue
                if self.end_date_obj and report_date > self.end_date_obj:
                    continue
                
                filtered_files.append(pdf_path)
            
            # 更新报告文件列表
            self.report_files = [str(f) for f in filtered_files]
            
            if filtered_files:
                logger.info(f"在日期范围内找到 {len(filtered_files)} 个现有财报文件")
                return True
            else:
                logger.info(f"在日期范围内没有找到现有财报文件，需要下载")
                return False
        else:
            # 按季度数量判断
            if len(pdf_files) >= self.quarters:
                logger.info(f"已有足够的财报文件: {len(pdf_files)} >= {self.quarters}")
                
                # 更新报告文件列表
                self.report_files = [str(f) for f in pdf_files]
                
                return True
            else:
                logger.info(f"需要下载更多财报: {len(pdf_files)} < {self.quarters}")
                return False 