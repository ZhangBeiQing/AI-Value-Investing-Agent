#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
PDF分析模块
负责使用Gemini API处理PDF内容并生成分析
"""

import time
from pathlib import Path
from typing import List, Dict, Any, Optional, Union, Tuple
from datetime import datetime
from google import genai

from config import logger, GEMINI_MODEL
from utils import retry, save_to_file
from gemini_utility import basic_convert  # 导入PDF转Markdown函数

class PDFAnalyzer:
    """PDF分析类，使用Gemini API处理PDF文件并生成分析"""
    
    def __init__(self, stock_code: str, stock_name: str, full_stock_code: str, 
                 reports_dir: Optional[Path] = None, client=None):
        """
        初始化PDF分析器
        
        参数:
            stock_code (str): 股票代码
            stock_name (str): 股票名称
            full_stock_code (str): 带交易所前缀的完整股票代码
            reports_dir (Path, optional): 报告存储目录
            client: Gemini客户端实例
        """
        self.stock_code = stock_code
        self.stock_name = stock_name
        self.full_stock_code = full_stock_code
        
        # 设置报告目录
        if reports_dir is None:
            self.base_dir = Path(f"data/{stock_name}_{stock_code}/fundamentals")
            self.reports_dir = self.base_dir / "reports"
        else:
            self.reports_dir = reports_dir
            self.base_dir = reports_dir.parent
        
        # 设置Gemini客户端
        self.client = client
        self.model = GEMINI_MODEL
        
        # 创建转换目录
        self.markdown_dir = self.base_dir / "markdown_files"
        self.markdown_dir.mkdir(parents=True, exist_ok=True)
        
        # 创建分析结果目录
        self.analysis_dir = self.base_dir / "analysis"
        self.analysis_dir.mkdir(parents=True, exist_ok=True)

        # 创建PDF摘要子文件夹
        self.summaries_dir = self.base_dir / "pdf_summaries"
        if not self.summaries_dir.exists():
            self.summaries_dir.mkdir(exist_ok=True)

    def _save_prompt_to_file(self, prompt, prompt_type):
        """
        将prompt保存到文件中，方便检查

        参数:
            prompt (str): 要保存的prompt内容
            prompt_type (str): prompt类型，用于文件命名
        """
        try:
            # 创建prompts文件夹
            prompts_dir = self.base_dir / "prompts"
            if not prompts_dir.exists():
                prompts_dir.mkdir(exist_ok=True)

            # 检查是否已存在相同类型的prompt文件（不带时间戳）
            # 对于PDF汇总和最终综合分析，使用固定文件名而不带时间戳
            if prompt_type in ["PDF汇总", "最终综合分析"]:
                prompt_file = prompts_dir / f"{self.stock_name}_{self.stock_code}_{prompt_type}.md"

                # 如果文件已存在并且是在同一天生成的，跳过保存
                if prompt_file.exists():
                    file_mtime = datetime.fromtimestamp(prompt_file.stat().st_mtime)
                    if datetime.now().date() == file_mtime.date():
                        logger.info(f"跳过保存prompt，已存在今日生成的文件: {prompt_file}")
                        return
            else:
                # 其他类型的prompt仍然使用带时间戳的文件名
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                prompt_file = prompts_dir / f"{self.stock_name}_{self.stock_code}_{prompt_type}_{timestamp}.md"

            with open(prompt_file, 'w', encoding='utf-8') as f:
                f.write(f"# {self.stock_name} ({self.full_stock_code}) {prompt_type} Prompt\n\n")
                f.write(prompt)

            logger.info(f"已保存{prompt_type}prompt至: {prompt_file}")

        except Exception as e:
            logger.error(f"保存prompt到文件时出错: {e}")

    @retry(max_retries=3, base_wait=10)
    def _analyze_single_pdf(self, pdf_path: str) -> Dict[str, str]:
        """
        分析单个PDF文件
        
        参数:
            pdf_path (str): PDF文件路径
            
        返回:
            dict: 包含分析结果的字典
        """
        try:
            pdf_path = Path(pdf_path)
            pdf_name = pdf_path.name
            # 创建分析摘要文件名
            summary_file = self.summaries_dir / f"{self.stock_name}_{self.stock_code}_{pdf_name}_summary.md"
            # 如果已经存在分析结果，且是今天生成的，则直接使用
            if summary_file.exists():
                # 检查文件是否是今天生成的
                    logger.info(f"使用已存在的分析摘要: {summary_file}")
                    with open(summary_file, 'r', encoding='utf-8') as f:
                        # 跳过标题行
                        content = f.read()
                        # 提取正文（去掉标题）
                        header = f"# {pdf_name} 分析摘要\n\n"
                        if content.startswith(header):
                            analysis = content[len(header):]
                        else:
                            analysis = content
                    
                    return {
                        "pdf_name": pdf_name,
                        "analysis": analysis
                    }
            
            # 转换PDF到Markdown
            markdown_file = self.markdown_dir / f"{pdf_path.stem}.md"
            
            # 如果Markdown文件不存在，则转换PDF
            if not markdown_file.exists():
                logger.info(f"将PDF转换为Markdown: {pdf_path}")
                markdown_text = basic_convert(str(pdf_path), output_dir=str(self.markdown_dir))
                if not markdown_text:
                    logger.error(f"转换PDF失败: {pdf_path}")
                    return {
                        "pdf_name": pdf_name,
                        "analysis": f"无法转换PDF到Markdown: {pdf_path}"
                    }
            else:
                # 读取已有的Markdown文件
                logger.info(f"使用已转换的Markdown文件: {markdown_file}")
                with open(markdown_file, 'r', encoding='utf-8') as f:
                    markdown_text = f.read()
            
            # 截取Markdown文本，避免过长
            # if len(markdown_text) > 20000:
            #     logger.info(f"Markdown文本过长，进行截取: {len(markdown_text)} -> 20000字符")
            #     markdown_text = markdown_text[:20000] + "...[内容过长已截断]"
            
            # 创建提示
            prompt = f"""
                        你是一位专业的财务报告分析师，专注于挖掘财务报表中的深层信息。

                        请分析提供的这份{self.stock_name}({self.full_stock_code})财报，并提供以下内容：

                        1. 报告信息摘要：
                        - 报告类型（年报/半年报/季报）
                        - 报告时间范围
                        - 文件中包含的主要章节

                        2. 管理层讨论与分析要点：
                        - 管理层对业绩的解释
                        - 公司战略和业务发展规划
                        - 对未来趋势的预测

                        3. 关键财务数据：
                        - 主要财务指标及变化
                        - 细分业务表现
                        - 特殊项目或非经常性损益

                        4. 风险和不确定性：
                        - 管理层披露的主要风险
                        - 风险缓解策略
                        - 可能影响未来业绩的因素

                        5. 会计政策和重要估计：
                        - 重要会计政策变更
                        - 关键会计估计和判断
                        - 可能影响报表理解的特殊条款

                        6. 潜在的"异常项"和值得注意的披露：
                        - 财务报表附注中的重要信息
                        - 任何可能表明管理层意图的措辞或表述变化
                        - 可能被忽视的重要信息

                        以下是财报的Markdown格式内容:

                        ```
                        {markdown_text}
                        ```

                        请提供一个800-1200字的详细分析，重点关注可能未在结构化财务数据中体现的内容、管理层的主观判断和潜在风险信号。

                        你的分析应该客观、深入且关注细节，特别是那些财务数据表格无法体现的信息, 每个客观观点都尽可能引用详细的财报数据进展佐证。
                    """

            # 保存单个PDF分析提示到文件
            self._save_prompt_to_file(prompt, f"单个PDF分析_{pdf_name}")

            # 调用Gemini API进行分析
            logger.info(f"使用Gemini分析PDF: {pdf_name}")
            
            analysis = ""
            response = self.client.models.generate_content_stream(
                model=self.model,
                contents=prompt
            )
            
            # 获取响应文本
            for chunk in response:
                if chunk.candidates and chunk.candidates[0].content and chunk.candidates[0].content.parts:
                    for part in chunk.candidates[0].content.parts:
                        if hasattr(part, 'text') and part.text:
                            analysis += part.text
            
            # 保存分析结果到子文件夹中
            with open(summary_file, 'w', encoding='utf-8') as f:
                f.write(f"# {pdf_name} 分析摘要\n\n")
                f.write(analysis)
            
            logger.info(f"PDF分析摘要已保存至 {summary_file}")
            
            return {
                "pdf_name": pdf_name,
                "analysis": analysis
            }
            
        except Exception as e:
            logger.error(f"分析PDF文件出错: {pdf_path}, {e}")
            return {
                "pdf_name": Path(pdf_path).name,
                "analysis": f"分析出错: {str(e)}"
            }
    
    def _batch_process_pdfs(self, pdf_files: List[str], financial_table: Optional[str] = None, 
                           additional_info: Optional[str] = None) -> str:
        """
        批处理多个PDF文件
        
        参数:
            pdf_files (List[str]): PDF文件路径列表
            financial_table (str, optional): 财务表格数据
            additional_info (str, optional): 额外的提示信息
            
        返回:
            str: 生成的综合分析文本
        """
        # 首先检查是否有缓存
        cache_file = self.base_dir / f"{self.stock_name}_{self.stock_code}_final_analysis.md"
        final_analysis = ""
        
        if cache_file.exists():
            # 检查文件是否是今天生成的
            file_mtime = datetime.fromtimestamp(cache_file.stat().st_mtime)
            if datetime.now().date() == file_mtime.date():
                logger.info(f"使用今天生成的缓存: {cache_file}")
                with open(cache_file, 'r', encoding='utf-8') as f:
                    content = f.read()
                    # 提取正文（去掉标题）
                    header = f"# {self.stock_name} ({self.full_stock_code}) 综合财务分析\n\n"
                    if content.startswith(header):
                        final_analysis = content[len(header):]
                    else:
                        final_analysis = content
                
                return final_analysis
        
        # 没有缓存，需要处理PDF
        # 首先分析每个PDF文件
        pdf_analyses = []
        
        for pdf_path in pdf_files:
            analysis_result = self._analyze_single_pdf(pdf_path)
            pdf_analyses.append(analysis_result)
        
        # 检查PDF汇总分析的缓存
        summary_file = self.summaries_dir / f"{self.stock_name}_{self.stock_code}_pdf_summary.md"
        pdf_summary = ""
        
        if summary_file.exists():
            logger.info(f"找到已存在的PDF汇总分析缓存: {summary_file}")
            with open(summary_file, 'r', encoding='utf-8') as f:
                content = f.read()
                # 提取正文（去掉标题）
                header = f"# {self.stock_name} ({self.full_stock_code}) 财报PDF汇总分析\n\n"
                if content.startswith(header):
                    pdf_summary = content[len(header):].strip()
        
        # 如果没有缓存或缓存过期，重新生成PDF汇总分析
        if not pdf_summary:
            pdf_summary = self._create_pdf_summary(pdf_analyses)
            
            # 保存PDF汇总分析
            with open(summary_file, 'w', encoding='utf-8') as f:
                f.write(f"# {self.stock_name} ({self.full_stock_code}) 财报PDF汇总分析\n\n")
                f.write(pdf_summary)
                
            logger.info(f"财报PDF汇总分析已保存至 {summary_file}")
        
        if not final_analysis:
            # 如果没有缓存或缓存无效，则重新生成
            # 创建整合了PDF汇总和财务表格数据的提示
            final_prompt = self._create_final_integrated_prompt(pdf_summary, financial_table, additional_info)

            # 保存最终提示到文件（使用优化后的方法，使用固定文件名）
            self._save_prompt_to_file(final_prompt, "最终综合分析")
            
            # 使用重试机制调用API
            final_analysis = self._generate_final_analysis(final_prompt)
            
            # 保存最终分析结果
            save_to_file(
                final_analysis, 
                cache_file, 
                header=f"# {self.stock_name} ({self.full_stock_code}) 综合财务分析"
            )
            
            logger.info(f"综合财务分析已保存至 {cache_file}")
        
        return final_analysis
    
    @retry(max_retries=3, base_wait=15)
    def _create_pdf_summary(self, pdf_analyses: List[Dict[str, str]]) -> str:
        """
        创建PDF汇总分析
        
        参数:
            pdf_analyses (List[Dict[str, str]]): PDF分析结果列表
            
        返回:
            str: 汇总分析文本
        """
        prompt = f"""
            你是一位专业的财务分析专家，擅长整合多份财务报告的信息，提取关键见解。

            以下是对{self.stock_name}({self.full_stock_code})多份财报PDF的单独分析结果：

          """
        
        for i, analysis in enumerate(pdf_analyses, 1):
            prompt += f"""
                 ## PDF {i}: {analysis['pdf_name']}

                 {analysis['analysis']}

                 ---

               """
        
        prompt += f"""
            请整合以上所有PDF分析，提供一份全面的汇总分析，重点关注以下方面：

            1. 公司战略和业务模式：
            - 根据多期报告，总结公司的核心战略方向
            - 业务模式的演变和调整
            - 管理层关注的优先事项变化

            2. 财务状况趋势：
            - 跨报告期的关键财务指标变化
            - 收入和利润驱动因素
            - 资产负债结构变化

            3. 管理层叙述分析：
            - 管理层对业绩波动的解释一致性
            - 战略执行的连贯性
            - 表述语气和重点的变化

            4. 风险因素演变：
            - 风险披露的变化和新增风险
            - 管理层应对风险的策略调整
            - 值得关注的潜在问题

            5. 会计处理和披露：
            - 会计政策变更和一致性
            - 特殊或非经常性项目
            - 可能影响财务真实性的因素

            6. 深层次分析和见解：
            - 跨报告期的矛盾或不一致之处
            - 可能被掩盖的业务挑战
            - 财务健康状况的实质性评估

            请提供一个1500-2000字的深度分析，作为对这些财报的综合解读，揭示可能仅依靠财务数字无法发现的趋势、风险和机遇。重点关注管理层可能未明确表达但通过报告语言、结构和重点变化所暗示的内容。要在回复中加入具体的财务指标和数据
        """

        self._save_prompt_to_file(prompt, f"PDF汇总")

        logger.info(f"调用gemini API进行财报PDF汇总\n")
        summary = ""
        response = self.client.models.generate_content_stream(
            model=self.model,
            contents=prompt
        )
        
        # 获取响应文本
        for chunk in response:
            if chunk.candidates and chunk.candidates[0].content and chunk.candidates[0].content.parts:
                for part in chunk.candidates[0].content.parts:
                    if hasattr(part, 'text') and part.text:
                        summary += part.text
        
        return summary
    
    @retry(max_retries=3, base_wait=15)
    def _generate_final_analysis(self, prompt: str) -> str:
        """
        生成最终分析
        
        参数:
            prompt (str): 提示文本
            
        返回:
            str: 生成的分析文本
        """

        logger.info(f"调用gemini API进行最终结果汇总")
        final_analysis = ""
        response = self.client.models.generate_content_stream(
            model=self.model,
            contents=prompt
        )
        
        # 获取最终分析
        for part in response.candidates[0].content.parts:
            if hasattr(part, 'text') and part.text:
                final_analysis += part.text + "\n"
                
        return final_analysis
    
    def _create_final_integrated_prompt(self, pdf_summary: str, financial_table: Optional[str] = None, 
                                       additional_info: Optional[str] = None) -> str:
        """
        创建整合PDF汇总和财务表格数据的最终提示
        
        参数:
            pdf_summary (str): PDF汇总分析
            financial_table (str, optional): 财务表格数据
            additional_info (str, optional): 额外的提示信息，如重要背景、行业信息等
            
        返回:
            str: 最终整合提示
        """
        prompt = f"""
                    # {self.stock_name}({self.full_stock_code}) 深度财务分析任务

                    ## 角色设定：

                    你是一位世界级金融分析专家，曾在顶级投资银行担任研究主管，专长于发现上市公司财报中的隐藏信息和战略意图。你擅长将定量数据（结构化财务数据）与定性分析（财报叙述、管理层讨论）相结合，揭示公司真实的经营状况、财务健康度和潜在风险。**你的分析必须以数据为基础，每一个关键结论都需要明确引用具体的财务指标及其数值或趋势作为证据。请确保引用清晰**

                    ## 分析材料：
                    
                  """

        # 如果有财务表格数据，添加到提示中
        if financial_table:
            prompt += f"""
                        1.  **结构化财务数据**：从专业金融数据库获取的跨时期财务报表数据：
                            =================================结构化财务数据开始===============================================
                            ```
                            {financial_table}
                            ``` 
                            =================================结构化财务数据结束===============================================   
    
    
                            **数据格式说明：**
                            *   BS_前缀：资产负债表指标
                            *   IS_前缀：利润表指标
                            *   CF_前缀：现金流量表指标
                            *   货币单位：万元、千万元、亿元
                            *   百分比：增长率或比率
                       """

        prompt += f"""
        
                    2.  **财报PDF深度分析总结**：以下是对公司多份财报PDF文件的综合分析总结，揭示了管理层叙述、战略意图、风险披露和潜在问题的演变趋势：
                     ====================================财报PDF深度分析总结开始===========================================
                    ```
                    {pdf_summary}
                    ```
                    =====================================财报PDF深度分析总结结束===========================================
                    ## 核心分析任务：

                    请将上述信息源整合，提供一份全面、深入、**且数据驱动**的分析报告，揭示：

                    ### 1. 财务与叙述的一致性（30%权重）
                        *   评估管理层解释（来自PDF总结）与实际财务数据的匹配度。**对每一个评估点，明确指出管理层的说法，并直接引用具体的财务指标（名称、数值/趋势）来证实或证伪。
                        *   分析公司战略目标与资金分配的一致性。**量化资源投入，并评估其与宣称战略的匹配程度。**
                        *   分析风险披露与财务波动的关联性。**当管理层披露某风险时，查找是否有对应财务指标的恶化趋势。**
                        *   **使用结构化的项目符号列表（bullet points）来清晰展示数据与叙述之间显著的矛盾或不一致之处。**

                    ### 2. 财务真实性评估（25%权重）
                        *   评估收入确认的合理性，关联应收账款和存货的变动与营收的匹配度。**分析应收账款和存货相对于营收的增长趋势，结合周转情况进行判断。**
                        *   分析费用分类的波动和计提的适当性。**指出异常波动或重大计提，并引用具体数值和同比变化。**
                        *   深入分析非经常性项目的性质、金额、频率及其对利润的影响。**量化非经常性损益占净利润的比例，评估其对盈利质量的影响。**
                        *   对比现金流质量与利润质量。**分析经营活动现金流净额与净利润的比率及其变化趋势，明确指出两者是否显著背离及可能原因。**
                        *   **非常重要的一点**。**部分公司的经营现金流呈季节性波动的，你不能只看一个季度的现金流，需要考虑年度周期性，
                            同时需要跨多年多个季度对比现金流等财务状况。比如某公司经常不同年份不同季度现金流正负波动，但是最后年终正现金流都很大，这证明
                            其现金流具有较大季节性，如部分政府业务习惯年终回款，导致不同季节现金流会显著波动。同时思考现金流变化要考虑公司整体资产，若波动几亿
                            现金流但公司总共几百几千亿资产，此时虽然现金流YOY波动很大，但其实不重要，所以需要不同年度不同季节之间考虑这些数据之间的关系。**

                    ### 3. 战略执行与资源配置（20%权重）
                        *   评估资本支出的方向和规模是否与公司宣称的战略重点相匹配。**指出主要的资本投向，并引用数据说明其规模和变化趋势。**
                        *   分析研发投入与营收和毛利率变化的关系。**评估研发投入的效率，说明高投入是否带来了相应的业绩增长或盈利能力提升。**
                        *   评估财务杠杆与增长策略和风险承受能力的协调性。**引用关键负债指标的变化趋势，分析公司的融资策略和偿债风险。**
                        *   结合管理层激励与公司长期价值指标的一致性。

                    ### 4. 未来展望与隐藏风险（25%权重）
                        *   基于历史财务数据模式，对未来1-2年的业绩趋势进行**有数据支持的预测**。（例如，鉴于营收连续 X 季度放缓/下滑 (IS_OPERATE_INCOME_YOY 数据...)，短期内预计增长压力持续）
                        *   识别财报中暗示但未明确陈述的潜在风险。**使用项目符号列表清晰列出，每个风险点后附带具体指标的恶化趋势作为证据**。（例如：- 流动性风险：经营现金流持续为负 (CF_NETCASH_OPERATE 在 X, Y, Z 季度分别为 A, B, C)，且短期借款增加 (BS_SHORT_LOAN YOY: D%)）
                        *   评估公司应对市场变化的适应能力，结合其战略调整和资源配置效率。
                        *   明确列出潜在的财务压力信号（"红旗"信号），**使用项目符号列表，每个信号必须附带具体的财务指标名称和数据表现**。（例如：- **经营现金流恶化：** CF_NETCASH_OPERATE 连续多期为负，2024Q3为 -1.25亿元。 - **毛利率下滑：** IS_毛利率 从X%降至Y%。 - **利润依赖非经常性损益：** 2023年扣非净利润 IS_DEDUCT_PARENT_NETPROFIT (yy亿元) 远低于净利润 IS_NETPROFIT (xx亿元)，差额主要来自 IS_INVEST_INCOME (zz亿元)）

                    ## 分析方法要求：

                    1.  **数据驱动**：**所有关键分析点和结论必须有明确的结构化财务数据指标（名称和数值/趋势）作为支撑。**
                    2.  **交叉验证**：严格将定性信息（PDF总结、管理层叙述）与定量数据（结构化数据）进行交叉验证，**明确指出一致、不一致或矛盾之处。**
                    3.  **趋势洞察**：分析财务指标的**时间序列变化趋势**，挖掘趋势背后的驱动因素和潜在问题。
                    4.  **结构分析**：评估资产负债表、利润表、现金流量表**内部结构**的合理性、变化趋势及其影响。
                    5.  **红旗标记**：**量化并明确**指出任何预警性的财务信号或异常变化。
                    6.  **格式清晰**：**优先使用项目符号列表（bullet points）和段落内嵌数据引用的方式进行阐述。**

                    ## 输出格式：

                    请提供一份深度财务分析报告，结构如下：

                    1.  **执行摘要**（约300字）：高度概括核心发现、主要风险以及投资启示。
                    2.  **财务与叙述一致性分析**：详细分析管理层报告可信度，**优先使用结构化列表清晰展示叙述与数据的印证或矛盾之处。**
                    3.  **财务真实性深度评估**：审视数据质量和盈利、现金流的真实性。
                    4.  **战略执行与资源配置分析**：评价公司战略落地效果和资源使用效率。
                    5.  **未来展望与隐藏风险提示**：基于历史数据模式预测未来并识别潜在风险。
                    6.  **整体财务评估与建议**：对公司财务健康状况的最终评价。
                """
                
        # 如果有额外信息，添加到prompt末尾
        if additional_info:
            prompt += f"""

                        ## 额外重要背景信息：

                        以下是财报中未包含的重要信息，请将其纳入你的分析考量：

                        {additional_info}

                        请确保将这些额外信息与财报数据和分析结合起来，以提供更全面的评估。
                       """

        prompt += """

                        请确保分析既有高度专业性，又能清晰传达复杂信息。不要回避敏感发现，直接指出任何值得关注的问题或矛盾，**并用清晰、简洁的方式呈现数据支撑你的每一个论点**。你的分析将成为投资决策的重要参考。
                  """
        return prompt
    
    def process(self, pdf_files: List[str], financial_table: Optional[str] = None, 
               batch_mode: bool = True, additional_info: Optional[str] = None) -> str:
        """
        处理PDF文件并生成分析
        
        参数:
            pdf_files (List[str]): PDF文件路径列表
            financial_table (str, optional): 财务表格数据
            batch_mode (bool): 是否使用批处理模式，默认为True
            additional_info (str, optional): 额外的提示信息
            
        返回:
            str: 生成的分析文本
        """
        if batch_mode:
            logger.info("使用批处理模式处理PDF文件")
            return self._batch_process_pdfs(pdf_files, financial_table, additional_info)
        else:
            # 一次性处理所有PDF
            logger.info("使用一次性模式处理PDF和财务数据")
            
            # 转换所有PDF到Markdown并读取内容
            markdown_contents = []
            for pdf_path in pdf_files:
                try:
                    # 使用basic_convert转换PDF到Markdown
                    pdf_path = Path(pdf_path)
                    markdown_file_path = self.markdown_dir / f"{pdf_path.stem}.md"
                    
                    if not markdown_file_path.exists():
                        logger.info(f"将PDF转换为Markdown: {pdf_path}")
                        markdown_text = basic_convert(str(pdf_path), output_dir=str(self.markdown_dir))
                        if not markdown_text:
                            logger.error(f"PDF转Markdown失败: {pdf_path}")
                            continue
                    else:
                        # 如果Markdown文件已存在，直接读取内容
                        logger.info(f"找到已转换的Markdown文件: {markdown_file_path}")
                        with open(markdown_file_path, 'r', encoding='utf-8') as f:
                            markdown_text = f.read()
                    
                    pdf_name = pdf_path.name
                    markdown_contents.append({
                        "name": pdf_name,
                        "content": markdown_text
                    })
                    logger.info(f"已处理Markdown文件: {pdf_path}")
                    
                except Exception as e:
                    logger.error(f"处理PDF文件转换为Markdown出错: {pdf_path}, {e}")
            
            if not markdown_contents:
                return "无法处理任何PDF文件"
            
            # 创建一次性处理的提示
            prompt = self._create_one_time_processing_prompt(markdown_contents, financial_table, additional_info)
            
            # 调用Gemini API处理Markdown和财务数据
            analysis = self._generate_one_time_analysis(prompt)
            
            # 保存分析结果
            analysis_file = self.base_dir / f"{self.stock_name}_{self.stock_code}_onetime_analysis.md"
            save_to_file(
                analysis, 
                analysis_file, 
                header=f"# {self.stock_name} ({self.full_stock_code}) 一次性分析"
            )
            
            logger.info(f"一次性分析已保存至 {analysis_file}")
            
            return analysis
    
    def _create_one_time_processing_prompt(self, markdown_contents: List[Dict[str, str]], 
                                          financial_table: Optional[str] = None, 
                                          additional_info: Optional[str] = None) -> str:
        """
        创建用于一次性处理多个Markdown文件和财务数据的提示
        
        参数:
            markdown_contents (list): Markdown内容列表，每项包含name和content
            financial_table (str, optional): 财务表格数据
            additional_info (str, optional): 额外的提示信息，如重要背景、行业信息等
            
        返回:
            str: 提示文本
        """
        prompt = f"""
                    # {self.stock_name}({self.full_stock_code})综合财务分析任务

                    ## 角色设定
                    你是一位资深财务分析师，专注于上市公司财报深度研究，具有敏锐的洞察力和丰富的行业经验。

                    ## 分析材料
                    我提供了以下资料供你分析：

                    ### 财报Markdown文件（已从PDF转换）：

                  """

        # 添加所有Markdown内容
        for i, item in enumerate(markdown_contents, 1):
            # 截取内容，防止提示过长
            content = item["content"]
            max_length = 12000  # 设置最大长度
            if len(content) > max_length:
                content = content[:max_length] + "...[内容过长已截断]"
            
            prompt += f"""
                        #### 文件 {i}: {item["name"]}
                        ```
                        {content}
                        ```

                       """

        # 如果有财务表格数据，添加到提示中
        if financial_table:
            prompt += f"""
                        ### 财务表格数据（结构化数据）：
                        ```
                        {financial_table}
                        ```

                       """
        
        # 添加分析任务说明
        prompt += f"""
                        ## 分析任务

                        请基于上述财报文件和财务数据，提供一份全面而深入的分析报告，包括：

                        1. **公司概况与业务模式**：简要介绍公司的主营业务、商业模式和核心竞争力

                        2. **关键财务指标分析**：
                        - 收入和利润增长趋势
                        - 盈利能力指标（毛利率、净利率、ROE等）
                        - 运营效率指标
                        - 偿债能力和资本结构

                        3. **业绩驱动因素**：
                        - 收入和利润增长的主要来源
                        - 各业务板块表现
                        - 成本结构变化

                        4. **管理层战略解读**：
                        - 公司的发展战略和执行情况
                        - 战略调整的原因和可能效果
                        - 资本支出和投资方向

                        5. **风险评估**：
                        - 管理层披露的主要风险因素
                        - 未明确披露但可能存在的风险
                        - 风险缓解措施

                        6. **未来展望**：
                        - 中长期增长潜力
                        - 可能影响未来业绩的关键因素
                        - 管理层对未来的规划和预期

                        请注意以下几点：
                        - 将财报叙述与财务数据相互验证，识别任何不一致之处
                        - 关注年度/季度间的变化趋势，而不仅是单期数据
                        - 寻找报告中可能被掩盖的潜在问题
                        - 提供你的专业判断，而不只是复述财报内容
                        - 使用量化数据支持你的观点

                        最终报告应当长度适中（2000-3000字），既有深度也有广度，能为投资者提供对{self.stock_name}的全面了解。要在回复中加入具体的财务指标和数据
                        """
                        
        # 如果有额外信息，添加到提示中
        if additional_info:
            prompt += f"""

                        ## 额外重要背景信息：

                        以下是财报中未包含的重要信息，请将其纳入你的分析考量：

                        {additional_info}

                        请确保将这些额外信息与财报数据和分析结合起来，以提供更全面的评估。
                       """

        return prompt
    
    @retry(max_retries=3, base_wait=15)
    def _generate_one_time_analysis(self, prompt: str) -> str:
        """
        生成一次性分析
        
        参数:
            prompt (str): 提示文本
            
        返回:
            str: 生成的分析文本
        """
        analysis = ""
        response = self.client.models.generate_content_stream(
            model=self.model,
            contents=prompt
        )
        
        # 获取响应文本
        for chunk in response:
            if chunk.candidates and chunk.candidates[0].content and chunk.candidates[0].content.parts:
                for part in chunk.candidates[0].content.parts:
                    if hasattr(part, 'text') and part.text:
                        analysis += part.text
                
        return analysis