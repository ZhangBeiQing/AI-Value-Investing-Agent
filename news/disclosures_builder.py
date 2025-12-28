#!/usr/bin/env python
from __future__ import annotations

import hashlib
import json
import ast
from dataclasses import dataclass
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union
import concurrent.futures
import threading
import logging

import argparse
import re
import pandas as pd


import sys
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from utlity import SymbolInfo, get_stock_data_dir, parse_symbol
from configs.stock_pool import TRACKED_A_STOCKS
from openai import OpenAI
from shared_data_access.cache_registry import CacheKind, build_cache_dir
from shared_data_access.data_access import SharedDataAccess
from news.gemini_utility import basic_convert
from dotenv import load_dotenv

load_dotenv()

LOGGER = logging.getLogger("disclosures_builder.shared")
if not LOGGER.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("[shared][%(asctime)s] %(message)s"))
    LOGGER.addHandler(handler)
LOGGER.setLevel(logging.INFO)
LOGGER.propagate = False


# 用于缓存不同用途的客户端
_CLIENTS_CACHE: Dict[str, OpenAI] = {}
_CLIENT_LOCK = threading.Lock()

# =================模型与配置=================
# 支持直接上传PDF进行分析的模型列表
SUPPORTED_DIRECT_PDF_MODELS = ["qwen-doc-turbo", "qwen-long"]


# ================= Prompt 模板 =================
SYSTEM_PROMPT = """你是一位拥有20年经验的A股专业金融分析师。
你的任务是阅读上市公司的公告文档，提取对股价有实质性影响的关键信息，并将其转化为结构化的JSON数据。
请保持客观、精准，忽略客套话和法律免责声明，直击核心。
"""

# 针对普通公告的 Prompt
NORMAL_ANNOUNCEMENT_PROMPT = """
你是一个严谨的金融文档处理AI，你的任务只有一个：**从上市公司公告中，提取100%客观、可量化的事实数据，不进行任何解读、分类或情绪判断。**

【核心原则】
1.  **绝对客观**：只提取公告中明确写明的数字、日期、名称、事件动作。不推断“利好”或“利空”。
2.  **事实罗列**：采用“谁，在什么时间，做了什么事，涉及什么数字”的平实语言串联事实。
3.  **保留模糊**：如果公告对某事件的描述模糊（如“有利于公司长期发展”），此描述本身可作为事实引用，但你不需判断它是否真的“有利”。
4.  **去除冗余**：删除所有法律模板句式（如“公司及董事会全体成员保证信息披露的内容真实、准确、完整”）、格式性语句和重复表达。

【输出JSON字段指南】
你仅输出以下5个字段，不输出任何其他字段或分析：

1.  `raw_facts` (全量事实文本)
    *   **要求**：生成一段**纯事实陈述**的文本。必须按时间顺序和逻辑，包含所有核心数据点：**公告发布日期、相关主体（公司、股东、交易对手等）、具体事件、所有金额（单位：元/万元/亿元）、比例、数量、日期（股权登记日、合同生效日等）、人名、公司名、考核指标具体数值（如营收增长率≥15%）、费用金额（如股份支付费用X亿元）等**。
    *   **格式**：连贯的段落。例如：“[日期]，[主体A]与[主体B]签署了[协议名称]。协议约定[具体条款，含金额]。同时，[主体A]披露了[另一事实，含数字]。”
    *   **禁止**：禁止使用“利好”、“利空”、“彰显信心”、“拖累业绩”等任何评价性词语。

2.  `category` (事件分类)
    *   **依据**：仅根据事件的**最表层、最无争议的动作**进行分类。
    *   **列表**：[`Financial_Report`, `Contract`, `M&A`, `Litigation`, `Regulation`, `Personnel`, `Equity_Change`, `Operation`, `Others`]
    *   **示例**：
        *   股东减持、股份质押、股权激励计划 -> `Equity_Change`
        *   收到证监会警示函、立案调查 -> `Regulation`
        *   高管辞职、董事会换届 -> `Personnel`
        *   签订销售合同、中标 -> `Contract`
        *   业绩快报、年报 -> `Financial_Report`

3.  `quantitative_data` (关键量化数据字典)
    *   **要求**：将`raw_facts`中**最重要的数字**，以键值对形式清晰剥离，便于程序直接调用。确保单位统一（通常转换为“元”或“%”）。
    *   **格式**：一个JSON对象。键名应直观，如 `total_contract_amount`, `net_profit`, `shareholding_percentage_after_change`, `pledge_ratio`。
    *   **示例**：
        ```json
        "quantitative_data": {
            "total_contract_amount": 2000000000,
            "net_profit_growth_rate": 45.71,
            "shares_pledged_this_time": 5000000,
            "accumulated_pledge_ratio": 98.5
        }
        ```

4.  `timestamp` (公告时间)
    *   **要求**：提取公告的**发布日期**，格式必须为“YYYY-MM-DD”。

5.  `title` (公告标题)
    *   **要求**：直接复制公告原标题，不做任何修改。

【输出格式约束（必须严格遵守）】
1.  整个回复只能包含一个 JSON 对象，不得附带额外文字、Markdown、注释或多余解释。
2.  必须使用标准 JSON 语法：所有键名使用双引号，值类型与字段定义一致；禁止出现反引号或中文引号。
3.  键名只能是 `raw_facts`、`category`、`quantitative_data`、`timestamp`、`title`，顺序不限，严禁新增或遗漏字段。
4.  `quantitative_data` 必须是对象；若无明确数据，请输出空对象 `{}`，不可返回 `null`、字符串或数组。
5.  整体必须可被 `json.loads` 直接解析，严禁结果结尾额外加个逗号、额外空行、换行符或多余文本，这样会导致json.loads解析不了
"""

AUDIT_AND_REFINE_PROMPT = """
你是一位**拥有联网能力的华尔街资深法务会计师**和**尽职调查(Due Diligence)专家**。
你的任务是处理由初级AI提取的上市公司公告，结合**联网搜索**获取的背景信息，为量化回测系统输出一份**极简、高保真、去伪存真**的结构化数据。

你的核心目标是：**消除由于缺乏背景信息导致的“错杀”和“漏杀”，区分“纸面风险”与“实质风险”。**

---

【处理流程】
请严格按照 **过滤 -> 搜索验证 -> 清洗 -> 深度审计** 的顺序处理。

### 第一步：极简去噪与过滤 (Aggressive Filtering)
**直接丢弃**以下类型的公告（不要输出到最终 JSON 中）：
1. **常规技术调整**：如“行权价格调整”、“权益分派实施”（保留预案）。
2. **安全范围内的质押**：大股东累计质押比例 < 50% 的“质押/解质押”。
3. **低风险关联交易**：**特指**“在集团财务公司存款/结算”的关联交易，且利率公允、未出现逾期违约历史的。（这通常是大型企业的常规资金归集，非风险）。
4. **橡皮图章会议**：“监事会决议”、“法律意见书”、“核查意见”。
5. **PR与合规文件**：ESG报告、社会责任报告、保密制度修订等。
6. **微小金额**：对于该公司体量而言金额极小（<最近一期经审计净资产0.5%）的非核心业务公告。

### 第二步：联网搜索与背景调查 (Mandatory Search & Verification)
**这是你与普通AI的区别。对于保留下来的每一条重要公告，你必须调用搜索工具进行背景核查**

1.  **针对【股权质押/冻结】**：
    *   *Search*: “XX公司 控股股东 资金链状况”， “XX公司 历史质押比例常态”。
    *   *Judgment*: 如果大股东长期维持高比例质押（>70%）且无违约记录（如某些民企龙头），风险标记为 Medium 而非 High。如果搜到近期有“债务违约”或“被列为被执行人”，标记为 High。

2.  **针对【关联交易/资金往来】**：
    *   *Search*: “关联方名称 股权结构”， “XX公司 利益输送 质疑”。
    *   *Judgment*: 区分“正常业务协同”（如向母公司采购原材料）与“掏空上市公司”（如购买母公司劣质资产、非经营性资金占用）。**若是集团财务公司存款，确认为Neutral。**

3.  **针对【业绩/合同】**：
    *   *Search*: “XX公司 行业地位”， “合同对方 履约能力”。
    *   *Judgment*: 那个合同金额占公司去年营收的比例是多少？（搜公司去年营收）。

4.  **针对【立案调查/监管函】**：
    *   *Search*: “XX公司 被立案 原因”， “历史违规记录”。
    *   *Judgment*: 是核心业务造假（High Risk）还是信披轻微违规（Low Risk）？

5.  **针对【股份回购 (Share Repurchase)】**：
    *   **Search 1 (股价锚点)**: “{公司名称} 股价 {公告日期前一日}”， “{公司名称} 历史股价走势 {公告年份}”。
    *   **Search 2 (回购历史)**: “{公司名称} 历史回购完成情况”， “XX公司 忽悠式回购”。
    *   **Judgment (核心计算)**:
        *   提取公告中的 **回购价格上限 (Cap Price)**。
        *   对比搜索到的 **公告日前一日收盘价 (Current Price)**。
        *   **计算溢价率**: (Cap Price - Current Price) / Current Price。
        *   *判断逻辑*：
            *   若溢价率 > 50%：强烈看多信号，管理层认为严重低估。
            *   若溢价率 < 10% 或 接近现价：仅仅是护盘或为了发股权激励，信号偏中性。
            *   若回购用途为**“注销 (Cancellation)”**：权重极大，直接视为 High Impact Positive。
            *   若历史上有“宣而不做”的记录：标记为 Negative/Risk。

6.  **针对【业绩报告中的关键异常指标】**:
    *   **触发条件**: 当公告中出现“利润增长与现金流严重背离”、“关键财务比率剧烈波动（超过20个百分点）”或“管理层解释模糊”时，**必须**启动调查。
    *   **强制搜索指令**:
        1.  *管理层归因*: “`{公司名称}` `{报告期}` 业绩说明会 `{异常指标，如现金流}` 解释”。
        2.  *行业坐标*: “`{行业名称}` `{报告期}` 营运资金 状况” 或 “`{同行公司}` `{报告期}` 现金流”。
        3.  *历史对比*: “`{公司名称}` 历年 `{报告期}` `{异常指标}`”。
        4.  *后续验证*: “`{公司名称}` `{报告期后一季度}` 现金流”。

### 第三步：去重与合并 (Deduplication)
- **同日/同事件合并**：若同一天（或相邻1天）出现针对同一事件的多条公告（如《摘要》、《全文》、《回查意见》），**仅保留一条**信息量最大的。
- **月度数据处理**：若同月出现“经营简报”和“数据点评”，只保留“经营简报”，【**经营简报必须保留**】。

### 第四步：摘要事实清洗 (Summary Refinement)
遵循**“事实神圣”**原则：
- **保留 (KEEP)**：硬数据（金额、日期、比例、KPI条款）。
- **删除 (REMOVE)**：删除所有宣传性废话（如“旨在...”、“为了促进...”、“符合法律法规...”）。
- **补充 (ENRICH)**：**将搜索到的关键背景信息补充进 Summary 中**（例如：“...本次质押后比例达80%，*经查该股东过去三年常态质押比例维持在75%左右*...”）。

### 第五步：深度审计与信号修正 (Deep Audit) - 重构版
在生成 `audit_analysis` 时，你必须严格遵循以下“调查-诊断”流程。本部分是你作为法务会计师的核心价值输出。

**第1步：强制调查与验证（针对财务异常）**
当报告中出现“利润增长与现金流严重背离”、“应收账款/存货激增”等关键异常时，你必须基于第二步第6点的搜索结果进行分析。

**第2步：诊断分析与风险校准**
基于调查结果，你的 `audit_analysis` 必须清晰回答以下问题，并结构化呈现：
A. **业务实质是什么？**
   - 这是**为支持未来销售的主动战略投入**（如提前备货、锁定原材料），还是**因运营效率低下或回款困难导致的被动失血**？
   - **关键证据**：你搜索到的“合同负债（预收款）变化”、“管理层在业绩会上的具体解释”是什么？

B. **横向与纵向对比是否异常？**
   - **纵向（公司自身）**：当前异常的财务指标（如现金流占营收比例）与过去3年同期相比，偏离常态多少？是否在业务扩张期曾出现类似模式？
   - **横向（行业）**：同期主要竞争对手是否面临相似压力？这是行业共性还是个体问题？

C. **风险性质的最终判断**
   - 若判断为 **“主动战略投入”** ，必须同时满足：
     1. 营收/订单（合同负债）同步高增长；
     2. 管理层有清晰、合理的业务解释（如签下大单需备货）；
     3. 历史上有类似模式且后续季度现金流能回正。
     *→ 结论应强调“短期财务压力是增长的必要代价”，并将风险性质从“经营风险”重定义为“增长性资金需求风险”。*
   - 若判断为 **“被动运营恶化”** ，通常存在：
     1. 营收增长乏力或毛利率下滑；
     2. 应收账款周转率恶化且无合理解释；
     3. 行业并无普遍性资金紧张。
     *→ 结论应坚持高风险预警。*

**第3步：输出结构化审计分析**
最终输出的 `audit_analysis` 应遵循以下句式模板：
【业务归因】基于管理层解释及财报附注，说明异常的核心原因。
【横向对比】说明此现象是行业共性还是个体特例。
【纵向对比】对比公司历史数据，判断当前是否偏离常态。
【综合诊断】给出最终定性，并明确核心监控点。

**针对思源电气/美的集团类案例的特别修正**：对于优质白马股在行业高景气周期中，因主动备货、支付货款导致的阶段性大额负现金流，必须在审计中明确指出“此为支持业务高增长的主动财务策略，其风险与增长机会并存”，并结合订单（合同负债）增长情况，显著调低其风险等级。

**最后针对【股权结构变动（含减持、赠与、离婚分割等）】的深度审计**：
    *   **调查触发条件**：只要公告涉及5%以上股东、控股股东、实际控制人、董监高的股份发生**所有权变动**（包括减持、赠与、离婚分割、继承、协议转让等），无论公告标题是否包含“减持”，都必须启动调查。
    *   **强制搜索指令**：
        *   *Search 1 (股东动机与背景)*: `"{股东姓名/名称} 近期 减持 计划"`, `"{股东姓名/名称} {公司名称} 股份 质押"`。
        *   *Search 2 (关联关系核查)*: `"{受让方/获赠方名称} 与 {股东姓名/名称} 关联关系"`, `"{公司名称} {股东姓名/名称} 一致行动人"`。
        *   *Search 3 (监管与历史记录)*: `"{股东姓名/名称} 违规 减持"`, `"{公司名称} 股价 破发 破净"`[citation:8]。
    *   **审计与判断逻辑 (核心：实质重于形式)**：
        1.  **第一步：判断是否存在“变相减持”的合理怀疑**。
            *   **怀疑信号**：股份变动后，**过出方（原股东）的持股比例显著下降，且接近或低于5%等重要披露门槛**；变动发生在公司“破发、破净、分红不达标”等减持敏感期[citation:5][citation:8]；变动方式非常规（如天价离婚、向无关联第三方赠与）。
        2.  **第二步：穿透核查，寻找关键证据**。
            *   **关键证据链**：通过搜索结果，尝试构建以下证据链：
                *   **证据A（关联性）**：过出方与过入方是否存在未披露的关联关系或潜在一致行动关系？(参考宝新能源代持案[citation:1][citation:10])
                *   **证据B（动机性）**：过出方是否有强烈的资金需求（如高比例股权质押）或近期有减持记录？
                *   **证据C（合规性）**：本次变动是否发生在公司或股东自身被立案调查、行政处罚等“不得减持”期间？[citation:2][citation:8]
        3.  **第三步：综合定性**。
            *   若发现**证据链A、B、C中任何一条成立**，且变动导致过出方持股比例大幅下降，则**高度疑似变相减持**。应在`audit_analysis`中明确指出：“该次股份变动虽形式为`{赠与/离婚等}`，但结合`{搜索到的背景}`，实质可能构成规避减持限制的套现行为。”
            *   若经查，过出方与过入方为直系亲属间财产规划、或员工持股平台内部调整等，且无上述证据链支持，则可初步判定为正常安排。

 **关于“减持”相关公告的情绪（sentiment）硬性约束规则**
1.  **适用范围**：以下所有公告类型，其 `sentiment` 字段 **绝对不得** 标记为 `Positive`，只能是 `Neutral` 或 `Negative`：
    *   **直接减持**：股东、董监高的“减持计划”、“减持进展”、“减持结果”。
    *   **间接或潜在减持**：
        *   导致股东持股比例下降的 **“股份赠与”**（如向慈善基金捐赠，但降低了自身持股）。
        *   因 **离婚、继承、法人终止** 导致的股份分割与过户。
        *   股东参与 **“转融通”出借** 业务（增加了市场流通盘）。
        *   用于员工持股计划或股权激励的 **“非交易过户”**（虽非立即套现，但解锁后形成潜在卖压）。
        *   任何可能导致股东在未来一定期限后 **无需预披露即可减持** 的股份变动（如持股比例降至5%以下）。

2.  **情绪赋值逻辑**：
    *   **标记为 `Negative` 的情况**：
        *   减持方为 **控股股东、实际控制人、核心董监高**。
        *   减持发生在公司 **股价“破发”、“破净”或近期表现低迷** 时期。
        *   减持规模 **巨大**（例如，占其持股比例较高或占日均成交量比重很大）。
        *   减持背景可疑，如股东自身 **存在高比例质押、债务缠身** 等强烈的套现动机。
        *   涉及 **隐瞒一致行动关系、违规减持** 等行为。
    *   **可标记为 `Neutral` 的罕见情况**：
        *   减持方是 **早期的财务投资人**（如VC/PE），在公司上市锁定期满后进行的 **有序、小幅、已充分预期的退出**，且对公司经营无影响。
        *   为 **实施员工持股计划** 而进行的股份非交易过户，且过户来源清晰、规模合理。
        *   **必须同时满足**：减持行为完全合规、提前充分披露、未引发市场恐慌、且公司基本面强劲。即便如此，分析中也应指出其带来的潜在流动性压力。

3.  **对应审计分析要求**：
    在 `audit_analysis` 中，凡是涉及上述行为，必须包含类似表述：
    “**无论本次股份变动以何种形式进行，其最终结果均增加了实际可流通盘或降低了重要股东的持股稳定性。** 结合`{搜索到的股东资金状况、股价位置等背景}`，此举主要反映了`{股东名称}`的`{资金需求/退出意愿}`，对二级市场情绪构成压力。”           
---

【输入数据】
初级AI提取的一段时间公告的JSON列表（包含原 summary, sentiment 等）。

【输出 JSON 字段格式要求】
对于保留下来的每一条公告，请输出以下字段：

{
  “title”: “…”, // 清洗后的标题，标题需要和原来保持一致。如果是重复标题，选择其中一个作为标题
  “datetime”: “YYYY-MM-DD”， //和原输入公告保持一致
  “category”: “…”，

  // 1. 事实层：客观、高保真的摘要
  “summary”: “…”，
  // 要求：保留原 summary 中的所有数字和核心条款。仅删去“旨在…”、“有利于…”等主观宣传语。
  // 顺丰案例标准：保留“赠与2亿股”、“授予价35元”、“锁定期12个月”等细节，删除“旨在绑定核心人才”等废话。

  // 2. 信号层：重新评估的情绪与力度
  “impact_level”: “…”，       // [High, Medium, Low] - 基于真实财务/治理影响重新定级
  “sentiment”: “…”，          // [Positive, Negative, Neutral] - 修正后的真实利空/利好方向
  “validity_period”: “…”，   // [Short, Medium, Long] - 基于锁定期/有效期判断
  // 3. 审计层：新增的批判性分析 (这是你的思考结晶)
  “audit_analysis”: “…”，
  // **必须严格按照第五步的结构化模板撰写。**
  // 针对顺丰案例：应指出“虽然不稀释股本，但授予价(35元)与市价存在差额，根据会计准则将产生巨额股份支付费用，对当期及未来几年净利润构成重大摊销压力。”
  // 针对捐赠案例：应指出“大股东通过捐赠降低持股比例，需关注是否因此将持股降至5%以下，从而规避后续减持的披露义务。”
  // **针对思源电气类现金流案例：必须包含业务归因、行业对比、历史对比和综合诊断四部分。**

  “financial_implication”: “…”，
  // 【关键】量化财务影响。如：“预计产生股份支付费用约xx亿元，计入管理费用” 或 “现金流无实质影响”。若无影响填 null。

  “price_driver”: “…”，       // 修正后的驱动逻辑（如：短期利润承压 / 减持预期 / 业务放量 / 增长性资金需求）
  “risk_warning”: “…”        // 修正后的风险提示
}
【任务开始】
请处理用户输入的JSON数据。
"""

BLACKLIST = [
    # === 1. 会议与通知类 (Process & Notice) ===
    "股东大会通知", "关于召开股东大会", "关于召开", "会议通知", 
    "业绩说明会", "接待日", "网上路演", "提示性公告", "取消", "延期",
    "通知债权人", "债权人通知", # 减资或注销时的法定流程

    # === 2. 法律合规与中介机构类 (Legal & Audit) ===
    "法律意见", "律师事务所", "核查意见", "鉴证报告", "审计报告",
    "评估报告", "保荐", "受托管理", "监督职责", "履职报告", 
    "述职报告", "自查报告", "内部控制", "内控", "社会责任", 
    "ESG", "环境", "可持续发展", "独立董事", "监事会", "绿色低碳", # 监事会通常只发合规意见，不发战略决策

    # === 3. 制度与文本类 (Docs & Rules) ===
    "公司章程", "议事规则", "制度", "管理办法", "工作细则" , "规则", "通函"
    "摘要版", "更正公告", "补充公告", "申请版本", "资料集",
    "翌日披露报表", "报表", "名册", "名单", # 过滤掉长篇大论的名单

    # === 6. 可转债与衍生品日常数据 (Derivatives Routine) ===
    "付息公告", "跟踪评级", "信用评级", "票面利率调整",
    "转股价格调整", "不向下修正", "预计满足转股价格修正条件", # 这些是技术性调整
    "转股结果", "行权结果", "股份变动结果", "实施结果", # 月度/季度例行统计

    "股份发行人的证券变动", # 完整表述
    "Monthly Return",    # 英文版关键词
    "Next Day Disclosure Return", # 港股的“翌日披露报表”，也一并过滤
    "顾问报告","会议召开日期","审核意见",  "合规性说明", "续聘", "通知信函","提名委员会职"
]

FINAL_REPORT_KEYS = [
        "年度报告", "半年度报告", "季度报告", "年报", "半年报", "一季报", "三季报","财务报告", "Annual", "业绩公告", "中期报告", "英文版"
    ]

@dataclass
class AnnouncementMeta:
    """
    摘要: 公告元数据结构
    Args:
        announcement_id: 公告唯一ID（优先从URL解析）
        org_id: 机构ID
        stock_code: 股票代码
        title: 公告标题
        date: 公告日期（YYYY-MM-DD）
        url: 公告详情页URL
        pdf_path: 本地PDF路径（可为空表示未下载）
        md_path: 本地Markdown路径（可为空）
        file_id: 模型云盘fileId（可为空）
        downloaded: 是否已下载PDF
        summarized: 是否已生成摘要并写入news.json
        summary_json_path: 单条摘要文件路径
        category: 公告类别（简单枚举或规则识别）
        is_financial_report: 是否为财报类公告
        dedupe_key: 去重键（如标题+日期哈希）
    Returns:
        AnnouncementMeta实例
    """
    announcement_id: str
    org_id: Optional[str]
    stock_code: str
    title: str
    date: str
    url: str
    pdf_path: Optional[str] = None
    md_path: Optional[str] = None
    file_id: Optional[str] = None
    downloaded: bool = False
    summarized: bool = False
    summary_json_path: Optional[str] = None
    category: Optional[str] = None
    is_financial_report: bool = False
    dedupe_key: Optional[str] = None
    audited: bool = False
    audit_model: Optional[str] = None
    audit_timestamp: Optional[str] = None


def _slugify(text: str) -> str:
    return "".join(ch if ch.isalnum() else "_" for ch in text).strip("_")[:80]


def _hash_key(*parts: str) -> str:
    h = hashlib.sha256("||".join(parts).encode("utf-8")).hexdigest()
    return h[:16]


def _meta_key(meta: AnnouncementMeta) -> str:
    return meta.announcement_id or meta.dedupe_key or _hash_key(meta.title, meta.date)


def _item_key(item: Dict[str, Any]) -> str:
    key = item.get("announcement_id") or item.get("dedupe_key")
    if key:
        return str(key)
    return _hash_key(str(item.get("title") or ""), str(item.get("datetime") or ""))


def _chunk_list(seq: List[Any], size: int):
    step = max(size, 1)
    for i in range(0, len(seq), step):
        yield seq[i : i + step]


def _sort_items_desc(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    def _key(it: Dict[str, Any]) -> datetime:
        raw = it.get("datetime") or it.get("date")
        if not raw:
            return datetime.min
        try:
            return datetime.strptime(str(raw)[:10], "%Y-%m-%d")
        except Exception:
            return datetime.min

    return sorted(items, key=_key, reverse=True)


def _log(msg: str) -> None:
    """
    摘要: 标准化调试打印输出（带时间与模块标签）
    Args:
        msg: 文本消息
    Returns:
        None
    """
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[disclosures][{ts}] {msg}", flush=True)


def parse_announcement_row(row: Dict[str, Any]) -> Tuple[str, str, str, str, Optional[str]]:
    """
    摘要: 从 akshare 行记录提取关键字段
    Args:
        row: akshare返回的记录字典（已转换）
    Returns:
        (stock_code, title, date, url, announcement_id)
    """
    stock_code = str(row.get("代码") or row.get("stockCode") or "").strip()
    title = str(row.get("公告标题") or row.get("title") or "").strip()
    dt = str(row.get("公告时间") or row.get("date") or "").strip()
    date = dt.split(" ")[0]
    url = str(row.get("公告链接") or row.get("url") or "").strip()

    announcement_id = None
    if "announcementId=" in url:
        try:
            announcement_id = url.split("announcementId=")[1].split("&")[0]
        except Exception:
            announcement_id = None
    return stock_code, title, date, url, announcement_id


def load_index(index_path: Path) -> Dict[str, AnnouncementMeta]:
    """
    摘要: 读取公告索引为字典
    Args:
        index_path: 索引文件路径
    Returns:
        以announcement_id或dedupe_key为键的字典
    """
    if not index_path.exists():
        _log(f"索引不存在，返回空: {index_path}")
        return {}
    try:
        raw = json.loads(index_path.read_text(encoding="utf-8"))
        out: Dict[str, AnnouncementMeta] = {}
        for key, meta in raw.items():
            out[key] = AnnouncementMeta(**meta)
        _log(f"索引加载完成: {index_path}, 条数={len(out)}")
        return out
    except Exception:
        _log(f"索引读取失败，返回空: {index_path}")
        return {}


def save_index(index_path: Path, items: Dict[str, AnnouncementMeta]) -> None:
    """
    摘要: 保存公告索引
    Args:
        index_path: 索引文件路径
        items: 索引字典
    Returns:
        None
    """
    payload = {k: vars(v) for k, v in items.items()}
    index_path.parent.mkdir(parents=True, exist_ok=True)
    index_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    _log(f"索引已保存: {index_path}, 条数={len(items)}")


def save_index_merge(index_path: Path, items: Dict[str, AnnouncementMeta]) -> None:
    """
    摘要: 以追加合并方式保存公告索引（保留旧数据并覆盖同键新数据）
    Args:
        index_path: 索引文件路径
        items: 当前内存中的索引字典
    Returns:
        None
    """
    existing: Dict[str, Any] = {}
    if index_path.exists():
        try:
            existing = json.loads(index_path.read_text(encoding="utf-8"))
        except Exception:
            existing = {}
    merged: Dict[str, Any] = dict(existing)
    for k, v in items.items():
        merged[k] = vars(v)
    index_path.parent.mkdir(parents=True, exist_ok=True)
    index_path.write_text(json.dumps(merged, ensure_ascii=False, indent=2), encoding="utf-8")
    _log(f"索引已合并保存: {index_path}, 旧条数={len(existing)}, 新条数={len(items)}, 合并后={len(merged)}")


def disclosures_cache_dir(symbol_info: SymbolInfo) -> Path:
    return build_cache_dir(symbol_info, CacheKind.DISCLOSURES)


def pdfs_dir(symbol_info: SymbolInfo) -> Path:
    return disclosures_cache_dir(symbol_info) / "pdfs"


def md_dir(symbol_info: SymbolInfo) -> Path:
    return disclosures_cache_dir(symbol_info) / "md"


def index_path(symbol_info: SymbolInfo) -> Path:
    return disclosures_cache_dir(symbol_info) / "index.json"


def news_json_path(symbol_info: SymbolInfo) -> Path:
    root = get_stock_data_dir(symbol_info)
    return root / "news" / "news.json"


def download_pdf(url: str, out_path: Path) -> bool:
    """
    摘要: 下载公告PDF，优先通过公告详情接口获取真实直链，必要时回退到静态路径规则。
    Args:
        url: 公告详情URL
        out_path: PDF输出路径
    Returns:
        是否下载成功
    """
    import requests
    from urllib.parse import parse_qs, unquote, urlparse

    user_agent = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
    api_headers = {
        "User-Agent": user_agent,
        "Accept": "application/json, text/plain, */*",
        "X-Requested-With": "XMLHttpRequest",
        "Origin": "https://www.cninfo.com.cn",
        "Referer": url,
    }
    download_headers = {
        "User-Agent": user_agent,
        "Accept": "application/pdf,application/octet-stream,*/*;q=0.8",
        "Referer": url,
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "Connection": "keep-alive",
    }
    out_path.parent.mkdir(parents=True, exist_ok=True)
    _log(f"尝试下载PDF: {url} -> {out_path}")

    def _save_from_url(pdf_url: str) -> bool:
        if not pdf_url:
            return False
        _log(f"尝试PDF直链: {pdf_url}")
        try:
            resp = requests.get(pdf_url, headers=download_headers, timeout=60, stream=True)
            resp.raise_for_status()
            with open(out_path, "wb") as f:
                for chunk in resp.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
            ok = out_path.exists() and out_path.stat().st_size > 0
            _log(f"PDF下载{'成功' if ok else '失败'}: {pdf_url}")
            return ok
        except Exception as exc:
            _log(f"PDF直链下载失败: {pdf_url}, 错误: {exc}")
            return False

    parsed = urlparse(url)
    query_params = parse_qs(parsed.query)

    def _extract_param(name: str) -> str:
        values = query_params.get(name)
        if not values:
            return ""
        return unquote(str(values[0]))

    announcement_id = _extract_param("announcementId")
    announcement_time = _extract_param("announcementTime")
    plate = _extract_param("plate").lower()

    if announcement_id and announcement_time:
        detail_api = "https://www.cninfo.com.cn/new/announcement/bulletin_detail"
        detail_params = {
            "announceId": announcement_id,
            "flag": "true" if plate == "szse" else "false",
            "announceTime": announcement_time,
        }
        try:
            resp = requests.post(detail_api, params=detail_params, headers=api_headers, timeout=20)
            resp.raise_for_status()
            payload = resp.json()
            file_url = payload.get("fileUrl")
            if not file_url:
                announcement = payload.get("announcement") or {}
                adjunct_url = announcement.get("adjunctUrl")
                if adjunct_url:
                    base = "https://static.cninfo.com.cn/"
                    file_url = base.rstrip("/") + "/" + adjunct_url.lstrip("/")
            if file_url and _save_from_url(file_url):
                return True
            _log("公告详情接口未返回有效PDF地址，尝试静态路径")
        except Exception as exc:
            _log(f"公告详情接口请求失败: {exc}")

    ann_date = announcement_time.split(" ")[0] if announcement_time else ""
    if not ann_date and "announcementTime=" in url:
        ann_date = unquote(url.split("announcementTime=")[1].split("&")[0]).split(" ")[0]
    if announcement_id and ann_date:
        pdf_urls = [
            f"https://static.cninfo.com.cn/finalpage/{ann_date}/{announcement_id}.PDF",
            f"https://static.cninfo.com.cn/finalpage/{ann_date}/{announcement_id}.pdf",
            f"http://static.cninfo.com.cn/finalpage/{ann_date}/{announcement_id}.PDF",
            f"http://static.cninfo.com.cn/finalpage/{ann_date}/{announcement_id}.pdf",
        ]
        for pdf_url in pdf_urls:
            if _save_from_url(pdf_url):
                return True

    _log("所有PDF下载尝试失败")
    return False


def is_financial_report(title: str) -> bool:
    return any(k in title for k in FINAL_REPORT_KEYS)


def simple_category(title: str) -> str:
    if is_financial_report(title):
        return "Financial"
    keys = {
        "合同": "Contract",
        "诉讼": "Litigation",
        "股东大会": "Governance",
        "回购": "Capital",
    }
    for k, v in keys.items():
        if k in title:
            return v
    return "Others"


def should_skip_announcement(title: str) -> bool:
    """
    摘要: 判断公告是否为低价值/冗余，进行过滤
    Args:
        title: 公告标题
    Returns:
        是否应跳过该公告
    """
    return any(k in title for k in BLACKLIST)


def _get_openai_client(purpose: str, model_name_for_logging: str) -> OpenAI:
    """
    摘要: 构建并缓存一个OpenAI兼容客户端。
    它会根据 'purpose' 从环境变量中读取特定的 API KEY 和 BASE URL。
    如果未找到所需的环境变量，将直接报错并退出。
    Args:
        purpose (str): 用途， "extraction" 或 "audit"。
        model_name_for_logging (str): 用于日志记录的模型名称。
    Returns:
        OpenAI 客户端实例。
    Raises:
        ValueError: 如果相关的环境变量未在.env文件中设置。
    """
    client_key = purpose
    
    with _CLIENT_LOCK:
        if client_key in _CLIENTS_CACHE:
            return _CLIENTS_CACHE[client_key]

        if purpose == "audit":
            api_key_env = "AUDIT_MODEL_API_KEY"
            base_url_env = "AUDIT_MODEL_BASE_URL"
        elif purpose == "extraction":
            api_key_env = "EXTRACTION_MODEL_API_KEY"
            base_url_env = "EXTRACTION_MODEL_BASE_URL"
        else:
            error_message = f"内部错误: 调用客户端时提供了未知的用途: {purpose}"
            _log(error_message)
            raise ValueError(error_message)

        api_key = os.getenv(api_key_env)
        base_url = os.getenv(base_url_env)

        if not api_key:
            error_message = f"配置错误: 必须在 .env 文件中设置环境变量 '{api_key_env}'。"
            _log(error_message)
            raise ValueError(error_message)
        
        if not base_url:
            error_message = f"配置错误: 必须在 .env 文件中设置环境变量 '{base_url_env}'。"
            _log(error_message)
            raise ValueError(error_message)
        
        client = OpenAI(api_key=api_key, base_url=base_url)
        _CLIENTS_CACHE[client_key] = client
        _log(f"为 {purpose} ({model_name_for_logging}) 的OpenAI兼容客户端已初始化 (base_url: {base_url})")
        return client


def upload_file_to_dashscope(file_path: Path) -> Optional[str]:
    """
    摘要: 上传本地文件到百炼并返回 file_id
    Args:
        file_path: 本地文件路径
    Returns:
        file_id 字符串；失败返回 None
    """
    client = _get_openai_client("extraction", "qwen-doc-turbo/file-upload")
    if client is None:
        _log("上传文件失败：客户端未初始化")
        return None
    try:
        obj = client.files.create(file=file_path, purpose="file-extract")
        fid = getattr(obj, "id", None)
        _log(f"文件上传完成: {file_path}, file_id={fid}")
        return fid
    except Exception:
        _log("文件上传异常")
        return None


def _build_general_prompt(stock_name: str, symbol: str, meta: AnnouncementMeta) -> str:
    return (
        "请从该公告中提取结构化关键信息并仅以JSON对象返回，不要输出额外文字。" 
        "字段要求：title(字符串)，summary(不超过1000字)，impact(High/Medium/Low)，"
        "sentiment(Positive/Negative/Neutral)，influence_window(如'短期/中期/长期')，"
        "category(字符串)，date(YYYY-MM-DD)。"
    )


def _build_financial_prompt(stock_name: str, symbol: str, meta: AnnouncementMeta) -> str:
    return (
        "这是定期财报类公告，请仅以JSON对象输出定性摘要，字段："
        "title，date，management_discussion(经营情况与展望，≤500字)，"
        "risks(关键风险与不确定性，≤120字)，"
        "major_matters(重大事项/合同/投融资，≤150字)，"
        "sentiment(Positive/Negative/Neutral)，impact(High/Medium/Low)，"
        "influence_window(中期/长期)。"
        "不要输出额外文字。"
    )


JSON_DECODER = json.JSONDecoder()
_BACKTICK_KEY_PATTERN = re.compile(r"`([^`]+)`")
_BROKEN_KEY_PATTERN = re.compile(r'"([^"\n]+):')


def _normalize_json_like(snippet: str) -> str:
    """
    将部分常见的“类JSON”格式修正为合法JSON，便于解析。
    当前修正常见问题：
    1. 键名使用反引号而非双引号
    2. 中文/花式引号
    """
    normalized = snippet
    if "`" in normalized:
        normalized = _BACKTICK_KEY_PATTERN.sub(lambda m: f"\"{m.group(1).strip()}\"", normalized)
    if "“" in normalized or "”" in normalized:
        normalized = normalized.replace("“", "'").replace("”", "'")
    if "‘" in normalized or "’" in normalized:
        normalized = normalized.replace("‘", "'").replace("’", "'")
    return normalized


def _parse_json_response_text(text: str) -> Optional[Union[Dict[str, Any], List[Any]]]:
    """
    摘要: 从模型输出中鲁棒提取并解析JSON（支持```json fenced块）。
    Args:
        text: 原始模型输出文本
    Returns:
        解析后的字典；失败返回None
    """
    if not text:
        return None

    def _attempt(snippet: str) -> Optional[Union[Dict[str, Any], List[Any]]]:
        snippet = (snippet or "").strip()
        if not snippet:
            return None

        snippet = (
            snippet.replace("\ufeff", "")
            .replace("\u200b", "")
            .replace("\r\n", "\n")
            .replace("\r", "\n")
        )

        candidates: List[str] = []

        def _push(text: str) -> None:
            text = (text or "").strip()
            if text and text not in candidates:
                candidates.append(text)

        _push(snippet)
        _push(_normalize_json_like(snippet))

        for ch in ("`", "\"", "'"):
            if snippet.startswith(ch) and snippet.endswith(ch) and len(snippet) >= 2:
                _push(snippet[1:-1])

        for existing in list(candidates):
            idx = max(existing.rfind("}"), existing.rfind("]"))
            if idx != -1 and idx < len(existing) - 1:
                _push(existing[: idx + 1])
            trimmed_newline = existing.rstrip("\n")
            if trimmed_newline != existing:
                _push(trimmed_newline)

        for candidate in candidates:
            try:
                return json.loads(candidate)
            except Exception:
                pass
            try:
                return ast.literal_eval(candidate)
            except Exception:
                pass
            try:
                obj, _ = JSON_DECODER.raw_decode(candidate)
                return obj
            except Exception:
                continue
        return None

    t = text.strip()
    parsed = _attempt(t)
    if parsed is not None:
        return parsed

    fence_patterns = [
        (r"```json\s*([\s\S]*?)```", re.IGNORECASE),
        (r"```\s*([\s\S]*?)```", 0),
    ]
    for pattern, flags in fence_patterns:
        for match in re.finditer(pattern, t, flags):
            parsed = _attempt(match.group(1))
            if parsed is not None:
                return parsed

    first_brace_idx = next((i for i, ch in enumerate(t) if ch in "{["), -1)
    if first_brace_idx != -1:
        parsed = _attempt(t[first_brace_idx:])
        if parsed is not None:
            return parsed

    list_match = re.search(r"(\[[\s\S]*?\])", t)
    if list_match:
        parsed = _attempt(list_match.group(1))
        if parsed is not None:
            return parsed

    obj_match = re.search(r"(\{[\s\S]*\})", t)
    if obj_match:
        parsed = _attempt(obj_match.group(1))
        if parsed is not None:
            return parsed

    return None


def qwen_doc_summarize_with_fileid(stock_name: str, symbol: str, meta: AnnouncementMeta) -> Optional[Dict[str, Any]]:
    """
    摘要: 使用 Qwen-Doc-Turbo 基于 file_id 生成公告结构化摘要
    Args:
        stock_name: 股票名称
        symbol: 股票代码
        meta: 公告元数据（需包含 file_id）
    Returns:
        新闻项字典；失败返回 None
    """
    client = _get_openai_client("extraction", "qwen-doc-turbo")
    if client is None or not meta.file_id:
        _log("无法调用摘要：客户端未初始化或缺少file_id")
        return None
    prompt = NORMAL_ANNOUNCEMENT_PROMPT
    try:
        _log(f"调用Qwen摘要: stock_name={stock_name}, file_id={meta.file_id}, title={meta.title[:40]}")
        completion = client.chat.completions.create(
            model="qwen-doc-turbo",
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "system", "content": f"fileid://{meta.file_id}"},
                {"role": "user", "content": prompt},
            ]
        )
        content = completion.choices[0].message.content
        data = _parse_json_response_text(content)
        if not isinstance(data, dict):
            raise RuntimeError(f"Qwen摘要返回无法解析为字典: {content}")
        _log("Qwen摘要解析成功，返回结构化字段")
        raw_facts = (data.get("raw_facts") or data.get("summary") or meta.title).strip()
        category = data.get("category") or "Others"
        quantitative_data = data.get("quantitative_data")
        if not isinstance(quantitative_data, dict):
            quantitative_data = {}
        timestamp = data.get("timestamp") or data.get("date") or meta.date
        title = data.get("title") or meta.title
        return {
            "announcement_id": meta.announcement_id or meta.dedupe_key,
            "dedupe_key": meta.dedupe_key,
            "title": title,
            "datetime": timestamp,
            "summary": raw_facts,
            "raw_facts": raw_facts,
            "category": category,
            "quantitative_data": quantitative_data,
            "source": "公告",
        }
    except Exception as exc:
        _log("调用Qwen摘要异常")
        if isinstance(exc, RuntimeError):
            raise
        return None


def convert_pdf_to_markdown(pdf_path: Path, md_path: Path) -> Optional[str]:
    """
    摘要: 将PDF文件转换为Markdown，并利用缓存
    Args:
        pdf_path (Path): 输入的PDF文件路径
        md_path (Path): 缓存Markdown的输出路径
    Returns:
        Optional[str]: 转换后的Markdown文本，如果失败则返回None
    """
    if md_path.exists() and md_path.stat().st_size > 0:
        _log(f"使用缓存的Markdown: {md_path}")
        return md_path.read_text(encoding="utf-8")

    _log(f"开始将PDF转换为Markdown: {pdf_path}")
    try:
        markdown_content = basic_convert(str(pdf_path), output_dir=str(md_path.parent))
        if markdown_content:
            if md_path.exists() and md_path.stat().st_size == 0:
                md_path.write_text(markdown_content, encoding="utf-8")
            _log(f"Markdown转换成功: {md_path}")
            return markdown_content
        _log(f"PDF转Markdown失败: basic_convert没有返回有效内容 for {pdf_path}")
        return None
    except Exception as e:
        _log(f"PDF转Markdown过程出错: {pdf_path}, 错误: {e}")
        return None


def qwen_summarize_with_markdown(stock_name: str, symbol: str, meta: AnnouncementMeta, markdown_content: str, model: str) -> Optional[Dict[str, Any]]:
    """
    摘要: 使用Qwen模型基于Markdown文本生成公告结构化摘要
    Args:
        stock_name (str): 股票名称
        symbol (str): 股票代码
        meta (AnnouncementMeta): 公告元数据
        markdown_content (str): Markdown格式的公告内容
        model (str): 使用的AI模型
    Returns:
        新闻项字典；失败返回 None
    """
    client = _get_openai_client("extraction", model)
    if client is None:
        _log("无法调用摘要：客户端未初始化")
        return None

    prompt = NORMAL_ANNOUNCEMENT_PROMPT
    # 为了避免超长，对markdown内容进行截断
    max_length = 50000  # 根据模型上下文调整
    truncated_content = markdown_content[:max_length]

    try:
        _log(f"调用模型({model})基于Markdown进行摘要: title={meta.title[:40]}")
        completion = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": f"这是公告的Markdown内容:\n\n{truncated_content}\n\n请根据以上内容完成任务。"},
                {"role": "user", "content": prompt},
            ],
        )
        content = completion.choices[0].message.content
        data = _parse_json_response_text(content)
        if not isinstance(data, dict):
            raise RuntimeError(f"模型({model})返回无法解析为字典: {content}")
        _log("模型摘要解析成功，返回结构化字段")
        raw_facts = (data.get("raw_facts") or data.get("summary") or meta.title).strip()
        category = data.get("category") or "Others"
        quantitative_data = data.get("quantitative_data")
        if not isinstance(quantitative_data, dict):
            quantitative_data = {}
        timestamp = data.get("timestamp") or data.get("date") or meta.date
        title = data.get("title") or meta.title
        return {
            "announcement_id": meta.announcement_id or meta.dedupe_key,
            "dedupe_key": meta.dedupe_key,
            "title": title,
            "datetime": timestamp,
            "summary": raw_facts,
            "raw_facts": raw_facts,
            "category": category,
            "quantitative_data": quantitative_data,
            "source": "公告",
        }
    except Exception as exc:
        _log(f"调用模型({model})摘要异常")
        if isinstance(exc, RuntimeError):
            raise
        return None


def write_news_json(symbol_info: SymbolInfo, items: List[Dict[str, Any]]) -> None:
    """
    摘要: 追加写入 news/news.json（保留已存在内容并追加新项）
    Args:
        stock_name: 股票名称
        symbol: 股票代码
        items: 新增的新闻项列表（仅追加，不覆盖已有项）
    Returns:
        None
    """
    target = news_json_path(symbol_info)
    target.parent.mkdir(parents=True, exist_ok=True)
    if not items:
        _log("无新增新闻项，跳过写入")
        return
    existing: Dict[str, Any] = {}
    if target.exists():
        try:
            existing = json.loads(target.read_text(encoding="utf-8"))
        except Exception:
            existing = {}
    prev_items = list(existing.get("news_items", []))
    merged_raw = prev_items + items
    seen = set()
    merged: List[Dict[str, Any]] = []
    for it in merged_raw:
        key = _item_key(it)
        if key in seen:
            continue
        seen.add(key)
        merged.append(it)
    merged = _sort_items_desc(merged)
    _log(f"写入news.json: {target}, 旧条数={len(prev_items)}, 新增={len(items)}, 合并后={len(merged)}")
    payload = {
        "stock": existing.get("stock") or f"{symbol_info.stock_name} ({symbol_info.symbol})",
        "today": datetime.now().strftime("%Y-%m-%d"),
        "news_items": merged,
        "diagnostics": existing.get("diagnostics", []),
    }
    target.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def update_disclosures_for_stock(
    symbol_info: SymbolInfo,
    lookback_days: int = 365,
    model: str = "qwen-doc-turbo",
    data_access: Optional[SharedDataAccess] = None,
) -> int:
    """
    摘要: 构建或增量更新单只股票的公告新闻
    Args:
        symbol_info: 股票信息对象
        lookback_days: 首次入池回溯天数，默认365
        model: 用于摘要的AI模型
        data_access: 可选的共享数据访问实例
    Returns:
        成功写入的新闻条数
    """
    stock_name = symbol_info.stock_name
    symbol = symbol_info.symbol
    _log(f"开始更新公告: {stock_name} ({symbol}), lookback={lookback_days}天, model={model}")
    idx_path = index_path(symbol_info)
    idx = load_index(idx_path)

    access = data_access or SharedDataAccess(logger=LOGGER)
    prepared = access.prepare_dataset(
        symbolInfo=symbol_info,
        as_of_date=datetime.now().strftime("%Y-%m-%d"),
        include_disclosures=True,
        disclosure_lookback_days=lookback_days,
    )
    disclosure_bundle = prepared.disclosures
    if disclosure_bundle is None or disclosure_bundle.frame.empty:
        _log("公告列表为空，返回")
        return 0
    df = disclosure_bundle.frame

    items: List[Dict[str, Any]] = []
    batch_items: List[Dict[str, Any]] = []
    processed_since_flush = 0
    for _, row in df.iterrows():
        row_dict = row.to_dict()
        stock_code, title, date, url, ann_id = parse_announcement_row(row_dict)
        _log(f"处理公告: {date} {title[:40]}... ann_id={ann_id}")
        if should_skip_announcement(title):
            _log("命中过滤黑名单，跳过")
            continue
        if is_financial_report(title):
            _log("跳过财报公告")
            continue

        key = ann_id or _hash_key(title, date)
        if key in idx:
            meta = idx[key]
            if meta.summarized:
                _log(f"已摘要，跳过: key={key}")
                continue
        else:
            meta = AnnouncementMeta(
                announcement_id=ann_id or key,
                org_id=str(row_dict.get("orgId") or ""),
                stock_code=stock_code,
                title=title,
                date=date,
                url=url,
                category=simple_category(title),
                is_financial_report=False,
                dedupe_key=key,
            )
            _log(f"加入索引: key={key}")

        if not meta.downloaded:
            file_name = f"{date}__{stock_code}__{meta.announcement_id}__{_slugify(title)}.pdf"
            out = pdfs_dir(symbol_info) / file_name
            ok = download_pdf(url, out)
            if ok:
                meta.pdf_path = str(out)
                meta.downloaded = True
                _log(f"PDF下载完成: {out}")

        if not meta.summarized and meta.downloaded:
            summary_item = None
            use_direct_pdf = model in SUPPORTED_DIRECT_PDF_MODELS
            _log(f"模型 {model} 是否支持PDF直读: {use_direct_pdf}")

            if use_direct_pdf:
                if not meta.file_id:
                    fid = upload_file_to_dashscope(Path(meta.pdf_path))
                    if fid:
                        meta.file_id = fid
                        _log(f"记录file_id: {fid}")
                if meta.file_id:
                    summary_item = qwen_doc_summarize_with_fileid(stock_name, symbol, meta)
            else:
                # PDF -> Markdown -> LLM 流程
                md_file_name = f"{date}__{stock_code}__{meta.announcement_id}__{_slugify(title)}.md"
                md_path_obj = md_dir(symbol_info) / md_file_name
                markdown_content = convert_pdf_to_markdown(Path(meta.pdf_path), md_path_obj)
                if markdown_content:
                    meta.md_path = str(md_path_obj)
                    summary_item = qwen_summarize_with_markdown(stock_name, symbol, meta, markdown_content, model)

            if summary_item is None:
                _log(f"无法为公告 {meta.announcement_id} 生成摘要，跳过")
                continue

            if summary_item is not None:
                items.append(summary_item)
                batch_items.append(summary_item)
                meta.summarized = True
                processed_since_flush += 1
                if processed_since_flush >= 5:
                    idx[key] = meta
                    save_index_merge(idx_path, idx)
                    write_news_json(symbol_info, batch_items)
                    _log(f"批次落盘：已处理5条，索引与news已保存")
                    batch_items = []
                    processed_since_flush = 0
        idx[key] = meta

    if batch_items:
        save_index_merge(idx_path, idx)
        write_news_json(symbol_info, batch_items)
        _log(f"末批次落盘：剩余{len(batch_items)}条已保存")
    else:
        save_index_merge(idx_path, idx)
    if not items:
        _log("本次无新增项，跳过写入news.json")
        return 0
    _log(f"更新完成，共新增{len(items)}条")
    return len(items)


def update_all_tracked_stocks(tracked: List[SymbolInfo], model: str, lookback_days: int = 365) -> Dict[str, int]:
    """
    摘要: 批量更新股票公告新闻（并发版）
    Args:
        tracked: SymbolInfo 列表
        model: 用于摘要的AI模型
        lookback_days: 回溯天数
    Returns:
        每只股票新增条数统计
    """
    _log(f"开始批量并发更新 {len(tracked)} 只股票")
    out: Dict[str, int] = {}
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
        future_to_stock = {
            executor.submit(
                update_disclosures_for_stock,
                symbol_info,
                model=model,
                lookback_days=lookback_days,
            ): symbol_info for symbol_info in tracked
        }
        
        for future in concurrent.futures.as_completed(future_to_stock):
            stock_info = future_to_stock[future]
            stock_symbol = stock_info.symbol
            try:
                cnt = future.result()
                out[stock_symbol] = cnt
                _log(f"批量更新完成: {stock_info.stock_name}({stock_symbol}) 新增={cnt}")
            except Exception as exc:
                _log(f"股票 {stock_symbol} 在并发更新中产生异常: {exc}")
                out[stock_symbol] = -1  # 标记为失败

    return out


def audit_news_json(symbol_info: SymbolInfo, audit_model: str) -> None:
    """
    摘要: 对 news.json 中新增的公告摘要执行增量审计，只处理尚未审计的记录。
    """
    stock_name = symbol_info.stock_name
    symbol = symbol_info.symbol
    _log(f"开始对 {stock_name}({symbol}) 的news.json进行增量审计，模型: {audit_model}")
    news_path = news_json_path(symbol_info)
    audited_news_path = news_path.with_name("news_audited.json")
    idx_path = index_path(symbol_info)
    idx = load_index(idx_path)

    if not news_path.exists():
        _log(f"源新闻文件不存在，跳过审计: {news_path}")
        return

    with open(news_path, "r", encoding="utf-8") as f:
        src_payload = json.load(f)
    src_items = list(src_payload.get("news_items") or [])
    if not src_items:
        _log("news.json为空，无需审计")
        return

    news_lookup = {_item_key(it): it for it in src_items}
    news_modified = False

    def _align_summary_item(meta: AnnouncementMeta) -> Optional[Dict[str, Any]]:
        """根据标题+日期对齐旧news项，并补写缺失字段。"""
        title = (meta.title or "").strip()
        ann_id = meta.announcement_id or meta.dedupe_key
        for item in src_items:
            item_title = (item.get("title") or "").strip()
            if item_title != title:
                continue
            raw_date = str(item.get("datetime") or item.get("date") or "").split(" ")[0]
            if raw_date != meta.date:
                continue
            if ann_id:
                item.setdefault("announcement_id", ann_id)
            if meta.dedupe_key:
                item.setdefault("dedupe_key", meta.dedupe_key)
            return item
        return None
    pending_meta = [
        meta for meta in idx.values()
        if meta.summarized and not meta.audited
    ]
    if not pending_meta:
        _log("所有公告均已审计，退出")
        return

    pending_meta.sort(key=lambda m: m.date)
    pending_pairs: List[Tuple[AnnouncementMeta, Dict[str, Any]]] = []
    for meta in pending_meta:
        key = _meta_key(meta)
        summary_item = news_lookup.get(key)
        if not summary_item:
            summary_item = _align_summary_item(meta)
            if summary_item:
                news_lookup[key] = summary_item
                news_modified = True
                _log(f"旧版摘要缺少announcement_id，已补写: {meta.title} ({meta.date})")
            else:
                _log(f"缺少公告摘要，无法审计: {meta.title} ({meta.date})")
                continue
        pending_pairs.append((meta, summary_item))

    if news_modified:
        src_payload["news_items"] = src_items
        with open(news_path, "w", encoding="utf-8") as f:
            json.dump(src_payload, f, ensure_ascii=False, indent=2)
        _log("已修复 news.json 中缺失 announcement_id 的记录")

    client = _get_openai_client("audit", audit_model)
    if client is None:
        _log("审计模型初始化失败，退出")
        return

    audited_payload: Dict[str, Any] = {}
    audited_items: List[Dict[str, Any]] = []
    audited_seen: set[str] = set()
    if audited_news_path.exists():
        try:
            with open(audited_news_path, "r", encoding="utf-8") as f:
                audited_payload = json.load(f)
            audited_items = list(audited_payload.get("news_items") or [])
            for item in audited_items:
                audited_seen.add(_item_key(item))
        except Exception:
            audited_payload = {}
            audited_items = []
            audited_seen = set()

    if audited_seen:
        filtered_pairs: List[Tuple[AnnouncementMeta, Dict[str, Any]]] = []
        skipped = 0
        for meta, summary_item in pending_pairs:
            item_key = _item_key(summary_item)
            if item_key in audited_seen:
                meta.audited = True
                meta.audit_model = meta.audit_model or audit_model or "cached"
                meta.audit_timestamp = meta.audit_timestamp or audited_payload.get("today")
                skipped += 1
                continue
            filtered_pairs.append((meta, summary_item))
        if skipped:
            save_index_merge(idx_path, idx)
            _log(f"跳过 {skipped} 条已存在于审计结果中的公告，避免重复审计")
        pending_pairs = filtered_pairs

    if not pending_pairs:
        if news_modified:
            save_index_merge(idx_path, idx)
        _log("无可审计的公告摘要，退出")
        return

    def _flush_audited() -> None:
        audited_items.sort(key=lambda x: x.get("datetime", ""), reverse=True)
        out_payload = {
            "stock": audited_payload.get("stock") or src_payload.get("stock") or f"{stock_name} ({symbol})",
            "today": datetime.now().strftime("%Y-%m-%d"),
            "news_items": audited_items,
            "diagnostics": audited_payload.get("diagnostics") or src_payload.get("diagnostics") or [],
        }
        with open(audited_news_path, "w", encoding="utf-8") as f:
            json.dump(out_payload, f, ensure_ascii=False, indent=2)

    def _merge_audited(new_items: List[Dict[str, Any]]) -> int:
        added = 0
        for it in new_items:
            if not isinstance(it, dict):
                continue
            key = _item_key(it)
            if key in audited_seen:
                continue
            audited_seen.add(key)
            audited_items.append(it)
            added += 1
        if added:
            _flush_audited()
            _log(f"审计结果新增 {added} 条")
        return added

    total_processed = 0
    batch_size = 7
    for batch in _chunk_list(pending_pairs, batch_size):
        payload = [item for _, item in batch]
        result_list: List[Dict[str, Any]] = []
        try:
            completion = client.chat.completions.create(
                model=audit_model,
                messages=[
                    {"role": "user", "content": AUDIT_AND_REFINE_PROMPT},
                    {"role": "user", "content": json.dumps(payload, ensure_ascii=False, indent=2)},
                ],
                extra_body={
                        "enable_search": True, 
                        "search_options": {
                            "forced_search": True,  # 强制联网搜索
                            "search_strategy": "max"  # 配置搜索策略为高性能模式
                        },
                },
            )
            audited_content = completion.choices[0].message.content
            parsed = _parse_json_response_text(audited_content)
            if parsed is None:
                raise RuntimeError(f"审计模型输出无法解析: {audited_content}")
            if isinstance(parsed, dict):
                found_list = None
                for v in parsed.values():
                    if isinstance(v, list):
                        found_list = v
                        break
                result_list = found_list if found_list is not None else [parsed]
            elif isinstance(parsed, list):
                result_list = parsed
            else:
                raise RuntimeError(f"审计模型输出不是dict或list: {audited_content}")
        except Exception as exc:
            _log(f"审计模型调用异常: {exc}")
            raise

        _merge_audited(result_list)
        timestamp = datetime.now().isoformat()
        for meta, _ in batch:
            meta.audited = True
            meta.audit_model = audit_model
            meta.audit_timestamp = timestamp
        save_index_merge(idx_path, idx)
        total_processed += len(batch)
        _log(f"批次审计完成：处理 {len(batch)} 条公告")

    if audited_items:
        _flush_audited()
    _log(f"审计完成，共处理 {total_processed} 条公告，输出文件: {audited_news_path}")


def main() -> int:
    """
    摘要: 命令行入口，用于更新公告并生成news.json
    Args:
        无（通过命令行参数提供）
    Returns:
        int: 成功新增的新闻条数（单股模式下）；批量模式返回0
    """
    parser = argparse.ArgumentParser(description="构建/更新/审计上市公司公告新闻")
    parser.add_argument("--symbol", type=str, help="标准股票代码，如 002371.SZ")
    parser.add_argument("--lookback", type=int, default=120, help="首次入池回溯天数，默认365")
    parser.add_argument("--all", action="store_true", help="批量更新 TRACKED_A_STOCKS")
    parser.add_argument("--model", type=str, default="qwen-doc-turbo", help="用于原子摘要的AI模型")
    parser.add_argument("--audit-model", default="deepseek-v3.2-exp", type=str, help="（可选）用于战略审计的AI模型，提供此参数将触发审计流程")
    args = parser.parse_args()

    _log("命令启动")
    if args.all:
        tracked_infos = [parse_symbol(entry.symbol) for entry in TRACKED_A_STOCKS]
        stats = update_all_tracked_stocks(tracked_infos, model=args.model, lookback_days=args.lookback)
        print("--- 摘要提取阶段完成 ---")
        print(json.dumps(stats, ensure_ascii=False, indent=2))
        
        if args.audit_model:
            _log(f"开始对所有跟踪的股票进行并发审计，使用模型: {args.audit_model}")
            with concurrent.futures.ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
                future_to_stock = {
                    executor.submit(
                        audit_news_json,
                        info,
                        args.audit_model
                    ): info for info in tracked_infos
                }
                
                for future in concurrent.futures.as_completed(future_to_stock):
                    stock_info = future_to_stock[future]
                    stock_symbol = stock_info.symbol
                    try:
                        future.result()  # 检查是否有异常
                        _log(f"股票 {stock_symbol} 的审计任务完成")
                    except Exception as exc:
                        _log(f"股票 {stock_symbol} 在并发审计中产生异常: {exc}")
            _log("--- 并发审计阶段完成 ---")
        return 0

    if not args.symbol:
        print("必须提供 --symbol，或使用 --all")
        return 0
    symbol_info = parse_symbol(args.symbol)
    
    data_access = SharedDataAccess(logger=LOGGER)
    cnt = update_disclosures_for_stock(
        symbol_info,
        lookback_days=args.lookback,
        model=args.model,
        data_access=data_access,
    )
    print(json.dumps({"symbol": symbol_info.symbol, "name": symbol_info.stock_name, "added": cnt}, ensure_ascii=False))
    
    # 如果指定了审计模型，在更新完摘要后执行审计
    if args.audit_model:
        audit_news_json(symbol_info, args.audit_model)
        
    return cnt
 

if __name__ == "__main__":
    main()
