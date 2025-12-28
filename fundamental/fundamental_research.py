#!/usr/bin/env python
"""
本文件暂时废弃，不要修改
"""
from __future__ import annotations

import argparse
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

import os
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from utlity import SymbolInfo, get_stock_data_dir, parse_symbol
from shared_data_access.data_access import SharedDataAccess
from news.disclosures_builder import (
    AnnouncementMeta,
    SUPPORTED_DIRECT_PDF_MODELS,
    _get_openai_client,
    _parse_json_response_text,
    convert_pdf_to_markdown,
    download_pdf,
    index_path,
    is_financial_report,
    load_index,
    md_dir,
    pdfs_dir,
    parse_announcement_row,
    save_index_merge,
    upload_file_to_dashscope,
)
from shared_data_access.models import PreparedData
from news.disclosures_builder import _hash_key  # reuse helper

# =====================================================================================
# Prompt 模板（已由用户提供）
# =====================================================================================

DOC_EXTRACTION_PROMPT = """
**角色与核心目标：**
你是一名顶级的财务分析师与语义侦探。你的任务不是简单地复制文本，而是**穿透格式化语言，从上市公司财报/公告中，系统性地提取关键事实、量化数据、未来承诺，并敏锐地识别管理层的叙述倾向与潜在风险暗示**。

**核心原则：**
1.  **事实与观点分离**：严格区分**已发生的客观事实**、**管理层的观点/预测**以及**对未来的目标承诺**。并为它们打上标签。
2.  **数据与实体保全**：所有具体数字（金额、比率、数量、日期）、具体名称（客户、供应商、项目、竞争对手）必须无一遗漏地提取。
3.  **可验证性优先**：所有关键信息必须注明精确来源（页码、章节标题或附近的小标题），格式为 `(来源：P.XX, “章节名”)`。
4.  **解释重于罗列**：不要孤立地列出数字，要将数字与其业务解释、市场原因绑定在一起。例如，不要只写“毛利率上升5%”，而要写“毛利率因原材料锂价下跌而上升5个百分点”。
5.  **智能筛选，而非全盘照抄**：忽略纯粹的法律模板文字（如“本公司保证…”）、财报三张表的详细条目（除非在分析部分被特别讨论）。专注于具有经济实质和信息含量的内容。

**输出结构要求：**
请严格按照以下五个部分组织你的输出，使用清晰的标题和列表。

### **第一部分：核心事实与业绩归因 (Key Facts & Attribution)**
*   **总体业绩快照**：用列表列出本期最关键的财务事实（营收、净利润、扣非净利润的数额及同比变化）。
*   **分业务/分地区量化分析**：以表格或列表形式，提取各细分板块的营收、毛利、增速。**明确指出增长贡献最大和拖累最大的板块，并附上管理层给出的具体原因**。
*   **盈利能力变动解析**：毛利率、净利率变动的原因（成本端：原材料、折旧、运费等；收入端：产品结构、定价权等）。

### **第二部分：未来导向的承诺与计划 (Forward-Looking Commitments & Plans)**
*   **量化目标**：提取所有关于未来营收、利润、产量、市场份额的**具体数字目标**（例如：“力争2025年实现营收100亿元”），并标记为 `[目标承诺]`。
*   **业务进展事实**：所有关于“在手订单”、“新签合同”、“中标项目”的**具体金额和数量**，标记为 `[事实-业务进展]`。
*   **资本开支(CAPEX)路线图**：所有“在建工程”、“产能扩建”、“研发中心”项目的**当前状态、预计投产时间、预计投资额及预期效益**。
*   **行业展望观点**：管理层对行业趋势（供需、价格、政策）的**判断性描述**（例如：“预计下半年需求将温和复苏”），标记为 `[观点-行业展望]`。

### **第三部分：竞争优势与资源投入 (Competitive Moat & Investments)**
*   **技术与产品进展**：新产品发布、核心技术突破、研发投入金额及占营收比重、重大专利获取。
*   **客户与市场突破**：新进入的客户或供应商名单（尽可能保留原名）、关键认证获得、市场份额的**具体数据**（例如：“在XX细分市场占有率提升至30%”）。

### **第四部分：风险、责任与公司治理事项 (Risks, Liabilities & Governance)**
*   **资产减值事实**：计提商誉、存货、固定资产减值的**具体金额和原因**。
*   **重大或有事项**：所有未决诉讼/仲裁的**涉案金额、基本案由和当前阶段**；所有重大对外担保的**被担保方、金额、期限**。
*   **关联交易**：非经营必需的、或金额重大的关联交易描述与金额。
*   **股东回报方案**：具体的分红预案（每10股派息）、股份回购计划（金额、期限、价格区间）。

### **第五部分：叙述分析与语义侦察 (Narrative Analysis & Semantic Detection)**
这是最关键的部分。你需要像侦探一样分析文本的“弦外之音”。
*   **风险暗示识别**：不要寻找直接说“我们很担忧”的句子。而是识别并摘录以下内容：
    *   反复强调的“挑战”或“困难”具体是什么？（例如：“原材料价格高位震荡”、“行业竞争日趋激烈”）
    *   使用“不确定性”、“复杂性”、“波动性”等词汇的上下文段落。
    *   风险因素章节中，**排名最靠前**或**描述篇幅最长**的风险是什么？
    *   是否有将业绩不佳归因于“宏观环境”、“不可抗力”或“行业周期性波动”的倾向？请摘录相关原文。
*   **信心与乐观信号**：识别并摘录：
    *   使用“显著提升”、“历史新高”、“重大突破”、“核心竞争力”等强肯定词汇的句子。
    *   对未来表达“充满信心”、“坚实基础”、“广阔空间”等表述的段落。
*   **【必须执行】**：对于本部分摘录的每一条原文，请在后面用括号分析其**潜台词或可能意图**。例如：
    > *   “公司面临的成本压力依然较大。” `(来源：P.15，“风险因素”)` → **潜台词：** 预示未来毛利率可能持续承压，为未来业绩设置低调预期。
    > *   “我们在新兴赛道已完成前瞻性布局。” `(来源：P.8，“董事长致辞”)` → **潜台词：** 强调长期成长故事，转移对当前业绩乏力的注意力。

---
**任务开始：**
请基于以上指令，对提供的文档进行解析和提取。确保输出**条理清晰、证据确凿、洞察深刻**。
"""


REPORT_ANALYSIS_AGENT_PROMPT = """
## **角色与核心目标**
你是一位**客观、严谨、注重平衡的财务验证专家**。你的核心任务是**系统性地验证上市公司管理层提供的“故事”与其实质财务、业务数据之间的内洽性**。

你的工作不是预先怀疑或盲目相信，而是：
1.  **识别叙事**：清晰地总结管理层对公司业绩和未来的核心论述。
2.  **搜集证据**：从提供的所有数据中，寻找支持或否定该论述的证据。
3.  **评估权重**：基于证据的强弱和一致性，给出平衡的评估。
4.  **指出断层**：当叙事与数据出现明显矛盾或缺失时，明确指出来。

**核心心法：你的结论必须由证据的权重决定，而非预设的立场。**

---

## **输入数据与处理协议**
（此部分与之前保持一致，是处理多文档的基础）
你将收到三份结构化输入：
1.  **【主分析文档】**：基于最新财报/公告提取的详细事实与叙述。
2.  **【参考背景文档】**：基于上一期详细财报（如年报）提取的事实与叙述。
3.  **【本期财务三张表】**：最新季度的利润表、资产负债表、现金流量表的核心数据。

**处理原则：**
*   **主次分明**：以**【主分析文档】**和**【三张表】**为当期业绩判断的**主要依据**。
*   **连贯性验证**：使用**【参考背景文档】**来理解公司的长期战略，并验证当期业绩是否与长期承诺的进展方向一致。
*   **数据锚定**：所有分析最终需回归到**【三张表】**的具体数字上进行健康度检验。

---

## **核心分析框架：叙事-证据双向验证**

请按照以下四个步骤进行思考和分析，确保每个结论都有对应证据支撑。

### **步骤一：核心叙事提取与业绩快照**
*   **任务**：首先，基于**【主分析文档】**，用一两句话概括管理层对本季度的**核心解释**（例如：“管理层将增长归因于A产品在B市场的爆发及成本下降”）。
*   **客观快照**：列出最关键的当期财务事实（营收、净利润、扣非净利润的同比/环比关键变化），不加评论，只呈现数据。

### **步骤二：增长叙事分解验证**
*   **验证逻辑**：针对第一步提取的“增长归因”，逐一进行证据匹配。
*   **执行动作**：
    1.  **业务拆分验证**：如果增长归因于某具体业务，该业务的营收、毛利数据是否确实表现出色？其增速是否显著高于公司平均？
    2.  **驱动因子验证**：
        *   若归因为“需求旺盛”，**【主分析文档】**中是否有“订单”、“产能利用率”、“客户突破”等证据支持？
        *   若归因为“成本下降”，**【三张表】**中的毛利率变化、或**【主分析文档】**中对原材料价格的描述是否吻合？
    3.  **可持续性初判**：参考**【参考背景文档】**，该增长点是否为长期战略的一部分？是否有产能或研发的持续投入作为支撑？

### **步骤三：未来指引与财务健康度交叉检验**
*   **验证逻辑**：评估公司对未来的承诺（指引）是否建立在坚实的财务基础和清晰的业务路径上。
*   **执行动作**：
    1.  **指引与历史对比**：将**【主分析文档】**中的最新指引（或**【参考背景文档】**中的年度目标）与**当期完成进度**进行客观对比。得出“超/符/低于内部指引”的结论。
    2.  **财务基础检验（关键）**：
        *   **现金流验证利润**：经营现金流净额是否与净利润规模匹配？如果差异大，**【主分析文档】**是否给出了合理解释（如季节性备货、项目结算周期）？
        *   **资源投入匹配**：公司的资本开支（购建固定资产现金流出）是否与它所宣称的产能扩张计划相匹配？
        *   **风险信号筛查**：客观检查**【三张表】**中是否有异常波动项（如应收账款激增、存货陡升、有息负债大幅增长），并查看**【主分析文档】**中管理层是否对此进行了说明。

### **步骤四：优势与不确定性平衡总结**
*   **任务**：基于前三步的验证，进行平衡总结。
*   **执行动作**：
    1.  **证据确凿的优势**：列出哪些管理层的积极论述得到了**强数据证据**的支持（例如：“新产品增长叙事成立，因该业务线营收同比+150%且毛利提升”）。
    2.  **需要关注的节点**：列出哪些未来承诺或计划，其进展**尚未在当期数据中得到充分体现**，需要后续财报验证（例如：“公司规划的X新工厂预计Q4投产，但当期资本开支进度较缓，需关注下季报进展”）。
    3.  **存在的矛盾或风险**：明确指出哪些地方存在**叙事与数据的明显矛盾**，或**关键数据表现不佳但管理层未充分解释**（例如：“公司宣称市场份额提升，但当期营收增速却低于行业平均，两者存在矛盾”）。

---

## **输出格式规范**

请输出一个 **JSON 对象**，严格包含以下字段：

```json
{
  "meta": {
    "report_date": "YYYY-MM-DD",
    "core_narrative": "从主文档中提炼的管理层对本季业绩的核心解释（1-2句话）"
  },
  "performance_assessment": {
    "vs_prior_guidance": "超出指引 (Beat) | 符合指引 (Inline) | 未达指引 (Miss)",
    "explanation": "客观陈述对比的依据和结果。"
  },
  "balanced_verdict": {
    "supported_strengths": [
      "列举1：具体优势 + 支撑证据（如：'A业务毛利率大幅提升5个百分点，与所述'产品结构优化'叙事相符。'）",
      "列举2：..."
    ],
    "key_monitor_points": [
      "列举1：需要后续验证的承诺或节点（如：'B新产线投产时间已近，下期财报需观察其转固及产能爬坡情况。'）",
      "列举2：..."
    ],
    "identified_concerns_or_contradictions": [
      "列举1：明确的矛盾或未解释的风险（如：'经营现金流净额仅为净利润的30%，但管理层未在讨论中具体解释大幅差异的原因。'）",
      "列举2：..."
    ]
  },
  "objective_analysis_report": "## 本期业绩核心验证总结\n\n（一段话，概括性地回答：管理层的主要叙事是否得到验证？业绩成色如何？）\n\n## 详细验证过程\n\n### 1. 增长归因验证\n（对应步骤二，逐条验证增长驱动因素）\n\n### 2. 财务健康度与现金流分析\n（对应步骤三的财务基础检验，客观描述发现）\n\n### 3. 未来能见度评估\n（结合订单、产能、指引，评估未来增长的确定性依据）\n\n### 4. 综合评估：优势与待观察项\n（将`balanced_verdict`中的三项内容展开论述，形成完整画面）"
}
```

---

## **任务开始**
请基于以上所有指令，对提供的输入数据进行**客观、平衡的验证与分析**。
**记住你的原则：呈现证据，验证逻辑，指出矛盾，平衡总结。**

开始分析。
"""

# =====================================================================================
# 其它全局变量
# =====================================================================================
PRIMARY_REPORT_KEYWORDS = [
    "年度报告",
    "年报",
    "半年度报告",
    "半年度报告书",
    "半年报",
    "半年度报告全文",
    "一季度报告",
    "第一季度报告",
    "三季度报告",
    "第三季度报告",
    "三季报",
]
EXCLUDED_REPORT_KEYWORDS = [
    "摘要",
    "说明会",
    "英文",
    "财务报告",
    "财务报表",
    "问答",
    "决议",
    "资料",
    "公告稿",
]

# =====================================================================================
# 日志
# =====================================================================================

LOGGER = logging.getLogger("fundamental.research")
if not LOGGER.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("[fundamental][%(asctime)s] %(message)s"))
    LOGGER.addHandler(handler)
LOGGER.setLevel(logging.INFO)
LOGGER.propagate = False


def _log(message: str) -> None:
    LOGGER.info(message)

# =====================================================================================
# 路径 & 工具函数
# =====================================================================================

def _financial_root(symbol_info: SymbolInfo) -> Path:
    root = get_stock_data_dir(symbol_info)
    out = root / "financial_reports"
    out.mkdir(parents=True, exist_ok=True)
    return out


def _summaries_dir(symbol_info: SymbolInfo) -> Path:
    out = _financial_root(symbol_info) / "summaries"
    out.mkdir(parents=True, exist_ok=True)
    return out


def _reports_json_path(symbol_info: SymbolInfo) -> Path:
    return _financial_root(symbol_info) / "financial_reports.json"


def _analysis_json_path(symbol_info: SymbolInfo) -> Path:
    return _financial_root(symbol_info) / "fundamental_analysis.json"


def prepare_input_text(
    current_report: Dict[str, Any],
    previous_report: Optional[Dict[str, Any]],
    current_statement: Optional[Dict[str, Any]],
    previous_statement: Optional[Dict[str, Any]],
) -> str:
    reference_block = ""
    if previous_report:
        reference_block = f"""
================================================

【背景参考资料 (Reference Material)】
报告名称：{previous_report['title']} (日期: {previous_report['date']})
内容：
{previous_report['content']}
"""

    combined_text = f"""
【主分析对象 (Primary Source)】
报告名称：{current_report['title']} (日期: {current_report['date']})
内容：
{current_report['content']}

{reference_block}

"""
    statement_block = ""
    if current_statement:
        statement_block += f"""
================================================

【主财务三表 (Primary Statements)】
报告日期：{current_statement['date']}  类型：{current_statement.get('report_type','')}
{current_statement['content']}
"""
    if previous_statement:
        statement_block += f"""
------------------------------------------------

【辅助财务三表 (Reference Statements)】
报告日期：{previous_statement['date']}  类型：{previous_statement.get('report_type','')}
{previous_statement['content']}
"""
    return (combined_text + statement_block).strip()


def build_financial_statement_entries(
    symbol_info: SymbolInfo,
    prepared: Optional[PreparedData],
) -> List[Dict[str, Any]]:
    accessor = SharedDataAccess(logger=LOGGER) if prepared is None else None
    dataset = prepared
    if dataset is None:
        dataset = accessor.prepare_dataset(
            symbolInfo=symbol_info,
            as_of_date=datetime.now().strftime("%Y-%m-%d"),
            include_disclosures=False,
        )
    bundle = dataset.financials if dataset else None
    if bundle is None:
        return []

    frames = {
        "profit": getattr(bundle, "profit_sheet", pd.DataFrame()),
        "balance": getattr(bundle, "balance_sheet", pd.DataFrame()),
        "cash": getattr(bundle, "cash_flow_sheet", pd.DataFrame()),
    }
    date_map: Dict[str, Dict[str, str]] = {}
    for key, df in frames.items():
        rows = _extract_statement_rows(df)
        for date_str, table_text in rows:
            bucket = date_map.setdefault(date_str, {})
            bucket[key] = table_text

    entries: List[Dict[str, Any]] = []
    for date_str, sections in date_map.items():
        blocks = []
        if "profit" in sections:
            blocks.append(f"#### 利润表\n{sections['profit']}")
        if "balance" in sections:
            blocks.append(f"#### 资产负债表\n{sections['balance']}")
        if "cash" in sections:
            blocks.append(f"#### 现金流量表\n{sections['cash']}")
        if not blocks:
            continue
        report_type = _infer_report_type_by_date(date_str)
        entries.append(
            {
                "title": f"{symbol_info.stock_name} 财务报表 {date_str}",
                "date": date_str,
                "report_type": report_type,
                "content": "\n\n".join(blocks),
            }
        )

    entries.sort(key=lambda x: x["date"], reverse=True)
    return entries[:1]


# =====================================================================================
# 财报提取
# =====================================================================================

class FinancialReportExtractor:
    def __init__(
        self,
        symbol_info: SymbolInfo,
        model: str,
        *,
        lookback_days: int = 540,
        direct_pdf: bool = False,
    ) -> None:
        self.symbol_info = symbol_info
        self.model = model
        self.lookback_days = lookback_days
        if direct_pdf and model not in SUPPORTED_DIRECT_PDF_MODELS:
            raise ValueError(
                f"模型 {model} 不支持直接上传 PDF，请关闭 --direct-pdf 或更换为 {SUPPORTED_DIRECT_PDF_MODELS}"
            )
        self.direct_pdf = direct_pdf and model in SUPPORTED_DIRECT_PDF_MODELS
        self.client = _get_openai_client("extraction", model)
        self.data_access = SharedDataAccess(logger=LOGGER)
        self.latest_dataset: Optional[PreparedData] = None

    def run(self) -> List[Dict[str, Any]]:
        idx = self._sync_disclosures()
        reports = self._ensure_summaries(idx)
        self._save_reports_payload(reports)
        return reports

    # ------------------------------------------------------------------
    def _sync_disclosures(self) -> Dict[str, AnnouncementMeta]:
        _log(f"同步公告索引: {self.symbol_info.stock_name} lookback={self.lookback_days}")
        idx_file = index_path(self.symbol_info)
        idx = load_index(idx_file)

        prepared = self.data_access.prepare_dataset(
            symbolInfo=self.symbol_info,
            as_of_date=datetime.now().strftime("%Y-%m-%d"),
            include_disclosures=True,
            disclosure_lookback_days=self.lookback_days,
        )
        self.latest_dataset = prepared
        disclosure_bundle = prepared.disclosures
        if disclosure_bundle is None or disclosure_bundle.frame.empty:
            _log("公告数据为空")
            return idx

        df: pd.DataFrame = disclosure_bundle.frame
        for _, row in df.iterrows():
            row_dict = row.to_dict()
            stock_code, title, dt, url, ann_id = parse_announcement_row(row_dict)
            if not is_financial_report(title):
                continue

            key = ann_id or _hash_key(title, dt)
            meta = idx.get(key)
            if not meta:
                meta = AnnouncementMeta(
                    announcement_id=str(ann_id or key),
                    org_id=str(row_dict.get("orgId") or ""),
                    stock_code=stock_code,
                    title=title,
                    date=dt,
                    url=url,
                    category="Financial",
                    is_financial_report=True,
                    dedupe_key=key,
                )
                idx[key] = meta
            else:
                meta.is_financial_report = True
                meta.url = url

            if not meta.downloaded:
                pdf_name = f"{dt}__{stock_code}__{meta.announcement_id}__{_safe_slug(title)}.pdf"
                pdf_path = pdfs_dir(self.symbol_info) / pdf_name
                ok = download_pdf(url, pdf_path)
                if ok:
                    meta.pdf_path = str(pdf_path)
                    meta.downloaded = True
                else:
                    _log(f"PDF 下载失败: {title}")

        save_index_merge(idx_file, idx)
        return idx

    # ------------------------------------------------------------------
    def _ensure_summaries(self, idx: Dict[str, AnnouncementMeta]) -> List[Dict[str, Any]]:
        metas = [
            meta for meta in idx.values()
            if meta.is_financial_report and meta.downloaded and _is_primary_report_title(meta.title)
        ]
        metas.sort(key=lambda m: m.date)

        LOGGER.info(f"提取财报: {[meta.title+meta.date for meta in metas]}")
        
        reports: List[Dict[str, Any]] = []
        for meta in metas:
            summary_path = Path(meta.summary_json_path) if meta.summary_json_path else None
            if meta.summarized and summary_path and summary_path.exists():
                content = summary_path.read_text(encoding="utf-8")
            else:
                content = self._summarize_single(meta)
                if not content:
                    continue
                summary_path = self._write_summary(meta, content)
                meta.summary_json_path = str(summary_path)
                meta.summarized = True

            reports.append(
                {
                    "announcement_id": meta.announcement_id,
                    "title": meta.title,
                    "date": meta.date,
                    "report_type": _infer_report_type(meta.title),
                    "content": content,
                }
            )
        save_index_merge(index_path(self.symbol_info), idx)
        return sorted(reports, key=lambda x: x["date"], reverse=True)

    def _summarize_single(self, meta: AnnouncementMeta) -> Optional[str]:
        if not meta.pdf_path:
            _log(f"缺少 PDF，无法提取: {meta.title}")
            return None
        pdf_path = Path(meta.pdf_path)
        if not pdf_path.exists():
            _log(f"PDF 文件不存在: {pdf_path}")
            return None

        markdown_messages_cache: Optional[List[Dict[str, str]]] = None

        def _build_markdown_messages() -> Optional[List[Dict[str, str]]]:
            nonlocal markdown_messages_cache
            if markdown_messages_cache is not None:
                return markdown_messages_cache
            md_name = f"{meta.date}__{meta.stock_code}__{meta.announcement_id}__{_safe_slug(meta.title)}.md"
            markdown_path = md_dir(self.symbol_info) / md_name
            markdown_content = convert_pdf_to_markdown(pdf_path, markdown_path)
            if not markdown_content:
                _log(f"Markdown 转换失败: {meta.title}")
                return None
            truncated = markdown_content[:150000]
            markdown_messages_cache = [
                {"role": "system", "content": "你是资深财报分析助手"},
                {"role": "user", "content": DOC_EXTRACTION_PROMPT},
                {"role": "user", "content": f"以下为财报内容（已截断）：\n{truncated}"},
            ]
            return markdown_messages_cache

        use_markdown = not self.direct_pdf
        while True:
            if not use_markdown:
                if meta.file_id is None:
                    fid = upload_file_to_dashscope(pdf_path)
                    meta.file_id = fid
                if not meta.file_id:
                    _log("缺少 file_id，无法走直传流程")
                    use_markdown = True
                    continue
                messages = [
                    {"role": "system", "content": f"fileid://{meta.file_id}"},
                    {"role": "user", "content": DOC_EXTRACTION_PROMPT},
                ]
                mode = "direct"
            else:
                messages = _build_markdown_messages()
                if not messages:
                    return None
                mode = "markdown"

            try:
                completion = self.client.chat.completions.create(
                    model=self.model,
                    messages=messages,
                )
                content = completion.choices[0].message.content
                return content
            except Exception as exc:
                _log(f"模型提取失败({mode}): {exc}")
                if mode == "direct":
                    _log("直传模式失败，尝试使用 Markdown 截断模式重新提取")
                    use_markdown = True
                    continue
                return None

    def _write_summary(self, meta: AnnouncementMeta, content: str) -> Path:
        out_dir = _summaries_dir(self.symbol_info)
        file_path = out_dir / f"{meta.date}_{meta.title}.md"
        file_path.write_text(content, encoding="utf-8")
        _log(f"财报摘要已保存: {file_path}")
        return file_path

    def _save_reports_payload(self, reports: List[Dict[str, Any]]) -> None:
        payload = {
            "stock": f"{self.symbol_info.stock_name} ({self.symbol_info.symbol})",
            "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "reports": reports,
        }
        path = _reports_json_path(self.symbol_info)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        _log(f"已写入财报提取列表: {path}, 条数={len(reports)}")


# =====================================================================================
# 财报分析
# =====================================================================================

class FundamentalResearchAgent:
    def __init__(self, symbol_info: SymbolInfo, model: str) -> None:
        self.symbol_info = symbol_info
        self.model = model
        self.client = _get_openai_client("audit", model)

    def run(
        self,
        reports: List[Dict[str, Any]],
        statements: List[Dict[str, Any]],
    ) -> Optional[Dict[str, Any]]:
        primary, reference = _select_reports_for_analysis(reports)
        if not primary:
            _log("没有可用于分析的财报")
            return None
        stmt_primary, stmt_reference = _select_reports_for_analysis(statements)

        combined = prepare_input_text(primary, reference, stmt_primary, stmt_reference)
        messages = [
            {"role": "user", "content": REPORT_ANALYSIS_AGENT_PROMPT},
            {"role": "user", "content": combined},
        ]
        extra_body = {
            "enable_search": True,
            "search_options": {
                "forced_search": True,
                "search_strategy": "max",
            },
        }
        try:
            completion = self.client.chat.completions.create(
                model=self.model,
                messages=messages,
                extra_body=extra_body,
            )
        except Exception as exc:
            _log(f"分析模型调用失败: {exc}")
            return None

        content = completion.choices[0].message.content
        parsed = _parse_json_response_text(content)
        if not isinstance(parsed, dict):
            _log("分析结果无法解析为 JSON")
            return None

        parsed["symbol"] = self.symbol_info.symbol
        parsed["stock_name"] = self.symbol_info.stock_name
        parsed["generated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        parsed["primary_report"] = {
            "title": primary["title"],
            "date": primary["date"],
            "type": primary["report_type"],
        }
        if reference:
            parsed["reference_report"] = {
                "title": reference["title"],
                "date": reference["date"],
                "type": reference["report_type"],
            }
        if stmt_primary:
            parsed["primary_statements"] = {
                "date": stmt_primary["date"],
                "type": stmt_primary.get("report_type"),
            }
        if stmt_reference:
            parsed["reference_statements"] = {
                "date": stmt_reference["date"],
                "type": stmt_reference.get("report_type"),
            }

        self._append_analysis(parsed)
        _log("财报深度分析已生成")
        return parsed

    def _append_analysis(self, analysis: Dict[str, Any]) -> None:
        path = _analysis_json_path(self.symbol_info)
        existing: Dict[str, Any] = {}
        if path.exists():
            try:
                existing = json.loads(path.read_text(encoding="utf-8"))
            except Exception:
                existing = {}
        items = list(existing.get("reports", []))

        def _key(entry: Dict[str, Any]) -> str:
            return f"{entry.get('symbol')}|{entry.get('datetime')}|{entry.get('generated_at')}"

        seen = {_key(it) for it in items}
        k = _key(analysis)
        if k not in seen:
            items.append(analysis)
        items.sort(key=lambda it: it.get("datetime", ""), reverse=True)
        payload = {
            "stock": f"{self.symbol_info.stock_name} ({self.symbol_info.symbol})",
            "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "reports": items,
        }
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        _log(f"分析结果已写入: {path}, 累计{len(items)}条")


# =====================================================================================
# 辅助逻辑
# =====================================================================================
def _safe_slug(text: str) -> str:
    return "".join(ch if ch.isalnum() else "_" for ch in text).strip("_")[:80]


def _is_primary_report_title(title: str) -> bool:
    title = title or ""
    if not any(k in title for k in PRIMARY_REPORT_KEYWORDS):
        return False
    if any(k in title for k in EXCLUDED_REPORT_KEYWORDS):
        return False
    return True


def _infer_report_type(title: str) -> str:
    title = title or ""
    if any(k in title for k in ["半年度", "中期"]):
        return "interim"
    if any(k in title for k in ["年度报告", "年报"]):
        return "annual"
    if any(k in title for k in ["第一季度", "一季度"]):
        return "q1"
    if any(k in title for k in ["第三季度", "三季度", "三季报"]):
        return "q3"
    return "other"


def _select_reports_for_analysis(
    reports: List[Dict[str, Any]]
) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
    """
    从财报报告列表中选择主要分析报告和参考报告
    
    该函数的逻辑是：
    1. 选择最新的报告作为主要分析报告
    2. 根据主要报告的类型，选择一个合适的参考报告
    
    Args:
        reports: 财报报告列表，每个报告包含date、report_type等字段
        
    Returns:
        Tuple[主要报告, 参考报告]: 返回一个元组，包含主要分析报告和参考报告
        如果没有报告则返回(None, None)
    """
    # 如果报告列表为空，直接返回两个None
    if not reports:
        return None, None
    
    # 按日期倒序排列报告，最新的在前面
    reports_sorted = sorted(reports, key=lambda x: x["date"], reverse=True)
    
    # 选择最新的报告作为主要分析报告
    primary = reports_sorted[0]
    
    # 获取主要报告的类型，默认为"other"
    primary_type = primary.get("report_type", "other")
    
    # 初始化参考报告为None
    reference = None
    
    # 定义不同类型的主要报告应该选择什么类型的参考报告
    # 规则是：
    # - 如果主要报告是Q3财报，选择中期报告作为参考
    # - 如果主要报告是中期报告，不选择参考报告
    # - 如果主要报告是Q1财报，选择年度报告作为参考
    # - 如果主要报告是年度报告，选择Q3财报作为参考
    # - 如果主要报告是其他类型，按优先级选择年度、中期、Q3、Q1报告作为参考
    desired_map = {
        "q3": ("interim",),           # Q3财报 -> 中期报告
        "interim": tuple(),           # 中期报告 -> 无参考报告
        "q1": ("annual",),            # Q1财报 -> 年度报告
        "annual": ("q3",),            # 年度报告 -> Q3财报
        "other": ("annual", "interim", "q3", "q1"),  # 其他类型 -> 按优先级选择
    }
    
    # 获取当前主要报告类型对应的期望参考报告类型列表
    desired = desired_map.get(primary_type, ("annual", "interim"))
    
    # 如果有期望的参考报告类型，则在剩余报告中寻找第一个符合类型的报告作为参考
    if desired:
        # 遍历除主要报告外的其他报告
        for cand in reports_sorted[1:]:
            # 如果候选报告的类型在期望的参考报告类型列表中，则选为参考报告
            if cand.get("report_type") in desired:
                reference = cand
                break
    
    # 返回主要报告和参考报告
    return primary, reference


def _extract_statement_rows(df: pd.DataFrame) -> List[Tuple[str, str]]:
    if df is None or df.empty:
        return []
    work = df.copy()
    date_col = _find_date_column(work)
    if date_col is None:
        work = work.reset_index()
        date_col = _find_date_column(work)
        if date_col is None:
            return []
    work[date_col] = pd.to_datetime(work[date_col], errors="coerce")
    work = work.dropna(subset=[date_col])
    work = work.sort_values(date_col, ascending=False)
    rows: List[Tuple[str, str]] = []
    for _, row in work.iterrows():
        dt = row[date_col]
        if isinstance(dt, pd.Timestamp):
            date_str = dt.strftime("%Y-%m-%d")
        else:
            date_str = str(dt)[:10]
        table_text = _row_to_table(row)
        rows.append((date_str, table_text))
    return rows


def _find_date_column(df: pd.DataFrame) -> Optional[str]:
    candidates = [
        "REPORT_DATE",
        "报告期",
        "report_date",
        "REPORTDATE",
        "报表日期",
        "END_DATE",
        "ENDDATE",
    ]
    for col in candidates:
        if col in df.columns:
            return col
    return None


def _row_to_table(row: pd.Series) -> str:
    skip_cols = {
        "REPORT_DATE",
        "report_date",
        "REPORTDATE",
        "REPORT_TYPE",
        "DATE_TYPE_CODE",
        "S_INFO_COMPCODE",
        "SECURITY_CODE",
    }
    lines = ["| 指标 | 数值 |", "|---|---|"]
    for col, value in row.items():
        if col in skip_cols:
            continue
        if pd.isna(value):
            continue
        if isinstance(value, float):
            value = round(value, 4)
        lines.append(f"| {col} | {value} |")
    return "\n".join(lines)


def _infer_report_type_by_date(date_str: str) -> str:
    try:
        dt = pd.to_datetime(date_str)
    except Exception:
        return "other"
    month = dt.month
    day = dt.day
    if (month, day) in [(12, 31)]:
        return "annual"
    if (month, day) in [(6, 30)]:
        return "interim"
    if (month, day) in [(3, 31)]:
        return "q1"
    if (month, day) in [(9, 30)]:
        return "q3"
    return "other"


# =====================================================================================
# CLI
# =====================================================================================

def main() -> None:
    parser = argparse.ArgumentParser(description="财报提取与分析")
    parser.add_argument("--symbol", required=True, default="002028.SZ", help="股票代码，如 600000.SH")
    parser.add_argument("--extract-model", default="qwen-doc-turbo", help="提取模型名称")
    parser.add_argument("--analysis-model", default="deepseek-v3.2-exp", help="分析模型名称")
    parser.add_argument("--lookback-days", type=int, default=540, help="公告回溯天数")
    parser.add_argument(
        "--direct-pdf",
        action="store_true",
        default=True,
        help="使用支持直传 PDF 的模型进行解析（仅限特定模型）",
    )
    args = parser.parse_args()

    symbol_info = parse_symbol(args.symbol)
    extractor = FinancialReportExtractor(
        symbol_info,
        args.extract_model,
        lookback_days=args.lookback_days,
        direct_pdf=args.direct_pdf,
    )
    reports = extractor.run()
    if not reports:
        _log("未找到可用财报，流程结束")
        return

    statements = build_financial_statement_entries(symbol_info, extractor.latest_dataset)
    agent = FundamentalResearchAgent(symbol_info, args.analysis_model)
    agent.run(reports, statements)


if __name__ == "__main__":
    main()
