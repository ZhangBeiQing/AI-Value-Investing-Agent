"""Run-specific prompt fragments for financial report workdir generation.

Stable methodology lives under configs/research/. These prompts intentionally
avoid duplicating the complete research policy.
"""

REPORT_ANALYSIS_PROMPT_TEMPLATE = """# {company_name}{report_period}财报事实分析任务

本文件只定义本期财报的事实核对重点。完整方法以
`configs/research/financial_fundamental_research_policy.md` 为准。

## 目标

- 从本期与上期财报原文建立最近两年季度趋势；
- 同时分析同比、环比和季节性；
- 从公司整体下钻到重大业务、产品、区域与子公司；
- 定量区分利润变化的主要原因、次要原因和一次性项目；
- 检查利润、现金流、应收、存货、合同负债、资本开支与产能是否相互验证；
- 识别披露口径变化、异常聚合、重要缺失和管理层解释中的反事实问题。

## 边界

- 不得用财报后信息重建财报前预期；
- 不得把年度一致预期冒充季度一致预期；
- 不得虚构未披露的分部数据；
- 累计值推导单季度时必须写公式、单位和“推导值”；
- 重大数字必须回到财报原文核对。
"""


FUTURE_OUTLOOK_PROMPT_TEMPLATE = """# {company_name}未来六至十二个月经营推演任务

`{industry_name}` 只是准备阶段的候选行业标签，不代表已验证的细分产业链，
也不代表公司是龙头。Industry Researcher 必须按
`configs/research/industry_chain_research_policy.md` 重新识别
`terminal_theme → subchain → value_chain_node → company_exposure`。

## 目标

- 按公司类型选择未来经营驱动因素；
- 研究订单、销量、价格、库存、产能、利用率、客户验证、技术路线和资本开支；
- 区分公司正式指引、机构一致预期和 Agent 推演；
- 计算达到全年一致预期所需的剩余期间业绩；
- 构建可观察的向下、基准和向上情景；
- 说明景气下行、供给释放、竞争、技术替代和公司执行风险；
- 给出下一季度验证指标和证伪条件。

不限制指标、催化剂、风险或问题数量。没有证据时明确写未知。
"""


__all__ = [
    "FUTURE_OUTLOOK_PROMPT_TEMPLATE",
    "REPORT_ANALYSIS_PROMPT_TEMPLATE",
]
