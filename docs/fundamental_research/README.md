# 财报研究系统设计说明

## 目标与当前状态
- 对接 `SharedDataAccess.prepare_dataset` 提供的公告索引，在新闻系统基础上分离出 **财报专用** 的抓取、解析与分析流程。
- 借助现有的 PDF 下载/缓存机制，把所有财报公告完整落地，输出结构化摘要并交给专用的财报分析 Agent。
- 复用 `SymbolInfo` 传递股票信息，确保所有外部数据均由 `prepare_dataset` 缓存。

> **现状说明（2025-12 更新）：**  
> 原定的自动化 `fundamental_research.py` 流水线在多轮验证后效果不佳，已暂时停用。现阶段的财报研究结果改为“人工 deepResearch+AI 总结”的 Markdown 文件，并存放在 `data/stock_info/{stock_name}_{stock_code}/financial_reports/` 目录。  
> 
> - 文件命名以 `YYYYMMDD` 为前缀，必要时追加 `_25一季报`、`_24年报` 等后缀以区分同日发布的不同财报。  
> - 在同一天存在多份财报时，遵循“用新不用旧”原则，优先使用代表最新会计期间的文件（如 `20250430_25一季报.md` 相比 `20250430_24年报.md`）。  
> - Agent 工具需在当前 `today_time` 之前选取最近的一份 Markdown，并额外返回距离财报发布日期的天数、下一份财报的预计发布日期（若已存在更晚日期的 Markdown，则以其日期推算 gap）。  
> - 自动化管线未来继续迭代时，应保持该人工文件格式兼容，或提供迁移脚本同步到同一目录结构。

## 总体流程
1. **数据准备**  
   - 调用 `prepare_dataset(symbolInfo, include_disclosures=True)` 让 `update_disclosures_cached` 拉取公告列表。  
   - 在 index 中筛选 `is_financial_report(meta)` 的记录，构建 `AnnouncementMeta` 队列。
2. **文档落地**  
   - 沿用 `news/disclosures_builder.py` 中的 PDF 下载 + Markdown 缓存逻辑：  
     - PDF 优先落地 `disclosures/pdfs/`，必要时上传到模型云盘并记录 `file_id`。  
     - 对不支持直读 PDF 的模型，借助 `marker` 将 PDF 转成 Markdown。
   - 仅允许 `["qwen-doc-turbo", "qwen-long"]` 走直传 PDF；其他模型请求此模式直接 `raise`。
3. **信息提取**  
   - 对所有财报逐份调用 DOC_EXTRACTION_PROMPT，生成结构化 JSON（如收入、利润、风险、展望等字段）。  
   - Meta 中写入 `summarized=True`，并把摘要附加到 `financial_reports.json`。
4. **多期对比**  
   - 选择最新一份财报作为主输入；若其类型为“年报”则找上一期中报，反之取上一期同级别报告。  
   - 聚合为 `prepare_input_text()` 的升级版：包含两期关键指标与提取摘要。
5. **分析 Agent**  
   - 使用 REPORT_ANALYSIS_AGENT_PROMPT；调用 `client.chat.completions.create(..., extra_body={"enable_search": True, "search_options":{"forced_search": True, "search_strategy":"max"}})` 强制联网。  
   - 输出财报点评、潜在风险、估值影响等结论，写入 `fundamental_reports/`。

## 模块划分
| 模块 | 说明 |
|------|------|
| `fundamental/fundamental_research.py` | 主入口：解析 CLI、调用 `FinancialReportExtractor` 与 `FundamentalResearchAgent`。 |
| `FinancialReportExtractor` | 负责调 `prepare_dataset`、筛选财报、下载/缓存 PDF、执行 DOC_EXTRACTION_PROMPT。 |
| `FundamentalResearchAgent` | 读取最新两份财报摘要，拼装 prompt，调用具备 search 的模型生成分析。 |
| `docs/fundamental_research/README.md` | 当前文档，记录架构与使用方式。 |

## 复用策略
- `AnnouncementMeta`、索引文件结构、批量写入 helper 均直接借用 `news/disclosures_builder.py`，减少重复逻辑。
- `SharedDataAccess` 不做修改；若后续需要额外财务字段，统一在该层扩展。
- 提供 `SUPPORTED_DIRECT_PDF_MODELS` 常量，未来新增模型只需更新列表。

## 下一步
- 在 `fundamental_research.py` 实现提取/分析类和 CLI。  
- 为 `financial_reports.json`、`fundamental_reports.json` 定义输出 schema，并在测试后补充使用说明。  
