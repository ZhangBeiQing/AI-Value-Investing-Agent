## 1. 架构与模块
本系统采用“ETL预处理 → 索引与缓存 → **原子摘要 → 战略审计** → 动态组装（回测上下文）”架构，并与项目 `shared_data_access/cache_registry.py` 接口对齐。

```mermaid
graph TD
    A[Akshare API] --> B(清洗过滤器)
    B --> C{文档处理策略}
    C -->|模型支持PDF| D[上传PDF]
    C -->|模型不支持PDF| E[PDF转MD(带缓存)]
    D & E --> F(原子摘要器 LLM)
    F --> G[索引管理 index.json]
    G --> H[原子摘要库 news.json]
    H --> I{战略审计流程}
    I -->|滚动审计(3个月/批)| J(审计模型 LLM)
    J --> K[替换旧数据]
    K --> L(精炼摘要库 news.json)
    M[仿真日期 T] --> L
    L --> N[上下文组装器]
```

## 2. 目录与缓存（兼容项目）
- 每只股票目录：`data/stock_info/<股票名_代码>/`
  - `disclosures/pdfs/`：原始 PDF（只下载一次）
  - `disclosures/md/`：**（新增）** 可选 Markdown 转换产物，带缓存。
  - `disclosures/index.json`：公告元数据索引。
  - `news/news.json`：**（修订）** 经审计的公告摘要总表。

## 3. 数据获取与清洗
- 输入：股票代码、开始日期、结束日期。
- 过滤规则：
  - 排除低价值行政类：如例行股东大会通知、一般法律意见、重复性说明；保留影响股价的实质性公告。
  - 自动识别财报类公告：年报、半年报、一季报、三季报、业绩预告等。
- 输出：有效公告列表（含 URL、日期、标题）。

## 4. 索引与去重（index.json）
- 唯一键：优先 `announcementId`（从 URL 提取），后备 `title+date` 哈希。
- 字段：`announcementId`, `orgId`, `stockCode`, `title`, `date`, `url`, `pdf_path`, **`md_path`（新增）**, `fileId`, `downloaded`, `summarized`, `audited` **(新增)**, `last_processed_ts`, `category`, `is_financial_report`, `dedupe_key`。
- 文件命名：`YYYY-MM-DD__<stockCode>__<announcementId>__<slug-title>.pdf`。

## 5. 原子化摘要器

- **（修订）处理流程**：
  1.  检查当前模型是否在 `SUPPORTED_DIRECT_PDF_MODELS` 列表中。
  2.  **若支持**：直接上传 PDF（或使用 `fileId`），调用模型生成摘要。
  3.  **若不支持**：
      a.  检查 `disclosures/md/` 目录下是否存在对应的 Markdown 缓存文件。
      b.  若存在，直接读取；若不存在，调用转换工具将 PDF 转为 Markdown 并保存至缓存目录。
      c.  更新 `index.json` 中的 `md_path` 字段。
      d.  将 Markdown 文本提交给模型生成摘要。
- 普通公告：
  - 输出结构：`date`、`title`、`category`、`summary`（≤100字）、`impact`、`sentiment`、`influence_window`。

- 财报公告(暂不实现，遇到财报公告直接返回)：
  - 专用 Prompt：抽取经营情况、风险、重大事项、展望等定性信息。
  - 大文档处理：按章节抽取或分段摘要并合并。

## 6. 新闻库与回测接口
- `news/news.json` 构成：按时间排序的、**经审计的**公告摘要列表；每日增量合并。
- 回测接口：`get_news_context(stock_code, simulation_date, lookback_days=60)`
  - 仅返回 `simulation_date` 前的json数据；默认窗口 60 天；确保包含最近季度财报相关公告。
  - 在组装文本时插入时效标签（如“今日”“X天前”）。

## 7. （新增）战略审计与精炼流程
- **触发时机**：可独立脚本触发，或在批量更新后自动运行。
- **输入**：`news/news.json` 文件路径。
- **核心逻辑**：
  1.  加载 `news.json` 全量内容与（若存在）`news_audited.json`。
  2.  计算两者最新日期：若 `news.json` 最新日期不大于 `news_audited.json` 最新日期，直接退出；否则仅选取新增区间。
  3.  **分批策略（新增）**：若新增区间长度超过60天，则按60天窗口分批处理；每批次完成后立即把模型输出追加到 `news_audited.json`，保证断点续审与可靠落盘。
  4.  **模型调用**：每个批次将 `news_items` 列表作为 JSON 输入，连同 `AUDIT_AND_REFINE_PROMPT` 发送给审核模型；解析输出为列表后进行去重合并（键：`title+datetime`）。
  5.  **写盘规范**：
      - 结果文件：`news_audited.json`
      - 字段：`stock`, `today`, `news_items`, `diagnostics`
      - 写入方式：每批次覆盖写入但保持历史 items 追加扩展，确保文件总是处于可用状态。

## 8. 更新调度（TTL=1天）
- 夜间任务：
  - 拉取当日公告 → 比对 `index.json` → 下载新增 PDF → **生成原子摘要** → 更新 `news.json` → `record_cache_refresh` 标记。
  - （新增）如启用增量审核缓存：检测 `news_audited.json` 与 `news.json` 最新日期，若有新增区间则触发增量分批审核并写入 `news_audited.json`。

## 9. 模型策略与限制
- **（修订）PDF处理模型列表**：
  - 在代码中定义一个全局变量，如 `SUPPORTED_DIRECT_PDF_MODELS = ["qwen-doc-turbo", "qwen-long"]`，用于逻辑判断。
- **（修订）模型职责**：
  - **原子摘要模型**：默认为 `qwen-doc-turbo`，负责事实提取。
  - **审计模型**：由用户在运行时指定，应为能力更强的模型（如 GPT-4, Claude 3 Opus, Qwen-Max 等）。

## 10. 实施步骤
1. 在 `disclosures_builder.py` 中增加 `SUPPORTED_DIRECT_PDF_MODELS` 全局变量和 PDF 处理的判断逻辑。
2. 实现 PDF 到 Markdown 的转换函数及缓存机制（可参考 `progressive_news_summarizer.py`）。
3. **（新增）** 创建一个新的函数或类，专门负责“战略审计流程”，实现3个月滚动批处理和数据替换逻辑。
4. **（新增）并发改造**：针对 `--all` 批量处理模式，引入 `concurrent.futures.ThreadPoolExecutor`。对信息提取 (`update_disclosures_for_stock`) 和战略审计 (`audit_news_json`) 的循环进行并发改造，实现每只股票一个线程并行处理，大幅提升整体效率。
5. 修改主流程，在生成原子摘要后，可以调用审计流程。
6. 提供回测接口并验证“防未来函数”。
