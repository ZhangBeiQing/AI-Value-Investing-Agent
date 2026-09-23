# 单股研究网页工作台

## 目的与边界

网页可读取正式 fixed_tracked 历史决策和逐股研究包，也可发起单股研究任务。运行中产物与正式交易日目录隔离；最终裁决校验通过后，在任务目录生成单股 `05-08`，并通过现有交易后处理服务合入正式 `stock_decisions.json`、`decision_summary.json`、`latest_decision_snapshot.json`，同时用本次隔离快照中的分析日价格更新 `data/skill_runs/_analysis_index.json`。BUY/SELL 更新 Agent 虚拟账本 `position.jsonl`，从而进入 BUY 建议收益统计。`manual_position_override.json` 是用户手工维护的真实持仓，只供 Agent 分析读取，网页和虚拟交易执行都不得修改它。

写入接口只接受本机访问；如要开放给其他用户，须先增加认证、授权与请求审计。当前不应通过 `--host 0.0.0.0` 将其作为公网服务。

## 任务契约

Step Name: `dashboard_single_stock_research`

Input: 标准股票代码、当天已完成的正式 `01_global_context.md` 和 `03_stock_analysis_input.md`、共享行情/财报/公告缓存、人工持仓及历史研究记忆。

Output: `data/web_research_runs/{job_id}/skill_runs/{date}/fixed_tracked/` 下的单股 `01-08` 和 `debate/{name}_{symbol}/`；正式决策汇总写入 `data/agent_data/book-fixed_tracked/`。任务状态、进度及日志在 `data/web_research_runs/jobs/`。任何阶段失败都标记为 `failed`，不得把未验证的裁决发布为成功。

Side Effects: 使用 `manage_daily_data --symbols {symbol}` 更新这只股票的共享数据缓存；可经用户网页操作将经本地名称映射核实的股票加入 `master_universe.json`。股票宇宙新增项按现有 schema 默认 `stock_type=growth`，这只是临时分类，后续应人工核对行业与类型。发布成功后写正式决策摘要，BUY/SELL 依原交易执行规则更新虚拟账本；HOLD/FLAT 不追加虚拟仓位行，避免同日空操作重复入账。

Validation: 校验单股研究包实际落盘；辩论结束后调用 `validate_debate_artifacts(..., require_verdict=True)`、单股 schema 和 `05` 契约校验。同股同日已有不同正式决策则拒绝覆盖；既有日常后处理也拒绝从另一份 `05` 对已入账的同股同日决策再次执行。发现疑似已写虚拟交易但缺少执行日志时停止，人工核查后恢复，防止重复入账。不能以 Agent 进程退出码单独判定成功。

Compatibility Notes: 不调用会清理整日目录的 `run_daily_pipeline`；正式 `01-08` 文件名和字段不变。网页任务复用现有单股研究生成器和 fixed_tracked 辩论角色规范，单股 `05-08` 保存在任务目录以免覆盖整日正式文件。日常固定池交易流程仍按原 skill 执行；其 `03` 继续读取人工真实持仓，但交易执行和无交易日记录仅从虚拟 `position.jsonl` 延续，不再用人工持仓覆盖 Agent 的收益账本。

## 名称匹配与任务状态

中文名称先在 `master_universe.json` 查找，再查本地 `symbol_stock_name_mapping.csv`，最后复用已有的板块行情快照及全 A 财务横截面原始缓存；此查询只读、不联网、不强刷缓存。唯一匹配可直接添加并启动；同名多代码由用户选定；没有可靠映射时不猜证券代码。新增股票在任务区立即出现，完整裁决通过校验及发布后才进入“已分析股票”列表。

网页任务选取当天已完成的公共研究输入；如果当天尚未准备完成，选上一个交易日已完成的输入，并在任务列表显示实际分析日。任务必须有该分析日的准确收盘价，不会拿其他日期价格顶替。Agent 阶段默认调用 `opencode run --auto --model deepseek/deepseek-v4-flash`；启动网页服务前设置 `DASHBOARD_RESEARCH_MODEL` 可覆盖模型。

任务状态为 `queued → preparing_data → financial_research（有新财报时）→ building_research → debating → validating → publishing → complete`；任一环节失败转为 `failed`。辩论阶段按 9 份角色/汇总文件统计进度。浏览器每 5 秒查询活动任务，服务端仅返回日志末尾，任务进程独立于浏览器请求运行。

用户可在任务列表或进度抽屉取消尚未发布的任务。服务端会校验 PID 确属对应网页 worker，再向该独立进程组发送终止信号，使数据准备、OpenCode 及其子进程一并停止；状态记为 `cancelled`，隔离目录中的中间产物保留审计但不发布、不记账。任务一旦进入 `publishing` 不允许取消，避免正式决策汇总或虚拟账本写到一半。

财报准备是网页单股研究的强制质量门禁。若公告索引已识别到最新正式财报，但 PDF 下载失败、PDF 转 Markdown 失败或 Markdown 未准备完成，任务必须在生成 `04_stock_research` 前转为 `failed`，不得用缺少财报深研的研究包继续辩论或发布正式裁决。已登记且最终报告文件仍有效的最新财报可复用，不重复研究。

## 股票详情 OpenCode 对话（本机试用）

已完成正式分析的股票详情底部可继续提问，并可选择本机 `opencode models` 列出的模型。首次提问附带最新正式决策、最新辩论、唯一一份最新完整研究包，以及**该分析日**的 `03_agent_input.md` 组合与人工真实持仓输入；研究包优先采用最近一次已完成的网页重新分析产物，没有时才回退到最新正式流水线产物。后续提问沿用同股 OpenCode 会话。对话与资料快照保存在 `data/web_research_runs/chats/`，不会写入 `05`、决策汇总或任何持仓账本。

网页对话使用独立工作目录，完整附带唯一一份最新研究包、正式决策、完整辩论和对应分析日组合输入。完整快照保存在 `context.md`，实际传给 OpenCode 时按小于单附件读取上限的 `context_part_*.md` 分片附加，并把研究包排在最前，避免大文件只读取前约 50KB 时误判研究包缺失。OpenCode 只允许读取当前股票工作目录里的上述上下文文件和调用 `WebSearch_*` 搜索工具；其他文件读取、写入、Shell、子 Agent 等工具均拒绝。百炼 WebSearch 通过本地 stdio 桥接器兼容其 Streamable HTTP JSON 响应。对话只用于讨论，不执行研究刷新或交易。模型列表是 OpenCode 已知模型，不代表每个模型都已完成认证；不可用模型会在对话区显示错误。完成新的研究后点击“新对话”可重新抓取资料，但原对话会从网页记录中移除。当前仍是单机单用户工作台，不能仅开放监听地址就提供给公众使用；面向多用户需增加身份认证、数据隔离、配额和隐私控制。

OpenCode 的 assistant 回复在浏览器端按安全 Markdown 渲染，支持标题、粗体、斜体、列表、引用、代码块、行内代码、链接、分隔线和表格；用户输入仍按纯文本展示。渲染过程使用 DOM 文本节点而非直接注入模型返回的 HTML。

股票详情抽屉可从左侧边缘拖动调整宽度，桌面端在可用视口内限制最小和最大宽度，并通过浏览器 `localStorage` 记住选择；双击调整柄或聚焦后按 `Home` 恢复默认宽度，移动端保持全宽。
