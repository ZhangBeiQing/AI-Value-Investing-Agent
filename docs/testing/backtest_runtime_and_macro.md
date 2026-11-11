# 回测模型记录与隔离宏观重写

- 模型元数据：创建实验通过 `--model`、`--reasoning-effort`、`--model-provider`、`--runtime` 记录经核实的运行时；参数不负责选择模型。未取得的信息记录 unknown，角色继承当前模型。恢复时按 skill 核对模型与档位。
- 组合复核：全部 P0 裁决校验后、合并前由主 Agent 写 `00_portfolio_review.md`，复核现金与多股 BUY、集中风险和冲突；调整交原角色处理。

Step Name: Historical Macro Override
Input: 实验配置 macro_summary_policy 与实验内 macro_economy/YYYYMMDD.md。
Output: 原契约 01_global_context.md，宏观节消费隔离新版。
Side Effects: 不覆盖共享宏观稿，不修改行情、成交或仓位规则。
Validation: regenerate 缺稿、日期缺失、五节缺失或超 3000 非空白字符时状态为 needs_macro_research，禁止生成输入或进入 P0。内容的历史真实性仍须 Agent 研究核实，程序格式检查不能证明事实正确。
Compatibility Notes: 默认 reuse，老实验缺配置时仍复用；重写模式禁止复用正式 01-04。01-08 文件与字段保持兼容。宏观 override 是显式参数，不改变全局环境变量或共享目录。

财报沿用当前优先读取 daily.json 摘要的机制。回测财报门禁额外返回 daily_brief_items：已有深研但缺少有效历史摘要时须由 Author 补建，状态保持 needs_financial_research；不因摘要缺失重跑完整季度研究。新深研注册要求 require-daily-brief。完整报告保留供关键数字和 §15 复核。宏观 subagent 与纯 Python 财报 prepare 并行；两者结束后主 Agent 调度财报角色，均验收后重建 01-04。


## 2026-11-10 清理与验证

- schema 2 与每日主干共享固定池，移除量化扩池、聚焦门禁绕过和复制正式输入。
- 删除回测专属公告刷新与财报同步编排；prepare-day 只读缓存，不调用摘要模型。
- 删除按置信度排列买入、现金不足缩量、交易数量向下取整和卖超截断；共用主干交易单位、先卖后买批次及币种折算。
- 修复美股未收盘数据与港股休市误报；固定美元折算口径，旧实验拒绝混入新规则。
- 注册、prepare 跳过判定、报告读取和 PDF 输出拦截误放原文；同代码名称目录可回查，研究日期不再等同于文件名。
- 已删除星宸科技与万通发展总结目录中两份与披露缓存 SHA256 完全相同的原文副本、一个错误登记和 47 份引用副本的派生缓存。真正披露原文和有效深研保留。
- 经营数据公告不按标题排除；明确是指引、非完整季报的底稿保留作为基线，不能据此假定后续实际财报已研究。
- 最新验证：312 项测试通过；单 ETF 的 run_daily_pipeline 在临时目录成功生成 01-04；29 标的本地历史预检无行情缓存误报，按财报历史适用性门禁停止，未调用研究 subagent 或重启正式回测。
- 过程证据：data/tmp_backtest_alignment/verification.json、raw_report_audit.json、pytest_final.log。
