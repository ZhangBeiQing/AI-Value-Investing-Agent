# 当前 fixed_tracked 回测设计

本文件只记录回测差异，投资和研究策略以当前主干 skill 与 policy 为单一来源。

## 范围与版本

schema 2 冻结 configs.stock_pool.fixed_tracked_symbols()，与每日默认固定池一致，包含美股。仅额外保留隔离已有持仓；移除每日量化候选扩池、历史因子构建、聚焦股票绕过门禁和复用正式 01-04 的入口。
旧 schema 1 实验保留只读审计，拒绝续跑，防止将新交易规则混入旧账本。

## 两阶段调度

历史准备复用 daily-data-preparation 的角色分工，随后复用 auto-trading-fixed-tracked 的 P0、Bull/Bear、独立裁决和组合复核。具体运行以 backtest-fixed-tracked/SKILL.md 为准。
prepare-day 只检查缓存并生成隔离输入，不同步公告、不调用摘要 API，不以现实最新数据填充历史缺口。
宏观默认复用，显式 regenerate 才由独立 subagent 生成隔离短稿；主 Agent 只验收。财报分新深研、旧研究更新、摘要补建，不将缺摘要等同为缺深研。

## 01-04 契约

Step Name: Historical Inputs
Input: 冻结股池、隔离持仓/记忆、共享缓存、历史宏观与财报。
Output: 实验内 01-04、run_manifest、00_prepare_status；名称和单股 JSON 契约不变。
Side Effects: 不写正式交易目录；默认不抓取或刷新数据。
Validation: 检查精确历史价格、公告索引、研究知识截止和摘要；缺口明确分类并阻止 P0。
Compatibility Notes: 已删除旧回测专属选股逻辑和复制正式输入接口；旧实验不可续跑。

## 财报目录与身份

financial_reports 是基本面研究，disclosures/md 是财报原文。原文不可登记为总结，也不可因文件存在而跳过研究。注册、prepare 跳过判定、日常读取与 PDF 输出均设置边界。
同 symbol 名称目录可回查；回测额外验证研究知识截止。文件名公告日期不是研究知识日期。日期未知时报告需核验，不伪造历史截止。可复用的旧深研作为更新基线，明确缺的是哪一报告期。
经营数据公告是可用研究材料，不按标题排除；研究须区分已实现数据、初步核算和未来指引，后续实际结果按内容差异更新。

## 05-08 契约

Step Name: Isolated Next Open Execution
Input: 经辩论校验与组合复核的 05、隔离账本、下一实验交易日精确开盘价。
Output: 隔离 06、07、08 与交易记忆。
Side Effects: 不连接券商，不写真实持仓或正式账本。
Validation: 复用主干交易单位与币种折算，先卖后买、整批校验，不缩量、不按自报信心重新分配资金。缺价 pending；成功 06 幂等；账本存在执行而缺 06 时失败关闭。
Compatibility Notes: 回测特有差异仅保留历史价格、日期因果、隔离路径与自动模拟执行。

## 时间与计价

主调度用 CN 实际交易日序列。D 日以北京时间 23:59:59 为知识截止；D+1 取下一实验交易日的精确开盘价，标的所属市场当天无开盘价则 pending，不拿别日价格补位。
美股收盘使用截止时已完成的美东交易日，02 和 04 显示真实日期；美元现金流和资产折算沿用主干规则。港股沿用当前主干计价约定。
当前未模拟文字条件单、费用、滑点或跨日追单；当前固定池回看历史仍有幸存者偏差。finalize 拒绝未完成区间。

## 验证

使用 tests/test_backtest_alignment.py 与既有回归网验证股池、交易、日期、财报身份与污染拦截。另在 data/tmp_backtest_alignment 中做真实缓存只读审计与隔离流水线验证；不启动研究 subagent 或正式回测。
