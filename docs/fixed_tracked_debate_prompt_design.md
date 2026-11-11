# fixed_tracked 独立研究与单裁决设计

更新日期：2026-11-09。

## 职责划分

- 主 Agent 读组合输入、选择 P0、征求确认、调度角色、做组合复核；不得代写个股结论。
- Bull / Bear 独立读取相同当日材料，从增加/维持与减少/回避敞口两个方向研究，各写 opening。
- Adjudicator 使用第三个独立会话，核对双方决定性事实、推理、反面证据与缺口，独立决定动作、数量和价格印象，同时完成最终输出。
- 有决定性争议时由主 Agent 复用原 Advocate 定向回应，再由原裁决者核验；没有争议不固定反驳。

## 规则唯一来源

投资哲学：`configs/prompt_flow/fixed_tracked/investment_policy.md`。
主 Agent 方法：`main_policy.md`；个股共同方法：`stock_analysis_policy.md`。
搜索与证据：`configs/research/web_research_policy.md`。
调度、确认点及角色模板：`.codex/skills/auto-trading-fixed-tracked/SKILL.md` 与其 references。
最终字段：`stock_decision.schema.json`。不在角色派单中转述固定规则。

## 当前文件

```text
debate/{stock_name}_{symbol}/
├── review_context.json
├── advocates/bull/opening.json
├── advocates/bear/opening.json
├── advocates/*/rebuttal.json             # 有实质争议时才写
├── adjudicator/review_request.json       # 有需要才写
├── adjudicator/review_resolution.json    # 逐项解决质询
└── final/stock_verdict.json
```

review_context 记录股票、日期与当日实际持股，用于校验持仓语义。

## 程序与人工边界

manage_debate 只有 prepare / validate。prepare 接收实际持股数并建目录，validate 检查
必需角色产物、质询闭环、schema、动作/持仓语义、卖出不超仓和现有执行器交易单位。
原有 01–05 文件名、最终 decision 字段与交易执行规则保留，新增运行须有仓位及数量依据。
字段检查不是事实核验；关键数字仍需原文验证。现金与组合风险由主 Agent 复核，执行器继续检查现金。

用户确认 P0 后研究，确认最终方案后合并05，另行授权后才运行真实交易后处理。
回测复用相同研究模板，所有目录与持股来自隔离实验，D+1 精确开盘价规则保持不变。
网页入口显示双方研究、必要质询与独立裁决。

## 输入成本

完整季度深研继续保留；每日默认使用有效的财报日常摘要，缺少或失效时明确回退全文。
角色读完整当日研究包，交易依赖的关键财务数字按需回查完整报告对应章节。
对有季度深研的公司股票，三名交易角色均完整读取完整版 §15 的情景、倍数依据与限制，产业空间依赖项再回查 §10。
常规为 3 个角色、3 个 turn。实际费用仍受搜索、推理、缓存与模型计费影响，不承诺固定节省比例或收益。

## 决策假设审查

质询同时覆盖事实冲突及足以改变动作的合理假设分歧。三名角色须明确持有期限、利润口径、倍数依据、分红及稀释，比较固定利润与固定倍数的敏感性。关键论证缺失时向原 Advocate 定向质询；回应后裁决者必须选择工作假设并给出最终动作，不要求排除所有其他合理参数或消除预测不确定性。HOLD为主动持有判断；无需增加角色。主 Agent 复核该闭环，不代写动作。

历史估值生成使用财报公告次日起首个交易日价格，极值与每日分布只匹配当时可用财报；缺少公告日不回填报告期。日期型公告统一延至下一自然日供历史价格匹配，避免把盘后披露用于当日收盘。报告列出估值日期、复权及股本限制，季度样本不作为每日极值或分位。现有输出文件与字段保留，历史表增加估值日期。

裁决者综合证据、情景相对概率、收益损失、期限及费用，选择当前最合适的动作；允许有依据的主观概率或范围，无法精确量化时仍作定性权衡并给出最终决策。
