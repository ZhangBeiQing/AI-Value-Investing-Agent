# Subagent 决策偏差根因分析与 Pipeline 改进方案

> 从 2026-06-22 思特威（688213.SH）BUY 误判案例中提取的系统性改进建议
>
> 历史说明：本文记录旧版“单 subagent + skill_flow.json”设计。fixed_tracked 当前修改入口是 `docs/fixed_tracked_debate_prompt_design.md`，本文不再代表当前 Prompt 和辩论架构。

---

## 一、案例复盘

### 发生了什么

思特威今日收 95.96 元（-6.22%），subagent 给出 BUY 建议（100 股，置信度 0.65），价格印象"偏低估"。

### 实际数据

| 指标 | 数值 |
|:---|:---|
| TTM 扣非 PE | 37.09x |
| Q1 净利润增速 | +29.53% |
| TTM PEG | 1.26 |
| 一致预期 2026E 增速 | +39% |
| Forward PE（基于一致预期） | 27.65x |
| Forward PEG（基于一致预期） | 0.71 |

### 为什么 BUY 是误判

Q1 增速仅 24%（归母），要全年 39% 意味着 Q2-Q4 需同比 +43%——Q1 数据在证伪全年预期，而非支持。Forward PEG 0.71 的"偏低估"建立在尚未被数据支撑的乐观假设上。TTM PEG 1.26 才是更诚实的定价参照。

主 agent 复审裁决：**FLAT，价格印象调整为"合理偏贵"。**

---

## 二、根因分析

### 根因 1：history_anchor 存精确交易指令，被当作执行命令

上一轮 FLAT 时写了：

> "建议 FOMC 后板块抗跌确认后，在 92-97 元首次试探建仓"

价格跌入 92-97 区间后，subagent 产生了"条件已触发、应该执行"的锚定效应，跳过了"假设是否仍然成立"的重新验证。

**本质问题**：history_anchor 在承担"决策记忆"功能的同时，不慎承担了"交易指令"功能。下一轮 subagent 看到数字就执行，而不是看到框架就重算。

### 根因 2：Forward PEG 给人虚假的"算过账"安全感

计算链条：95.96 元 → 市值 386 亿 → 一致预期 13.95 亿 → Forward PE 27.65x → 增速 39% → PEG 0.71 → "偏低估"

每一步看起来都有数据支撑，但链条底座的"一致预期 13.95 亿/增速 39%"是过去 90 天机构在没看到 Q1 数据前拍出来的。Q1 已经告诉你了增速只有 24%，但 Forward PE 一算出来就是一个"干净的数字"——数字的确定感压倒了趋势的警示。

**本质问题**：Forward PE 的计算门槛太低（市值 ÷ 一致预期），让人产生"我算过了"的安全感。但一致预期本身就是最需要被质疑的变量。

### 根因 3：con 赢了辩论，输了投票

subagent 自己的 con 部分写得很诚实：

> "增速中枢下移可能引发估值框架重估...若全年增速仅 25-30%，当前 27.65x Forward PE 不再有明显折价"
> "中金已率先下调→一致预期存在下修风险"
> "融资盘 90% 分位是悬在头上的达摩克利斯之剑"

但 verdict 还是 BUY。理由：**"这些问题更适合通过仓位管理和后续跟踪来解决，而非作为完全不买的理由。"**

con 中属于"假设被证伪"的论据（增速降档），被错误地归类为"可管理的风险事项"，而不是否决项。motion 是"BUY 候选"，庭审的默认前提就是"要不要买"，而非"要不要推翻 motion"——预设了方向。

**本质问题**：庭审框架没有区分"风险事件"和"假设被证伪"。前者可以通过仓位管理消化，后者应该直接推翻 motion。

---

## 三、改进方案

### 涉及文件

| 文件 | 作用 |
|:---|:---|
| `configs/prompt_flow/skill_flow.json` | 生成 `03_agent_input.md` 的模板（fixed_tracked） |
| `configs/prompt_flow/skill_flow_short_book.json` | short_book 版本 |
| `configs/prompt_flow/skill_flow_long_book.json` | long_book 版本 |
| `.codex/skills/auto-trading-fixed-tracked/SKILL.md` | subagent prompt 模板末尾加警示 |

---

### 修改点 1：重写 `input_blocks` 中的"历史交易总结使用指南"

**现状**（一句话）：

> subagent今日分析某只股票时，优先使用该股票研究包 `04_stock_research/{symbol}_research.md` 中的"最近一次交易日历史交易总结"作为 `history_anchor` 主来源

**改为**：

```
【历史交易总结使用指南】：
subagent今日分析某只股票时，优先使用该股票研究包 `04_stock_research/{symbol}_research.md` 中的
"最近一次交易日历史交易总结"作为 `history_anchor` 主来源。

⚠️ history_anchor 的正确使用方式：
- history_anchor 是"决策记忆锚"——记录上一次的判断框架、估值逻辑和核心假设，供今日继承和对比。
- history_anchor 中的具体价格区间、买入条件、建仓建议，是上一轮的"分析产物"而非"执行指令"。
- 当日价进入 history_anchor 中提到的区间时，你应该：①重新验证当时设定的估值假设今天是否仍然成立；
  ②独立形成今日的价格印象和判断；③再决定是否行动。不得因为"上次说过这个价位可以买"而直接下单。
  市场每天都在快速变化，之前的交易决策仅仅作为初略的参考
```

---

### 修改点 2：`final_summary_rules` 增补三条硬规则

在 `【不宜写法】` 小节之后、`【输出格式示例】` 之前插入：

```
【HOLD / FLAT 时的硬性约束】
- 当 court.verdict 为 HOLD 或 FLAT 时，"verdict"和`recommended_action` 中禁止写"若回调至 XX 元则买入/建仓/加仓"等
  因为你的结论是HOLD和FLAT，你不应该给出任何buy的建议。你可以写成"若回调至 XX 元可重新评估是否买入"。
  具体的未来的buy决策，应该让下一交易日的subagent自己评估

```

---

### 修改点 3：`workflow_steps` Step 5 增加"假设检验"强制步骤

在 `Step 5` 末尾、"若裁决为 BUY，还必须回答四个问题"之前插入：

```
在进入庭审辩论前，subagent 必须先完成【假设核验】步骤：
  1. 回顾 history_anchor 中上一轮分析的核心假设（隐含的增速/毛利率/估值框架等）
  2. 逐条判断：这些假设在今日是否仍然成立？有没有被新数据证伪或削弱？
  3. 如果任何核心假设已被削弱，必须在 delta_summary 中显式记录，并在正反辩论中作为反方论据
```

---

### 修改点 4：SKILL.md subagent prompt 模板末尾加警示

在 subagent prompt 模板的结尾（`写完后回传一句话确认` 之前）插入：

```
⚠️ 注意：history_anchor 中上一轮 HOLD/FLAT 分析提出的买入价格区间是参考框架，不是执行指令。
即使今日价格进入该区间，也必须基于最新数据进行独立判断，不要因"之前说过这里可以买"而行动。
```

---

## 四、修改点与根因对照

| 根因 | 修改点 |
|:---|:---|
| history_anchor 存指令 → 被当作执行命令 | 修改点 1（使用指南重写）+ 修改点 4（prompt 末尾警示） |
| Forward PEG 给出虚假安全感 | 修改点 2（Forward PE 使用规范） |
| con 赢了辩论输了投票 | 修改点 2（庭审否决规则）+ 修改点 3（假设检验强制步骤） |

---

## 五、影响范围

- 三个 `skill_flow*.json` 配置文件：修改后下次 `run_daily_pipeline` 生成的 `03_agent_input.md` 将包含新规则
- SKILL.md：主 agent 在派发 subagent 时 prompt 末尾自动携带警示
- 不影响 `05_decision.json` 的结构兼容性，仅改变 subagent 的行为规则
- 对已有 history_anchor 中的旧格式精确区间无破坏——subagent 会被新规则约束重新评估而非盲从
