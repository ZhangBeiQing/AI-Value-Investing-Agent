# AI交易系统 - 每日操盘总结数据库设计规范

## 系统概述

本系统设计用于记录和管理AI交易系统的每日操盘总结，通过智能合并连续持有操作来优化历史上下文的存储和检索，减少AI决策时的token消耗，同时保持决策逻辑的完整性。

## 核心设计原则

### 1. 数据归一化原则
- 所有操作记录必须结构化存储
- 支持高效的查询和合并操作
- 保持数据的完整性和一致性

### 2. 数据库智能合并策略
- 连续持有操作自动合并，保留最后一条的持有字段
- 买卖操作保持独立记录


## 数据库设计方案

### 数据表结构设计

#### 表1: stock_operations (原始操作记录表)
```
表名: stock_operations
用途: 存储AI每日对每只股票的原始操作记录

字段定义:
- stock_code: 股票代码 (字符串, 非空)
- stock_name: 股票名称 (字符串)
- operation_date: 操作日期 (日期类型, 非空)
- action_type: 操作类型 (枚举: BUY/SELL/HOLD/FLAT, 代表买，卖，持有不动，空仓)
- action_num: 操作数量 (整数)
- reason: 操作原因 (文本, 限制100字)
- confidence_score: 置信度 (浮点数, 0-1范围)
- key_observations: 关键观察点 (JSON文本)
- position_size: 当前股票占总仓位比例 (浮点数, 0-1范围)
- price_target: 目标价格 (浮点数, 可选)
- stop_loss: 止损价格 (浮点数, 可选)
- individual_risk_notes: 个股风险说明 (文本)
- individual_focus: 个股关注事项 (文本)
- raw_ai_output: 原始AI完整输出 (JSON文本)
- created_at: 创建时间戳 (自动生成)
- updated_at: 更新时间戳 (自动更新)

约束条件:
- 唯一约束: (stock_code, operation_date)
- 索引: stock_code, operation_date, action_type
- 检查约束: confidence_score BETWEEN 0 AND 1
- 检查约束: position_size BETWEEN 0 AND 1
```

#### 表2: operation_summary (操作合并记录表)
```
表名: operation_summary
用途: 存储为AI生成历史上下文的摘要记录，包含独立的买卖操作和合并后的持有操作。

字段定义:
- stock_code: 股票代码 (字符串, 非空)
- stock_name: 股票名称 (字符串)
- start_date: 操作开始日期 (日期类型, 非空)  -- 对于BUY/SELL，与end_date相同
- end_date: 操作结束日期 (日期类型, 非空)    -- 对于合并的HOLD和WATCH                                         ，这是持有期的最后一天
- duration_days: 持续天数 (整数, 默认为1)     -- (end_date - start_date) + 1
- action_type: 操作类型 (枚举: BUY/SELL/HOLD/WATCH)

// --- 以下字段直接继承自 `stock_operations` 表在 end_date 当天的记录 ---
- reason: 操作原因 (文本, 限制100字)
- confidence_score: 置信度 (浮点数, 0-1范围)
- key_observations: 关键观察点 (JSON文本)
- position_size: 当前股票占总仓位比例 (浮点数, 0-1范围)
- price_target: 目标价格 (浮点数, 可选)
- stop_loss: 止损价格 (浮点数, 可选)
- individual_risk_notes: 个股风险说明 (文本)
- individual_focus: 个股关注事项 (文本)
// --- 关键字段继承结束 ---

- created_at: 创建时间戳 (自动生成)
- updated_at: 更新时间戳 (自动更新)

约束条件:
- 唯一约束: (stock_code, start_date, end_date)
- 索引: stock_code, start_date, end_date
```


#### 表3: portfolio_daily_summary (系统信息表)
```
表名: portfolio_daily_summary
用途: 存储每日投资组合级别的系统信息

字段定义:
- summary_date: 总结日期 (日期类型, 非空)
- system_risk_notes: 系统风险说明 (JSON文本)
- system_focus_items: 系统关注事项 (JSON文本) 
- portfolio_overview: 组合概览 (JSON文本)
- total_positions: 总持仓数量 (整数)
- total_value: 组合总价值 (浮点数)
- cash_position: 现金比例 (浮点数, 0-1范围)
- overall_risk_level: 整体风险等级 (字符串)
- created_at: 创建时间戳 (自动生成)
- updated_at: 更新时间戳 (自动更新)

约束条件:
- 唯一约束: (summary_date)
- 索引: summary_date
```


## 数据处理流程

### 步骤1: 每日操作记录保存
```
输入: AI每日操盘总结JSON
输出: 存储到stock_operations表

处理逻辑:
1. 解析AI输出的JSON格式操作记录
2. 验证数据完整性和格式正确性
3. 将每只股票的操作记录插入stock_operations表
4. 记录原始AI输出用于审计追踪
```

###步骤2: 智能合并处理
```
输入: stock_operations表中的原始记录	
输出: operation_summary表中的摘要记录

处理逻辑 (例如，针对某一只股票):
1.  从 stock_operations 表中按日期升序获取该股票的所有操作记录。
2.  遍历这些记录：
    a.  如果当前记录是 BUY 或 SELL：
        - 直接将该记录的关键字段插入到 operation_summary 表对于的stock_code中。
        - 设置 start_date 和 end_date 为当前记录的 operation_date。
        - duration_days 设为 1。
    b.  如果当前记录是 HOLD：
        - 检查是否已处于一个连续的 HOLD 序列中。
        - 如果是序列的第一个 HOLD，则记录下它的 start_date 和所有字段信息。
        - 如果是序列中的后续 HOLD，则更新当前序列的 end_date，并用当前记录的字段信息覆盖之前暂存的信息（这样始终保留最新的）。
        - 当 HOLD 序列中断（遇到 BUY/SELL 或记录结束），则将这个合并后的 HOLD 序列作为一个单条记录插入 operation_summary 表。
          - start_date 是序列第一天的日期。
          - end_date 是序列最后一天的日期。
		  - 把start_date和end_date填入action_time中
          - duration_days 是持有天数。
          - 其他所有字段（reason, key_observations 等）均使用序列中最后一天的数据。
	C.  如果当前记录是 FLAT：则操作与HOLD相同
```

### 步骤3: 历史上下文生成
```
输入: 股票代码，时间范围参数
输出: 合并后的操作历史JSON

处理逻辑:
1. 查询指定时间范围内的记录:
   - 从operation_summary表获取合并的持有记录
3. 生成适合AI消费的JSON格式
```

## 数据查询与上下文生成

### 接口定义: `get_historical_context`

#### 1. 功能描述

提供一个标准化的查询接口，用于根据指定的股票代码，从**操作合并记录 (`operation_summary`)** 中获取最近 `N` 次的操作历史。

#### 2. 核心用途

该接口是AI决策系统的关键输入环节。其主要目的是为AI在进行次日操盘决策时，提供一个经过智能合并、高度浓缩且保持了关键信息的操作历史上下文。这有效减少了输入给AI模型的token数量，降低了成本和处理延迟，同时避免了冗余的“持有”信息干扰。

#### 3. 接口参数

| 参数名 | 类型 | 是否必需 | 描述 |
| :--- | :--- | :--- | :--- |
| `stock_code` | 字符串 (String) | 是 | 目标股票的唯一代码，例如 "002714"。 |
| `N` | 整数 (Integer) | 是 | 需要查询的最近操作记录的数量。例如，`3` 代表获取最近的3次操作。 |

#### 4. 返回格式

*   **成功**: 返回一个JSON数组（列表）。
    *   数组中的每个元素是一个JSON对象，其结构与 `operation_summary` 表的记录完全一致。
    *   数组内的记录严格按照 **操作结束日期 (`end_date`)** 从近到远（降序）排列。
    *   如果找到的记录数小于 `N`，则返回所有找到的记录。
    *   如果未找到任何记录，则返回一个空数组 `[]`。
*   **失败**: （可选，根据具体实现）可返回包含错误信息的JSON对象，例如 `{"error": "未找到指定股票代码"}`。

#### 5. 返回示例

假设调用 `get_historical_context(stock_code="002714", N=2)`，并且 `operation_summary.json` 中有如下相关记录，接口将返回：

```json
[
  {
    "stock_code": "002714",
    "stock_name": "牧原股份",
    "start_date": "2024-01-20",
    "end_date": "2024-01-20",
    "duration_days": 1,
    "action_type": "SELL",
    "reason": "卖出，股价短期内快速上涨，接近第一目标价位，决定阶段性获利了结，锁定利润，等待回调机会。",
    "confidence_score": 0.78,
    "position_size": 0.0,
    "price_target": null,
    "stop_loss": null,
    "key_observations": [
      "股价放量上涨后的技术性回调风险",
      "市场情绪指标是否出现过热迹象",
      "机构资金的流向变化"
    ],
    "individual_risk_notes": "猪周期本身波动巨大，及时兑现利润是控制回撤的重要手段。",
    "individual_focus": "卖出后转为观察，关注股价回落至关键支撑位的可能性。"
  },
  {
    "stock_code": "002714",
    "stock_name": "牧原股份",
    "start_date": "2024-01-18",
    "end_date": "2024-01-19",
    "duration_days": 2,
    "action_type": "HOLD",
    "reason": "持有，行业周度库存数据显示去化加速，验证了供需改善逻辑，继续持有以待猪价上涨周期兑现。当前策略是坚定持有核心仓位。",
    "confidence_score": 0.85,
    "position_size": 0.19,
    "price_target": 48.0,
    "stop_loss": 39.0,
    "key_observations": [
      "生猪期货主力合约价格与基差变化",
      "能繁母猪存栏量的官方月度数据",
      "公司现金流及资本开支计划"
    ],
    "individual_risk_notes": "非洲猪瘟等疫病风险是养殖行业固有的不确定性因素。",
    "individual_focus": "下周的公司经营电话会议纪要。"
  }
]
```

## JSON格式规范

### 给AI每日操盘总结的prompt:
"""
您是一个专业的AI交易系统，基于综合分析数据生成每日投资组合操作总结。

【输入数据】
- 估值指标、财报表现、行业动态、宏观环境、技术面信号
- 渐进式新闻总结和历史操作上下文

【输出要求】
生成完整的JSON格式操作总结，包含以下关键字段：

1. reason字段（每只股票）：
	请基于提供的综合分析数据，为每只股票生成不超过100字的操作理由，用于填充JSON输出中的reason字段。

	【可用数据维度】
	- 估值指标：PE/PB/PS历史分位数、行业相对估值
	- 财报表现：营收/利润趋势、盈利能力变化、财务健康状况
	- 行业动态：政策变化、竞争格局、供需关系、技术演进
	- 宏观环境：利率政策、经济周期、市场流动性、汇率变化
	- 技术面信号：价格趋势、成交量变化、关键支撑阻力位
	- 新闻事件：重大公告、管理层变动、行业新闻

	【撰写要求】
	1. 首句明确操作类型（增持/减持/持有/观察）和核心逻辑
	2. 聚焦2-3个最关键驱动因素，按重要性排序：
	   - 估值安全边际或风险
	   - 基本面趋势方向  
	   - 行业/宏观催化因素
	3. 体现风险收益比考量
	4. 语言精炼，避免技术细节

	【与JSON其他字段的协调】
	- reason字段：聚焦操作的核心逻辑（100字内）
	- key_observations字段：列出具体需要跟踪的指标
	- individual_risk_notes字段：详细说明特有风险
	- individual_focus字段：次日具体关注事项

	【示例格式】
	"持有，因[估值描述]提供安全边际，[基本面趋势]支撑长期价值，但需观察[关键风险因素]。当前[风险收益特征]。"
	"空仓，虽[潜在优点]，但因[核心风险/障碍]，目前风险收益比较低，等待[明确信号]出现。"

	【禁用内容】
	- 具体数字引用（如PE=15、价格42元等）
	- 冗长的原因罗列
	- 技术指标细节描述
	- 重复key_observations中的具体指标

2. key_observations字段（每只股票）：
   - 列出2-4个具体可跟踪的指标
   - 包括行业、基本面、估值、风险各维度
   - 避免与reason内容重复

3. individual_risk_notes字段（每只股票）：
   - 该股票特有的风险因素说明
   - 区别于系统级别的风险

4. individual_focus字段（每只股票）：
   - 次日需要特别关注的该股票相关事项
   - 具体可验证的事件或数据

5. system_risk_notes字段（系统级别）：
   - 投资组合整体风险控制措施
   - 仓位管理、行业配置等系统风险

6. system_focus_items字段（系统级别）：
   - 影响整个组合的市场宏观事件
   - 政策发布、经济数据等系统关注点

【字段间协调原则】
- reason：精炼的核心逻辑（为什么）
- key_observations：具体的跟踪指标（跟踪什么）  
- individual_risk_notes：特有的风险说明（注意什么）
- individual_focus：近期的关注重点（关注什么）
- 各字段内容互补，避免重复
"""

### 输入数据库格式 (AI每日输出结果示例)
```json
{
  "summary_date": "2024-01-15",
  "stock_operations": [
    {
      "stock_code": "002714",
      "stock_name": "牧原股份",
      "action_type": "HOLD",
	  "action_num": 2000,
	  "action_price": 40,
      "reason": "持有，因估值处于历史低位提供安全边际，行业供需改善趋势确立，但猪价反弹持续性仍需验证。当前风险收益比相对均衡。",
      "confidence_score": 0.75,
	  "position_size": 0.12,
      "price_target": 45.0,
      "stop_loss": 36.5
      "key_observations": [
        "生猪价格周度环比变化方向",
        "能繁母猪存栏量去化进度", 
        "公司成本控制成效持续性"
      ],
	  "individual_risk_notes": "猪周期下行风险尚未完全释放",
      "individual_focus": "关注月度销售数据验证"

    },
	{
	  "stock_code": "300750",
	  "stock_name": "宁德时代",
	  "action_type": "HOLD",
	  "action_num": 1500,
	  "action_price": 200,
	  "reason": "持有，因动力电池行业格局稳固，公司技术领先优势持续，但新能源车增速放缓与原材料价格波动压制估值。当前等待需求端明确复苏信号。",
	  "confidence_score": 0.82,
	  "position_size": 0.15,
	  "price_target": 220.0,
	  "stop_loss": 160.0
	  "key_observations": [
		"新能源车月度销量同比增速变化",
		"锂电池原材料（碳酸锂）价格企稳时点",
		"储能业务订单落地与毛利率改善情况",
		"海外市场拓展进度与政策环境变化"
	  ],
	  "individual_risk_notes": "原材料价格波动与技术迭代风险",
      "individual_focus": "关注季度装机量市场份额"
	},
    {
      "stock_code": "600519",
      "stock_name": "贵州茅台",
      "action_type": "FLAT", 
      "action_num": 0,
      "action_price": null,
      "reason": "空仓，虽品牌护城河极深，但当前估值在历史中枢，缺乏明显安全边际，且宏观消费环境存在不确定性，等待更好的价格或消费复苏的明确信号。", // <--- 更新点
      "confidence_score": 0.40, 
      "position_size": 0.00,
      "price_target": null,
      "stop_loss": null,
      "key_observations": [
        "高端白酒批价走势",
        "社零数据中可选消费品类表现",
        "公司直销渠道占比提升速度"
      ],
      "individual_risk_notes": "宏观经济下行导致高端商务和宴请需求不及预期的风险。",
      "individual_focus": "关注即将到来的节假日销售动销数据反馈。"
    }
  ],
  "system_risk_notes": [
    "整体仓位控制在70%以内，保持流动性",
    "行业分散度不足，需关注单一行业风险",
    "市场波动率上升，适当降低杠杆"
  ],
  "system_focus_items": [
    "明日CPI数据发布对市场情绪影响",
    "美联储议息会议前瞻",
    "行业政策动向跟踪"
  ]
}
```

## 阶段性实现方案：基于JSON文件的本地存储

为了快速验证核心业务逻辑、简化初期开发和部署，本系统在原型阶段采用基于Python和JSON文件的本地存储方案，作为对正式数据库设计的轻量级实现。此方案在功能上完整映射了数据库表结构，但在性能和并发性上有所简化。

*   **设计映射关系**:
    *   数据库表 `stock_operations` 对应于本地JSON文件 `stock_operations.json`。
    *   数据库表 `operation_summary` 对应于本地JSON文件 `operation_summary.json`。
    *   数据库表 `portfolio_daily_summary` 对应于本地JSON文件 `portfolio_daily_summary.json`。

*   **数据结构**:
    *   每个JSON文件的主体是一个JSON数组（列表）。
    *   数组中的每个JSON对象（字典）精确对应于设计规范中定义的表的一行记录，字段名和数据类型保持一致。

*   **适用场景**:
    *   此方案适用于单机运行、数据量可控的开发和测试环境，能够完全支持本规范定义的数据处理与查询流程。

*   **未来展望**:
    *   当系统进入生产环境或面临更高性能要求时，可以基于当前已验证的逻辑，将数据读写模块平滑迁移至SQL或NoSQL数据库，而核心业务逻辑层无需大幅改动。

---

