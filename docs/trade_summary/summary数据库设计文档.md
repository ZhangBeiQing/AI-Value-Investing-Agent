# AI交易系统 - 每日操盘总结数据库设计规范

## 系统概述

本系统设计用于记录和管理AI交易系统的每日操盘总结，通过智能合并连续持有操作来优化历史上下文的存储和检索，减少AI决策时的token消耗，同时保持决策逻辑的完整性。

## 核心设计原则

### 1. 数据归一化原则
- 所有操作记录必须结构化存储
- 支持高效的查询和合并操作
- 保持数据的完整性和一致性

### 2. 数据库智能合并策略
- 连续持有操作自动合并，保留最后一条的持有原因，观察重点
- 买卖操作保持独立记录


## 数据库设计方案

### 数据表结构设计

#### 表1: stock_operations (原始操作记录表)
```
表名: stock_operations
用途: 存储AI每日对每只股票的原始操作记录

字段定义:
- id: 唯一标识符 (主键, 自增)
- stock_code: 股票代码 (字符串, 非空)
- stock_name: 股票名称 (字符串)
- operation_date: 操作日期 (日期类型, 非空)
- action_type: 操作类型 (枚举: BUY/SELL/HOLD/INCREASE/DECREASE)
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

#### 表2: merged_operations (合并操作记录表)
```
表名: merged_operations
用途: 存储合并后的连续持有操作记录

字段定义:
- id: 唯一标识符 (主键, 自增)
- stock_code: 股票代码 (字符串, 非空)
- start_date: 合并开始日期 (日期类型)
- end_date: 合并结束日期 (日期类型)
- action_type: 操作类型 (固定为'HOLD')
- merged_reason: 合并后的原因摘要 (文本)
- original_count: 合并的原始记录数量 (整数)
- key_observations: 关键观察点变化 (JSON文本)
- duration_days: 持有天数 (整数)
- created_at: 创建时间戳 (自动生成)

约束条件:
- 唯一约束: (stock_code, start_date, end_date)
- 索引: stock_code, start_date
```

#### 表3: portfolio_daily_summary (系统信息表)
```
表名: portfolio_daily_summary
用途: 存储每日投资组合级别的系统信息

字段定义:
- id: 唯一标识符 (主键, 自增)
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

### 步骤2: 智能合并处理
```
输入: stock_operations表中的原始记录
输出: merged_operations表中的合并记录

处理逻辑:
1. 按股票代码分组，按日期排序操作记录
2. 识别连续的HOLD操作序列
3. 对连续HOLD序列进行智能合并:
   - 合并条件: 相同股票，连续日期，均为HOLD操作
   - 保留最新日期的字段为合并后字段
   - 记录合并的原始记录数量
   - 提取关键观察点变化轨迹
4. 将合并结果插入merged_operations表
5. 买卖操作保持独立，不进行合并
```

### 步骤3: 历史上下文生成
```
输入: 股票代码，时间范围参数
输出: 合并后的操作历史JSON

处理逻辑:
1. 查询指定时间范围内的记录:
   - 从merged_operations表获取合并的持有记录
   - 从stock_operations表获取独立的买卖操作
2. 按时间顺序组合两类记录
3. 生成适合AI消费的JSON格式
4. 应用token优化策略控制输出大小
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
	1. 首句明确操作类型（增持/减持/持有）和核心逻辑
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

### 输出格式 (提供给AI的历史上下文)
```json
{
  "retrieval_date": "2024-01-15",
  "time_range": "2023-12-01 至 2024-01-15",
  "stock_operations_history": [
    {
      "stock_code": "002714",
      "date_period": "2023-12-01 至 2024-01-10",
      "action": "HOLD",
      "reason": "持续持有41天 - 猪周期底部震荡，等待行业复苏信号",
      "duration_days": 41,
      "key_trend_changes": [
        "12月中旬猪价企稳",
        "1月初行业库存下降"
      ]
    },
    {
      "stock_code": "002714",
      "date": "2024-01-11",
      "action": "INCREASE",
      "reason": "行业数据显示供需关系改善，适度增加仓位",
      "confidence": 0.78,
      "position_change": "+5%"
    },
    {
      "stock_code": "002714", 
      "date_period": "2024-01-12 至 2024-01-15",
      "action": "HOLD",
      "reason": "继续观察猪价反弹持续性",
      "duration_days": 4
    }
  ],
  "summary_statistics": {
    "total_operations": 15,
    "hold_operations_merged": 12,
    "buy_sell_operations": 3,
    "token_efficiency_improvement": "68%"
  }
}
```

## 合并算法详细说明

### 连续持有检测算法
```
伪代码:
function detect_continuous_hold_operations(stock_operations):
    按stock_code分组操作记录
    对每组记录按operation_date排序
    
    初始化合并结果列表
    对于每个股票分组:
        初始化当前持有序列 = []
        对于排序后的每条记录:
            如果 action_type == 'HOLD':
                将记录加入当前持有序列
            否则:
                如果当前持有序列不为空:
                    处理并合并当前持有序列
                    清空当前持有序列
                将当前买卖操作直接加入结果
        
        处理最后可能剩余的持有序列
    
    返回合并结果列表
```

### 持有序列合并策略
```
伪代码:
function merge_hold_sequence(hold_sequence):
    如果 序列长度 == 0: 返回空
    如果 序列长度 == 1: 返回单条记录（不合并）
    
    计算持有天数 = 最后日期 - 开始日期 + 1
    提取关键观察点 = 分析序列中reason字段的变化模式
    生成合并原因 = 基于最新原因和持有天数的摘要
    
    返回合并记录:
        stock_code: 股票代码
        start_date: 序列开始日期  
        end_date: 序列结束日期
        action_type: 'HOLD'
        merged_reason: 合并原因
        original_count: 序列长度
        key_observations: 关键观察点
        duration_days: 持有天数
```

## 性能优化策略

### 查询优化
- 使用复合索引加速时间范围查询
- 预计算常用统计指标
- 实现分页查询支持大规模历史数据

### Token效率优化
- 默认提供30天历史窗口
- 支持可配置的时间范围参数
- 自动截断过长的文本字段

### 数据维护
- 定期归档历史数据
- 实现数据备份和恢复机制
- 监控数据库性能和存储使用

## 集成接口规范

### 数据写入接口
```
端点: /api/operations
方法: POST
内容类型: application/json
请求体: AI每日输出JSON格式
响应: 操作结果状态
```

### 历史查询接口  
```
端点: /api/operations/history
方法: GET
参数:
  - stock_code: 股票代码（可选）
  - start_date: 开始日期（可选）
  - end_date: 结束日期（可选）
  - max_records: 最大记录数（可选）
响应: 合并后的操作历史JSON
```

### 统计查询接口
```
端点: /api/operations/statistics
方法: GET
参数:
  - stock_code: 股票代码（可选）
  - time_range: 时间范围（可选）
响应: 操作统计信息JSON
```

## 错误处理机制

### 数据验证错误
- 无效的股票代码格式
- 日期格式不正确
- 操作类型枚举值无效
- 必填字段缺失

### 业务逻辑错误
- 重复的操作记录
- 时间顺序不一致
- 合并冲突检测

### 系统级错误
- 数据库连接失败
- 存储空间不足
- 并发访问冲突

此设计规范为AI交易系统提供了完整的每日操盘总结管理方案，通过智能合并连续持有操作显著优化了AI决策的历史上下文效率，同时保持了决策逻辑的完整性和可追溯性。