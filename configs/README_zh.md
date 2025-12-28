# 配置文件

此目录包含AI-Trader Bench的配置文件。这些JSON配置文件定义了交易代理在执行过程中使用的参数和设置。

## 文件说明

### `default_config.json`

主要的配置文件，定义了所有系统参数。该文件由`livebaseagent_config.py`加载，包含以下部分：

#### 代理配置
- **`agent_type`**: 指定要使用的代理类
- **`agent_config`**: 代理特定参数
  - `max_steps`: 每次交易决策的最大推理步数（默认：30）
  - `max_retries`: 失败操作的最大重试次数（默认：3）
  - `base_delay`: 操作间的基础延迟时间（秒）（默认：1.0）
  - `initial_cash`: 交易起始资金（默认：$10,000）

#### 日期范围
- **`date_range`**: 交易周期配置
  - `init_date`: 交易模拟开始日期（格式：YYYY-MM-DD）
  - `end_date`: 交易模拟结束日期（格式：YYYY-MM-DD）

#### 模型配置
- **`models`**: 用于交易决策的AI模型列表
  - 每个模型条目包含：
    - `name`: 模型的显示名称
    - `basemodel`: 完整的模型标识符/路径
    - `signature`: API调用的模型签名
    - `enabled`: 启用/禁用模型

#### 日志配置
- **`log_config`**: 日志参数
  - `log_path`: 存储代理数据和日志的目录路径

## 使用方法

### 默认配置
当未指定特定配置文件时，系统会自动加载`default_config.json`：

```bash
python livebaseagent_config.py
```

### 自定义配置
您可以指定自定义配置文件：

```bash
python livebaseagent_config.py configs/my_custom_config.json
```

### 环境变量覆盖
某些配置值可以通过环境变量覆盖：
- `INIT_DATE`: 覆盖初始交易日期
- `END_DATE`: 覆盖结束交易日期

## 配置示例

### 最小配置
```json
{
  "agent_type": "BaseAgent",
  "date_range": {
    "init_date": "2025-01-01",
    "end_date": "2025-01-31"
  },
  "models": [
    {
      "name": "gpt-4o",
      "basemodel": "openai/gpt-4o-2024-11-20",
      "signature": "gpt-4o-2024-11-20",
      "enabled": true
    }
  ],
  "agent_config": {
    "max_steps": 10,
    "initial_cash": 5000.0
  },
  "log_config": {
    "log_path": "./data/agent_data"
  }
}
```

### 多模型配置
```json
{
  "agent_type": "BaseAgent",
  "date_range": {
    "init_date": "2025-01-01",
    "end_date": "2025-01-31"
  },
  "models": [
    {
      "name": "claude-3.7-sonnet",
      "basemodel": "anthropic/claude-3.7-sonnet",
      "signature": "claude-3.7-sonnet",
      "enabled": true
    },
    {
      "name": "gpt-4o",
      "basemodel": "openai/gpt-4o-2024-11-20",
      "signature": "gpt-4o-2024-11-20",
      "enabled": true
    },
    {
      "name": "qwen3-max",
      "basemodel": "qwen/qwen3-max",
      "signature": "qwen3-max",
      "enabled": false
    }
  ],
  "agent_config": {
    "max_steps": 50,
    "max_retries": 5,
    "base_delay": 2.0,
    "initial_cash": 20000.0
  },
  "log_config": {
    "log_path": "./data/agent_data"
  }
}
```

## 注意事项

- 配置文件必须是有效的JSON格式
- 系统会验证日期范围，确保`init_date`不大于`end_date`
- 只有`enabled: true`的模型才会用于交易模拟
- 配置错误会导致系统退出并显示相应的错误消息
- 配置系统通过`AGENT_REGISTRY`映射支持动态代理类加载

## 配置参数详解

### 代理类型 (agent_type)
目前支持的类型：
- `BaseAgent`: 基础交易代理，使用MCP工具链进行交易决策

### 模型配置 (models)
每个模型需要包含以下字段：
- `name`: 用于日志和显示的名称
- `basemodel`: 完整的模型路径，用于API调用
- `signature`: 模型签名，用于标识特定模型版本
- `enabled`: 是否启用该模型参与交易

### 代理参数 (agent_config)
- `max_steps`: 控制AI代理的推理深度，数值越大分析越深入但耗时越长
- `max_retries`: 操作失败时的重试次数，提高系统稳定性
- `base_delay`: 操作间延迟，避免API调用过于频繁
- `initial_cash`: 初始资金，影响交易策略和风险控制

### 日志路径 (log_config)
- `log_path`: 所有代理数据、交易记录和日志的存储位置
