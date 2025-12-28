# AI Value Investing Agent (AI 价值投资代理)

这是一个基于 AI 的投资代理项目，旨在进行价值投资分析和模拟交易，目前支持 **A股和港股**。

该项目利用大语言模型（LLM）分析财务数据、新闻和宏观经济指标，根据可配置的策略执行回测或实时交易模拟。

## 📖 文档

详细的设计文档位于 `docs/` 文件夹下的各个子文件夹中，请查阅以获取更多技术细节。

## 🚀 快速开始

### 1. 安装依赖

使用 pip 安装所需的 Python 依赖包：

```bash
pip install -r requirements.txt
```

### 2. 配置

#### 股票池配置

修改 `configs/stock_pool.py` 文件，定义您想要追踪和投资的股票池。请更新 `TRACKED_A_STOCKS` 列表：

```python
# configs/stock_pool.py
TRACKED_A_STOCKS: List[StockEntry] = [
    StockEntry("002352.SZ", "顺丰控股", "物流龙头"),
    # 在此处添加您的股票...
]
```

#### 运行配置

修改 `configs/default_config.json`（或创建您自己的配置文件），选择用于回测或实时运行的模型以及日期范围：

```json
{
  "date_range": {
    "init_date": "2024-01-01",
    "end_date": "2024-12-31"
  },
  "models": [
    {
      "name": "deepseek-reasoner",
      "basemodel": "deepseek/deepseek-reasoner",
      "signature": "deepseek-reasoner",
      "enabled": true
    }
  ]
  // ... 其他设置
}
```

#### 环境变量

将示例环境变量文件复制为 `.env`，并填入您的 API Key：

```bash
cp .env.example .env
```

编辑 `.env` 文件，设置您的模型 API Key 和 Base URL（例如 OPENAI_API_KEY, DEEPSEEK_API_KEY 等）。

### 3. 数据管理

运行日常数据管理脚本，更新所有需要的数据（股价、财报、新闻等）：

```bash
python scripts/manage_daily_data.py
```

### 4. 运行代理

最后，运行主程序以开始 `default_config.json` 中指定的 AI 回测或模拟：

```bash
python main.py
```

## � 效果展示

在正式使用前，您可以在 `data/agent_data/deepseek-reasoner/v2.1版本_价值投资_回测_20240101-20250101/` 文件夹下查看本项目的效果展示。

- `position/`: 包含 AI 的交易记录。
- `log/`: 包含 AI 每天的思考过程。

## �📂 项目结构

- `agent/`: 代理核心逻辑。
- `configs/`: 配置文件。
- `data/`: 数据存储（缓存、日志、结果）。
- `docs/`: 设计和系统文档。
- `scripts/`: 工具脚本（数据管理等）。
- `tools/`: 代理使用的 MCP 工具。

## 📄 许可证

[MIT License](LICENSE)
