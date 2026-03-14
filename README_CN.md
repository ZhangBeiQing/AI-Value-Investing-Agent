# AI Value Investing Agent (AI 价值投资代理)

这是一个本地 `skill-only` 的 AI 投资工作流项目，目前支持 **A股和港股**。

项目会在本地准备好每日研究输入，再由你本地的桌面 Agent / LLM 按固定 skill 流程读取这些文件完成分析，并通过本地 Python 完成模拟交易。

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

#### Prompt Flow 配置

当前启用的提示词流程配置是 `configs/prompt_flow/skill_flow.json`。

本项目现在的主流程是生成 `data/skill_runs/{date}/` 下的每日输入文件，供本地 Agent 读取，而不是再通过 MCP 服务实时取数。

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

### 4. 生成每日 Skill 输入

生成给本地 Agent 使用的每日输入包：

```bash
python scripts/run_daily_pipeline.py --date 2026-03-14
```

会生成：

- `01_global_context.md`
- `02_basic_snapshot_payload.json`
- `03_agent_input.md`
- `04_stock_research/*.md`

### 5. 运行交易后处理

当本地 Agent 产出 `05_decision.json` 后，运行：

```bash
python scripts/run_post_trade.py --date 2026-03-14
```

## � 效果展示

在正式使用前，您可以在 `data/agent_data/deepseek-reasoner/v2.1版本_价值投资_回测_20240101-20250101/` 文件夹下查看本项目的效果展示。

- `position/`: 包含 AI 的交易记录。
- `log/`: 包含 AI 每天的思考过程。

## �📂 项目结构

- `services/`: skill-only 业务核心服务层。
- `configs/`: 配置文件。
- `data/`: 数据存储（缓存、日志、结果）。
- `docs/`: 设计和系统文档。
- `scripts/`: 每日工作流 CLI 入口。
- `shared_data_access/`: 统一数据访问与缓存层。
- `agent_tools/`: 少量历史导入路径的兼容包装层。

## 📄 许可证

[MIT License](LICENSE)
