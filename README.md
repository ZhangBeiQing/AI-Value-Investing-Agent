# AI Value Investing Agent

[中文版](README_CN.md)

This project is an AI-driven investment agent capable of conducting value investing analysis and simulated trading for **A-shares and HK-shares**.

It leverages Large Language Models (LLMs) to analyze financial data, news, and macroeconomic indicators, performing backtesting or live trading simulations based on configurable strategies.

## 📖 Documentation

Detailed design documentation can be found in the `docs/` directory's subfolders.

## 🚀 Quick Start

### 1. Installation

Install the required dependencies using pip:

```bash
pip install -r requirements.txt
```

### 2. Configuration

#### Stock Pool

Modify `configs/stock_pool.py` to define the list of stocks you want to track and invest in. Update the `TRACKED_A_STOCKS` list:

```python
# configs/stock_pool.py
TRACKED_A_STOCKS: List[StockEntry] = [
    StockEntry("002352.SZ", "SF Holding", "Logistics Leader"),
    # Add your stocks here...
]
```

#### Run Configuration

Modify `configs/default_config.json` (or create your own config file) to select the AI models to use and the date range for backtesting or live simulation:

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
  // ... other settings
}
```

#### Environment Variables

Copy the example environment file to `.env` and fill in your API keys:

```bash
cp .env.example .env
```

Edit `.env` and set your model API keys and base URLs (e.g., OPENAI_API_KEY, DEEPSEEK_API_KEY).

### 3. Data Management

Run the daily data management script to update all necessary data (prices, financials, news, etc.):

```bash
python scripts/manage_daily_data.py
```

### 4. Run the Agent

Finally, run the main script to start the AI backtest or simulation defined in your configuration:

```bash
python main.py
```

## 📊 Example Results

You can view the example results in the `data/agent_data/deepseek-reasoner/v2.1版本_价值投资_回测_20240101-20250101/` directory before using the project.

- `position/`: Contains the AI's trading records.
- `log/`: Contains the AI's daily thought processes and reasoning.

## 📂 Project Structure

- `agent/`: Core agent logic.
- `configs/`: Configuration files.
- `data/`: Data storage (cache, logs, results).
- `docs/`: Design and system documentation.
- `scripts/`: Utility scripts (data management, etc.).
- `tools/`: MCP tools for the agent.

## 📄 License

[MIT License](LICENSE)
