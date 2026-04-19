# AI Value Investing Agent

[中文版](README_CN.md)

This project is a local `skill-only` AI investment workflow for **A-shares and HK-shares**.

It prepares daily research inputs locally, lets your desktop LLM/agent read those prepared files via a fixed skill flow, and then executes simulated trading with local Python code.

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

#### Prompt Flow Configuration

The active prompt flow is `configs/prompt_flow/skill_flow.json`.

Your local agent should read the generated files under `data/skill_runs/{date}/` instead of calling MCP services.

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

### 4. Build Daily Skill Inputs

Generate the daily skill package for your local agent:

```bash
python scripts/run_daily_pipeline.py --date 2026-03-14
```

This creates:

- `01_global_context.md`
- `02_basic_snapshot_payload.json`
- `03_agent_input.md`
- `04_stock_research/*.md`

### 5. Execute Post-Trade Processing

After your local agent writes `05_decision.json`, run:

```bash
python scripts/run_post_trade.py --date 2026-03-14
```

## 📊 Example Results

You can view the example results in the `data/agent_data/deepseek-reasoner/v2.1版本_价值投资_回测_20240101-20250101/` directory before using the project.

- `position/`: Contains the AI's trading records.
- `log/`: Contains the AI's daily thought processes and reasoning.

## 📂 Project Structure

- `services/`: Core skill-only business services.
- `configs/`: Configuration files.
- `data/`: Data storage (cache, logs, results).
- `docs/`: Design and system documentation.
- `scripts/`: CLI entry points for daily workflow.
- `shared_data_access/`: Unified data access and cache layer.
- `agent_tools/`: Thin legacy compatibility wrappers for a few historical import paths.

## 📄 License

[MIT License](LICENSE)


 等每个账本的 05_decision.json 人工确认后，再分别执行：
  python scripts/run_post_trade.py --date 2026-04-18 --book-type fixed_tracked --signature book-fixed_tracked
  python scripts/run_post_trade.py --date 2026-04-18 --book-type short_book --signature book-short_book
  python scripts/run_post_trade.py --date 2026-04-18 --book-type long_book --signature book-long_book

  python scripts/run_daily_pipeline.py --date 2026-04-18 --max-workers 6
