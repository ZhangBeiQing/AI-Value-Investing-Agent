# Configuration Files

This directory now serves the local `skill-only` workflow. The historical `default_config.json` has been removed together with the old MCP/agent runtime.

## Active Files

### `stock_pool.py`

Defines the stock pool used by the daily refresh and pipeline scripts.

### `prompt_flow/skill_flow.json`

Defines the prompt and decision flow consumed by the local desktop agent after `scripts/run_daily_pipeline.py` generates the daily package.

### `.env`

Runtime defaults are now expected to come from environment variables instead of a monolithic JSON config. Common values include:

- `SIGNATURE` or `DEFAULT_SIGNATURE`
- `INITIAL_CASH` or `INIT_CASH`
- `ALLOW_BUY_EXECUTION`: disabled by default. `BUY` actions are only executed when this is explicitly set to `true/1/yes/on`; otherwise the execution layer downgrades them to research-only outcomes with no automatic order placement.
- model API keys and base URLs

## Runtime Contract

The stable daily workflow is:

```bash
python scripts/manage_daily_data.py
python scripts/run_daily_pipeline.py --date YYYY-MM-DD
python scripts/run_post_trade.py --date YYYY-MM-DD
```

The generated input package lives under `data/skill_runs/{date}/`.

## Notes

- New project-level defaults should prefer environment variables or small focused config files.
- Do not reintroduce a catch-all runtime JSON config unless there is a clear operational need.
- `skill_flow.json` is the only active prompt-flow config in the current architecture.
