# Configuration Files

This directory now serves the local `skill-only` workflow. The historical `default_config.json` has been removed together with the old MCP/agent runtime.

## Active Files

### `stock_pool.py`

Defines the stock pool used by the daily refresh and pipeline scripts.

### `prompt_flow/fixed_tracked/`

Defines the active fixed_tracked prompt sources:

- `investment_policy.md` is the single source of the core investment strategy shared by every fixed_tracked agent
- `investment_policy.md` + `main_policy.md` generate the main-agent `03_agent_input.md`
- `investment_policy.md` + `configs/research/web_research_policy.md` + `stock_analysis_policy.md` generate `03_stock_analysis_input.md` for stock debate roles
- `stock_decision.schema.json` defines the final per-stock verdict
- `stock_decision.example.json` gives the finalizer a complete schema-valid output example

`prompt_flow/skill_flow_short_book.json` remains active for short_book. The root `skill_flow.json` and `skill_flow_long_book.json` remain only for compatibility and historical reference.

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
- No extra `manifest.json` is used; the pipeline code explicitly maps each prompt file to its generated artifact.
