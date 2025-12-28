# Configuration Files

This directory contains configuration files for the AI-Trader Bench. These JSON configuration files define the parameters and settings used by the trading agents during execution.

## Files

### `default_config.json`

The main configuration file that defines all system parameters. This file is loaded by `livebaseagent_config.py` and contains the following sections:

#### Agent Configuration
- **`agent_type`**: Specifies which agent class to use 
- **`agent_config`**: Agent-specific parameters
  - `max_steps`: Maximum number of reasoning steps per trading decision (default: 30)
  - `max_retries`: Maximum retry attempts for failed operations (default: 3)
  - `base_delay`: Base delay between operations in seconds (default: 1.0)
  - `initial_cash`: Starting cash amount for trading (default: $10,000)

#### Date Range
- **`date_range`**: Trading period configuration
  - `init_date`: Start date for trading simulation (format: YYYY-MM-DD)
  - `end_date`: End date for trading simulation (format: YYYY-MM-DD)

#### Model Configuration
- **`models`**: List of AI models to use for trading decisions
  - Each model entry contains:
    - `name`: Display name for the model
    - `basemodel`: Full model identifier/path
    - `signature`: Model signature for API calls
    - `enabled`: Boolean flag to enable/disable the model

#### Logging Configuration
- **`log_config`**: Logging parameters
  - `log_path`: Directory path where agent data and logs are stored

## Usage

### Default Configuration
The system automatically loads `default_config.json` when no specific configuration file is provided:

```bash
python livebaseagent_config.py
```

### Custom Configuration
You can specify a custom configuration file:

```bash
python livebaseagent_config.py configs/my_custom_config.json
```

### Environment Variable Overrides
Certain configuration values can be overridden using environment variables:
- `INIT_DATE`: Overrides the initial trading date
- `END_DATE`: Overrides the end trading date

## Configuration Examples

### Minimal Configuration
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

### Multi-Model Configuration
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

## Notes

- Configuration files must be valid JSON format
- The system validates date ranges and ensures `init_date` is not greater than `end_date`
- Only models with `enabled: true` will be used for trading simulations
- Configuration errors will cause the system to exit with appropriate error messages
- The configuration system supports dynamic agent class loading through the `AGENT_REGISTRY` mapping
