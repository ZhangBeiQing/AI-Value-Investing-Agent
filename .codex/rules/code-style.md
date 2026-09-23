# 代码风格与日志规范

本规则补充 `pre_commit_rule.md`，约束本仓库在 `skill-only` 架构下的编码与日志实现方式。

## 1. 基本代码风格

- Python 统一使用 4 个空格缩进。
- 变量、函数使用 `snake_case`。
- 类、具名组件、日志组件名使用 `CapWords` / PascalCase。
- 优先显式导入，禁止 `from x import *`。
- 新业务逻辑优先放在 `services/`、`core/`、`shared_data_access/`，不要继续把真实实现塞回历史兼容层。
- 模块对外应提供清晰入口；复杂行为补充类型注解和简短 docstring。

## 2. 设计偏好

- 优先组合而不是深继承。
- `scripts/` 只放 CLI 和参数解析；真实业务逻辑下沉到 `services/`。
- 统一通过 `shared_data_access` 访问行情、财报、股本、公告缓存。

## 3. 日志系统总则

本项目日志适配当前本地 `skill-only` 架构：运行日志按「日期 + 流程」聚合到 `logs/runs/`，调试日志扁平到 `logs/debug/`，不再按模型签名分层。

- 禁止在 `services/`、`core/`、`shared_data_access/` 这类库代码里直接使用 `print` 做运行日志。
- 禁止新增裸 `logging.getLogger(__name__)` 或点路径风格 logger 名称。
- 必须优先使用 `core.logging` 提供的统一入口：
  - `get_logger(...)`
  - `init_component_logger(...)`
  - `init_tool_logger(...)`

允许保留 `print` 的场景：

- CLI 最终结果输出
- demo / 手工调试入口
- 必须直接向终端展示的用户提示

## 4. Logger 命名规则

- Logger 名必须是**具业务含义的 PascalCase 组件名**。
- 推荐：
  - `ManageDailyData`
  - `DailyPipeline`
  - `StockAnalysis`
  - `MacroSummary`
  - `FinancialReport`
  - `TradeSummary`
- 不推荐：
  - `__name__`
  - `shared_data_access.cache_registry`
  - `services.trading.trade_summary`

如果是带 rank / worker 标识的动态名字，使用：

- `[{Component} Rank {N}]`
- 例如：`[StockAnalysis Rank 0]`

## 5. 日志级别约定

- `DEBUG`: 详细跟踪，只用于排障或低频路径。
- `INFO`: 阶段性里程碑、关键状态切换、输入输出摘要。
- `WARNING`: 可恢复问题、降级、跳过、缓存异常但流程继续。
- `ERROR`: 当前步骤失败，需要人工关注。
- `CRITICAL`: 整个运行流程已不可继续。

不要把正常流程刷成 `WARNING`，也不要把真正失败写成 `INFO`。

## 6. 输出与落盘规则

- 统一日志头为 `(AI-Stock)`。
- 控制台日志使用按组件分类的颜色；`WARNING` / `ERROR` 颜色优先级高于组件颜色。
- **运行日志按「日期 + 流程」聚合**：入口脚本在 import services 之前调用
  `core.logging.bootstrap_run_logging_from_argv("<flow>")`（或直接
  `configure_run_logging(flow, run_date)`），把 `AI_STOCK_LOG_DIR` 指向
  `logs/runs/<YYYY-MM-DD>/<flow>/`。同一流程的所有组件共享该目录，并共同追加到一个
  `merged.log`；子进程通过环境变量自动继承。
  - 当前流程名：每日材料准备 = `data_prep`；每日固定股池交易 = `fixed_tracked_trade`。
- **未设定运行目录时**（独立调试 / 临时脚本）落 `logs/debug/`，且不再按模型签名分层：
  - 组件：`logs/debug/<group>/<ComponentName>/<prefix>_<时间戳>.log`
  - 工具：`logs/debug/tools/<tool>/<时间戳>.log`
- 每个 logger 只写：自己的文件 + 所在运行目录的 `merged.log`；不要再为每个组件单独建
  `merged.log`。
- 保留策略：`logs/runs/` 默认只保留最近 14 天（`AI_STOCK_LOG_KEEP_DAYS` 可调），由
  `configure_run_logging` 自动清理；也可手动运行 `python scripts/clean_logs.py`
  （`--include-debug` 清理 debug，`--purge-legacy --yes` 删除旧的分层目录）。

## 7. 新增组件如何接入日志

### 7.1 Tool / 兼容层

```python
from core.logging import init_tool_logger

logger = init_tool_logger("stock_analysis")
```

### 7.2 脚本 / 主入口

```python
from core.logging import init_component_logger

logger = init_component_logger(
    "ManageDailyData",
    group="main_scripts",
    filename_prefix="manage_daily_data",
)
```

### 7.3 普通服务模块

```python
from core.logging import get_logger

logger = get_logger("CacheRegistry")
```

## 8. 颜色注册规则

如果新增的是长期存在的核心组件，需要在 `core/logging.py` 里注册颜色：

- 优先更新 `LOGGER_COLORS_EXACT`
- 如需按前缀匹配，再更新 `LOGGER_PATTERNS`

不要在业务文件里自己写颜色逻辑。

## 9. 迁移原则

- 新改动必须使用新日志入口。
- 旧模块如果本次被修改，顺手迁移到 `core.logging`。
- 不要求一次性把仓库所有历史 `print` 全部清干净，但主链路和新增代码必须遵守本规则。
