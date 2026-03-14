---
paths:
  - scripts/**
  - services/**
  - shared_data_access/**
  - test/**
  - "*.py"
---

# Testing and Evidence Rules

本规则吸收了 AReaL 的核心经验：先设计 evidence，再写或改代码。

## Evidence-First

- 不要只凭静态阅读判断主链路是否正确，尽量给出运行证据。
- 出 bug 时先找证据，再猜原因。常见证据包括：
  - `run_manifest.json`
  - `logs/` 下的组件日志
  - `latest_status.json`
  - 失败步骤的输入输出文件
- 如果问题复杂，先缩成最小复现，不要一上来全链路硬跑。

## 最小复现优先

- 优先用单股票池、临时 `--base-dir`、单脚本入口定位问题。
- 改 `run_daily_pipeline` 时，优先使用：
  - `python scripts/run_daily_pipeline.py --date YYYY-MM-DD --base-dir data/tmp_<name>`
- 改数据刷新时，如果全量太慢，优先用少量 symbol 验证。

## 修改后最低验证标准

### 改数据访问或数据刷新

- 至少验证目标模块能 import / 运行
- 如影响主链路，再补：
  - `python scripts/manage_daily_data.py`

### 改 `01-04` 产物生成

- 至少验证：
  - `python scripts/run_daily_pipeline.py --date YYYY-MM-DD --base-dir data/tmp_<name>`
- 检查：
  - `run_manifest.json` 状态
  - 目标输出文件是否生成

### 改交易后处理

- 至少验证：
  - `python scripts/run_post_trade.py --date YYYY-MM-DD`
- 检查：
  - `06_execution_log.json`
  - `07_daily_summary.json`
  - `08_history_merge.json`
  - `data/agent_data/{signature}` 下的汇总文件

## 测试风格

- 测试名优先使用 `test_<what>_<condition>_<expected>`
- 先 Arrange，再 Act，再 Assert
- 如果没有自动化测试，最终说明里必须明确写出实际运行了什么命令、验证了什么

## 禁止事项

- 不要把运行产生的临时数据误提交为源码改动
- 不要因为验证慢就完全跳过验证，至少给一个更小范围的证据
- 不要把“代码能 import”当成“功能已正确”的替代

