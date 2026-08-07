# 单日自动调度

实验交易日序列中，前 `N-1` 日是决策日，最后一日是最终按收盘价计价日。
不要为最后一日创建无法在区间内成交的订单。

交易日序列必须使用 `resolve_experiment_trading_dates()` 的结果。该函数优先读取
`000001.IDX` 实际行情日期，不能完整覆盖时使用 `SSE` 交易所日历；不要自行枚举
自然日或工作日。`execution_date` 必须等于该序列中 `decision_date` 后的紧邻日期。

## 长耗时命令执行

公告同步与审计、财报准备、PDF 转 Markdown、`prepare-day`、批量重建研究包等命令
可能持续数十分钟。主 Agent执行这些命令时必须遵守：

1. 若命令工具支持完整超时参数，将超时设为 `3600` 秒（`3600000` 毫秒）。这是
   最大允许运行时间，不是固定等待时间；命令运行 20 分钟完成时应立即返回。
2. 若命令工具采用 session、PTY、yield 或 poll 机制，保持同一个运行会话并持续
   轮询直到进程退出，累计最多等待 1 小时。初次调用只返回 session、暂时没有新
   输出或工具短暂 yield，都不代表命令失败，不得因此重启同一命令。
3. 长命令执行期间至少每 60 秒检查一次会话；仍在运行时向用户简短报告“命令仍
   在运行”和已运行阶段/时长，不能让用户长时间无法判断程序是正常运行还是卡住。
4. 执行长命令时保留原始标准输出和标准错误，禁止在命令末尾追加
   `2>&1 | tail -40`、`| tail`、`| head`、`| grep` 等截断或过滤实时输出的管道，
   也不要把命令静默放入后台。需要排查时，应在命令结束后另行读取日志文件。
5. 命令退出后记录退出码，并读取其状态文件、checkpoint 或输出文件确认真实结果；
   不能只根据最后几行文本判断成功。

## 输入准备

1. 运行 `prepare-day --build-missing-inputs`。
2. 读取 `00_prepare_status.json`。
3. 检查 `announcement_preparation`。单股公告准备失败会记录 warning 并在后续
   日期重试；04 会暂时使用该股已有的本地审计公告，不能用未来公告补空。
4. `status == needs_financial_research` 时，执行返回的财报准备命令和完整
   `financial-report-summary` 多角色闭环，使用 `--as-of-date` 注册，再重跑
   返回的 `resume_prepare_day_command`。财报门禁通过以前禁止进入 P0。
5. 其他 `status != ready` 时停止该日。
6. 读取当日 `run_manifest.json`，确认股票集合由冻结固定池、12 长期量化候选和持仓股构成。

## P0

主 Agent只读取 00、01、02、03 和可选市场文件，按 `03_agent_input.md` 建立 P0。不要打开 04。

无 P0 时不凭空创建个股 verdict，运行：

```bash
python scripts/manage_fixed_tracked_backtest.py no-trade-day \
  --experiment-id {experiment_id} \
  --date {date} \
  --reason "P0 为空，组合层未发现需要深度辩论的交易机会"
```

它只在回测实验目录生成显式空 `05_decision.json`；正式日常 05 的非空契约不变。

## 辩论

对每只 P0：

1. `manage_debate.py prepare`，`--base-dir` 指向实验 `skill_runs`。
2. Bull/Bear 同时开始，各自读取 00、共同规则、本股 04 和角色 reference。**Bull 与 Bear 是每只 P0 固定的两个 subagent 会话，保存其 task_id，整个辩论流程必须复用同一会话，不得在任一阶段重新创建全新 Agent。**
3. Opening 完成后**唤醒原 Agent 的同一会话**（复用 task_id）发送 rebuttal follow-up：反驳 Bear 用原 Bull 会话，反驳 Bull 用原 Bear 会话。subagent 返回空或未落盘时，用原 task_id 续写，不立即新起 Agent。
4. 三名 Juror 相互独立，不读取彼此 ballot。
5. `aggregate` 使用实验 `position.jsonl` 中当日实际持股数。
6. Finalizer 服从 `resolved_action`，生成完整股票 verdict。
7. `validate --require-verdict`。

不同股票可按平台上限并行；同一股票阶段不能乱序。

## 合并和执行

所有 P0 校验后合并 05。回测不暂停等待用户确认。

后处理必须显式提供 `--backtest-root` 和 `--execution-date`。缺少这两个参数时禁止调用，避免落入真实分支。

成功 06 是当日成交幂等凭证。不得删除成功 06 后重跑同一订单。

## 扩展实验

当用户要求从原结果继续到更晚日期时，先执行 `manage_fixed_tracked_backtest.py
extend`，再执行 `status`。不要创建第二个实验，也不要复制或重命名原实验目录。
原结束日扩展后会成为新的决策日；从 `status.progress.next_date` 继续本文件的单日
流程，新增区间完成后重新 `finalize`。
