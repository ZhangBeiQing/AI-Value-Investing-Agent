---
name: auto-selection-daily-pipeline
description: >
  用于运行每日自动选股流水线。当用户说“开始今天自动选股”、“生成今日选股结果”、
  “运行每日选股流水线”或类似请求时使用。该 skill 会先检查并补齐当天缺失的宏观总结、
  渐进式新闻主题总结、板块热度层、shared context 与 08/09 选股输入文件；当 08/09 输入
  准备完成后，再由主 agent 启动 2 个 subagent，分别基于 08_short_book_input.* 与
  09_long_book_input.* 生成当天短期池和长期池选股结果，merge后生成最终的短期和长期深研队列。
---

# 1. 适用场景

- 用户要“开始今天自动选股”
- 用户要“生成今天的短期池/长期池选股结果”
- 用户要“自动补齐选股输入后再做今日选股”
- 用户要在缺少当天宏观总结、新闻主题状态或 08/09 输入文件时，一次性完成补齐和选股

# 2. 日期与目录约定

- 若用户明确提供日期，使用该日期，格式必须为 `YYYY-MM-DD`
- 若用户说“今天”，默认使用当前日期
- 主目录固定为：`data/selection_runs/YYYY-MM-DD/`

本 skill 直接关心的文件：

- `data/macro_economy/YYYYMMDD.md`
- `data/selection_runs/YYYY-MM-DD/03_news_prompt_input.json`
- `data/selection_runs/YYYY-MM-DD/05_board_heat_digest.json`
- `data/selection_runs/YYYY-MM-DD/05_board_heat_state.json`
- `data/selection_runs/YYYY-MM-DD/06_hot_news_state.json`
- `data/selection_runs/YYYY-MM-DD/06_hot_news_state_ops.json`
- `data/selection_runs/YYYY-MM-DD/07_shared_selection_context.md`
- `data/selection_runs/YYYY-MM-DD/08_short_book_input.json`
- `data/selection_runs/YYYY-MM-DD/08_short_book_input.md`
- `data/selection_runs/YYYY-MM-DD/09_long_book_input.json`
- `data/selection_runs/YYYY-MM-DD/09_long_book_input.md`

最终输出：

- `data/selection_runs/YYYY-MM-DD/08_short_book_candidates.json`
- `data/selection_runs/YYYY-MM-DD/09_long_book_candidates.json`
- `data/selection_runs/YYYY-MM-DD/10_candidate_merge.json`
- `data/selection_runs/YYYY-MM-DD/11_deep_research_queue.json`

# 3. Python 环境要求

必须使用项目虚拟环境：

```bash
source /home/zhangbeiqing/venv/ai_stock/bin/activate
```

# 4. 固定工作流

## 阶段 A：检查并补齐缺失输入

### Step 1. 检查宏观总结

目标文件：

- `data/macro_economy/YYYYMMDD.md`

规则：

- 若当天宏观总结存在，直接使用
- 若不存在，必须先按 `daily-macro-summary` skill 更新当天宏观总结
- 生成逻辑与要求复用：`.codex/skills/daily-macro-summary/SKILL.md`

说明：

- 宏观总结是新闻主题状态和后续选股的上游校准器，不要跳过
- 若今天文件缺失，不要试图用昨天文件直接代替

### Step 2. 检查渐进式新闻主题总结

目标文件：

- `data/selection_runs/YYYY-MM-DD/06_hot_news_state.json`
- `data/selection_runs/YYYY-MM-DD/06_hot_news_state_ops.json`

若缺失，需要确保以下输入可用：

- `data/selection_runs/YYYY-MM-DD/03_news_prompt_input.json`
- `data/selection_runs/YYYY-MM-DD/05_board_heat_digest.json`
- `data/selection_runs/YYYY-MM-DD/05_board_heat_state.json`

规则：

- 若当天 `06_hot_news_state.json` 已存在，直接使用
- 若缺失，必须按 `gradual-hot-news-summary` skill 生成
- 生成逻辑与要求复用：`.codex/skills/gradual-hot-news-summary/SKILL.md`

补齐要求：

1. 若 `03_news_prompt_input.json` 不存在，先运行新闻链：

```bash
python scripts/manage_selection_system.py --base-dir data run-news --date YYYY-MM-DD
```

2. 若 `05_board_heat_digest.json` 或 `05_board_heat_state.json` 不存在，先运行板块热度层：

```bash
python scripts/manage_selection_system.py --base-dir data build-board-heat-state --date YYYY-MM-DD
```

3. 再按 `gradual-hot-news-summary` 的输入契约生成当天 `06_hot_news_state.json`

说明：

- 生成 `06` 时，板块层不是附属材料，而是主输入的一部分
- 当天 `05_board_heat_digest.json` 与 `05_board_heat_state.json` 必须和 `06` 配套

### Step 3. 检查 shared context

目标文件：

- `data/selection_runs/YYYY-MM-DD/07_shared_selection_context.md`

规则：

- 若存在，直接使用
- 若缺失，生成：

```bash
python scripts/manage_selection_system.py --base-dir data build-shared-context --date YYYY-MM-DD
```

### Step 4. 生成 08/09 输入文件

目标文件：

- `08_short_book_input.json`
- `08_short_book_input.md`
- `09_long_book_input.json`
- `09_long_book_input.md`

规则：

- 若 4 个文件都已存在，可直接进入下一阶段
- 若任一文件缺失，统一重新生成：

```bash
python scripts/manage_selection_system.py --base-dir data build-candidate-pools --date YYYY-MM-DD
```

重要说明：

- 当前默认口径：`08_short_book_input.*` 与 `09_long_book_input.*` 都要求本地 agent 各输出 `15` 只股票
- `snapshot` 字段不需要单独手工准备
- `build-candidate-pools` 内部会调用选股系统的 snapshot 覆盖逻辑，自动补齐 `master_universe` 的 snapshot 数据
- 因此，“snapshot 字段生成”在本 skill 中默认由 `build-candidate-pools` 隐式完成

## 阶段 B：08/09 准备完成后再启动 2 个 subagent

只有在以下 4 个文件全部存在后，才允许进入 subagent 选股阶段：

- `08_short_book_input.json`
- `08_short_book_input.md`
- `09_long_book_input.json`
- `09_long_book_input.md`

原因：

- 08/09 是选股阶段的专用输入
- 宏观、新闻主题、板块、snapshot 的大上下文已经被压缩进 08/09
- 选股 subagent 不需要再回头重读 01-07 的原始文件

### Step 5. 主 agent 启动短期池 subagent

主 agent 启动 1 个 short-book subagent，任务边界固定为：

- 主输入：
  - `08_short_book_input.json`
  - `08_short_book_input.md`
  - `data/selection_runs/<上一交易日>/08_short_book_candidates.json`（若存在，必须读取）
- 目标输出：
  - `08_short_book_candidates.json`

short-book subagent 的强制要求：

1. 先完整阅读 `08_short_book_input.md`
2. 再完整阅读 `08_short_book_input.json`
3. 再直接回看上一交易日的 `08_short_book_candidates.json`；这一步属于强制连续性检查，不视为回读 01-07 原始中间文件
4. 以 `08_short_book_input.json` 中的 `shared_context_excerpt` 作为共享上下文主来源
5. 不要默认回头重读 `07_shared_selection_context.md` 或更早的原始中间文件
6. 短期池对上一交易日结果只采用“弱先验”口径，必须先判断：昨天的催化今天是强化、兑现中、钝化还是证伪
7. 只有当昨日催化仍在强化或仍在扩散时，昨日入池股票才应优先保留；若昨日只是单日脉冲、今天没有延续，应快速出池
8. 新进票可以大量替换昨日旧票，不要求短期池名单高稳定性；但新进票必须能回答：它为何比被替换的昨日旧票更值得占用今天的 short-book 名额
9. 若需要补充板块细节，只使用：

```bash
python scripts/query_board_snapshot.py --date YYYY-MM-DD --board-name "板块A"
```

10. 若需要补充单股 snapshot，只使用：

```bash
python scripts/query_stock_snapshot.py --date YYYY-MM-DD --symbol 000977.SZ
```

11. 若上一交易日 `08_short_book_candidates.json` 缺失，允许按冷启动口径筛选，但必须在最终分析中显式说明昨日短期池锚点缺失，今天无法做连续性比较
12. 输出必须写到：

- `data/selection_runs/YYYY-MM-DD/08_short_book_candidates.json`

### Step 6. 主 agent 启动长期池 subagent

主 agent 启动 1 个 long-book subagent，任务边界固定为：

- 主输入：
  - `09_long_book_input.json`
  - `09_long_book_input.md`
  - `data/selection_runs/<上一交易日>/09_long_book_candidates.json`（若存在，必须读取）
- 目标输出：
  - `09_long_book_candidates.json`

long-book subagent 的强制要求：

1. 先完整阅读 `09_long_book_input.md`
2. 再完整阅读 `09_long_book_input.json`
3. 再直接回看上一交易日的 `09_long_book_candidates.json`，并把它作为今日长期池筛选的主锚；这一步属于强制连续性检查，不视为回读 01-07 原始中间文件
4. 以 `09_long_book_input.json` 中的 `shared_context_excerpt` 作为共享上下文主来源
5. 不要默认回头重读 `07_shared_selection_context.md` 或更早的原始中间文件
6. 长期池必须把昨日入池结果视为强先验，默认先问：昨天为什么选它，今天这些理由是否仍成立
7. 若昨日逻辑仍成立，优先保留并只调整排序；若逻辑加强，升级优先级；若逻辑弱化但未证伪，降级观察；若逻辑被证伪，再移出池子
8. 新进票必须回答：它为什么比某个昨日老票更值得占用今天的 long-book 名额
9. 若需要补充板块或 snapshot，优先使用查询脚本，而不是回退到原始中间文件
10. 若上一交易日 `09_long_book_candidates.json` 缺失，允许按冷启动口径筛选，但必须在最终分析中显式说明昨日长期池主锚缺失，今天无法做连续性比较
11. 输出必须写到：

- `data/selection_runs/YYYY-MM-DD/09_long_book_candidates.json`

### Step 7. subagent 任务口径

两个 subagent 都只负责“选股”，不负责重新生成上游输入。

short-book subagent 关注：

- 主题强化
- 板块确认
- 公告催化
- 量价与流动性
- 近端风险

long-book subagent 关注：

- 公司质量
- 增长持续性
- 估值赔率
- 跨季度 thesis
- 财报/治理风险

两个 subagent 都必须遵守各自 markdown 文件中的字段契约，输出唯一 JSON 对象。

## 阶段 C：默认执行 merge 收尾

当 `08_short_book_candidates.json` 和 `09_long_book_candidates.json` 都生成成功后，主 agent 默认必须继续生成 merge 结果：

```bash
python scripts/manage_selection_system.py --base-dir data merge-candidates --date YYYY-MM-DD
```

输出：

- `10_candidate_merge.json`
- `11_deep_research_queue.json`

说明：

- 根据一期原始设计，`10_candidate_merge.json` 与 `11_deep_research_queue.json` 属于主流程标准产物
- 不应把 08/09 候选结果视为流程终点；标准终点应是 merge 与 deep research queue 都已生成

# 5. 主 agent 的执行顺序

推荐严格按以下顺序执行：

1. 解析日期
2. 检查 `data/macro_economy/YYYYMMDD.md`
3. 缺失则调用 `daily-macro-summary`
4. 检查当天 `06_hot_news_state.json`
5. 若缺失，则按 `gradual-hot-news-summary` 所需补齐 `03/05` 后生成 `06`
6. 生成或校验 `07_shared_selection_context.md`
7. 运行 `build-candidate-pools`
8. 确认 `08/09 *_input.json` 与 `08/09 *_input.md` 已存在
9. 启动 2 个 subagent：
   - 一个只处理 short-book
   - 一个只处理 long-book
10. 收集 subagent 结果
11. 运行 `merge-candidates`
12. 确认 `10_candidate_merge.json` 与 `11_deep_research_queue.json` 已生成

# 6. subagent 提示词建议

主 agent 给 short-book subagent 的任务应接近：

```text
请只基于 data/selection_runs/YYYY-MM-DD/08_short_book_input.md 和
data/selection_runs/YYYY-MM-DD/08_short_book_input.json 完成今日 short book 选股。
另外，必须直接回看上一交易日的 08_short_book_candidates.json，判断昨日催化今天是强化、兑现中、钝化还是证伪；
这一步属于连续性检查，不属于回读 07 或更早的原始中间文件。
并把结果写入 data/selection_runs/YYYY-MM-DD/08_short_book_candidates.json。
除非需要补充板块或 snapshot 细节，否则不要回读 07 或更早的中间文件。
```

主 agent 给 long-book subagent 的任务应接近：

```text
请只基于 data/selection_runs/YYYY-MM-DD/09_long_book_input.md 和
data/selection_runs/YYYY-MM-DD/09_long_book_input.json 完成今日 long book 选股。
另外，必须直接回看上一交易日的 09_long_book_candidates.json，并把它作为今日 long book 的主锚；
默认先问昨天为什么选它、今天这些理由是否仍成立。
并把结果写入 data/selection_runs/YYYY-MM-DD/09_long_book_candidates.json。
除非需要补充板块或 snapshot 细节，否则不要回读 07 或更早的中间文件。
```

# 7. 强制约束

- 不要在 08/09 生成前就启动选股 subagent
- 不要让 short-book subagent 负责 long-book，反之亦然
- 不要在选股 subagent 阶段默认重读 01-07 原始文件
- 不要把“直接回看上一交易日 08/09 候选池”误判成回读 01-07 原始中间文件；这是强制连续性检查步骤
- 缺少宏观总结时，不要跳过 `daily-macro-summary`
- 缺少 `06_hot_news_state.json` 时，不要跳过板块热度层
- 不要手工拼接 08/09；统一走 `build-candidate-pools`
- 不要把 08/09 的输入准备和最终选股混在一个超长上下文里完成；先补齐输入，再分发给 2 个 subagent

# 8. 完成标准

完成标准：

1. 当天宏观总结存在
2. 当天 `06_hot_news_state.json` 存在
3. 当天 `05_board_heat_digest.json` 与 `05_board_heat_state.json` 存在
4. 当天 `08_short_book_input.json/md` 存在
5. 当天 `09_long_book_input.json/md` 存在
6. `08_short_book_candidates.json` 与 `09_long_book_candidates.json` 成功生成
7. `10_candidate_merge.json` 与 `11_deep_research_queue.json` 成功生成
8. 主 agent 向用户明确汇报哪些输入是复用的、哪些是本轮自动补齐的
