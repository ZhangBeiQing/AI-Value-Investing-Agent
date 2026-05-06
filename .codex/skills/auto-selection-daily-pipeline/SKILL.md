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

## 日期语义（必读）

本 skill 所有路径、`run_date`、以及"上一交易日"引用中的 `YYYY-MM-DD`，
都与 `AGENTS.md` 的 `--date` 口径完全一致，指**要分析的交易日**，
即**最近一个已收盘的交易日**，默认 `today - 1`。

- 典型节奏：第二天早上跑昨日收盘数据，`YYYY-MM-DD = 昨天`
- 周一或节假日后的第一个早上应手动传上一个交易日（如周一跑上周五）
- 用户说"今天自动选股"时，默认 `YYYY-MM-DD = today - 1`，不是日历的 `today`
- "上一交易日"指相对于 `run_date` 的前一个交易日，不是相对于日历今天
  - 例：`run_date=2026-04-22` → 上一交易日 = `2026-04-21`（而非 `2026-04-21` 之前再往前退）
- 开始执行前，如果用户没有显式指定日期，先用 `date -d 'yesterday' +%F` 或等价方式推导出目标交易日，并向用户确认一次再继续

## 目录约定

- 日期格式必须为 `YYYY-MM-DD`
- 主目录固定为：`data/selection_runs/YYYY-MM-DD/`（`YYYY-MM-DD` = 要分析的交易日）

本 skill 直接关心的文件：

- `data/macro_economy/YYYYMMDD.md`
- `data/selection_runs/YYYY-MM-DD/03_news_prompt_input.json`
- `data/selection_runs/YYYY-MM-DD/05_board_heat_digest.json`
- `data/selection_runs/YYYY-MM-DD/05_board_heat_state.json`
- `data/selection_runs/YYYY-MM-DD/06_hot_news_state.json`
- `data/selection_runs/YYYY-MM-DD/04_recent_company_announcements.json`
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
- 若不存在，使用最新的宏观总结

说明：

- 宏观总结是新闻主题状态和后续选股的上游校准器，不要跳过

### Step 2. 检查渐进式新闻主题总结

目标文件：

- `data/selection_runs/YYYY-MM-DD/06_hot_news_state.json`

依赖的输入（由 `python scripts/refresh_all_for_date.py` 预先生成，本 skill 不再补跑）：

- `data/selection_runs/YYYY-MM-DD/03_news_prompt_input.json`
- `data/selection_runs/YYYY-MM-DD/05_board_heat_digest.json`
- `data/selection_runs/YYYY-MM-DD/05_board_heat_state.json`

规则：

- 若当天 `06_hot_news_state.json` 已存在，直接使用
- 若缺失，调用 `gradual-hot-news-summary` skill 生成
- 生成逻辑与要求复用：`.codex/skills/gradual-hot-news-summary/SKILL.md`
- 若上游 03/05 文件缺失，直接提示用户回去跑 `refresh_all_for_date.py`，不要自行补跑中国 API 脚本

说明：

- 生成 `06` 时，板块层不是附属材料，而是主输入的一部分
- 当天 `05_board_heat_digest.json` 与 `05_board_heat_state.json` 必须和 `06` 配套

### Step 3. 检查 shared context

目标文件（均由 `python scripts/refresh_all_for_date.py` 预先生成）：

- `data/selection_runs/YYYY-MM-DD/04_recent_company_announcements.json`
- `data/selection_runs/YYYY-MM-DD/07_shared_selection_context.md`

规则：

- 两个文件存在则直接进入下一步
- 若缺失，直接提示用户回去跑 `refresh_all_for_date.py`，不要自行补跑 `build-announcements` / `build-shared-context`

### Step 4. 检查 08/09 输入文件

目标文件（均由 `python scripts/refresh_all_for_date.py` 的 `build-candidate-pools` 步骤预先生成）：

- `08_short_book_input.json`
- `08_short_book_input.md`
- `09_long_book_input.json`
- `09_long_book_input.md`

规则：

- 4 个文件都存在则直接进入下一阶段
- 若任一缺失，直接提示用户回去跑 `refresh_all_for_date.py`，不要自行补跑 `build-candidate-pools`

重要说明：

- 当前默认口径：`08_short_book_input.*` 与 `09_long_book_input.*` 都要求本地 agent 各输出 `15` 只股票
- `snapshot` 字段在 `build-candidate-pools` 执行时由 orchestrator 隐式补齐；skill 本体不再关心

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

### Step 5. 主 agent 先继承上一交易日结果，再启动短期池 subagent

#### Step 5.1 先 cp 上一交易日的短期池结果作为今日起点

启动短期池 subagent **之前**，主 agent 必须先把上一交易日的候选池复制到今天作为"昨天的状态机"：

- 源文件：`data/selection_runs/<上一交易日>/08_short_book_candidates.json`
- 目标文件：`data/selection_runs/YYYY-MM-DD/08_short_book_candidates.json`

执行规则：

1. 若今日的 `08_short_book_candidates.json` 已存在，先读取并向用户确认是覆盖重跑还是跳过本次继承
2. 若源文件存在，用 `cp` 命令整体复制，保留昨日候选池的完整字段结构
3. 若源文件不存在，跳过复制，进入冷启动分支，并在给 subagent 的 prompt 中显式告知"昨日短期池锚点缺失"
4. 复制完成后，今日文件的身份从"昨天的终态"转变为"今天的起始状态"；subagent 会在这个起始状态上做增量修改

设计意图与 `daily-macro-summary` 一致：今天的文件从"继承昨日状态"开始，再做增量更新，而不是每天从零重写。

#### Step 5.2 启动 short-book subagent（在昨日文件副本上做增量修改）

主 agent 启动 1 个 short-book subagent，任务边界固定为：

- 主输入：
  - `08_short_book_input.json`
  - `08_short_book_input.md`
  - `data/selection_runs/YYYY-MM-DD/08_short_book_candidates.json`（已由 Step 5.1 从昨日 cp 过来；若冷启动则不存在）
- 目标输出（原地修改 / 冷启动新建）：
  - `data/selection_runs/YYYY-MM-DD/08_short_book_candidates.json`

short-book subagent 的强制要求：

1. 先完整阅读 `08_short_book_input.md`
2. 再完整阅读 `08_short_book_input.json`
3. 再完整阅读当前今日路径下的 `08_short_book_candidates.json`（若 Step 5.1 已 cp 成功，这就是昨日终态；若未 cp，则进入冷启动）；这一步属于强制连续性检查，不视为回读 01-07 原始中间文件
4. 以 `08_short_book_input.json` 中的 `shared_context_excerpt` 作为共享上下文主来源
5. 不要默认回头重读 `07_shared_selection_context.md` 或更早的原始中间文件
6. 短期池对继承的昨日文件只采用"弱先验"口径，必须逐票判断：昨天的催化今天是强化、兑现中、钝化还是证伪
7. 只有当昨日催化仍在强化或仍在扩散时，昨日入池股票才应保留；若昨日只是单日脉冲、今天没有延续，应快速出池
8. 新进票可以大量替换昨日旧票，不要求短期池名单高稳定性；但新进票必须能回答：它为何比被替换的昨日旧票更值得占用今天的 short-book 名额
9. 修改后必须更新文件内所有日期、`run_date`、时效性字段，使文件的"时间身份"整体切换到今日；不允许出现残留的昨日日期
10. 若需要补充板块细节，只使用：

```bash
python scripts/query_board_snapshot.py --date YYYY-MM-DD --board-name "板块A"
```

11. 若需要补充单股 snapshot，只使用：

```bash
python scripts/query_stock_snapshot.py --date YYYY-MM-DD --symbol 000977.SZ
```

12. 若 Step 5.1 因源文件缺失而未 cp（冷启动），允许按冷启动口径从零筛选，但必须在最终分析中显式说明昨日短期池锚点缺失，今天无法做连续性比较
13. 最终产物必须写回（覆盖）到：

- `data/selection_runs/YYYY-MM-DD/08_short_book_candidates.json`

### Step 6. 主 agent 先继承上一交易日结果，再启动长期池 subagent

#### Step 6.1 先 cp 上一交易日的长期池结果作为今日起点

启动长期池 subagent **之前**，主 agent 必须先把上一交易日的候选池复制到今天作为"昨天的状态机"：

- 源文件：`data/selection_runs/<上一交易日>/09_long_book_candidates.json`
- 目标文件：`data/selection_runs/YYYY-MM-DD/09_long_book_candidates.json`

执行规则：

1. 若今日的 `09_long_book_candidates.json` 已存在，先读取并向用户确认是覆盖重跑还是跳过本次继承
2. 若源文件存在，用 `cp` 命令整体复制，保留昨日候选池的完整字段结构
3. 若源文件不存在，跳过复制，进入冷启动分支，并在给 subagent 的 prompt 中显式告知"昨日长期池主锚缺失"
4. 长期池对昨日结果的继承强度显著高于短期池；默认应保留大多数昨日入池股票，只在逻辑弱化或证伪时才调整

#### Step 6.2 启动 long-book subagent（在昨日文件副本上做增量修改）

主 agent 启动 1 个 long-book subagent，任务边界固定为：

- 主输入：
  - `09_long_book_input.json`
  - `09_long_book_input.md`
  - `data/selection_runs/YYYY-MM-DD/09_long_book_candidates.json`（已由 Step 6.1 从昨日 cp 过来；若冷启动则不存在）
- 目标输出（原地修改 / 冷启动新建）：
  - `data/selection_runs/YYYY-MM-DD/09_long_book_candidates.json`

long-book subagent 的强制要求：

1. 先完整阅读 `09_long_book_input.md`
2. 再完整阅读 `09_long_book_input.json`
3. 再完整阅读当前今日路径下的 `09_long_book_candidates.json`（若 Step 6.1 已 cp 成功，这就是昨日终态；若未 cp，则进入冷启动），并把它作为今日长期池筛选的主锚；这一步属于强制连续性检查，不视为回读 01-07 原始中间文件
4. 以 `09_long_book_input.json` 中的 `shared_context_excerpt` 作为共享上下文主来源
5. 不要默认回头重读 `07_shared_selection_context.md` 或更早的原始中间文件
6. 长期池必须把继承下来的昨日文件视为强先验，默认先问：昨天为什么选它，今天这些理由是否仍成立
7. 若昨日逻辑仍成立，优先保留并只调整排序与理由细节；若逻辑加强，升级优先级；若逻辑弱化但未证伪，降级观察；若逻辑被证伪，再移出池子
8. 新进票必须回答：它为什么比某个昨日老票更值得占用今天的 long-book 名额
9. 修改后必须更新文件内所有日期、`run_date`、时效性字段，使文件的"时间身份"整体切换到今日；不允许出现残留的昨日日期
10. 若需要补充板块或 snapshot，优先使用查询脚本，而不是回退到原始中间文件
11. 若 Step 6.1 因源文件缺失而未 cp（冷启动），允许按冷启动口径从零筛选，但必须在最终分析中显式说明昨日长期池主锚缺失，今天无法做连续性比较
12. 最终产物必须写回（覆盖）到：

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

1. 解析日期：按 Section 2 的日期语义确定 `YYYY-MM-DD = 要分析的交易日`（默认 `today - 1`，用户没显式指定时先推导并向用户确认）
2. 同步推导"上一交易日"的实际日期（用于后续 cp 继承），并向用户确认一次
3. 检查 `data/macro_economy/YYYYMMDD.md`
4. 缺失则调用 `daily-macro-summary`
5. 检查当天 `06_hot_news_state.json`
6. 若缺失，则按 `gradual-hot-news-summary` 所需补齐 `03/05` 后生成 `06`
7. 生成或校验 `07_shared_selection_context.md`
8. 运行 `build-candidate-pools`
9. 确认 `08/09 *_input.json` 与 `08/09 *_input.md` 已存在
10. 继承昨日候选池作为今日起点：
    - `cp` 上一交易日 `08_short_book_candidates.json` 到今日路径（若源存在）
    - `cp` 上一交易日 `09_long_book_candidates.json` 到今日路径（若源存在）
    - 若今日对应文件已存在，先与用户确认是覆盖重跑还是跳过继承
11. 同时并行启动 2 个 subagent，均在继承下来的今日文件副本上做增量修改：
    - 一个只处理 short-book
    - 一个只处理 long-book
12. 收集 subagent 结果（两者均为对今日候选文件的原地覆盖写）
13. 运行 `merge-candidates`
14. 确认 `10_candidate_merge.json` 与 `11_deep_research_queue.json` 已生成

# 6. subagent 提示词建议

主 agent 给 short-book subagent 的任务应接近：

```text
今天的 data/selection_runs/YYYY-MM-DD/08_short_book_candidates.json 已经由主 agent
从上一交易日 (<上一交易日>) 的 08_short_book_candidates.json cp 过来，作为今日的起始状态。
请先完整阅读 data/selection_runs/YYYY-MM-DD/08_short_book_input.md
和 data/selection_runs/YYYY-MM-DD/08_short_book_input.json，
再完整阅读今日路径下已继承的 08_short_book_candidates.json。
然后在该文件上做增量修改：逐票判断昨日催化今天是强化、兑现中、钝化还是证伪，
快速出池已钝化/证伪的票，保留仍在强化的票，并加入新进票；
务必把文件内所有日期与 run_date 整体切换到今日，不允许残留昨日日期。
这一步属于连续性继承，不属于回读 07 或更早的原始中间文件。
最终把修改结果覆盖写回 data/selection_runs/YYYY-MM-DD/08_short_book_candidates.json。
除非需要补充板块或 snapshot 细节，否则不要回读 07 或更早的中间文件。
若今日路径下 08_short_book_candidates.json 不存在（冷启动），按冷启动口径从零构建，
并在最终分析中显式说明昨日短期池锚点缺失。
```

主 agent 给 long-book subagent 的任务应接近：

```text
今天的 data/selection_runs/YYYY-MM-DD/09_long_book_candidates.json 已经由主 agent
从上一交易日 (<上一交易日>) 的 09_long_book_candidates.json cp 过来，作为今日的起始状态。
请先完整阅读 data/selection_runs/YYYY-MM-DD/09_long_book_input.md
和 data/selection_runs/YYYY-MM-DD/09_long_book_input.json，
再完整阅读今日路径下已继承的 09_long_book_candidates.json，并把它作为今日 long book 的主锚。
然后在该文件上做增量修改：默认先问昨天为什么选它、今天这些理由是否仍成立；
逻辑仍成立则保留，加强则升级，弱化未证伪则降级，证伪才出池，并加入新进票；
务必把文件内所有日期与 run_date 整体切换到今日，不允许残留昨日日期。
这一步属于连续性继承，不属于回读 07 或更早的原始中间文件。
最终把修改结果覆盖写回 data/selection_runs/YYYY-MM-DD/09_long_book_candidates.json。
除非需要补充板块或 snapshot 细节，否则不要回读 07 或更早的中间文件。
若今日路径下 09_long_book_candidates.json 不存在（冷启动），按冷启动口径从零构建，
并在最终分析中显式说明昨日长期池主锚缺失。
```

# 7. 强制约束

- 不要在 08/09 生成前就启动选股 subagent
- 不要在没有先 cp 昨日候选池（Step 5.1 / Step 6.1）的情况下直接启动选股 subagent；冷启动除外
- 不要让 short-book subagent 负责 long-book，反之亦然
- 不要在选股 subagent 阶段默认重读 01-07 原始文件
- 不要把"继承昨日 08/09 候选池作为今日起点"误判成回读 01-07 原始中间文件；这是强制连续性继承步骤
- 不要让 subagent 保留昨日日期字段；每次修改都必须把 `run_date` 等时间字段整体切换到今日
- 不要在今日已存在对应候选文件时直接覆盖 cp 而不与用户确认；避免误覆盖用户已经手工编辑过的结果
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
