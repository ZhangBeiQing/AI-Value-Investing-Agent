# AI选股系统一期项目记录

更新日期：2026-03-17

说明：

1. 本文件只记录已经完成的落地内容、验证记录与历史设计变更。
2. 最新设计思路请看 `docs/selection_system/AI选股系统一期落地设计.md`。

## 0. 当前已完成内容

### 0.1 基础设施

已完成：

1. `master_universe` 基础设施已落地，当前人工维护样例池为 `110` 只股票。
2. 选股系统独立入口已落地：
   - `scripts/manage_selection_system.py`
3. 已建立并实际使用以下目录：
   - `data/universe/`
   - `data/market_state/raw_news/`
   - `data/market_state/board_signals/`
   - `data/market_state/stock_heat/`
   - `data/symbol_memory/`
   - `data/selection_runs/`

### 0.2 独立新闻链

已完成：

1. 独立新闻链已落地并跑通。
2. 当前新闻链产物为：

```text
data/selection_runs/YYYY-MM-DD/
  01_news_candidates.json
  02_news_dedup_decisions.json
  02_news_deduped.json
  03_news_enriched.json
```

3. 当前链路已经实现：
   - 候选新闻采集
   - DeepSeek 去重与筛噪
   - 打开链接提取正文
   - 财经早餐正文增强

### 0.3 独立市场信号链

已完成：

1. 独立市场信号链第一版已落地。
2. 当前已生成过的结构化产物包括：
   - `04_board_signals.json`
   - `05_stock_heat_signals.json`
3. 相关 manifest 已生成并验证过“失败可落盘”。

说明：

1. 这一层是历史第一版输入层。
2. 最新设计已经将其上层职责调整为更强调 `board_heat_state` 深度研究。

### 0.4 面向强模型的输入层

已完成：

1. `06_hot_state_input.md` 第一版已落地。
2. 它证明了当前系统已经能够把新闻链与市场信号链渲染成面向强模型可读的输入层。

说明：

1. 该输入层是重要过渡成果。
2. 但它不是当前最新设计中的最终状态管理方案。

## 1. 已实现命令

当前已实现并使用过的主命令：

```bash
python scripts/manage_selection_system.py --base-dir data init
python scripts/manage_selection_system.py --base-dir data validate-universe
python scripts/manage_selection_system.py --base-dir data show-universe --limit 10
python scripts/manage_selection_system.py --base-dir data run-news --date YYYY-MM-DD --model deepseek-v3.2-exp --batch-size 20
python scripts/manage_selection_system.py --base-dir data run-signals --date YYYY-MM-DD --board-limit 12 --stock-limit 80
python scripts/manage_selection_system.py --base-dir data render-hot-input --date YYYY-MM-DD
```

## 2. 已验证结果

### 2.1 `master_universe`

已验证：

1. `master_universe` 校验通过。
2. 当前股票数为 `110`。

### 2.2 新闻链实跑

已验证：

1. 独立新闻链在 `2026-03-16` 已实跑。
2. 结果如下：
   - 候选新闻 `97`
   - 去重后 `46`
   - 正文增强后 `46`

### 2.3 输入层渲染

已验证：

1. `render-hot-input` 已能基于现有产物输出 `06_hot_state_input.md`。

### 2.4 市场信号链

已验证：

1. 独立市场信号链已验证“失败可落盘”。
2. 即使外部数据源失败，仍能生成：
   - `04_board_signals.json`
   - `05_stock_heat_signals.json`
   - `board_signals/manifest.json`
   - `stock_heat/manifest.json`

## 3. 已知环境与限制现场

截至当前，已确认的环境限制包括：

1. 当前环境对东财 / 雪球相关域名存在 DNS 解析失败。
2. 已受影响的域名包括：
   - `push2ex.eastmoney.com`
   - `emappdata.eastmoney.com`
   - `xueqiu.com`
3. 这会导致 `run-signals` 在某些运行中产物为空。
4. 但相关错误现场已经被完整落盘，可用于后续排查。

## 4. 已落地的早餐增强链

当前新闻链中的 `em_breakfast` 已按以下方式增强：

1. 先读取 `stock_info_cjzc_em` 返回的标题、摘要、链接。
2. 进入东财文章页，抓取 `div#ContentBody` 正文。
3. 识别 `环球市场` 小节中的图片资源，并下载原图到本地缓存目录。
4. 调用 `qwen-doc-turbo` 对该图片做结构化提取。
5. 将以下内容合并回早餐候选与正文增强链：
   - 早餐标题
   - AkShare 摘要
   - 全文正文
   - 环球市场图片提取结果

当前缓存位置：

```text
data/market_state/raw_news_assets/breakfast/{article_id}.html
data/market_state/raw_news_assets/breakfast/{article_id}_global_market.png
data/market_state/raw_news_assets/breakfast/{article_id}_global_market.json
```

当前落地原则：

1. 候选层尽可能高保真，不在这一层压缩早餐正文。
2. 图片 OCR 结果会进入后续新闻增强内容。
3. 如果东财正文或图片提取失败，则降级保留 AkShare 摘要，不阻断整条新闻链。

## 5. 历史设计切换记录

### 5.1 已废弃的旧主方案

以下内容曾作为阶段性设计存在，但现在已经不再是主方案：

1. 先规则打分筛出 `hot_candidates` / `core_candidates`，再让模型做边界判断。
2. 把一期主目标定义为“双策略候选池 + 规则候选筛选器”。
3. 将板块层主接口写成“板块异动接口”。
4. 把新闻链、板块链、个股热度链混成一个规则状态机。

### 5.2 当前确认的设计修正

截至当前，已经明确的设计修正包括：

1. 板块热度层以 `test/test_akshare.py` 中验证过的同花顺行业板块排行抓取逻辑为准。
2. DeepSeek 研究的主对象是“板块”，不是“逐只个股深挖”。
3. 板块相关股票在线索层只作为辅助上下文，或由 DeepSeek 反向推荐。
4. 最终选股不再是单一 `selection_skill`，而是逐步转向：
   - `hot_book`
   - `core_book`
   - `portfolio_orchestrator`

## 6. 下一阶段待落地能力

虽然本文件不讨论完整设计，但从当前落地状态看，下一阶段最关键的空白仍然是：

1. `hot_news_state` 的本地增量融合 skill。
2. 板块热度层的 DeepSeek 深度研究输出。
3. `hot_book_state` 与 `core_book_state` 的正式落地。
4. 顶层 `portfolio_orchestrator`。

这些能力的设计方案见：

- `docs/selection_system/AI选股系统一期落地设计.md`
