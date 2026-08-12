# JSON 书写指南（所有写 JSON 的角色必读）

本文档适用于所有需要落盘 JSON 的角色：Bull / Bear / Juror / Finalizer，以及任何需要写
`opening.json`、`rebuttal.json`、`ballot.json`、`stock_verdict.json` 或类似文件的 subagent。

写入任何 JSON 文件前，先读本指南；写完必须用命令行校验（见「提交前强制自检」）。

## 为什么总在 JSON 上出错

本项目所有 JSON 由 Python `json.load` 解析，它对语法极其严格：
**任何一处非法字符都会导致整份文件解析失败**，下游聚合脚本（`manage_debate.py aggregate`、
`validate`、`merge_subagent_decisions.py`）无法读取，从而拖慢整个辩论流程。

## 高频错误清单（按历史出现频率排序）

### 1. 字符串内部未转义的双引号（最常犯）

JSON 字符串值里，凡是要写「引号」必须用 **英文双引号 `\"`**，不能直接写裸的 `"`。

错误（`"卖得多亏得多"` 里的裸引号会提前结束字符串，报 `Expecting ',' delimiter`）：

```json
{ "reason": "空头称\"卖得多亏得多\"，但证据不足" }
```

上面其实是好的。真正的错误写法是把内容里的引号写成中文引号或裸 ASCII 引号：

```json
{ "reason": "空头说"卖得多亏得多"，但证据不足" }
```

正确做法有两种，任选其一：

- **方法 A（推荐）**：正文用中文引号「」或""，不与 JSON 语法冲突：

  ```json
  { "reason": "空头说「卖得多亏得多」，但证据不足" }
  ```

- **方法 B**：必须用 ASCII 引号时，加反斜杠转义：

  ```json
  { "reason": "空头说\"卖得多亏得多\"，但证据不足" }
  ```

### 2. 把 JSON 写进 markdown 代码块或加包装

错误（文件内容被 ```json 包裹，或外层包了对话文本）：

```text
```json
{ "action_type": "FLAT" }
```
```

正确（文件内容就是纯 JSON，无任何 markdown、无代码块标记、无前后注释）：

```json
{ "action_type": "FLAT" }
```

### 3. 多加了 schema 不允许的字段

每个输出文件都有严格字段约束，不能自由增删字段：

- `ballot.json` 新产物只允许 `action_type` / `action_num` / `price_impression` / `reason` 四字段；
- `rebuttal.json` 的每个 `rebuttals[i]` 只允许 `original_argument` / `rebuttal` 两字段；
- `stock_verdict.json` 只允许 schema 声明的字段；当前新 verdict 使用 16 个字段，其中包含 2 个仓位与数量诊断字段。

错误示例（多加了 `is_rebutted`）：

```json
{ "original_argument": "...", "rebuttal": "...", "is_rebutted": true }
```

正确：

```json
{ "original_argument": "...", "rebuttal": "..." }
```

### 4. 结构套用错位（把别的文件的 schema 套到本文件上，弱模型高发）

每个文件类型有**独立**的顶层结构，不能把「相似文件」的模板照搬过来：

| 文件 | 顶层结构 | 元素类型 |
|---|---|---|
| `opening.json` | `{ "arguments": [...] }` | **字符串**数组 |
| `rebuttal.json` | `{ "rebuttals": [...] }` | **对象**数组，每个对象含 `original_argument` + `rebuttal` |
| `ballot.json` | `{ "action_type", "action_num", "price_impression", "reason" }` | 纯字段对象 |

错误（把 opening 的纯字符串数组当成 rebuttal 写，或把对方论点和反驳拼成一条字符串）：

```json
{
  "rebuttals": [
    "对方论点一……（反驳内容直接拼在这里）",
    "对方论点二……（反驳内容直接拼在这里）"
  ]
}
```

正确（每个元素是一个对象，`original_argument` 原样引用对方一条论点，`rebuttal` 写对应反驳，条目与你要反驳的对方论点一一对应）：

```json
{
  "rebuttals": [
    {
      "original_argument": "对方第一条论点的原文",
      "rebuttal": "对第一条论点的反驳"
    },
    {
      "original_argument": "对方第二条论点的原文",
      "rebuttal": "对第二条论点的反驳"
    }
  ]
}
```

写文件前先确认本文件类型的 schema（参考对应角色 reference 里的模板），不要凭对 `opening.json` 等相似文件的记忆套用。

### 5. 字符串值里内嵌 JSON 对象/数组（双编码）

错误（把对象序列化成字符串再塞进字符串字段）：

```json
{ "reason": "{\"x\": 1}" }
```

正确（对象直接作为字段值，不要转成字符串）：

```json
{ "court": { "pro": ["..."], "con": ["..."] } }
```

### 6. 尾逗号（trailing comma）

JSON 不允许在最后一个元素后写逗号：

错误：

```json
{ "action_type": "FLAT", "price_impression": "合理", }
```

正确：

```json
{ "action_type": "FLAT", "price_impression": "合理" }
```

### 7. 枚举值写错或写自定义词

- `action_type` 只能取：`BUY` / `SELL` / `HOLD` / `FLAT`
- `price_impression` 只能取：`明显低估` / `偏低估` / `合理偏低估` / `合理` / `合理偏贵` / `偏贵` / `明显高估` / `泡沫`

不要写「持有」「观望」「Hold on」「略低」等自定义词，也不要写中文动作名。

### 8. 缩进/全角符号混入

- 逗号、冒号、花括号、方括号必须用**英文半角**字符；
- 数字和 `true` / `false` / `null` 不要带引号；
- 避免用全角逗号 `，`、全角冒号 `：`、全角引号 `“”`（全角引号本身合法，但容易手滑成裸 ASCII 引号，见第 1 条）。

## 正确写法模板

### ballot.json

```json
{
  "action_type": "FLAT",
  "action_num": 0,
  "price_impression": "合理",
  "reason": "支持该动作和价格印象的核心证据权衡，解释两者关系。引用事实时用「中文引号」或转义的英文引号。"
}
```

### opening.json（Bull/Bear）

```json
{
  "arguments": [
    "第一个完整理由，直接写清事实、推断、日期和来源。",
    "第二个完整理由。"
  ]
}
```

### rebuttal.json

```json
{
  "rebuttals": [
    {
      "original_argument": "被反驳的原始论点原文",
      "rebuttal": "对该论点的反驳"
    }
  ]
}
```

### stock_verdict.json

严格按 `configs/prompt_flow/fixed_tracked/stock_decision.schema.json` 的当前字段输出，
参考 `stock_decision.example.json` 的结构，但不复制其中的示例内容。

## 提交前强制自检（每份文件必做）

写完任何 JSON 后，必须使用先验证json的格式：

```bash
/home/zhangbeiqing/venv/ai_stock/bin/python -c "import json; d=json.load(open('<你的文件绝对路径>')); print('JSON OK', type(d).__name__)"
```
失败则修复后重新校验。不要回传一个自己都没验证过的 JSON。

额外检查：
- 文件是否被 markdown 代码块或任何非 JSON 文本包裹；
- 顶层结构是否符合本文件类型（`opening` 是字符串数组、`rebuttal` 是对象数组、`ballot` 是纯字段对象），不要把别的文件模板套过来；
- 字段名和字段数是否符合对应角色的唯一输出要求；
- 枚举值是否在允许范围内；
- 字符串内是否含未转义的裸 ASCII 引号（建议正文一律用「」中文引号规避）。
