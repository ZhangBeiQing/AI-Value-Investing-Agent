# Git 提交规范

## 核心规则

1. **Angular 提交格式 + 简体中文**
   - 按照 Angular 格式来提交每次的修改 commit
   - 但是提交记录应该是简体中文

## Angular 提交格式

```
<type>(<scope>): <subject>
<BLANK LINE>
<body>
<BLANK LINE>
<footer>
```

### Type 类型

- `feat`: 新功能
- `fix`: 修复 bug
- `docs`: 文档变更
- `style`: 代码格式（不影响代码运行的变动）
- `refactor`: 重构（既不是新功能也不是 bug 修复）
- `perf`: 性能优化
- `test`: 添加或修改测试
- `chore`: 构建过程或辅助工具变动

### 示例

```
feat(data): 价格缓存支持增量刷新

为缩短每日刷新耗时，price 缓存改为在既有数据上增量抓取最近窗口，不再每天全量重抓。

主要改动：
- shared_data_access/cache_registry.py 增加重叠窗口校验与增量合并
- 统一 price.csv 的成交量、换手率口径
- 新增 tests/test_price_incremental_refresh.py
```
