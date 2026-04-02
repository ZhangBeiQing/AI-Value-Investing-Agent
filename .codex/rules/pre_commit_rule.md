# Git 提交规范

## 核心规则

1. **只添加本次修改相关的文件**
   - 只 `git add` 和 `git commit` 本次修改产生的代码、配置文件等
   - 观察.gitignore，部分非本次修改产生的文件，尤其是一些如csv, log，或者test_xxx.py或者tmp_xxx等文件，没有被忽略但你感觉不应该跟踪，此时你应该停下来询问开发者是否要跟踪这些非本次修改并且疑似运行中间过程文件

2. **Angular 提交格式 + 简体中文**
   - 按照 Angular 格式来提交每次的修改 commit
   - 但是提交记录应该是简体中文

3. **新增修改等用户确认后AI才能主动commit提交，禁止每次修改完没有给用户评审就自动提交**

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
feat(roi): 实现基于分位数的统一裁剪尺寸

为 VLM 输入实现基于分位数的 bbox 归一化，替代黑色填充方案。

主要改动：
- 添加 compute_percentile_size() 计算目标尺寸
- 添加 normalize_bbox_to_target() 调整 bbox 到统一尺寸
- 在 BBox 类中添加 with_size() 和 center_x/center_y 属性
- 在 engine 中实现两遍处理：先收集 bbox，再用统一尺寸裁剪

BREAKING CHANGE: ROI 裁剪尺寸现在使用基于分位数的方案