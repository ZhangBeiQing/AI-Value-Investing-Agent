# Git 提交规范

## 核心规则

1. **不同主题修改分批次提交**
   - 有时候我们改了很多代码，一直没提交导致代码改动很多。此时需要根据代码的不同修改主题分多次提及，
   这样改动记录才能清晰，如果都是围绕某一主题的修改，则直接全部提及即可

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