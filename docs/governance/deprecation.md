# 弃用策略（Deprecation Policy）

> 版本：T23（依赖 T05/T11）；维护人：Gemini 架构组。

## 目标与适用范围

- 对外发布的 API、配置键、可观测性指标一旦标记 `#[deprecated]`，必须提供**两个发布版本**的过渡期。
- 本策略适用于 `spark-core` workspace 内所有 crate，涵盖库代码、可执行工具与示例。
- 各实现必须同时提供**编译期警告**与**运行时告警**，以确保调用方能在开发与生产环境及时识别风险。

## 版本治理基线

| 发布序列 | 变更动作 | 要求 |
| --- | --- | --- |
| N | 标记弃用 | 在 `since` 字段填入当前版本号，`note` 字段中写明目标移除版本与迁移路径。 |
| N + 1 | 维持兼容 | 禁止移除；若需补充迁移能力，可更新注释或示例。 |
| N + 2 | 可移除 | 在合并前确认对外说明已覆盖并通过 `cargo semver-checks`。 |

- **必填元信息**
  - `since`：首次宣告弃用的版本，使用 `MAJOR.MINOR.PATCH` 格式。
  - `note`：统一采用 `removal: vx.y.z; migration: <建议>; tracking: <链接或任务>` 模式，供 CI 校验。
- 若迁移路径涉及多步骤，可在文档或示例中补充详细流程。

## CI 检查规则

- `make ci-lints` 将调用 `spark-deprecation-lint` 工具，自动扫描仓库中的 `#[deprecated]` 属性。
- 工具会验证以下约束：
  1. 存在 `since = "..."` 字段。
  2. 存在 `note = "..."` 字段，并同时包含 `removal:` 与 `migration:` 关键字。
  3. 禁止使用占位值（例如 `TBD` 或空字符串）。
- 检测失败会以非零退出码终止 CI，并附带文件、行号与修复建议。

## 运行时告警

- 所有弃用 API 必须在首次触发时输出一次 WARN 级别日志，内容至少包含：
  - 弃用符号名称（`deprecation.symbol`）。
  - 生效版本（`deprecation.since`）。
  - 计划移除版本（`deprecation.removal`）。
  - 追踪文档或任务链接（`deprecation.tracking`）。
- 推荐使用 `DeprecationNotice::emit` 辅助函数，确保多次调用仅输出一次告警，并在 `no_std + alloc` 环境下同样可用。
- 对于缺少日志设施的调用场景：
  - `std` 环境回退至 `eprintln!`。
  - 纯 `alloc` 环境可在上层通过宿主提供的告警通道补齐。

## 示例弃用：`common::legacy_loopback_outbound`

- 标注方式：
  ```rust
  #[deprecated(
      since = "0.1.0",
      note = "removal: v0.3.0; migration: use Loopback::fire_loopback_outbound; tracking: T23",
  )]
  pub fn legacy_loopback_outbound(...) { ... }
  ```
- 运行时行为：首次调用会通过 `DeprecationNotice` 输出告警，并附带迁移指引。
- 保留周期：最早可在 `0.3.0` 移除，期间如需扩展迁移方案，须同步更新本文件与示例代码。

## 维护流程

1. 新增弃用 → 更新或新增文档条目，并运行 `cargo fmt` / `cargo clippy` / `spark-deprecation-lint`。
2. 发布说明 → 在版本发布文档中引用对应段落，提醒下游升级计划。
3. 到期移除 → 提交前执行 `cargo semver-checks`，并在迁移指南中声明最终移除版本。

> 若在执行策略过程中遇到例外，请联系架构组评估是否需要延长期或提供 Feature Flag 兼容方案。
