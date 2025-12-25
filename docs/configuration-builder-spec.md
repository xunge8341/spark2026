# ConfigurationBuilder 规范（T08）

## 目标（Why）

- **可验证**：构建流程在校验、加载、收尾三个阶段输出结构化结果，支持随机错误注入实验统计覆盖率。
- **可快照**：构建成功后立即生成脱敏快照，用于审计、回放与实验复现。
- **可脱敏打印**：错误、报告、快照的 `Debug/Display` 输出不会泄露加密字段或敏感值。

## 核心契约（What）

| 能力 | 类型 | 说明 |
| --- | --- | --- |
| 构建结果 | [`BuildOutcome`](../crates/spark-core/src/governance/configuration/builder.rs) | 封装 `handle`、`initial`、`report` 三元组。 |
| 校验报告 | [`ValidationReport`](../crates/spark-core/src/governance/configuration/builder.rs) | 逐条记录检查项，提供 `passed_count/failed_count` 指标。 |
| 构建错误 | [`BuildError`](../crates/spark-core/src/governance/configuration/builder.rs) | 暴露 `kind/stage/report/cause`，`Display` 仅输出脱敏摘要。 |
| 快照 | [`ConfigurationSnapshot`](../crates/spark-core/src/governance/configuration/snapshot.rs) | 包含 Profile 概要、Layer 元数据与脱敏后的键值条目。 |

### `BuildErrorKind`

| 枚举值 | 触发条件 | 默认修复建议 |
| --- | --- | --- |
| `MissingProfile` | 未调用 `with_profile` | 补充 ProfileDescriptor。 |
| `MissingSources` | 未注册数据源 | 注册至少一个 `ConfigurationSource`。 |
| `ProfileExtendsDuplicate` | `extends` 列表出现重复 | 调整 Profile 拓扑，确保唯一。 |
| `ProfileExtendsSelfReference` | `extends` 包含自身 | 移除自引用，避免无限递归。 |
| `SourceLoadFailure { index }` | 第 `index` 个数据源加载失败 | 结合 `cause()` 查看 `ConfigurationError` 并处理。 |

### 脱敏策略

- 若 `ConfigMetadata.encrypted == true` 或值为 `Binary`，快照中统一输出 `"[REDACTED]"`。
- `BuildError` 和 `BuildReport` 的 `Display` 仅返回概要统计信息。
- `ConfigurationSnapshot::to_json()` 会转义字符串并去除敏感值，可直接写入审计系统。

## 使用示例（How）

```rust
use spark_core::configuration::{
    BuildErrorKind, ConfigurationBuilder, ConfigurationLayer, ConfigurationSnapshot,
    ConfigKey, ConfigMetadata, ConfigScope, ConfigValue, ProfileDescriptor, ProfileId,
    ProfileLayering, SourceMetadata,
};

let profile = ProfileDescriptor::new(
    ProfileId::new("prod"),
    vec![ProfileId::new("base")],
    ProfileLayering::BaseFirst,
    "production profile",
);

let layer = ConfigurationLayer {
    metadata: SourceMetadata::new("bootstrap", 0, None),
    entries: vec![
        (
            ConfigKey::new("runtime", "token", ConfigScope::Global, "api token"),
            ConfigValue::Text(
                "secret-token".into(),
                ConfigMetadata { encrypted: true, ..ConfigMetadata::default() },
            ),
        ),
    ],
};

let mut builder = ConfigurationBuilder::new().with_profile(profile);
builder.register_source(Box::new(StaticSource::new(vec![layer])))?;

let outcome = builder.build()?;
println!("{}", outcome.report); // 输出概要信息
println!("{}", outcome.report.snapshot().to_json()); // 可直接落盘
```

> 若构建失败，可通过 `err.kind()` 与 `err.stage()` 快速定位问题，再配合 `err.report().findings()` 查看已执行的检查项。

> 序列化指引：`ConfigKey` 与 `ConfigValue` 不再直接实现 `serde`，若需结构化输出，可复用 `ConfigurationSnapshot::to_json()`、`AuditChangeEntry`
> （审计链路）等辅助类型，这些类型在内部完成格式转换并保持公共 API 的框架无关性。

## 快照文件（示例）

仓库根目录的 [`snapshots/configuration-example.json`](../snapshots/configuration-example.json) 展示了一个脱敏后的快照，可用于对照输出格式与字段含义。
