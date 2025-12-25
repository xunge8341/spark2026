# spark-core

## 职责边界
- 作为全局契约中心，统一定义调用上下文、错误分类、状态机与背压语义，供 `crates/` 下所有运行时、传输与编解码实现复用。
- 向外提供 `no_std + alloc` 友好的基础设施：包括缓冲池、配置系统、可观测性挂钩与任务调度抽象，确保扩展 crate 不直接依赖具体运行时。
- 通过 [`docs/global-architecture.md`](../../docs/global-architecture.md) 中描述的分层架构衔接上层 SDK 与下层传输通道，是契约测试与合规审计的锚点。
- 在可观测性层仅保留 `TraceContext`、`TraceId`、`SpanId`、`AuditTag` 与资源属性等纯类型及键常量，具体的 OpenTelemetry 安装与导出
  逻辑由 [`crates/spark-otel`](../spark-otel) 提供，避免核心 crate 引入第三方运行时依赖。

## 公共接口入口
- [`src/contract.rs`](./src/contract.rs)：公开 `CallContext`、`Budget`、`Deadline` 等运行期契约，同时封装取消、关闭原因与安全上下文。
- [`src/status/mod.rs`](./src/status/mod.rs)：维护 `ReadyState` 状态机、`BusyReason` 与 `RetryAdvice`，用于背压、退避与半关闭流程。
- [`src/error.rs`](./src/error.rs)：提供 `CoreError`、`DomainError`、`ErrorCategory` 及编码常量；所有扩展 crate 需通过 `IntoCoreError` 对齐分类。
- [`src/codec/mod.rs`](./src/codec/mod.rs)：定义 `Codec`/`DecodeOutcome` 接口与 `CodecRegistry` 注册机制，连接编解码扩展与运行时。
- [`src/data_plane/pipeline/mod.rs`](./src/data_plane/pipeline/mod.rs)：以 “Pipeline” 为核心术语收敛请求生命周期，覆盖 `Channel`、`PipelineEvent`、`HandlerRegistry` 等控制平面契约。
- [`src/data_plane/pipeline/initializer.rs`](./src/data_plane/pipeline/initializer.rs)：提供 `PipelineInitializer`、`ChainBuilder` 与初始化管线构建协议，是旧 `Controller`/`Router`/`Middleware` 名称的统一替代入口。
- [`src/host/mod.rs`](./src/host/mod.rs) 与 [`src/runtime/mod.rs`](./src/runtime/mod.rs)：规定宿主生命周期、组件注册与任务调度 API，是 `transport` 与 `sdk` 层实现的基线。

## 状态机与错误域
- `ReadyState` 的转移规则与背压语义需同时满足 [`docs/state_machines.md`](../../docs/state_machines.md) 和 [`docs/graceful-shutdown-contract.md`](../../docs/graceful-shutdown-contract.md)；`spark-core` 中的 `pipeline::default_handlers` 负责在运行时 enforce 该序列。
- 错误分类遵循 [`docs/error-category-matrix.md`](../../docs/error-category-matrix.md) 与 [`docs/error-hierarchy.md`](../../docs/error-hierarchy.md)，`error::codes` 列举所有可复用编码，契约测试会校验实现是否正确映射。
- 背压与预算的约束在 [`docs/resource-limits.md`](../../docs/resource-limits.md) 与 [`docs/retry-policy.md`](../../docs/retry-policy.md) 中记录；`limits::Budget` 与 `retry` 模块提供参考实现。

## 关联契约与测试
- [`crates/spark-contract-tests`](../spark-contract-tests) 使用 `spark-core` 提供的类型驱动黑盒测试，验证传输与编解码实现是否遵循 ReadyState、错误分类与半关闭顺序。
- [`crates/spark-tck`](../spark-tck) 扩展契约测试以覆盖宿主实现，重点围绕 `CallContext` 生命周期与 `Budget` 传播。
- 所有宏与代码生成通过 [`crates/spark-macros`](../spark-macros) 间接依赖 `spark-core`，需先在 `spark-core` 中完成契约定义再向外暴露。

## 集成注意事项
- `spark-core` 对外 re-export `spark_macros::service`，业务代码应按文档统一书写 `use spark_core as spark;`，以便未来演进宏命名空间。
- 安全相关接口（`security`、`transport::quic` 等）仅提供契约，实际实现位于 `transport` 分层，对应的审计要求参见 [`docs/safety-audit.md`](../../docs/safety-audit.md)。
- 若需扩展半关闭语义或引入新的错误分类，需同步更新上述文档与契约测试，避免破坏版本兼容性。
