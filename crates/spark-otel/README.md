# spark-otel

## 职责边界
- 将 `spark-core` 的可观测性契约（Tracing、Budget、ReadyState）映射到 OpenTelemetry 生态，提供跨语言的链路追踪互操作。
- 提供安装与运行时钩子，帮助宿主在不中断主流程的情况下输出 Span、指标与事件，符合 [`docs/observability-contract.md`](../../docs/observability-contract.md)。
- 维护 Trace Context 与 W3C 标准的一致性，确保 `CallContext` 与外部系统的标识保持同步。
- 依赖 `spark-core` 暴露的 `TraceId`、`SpanId`、`AuditTag`、`ResourceAttr` 等纯类型完成映射，核心 crate 本身不再绑定 OpenTelemetry
  运行时依赖。

## 公共接口入口
- [`src/lib.rs`](./src/lib.rs)：集中实现 `install`、W3C TraceContext 互转、`HandlerSpanTracer` 注册与导出器集成等核心逻辑。
- `resource_from_attrs`：将 `spark-core` 的 `ResourceAttrSet` 映射为 OpenTelemetry `Resource`，保持资源标签的一致性。

## 状态机与错误域
- 观测链路默认不引入新的 ReadyState，除非检测到导出器阻塞或预算耗尽；这些状态需遵循 [`docs/state_machines.md`](../../docs/state_machines.md) 中的背压定义。
- 安装与运行期错误通过 `spark_otel::Error` 与 `HandlerTracerError` 表达，并映射到 `ErrorCategory::ObservabilityFailure` 或 `ImplementationError`。
- 对 TLS/QUIC 安全事件的采样需遵守 [`docs/safety-audit.md`](../../docs/safety-audit.md) 的记录要求。

## 关联契约与测试
- 与 [`crates/spark-core`](../spark-core) 的 `observability` 模块协同，确保预算与 Span 生命周期匹配。
- 契约测试通过 [`crates/spark-contract-tests`](../spark-contract-tests) 的 observability 主题运行，验证 ReadyState 与错误分类保持一致。
- 集成示例可参考 [`docs/observability`](../../docs/observability) 目录下的部署与调优指南。
- 需启用 `test-util` Feature 才能运行带内存导出器的集成测试，例如：`cargo test -p spark-otel --features test-util`。

## 集成注意事项
- 安装时需在应用入口调用 `spark_otel::install`，并保证只执行一次；重复调用会返回错误以避免全局状态污染。
- 观测预算来源于 `CallContext::budget(BudgetKind::Observability)`，当导出器阻塞时会触发 `ReadyState::BudgetExhausted`，调用方应准备降级策略。
- 在 `no_std` 环境下暂未提供完整实现，需关注后续在 [`docs/no-std-compatibility-report.md`](../../docs/no-std-compatibility-report.md) 中的演进计划。
