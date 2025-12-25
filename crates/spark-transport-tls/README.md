# spark-transport-tls

## 职责边界
- 在 `spark-transport-tcp` 基础上提供 TLS 1.3 通道，保证握手、会话恢复与加密数据流符合 `spark-core::transport::channel` 契约。
- 继承 `CallContext` 的取消、截止、预算语义，并更新 `SecurityContextSnapshot` 以供观测与审计链路使用。
- 提供证书热更新与会话缓存管理，确保长连接服务在不中断流量的情况下轮换密钥。

## 公共接口入口
- [`src/lib.rs`](./src/lib.rs)：导出 `TlsAcceptor`, `TlsChannel` 等核心类型。
- [`src/acceptor.rs`](./src/acceptor.rs)：包装 `rustls` 接受器并连接到 `TcpServerChannel`，处理握手与 ALPN。
- [`src/channel.rs`](./src/channel.rs)：实现加密数据流的 `poll_ready`、`read`、`write`、`close_graceful` 与 `close_force`。
- [`src/hot_reload.rs`](./src/hot_reload.rs)：基于 `ArcSwap` 提供证书与密钥的热更新能力。
- [`src/error.rs`](./src/error.rs)：定义 `TlsError`/`TlsHandshakeError` 并映射到 `ErrorCategory`。

## 状态机与错误域
- 握手阶段的 ReadyState 映射：等待证书/OCSP → `Pending`，会话缓存饱和 → `Busy`，需要退避 → `RetryAfter`。
- 错误分类遵循 [`docs/error-category-matrix.md`](../../../docs/error-category-matrix.md)：
  - 证书/协议失败 → `SecurityViolation`
  - 资源耗尽 → `ResourceExhausted`
  - 内部 bug → `ImplementationError`
- 半关闭顺序与 `CloseNotify` 行为参照 [`docs/graceful-shutdown-contract.md`](../../../docs/graceful-shutdown-contract.md)。

## 关联契约与测试
- 契约测试通过 [`crates/spark-contract-tests`](../../spark-contract-tests) 的 security、graceful_shutdown 与 backpressure 主题验证握手、证书更新与半关闭流程。
- [`crates/spark-tck`](../../spark-tck) 的 TLS 套件会在真实证书环境下执行端到端测试，包括 0-RTT 与会话恢复。
- 观测指标在 [`docs/observability/dashboards/transport-health.json`](../../../docs/observability/dashboards/transport-health.json) 中配置，需确保字段与 `SecurityContextSnapshot` 一致。

## 集成注意事项
- 证书热更新需通过 `hot_reload::TlsReloadHandle` 触发；更新前请确认新证书链已通过安全审计。
- 当 `CallContext::deadline()` 触发时必须立即调用 `close_force()`，避免握手过程阻塞导致资源泄漏。
- 若启用 0-RTT，需在 README 与文档中记录退避策略与重放防御措施，确保契约测试覆盖该分支。
