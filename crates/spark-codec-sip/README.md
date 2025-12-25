# spark-codec-sip

## 职责边界
- 将 SIP 请求与响应报文映射为结构化数据，提供零拷贝解析/序列化能力以契合 `spark-core::codec::Codec` 契约。
- 支撑 `spark-core` 的呼叫控制、会话迁移与事务跟踪逻辑，确保与 `CallContext` 的预算、取消与半关闭语义保持一致。
- 为 `spark-tck` 的信令互操作测试提供基线实现，并在 `CodecRegistry` 中作为 SIP 族扩展的默认入口。

## 公共接口入口
- [`src/lib.rs`](./src/lib.rs)：暴露 `SipCodec`、`parse_request`、`parse_response` 与编码器入口。
- [`src/parse`](./src/parse)：实现请求行、响应行、头部与 URI 的解析状态机，处理增量缓冲。
- [`src/fmt`](./src/fmt)：提供序列化与格式化逻辑，确保与解析结果往返一致。
- [`src/error.rs`](./src/error.rs)：定义 `SipParseError`、`SipFormatError` 并映射到 `CoreError` 分类。
- [`src/transaction.rs`](./src/transaction.rs)：为事务标识、分支参数等提供辅助类型，方便与上层状态机联动。

## 状态机与错误域
- 解析状态机在缓冲不足时返回 `SipParseError::UnexpectedEof`，调用方需发布 `ReadyState::Pending`，符合 [`docs/state_machines.md`](../../../docs/state_machines.md)。
- 语法错误映射到 `ErrorCategory::ProtocolViolation` 并触发 `ReadyState::Busy`；资源超限（header/主体过大）需通过 `CallContext::budget` 返回 `ErrorCategory::ResourceExhausted`。
- 当底层 TLS/QUIC 握手失败或检测到安全风险时，调用方应将错误分类设置为 `ErrorCategory::SecurityViolation` 并终止通道，参见 [`docs/safety-audit.md`](../../../docs/safety-audit.md)。

## 关联契约与测试
- 使用 [`crates/spark-contract-tests`](../../spark-contract-tests) 的 backpressure、errors 与 graceful_shutdown 套件验证 ReadyState、错误分类与半关闭流程。
- 与 [`crates/spark-codec-sdp`](../spark-codec-sdp) 协同，SIP 消息中的 SDP 负载需保持字段一致，相关示例在两个 README 中相互引用。
- WebSocket 传输扩展位于 [`src/ws`](./src/ws)，未来若启用需同步更新 [`docs/global-architecture.md`](../../../docs/global-architecture.md) 的信令通道描述。

## 集成注意事项
- 半关闭阶段应停止解析新缓冲，仅处理已到达的数据，并在 `closed()` 完成后释放引用，遵循 [`docs/graceful-shutdown-contract.md`](../../../docs/graceful-shutdown-contract.md)。
- 若在业务侧自定义头部或扩展解析器，请在 README 与相关文档登记新字段，确保契约测试能够覆盖。
- 预算与背压策略建议结合 `CallContext::budget(BudgetKind::Flow)` 与 `ReadyState::RetryAfter`，并在 [`docs/observability/metrics.md`](../../../docs/observability/metrics.md) 中记录异常报文数量，便于观测与告警。
