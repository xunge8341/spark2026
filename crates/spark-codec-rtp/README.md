# spark-codec-rtp

## 职责边界
- 为 RTP 报文提供零拷贝解析与构造，支撑实时媒体传输在 `spark-core` 背压与半关闭语义下运行。
- 与 [`crates/spark-codec-sdp`](../spark-codec-sdp) 完成编解码协商，确保 `payload_type`、`ssrc`、`clock_rate` 等字段在媒体 pipeline 中保持一致。
- 提供可复用的时钟同步、序列号管理与统计数据，供传输实现和观测链路（参见 [`docs/observability/metrics.md`](../../../docs/observability/metrics.md)）消费。

## 公共接口入口
- [`src/lib.rs`](./src/lib.rs)：提供 `parse_rtp`、`RtpPacketBuilder`、序列号比较等核心 API，并定义错误类型与零拷贝视图。
- [`src/dtmf.rs`](./src/dtmf.rs)：实现 RFC 4733 DTMF 扩展的编解码，演示如何在不破坏主数据面的情况下扩展事件处理。

## 状态机与错误域
- 解码流程在分片不足时返回 `DecodeOutcome::Incomplete`，要求上游传播 `ReadyState::Pending`，符合 [`docs/state_machines.md`](../../../docs/state_machines.md)。
- Header、扩展或负载异常归类为 `ErrorCategory::ProtocolViolation` 并映射到 `ReadyState::Busy`；缓冲不足则触发 `ErrorCategory::ResourceExhausted`。
- 在 TLS/QUIC 安全事件中需将错误转换为 `ErrorCategory::SecurityViolation` 并立即关闭流，呼应 [`docs/safety-audit.md`](../../../docs/safety-audit.md)。

## 关联契约与测试
- 使用 [`crates/spark-contract-tests`](../../spark-contract-tests) 的 backpressure、graceful_shutdown 与 security 主题验证 ReadyState 序列。
- 与 [`crates/spark-tck`](../../spark-tck) 的 TCP/TLS/QUIC 套件协作，覆盖真实网络下的抖动、重传与密钥轮换情景。
- 媒体会话的 SDP 协商示例位于 [`crates/spark-codec-sdp`](../spark-codec-sdp)，README 需与之保持字段说明的一致性。

## 集成注意事项
- `RtpCodec` 默认使用 `ErasedSparkBuf` 以满足 [`docs/buffer-zerocopy-contract.md`](../../../docs/buffer-zerocopy-contract.md)；在自定义内存池时需确保生命周期与 `CallContext` 对齐。
- 半关闭阶段必须停止从底层拉取新包，仅处理已到达的数据，并在 `closed()` 完成后释放所有引用，遵循 [`docs/graceful-shutdown-contract.md`](../../../docs/graceful-shutdown-contract.md)。
- 如需根据丢包率动态退避，可结合 `CallContext::budget(BudgetKind::Flow)` 与 `ReadyState::RetryAfter`，并在 README 中补充新的策略描述。
