# spark-codec-rtcp

## 职责边界
- 解析与构造 RTCP 控制报文，为媒体质量监控、拥塞控制与会话管理提供统计输入。
- 与 [`crates/spark-codec-rtp`](../spark-codec-rtp) 协作，共享时钟同步与丢包统计，支撑 [`docs/observability/metrics.md`](../../../docs/observability/metrics.md) 中定义的媒体指标。
- 在契约测试中模拟真实网络行为，确保 `CallContext` 背压与半关闭语义在控制面场景同样成立。

## 公共接口入口
- [`src/lib.rs`](./src/lib.rs)：暴露 `RtcpCodec`、`RtcpPacket` 以及辅助构造器。
- [`src/error.rs`](./src/error.rs)：定义 `RtcpError` 并映射到 `CoreError`/`ErrorCategory`。
- [`src/parse.rs`](./src/parse.rs)：实现复合包解码状态机，处理截断、长度异常与报告类型分发。

## 状态机与错误域
- 解析流程遵循 [`docs/state_machines.md`](../../../docs/state_machines.md) 的 Pending/Ready 语义：复合包待补齐时返回 `DecodeOutcome::Incomplete`，触发 `ReadyState::Pending`。
- 协议违例（Header、长度、SSRC 不匹配）映射到 `ErrorCategory::ProtocolViolation`，驱动 `ReadyState::Busy` 并记录警报。
- 预算超限（异常大的复合包）使用 `ErrorCategory::ResourceExhausted`，提醒宿主按照 [`docs/resource-limits.md`](../../../docs/resource-limits.md) 执行限流。

## 关联契约与测试
- 结合 [`crates/spark-contract-tests`](../../spark-contract-tests) 的 backpressure 与 security 套件，验证截断、退避与鉴权场景。
- 与 [`crates/spark-tck`](../../spark-tck) 的 QUIC/TLS 套件配合，模拟 0-RTT 与密钥轮换下的控制面交互。
- 若扩展新的 RTCP 字段或事件，请同步更新 [`docs/observability/dashboards`](../../../docs/observability/dashboards) 中的可视化配置。

## 集成注意事项
- 在 DTLS-SRTP 或 QUIC datagram 模式下需保持零拷贝：本实现使用 `ErasedSparkBuf` 暴露切片，满足 [`docs/buffer-zerocopy-contract.md`](../../../docs/buffer-zerocopy-contract.md) 的约束。
- 若检测到持续高 RTT/丢包，可通过 `CallContext::budget` 调整发送速率，并向上游发布 `ReadyState::RetryAfter`。
- 半关闭期间仅消费已完整到达的复合包，未完成的数据会被丢弃，确保遵循 [`docs/graceful-shutdown-contract.md`](../../../docs/graceful-shutdown-contract.md)。
