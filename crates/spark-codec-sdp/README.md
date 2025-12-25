# spark-codec-sdp

## 职责边界
- 提供 Session Description Protocol (SDP) 的最小解析与生成骨架，连接 SIP 信令与 RTP/RTCP 数据面。
- 通过零拷贝结构与 `CallContext` 生命周期对齐，为未来的媒体属性、ICE/DTLS 参数扩展预留空间。
- 配合 `spark-core` 的编解码契约，为 `spark-tck` 与媒体相关的契约测试提供类型占位与能力声明。

## 公共接口入口
- [`src/lib.rs`](./src/lib.rs)：导出 `parse_sdp`/`format_sdp` 以及 `SessionDesc`、`MediaDesc`、`SdpCodecScaffold` 等数据结构。
- [`src/offer_answer.rs`](./src/offer_answer.rs)：实现最小的 RFC 3264 Offer/Answer 协商逻辑，支持 PCMU/PCMA 与 DTMF 能力选择。

## 状态机与错误域
- 当前实现仍处于骨架阶段，解析失败通过 `Result` 的错误支路返回；所有错误需映射到 `ErrorCategory::ProtocolViolation`，以符合 [`docs/error-category-matrix.md`](../../../docs/error-category-matrix.md)。
- 对于未覆盖的属性或媒体块，会以“占位”形式忽略；调用方应结合 [`docs/state_machines.md`](../../../docs/state_machines.md) 将此类情况视作 `ReadyState::Pending` 或记录告警。
- 当协商结果无法满足本地能力时，`offer_answer` 模块使用 `AudioAnswer::Rejected` 表示拒绝，调用方需转换为 `ReadyState::RetryAfter` 或终止会话。

## 关联契约与测试
- 与 [`crates/spark-contract-tests`](../../spark-contract-tests) 的 configuration 与 graceful_shutdown 主题协作，验证配置热更新、重协商流程。
- `offer_answer` 的能力枚举会被 [`crates/spark-tck`](../../spark-tck) 引用，用于端到端互操作测试。
- 若扩展实际的 SDP 行，请同步在 [`docs/transport-handshake-negotiation.md`](../../../docs/transport-handshake-negotiation.md) 中登记字段含义。

## 集成注意事项
- 目前仅覆盖 PCMU/PCMA + DTMF，其他编解码器与属性需在扩展时补充解析逻辑，并更新 README 与根索引。
- SDP 安全字段（如 `a=fingerprint`）尚未深入解析；在 TLS/QUIC 握手失败时，应由上层转换为 `ErrorCategory::SecurityViolation`。
- 当半关闭流程开始时，请停止解析新数据，仅保留已接收内容，并在 `closed()` 完成后释放引用，遵循 [`docs/graceful-shutdown-contract.md`](../../../docs/graceful-shutdown-contract.md)。
