# spark-transport-udp

## 职责边界
- 封装 Tokio `UdpSocket`，为 SIP/NAT/媒体心跳等无连接场景提供传输接口，并遵循 `spark-core::transport` 契约。
- 记录源地址、回源路径与 NAT 相关元数据，支撑 `CallContext` 在无连接环境中的取消与预算决策。
- 为未来的 QUIC/DTLS 封装提供基线，保证半关闭、背压与安全语义与其他传输实现一致。

## 公共接口入口
- [`src/lib.rs`](./src/lib.rs)：导出 `UdpEndpoint`, `UdpReturnRoute` 等类型，实现收发、背压与关闭逻辑。
- [`src/batch.rs`](./src/batch.rs)：提供批量收发 API，减少 syscalls 并提升在高并发场景下的吞吐能力。

## 状态机与错误域
- ReadyState 映射遵循 [`docs/state_machines.md`](../../../docs/state_machines.md)：
  - socket 可写 → `Ready`
  - 缓冲暂满 → `Pending`
  - 持续拥塞或速率限制 → `Busy`
  - 预算拒绝 → `BudgetExhausted`
  - NAT 限速等场景 → `RetryAfter`
- 错误分类参考 [`docs/error-category-matrix.md`](../../../docs/error-category-matrix.md)：IO/协议错误映射为 `Transport`/`ProtocolViolation`，异常来源或重放攻击映射为 `SecurityViolation`。

## 关联契约与测试
- [`crates/spark-contract-tests`](../../spark-contract-tests) 的 backpressure 与 security 主题会验证 NAT 退避、非法来源与预算耗尽的行为。
- [`crates/spark-tck`](../../spark-tck) 使用本实现模拟实际 UDP 流量（rport 回写、Keepalive），确保 ReadyState 与关闭顺序正确。
- 观测指标与报警配置可参照 [`docs/observability/dashboards/transport-health.json`](../../../docs/observability/dashboards/transport-health.json)。

## 集成注意事项
- 无连接环境下无 FIN 报文，但仍需在调用 `close_graceful` 后等待 `CallContext::closed()`，再释放 socket，以符合 [`docs/graceful-shutdown-contract.md`](../../../docs/graceful-shutdown-contract.md)。
- 若结合 QUIC/DTLS 使用，请在 `UdpReturnRoute` 中保留源地址与校验信息，便于安全层完成握手验证。
- 当业务侧新增速率控制或 NAT 缓存策略，请同步更新 README 与根索引，避免测试与运维信息不一致。
