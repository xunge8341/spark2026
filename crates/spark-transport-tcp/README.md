# spark-transport-tcp

## 职责边界
- 提供基于 Tokio 的 TCP 传输通道，实现 `spark-core::transport::channel` 契约，支持 `no_std + alloc` 环境下的宿主抽象。
- 负责连接建立、半关闭、背压与错误分类，将 socket 状态转换为 `ReadyState` 与 `CloseReason`。
- 作为其他传输实现（TLS、QUIC）的基线，输出统一的调度与观测指标。

## 公共接口入口
- [`src/lib.rs`](./src/lib.rs)：导出 `TcpServerChannel`, `TcpChannel` 及通道构造函数。
- [`src/channel.rs`](./src/channel.rs)：实现 `poll_ready`、`read`、`write`、`close_graceful`、`close_force` 等核心逻辑。
- [`src/backpressure.rs`](./src/backpressure.rs)：采样 `WouldBlock` 次数、RTT 与缓冲容量，转换为 `ReadyState::{Busy, RetryAfter, BudgetExhausted}`。
- [`src/error.rs`](./src/error.rs)：定义 `TcpError` 并映射到 `CoreError`/`ErrorCategory`。

## 状态机与错误域
- ReadyState 行为遵循 [`docs/state_machines.md`](../../../docs/state_machines.md)：
  - 可写 → `Ready`
  - 短暂阻塞 → `Pending`
  - 持续阻塞/拥塞 → `Busy`
  - 预算不足 → `BudgetExhausted`
  - 自适应退避 → `RetryAfter`
- 错误分类与关闭原因需符合 [`docs/error-category-matrix.md`](../../../docs/error-category-matrix.md) 与 [`docs/graceful-shutdown-contract.md`](../../../docs/graceful-shutdown-contract.md)。
- 安全相关错误（握手失败、非法地址）转换为 `ErrorCategory::SecurityViolation`，并在 `CloseReason` 中保留细节。

## 关联契约与测试
- 与 [`crates/spark-contract-tests`](../../spark-contract-tests) 的 backpressure、errors、graceful_shutdown 主题直接对接，验证 ReadyState 序列与半关闭流程。
- [`crates/spark-tck`](../../spark-tck) 使用该实现执行真实 socket 场景，覆盖取消、超时与多路并发。
- 性能指标与观测面在 [`docs/observability/dashboards/transport-health.json`](../../../docs/observability/dashboards/transport-health.json) 中展示，需保持字段一致。

## 半关闭顺序
- 遵循“写半关闭 → 等待对端确认 → 释放资源”契约：
  1. `close_graceful` 首先调用 `TcpStream::shutdown(Write)` 发送 FIN；
  2. 持续读取直到对端确认读半关闭或 `Deadline` 超时；
  3. 若超时触发 `close_force`，记录 `CloseReason::Timeout` 并关闭 socket。
- `spark-core::transport::ShutdownDirection` 枚举确保调用方显式声明关闭方向，防止顺序错误，实现与 QUIC/TLS 的共用语义。

## ReadyState 映射表
| Socket 状态 | ReadyState | 说明 |
| --- | --- | --- |
| 写缓冲可写 | `Ready` | 可以继续发送数据 |
| 短暂 `WouldBlock` | `Pending` | 等待下一次可写事件 |
| 持续 `WouldBlock` 或拥塞 | `Busy(BusyReason::downstream)` | 提醒调度器降低速率 |
| RTT 激增/自适应退避 | `RetryAfter` | `backpressure::TcpBackpressure` 根据采样计算 |
| 预算耗尽 | `BudgetExhausted` | `CallContext::budget` 拒绝继续发送 |
| 握手/安全失败 | `Busy` + `CloseReason::SecurityViolation` | 与 TLS/QUIC 上层对齐 |

## 超时/取消来源（CallContext）
- `CallContext::deadline()` 映射到 Tokio 的超时，保护连接建立、读写与半关闭流程。
- `CallContext::cancellation_token()` 在会话迁移或上层指令时触发，`TcpChannel` 在 `poll_ready`/IO 循环中检查并提前退出。
- `CallContext::budget` 控制单连接吞吐，结合 `BudgetDecision` 限制写入速率，确保背压信号及时反馈。
## 集成注意事项
- 默认假设宿主提供 Tokio Runtime；若在自定义执行器中使用，需要提供兼容的 `AsyncRead`/`AsyncWrite` 实现。
- 半关闭时遵循“写 FIN → 等待读确认 → 释放资源”流程，超时则触发 `close_force` 并记录 `CloseReason::Timeout`。
- 当在 CI/生产环境新增 socket 调优参数（如 TCP_NODELAY）时，请在 README 与根索引中同步更新，确保运维与测试知情。
