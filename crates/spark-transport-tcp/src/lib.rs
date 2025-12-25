#![doc = r#"
# spark-transport-tcp

## 设计动机（Why）
- **定位**：该 crate 提供 Spark 在 Tokio 运行时上的最小 TCP 通道实现，
  封装监听、建连、读写与背压等底层细节。
- **架构角色**：作为传输实现层的基础积木，对接 `spark-core` 的上下文与
  错误契约，为后续 TLS/QUIC 实现提供语义参照。
- **设计理念**：强调“上下文传递”与“错误分类”，所有网络操作均感知
  [`CallContext`](spark_core::contract::CallContext) 的取消、截止与预算约束，
  并在失败时映射为结构化的 [`CoreError`](spark_core::error::CoreError)。

## 核心契约（What）
- **输入条件**：调用方必须在 Tokio 运行时中使用本实现，并显式传递
  `CallContext`/`Context`；
- **输出保障**：监听、通道读写、半关闭与背压检查均返回语义化结果，
  出错时附带稳定错误码及 [`ErrorCategory`](spark_core::error::ErrorCategory)；半关闭方向采用
  `spark-core::transport::ShutdownDirection` 统一表述。
- **前置约束**：当前版本仅实现单连接的直接操作，尚未与 Pipeline 控制器
  集成；后续版本会继续对接 `spark-core::transport` 的泛型工厂。

## 实现策略（How）
- **执行框架**：完全依赖 Tokio 的 `TcpListener` 与 `TcpStream`，并通过
  `tokio::select!` 将取消/超时与 IO Future 组合；
- **上下文映射**：使用内部工具函数将 `Deadline` 转换为 Tokio 时间点，
  并周期性轮询 `Cancellation` 以响应取消；
- **背压治理**：在 `poll_ready` 中根据写缓冲的即时状态与 `WouldBlock`
  频次映射为 `ReadyState::{Busy, RetryAfter}`，为上层调度提供信号。

## 风险与考量（Trade-offs）
- **时间基准**：`Deadline` 被映射到本 crate 初始化时刻的单调时钟；若调用方
  使用不同计时源构造 `MonotonicTimePoint`，可能产生轻微漂移。
- **并发度**：当前实现通过 `tokio::sync::Mutex` 序列化读写；在高并发场景
  可能需要进一步拆分读/写半部或引入无锁缓冲。
- **扩展计划**：后续将引入连接工厂、管道集成及观测指标，当前版本着重
  提供最小可测试能力。
"#]
#![cfg_attr(
    not(feature = "runtime-tokio"),
    doc = r#"## 功能开关：`runtime-tokio`

默认构建启用 Tokio 运行时实现；若在最小依赖或文档生成场景需要跳过 Tokio，可禁用默认特性。
此时 crate 仅保留文档与契约说明，不会编译具体的传输实现。

启用方式：`spark-transport-tcp = { features = ["runtime-tokio"] }` 或使用默认特性。
"#
)]

#[cfg(feature = "runtime-tokio")]
mod backpressure;
#[cfg(feature = "runtime-tokio")]
mod channel;
#[cfg(feature = "runtime-tokio")]
mod error;
#[cfg(feature = "runtime-tokio")]
mod server_channel;
#[cfg(feature = "runtime-tokio")]
mod util;

#[cfg(feature = "runtime-tokio")]
pub use channel::{TcpChannel, TcpChannelParts, TcpSocketConfig};
#[cfg(feature = "runtime-tokio")]
pub use server_channel::{TcpServerChannel, TcpServerChannelBuilder};
pub use spark_core::transport::ShutdownDirection;
