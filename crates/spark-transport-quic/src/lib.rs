#![doc = r#"
# spark-transport-quic

## 设计动机（Why）
- **统一传输接口**：为 Spark 框架提供基于 QUIC 的多路复用通道，实现与 TCP/UDP 实现一致
  的 API 契约。
- **面向实践**：封装 `quinn`/`quinn-proto` 的核心能力，提供监听、建连与流级背压治理，便于
  上层快速集成。
- **扩展友好**：内部模块化拆分（Endpoint、Channel、Backpressure、Util），方便未来接入
  自定义拥塞控制或指标采样。

## 核心契约（What）
- `QuicEndpoint`：负责 UDP Socket 绑定、监听与发起连接。
- `QuicConnection`：表示一次 QUIC 连接，可打开/接受双向流。
- `QuicChannel`：封装 `quinn::SendStream/RecvStream`，提供读写、半关闭与背压探测。
- `ShutdownDirection`：定义半关闭方向，由 `spark-core` 提供统一抽象，这里通过再导出供调用方使用。

## 实现策略（How）
- 通过 `run_with_context` 注入 `CallContext` 的取消/截止语义，确保 QUIC IO 与框架契约一致。
- 使用 `QuicBackpressure` 将 `ConnectionStats` 映射到 `ReadyState::{Busy, RetryAfter}`。
- `error` 模块统一维护错误码映射，所有失败以 `CoreError` 形式返回。

## 风险与注意（Trade-offs）
- 当前实现偏向单节点实验环境，证书管理需由调用侧提供；
- 背压策略基于即时统计，极端场景可能需要更精细的指标采样；
- `poll_ready` 采用空写探测，存在轻微系统调用开销，但换取语义一致性。
"#]
#![cfg_attr(
    not(feature = "runtime-tokio"),
    doc = r#"## 功能开关：`runtime-tokio`

默认启用基于 Tokio + Quinn 的 QUIC 实现；若需要最小化依赖或仅查看文档，可禁用默认特性跳过运行时代码。
停用后 crate 仅暴露契约说明，不会链接实际 QUIC 逻辑。

启用方式：`spark-transport-quic = { features = ["runtime-tokio"] }` 或沿用默认特性。
"#
)]

#[cfg(feature = "runtime-tokio")]
mod backpressure;
#[cfg(feature = "runtime-tokio")]
mod channel;
#[cfg(feature = "runtime-tokio")]
mod endpoint;
#[cfg(feature = "runtime-tokio")]
mod error;
#[cfg(feature = "runtime-tokio")]
mod util;

#[cfg(feature = "runtime-tokio")]
pub use channel::QuicChannel;
#[cfg(feature = "runtime-tokio")]
pub use endpoint::{QuicConnection, QuicEndpoint, QuicEndpointBuilder};
pub use spark_core::transport::ShutdownDirection;
