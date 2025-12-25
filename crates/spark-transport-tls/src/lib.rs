#![doc = r#"
# spark-transport-tls

## 设计动机（Why）
- **安全入口**：为 `spark-transport-tcp` 接入层提供 TLS1.3 加密包装，确保链路机密性与完整性；
- **可运维性**：通过显式的错误分类（Security / ResourceExhausted / Retryable）与握手元数据（SNI、ALPN），方便路由、审计与自愈策略；
- **热更新**：依托 `ArcSwap` 在不中断现有连接的情况下替换证书或密码套件配置。

## 核心契约（What）
- [`TlsAcceptor`]：接收 `TcpChannel` 与 [`CallContext`](spark_core::contract::CallContext)，执行 TLS 握手并返回 [`TlsChannel`]；
- [`TlsChannel`]：封装加密后的读写接口，同时暴露协商出的 SNI 与 ALPN，供上层协议栈决策；
- 错误分类遵循 `Security`（证书/握手违规）、`ResourceExhausted`（资源不足或通道不可用）与 `Retryable`（可重试的瞬时故障）。

## 实现策略（How）
- 使用 `rustls` + `tokio-rustls` 完成异步握手与数据加解密；
- `run_with_context` 复用 `spark-core` 的取消/截止契约，确保 TLS 层尊重 `CallContext`；
- 通过 `TcpChannel::try_into_parts` 拆解原始 `TcpStream`，避免重复建立 TCP 连接。

## 风险与考量（Trade-offs）
- 握手时若 `TcpChannel` 被多处持有，将视作资源耗尽并拒绝进入 TLS 阶段；
- 轮询式取消存在毫秒级延迟，但能在 Tokio 上保持实现简单；
- 当前实现聚焦服务端接入，后续若需客户端支持或会话缓存，可在现有结构上扩展。
"#]
#![cfg_attr(
    not(feature = "runtime-tokio"),
    doc = r#"## 功能开关：`runtime-tokio`

默认启用 Tokio + rustls 组合的 TLS 实现；当需要最小依赖或在文档构建阶段排除 Tokio，可禁用默认特性。
此时 crate 仅保留能力说明与类型文档，不会链接实际 TLS 代码。

启用方式：`spark-transport-tls = { features = ["runtime-tokio"] }` 或沿用默认特性。
"#
)]

#[cfg(feature = "runtime-tokio")]
mod hot_reload;

#[cfg(feature = "runtime-tokio")]
pub use hot_reload::{HotReloadingServerConfig, TlsHandshakeError};
#[cfg(feature = "runtime-tokio")]
mod acceptor;
#[cfg(feature = "runtime-tokio")]
mod channel;
#[cfg(feature = "runtime-tokio")]
mod error;
#[cfg(feature = "runtime-tokio")]
mod util;

#[cfg(feature = "runtime-tokio")]
pub use acceptor::{TlsAcceptor, TlsAcceptorBuilder};
#[cfg(feature = "runtime-tokio")]
pub use channel::TlsChannel;
