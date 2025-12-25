use core::{fmt, future::Future};

use super::TransportSocketAddr;
use crate::Result;

/// 无连接报文端点抽象。
///
/// # 教案级注释
///
/// ## 意图（Why）
/// - 将 UDP 等无连接传输的收发语义标准化，
///   使上层能够在不知道具体协议细节的前提下统一处理报文；
/// - 与 [`super::channel::Channel`] 互补，分别覆盖
///   面向连接与无连接两类传输场景。
///
/// ## 契约（What）
/// - `Error`：统一错误类型；
/// - `CallCtx<'ctx>`：执行一次收发操作时的上下文；
/// - `InboundMeta` / `OutboundMeta`：收包与发包关联的元数据描述；
/// - `RecvFuture`：读取报文与元数据；
/// - `SendFuture`：发送报文；
/// - `local_addr()`：查询本地绑定地址；
/// - `recv()`：读取报文，返回长度与元数据；
/// - `send()`：按元数据发送报文；
/// - **前置条件**：实现需确保上下文在 Future 执行期间保持有效；
/// - **后置条件**：成功返回即表示收发完成或地址查询成功。
///
/// ## 解析逻辑（How）
/// - 通过 GAT 约束 Future，允许实现直接返回 `async` 块；
/// - `InboundMeta`/`OutboundMeta` 由实现定义，可承载 NAT、路由等额外信息；
/// - 所有操作返回 [`Result`]，统一错误传播。
///
/// ## 风险提示（Trade-offs）
/// - Trait 要求 `Send + Sync + 'static`，在极端嵌入式环境可能需要适配层；
/// - 若实现依赖异步运行时（Tokio 等），应在文档中明确说明前置条件；
/// - `InboundMeta` 与 `OutboundMeta` 不应包含引用临时数据，避免悬垂引用风险。
pub trait DatagramEndpoint: Send + Sync + 'static {
    type Error: fmt::Debug + Send + Sync + 'static;
    type CallCtx<'ctx>;
    type InboundMeta;
    type OutboundMeta;

    type RecvFuture<'ctx>: Future<Output = Result<(usize, Self::InboundMeta), Self::Error>>
        + Send
        + 'ctx
    where
        Self: 'ctx,
        Self::CallCtx<'ctx>: 'ctx;

    type SendFuture<'ctx>: Future<Output = Result<usize, Self::Error>> + Send + 'ctx
    where
        Self: 'ctx,
        Self::CallCtx<'ctx>: 'ctx;

    /// 查询本地绑定地址。
    fn local_addr(&self) -> Result<TransportSocketAddr, Self::Error>;

    /// 接收报文并返回载荷长度与元数据。
    fn recv<'ctx>(
        &'ctx self,
        ctx: &'ctx Self::CallCtx<'ctx>,
        buf: &'ctx mut [u8],
    ) -> Self::RecvFuture<'ctx>;

    /// 根据元数据发送报文。
    fn send<'ctx>(
        &'ctx self,
        ctx: &'ctx Self::CallCtx<'ctx>,
        payload: &'ctx [u8],
        meta: &'ctx Self::OutboundMeta,
    ) -> Self::SendFuture<'ctx>;
}
