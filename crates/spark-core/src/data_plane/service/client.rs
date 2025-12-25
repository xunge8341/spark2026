//! # ClientFactory：对象层出站调用构造器
//!
//! ## 设计动机（Why）
//! - B2BUA、SIP Proxy 等需要根据运行时决策（Dialplan、路由策略）动态拨出到下游。
//! - 将“如何基于 `CallContext` 构造对象层 [`BoxService`]”抽象为工厂，
//!   让上层只需关注业务决策，而无需了解底层传输/握手细节。
//!
//! ## 契约说明（What）
//! - `connect`：根据目标标识（如 URI、Service Name）创建对象层 Service；
//! - 所有实现都必须遵守 [`CallContext`] 的取消/截止语义，防止拨号阻塞。
//!
//! ## 架构位置（Where）
//! - 位于 `spark-core::service` 模块内，与泛型/对象层 Service 契约并列；
//! - 可由运行时或宿主在构建 [`CallContext`] 时注入，供业务按需复用。

use crate::{SparkError, contract::CallContext, service::BoxService};

/// 动态构造对象层出站 Service 的统一接口。
///
/// # 教案式注释
/// - **意图 (Why)**：将“如何与下游网络建立连接”与业务逻辑解耦，
///   例如 SIP Proxy 仅需告知目标 URI，具体的传输栈由工厂实现；
/// - **契约 (What)**：
///   - `connect` 接收调用上下文与目标字符串；实现可自由解析 URI、
///     Service Name 或任意命名空间；
///   - 返回值为对象层 [`BoxService`]，供 Pipeline 继续驱动；
///   - 发生错误时返回 [`SparkError`]，上层可据此决定快速失败或重试；
/// - **前置条件**：调用者必须确保 `ctx` 来自同一请求链路，
///   避免跨请求复用导致取消/预算语义错配；
/// - **后置条件**：成功时返回的 Service 必须遵守 `CallContext` 中的取消/截止约束；
/// - **风险提示 (Trade-offs)**：为保持对象安全，`connect` 采用同步接口；
///   若实现需要执行异步握手，可在内部 `tokio::spawn` 或使用阻塞运行时调度，
///   但务必遵守 `CallContext` 的超时约束以防止线程池被耗尽。
pub trait ClientFactory: Send + Sync + 'static {
    /// 根据目标字符串构造对象层 Service。
    fn connect(&self, ctx: &CallContext, target: &str) -> crate::Result<BoxService, SparkError>;
}
