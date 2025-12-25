use alloc::{format, string::String};
use core::time::Duration;

use super::TransportSocketAddr;

/// 描述监听端优雅关闭的策略。
///
/// # 设计动机（Why）
/// - 综合 Nginx、Envoy、gRPC 等组件的优雅关闭语义，支持“排空 + 截止时间”两阶段策略。
/// - 面向实时系统研究，允许显式声明是否等待现有会话完成，以评估不同排空策略的影响。
/// - 与 [`ShutdownGraceful`](crate::contract::ShutdownGraceful) 对应：本结构负责传输层局部参数，
///   而后者用于跨层传播关闭意图。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ListenerShutdown {
    deadline: Duration,
    drain_existing: bool,
}

impl ListenerShutdown {
    /// 创建新的关闭计划。
    ///
    /// # 契约说明（What）
    /// - `deadline`：从调用时刻起可接受的最长优雅关闭时间，对应
    ///   [`ShutdownGraceful::deadline`](crate::contract::ShutdownGraceful::deadline)；
    /// - `drain_existing`：是否等待现有连接排空；`false` 时允许立即拒绝新请求并中断现有会话。
    ///
    /// # 风险提示（Trade-offs）
    /// - 选择排空会延长关闭时间，但可避免请求丢失；不排空则可能导致业务重试压力增大。
    /// - 若上层提供了 [`ShutdownImmediate`](crate::contract::ShutdownImmediate)，应跳过此计划直接
    ///   执行强制关闭。
    pub fn new(deadline: Duration, drain_existing: bool) -> Self {
        Self {
            deadline,
            drain_existing,
        }
    }

    /// 截止时间。
    pub fn deadline(&self) -> Duration {
        self.deadline
    }

    /// 是否排空现有连接。
    pub fn drain_existing(&self) -> bool {
        self.drain_existing
    }
}

/// 服务端传输契约的泛型与对象层接口统一由 [`crate::transport`] 模块导出。
/// - 泛型接口：[`crate::transport::ServerChannel`]
/// - 对象接口：[`crate::transport::DynServerChannel`]
///
/// `TransportSocketAddr` 依旧在此模块暴露，便于调用方在关闭流程中记录地址信息。
pub fn describe_shutdown_target(addr: &TransportSocketAddr, plan: &ListenerShutdown) -> String {
    format!(
        "{} draining={}, deadline={}s",
        addr,
        plan.drain_existing(),
        plan.deadline().as_secs_f64()
    )
}
