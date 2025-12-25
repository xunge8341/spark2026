//! # core 模块说明
//!
//! ## 核心意图（Why）
//! - 提供交换机的状态与拓扑管理能力，将 `spark-core` 的会话契约落地；
//! - 负责任务编排、协议转换、路由选择等关键流程，保证信令流转的一致性。
//!
//! ## 结构规划（How）
//! - 建议划分为“会话仓储”“拓扑编排器”“协议互操作适配器”等子模块；
//! - 状态同步可优先使用 `dashmap` 等并发结构，同时暴露抽象接口以便单元测试替换。
//!
//! ## 契约边界（What）
//! - 需遵循 `spark_core::service` 与 `spark_router::ServiceFactory` 的接口约束；
//! - 模块对外应返回可追踪的错误类型，统一交由 `error` 模块定义的结构包装。
//!
//! ## 风险提示（Trade-offs）
//! - 并发状态更新需明确顺序保证，必要时通过事件日志或版本号进行幂等控制；
//! - 协议编排涉及 SIP/SDP 等多协议交织，需确保异常路径不会泄露会话资源。

/// 会话状态机与 A/B leg 管理。
pub mod session;

#[cfg(feature = "std")]
/// 基于 `DashMap` 的并发会话仓储。
pub mod session_manager;

pub use session::{CallLeg, CallSession, CallState};

#[cfg(feature = "std")]
pub use session_manager::{SessionManager, SessionRef, SessionRefMut};
