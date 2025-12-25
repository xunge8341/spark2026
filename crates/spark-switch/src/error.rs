//! # error 模块说明
//!
//! ## 角色定位（Why）
//! - 为交换机对外暴露的错误语义提供集中定义，确保与 `spark-core::SparkError` 对齐；
//! - 归档协议解析、会话调度、外部依赖失败等不同类别，方便运维与观测。
//!
//! ## 设计要求（What）
//! - 所有错误类型应实现 `thiserror::Error` 以兼容 `std::error::Error`；
//! - 保留细粒度枚举以支撑精确的告警与重试策略；
//! - 对于可恢复场景，建议区分软/硬错误，避免误触全链路熔断。
//!
//! ## 扩展建议（How）
//! - 结合 `spark-core` 的错误分类（如 `SparkErrorCategory`）提供转换函数；
//! - 对协议相关错误可附带 SIP/SDP 原始片段或上下文信息辅助排障。

#[cfg(not(feature = "std"))]
extern crate alloc;

#[cfg(not(feature = "std"))]
use core::fmt;

use spark_core::SparkError;

#[cfg(feature = "std")]
use thiserror::Error;

#[cfg(feature = "std")]
use std::{borrow::ToOwned, string::String};

#[cfg(not(feature = "std"))]
use alloc::{borrow::ToOwned, format, string::String};

use crate::core::session::{CallLeg, CallState};

/// 交换机核心错误域。
///
/// # 教案式说明
/// - **意图 (Why)**：聚合会话生命周期、服务编排等关键路径的异常，并为上层统一转换为
///   [`SparkError`] 做准备；借助细粒度枚举帮助运维快速定位故障来源。
/// - **契约 (What)**：
///   - 所有变体均实现 `Send + Sync + 'static`，可安全跨线程传播；
///   - 在启用 `std` 特性时派生 [`thiserror::Error`]，保证与生态兼容；
///   - 通过 [`From<SwitchError>`](From) 自动转换为领域错误，便于 `Service` 实现直接 `?` 传播。
/// - **执行逻辑 (How)**：每个变体都携带可读上下文（Call-ID、状态等），`From` 实现根据类别挑选稳定
///   错误码并拼装自然语言描述。
/// - **设计权衡 (Trade-offs)**：使用 `String` 保存上下文，牺牲少量堆分配换取易读性；若未来需零分配，
///   可引入 `Arc<str>` 版本并在转换时按需克隆。
#[cfg_attr(feature = "std", derive(Error))]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SwitchError {
    /// 创建会话时发现同名 Call-ID 已存在。
    ///
    /// - **意图 (Why)**：阻止重复注册导致状态覆盖或并发错乱。
    /// - **契约 (What)**：`call_id` 为受影响的会话标识；调用前无需额外前置条件。
    /// - **风险 (Trade-offs)**：提示上层重新评估幂等策略，必要时回退到已存在的会话引用。
    #[cfg_attr(
        feature = "std",
        error("call session already exists for Call-ID `{call_id}`")
    )]
    SessionAlreadyExists { call_id: String },

    /// 查询或更新会话时未命中。
    ///
    /// - **意图 (Why)**：确保调用方知晓会话已经被回收或从未创建。
    /// - **契约 (What)**：`call_id` 为请求访问的目标标识；上层可据此决定是否重试或直接终止流程。
    /// - **风险 (Trade-offs)**：频繁出现通常意味着控制面/媒体面状态不同步，需要额外观测告警。
    #[cfg_attr(feature = "std", error("call session `{call_id}` is not registered"))]
    SessionNotFound { call_id: String },

    /// 指定的呼叫腿已绑定服务，禁止重复挂载。
    ///
    /// - **意图 (Why)**：避免在 B-leg 尚未释放时重新绑定，导致旧服务泄露或状态错乱。
    /// - **契约 (What)**：`leg` 表示冲突的呼叫腿；`call_id` 指向当前会话；
    ///   前置条件是调用方已经持有可变会话引用。
    /// - **风险 (Trade-offs)**：若经常出现，说明调度器缺乏幂等保障，需排查上游重入逻辑。
    #[cfg_attr(
        feature = "std",
        error("call leg {leg:?} already bound for Call-ID `{call_id}`")
    )]
    LegAlreadyBound { call_id: String, leg: CallLeg },

    /// 状态机拒绝非法的状态跃迁。
    ///
    /// - **意图 (Why)**：保证 B2BUA 的生命周期顺序，避免 `Active` 之前提前发媒体或在终止后重复操作。
    /// - **契约 (What)**：`call_id` 对应触发变更的会话；`from` 为当前状态，`to` 为请求状态；
    ///   前置条件是调用方已持有该会话的独占可变引用。
    /// - **风险 (Trade-offs)**：错误出现时，通常为业务逻辑漏走某个事件顺序，需结合调用栈排查。
    #[cfg_attr(
        feature = "std",
        error(
            "invalid state transition for Call-ID `{call_id}`: {from:?} -> {to:?} is not permitted"
        )
    )]
    InvalidStateTransition {
        call_id: String,
        from: CallState,
        to: CallState,
    },

    /// 服务编排失败，例如 A/B leg service 构造或调用异常。
    ///
    /// - **意图 (Why)**：将 Service 层的失败包装为交换机错误，便于统一观测指标。
    /// - **契约 (What)**：`context` 描述失败环节，例如 `"a-leg.dispatch"`；`detail` 为人类可读说明。
    /// - **风险 (Trade-offs)**：细粒度上下文字符串可能导致告警维度膨胀，需与 SRE 约定命名规范。
    #[cfg_attr(feature = "std", error("service failure during `{context}`: {detail}"))]
    ServiceFailure { context: String, detail: String },

    /// 无法归类的内部异常。
    ///
    /// - **意图 (Why)**：为暂未细分的路径提供兜底，确保错误链不会因 `unreachable!` 触发 panic。
    /// - **契约 (What)**：`detail` 需包含足够排障信息；调用前应确认没有更匹配的错误变体。
    /// - **风险 (Trade-offs)**：过度使用会降低可观测性，应在后续迭代中持续拆分具体枚举。
    #[cfg_attr(feature = "std", error("internal switch failure: {detail}"))]
    Internal { detail: String },
}

impl SwitchError {
    /// 为错误附加 Call-ID 上下文。
    ///
    /// # 教案式说明
    /// - **意图 (Why)**：许多调用点只持有 `&str`，提供工具方法减少 `String` 分配样板代码。
    /// - **执行 (How)**：克隆 `&str` 为 `String` 并嵌入已有错误枚举。
    /// - **契约 (What)**：`call_id` 需保持可打印；方法将返回带填充字段的新错误实例。
    pub fn with_call_id(self, call_id: &str) -> Self {
        match self {
            SwitchError::SessionAlreadyExists { .. } => SwitchError::SessionAlreadyExists {
                call_id: call_id.to_owned(),
            },
            SwitchError::SessionNotFound { .. } => SwitchError::SessionNotFound {
                call_id: call_id.to_owned(),
            },
            SwitchError::LegAlreadyBound { leg, .. } => SwitchError::LegAlreadyBound {
                call_id: call_id.to_owned(),
                leg,
            },
            SwitchError::InvalidStateTransition { from, to, .. } => {
                SwitchError::InvalidStateTransition {
                    call_id: call_id.to_owned(),
                    from,
                    to,
                }
            }
            other => other,
        }
    }
}

impl From<SwitchError> for SparkError {
    /// 将交换机错误转换为统一的领域错误。
    ///
    /// # 教案式说明
    /// - **意图 (Why)**：`Service` 实现需要返回 [`SparkError`]，通过 `From` 实现使 `?` 运算符直接生效。
    /// - **执行 (How)**：依据错误类别选择稳定错误码前缀 `switch.*`，并拼接带上下文的描述信息。
    /// - **契约 (What)**：返回的 [`SparkError`] 至少包含错误码与消息，调用方可继续追加 Trace/节点信息。
    fn from(value: SwitchError) -> Self {
        match value {
            SwitchError::SessionAlreadyExists { call_id } => SparkError::new(
                "switch.session.exists",
                format!("Call-ID `{call_id}` already exists"),
            ),
            SwitchError::SessionNotFound { call_id } => SparkError::new(
                "switch.session.missing",
                format!("Call-ID `{call_id}` is not registered"),
            ),
            SwitchError::LegAlreadyBound { call_id, leg } => SparkError::new(
                "switch.session.leg_exists",
                format!("call leg {leg:?} already bound for Call-ID `{call_id}`"),
            ),
            SwitchError::InvalidStateTransition { call_id, from, to } => SparkError::new(
                "switch.session.invalid_state",
                format!(
                    "state transition `{from:?}` -> `{to:?}` is not allowed for Call-ID `{call_id}`"
                ),
            ),
            SwitchError::ServiceFailure { context, detail } => SparkError::new(
                "switch.service.failure",
                format!("service failure during `{context}`: {detail}"),
            ),
            SwitchError::Internal { detail } => {
                SparkError::new("switch.internal", format!("internal failure: {detail}"))
            }
        }
    }
}

#[cfg(not(feature = "std"))]
impl fmt::Display for SwitchError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        #[allow(unused_imports)]
        use SwitchError::*;

        match self {
            SessionAlreadyExists { call_id } => {
                write!(f, "call session already exists for Call-ID `{call_id}`")
            }
            SessionNotFound { call_id } => {
                write!(f, "call session `{call_id}` is not registered")
            }
            LegAlreadyBound { call_id, leg } => {
                write!(f, "call leg {leg:?} already bound for Call-ID `{call_id}`")
            }
            InvalidStateTransition { call_id, from, to } => write!(
                f,
                "invalid state transition for Call-ID `{call_id}`: {from:?} -> {to:?} is not permitted"
            ),
            ServiceFailure { context, detail } => {
                write!(f, "service failure during `{context}`: {detail}")
            }
            Internal { detail } => write!(f, "internal switch failure: {detail}"),
        }
    }
}
