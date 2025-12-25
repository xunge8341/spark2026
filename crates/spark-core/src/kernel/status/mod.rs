//! 状态语义模块，统一框架中“就绪探测”相关的枚举与辅助结构。
//!
//! # 模块定位（Why）
//! - 将分散在各子域（路由、集群、服务接口）中的“就绪/拥塞/预算耗尽”语义集中定义，
//!   避免术语在不同模块间产生别名或偏差。
//! - 通过统一的类型导出，方便实现者在 `no_std + alloc` 场景下共享相同的状态判定逻辑。
//!
//! # 结构概览（What）
//! - [`ready`]：描述服务就绪探测的状态机，包含 `Ready/Busy/BudgetExhausted/RetryAfter` 四种稳定语义。
//! - 未来若需扩展其他状态（如“连接上限”或“计划维护”），亦应在该模块内集中管理，
//!   以维持契约的一致性。
//!
//! # 使用建议（How）
//! - 所有需要暴露就绪信号的组件应直接返回 [`ready::PollReady`]，并通过辅助构造函数描述原因。
//! - 若业务需要自定义“繁忙原因”或“重试建议”，可在 `ready` 模块提供的结构上追加上下文信息，
//!   避免重新定义枚举导致语义漂移。

pub mod ready;

#[rustfmt::skip]
pub use ready::{
    BusyReason,
    PollReady,
    ReadyCheck,
    ReadyState,
    RetryAdvice,
    RetryRhythm,
    SubscriptionBudget,
};
#[cfg(feature = "std")]
pub use ready::RetryAfterThrottle;
