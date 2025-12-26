//! Platform Runtime Mechanism
//!
//! Foundation 版本仅保留执行器/定时器/任务抽象与无害的糖（sugar）。
//!
//! 说明：
//! - 已删除 `slo` / `hotreload` 等策略模块；
//! - 已删除任何策略层 crate 的 re-export；
//! - 为保证宏与上层调用路径连续，保留 `sugar` 相关导出。

mod executor;
mod hotreload;
mod services;
pub mod slo;
pub mod sugar;
mod task;
mod timer;

pub use executor::TaskExecutor;
pub use hotreload::{
    HotReloadApplyTimer, HotReloadFence, HotReloadObservability, HotReloadReadGuard,
    HotReloadWriteGuard,
};
pub use services::CoreServices;
pub use slo::{
    SloPolicyAction, SloPolicyConfigError, SloPolicyDirective, SloPolicyManager,
    SloPolicyReloadReport, SloPolicyRule, SloPolicyTrigger, slo_policy_table_key,
};
pub use task::{
    BlockingTaskSubmission, JoinHandle, LocalTaskSubmission, ManagedBlockingTask, ManagedLocalTask,
    ManagedSendTask, SendTaskSubmission, TaskCancellationStrategy, TaskError, TaskHandle,
    TaskLaunchOptions, TaskPriority, TaskResult,
};
pub use timer::{MonotonicTimePoint, TimeDriver};

/// 异步运行时的统一契约：聚合任务调度与时间驱动能力。
///
/// # 教案式说明（Why）
/// - 上层只需依赖该 trait 便能同时获得任务提交（[`TaskExecutor`]）与时间感知（[`TimeDriver`]）能力，
///   避免在函数签名中同时暴露多种运行时约束。
/// - 便于宿主实现通过单一类型提供全部能力，例如基于 Tokio/async-std 的适配层。
///
/// # 契约定义（What）
/// - 组合约束：实现者必须同时实现 [`TaskExecutor`] 与 [`TimeDriver`]；
/// - 典型用途：作为 `Arc<dyn AsyncRuntime>` 注入到 [`CoreServices`] 或 [`CallContext`]
///   中，供业务代码在合约层调用。
///
/// # 逻辑解析（How）
/// - 这是一个空 trait，仅作为标记；通过泛型约束或 trait 对象将两种能力绑定在一起；
/// - 默认为所有同时实现两种能力的类型提供 blanket 实现，无需额外样板。
///
/// # 前置/后置条件（Contract）
/// - **前置条件**：调用方需确保运行时对象在任务生命周期内有效，并遵循 [`TaskExecutor`] 的上下文传播要求；
/// - **后置条件**：实现者必须保证时间接口与任务调度接口在生命周期与线程安全语义上保持一致。
///
/// # 风险与注意事项（Trade-offs）
/// - 若未来需要拆分时间与任务能力（例如极简 `no_std` 场景），可通过新的标记 trait 做更细粒度区分；
/// - blanket 实现意味着无法为同一类型提供差异化标记，如需特殊语义应另行封装新类型。
pub trait AsyncRuntime: TaskExecutor + TimeDriver {}

impl<T> AsyncRuntime for T where T: TaskExecutor + TimeDriver {}

pub use sugar::{CallContext, PipelineContextCaps, RuntimeCaps, spawn_in};
