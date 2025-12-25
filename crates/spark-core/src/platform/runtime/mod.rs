//! # Contract-only Runtime Surface
//!
//! ## 契约声明
//! * **Contract-only：** 本模块仅定义运行时可供合约调用的抽象 API，约束业务侧只能依赖这些接口而非具体执行器实现，以确保在无状态执行环境、回放环境中保持一致行为。
//! * **禁止实现：** 本文件及其子模块不允许落地具体执行逻辑，实现必须由宿主运行时或测试替身在独立 crate 中提供，从而杜绝合约代码在此处混入状态机或 I/O 细节。
//! * **解耦外设：** 所有接口均以 `Send + Sync + 'static` 能力描述，对具体执行器、定时器、异步 runtime 完全解耦，便于在 wasm/no-std 等受限环境中替换宿主。
//!
//! ## 并发与错误语义
//! * **并发模型：** 默认遵循单请求上下文内的协作式并发——接口返回的 future/stream 必须可被外部执行器安全地 `await`；禁止在实现中假设特定调度器或线程池。
//! * **错误传播：** 约定使用 `SparkError` 系列枚举表达业务/系统异常；调用方必须准备处理超时、取消、幂等失败等场景，实现方不得吞掉错误或 panic。
//!
//! ## 使用前后条件
//! * **前置条件：** 合约端在调用前必须确保上下文由外部运行时注入（如 tracing span、租户信息），且所有句柄均来自这些契约。
//! * **后置条件：** 成功调用保证返回值可用于继续构造合约逻辑，但不会持有宿主资源的独占所有权，避免破坏运行时调度；任何需要长生命周期资源的对象都必须通过显式托管接口申请。
//!
//! ## 设计取舍提示
//! * **架构选择：** 通过模块化接口换取统一性，牺牲了直接操作底层 executor 的灵活性，却换来可测试性与跨平台部署能力。
//! * **边界情况：** 合约作者需关注超时、重复调度、以及宿主拒绝服务等边界；本模块接口文档会明确每个 API 的退化行为，便于上层实现补偿逻辑。
//!
//! 运行时能力聚合模块。
//!
//! # 设计综述（Why）
//! - 以跨平台通信内核为目标，将任务调度、计时驱动、基础服务注入抽象解耦。
//! - 对标 Tokio、Actix 等成熟运行时的稳定设计，结合学术界关于可观测调度的最新研究结果。
//!
//! # 模块结构（How）
//! - `task`：定义任务的生命周期语义与句柄契约。
//! - `executor`：封装运行时代办的任务提交流程与优先级调度参数。
//! - `timer`：统一时间原语，支持单调时间点与延迟等待。
//! - `services`：声明运行时向上层暴露的依赖注入集合。
//! - `sugar`：提供语法糖层，帮助业务以零样板方式绑定 [`contract::CallContext`](crate::contract::CallContext) 与运行时能力。
//!
//! # 使用契约（What）
//! - 宿主需实现 [`TaskExecutor`] 与 [`TimeDriver`]，并通过 [`AsyncRuntime`] 聚合。
//! - 运行时所有对象须满足 `Send + Sync + 'static`，以支撑多线程与 `no_std + alloc` 环境。
//!
//! # 风险提示（Trade-offs）
//! - 聚合接口强调上下文传播，执行器实现必须在 `spawn` 中处理 [`CallContext`](crate::contract::CallContext)；
//!   若运行时直接委托第三方执行器（如 Tokio），需自行封装一层以保证取消/截止信号不会丢失。
//! - 若使用 `spawn_local` 运行 `!Send` 任务，请确保宿主线程生命周期覆盖任务执行窗口（该能力需运行时自行扩展）。

mod executor;
mod hotreload;
mod services;
mod slo;
pub mod sugar;
mod task;
mod timer;

/// 重导出治理层的超时运行时视图，保持 `crate::runtime::Timeout*` 兼容。
pub use crate::governance::timeout::runtime::{
    TimeoutConfigError, TimeoutRuntimeConfig, TimeoutSettings,
};
pub use executor::TaskExecutor;
pub(crate) use hotreload::HotReloadObservability;
pub use hotreload::{HotReloadApplyTimer, HotReloadFence, HotReloadReadGuard, HotReloadWriteGuard};
pub use services::CoreServices;
pub use slo::{
    SloPolicyAction, SloPolicyConfigError, SloPolicyDirective, SloPolicyManager,
    SloPolicyReloadReport, SloPolicyRule, SloPolicyTrigger, slo_policy_table_key,
};
pub use sugar::{CallContext, PipelineContextCaps, RuntimeCaps, spawn_in};
pub use task::{
    BlockingTaskSubmission, JoinHandle, LocalTaskSubmission, ManagedBlockingTask, ManagedLocalTask,
    ManagedSendTask, SendTaskSubmission, TaskCancellationStrategy, TaskError, TaskHandle,
    TaskLaunchOptions, TaskPriority, TaskResult,
};
pub use timer::{MonotonicTimePoint, TimeDriver};

/// `AsyncRuntime` 聚合任务调度与时间驱动能力。
///
/// # 设计背景（Why）
/// - 维持运行时最小组合接口，既可对接成熟执行器（Tokio/async-std），亦方便在嵌入式场景自定义实现。
/// - 聚焦跨平台通信需求，保持与操作系统/硬件耦合度最低。
///
/// # 契约说明（What）
/// - **前置条件**：实现必须同时满足 [`TaskExecutor`] 与 [`TimeDriver`] 的契约，并具备线程安全性。
/// - **后置条件**：通过该 trait 注入 [`CoreServices`] 后，上层可假定任务调度与计时功能随时可用。
///
/// # 风险提示（Trade-offs）
/// - 该 trait 不额外声明新方法，便于以 `dyn AsyncRuntime` 作为统一注入入口；如需扩展请定义包裹结构体。
pub trait AsyncRuntime:
    TaskExecutor + TimeDriver + Send + Sync + 'static + crate::sealed::Sealed
{
}

impl<T> AsyncRuntime for T where T: TaskExecutor + TimeDriver + Send + Sync + 'static {}
