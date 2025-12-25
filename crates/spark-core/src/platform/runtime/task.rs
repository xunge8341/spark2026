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
use crate::{BoxFuture, LocalBoxFuture, async_trait, sealed::Sealed};
use alloc::{borrow::Cow, boxed::Box, string::String};
use core::fmt;

/// `TaskPriority` 描述调度器参考的优先级队列等级。
///
/// # 设计背景（Why）
/// - 借鉴操作系统中的多级反馈队列（MLFQ）与业界运行时（Tokio、Seastar），提供统一的优先级语义。
///
/// # 逻辑解析（How）
/// - 采用离散的五档等级，既能覆盖服务端常见场景，也便于实现者映射到不同的调度策略。
///
/// # 契约说明（What）
/// - 提交任务时默认使用 [`TaskPriority::Normal`]；实现者可根据内部策略映射到权重或队列索引。
///
/// # 风险提示（Trade-offs）
/// - 过度依赖优先级可能导致饥饿；实现者应结合老化策略避免长期低优先级任务饿死。
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
#[non_exhaustive]
pub enum TaskPriority {
    Critical,
    High,
    #[default]
    Normal,
    Low,
    Idle,
}

/// `TaskCancellationStrategy` 表达取消行为的强度。
///
/// # 设计背景（Why）
/// - 协作取消（cooperative cancellation）是跨平台运行时的共识，但在关机场景仍需要强制取消选项。
///
/// # 契约说明（What）
/// - `Cooperative` 要求任务自行检查取消信号；`Forceful` 则允许运行时立即终止。
///
/// # 风险提示（Trade-offs）
/// - 选择 `Forceful` 可能导致任务绕过析构流程；调用方需确保资源已通过其它途径安全释放。
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
#[non_exhaustive]
pub enum TaskCancellationStrategy {
    #[default]
    Cooperative,
    Forceful,
}

/// `TaskLaunchOptions` 汇总提交任务时的元信息。
///
/// # 设计背景（Why）
/// - 将命名、优先级、取消策略、链路追踪标签等元信息统一封装，便于执行器做可观测性增强。
///
/// # 契约说明（What）
/// - `name`：用于日志与指标聚合；建议使用静态字符串，便于编译期内联。
/// - `priority`：调度器参考优先级，默认为普通级别。
/// - `cancellation`：取消策略，默认为协作取消。
/// - `tracing_labels`：额外的标签信息，采用 `String` 以兼容运行时动态拼接。
///
/// # 前置条件
/// - 调用方应确保 `name` 对于同类任务具有稳定含义，避免可观测数据碎片化。
///
/// # 后置条件
/// - 运行时可假定 `TaskLaunchOptions` 在任务生命周期内保持不变，可安全缓存引用。
#[derive(Clone, Debug, Default)]
pub struct TaskLaunchOptions {
    pub name: Option<Cow<'static, str>>,
    pub priority: TaskPriority,
    pub cancellation: TaskCancellationStrategy,
    pub tracing_labels: Option<String>,
}

impl TaskLaunchOptions {
    /// 以给定任务名构造配置。
    pub fn named(name: impl Into<Cow<'static, str>>) -> Self {
        TaskLaunchOptions {
            name: Some(name.into()),
            ..Default::default()
        }
    }
}

/// `TaskResult` 统一表示任务执行结果。
///
/// # 契约说明（What）
/// - `Ok(T)`：任务成功完成并返回值。
/// - `Err(TaskError)`：任务取消、失败或执行器故障。
pub type TaskResult<T = ()> = crate::Result<T, TaskError>;

/// `TaskError` 枚举任务失败原因。
///
/// # 设计背景（Why）
/// - 吸收 Tokio `JoinError`、Fuchsia `TaskError` 等设计，区分取消、执行异常、执行器关闭等常见情况。
///
/// # 风险提示（Trade-offs）
/// - `Panicked` 仅包含静态信息；实际运行时若需捕获 panic payload，需额外在宿主实现层处理。
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum TaskError {
    Cancelled,
    Panicked,
    ExecutorTerminated,
    Failed(Cow<'static, str>),
}

impl fmt::Display for TaskError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TaskError::Cancelled => write!(f, "task cancelled"),
            TaskError::Panicked => write!(f, "task panicked"),
            TaskError::ExecutorTerminated => write!(f, "executor terminated"),
            TaskError::Failed(reason) => write!(f, "task failed: {reason}"),
        }
    }
}

/// `TaskHandle` 定义运行时返回的任务控制句柄。
///
/// # 设计背景（Why）
/// - 本次任务要求所有异步作业必须与 [`CallContext`](crate::contract::CallContext) 绑定，以确保取消/截止/预算三元组可在执行器内部传递。
///   为此我们需要一个能够跨线程携带泛型输出的控制面接口，既保留对象安全的灵活性，又不丢失类型信息。
/// - 设计上沿袭 Tokio `JoinHandle` 的经验，同时兼顾 `no_std + alloc` 环境中宿主实现的自由度，避免强制依赖具体运行时库。
///
/// # 逻辑解析（How）
/// - 通过关联类型 `Output` 表达任务成功完成时的返回值类型；
/// - 同步方法 `cancel`/`is_finished`/`is_cancelled`/`id` 允许调度器在无需 `await` 的情况下观测任务状态或注入信号；
/// - `detach` 释放控制权，`join` 则以异步方式返回 [`TaskResult<Self::Output>`]，统一错误语义。
///
/// # 契约说明（What）
/// - **前置条件**：实现者需确保内部持有对任务生命周期的引用（常见做法是引用计数或运行时句柄），保证在 `detach` 后任务仍可运行；
///   同时必须保证 `Output: Send + 'static` 以支持跨线程传输。
/// - **后置条件**：`join` 一旦返回，句柄即视为消费完成，后续再访问任何方法都属于未定义行为；实现者应在调试模式下主动检查重复使用。
///
/// # 风险提示（Trade-offs）
/// - 对象安全实现需要装箱 `Future`，在极端低延迟场景可能带来轻微分配开销；若宿主对性能极度敏感，可在其内部提供特化路径并在此 Trait 上封装。
/// - `cancel` 的语义仅为“请求取消”，并不保证立即终止；调用方应结合 `CallContext` 中的预算与截止信息自行兜底。
#[async_trait]
pub trait TaskHandle: Send + Sync + Sealed {
    /// 任务完成时返回的值类型。
    type Output: Send + 'static;

    /// 注入取消信号。
    fn cancel(&self, strategy: TaskCancellationStrategy);

    /// 查询任务是否已完成。
    fn is_finished(&self) -> bool;

    /// 查询任务是否因取消而结束。
    fn is_cancelled(&self) -> bool;

    /// 返回调试用的标识符。
    fn id(&self) -> Option<&str>;

    /// 在不等待结果的情况下释放控制权。
    fn detach(self: Box<Self>);

    /// 等待任务完成并返回统一的执行结果。
    async fn join(self: Box<Self>) -> TaskResult<Self::Output>;
}

/// `JoinHandle` 为 [`TaskExecutor::spawn`](super::executor::TaskExecutor::spawn) 的标准返回类型。
///
/// # 设计背景（Why）
/// - 向调用者暴露与 Tokio/async-std 等运行时一致的 `JoinHandle` 语义，降低学习成本；
/// - 通过封装内部的 [`TaskHandle`] 对象，兼顾对象安全实现（对运行时友好）与泛型化接口（对调用方友好）。
///
/// # 逻辑解析（How）
/// - 内部保存一个一次性消费的 [`TaskHandle`] 指针；
/// - `cancel`/`is_finished`/`is_cancelled`/`id` 直接透传给底层句柄，保持零额外分配；
/// - `join` 消费自身并调用底层句柄的异步等待逻辑，统一返回 [`TaskResult<T>`]。
///
/// # 契约说明（What）
/// - **输入**：只能由执行器实现构造，调用方无法直接创建，确保任务生命周期受控；
/// - **前置条件**：除 `detach` 外，其他方法均要求句柄仍持有底层任务；若句柄已被消费将触发 panic，以便尽早暴露错误；
/// - **后置条件**：调用 `join` 或 `detach` 后句柄即失效，后续操作均会 panic。
///
/// # 风险提示（Trade-offs）
/// - 由于内部持有动态分发指针，`JoinHandle` 默认不自动实现 `Clone`，避免误用导致任务被多次控制；
/// - 若运行时以强制终止方式实现 `cancel`，需在实现文档中注明，以免调用方误以为遵循协作式取消。
pub struct JoinHandle<T> {
    handle: Option<Box<dyn TaskHandle<Output = T>>>,
}

impl<T: Send + 'static> JoinHandle<T> {
    /// 供执行器实现将内部 [`TaskHandle`] 封装为通用句柄。
    pub fn from_task_handle(handle: Box<dyn TaskHandle<Output = T>>) -> Self {
        Self {
            handle: Some(handle),
        }
    }

    /// 请求运行时取消任务。
    pub fn cancel(&self, strategy: TaskCancellationStrategy) {
        self.inner().cancel(strategy);
    }

    /// 查询任务是否已完成。
    pub fn is_finished(&self) -> bool {
        self.inner().is_finished()
    }

    /// 查询任务是否因取消而结束。
    pub fn is_cancelled(&self) -> bool {
        self.inner().is_cancelled()
    }

    /// 返回运行时分配的任务标识。
    pub fn id(&self) -> Option<&str> {
        self.inner().id()
    }

    /// 分离控制权，允许任务在后台继续运行。
    pub fn detach(mut self) {
        if let Some(handle) = self.handle.take() {
            handle.detach();
        }
    }

    /// 等待任务完成并返回统一的执行结果。
    pub async fn join(mut self) -> TaskResult<T> {
        let handle = self
            .handle
            .take()
            .expect("JoinHandle::join 在句柄已被消费后再次调用");
        handle.join().await
    }

    /// 将任务结果映射为另一种输出类型，便于在类型擦除后恢复具体值。
    pub fn map<U, F>(mut self, mapper: F) -> JoinHandle<U>
    where
        T: Send + 'static,
        U: Send + 'static,
        F: FnOnce(TaskResult<T>) -> TaskResult<U> + Send + Sync + 'static,
    {
        let inner = self.handle.take().expect("JoinHandle 已被消费或分离");
        JoinHandle::from_task_handle(Box::new(MappedHandle {
            inner: Some(inner),
            mapper: Some(mapper),
        }))
    }

    fn inner(&self) -> &dyn TaskHandle<Output = T> {
        self.handle.as_deref().expect("JoinHandle 已被消费或分离")
    }
}

struct MappedHandle<T, U, F>
where
    F: FnOnce(TaskResult<T>) -> TaskResult<U> + Send + Sync + 'static,
{
    inner: Option<Box<dyn TaskHandle<Output = T>>>,
    mapper: Option<F>,
}

#[async_trait]
impl<T, U, F> TaskHandle for MappedHandle<T, U, F>
where
    T: Send + 'static,
    U: Send + 'static,
    F: FnOnce(TaskResult<T>) -> TaskResult<U> + Send + Sync + 'static,
{
    type Output = U;

    fn cancel(&self, strategy: TaskCancellationStrategy) {
        if let Some(inner) = self.inner.as_ref() {
            inner.cancel(strategy);
        }
    }

    fn is_finished(&self) -> bool {
        self.inner
            .as_ref()
            .map(|handle| handle.is_finished())
            .unwrap_or(true)
    }

    fn is_cancelled(&self) -> bool {
        self.inner
            .as_ref()
            .map(|handle| handle.is_cancelled())
            .unwrap_or(false)
    }

    fn id(&self) -> Option<&str> {
        self.inner.as_ref().and_then(|handle| handle.id())
    }

    fn detach(mut self: Box<Self>) {
        if let Some(inner) = self.inner.take() {
            inner.detach();
        }
    }

    async fn join(mut self: Box<Self>) -> TaskResult<Self::Output> {
        let mapper = self
            .mapper
            .take()
            .expect("MappedHandle::join mapper 已被消费");
        let inner = self
            .inner
            .take()
            .expect("MappedHandle::join inner 已被消费");
        let result = inner.join().await;
        mapper(result)
    }
}

/// `ManagedSendTask` 统一封装跨线程可运行的异步任务。
///
/// # 设计背景（Why）
/// - 采用 struct 包装 Future + 元信息，方便执行器在入队时进行统一的指标上报与调度决策。
///
/// # 前置条件
/// - `future` 必须满足 `Send + 'static`，以允许在多线程执行器中迁移。
///
/// # 后置条件
/// - 提交后运行时拥有任务所有权；调用方不再直接访问 Future。
pub struct ManagedSendTask {
    pub future: BoxFuture<'static, TaskResult>,
    pub options: TaskLaunchOptions,
}

/// `ManagedBlockingTask` 封装阻塞任务委托逻辑。
///
/// # 契约说明（What）
/// - `operation` 返回 `TaskResult`，供执行器统一处理错误。
/// - 运行时应在独立线程池执行该任务，避免阻塞核心调度器线程。
pub struct ManagedBlockingTask {
    pub operation: Box<dyn FnOnce() -> TaskResult + Send + 'static>,
    pub options: TaskLaunchOptions,
}

/// `ManagedLocalTask` 封装 `!Send` 异步任务。
///
/// # 契约说明（What）
/// - `future` 仅需满足 `'static`，可运行在单线程执行器。
/// - 调用方必须确保宿主线程在任务完成前不会退出。
pub struct ManagedLocalTask {
    pub future: LocalBoxFuture<'static, TaskResult>,
    pub options: TaskLaunchOptions,
}

/// `SendTaskSubmission` 为便捷构造器，配合 `Into` 自动转换。
pub struct SendTaskSubmission(pub ManagedSendTask);

impl From<ManagedSendTask> for SendTaskSubmission {
    fn from(value: ManagedSendTask) -> Self {
        SendTaskSubmission(value)
    }
}

/// `BlockingTaskSubmission` 为阻塞任务的便捷包装。
pub struct BlockingTaskSubmission(pub ManagedBlockingTask);

impl From<ManagedBlockingTask> for BlockingTaskSubmission {
    fn from(value: ManagedBlockingTask) -> Self {
        BlockingTaskSubmission(value)
    }
}

/// `LocalTaskSubmission` 为本地任务的便捷包装。
pub struct LocalTaskSubmission(pub ManagedLocalTask);

impl From<ManagedLocalTask> for LocalTaskSubmission {
    fn from(value: ManagedLocalTask) -> Self {
        LocalTaskSubmission(value)
    }
}
