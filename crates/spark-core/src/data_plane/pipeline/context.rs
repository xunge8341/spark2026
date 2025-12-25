use super::{
    channel::Channel,
    pipeline::{Pipeline, PipelineHandleId},
};
use crate::{
    buffer::{BufferPool, PipelineMessage},
    cluster::{ClusterMembership, ServiceDiscovery},
    context::Context as ExecutionView,
    contract::{CallContext, CloseReason, Deadline},
    observability::{Logger, MetricsProvider, TraceContext},
    runtime::{TaskExecutor, TimeDriver},
    sealed::Sealed,
};
use alloc::sync::Arc;

/// Handler 访问运行时能力与事件流的统一入口。
///
/// # 契约维度速览
/// - **语义**：提供 `channel`、`pipeline`、`executor` 等接口，让 Handler 能够读写消息、调度异步任务并访问核心服务。
/// - **错误**：写入失败返回 [`CoreError`](crate::CoreError)，常见错误码：`pipeline.write_closed`、`pipeline.buffer_unavailable`。
/// - **并发**：Trait 要求 `Send + Sync`；引用可跨线程短期借用，但需遵守生命周期约束并避免长期持有裸引用。
/// - **背压**：`write` 返回 [`WriteSignal`](super::WriteSignal)，调用方应依据其中的 [`BackpressureSignal`](crate::contract::BackpressureSignal) 调整速率。
/// - **超时**：通过 `call_context().deadline()` 与 `timer()` 协作，确保 Handler 在等待外部操作时能主动超时。
/// - **取消**：`call_context()` 暴露 [`Cancellation`](crate::contract::Cancellation)，长耗时逻辑需轮询取消位并提前退出。
/// - **观测标签**：统一在日志/指标中附带 `pipeline.stage`、`pipeline.handler`、`pipeline.event`，使用 `trace_context()` 与 `metrics()` 打点。
/// - **示例(伪码)**：
///   ```text
///   if ctx.call_context().cancellation().is_cancelled() { return; }
///   let pool = ctx.buffer_pool();
///   let mut buf = pool.acquire(1024)?;
///   encode(&mut buf)?;
///   ctx.write(PipelineMessage::from(buf.freeze()))?;
///   ```
///
/// # 设计背景（Why）
/// - 融合 Netty `ChannelHandlerContext`、Tower `ServiceContext`、Envoy Filter Callback、Akka `ActorContext` 的设计理念，提供集中化入口减少耦合。
/// - 通过对象安全 Trait，支持动态装配 Handler/Middleware，同时保留 `no_std` 可用性。
///
/// # 契约说明（What）
/// - `channel` / `pipeline`：返回当前连接与调度核心引用，便于查询状态或转发事件。
/// - `executor` / `timer`：异步调度能力，保障 Handler 中长耗时操作不会阻塞事件循环。
/// - `buffer_pool`：租借编解码缓冲，需遵循“租借即还”原则。
/// - `trace_context` / `metrics` / `logger`：可观测性三件套，方便 Handler 打点、打日志、串联分布式追踪。
/// - `membership` / `discovery`：分布式能力入口，允许 Handler 做路由或副本选择。
/// - `forward_read`：继续向后传递读事件，遵循责任链模式。
/// - `write` / `flush` / `close_graceful`：与 [`Channel`] 一致的写与关闭语义。
///
/// # 前置/后置条件（Contract）
/// - **前置**：调用者应在事件回调内部使用 Context；跨线程持有引用需要实现保证线程安全。
/// - **后置**：`write` 返回 [`crate::pipeline::WriteSignal`]，调用方需根据反馈调整速率；`close_graceful` 必须确保控制器收到关闭事件。
///
/// # 线程安全与生命周期说明
/// - Trait 约束为 `Send + Sync` 而非 `'static`：上下文仅在单次事件调度中存活，由控制器管理释放；
/// - `channel()` / `pipeline()` 返回的引用可跨线程复用，因为底层对象满足 `Send + Sync + 'static`；
/// - 若实现需要在事件回调外持有 Context，应先克隆必要的数据（如 `Arc`），避免悬垂引用。
///
/// # 风险提示（Trade-offs）
/// - 若实现使用 `Rc`/`RefCell` 等单线程结构，将无法满足 `Send + Sync` 要求，应在构造阶段检测。
/// - `forward_read` 在 Handler 链中是立即调用的同步行为，重计算或阻塞逻辑应移交给 `executor()`。
pub trait Context: Send + Sync + Sealed {
    /// 当前通道引用。
    fn channel(&self) -> &dyn Channel;

    /// 当前 Pipeline 引用。
    ///
    /// # 教案级说明
    /// - **意图（Why）**：Handler 通过该接口访问 Pipeline 控制面，以便在事件流中执行热插拔、广播等高级操作。
    /// - **逻辑（How）**：返回值为 `Arc<dyn Pipeline>` 的共享引用，调用方若需长期持有可自行 `Arc::clone`；上下文自身仅负责暴露视图，不介入生命周期管理。
    /// - **契约（What）**：
    ///   - **输入**：隐式输入为当前上下文 `self`，必须已绑定到具体的 Pipeline 调度实例；
    ///   - **输出**：返回指向 Pipeline 的共享指针视图，保证对象满足 `Send + Sync + 'static`；
    ///   - **前置条件**：调用前 Pipeline 必须已完成初始化并绑定到上下文；
    ///   - **后置条件**：调用不会改变 Pipeline 状态，但允许调用方进一步触发调度；
    /// - **权衡（Trade-offs）**：选择返回 `&Arc<_>` 而非 `&dyn _`，以便 Handler 在必要时克隆强引用，避免临时构造 `Arc` 带来的额外分配。
    fn pipeline(&self) -> &Arc<dyn Pipeline<HandleId = PipelineHandleId>>;

    /// 执行器引用。
    fn executor(&self) -> &dyn TaskExecutor;

    /// 计时器引用。
    fn timer(&self) -> &dyn TimeDriver;

    /// 缓冲池访问接口。
    ///
    /// # 契约说明
    /// - 返回值必须实现 [`BufferPool`]，供 Handler 在编解码过程中租借/归还缓冲。
    /// - 调用方不得缓存引用超过事件回调生命周期，避免破坏池的自适应调度。
    fn buffer_pool(&self) -> &dyn BufferPool;

    /// 链路追踪上下文。
    fn trace_context(&self) -> &TraceContext;

    /// 指标提供者。
    fn metrics(&self) -> &dyn MetricsProvider;

    /// 日志器。
    fn logger(&self) -> &dyn Logger;

    /// 集群成员能力。
    fn membership(&self) -> Option<&dyn ClusterMembership>;

    /// 服务发现能力。
    fn discovery(&self) -> Option<&dyn ServiceDiscovery>;

    /// 当前调用上下文，携带取消/截止/预算等统一契约。
    fn call_context(&self) -> &CallContext;

    /// 快速派生元数据视图，减少热路径中的上下文克隆。
    ///
    /// # 设计背景（Why）
    /// - Handler 在执行 `poll_ready`、预算检查或衍生追踪 span 时，只需访问取消/截止/预算/追踪/身份等元数据，
    ///   若强制克隆整份 [`CallContext`] 将带来多余的 `Arc` 管理成本。
    ///
    /// # 契约说明（What）
    /// - **前置条件**：`self` 必须保证返回的 [`CallContext`] 在 [`crate::context::Context`] 生命周期内有效；
    /// - **后置条件**：返回值为 [`crate::context::Context`] 只读视图，禁止通过该视图修改预算或取消状态。
    fn execution_context(&self) -> ExecutionView<'_> {
        self.call_context().execution()
    }

    /// 继续向后传播读事件。
    fn forward_read(&self, msg: PipelineMessage);

    /// 写消息。
    fn write(&self, msg: PipelineMessage) -> crate::Result<super::WriteSignal, crate::CoreError>;

    /// 刷新缓冲。
    fn flush(&self);

    /// 优雅关闭。
    fn close_graceful(&self, reason: CloseReason, deadline: Option<Deadline>);

    /// 等待关闭完成。
    fn closed(&self) -> crate::future::BoxFuture<'static, crate::Result<(), crate::SparkError>>;
}
