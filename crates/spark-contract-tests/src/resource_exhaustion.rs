use crate::case::{TckCase, TckSuite};
use parking_lot::Mutex;
use spark_core as spark_router;
use spark_core::buffer::{BufferPool, PoolStats, WritableBuffer};
use spark_core::contract::{CallContext, CloseReason};
use spark_core::error::codes;
use spark_core::observability::{CoreUserEvent, Logger, MetricsProvider, TraceContext, TraceFlags};
use spark_core::status::{BusyReason, ReadyState};
use spark_core::test_stubs::observability::{NoopLogger, NoopMetricsProvider};
use spark_core::{CoreError, PipelineMessage, SparkError, future::BoxFuture};
use spark_router::pipeline::channel::ChannelState;
use spark_router::pipeline::controller::PipelineHandleId;
use spark_router::pipeline::default_handlers::{ExceptionAutoResponder, ReadyStateEvent};
use spark_router::pipeline::handler::{InboundHandler, OutboundHandler};
use spark_router::pipeline::{
    Channel, Context, ExtensionsMap, HandlerRegistry, Pipeline, WriteSignal,
};
use std::any::TypeId;
use std::sync::Arc;
use std::time::Duration;

const CASES: &[TckCase] = &[
    TckCase {
        name: "channel_queue_exhaustion_emits_busy_then_retry_after",
        test: channel_queue_exhaustion_emits_busy_then_retry_after,
    },
    TckCase {
        name: "thread_pool_starvation_emits_busy_then_retry_after",
        test: thread_pool_starvation_emits_busy_then_retry_after,
    },
];

const SUITE: TckSuite = TckSuite {
    name: "resource_exhaustion",
    cases: CASES,
};

/// 返回“资源枯竭退避”主题套件。
///
/// # 教案式说明
/// - **意图 (Why)**：Slowloris 以外的另一个典型风险是队列/线程池耗尽，本套件验证框架是否会在资源紧张时
///   广播 `ReadyState::Busy` 与 `ReadyState::RetryAfter`，帮助上游实现退避。
/// - **逻辑 (How)**：复用 `ExceptionAutoResponder`，分别注入“下游队列溢出”和“上游线程池饱和”场景对应的
///   错误码，记录控制器广播的 ReadyState 序列。
/// - **契约 (What)**：调用方可使用 `suite()` 注册到宏中，获取统一的资源枯竭合约保障。
pub const fn suite() -> &'static TckSuite {
    &SUITE
}

/// 队列容量不足应先广播 Downstream Busy，再提供 180ms 的退避建议。
///
/// # 教案式拆解
/// - **意图 (Why)**：确保在限流器判定下游拥塞时，上游能够即时感知 `Busy(Downstream)`，并遵循矩阵给出的
///   退避窗口，避免继续向满载队列写入。
/// - **步骤 (How)**：
///   1. 使用 `APP_BACKPRESSURE_APPLIED` 错误码触发自动响应；
///   2. 收集控制器记录的 ReadyState；
///   3. 断言首个事件为 `Busy(Downstream)`，随后跟随 `RetryAfter(180ms, "下游正在施加背压，遵循等待窗口")`。
/// - **契约 (What)**：ReadyState 顺序与错误矩阵完全一致，退避理由文本准确传递。
fn channel_queue_exhaustion_emits_busy_then_retry_after() {
    let harness = ReadyHarness::new(CallContext::builder().build());
    let error = CoreError::new(codes::APP_BACKPRESSURE_APPLIED, "downstream saturated");

    ExceptionAutoResponder::new().on_exception_caught(&harness.ctx, error);

    let states = harness.ready_states();
    assert_eq!(
        states.len(),
        2,
        "队列溢出场景应产生 Busy + RetryAfter 两个事件"
    );

    match &states[0] {
        ReadyState::Busy(reason) => {
            assert_eq!(reason, &BusyReason::downstream(), "必须标注下游拥塞");
        }
        other => panic!("首个事件应为 Busy(Downstream)，得到 {other:?}"),
    }

    match &states[1] {
        ReadyState::RetryAfter(advice) => {
            assert_eq!(advice.wait, Duration::from_millis(180));
            assert_eq!(
                advice.reason.as_deref(),
                Some("下游正在施加背压，遵循等待窗口"),
                "退避提示应向上游解释等待原因"
            );
        }
        other => panic!("第二个事件应为 RetryAfter，得到 {other:?}"),
    }
}

/// 上游线程池饱和时应广播 Busy(Upstream) 并建议 250ms 退避。
///
/// # 教案式拆解
/// - **意图 (Why)**：当执行器或上游节点不可用时，需要向调用者解释是“上游”瓶颈，并给出统一的重试节奏，
///   防止大量请求在失败后立即重试导致雪崩。
/// - **步骤 (How)**：
///   1. 构造 `CLUSTER_NODE_UNAVAILABLE` 错误；
///   2. 触发自动响应并收集 ReadyState；
///   3. 断言事件顺序为 `Busy(Upstream)` → `RetryAfter(250ms, "集群节点暂不可用，稍后重试")`。
/// - **契约 (What)**：广播内容与 `error-category-matrix` 一致，证明框架在资源枯竭时能向上游提供统一语义。
fn thread_pool_starvation_emits_busy_then_retry_after() {
    let harness = ReadyHarness::new(CallContext::builder().build());
    let error = CoreError::new(codes::CLUSTER_NODE_UNAVAILABLE, "upstream saturated");

    ExceptionAutoResponder::new().on_exception_caught(&harness.ctx, error);

    let states = harness.ready_states();
    assert_eq!(states.len(), 2, "线程池枯竭应产生 Busy + RetryAfter 信号");

    match &states[0] {
        ReadyState::Busy(reason) => {
            assert_eq!(reason, &BusyReason::upstream(), "必须标注上游饱和");
        }
        other => panic!("首个事件应为 Busy(Upstream)，得到 {other:?}"),
    }

    match &states[1] {
        ReadyState::RetryAfter(advice) => {
            assert_eq!(advice.wait, Duration::from_millis(250));
            assert_eq!(
                advice.reason.as_deref(),
                Some("集群节点暂不可用，稍后重试"),
                "退避文案应提示上游节点不可用"
            );
        }
        other => panic!("第二个事件应为 RetryAfter，得到 {other:?}"),
    }
}

/// 测试环境封装：聚合控制器、通道与上下文，便于复用 ReadyState 记录逻辑。
struct ReadyHarness {
    controller: Arc<ReadyRecordingController>,
    ctx: ReadyContext,
}

impl ReadyHarness {
    fn new(call_context: CallContext) -> Self {
        let controller = Arc::new(ReadyRecordingController::default());
        let channel = Arc::new(ReadyChannel::new(Arc::clone(&controller)));
        let ctx = ReadyContext::new(Arc::clone(&controller), Arc::clone(&channel), call_context);
        Self { controller, ctx }
    }

    fn ready_states(&self) -> Vec<ReadyState> {
        self.controller.ready_states.lock().clone()
    }
}

#[derive(Default)]
struct ReadyRecordingController {
    ready_states: Mutex<Vec<ReadyState>>,
}

impl Pipeline for ReadyRecordingController {
    type HandleId = PipelineHandleId;

    fn register_inbound_handler(&self, _: &str, _: Box<dyn InboundHandler>) {}

    fn register_outbound_handler(&self, _: &str, _: Box<dyn OutboundHandler>) {}

    fn install_middleware(
        &self,
        _: &dyn spark_router::pipeline::PipelineInitializer,
        _: &spark_core::runtime::CoreServices,
    ) -> spark_core::Result<(), CoreError> {
        Ok(())
    }

    fn emit_channel_activated(&self) {}

    fn emit_read(&self, _: PipelineMessage) {}

    fn emit_read_completed(&self) {}

    fn emit_writability_changed(&self, _: bool) {}

    fn emit_user_event(&self, event: CoreUserEvent) {
        if let Some(ready) = event.downcast_application_event::<ReadyStateEvent>() {
            self.ready_states.lock().push(ready.state().clone());
        }
    }

    fn emit_exception(&self, _: CoreError) {}

    fn emit_channel_deactivated(&self) {}

    fn registry(&self) -> &dyn HandlerRegistry {
        &EmptyRegistry
    }

    fn add_handler_after(
        &self,
        anchor: Self::HandleId,
        _: &str,
        _: Arc<dyn spark_router::pipeline::controller::Handler>,
    ) -> Self::HandleId {
        anchor
    }

    fn remove_handler(&self, _: Self::HandleId) -> bool {
        false
    }

    fn replace_handler(
        &self,
        _: Self::HandleId,
        _: Arc<dyn spark_router::pipeline::controller::Handler>,
    ) -> bool {
        false
    }

    fn epoch(&self) -> u64 {
        0
    }
}

struct EmptyRegistry;

impl HandlerRegistry for EmptyRegistry {
    fn snapshot(&self) -> Vec<spark_router::pipeline::controller::HandlerRegistration> {
        Vec::new()
    }
}

struct ReadyChannel {
    controller: Arc<ReadyRecordingController>,
    extensions: ReadyExtensions,
}

impl ReadyChannel {
    fn new(controller: Arc<ReadyRecordingController>) -> Self {
        Self {
            controller,
            extensions: ReadyExtensions,
        }
    }
}

impl Channel for ReadyChannel {
    fn id(&self) -> &str {
        "resource-harness-channel"
    }

    fn state(&self) -> ChannelState {
        ChannelState::Active
    }

    fn is_writable(&self) -> bool {
        true
    }

    fn controller(&self) -> &dyn Pipeline<HandleId = PipelineHandleId> {
        &*self.controller
    }

    fn extensions(&self) -> &dyn ExtensionsMap {
        &self.extensions
    }

    fn peer_addr(&self) -> Option<spark_core::transport::TransportSocketAddr> {
        None
    }

    fn local_addr(&self) -> Option<spark_core::transport::TransportSocketAddr> {
        None
    }

    fn close_graceful(&self, _: CloseReason, _: Option<spark_core::contract::Deadline>) {}

    fn close(&self) {}

    fn closed(&self) -> BoxFuture<'static, spark_core::Result<(), SparkError>> {
        Box::pin(async { Ok(()) })
    }

    fn write(&self, _: PipelineMessage) -> spark_core::Result<WriteSignal, CoreError> {
        Ok(WriteSignal::Accepted)
    }

    fn flush(&self) {}
}

#[derive(Copy, Clone)]
struct ReadyExtensions;

impl ExtensionsMap for ReadyExtensions {
    fn insert(&self, _: TypeId, _: Box<dyn std::any::Any + Send + Sync>) {}

    fn get<'a>(&'a self, _: &TypeId) -> Option<&'a (dyn std::any::Any + Send + Sync + 'static)> {
        None
    }

    fn remove(&self, _: &TypeId) -> Option<Box<dyn std::any::Any + Send + Sync>> {
        None
    }

    fn contains_key(&self, _: &TypeId) -> bool {
        false
    }

    fn clear(&self) {}
}

struct ReadyContext {
    controller_view: Arc<dyn Pipeline<HandleId = PipelineHandleId>>,
    channel: Arc<ReadyChannel>,
    buffer_pool: NoopBufferPool,
    logger: NoopLogger,
    metrics: NoopMetricsProvider,
    executor: NoopExecutor,
    timer: NoopTimer,
    call_context: CallContext,
    trace: TraceContext,
}

impl ReadyContext {
    fn new(
        controller: Arc<ReadyRecordingController>,
        channel: Arc<ReadyChannel>,
        call_context: CallContext,
    ) -> Self {
        let trace = TraceContext::new(
            [0x11; TraceContext::TRACE_ID_LENGTH],
            [0x22; TraceContext::SPAN_ID_LENGTH],
            TraceFlags::new(TraceFlags::SAMPLED),
        );
        let controller_view: Arc<dyn Pipeline<HandleId = PipelineHandleId>> =
            controller.clone() as Arc<dyn Pipeline<HandleId = PipelineHandleId>>;
        Self {
            controller_view,
            channel,
            buffer_pool: NoopBufferPool,
            logger: NoopLogger,
            metrics: NoopMetricsProvider,
            executor: NoopExecutor,
            timer: NoopTimer,
            call_context,
            trace,
        }
    }
}

impl Context for ReadyContext {
    fn channel(&self) -> &dyn Channel {
        self.channel.as_ref()
    }

    fn pipeline(&self) -> &Arc<dyn Pipeline<HandleId = PipelineHandleId>> {
        &self.controller_view
    }

    fn executor(&self) -> &dyn spark_core::runtime::TaskExecutor {
        &self.executor
    }

    fn timer(&self) -> &dyn spark_core::runtime::TimeDriver {
        &self.timer
    }

    fn buffer_pool(&self) -> &dyn BufferPool {
        &self.buffer_pool
    }

    fn trace_context(&self) -> &TraceContext {
        &self.trace
    }

    fn metrics(&self) -> &dyn MetricsProvider {
        &self.metrics
    }

    fn logger(&self) -> &dyn Logger {
        &self.logger
    }

    fn membership(&self) -> Option<&dyn spark_core::cluster::ClusterMembership> {
        None
    }

    fn discovery(&self) -> Option<&dyn spark_core::cluster::ServiceDiscovery> {
        None
    }

    fn call_context(&self) -> &CallContext {
        &self.call_context
    }

    fn forward_read(&self, _: PipelineMessage) {}

    fn write(&self, msg: PipelineMessage) -> spark_core::Result<WriteSignal, CoreError> {
        self.channel.write(msg)
    }

    fn flush(&self) {
        self.channel.flush();
    }

    fn close_graceful(
        &self,
        reason: CloseReason,
        deadline: Option<spark_core::contract::Deadline>,
    ) {
        self.channel.close_graceful(reason, deadline);
    }

    fn closed(&self) -> BoxFuture<'static, spark_core::Result<(), SparkError>> {
        self.channel.closed()
    }
}

struct NoopBufferPool;

impl BufferPool for NoopBufferPool {
    fn acquire(&self, _: usize) -> spark_core::Result<Box<dyn WritableBuffer>, CoreError> {
        Err(CoreError::new("buffer.disabled", "buffer pool disabled"))
    }

    fn shrink_to_fit(&self) -> spark_core::Result<usize, CoreError> {
        Ok(0)
    }

    fn statistics(&self) -> spark_core::Result<PoolStats, CoreError> {
        Ok(PoolStats::default())
    }
}

struct NoopExecutor;

impl spark_core::runtime::TaskExecutor for NoopExecutor {
    fn spawn_dyn(
        &self,
        _: &CallContext,
        _: BoxFuture<'static, spark_core::runtime::TaskResult<Box<dyn std::any::Any + Send>>>,
    ) -> spark_core::runtime::JoinHandle<Box<dyn std::any::Any + Send>> {
        spark_core::runtime::JoinHandle::from_task_handle(Box::new(NoopTaskHandle))
    }
}

struct NoopTaskHandle;

#[spark_core::async_trait]
impl spark_core::runtime::TaskHandle for NoopTaskHandle {
    type Output = Box<dyn std::any::Any + Send>;

    fn cancel(&self, _: spark_core::runtime::TaskCancellationStrategy) {}

    fn is_finished(&self) -> bool {
        true
    }

    fn is_cancelled(&self) -> bool {
        false
    }

    fn id(&self) -> Option<&str> {
        None
    }

    fn detach(self: Box<Self>) {}

    async fn join(self: Box<Self>) -> spark_core::runtime::TaskResult<Self::Output> {
        Ok(Box::new(()) as Box<dyn std::any::Any + Send>)
    }
}

struct NoopTimer;

#[spark_core::async_trait]
impl spark_core::runtime::TimeDriver for NoopTimer {
    fn now(&self) -> spark_core::runtime::MonotonicTimePoint {
        spark_core::runtime::MonotonicTimePoint::from_offset(Duration::from_millis(0))
    }

    async fn sleep(&self, _: Duration) {}
}
