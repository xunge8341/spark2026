use crate::case::{TckCase, TckSuite};
use parking_lot::Mutex;
use spark_core as spark_router;
use spark_core::contract::{CallContext, CallContextBuilder, Cancellation, CloseReason};
use spark_core::error::{CoreError, ErrorCategory};
use spark_core::observability::metrics::MetricsProvider;
use spark_core::observability::{CoreUserEvent, Logger, TraceContext, TraceFlags};
use spark_core::runtime::{
    JoinHandle, MonotonicTimePoint, TaskCancellationStrategy, TaskExecutor, TaskHandle, TaskResult,
    TimeDriver,
};
use spark_core::status::{ReadyState, RetryAdvice};
use spark_core::test_stubs::observability::{NoopLogger, NoopMetricsProvider};
use spark_core::{PipelineMessage, SparkError, future::BoxFuture};
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
        name: "resource_exhausted_produces_budget_ready_state",
        test: resource_exhausted_produces_budget_ready_state,
    },
    TckCase {
        name: "retryable_maps_to_retry_after_signal",
        test: retryable_maps_to_retry_after_signal,
    },
    TckCase {
        name: "security_violation_triggers_graceful_close",
        test: security_violation_triggers_graceful_close,
    },
    TckCase {
        name: "protocol_violation_forces_shutdown",
        test: protocol_violation_forces_shutdown,
    },
    TckCase {
        name: "cancelled_marks_cancellation_token",
        test: cancelled_marks_cancellation_token,
    },
    TckCase {
        name: "timeout_marks_cancellation_token",
        test: timeout_marks_cancellation_token,
    },
    TckCase {
        name: "non_retryable_performs_no_side_effects",
        test: non_retryable_performs_no_side_effects,
    },
];

const SUITE: TckSuite = TckSuite {
    name: "errors",
    cases: CASES,
};

/// 返回“错误自动响应”主题的测试套件。
///
/// # 教案式说明
/// - **意图 (Why)**：验证 `ErrorCategory` 与控制器/通道之间的联动效果是否符合契约。
/// - **逻辑 (How)**：包含七个用例，涵盖预算耗尽、退避建议、安全异常等典型场景。
/// - **契约 (What)**：返回 `'static` 引用供宏调用。
pub const fn suite() -> &'static TckSuite {
    &SUITE
}

/// 验证 ResourceExhausted 分类会广播预算耗尽的 ReadyState。
///
/// # 教案式说明
/// - **意图 (Why)**：调用方需要根据预算耗尽信号施加背压，本测试确保自动响应器能按约定广播。
/// - **逻辑 (How)**：构造仅剩额度为零的预算，触发 `ErrorCategory::ResourceExhausted` 并检查 controller 记录的 ReadyState。
/// - **契约 (What)**：ReadyState 列表仅包含一条 `BudgetExhausted`，且快照字段正确。
fn resource_exhausted_produces_budget_ready_state() {
    let budget = spark_core::types::Budget::new(spark_core::types::BudgetKind::Flow, 1);
    let _ = budget.try_consume(1);
    let ctx = CallContext::builder().add_budget(budget).build();
    let harness = ErrorHarness::new(ctx);
    let error = CoreError::new("test.resource", "budget exhausted").with_category(
        ErrorCategory::ResourceExhausted(spark_core::types::BudgetKind::Flow),
    );

    ExceptionAutoResponder::new().on_exception_caught(&harness.ctx, error);

    let states = harness.ready_states();
    assert_eq!(states.len(), 1);
    match &states[0] {
        ReadyState::BudgetExhausted(snapshot) => {
            assert_eq!(snapshot.limit, 1);
            assert_eq!(snapshot.remaining, 0);
        }
        other => panic!("预期广播 BudgetExhausted，得到 {other:?}"),
    }
}

/// 验证 Retryable 分类会产生 `ReadyState::RetryAfter`。
///
/// # 教案式说明
/// - **意图 (Why)**：重试场景需要明确的退避建议，确保调用链不会立即重试导致雪崩。
/// - **逻辑 (How)**：构造带 150ms 建议的 `RetryAdvice`，触发错误后检查广播列表。
/// - **契约 (What)**：ReadyState 列表应恰好包含指定的 `RetryAfter`。
fn retryable_maps_to_retry_after_signal() {
    let harness = ErrorHarness::new(CallContext::builder().build());
    let advice = RetryAdvice::after(Duration::from_millis(150));
    let error = CoreError::new("test.retry", "try later")
        .with_category(ErrorCategory::Retryable(advice.clone()));

    ExceptionAutoResponder::new().on_exception_caught(&harness.ctx, error);

    let states = harness.ready_states();
    assert_eq!(states, [ReadyState::RetryAfter(advice)]);
}

/// 验证安全违规会触发优雅关闭并携带正确的关闭原因。
///
/// # 教案式说明
/// - **意图 (Why)**：安全域需要在违规时提供明确的关闭原因，以便审计与告警。
/// - **逻辑 (How)**：注入 `Security` 分类错误，读取 channel 记录的关闭原因并断言 code 字段。
/// - **契约 (What)**：关闭原因列表仅包含 `security.violation`。
fn security_violation_triggers_graceful_close() {
    let harness = ErrorHarness::new(CallContext::builder().build());
    let error = CoreError::new("test.security", "unauthorized").with_category(
        ErrorCategory::Security(spark_core::security::SecurityClass::Authorization),
    );

    ExceptionAutoResponder::new().on_exception_caught(&harness.ctx, error);

    let reasons = harness.closed_reasons();
    assert_eq!(reasons.len(), 1);
    assert_eq!(reasons[0].code(), "security.violation");
}

/// 验证协议违规会触发强制关闭。
///
/// # 教案式说明
/// - **意图 (Why)**：当协议解析失败时，链路必须立即关闭，避免继续处理无效数据。
/// - **逻辑 (How)**：触发 `ProtocolViolation` 分类并检查关闭原因。
/// - **契约 (What)**：关闭原因列表仅包含 `protocol.violation`。
fn protocol_violation_forces_shutdown() {
    let harness = ErrorHarness::new(CallContext::builder().build());
    let error = CoreError::new("test.protocol", "bad frame")
        .with_category(ErrorCategory::ProtocolViolation);

    ExceptionAutoResponder::new().on_exception_caught(&harness.ctx, error);

    let reasons = harness.closed_reasons();
    assert_eq!(reasons.len(), 1);
    assert_eq!(reasons[0].code(), "protocol.violation");
}

/// 验证 Cancelled 分类会标记取消令牌。
///
/// # 教案式说明
/// - **意图 (Why)**：上游取消应及时传播到业务逻辑，避免继续执行无意义任务。
/// - **逻辑 (How)**：构造带自定义取消令牌的上下文，触发 `Cancelled` 错误并检查令牌状态。
/// - **契约 (What)**：令牌最终处于取消态。
fn cancelled_marks_cancellation_token() {
    let cancellation = Cancellation::new();
    let ctx = CallContextBuilder::default()
        .with_cancellation(cancellation.clone())
        .build();
    let harness = ErrorHarness::new(ctx);
    let error =
        CoreError::new("test.cancelled", "cancelled").with_category(ErrorCategory::Cancelled);

    ExceptionAutoResponder::new().on_exception_caught(&harness.ctx, error);

    assert!(cancellation.is_cancelled());
}

/// 验证 Timeout 分类同样会标记取消令牌。
///
/// # 教案式说明
/// - **意图 (Why)**：请求超时后需中断后续逻辑，此处检查自动响应器能否及时触发取消。
/// - **逻辑 (How)**：与前一个用例类似，只是分类不同。
/// - **契约 (What)**：令牌最终处于取消态。
fn timeout_marks_cancellation_token() {
    let cancellation = Cancellation::new();
    let ctx = CallContextBuilder::default()
        .with_cancellation(cancellation.clone())
        .build();
    let harness = ErrorHarness::new(ctx);
    let error = CoreError::new("test.timeout", "timeout").with_category(ErrorCategory::Timeout);

    ExceptionAutoResponder::new().on_exception_caught(&harness.ctx, error);

    assert!(cancellation.is_cancelled());
}

/// 验证 NonRetryable 错误不会产生副作用。
///
/// # 教案式说明
/// - **意图 (Why)**：不可重试的错误应保持静默，不应误触发 ReadyState 或关闭通道。
/// - **逻辑 (How)**：触发 `NonRetryable` 分类后检查 ready/close 列表以及取消状态。
/// - **契约 (What)**：无 ReadyState、无关闭原因、取消令牌保持未触发。
fn non_retryable_performs_no_side_effects() {
    let cancellation = Cancellation::new();
    let ctx = CallContextBuilder::default()
        .with_cancellation(cancellation.clone())
        .build();
    let harness = ErrorHarness::new(ctx);
    let error = CoreError::new("test.other", "noop").with_category(ErrorCategory::NonRetryable);

    ExceptionAutoResponder::new().on_exception_caught(&harness.ctx, error);

    assert!(harness.ready_states().is_empty());
    assert!(harness.closed_reasons().is_empty());
    assert!(!cancellation.is_cancelled());
}

/// 错误自动响应测试的环境封装，统一提供控制器、通道与上下文。
struct ErrorHarness {
    controller: Arc<RecordingController>,
    channel: Arc<RecordingChannel>,
    ctx: RecordingContext,
}

impl ErrorHarness {
    /// 构造测试环境并注入指定的 `CallContext`。
    fn new(call_context: CallContext) -> Self {
        let controller = Arc::new(RecordingController::default());
        let channel = Arc::new(RecordingChannel::new(Arc::clone(&controller)));
        let ctx =
            RecordingContext::new(Arc::clone(&controller), Arc::clone(&channel), call_context);
        Self {
            controller,
            channel,
            ctx,
        }
    }

    fn ready_states(&self) -> Vec<ReadyState> {
        self.controller.ready_states.lock().clone()
    }

    fn closed_reasons(&self) -> Vec<CloseReason> {
        self.channel.closed.lock().clone()
    }
}

#[derive(Default)]
struct RecordingController {
    ready_states: Mutex<Vec<ReadyState>>,
}

impl Pipeline for RecordingController {
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

    fn emit_read(&self, _msg: PipelineMessage) {}

    fn emit_read_completed(&self) {}

    fn emit_writability_changed(&self, _is_writable: bool) {}

    fn emit_user_event(&self, event: CoreUserEvent) {
        if let Some(ready) = event.downcast_application_event::<ReadyStateEvent>() {
            self.ready_states.lock().push(ready.state().clone());
        }
    }

    fn emit_exception(&self, _error: CoreError) {}

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

struct RecordingChannel {
    controller: Arc<RecordingController>,
    closed: Mutex<Vec<CloseReason>>,
    extensions: NoopExtensions,
}

impl RecordingChannel {
    fn new(controller: Arc<RecordingController>) -> Self {
        Self {
            controller,
            closed: Mutex::new(Vec::new()),
            extensions: NoopExtensions,
        }
    }
}

impl Channel for RecordingChannel {
    fn id(&self) -> &str {
        "tck-channel"
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

    fn close_graceful(
        &self,
        reason: CloseReason,
        _deadline: Option<spark_core::contract::Deadline>,
    ) {
        self.closed.lock().push(reason);
    }

    fn close(&self) {}

    fn closed(&self) -> BoxFuture<'static, spark_core::Result<(), SparkError>> {
        Box::pin(async { Ok(()) })
    }

    fn write(&self, _msg: PipelineMessage) -> spark_core::Result<WriteSignal, CoreError> {
        Ok(WriteSignal::Accepted)
    }

    fn flush(&self) {}
}

#[derive(Copy, Clone)]
struct NoopExtensions;

impl ExtensionsMap for NoopExtensions {
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

struct RecordingContext {
    controller_view: Arc<dyn Pipeline<HandleId = PipelineHandleId>>,
    channel: Arc<RecordingChannel>,
    buffer_pool: NoopBufferPool,
    logger: NoopLogger,
    metrics: NoopMetricsProvider,
    executor: NoopExecutor,
    timer: NoopTimer,
    call_context: CallContext,
    trace: TraceContext,
}

impl RecordingContext {
    fn new(
        controller: Arc<RecordingController>,
        channel: Arc<RecordingChannel>,
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

impl Context for RecordingContext {
    fn channel(&self) -> &dyn Channel {
        self.channel.as_ref()
    }

    fn pipeline(&self) -> &Arc<dyn Pipeline<HandleId = PipelineHandleId>> {
        &self.controller_view
    }

    fn executor(&self) -> &dyn TaskExecutor {
        &self.executor
    }

    fn timer(&self) -> &dyn TimeDriver {
        &self.timer
    }

    fn buffer_pool(&self) -> &dyn spark_core::buffer::BufferPool {
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

    fn forward_read(&self, _msg: PipelineMessage) {}

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

impl spark_core::buffer::BufferPool for NoopBufferPool {
    fn acquire(
        &self,
        _: usize,
    ) -> spark_core::Result<Box<dyn spark_core::buffer::WritableBuffer>, CoreError> {
        Err(CoreError::new("buffer.disabled", "buffer pool disabled"))
    }

    fn shrink_to_fit(&self) -> spark_core::Result<usize, CoreError> {
        Ok(0)
    }

    fn statistics(&self) -> spark_core::Result<spark_core::buffer::PoolStats, CoreError> {
        Ok(spark_core::buffer::PoolStats {
            allocated_bytes: 0,
            resident_bytes: 0,
            active_leases: 0,
            available_bytes: 0,
            pending_lease_requests: 0,
            failed_acquisitions: 0,
            custom_dimensions: Vec::new(),
        })
    }
}

struct NoopExecutor;

impl TaskExecutor for NoopExecutor {
    fn spawn_dyn(
        &self,
        _: &CallContext,
        _: BoxFuture<'static, TaskResult<Box<dyn std::any::Any + Send>>>,
    ) -> JoinHandle<Box<dyn std::any::Any + Send>> {
        JoinHandle::from_task_handle(Box::new(NoopTaskHandle))
    }
}

struct NoopTaskHandle;

#[spark_core::async_trait]
impl TaskHandle for NoopTaskHandle {
    type Output = Box<dyn std::any::Any + Send>;

    fn cancel(&self, _: TaskCancellationStrategy) {}

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

    async fn join(self: Box<Self>) -> TaskResult<Self::Output> {
        Ok(Box::new(()) as Box<dyn std::any::Any + Send>)
    }
}

struct NoopTimer;

#[spark_core::async_trait]
impl TimeDriver for NoopTimer {
    fn now(&self) -> MonotonicTimePoint {
        MonotonicTimePoint::from_offset(Duration::from_millis(0))
    }

    async fn sleep(&self, _: Duration) {}
}
