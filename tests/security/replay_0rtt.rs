//! 针对 0-RTT 重放触发的安全分类，验证默认自动响应行为。
//!
//! - **目标（Why）**：历史上在 QUIC/HTTP3 等协议中 0-RTT 重放会污染 ReadyState，导致错误地重新开放通道。本用例确保
//!   安全分类被触发时只执行“拒绝/隔离”动作。
//! - **策略（How）**：构造 `CoreError` 并标记为 `ErrorCategory::Security(SecurityClass::Integrity)`，复用
//!   `ExceptionAutoResponder` 的默认处理逻辑，观察控制器与通道录制的副作用。
//! - **契约（What）**：安全违规应触发一次 `close_graceful`，同时 ReadyState 广播保持空列表，避免状态机被污染。

use std::any::TypeId;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use spark_core::contract::{CallContext, CallContextBuilder, CloseReason};
use spark_core::error::{CoreError, ErrorCategory};
use spark_core::future::BoxFuture;
use spark_core::observability::metrics::{
    AttributeSet, Counter, Gauge, Histogram, InstrumentDescriptor, MetricsProvider,
};
use spark_core::observability::{CoreUserEvent, LogRecord, Logger, TraceContext};
use spark_core as spark_router;
use spark_router::pipeline::channel::ChannelState;
use spark_router::pipeline::controller::{
    Pipeline, PipelineHandleId, HandlerRegistration,
};
use spark_router::pipeline::default_handlers::{
    ExceptionAutoResponder, ReadyStateEvent,
};
use spark_router::pipeline::handler::{InboundHandler, OutboundHandler};
use spark_router::pipeline::{
    Channel, Context, Pipeline as PipelineController, ExtensionsMap, HandlerRegistry,
    PipelineMessage, WriteSignal,
};
use spark_core::runtime::{
    JoinHandle, MonotonicTimePoint, TaskCancellationStrategy, TaskExecutor, TaskHandle,
    TaskResult, TimeDriver,
};
use spark_core::security::SecurityClass;
use spark_core::status::ReadyState;
use spark_core::{SparkError, async_trait};
use spark_core::test_stubs::observability::{
    NoopCounter, NoopGauge, NoopHistogram, NoopLogger, NoopMetricsProvider,
};

/// 复合测试夹具，负责提供通用的 Pipeline / Channel / Context 实现。
///
/// - **Why**：便于复用记录 ReadyState 与关闭原因的逻辑，避免每个安全用例重复搭建管线伪造物。
/// - **How**：内部以 `Arc` 包裹录制对象，`RecordingContext` 实现 `Context` trait 并代理至录制器。
/// - **What**：对外暴露 `controller`、`channel`、`ctx` 三个字段以及便捷的读取方法。
struct SecurityHarness {
    controller: Arc<RecordingController>,
    channel: Arc<RecordingChannel>,
    ctx: RecordingContext,
}

impl SecurityHarness {
    /// 构建新的测试环境。
    ///
    /// - **Why**：为每个测试提供独立的控制面与通道实例，避免状态泄漏。
    /// - **How**：创建录制控制器与通道，再由 `RecordingContext` 封装执行器、日志器等依赖。
    /// - **What**：返回 `SecurityHarness`，后续可直接注入 `ExceptionAutoResponder`。
    fn new() -> Self {
        let controller = Arc::new(RecordingController::default());
        let channel = Arc::new(RecordingChannel::new(Arc::clone(&controller)));
        let call_context = CallContextBuilder::default().build();
        let ctx = RecordingContext::new(
            Arc::clone(&controller),
            Arc::clone(&channel),
            call_context,
        );
        Self {
            controller,
            channel,
            ctx,
        }
    }
}

impl SecurityHarness {
    /// 读取记录的 ReadyState 序列。
    fn ready_states(&self) -> Vec<ReadyState> {
        self.controller.ready_states.lock().unwrap().clone()
    }

    /// 读取通道关闭原因。
    fn close_reasons(&self) -> Vec<CloseReason> {
        self.channel.closed.lock().unwrap().clone()
    }
}

#[test]
fn zero_rtt_replay_security_error_triggers_isolation() {
    // === Why === 确保 0-RTT 重放被分类为安全事件后，默认策略会直接隔离通道而非重新广播 ReadyState。
    // === How === 构造带有 `SecurityClass::Integrity` 的错误并交给 `ExceptionAutoResponder` 处理，随后检视录制的副作用。
    // === What === ReadyState 列表必须为空；关闭原因应编码为 `security.violation` 且消息为安全分类摘要。
    let harness = SecurityHarness::new();
    let error = CoreError::new(
        "transport.0rtt.replay_detected",
        "detected 0-RTT replay",
    )
    .with_category(ErrorCategory::Security(SecurityClass::Integrity));

    ExceptionAutoResponder::new().on_exception_caught(&harness.ctx, error);

    assert!(
        harness.ready_states().is_empty(),
        "安全违规不应广播 ReadyState，避免状态机被污染",
    );
    let reasons = harness.close_reasons();
    assert_eq!(reasons.len(), 1, "预期触发一次优雅关闭");
    let reason = &reasons[0];
    assert_eq!(reason.code(), "security.violation");
    assert_eq!(reason.message(), SecurityClass::Integrity.summary());
}

/// 录制控制器，捕获 ReadyState 广播。
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
    ) -> Result<(), CoreError> {
        Ok(())
    }

    fn emit_channel_activated(&self) {}

    fn emit_read(&self, _: PipelineMessage) {}

    fn emit_read_completed(&self) {}

    fn emit_writability_changed(&self, _: bool) {}

    fn emit_user_event(&self, event: CoreUserEvent) {
        if let Some(ready) = event.downcast_application_event::<ReadyStateEvent>() {
            self.ready_states
                .lock()
                .unwrap()
                .push(ready.state().clone());
        }
    }

    fn emit_exception(&self, _: CoreError) {}

    fn emit_channel_deactivated(&self) {}

    fn registry(&self) -> &dyn HandlerRegistry {
        &NoopRegistry
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

/// 空注册表实现，满足 Pipeline 契约。
struct NoopRegistry;

impl HandlerRegistry for NoopRegistry {
    fn snapshot(&self) -> Vec<HandlerRegistration> {
        Vec::new()
    }
}

/// 录制通道，记录关闭原因并对 `Context` 提供所需能力。
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
        "replay-0rtt-channel"
    }

    fn state(&self) -> ChannelState {
        ChannelState::Active
    }

    fn is_writable(&self) -> bool {
        true
    }

    fn controller(&self) -> &dyn PipelineController<HandleId = PipelineHandleId> {
        self.controller.as_ref()
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

    fn close_graceful(&self, reason: CloseReason, _: Option<spark_core::contract::Deadline>) {
        self.closed.lock().unwrap().push(reason);
    }

    fn close(&self) {}

    fn closed(&self) -> BoxFuture<'static, Result<(), SparkError>> {
        Box::pin(async { Ok(()) })
    }

    fn write(&self, _: PipelineMessage) -> Result<WriteSignal, CoreError> {
        Ok(WriteSignal::Accepted)
    }

    fn flush(&self) {}
}

/// 空扩展映射，满足管线契约。
#[derive(Copy, Clone)]
struct NoopExtensions;

impl ExtensionsMap for NoopExtensions {
    fn insert(&self, _: TypeId, _: Box<dyn std::any::Any + Send + Sync>) {}

    fn get<'a>(
        &'a self,
        _: &TypeId,
    ) -> Option<&'a (dyn std::any::Any + Send + Sync + 'static)> {
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

/// `Context` 实现，代理通道/控制器并提供无副作用的观测与执行器。
struct RecordingContext {
    controller_view: Arc<dyn PipelineController<HandleId = PipelineHandleId>>,
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
        let trace = TraceContext::generate();
        let controller_view: Arc<dyn PipelineController<HandleId = PipelineHandleId>> =
            controller.clone() as Arc<dyn PipelineController<HandleId = PipelineHandleId>>;
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

    fn pipeline(&self) -> &Arc<dyn PipelineController<HandleId = PipelineHandleId>> {
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

    fn forward_read(&self, _: PipelineMessage) {}

    fn write(&self, msg: PipelineMessage) -> Result<WriteSignal, CoreError> {
        self.channel.write(msg)
    }

    fn flush(&self) {
        self.channel.flush();
    }

    fn close_graceful(&self, reason: CloseReason, deadline: Option<spark_core::contract::Deadline>) {
        self.channel.close_graceful(reason, deadline);
    }

    fn closed(&self) -> BoxFuture<'static, Result<(), SparkError>> {
        self.channel.closed()
    }
}

/// 不提供缓冲能力的占位实现，避免测试在意外调用时产生未定义行为。
struct NoopBufferPool;

impl spark_core::buffer::BufferPool for NoopBufferPool {
    fn acquire(&self, _: usize) -> Result<Box<dyn spark_core::buffer::WritableBuffer>, CoreError> {
        Err(CoreError::new("buffer.disabled", "buffer pool disabled"))
    }

    fn shrink_to_fit(&self) -> Result<usize, CoreError> {
        Ok(0)
    }

    fn statistics(&self) -> Result<spark_core::buffer::PoolStats, CoreError> {
        Ok(spark_core::buffer::PoolStats::default())
    }
}


/// 不执行实际调度的占位执行器。
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

#[async_trait]
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

/// 恒定时间驱动，用于满足 `Context` 契约。
struct NoopTimer;

#[async_trait]
impl TimeDriver for NoopTimer {
    fn now(&self) -> MonotonicTimePoint {
        MonotonicTimePoint::from_offset(Duration::from_millis(0))
    }

    async fn sleep(&self, _: Duration) {}
}
