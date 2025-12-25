#![cfg(feature = "test-util")]

use std::{
    any::Any,
    pin::Pin,
    sync::{Arc, Mutex, OnceLock},
    task::{Context as TaskContext, Poll},
    time::Duration,
};

use spark_core::{
    CoreError, SparkError, Stream,
    buffer::{BufferPool, PipelineMessage, WritableBuffer},
    contract::{CallContext, CloseReason, Deadline},
    future::BoxFuture,
    observability::{
        EventPolicy, LogRecord, Logger, MetricsProvider, OpsEvent, OpsEventBus, OpsEventKind,
        SpanId, TraceContext,
    },
    runtime::{
        AsyncRuntime, CoreServices, JoinHandle, TaskCancellationStrategy, TaskError, TaskExecutor,
        TaskHandle, TaskResult, TimeDriver,
    },
    test_stubs::observability::NoopMetricsProvider,
};

use spark_core::async_trait;

use spark_otel::facade::DefaultObservabilityFacade;
use spark_otel::{self, Error as OtelError};

use spark_core as spark_router;
use spark_router::pipeline::{
    Channel, ChannelState, ExtensionsMap, Pipeline, WriteSignal,
    controller::{HotSwapPipeline, PipelineHandleId},
    handler::InboundHandler,
};

/// 验证 Handler 日志自动注入 TraceContext，且 Handler Span 的父子关系正确。
///
/// # 教案式说明
/// - **测试目标（Why）**：覆盖“零配置”追踪的关键约束：日志无需显式传参即可关联 Trace/Span，且下游 Handler 应继承上游 Span。
/// - **测试设计（How）**：构造两级入站 Handler，首个 Handler 记录日志并转发消息，次级 Handler 再记录日志；
///   通过 `spark-otel` 安装的 In-Memory Exporter 检查 Span 树形结构，并断言日志中的 Trace ID 与 Span ID。
/// - **验收契约（What）**：
///   1. 两条日志均携带同一 `trace_id` 且 `span_id` 分别对应各自 Span；
///   2. 第二个 Span 的 `parent_span_id` 等于第一个 Span 的 `span_id`；
///   3. 第一个 Span 的 `parent_span_id` 等于根 `CallContext` 中的 `span_id`。
#[test]
fn trace_context_auto_injected_and_spans_form_parent_child_tree() {
    ensure_otel_installed();
    spark_otel::testing::reset();

    let runtime = Arc::new(NoopRuntime::new());
    let logger = Arc::new(RecordingLogger::default());
    let ops = Arc::new(NoopOpsBus::default());
    let metrics = Arc::new(NoopMetricsProvider);

    let services = CoreServices::with_observability_facade(
        runtime as Arc<dyn AsyncRuntime>,
        Arc::new(NoopBufferPool),
        DefaultObservabilityFacade::new(
            logger.clone() as Arc<dyn Logger>,
            metrics as Arc<dyn MetricsProvider>,
            ops as Arc<dyn OpsEventBus>,
            Arc::new(Vec::new()),
        ),
    );

    let call_context = CallContext::builder().build();
    let root_trace = call_context.trace_context().clone();

    let channel = Arc::new(TestChannel::new("trace-auto-inject"));
    let controller =
        HotSwapPipeline::new(channel.clone() as Arc<dyn Channel>, services, call_context);
    channel.bind_controller(controller.clone() as Arc<dyn Pipeline<HandleId = PipelineHandleId>>);

    controller.register_inbound_handler("alpha", Box::new(AlphaHandler));
    controller.register_inbound_handler("beta", Box::new(BetaHandler));

    controller.emit_read(PipelineMessage::from_user(TestMessage { id: 7 }));

    spark_otel::testing::force_flush();

    let records = logger.take();
    let alpha_log = records
        .iter()
        .find(|entry| entry.message == "alpha-handled")
        .expect("应捕获 alpha Handler 的日志");
    let beta_log = records
        .iter()
        .find(|entry| entry.message == "beta-handled")
        .expect("应捕获 beta Handler 的日志");

    for entry in [alpha_log, beta_log] {
        assert!(entry.trace.is_some(), "日志应携带 TraceContext");
        assert_eq!(
            entry.trace.as_ref().unwrap().trace_id,
            root_trace.trace_id,
            "所有日志应共享根 trace_id"
        );
    }

    let alpha_trace = alpha_log
        .trace
        .as_ref()
        .expect("alpha 日志必须包含 TraceContext")
        .clone();
    let beta_trace = beta_log
        .trace
        .as_ref()
        .expect("beta 日志必须包含 TraceContext")
        .clone();

    assert_ne!(
        alpha_trace.span_id, beta_trace.span_id,
        "子 Span 应拥有独立 span_id"
    );

    let spans = spark_otel::testing::finished_spans();
    assert_eq!(spans.len(), 2, "应导出两个 Handler Span");

    let mut alpha_span_id: Option<SpanId> = None;
    let mut beta_parent: Option<SpanId> = None;
    let mut beta_span_id: Option<SpanId> = None;
    for span in spans {
        if span.name.contains("alpha") {
            alpha_span_id = Some(SpanId::from_bytes(span.span_context.span_id().to_bytes()));
            assert_eq!(
                SpanId::from_bytes(span.parent_span_id.to_bytes()),
                root_trace.span_id,
                "alpha Span 的父级应是根上下文"
            );
        }
        if span.name.contains("beta") {
            beta_parent = Some(SpanId::from_bytes(span.parent_span_id.to_bytes()));
            beta_span_id = Some(SpanId::from_bytes(span.span_context.span_id().to_bytes()));
        }
    }

    let alpha_span_id = alpha_span_id.expect("必须捕获 alpha Span");
    assert_eq!(
        beta_parent.expect("必须捕获 beta Span 父级"),
        alpha_span_id,
        "beta Span 的父级应指向 alpha Span",
    );

    assert_eq!(
        alpha_trace.span_id, alpha_span_id,
        "alpha 日志的 span_id 应与对应 Span 一致",
    );
    assert_eq!(
        beta_trace.span_id,
        beta_span_id.expect("必须捕获 beta Span"),
        "beta 日志的 span_id 应匹配其 Span",
    );
}

/// 确保 spark-otel 在测试进程中完成安装，并允许重复运行测试。
fn ensure_otel_installed() {
    if let Err(err) = spark_otel::install() {
        match err {
            OtelError::AlreadyInstalled => {}
            other => panic!("spark-otel 安装失败: {other}"),
        }
    }
}

/// 结构化记录日志的测试桩，实现自动注入链路上下文后的断言需求。
///
/// # 教案式说明
/// - **意图（Why）**：捕获 `InstrumentedLogger` 输出的 `LogRecord`，用于验证是否携带 `TraceContext`。
/// - **逻辑（How）**：内部以 `Mutex<Vec<_>>` 存储克隆后的消息与 Trace，测试结束时通过 [`take`] 方法提取。
/// - **契约（What）**：线程安全、可多次复用；`take` 会清空内部缓存，避免跨用例污染。
#[derive(Default)]
struct RecordingLogger {
    records: Mutex<Vec<RecordedLog>>,
}

/// 具备所有权的日志快照，包含消息与可选的 `TraceContext` 克隆。
struct RecordedLog {
    message: String,
    trace: Option<TraceContext>,
}

impl RecordingLogger {
    /// 取出并清空当前缓存的日志。
    fn take(&self) -> Vec<RecordedLog> {
        self.records
            .lock()
            .expect("record logger")
            .drain(..)
            .collect()
    }
}

impl Logger for RecordingLogger {
    fn log(&self, record: &LogRecord<'_>) {
        let entry = RecordedLog {
            message: record.message.clone().into_owned(),
            trace: record.trace_context.cloned(),
        };
        self.records.lock().expect("record logger").push(entry);
    }
}

/// 一级入站 Handler：负责记录第一条日志并继续转发消息。
struct AlphaHandler;

impl InboundHandler for AlphaHandler {
    fn describe(&self) -> spark_router::pipeline::initializer::InitializerDescriptor {
        spark_router::pipeline::initializer::InitializerDescriptor::new(
            "tests.alpha",
            "observability",
            "一级 Handler：记录日志并继续传递",
        )
    }

    fn on_channel_active(&self, _ctx: &dyn spark_router::pipeline::Context) {}

    fn on_read(&self, ctx: &dyn spark_router::pipeline::Context, msg: PipelineMessage) {
        ctx.logger().info("alpha-handled", None);
        ctx.forward_read(msg);
    }

    fn on_read_complete(&self, _ctx: &dyn spark_router::pipeline::Context) {}

    fn on_writability_changed(
        &self,
        _ctx: &dyn spark_router::pipeline::Context,
        _is_writable: bool,
    ) {
    }

    fn on_user_event(
        &self,
        _ctx: &dyn spark_router::pipeline::Context,
        _event: spark_core::CoreUserEvent,
    ) {
    }

    fn on_exception_caught(&self, _ctx: &dyn spark_router::pipeline::Context, _error: CoreError) {}

    fn on_channel_inactive(&self, _ctx: &dyn spark_router::pipeline::Context) {}
}

/// 二级入站 Handler：终止链路并记录日志，验证子 Span 行为。
struct BetaHandler;

impl InboundHandler for BetaHandler {
    fn describe(&self) -> spark_router::pipeline::initializer::InitializerDescriptor {
        spark_router::pipeline::initializer::InitializerDescriptor::new(
            "tests.beta",
            "observability",
            "二级 Handler：终止链路并记录日志",
        )
    }

    fn on_channel_active(&self, _ctx: &dyn spark_router::pipeline::Context) {}

    fn on_read(&self, ctx: &dyn spark_router::pipeline::Context, _msg: PipelineMessage) {
        ctx.logger().info("beta-handled", None);
    }

    fn on_read_complete(&self, _ctx: &dyn spark_router::pipeline::Context) {}

    fn on_writability_changed(
        &self,
        _ctx: &dyn spark_router::pipeline::Context,
        _is_writable: bool,
    ) {
    }

    fn on_user_event(
        &self,
        _ctx: &dyn spark_router::pipeline::Context,
        _event: spark_core::CoreUserEvent,
    ) {
    }

    fn on_exception_caught(&self, _ctx: &dyn spark_router::pipeline::Context, _error: CoreError) {}

    fn on_channel_inactive(&self, _ctx: &dyn spark_router::pipeline::Context) {}
}

/// 用于驱动 Pipeline 的伪造业务消息，只包含一个递增 ID。
#[derive(Clone, Copy)]
#[allow(dead_code)]
struct TestMessage {
    /// 教案级说明（Why）
    /// - 预留 `id` 字段便于后续扩展多消息场景，例如验证 Handler 根据消息编号分支逻辑。
    ///
    /// 教案级说明（Trade-offs）
    /// - 当前测试未读取该字段，为避免过早删除而加上 `#[allow(dead_code)]`；
    ///   若长时间未使用，应在测试整理阶段考虑精简结构。
    id: usize,
}

/// 极简 Channel 实现，桥接 HotSwapPipeline 并避免真实网络依赖。
struct TestChannel {
    id: String,
    controller: OnceLock<Arc<dyn Pipeline<HandleId = PipelineHandleId>>>,
    extensions: TestExtensions,
}

impl TestChannel {
    fn new(id: &str) -> Self {
        Self {
            id: id.to_string(),
            controller: OnceLock::new(),
            extensions: TestExtensions,
        }
    }

    fn bind_controller(&self, controller: Arc<dyn Pipeline<HandleId = PipelineHandleId>>) {
        let _ = self.controller.set(controller);
    }
}

impl Channel for TestChannel {
    fn id(&self) -> &str {
        &self.id
    }

    fn state(&self) -> ChannelState {
        ChannelState::Active
    }

    fn is_writable(&self) -> bool {
        true
    }

    fn controller(&self) -> &dyn Pipeline<HandleId = PipelineHandleId> {
        self.controller
            .get()
            .expect("controller 应提前绑定")
            .as_ref()
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

    fn close_graceful(&self, _reason: CloseReason, _deadline: Option<Deadline>) {}

    fn close(&self) {}

    fn closed(&self) -> BoxFuture<'static, spark_core::Result<(), SparkError>> {
        Box::pin(async { Ok(()) })
    }

    fn write(&self, _msg: PipelineMessage) -> spark_core::Result<WriteSignal, CoreError> {
        Ok(WriteSignal::Accepted)
    }

    fn flush(&self) {}
}

/// 空实现的扩展映射，确保测试过程不持久化额外状态。
#[derive(Default)]
struct TestExtensions;

impl ExtensionsMap for TestExtensions {
    fn insert(&self, _key: std::any::TypeId, _value: Box<dyn std::any::Any + Send + Sync>) {}

    fn get<'a>(
        &'a self,
        _key: &std::any::TypeId,
    ) -> Option<&'a (dyn std::any::Any + Send + Sync + 'static)> {
        None
    }

    fn remove(&self, _key: &std::any::TypeId) -> Option<Box<dyn std::any::Any + Send + Sync>> {
        None
    }

    fn contains_key(&self, _key: &std::any::TypeId) -> bool {
        false
    }

    fn clear(&self) {}
}

/// 将运维事件存入本地向量的空实现，用于满足 `CoreServices` 契约。
#[derive(Default)]
struct NoopOpsBus {
    events: Mutex<Vec<OpsEvent>>,
    policies: Mutex<Vec<(OpsEventKind, EventPolicy)>>,
}

impl OpsEventBus for NoopOpsBus {
    fn broadcast(&self, event: OpsEvent) {
        self.events.lock().expect("ops events").push(event);
    }

    fn subscribe(&self) -> spark_core::BoxStream<'static, OpsEvent> {
        Box::pin(EmptyStream)
    }

    fn set_event_policy(&self, kind: OpsEventKind, policy: EventPolicy) {
        self.policies
            .lock()
            .expect("ops policies")
            .push((kind, policy));
    }
}

/// 永远返回 `None` 的空事件流，避免拉起真实订阅。
struct EmptyStream;

impl Stream for EmptyStream {
    type Item = OpsEvent;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut TaskContext<'_>) -> Poll<Option<Self::Item>> {
        Poll::Ready(None)
    }
}

/// 不应被调用的缓冲池桩，实现最小接口即可。
struct NoopBufferPool;

impl BufferPool for NoopBufferPool {
    fn acquire(
        &self,
        _min_capacity: usize,
    ) -> spark_core::Result<Box<dyn WritableBuffer>, CoreError> {
        Err(CoreError::new("test.buffer", "测试中不应触发 acquire"))
    }

    fn shrink_to_fit(&self) -> spark_core::Result<usize, CoreError> {
        Ok(0)
    }

    fn statistics(&self) -> spark_core::Result<spark_core::buffer::PoolStats, CoreError> {
        Ok(Default::default())
    }
}

/// 仅维护单调时间的运行时桩，用于满足 `CoreServices` 的执行器/计时器接口。
#[derive(Default)]
struct NoopRuntime {
    now: Mutex<Duration>,
}

impl NoopRuntime {
    fn new() -> Self {
        Self::default()
    }
}

impl TaskExecutor for NoopRuntime {
    fn spawn_dyn(
        &self,
        _ctx: &CallContext,
        _fut: BoxFuture<'static, TaskResult<Box<dyn Any + Send>>>,
    ) -> JoinHandle<Box<dyn Any + Send>> {
        JoinHandle::from_task_handle(Box::new(NoopHandle))
    }
}

#[async_trait]
impl TimeDriver for NoopRuntime {
    fn now(&self) -> spark_core::runtime::MonotonicTimePoint {
        spark_core::runtime::MonotonicTimePoint::from_offset(*self.now.lock().expect("time"))
    }

    async fn sleep(&self, duration: Duration) {
        let mut guard = self.now.lock().expect("time");
        *guard = guard.checked_add(duration).unwrap_or(Duration::MAX);
    }
}

/// `NoopHandle` 模拟被执行器拒绝调度的任务。
#[derive(Default)]
struct NoopHandle;

#[async_trait]
impl TaskHandle for NoopHandle {
    type Output = Box<dyn Any + Send>;

    fn cancel(&self, _strategy: TaskCancellationStrategy) {}

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
        Err(TaskError::ExecutorTerminated)
    }
}
