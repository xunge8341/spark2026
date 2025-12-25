use std::{
    any::Any,
    pin::Pin,
    sync::{Arc, Mutex, OnceLock},
    task::{Context, Poll},
    time::Duration,
};

use spark_core::{
    CoreError, SparkError,
    buffer::{BufferPool, PipelineMessage, WritableBuffer},
    contract::{CallContext, CloseReason, Deadline},
    future::BoxFuture,
    observability::{
        EventPolicy, Logger, MetricsProvider, OpsEvent, OpsEventBus, OpsEventKind, TraceContext,
        TraceFlags,
    },
    pipeline::{
        Channel, ChannelState, ExtensionsMap, Pipeline, WriteSignal,
        controller::{Handler, HotSwapPipeline, PipelineHandleId, handler_from_inbound},
        handler::InboundHandler,
    },
    runtime::{
        AsyncRuntime, CoreServices, JoinHandle, MonotonicTimePoint, TaskCancellationStrategy,
        TaskError, TaskExecutor, TaskHandle, TaskResult, TimeDriver,
    },
    test_stubs::observability::{NoopLogger, NoopMetricsProvider, StaticObservabilityFacade},
};

/// 验证热插拔在运行期不会丢包或打乱顺序。
#[test]
fn hot_swap_inserts_handler_without_dropping_messages() {
    let runtime = Arc::new(NoopRuntime::new());
    let logger = Arc::new(NoopLogger);
    let ops = Arc::new(NoopOpsBus::default());
    let metrics = Arc::new(NoopMetricsProvider);

    let services = CoreServices::with_observability_facade(
        runtime as Arc<dyn AsyncRuntime>,
        Arc::new(NoopBufferPool),
        StaticObservabilityFacade::new(
            logger as Arc<dyn Logger>,
            metrics as Arc<dyn MetricsProvider>,
            ops as Arc<dyn OpsEventBus>,
            Arc::new(Vec::new()),
        ),
    );

    let trace_context = TraceContext::new(
        [0x11; TraceContext::TRACE_ID_LENGTH],
        [0x22; TraceContext::SPAN_ID_LENGTH],
        TraceFlags::new(TraceFlags::SAMPLED),
    );
    let call_context = CallContext::builder()
        .with_trace_context(trace_context.clone())
        .build();

    let channel = Arc::new(TestChannel::new("hot-swap-channel"));
    let controller =
        HotSwapPipeline::new(channel.clone() as Arc<dyn Channel>, services, call_context);
    channel.bind_controller(controller.clone() as Arc<dyn Pipeline<HandleId = PipelineHandleId>>);

    let events = Arc::new(Mutex::new(Vec::new()));
    controller.register_inbound_handler(
        "tls",
        Box::new(RecordingInbound::new("tls", Arc::clone(&events))),
    );

    controller.emit_read(PipelineMessage::from_user(TestMessage { id: 1 }));

    {
        let recorded = events.lock().expect("recording lock");
        assert_eq!(recorded.as_slice(), ["tls:1"], "插入前仅 TLS 处理消息");
    }

    let tls_handle = controller
        .registry()
        .snapshot()
        .into_iter()
        .find(|entry| entry.label() == "tls")
        .expect("应能在注册表中找到 TLS Handler")
        .handle_id();

    let logging_handler: Arc<dyn InboundHandler> =
        Arc::new(RecordingInbound::new("log", Arc::clone(&events)));
    let dyn_handler: Arc<dyn Handler> = handler_from_inbound(logging_handler);
    let epoch_before = controller.epoch();
    let logging_handle = controller.add_handler_after(tls_handle, "logging", dyn_handler);
    assert_ne!(
        logging_handle,
        PipelineHandleId::INBOUND_HEAD,
        "新句柄不应回退到锚点"
    );
    assert!(
        controller.epoch() > epoch_before,
        "新增 Handler 应自增 epoch"
    );

    controller.emit_read(PipelineMessage::from_user(TestMessage { id: 2 }));

    let recorded = events.lock().expect("recording lock");
    assert_eq!(
        recorded.as_slice(),
        ["tls:1", "tls:2", "log:2"],
        "热插拔后应保持顺序且仅新消息经过 Logging"
    );
}

#[cfg(all(feature = "std", any(loom, spark_loom)))]
mod loom_tests {
    use super::*;
    use loom::thread;

    #[test]
    fn remove_during_on_read_no_ub() {
        loom::model(|| {
            let runtime = Arc::new(NoopRuntime::new());
            let logger = Arc::new(NoopLogger);
            let ops = Arc::new(NoopOpsBus::default());
            let metrics = Arc::new(NoopMetricsProvider);

            let services = CoreServices::with_observability_facade(
                runtime as Arc<dyn AsyncRuntime>,
                Arc::new(NoopBufferPool),
                StaticObservabilityFacade::new(
                    logger as Arc<dyn Logger>,
                    metrics as Arc<dyn MetricsProvider>,
                    ops as Arc<dyn OpsEventBus>,
                    Arc::new(Vec::new()),
                ),
            );

            let trace_context = TraceContext::new(
                [0x33; TraceContext::TRACE_ID_LENGTH],
                [0x44; TraceContext::SPAN_ID_LENGTH],
                TraceFlags::new(TraceFlags::SAMPLED),
            );
            let call_context = CallContext::builder()
                .with_trace_context(trace_context.clone())
                .build();

            let channel = Arc::new(TestChannel::new("loom-channel"));
            let controller =
                HotSwapPipeline::new(channel.clone() as Arc<dyn Channel>, services, call_context);
            channel.bind_controller(
                controller.clone() as Arc<dyn Pipeline<HandleId = PipelineHandleId>>
            );

            controller.register_inbound_handler(
                "a",
                Box::new(RecordingInbound::new("a", Arc::new(Mutex::new(Vec::new())))),
            );
            controller.register_inbound_handler(
                "b",
                Box::new(RecordingInbound::new("b", Arc::new(Mutex::new(Vec::new())))),
            );

            let handle_to_remove = controller
                .registry()
                .snapshot()
                .into_iter()
                .find(|entry| entry.label() == "b")
                .map(|entry| entry.handle_id())
                .expect("第二个 Handler 句柄应存在");

            let reader = controller.clone();
            let remover = controller.clone();

            let t1 = thread::spawn(move || {
                reader.emit_read(PipelineMessage::from_user(TestMessage { id: 7 }));
            });

            let t2 = thread::spawn(move || {
                remover.remove_handler(handle_to_remove);
            });

            t1.join().expect("reader thread");
            t2.join().expect("remover thread");
        });
    }
}

/// 测试通道，为控制器提供固定上下文。
pub(crate) struct TestChannel {
    id: String,
    controller: OnceLock<Arc<dyn Pipeline<HandleId = PipelineHandleId>>>,
    extensions: TestExtensions,
}

impl TestChannel {
    pub(crate) fn new(id: &str) -> Self {
        Self {
            id: id.to_string(),
            controller: OnceLock::new(),
            extensions: TestExtensions,
        }
    }

    pub(crate) fn bind_controller(
        &self,
        controller: Arc<dyn Pipeline<HandleId = PipelineHandleId>>,
    ) {
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
            .expect("controller should be bound")
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

/// 测试用 ExtensionsMap，避免依赖真实存储以降低样例复杂度。
///
/// # 教案式说明
/// - **意图（Why）**：Pipeline API 要求提供 [`ExtensionsMap`]，但当前测试只关注 Handler 链路热插拔，
///   因此返回空实现即可，减少与外部状态的耦合。
/// - **逻辑（How）**：所有方法均为 no-op，`get` 始终返回 `None`，从而保证在测试中不会意外持有引用。
/// - **契约（What）**：调用方不可依赖此实现存储数据；若未来测试需要验证扩展传播，应替换为真正的 map。
/// - **权衡（Trade-offs）**：舍弃真实存储换取实现简洁，牺牲了对扩展读写的验证能力。
#[derive(Default)]
pub(crate) struct TestExtensions;

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

struct RecordingInbound {
    name: &'static str,
    events: Arc<Mutex<Vec<String>>>,
}

impl RecordingInbound {
    fn new(name: &'static str, events: Arc<Mutex<Vec<String>>>) -> Self {
        Self { name, events }
    }
}

impl InboundHandler for RecordingInbound {
    fn on_channel_active(&self, _ctx: &dyn spark_core::pipeline::Context) {}

    fn on_read(&self, ctx: &dyn spark_core::pipeline::Context, msg: PipelineMessage) {
        match msg.try_into_user::<TestMessage>() {
            Ok(user) => {
                self.events
                    .lock()
                    .expect("recording lock")
                    .push(format!("{}:{}", self.name, user.id));
                ctx.forward_read(PipelineMessage::from_user(user));
            }
            Err(other) => ctx.forward_read(other),
        }
    }

    fn on_read_complete(&self, _ctx: &dyn spark_core::pipeline::Context) {}

    fn on_writability_changed(&self, _ctx: &dyn spark_core::pipeline::Context, _is_writable: bool) {
    }

    fn on_user_event(
        &self,
        _ctx: &dyn spark_core::pipeline::Context,
        _event: spark_core::observability::CoreUserEvent,
    ) {
    }

    fn on_exception_caught(&self, _ctx: &dyn spark_core::pipeline::Context, _error: CoreError) {}

    fn on_channel_inactive(&self, _ctx: &dyn spark_core::pipeline::Context) {}
}

#[derive(Clone, Copy)]
struct TestMessage {
    id: usize,
}

#[derive(Default)]
pub(crate) struct NoopOpsBus {
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

struct EmptyStream;

impl spark_core::Stream for EmptyStream {
    type Item = OpsEvent;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Ready(None)
    }
}

pub(crate) struct NoopBufferPool;

impl BufferPool for NoopBufferPool {
    fn acquire(
        &self,
        _min_capacity: usize,
    ) -> spark_core::Result<Box<dyn WritableBuffer>, CoreError> {
        Err(CoreError::new("test.buffer", "acquire unused in tests"))
    }

    fn shrink_to_fit(&self) -> spark_core::Result<usize, CoreError> {
        Ok(0)
    }

    fn statistics(&self) -> spark_core::Result<spark_core::buffer::PoolStats, CoreError> {
        Ok(Default::default())
    }
}

#[derive(Default)]
pub(crate) struct NoopRuntime {
    now: Mutex<Duration>,
}

impl NoopRuntime {
    pub(crate) fn new() -> Self {
        Self::default()
    }
}

impl TaskExecutor for NoopRuntime {
    fn spawn_dyn(
        &self,
        _ctx: &CallContext,
        _fut: BoxFuture<'static, TaskResult<Box<dyn std::any::Any + Send>>>,
    ) -> JoinHandle<Box<dyn std::any::Any + Send>> {
        JoinHandle::from_task_handle(Box::new(NoopHandle))
    }
}

#[async_trait::async_trait]
impl TimeDriver for NoopRuntime {
    fn now(&self) -> MonotonicTimePoint {
        MonotonicTimePoint::from_offset(*self.now.lock().expect("time"))
    }

    async fn sleep(&self, duration: Duration) {
        let mut guard = self.now.lock().expect("time");
        *guard = guard.checked_add(duration).unwrap_or(Duration::MAX);
    }
}

/// `NoopHandle` 代表一个永远不会调度执行的任务句柄。
///
/// # 设计背景（Why）
/// - 热插拔与可观测性测试关注控制器逻辑，不需要真正的异步执行环境。
/// - 通过提供空实现，可以确保依赖任务接口的代码路径得以编译并被验证。
///
/// # 逻辑解析（How）
/// - 关联类型 [`TaskHandle::Output`] 固定为 `Box<dyn Any + Send>`，与 `spawn_dyn` 的类型擦除约定保持一致。
/// - `join` 直接返回 [`TaskError::ExecutorTerminated`]，模拟执行器拒绝任务的情形。
/// - 其他查询方法返回稳定的常量，以保持句柄语义的一致性。
///
/// # 契约说明（What）
/// - **前置条件**：调用方不得期望任务被实际执行；本句柄仅用于测试桩。
/// - **后置条件**：一旦 `join` 被调用，将立即得到错误结果且无副作用。
///
/// # 风险提示（Trade-offs）
/// - 若后续测试需要验证任务执行路径，应替换为可调度的运行时实现。
/// - 长期使用该桩可能掩盖上下文传播中的真实问题，需要在集成测试中补齐覆盖。
#[derive(Default)]
struct NoopHandle;

#[async_trait::async_trait]
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

// `AsyncRuntime` 为 blanket trait，无需显式实现。
