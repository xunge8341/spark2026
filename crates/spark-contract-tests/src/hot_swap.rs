use crate::case::{TckCase, TckSuite};
use crate::support::shared_vec;
use parking_lot::Mutex;
use spark_core as spark_router;
use spark_core::SparkError;
use spark_core::buffer::{BufferPool, PipelineMessage, WritableBuffer};
use spark_core::contract::{CallContext, CloseReason, Deadline};
use spark_core::future::BoxFuture;
use spark_core::observability::{
    EventPolicy, Logger, MetricsProvider, OpsEvent, OpsEventBus, OpsEventKind, TraceContext,
    TraceFlags,
};
use spark_core::runtime::{
    AsyncRuntime, CoreServices, MonotonicTimePoint, TaskCancellationStrategy, TaskExecutor,
    TaskHandle, TaskResult, TimeDriver,
};
use spark_core::test_stubs::observability::{NoopLogger, NoopMetricsProvider};
use spark_otel::facade::DefaultObservabilityFacade;
use spark_router::pipeline::channel::{ChannelState, WriteSignal};
use spark_router::pipeline::controller::{HotSwapPipeline, PipelineHandleId, handler_from_inbound};
use spark_router::pipeline::handler::InboundHandler;
use spark_router::pipeline::{
    Channel, ExtensionsMap, Pipeline, initializer::InitializerDescriptor,
};
use std::any::TypeId;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
use std::sync::{Arc, Barrier, OnceLock};
use std::task::{Context as TaskContext, Poll};
use std::thread;
use std::time::Duration;

const CASES: &[TckCase] = &[
    TckCase {
        name: "hot_swap_inserts_handler_without_dropping_messages",
        test: hot_swap_inserts_handler_without_dropping_messages,
    },
    TckCase {
        name: "hot_swap_replaces_handler_preserves_epoch_and_ordering",
        test: hot_swap_replaces_handler_preserves_epoch_and_ordering,
    },
    TckCase {
        name: "hot_swap_addition_during_inflight_dispatch_awaits_epoch_barrier",
        test: hot_swap_addition_during_inflight_dispatch_awaits_epoch_barrier,
    },
];

const SUITE: TckSuite = TckSuite {
    name: "hot_swap",
    cases: CASES,
};

/// 返回“热插拔”主题的测试套件。
///
/// # 教案式说明
/// - **意图 (Why)**：验证 Pipeline 在运行期插入 Handler 时的顺序性与状态维护。
/// - **逻辑 (How)**：目前包含单用例，关注 Handler 注册、重放与事件记录。
/// - **契约 (What)**：返回 `'static` 引用供宏调用。
pub const fn suite() -> &'static TckSuite {
    &SUITE
}

/// 验证 `HotSwapPipeline` 在链路中插入新 Handler 时不会丢失消息或破坏顺序。
///
/// # 教案式说明
/// - **意图 (Why)**：运行期升级 Transport/Protocol 时需要动态插入 Handler，必须保证已有流量不受影响。
/// - **逻辑 (How)**：构造只记录事件的通道，先注册 TLS Handler，再在其后插入 Logging Handler，观察事件顺序与 epoch。
/// - **契约 (What)**：
///   - 插入前事件序列为 `tls:1`；
///   - 插入后事件序列为 `tls:1`, `tls:2`, `log:2`；
///   - Pipeline 的 epoch 自增，新增句柄不指向链表头。
fn hot_swap_inserts_handler_without_dropping_messages() {
    let (_channel, controller) = build_hot_swap_controller("hot-swap-channel");

    let events = shared_vec();
    controller.register_inbound_handler(
        "tls",
        Box::new(RecordingInbound::new("tls", Arc::clone(&events))),
    );

    controller.emit_read(PipelineMessage::from_user(TestMessage { id: 1 }));
    {
        let recorded = events.lock();
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
    let dyn_handler = handler_from_inbound(logging_handler);
    let epoch_before = controller.epoch();
    let logging_handle = controller.add_handler_after(tls_handle, "logging", dyn_handler);
    assert_ne!(logging_handle, PipelineHandleId::INBOUND_HEAD);
    assert!(controller.epoch() > epoch_before);

    controller.emit_read(PipelineMessage::from_user(TestMessage { id: 2 }));

    let recorded = events.lock();
    assert_eq!(recorded.as_slice(), ["tls:1", "tls:2", "log:2"]);
}

/// 验证 `replace_handler` 在运行期替换链路节点时，既能维持消息顺序，又会通过 epoch 栅栏告知观察者新快照已提交。
///
/// # 教案式说明
/// - **意图 (Why)**：运维在发布补丁版本的 Handler 时，常用“无缝替换”策略，该测试确认旧节点不会在替换后继续处理新消息。
/// - **逻辑 (How)**：
///   1. 构造控制器并注册 `alpha` 记录器；
///   2. 发送首条消息确保旧 Handler 正常工作；
///   3. 调用 `replace_handler` 将 `alpha` 替换为 `beta`，断言 epoch 自增与注册表更新；
///   4. 再次发送消息，核对事件序列仅由新 Handler 记录。
/// - **契约 (What)**：替换完成后，`controller.epoch()` 必须单调递增，注册表标签更新为 `beta`，且事件序列不再出现 `alpha` 处理的新消息。
fn hot_swap_replaces_handler_preserves_epoch_and_ordering() {
    let (_channel, controller) = build_hot_swap_controller("hot-swap-replace");

    let events = shared_vec();
    controller.register_inbound_handler(
        "alpha",
        Box::new(RecordingInbound::new("alpha", Arc::clone(&events))),
    );

    controller.emit_read(PipelineMessage::from_user(TestMessage { id: 1 }));
    {
        let recorded = events.lock();
        assert_eq!(recorded.as_slice(), ["alpha:1"], "替换前应仅由 alpha 处理");
    }

    let alpha_handle = controller
        .registry()
        .snapshot()
        .into_iter()
        .find(|entry| entry.label() == "alpha")
        .expect("应能找到 alpha Handler 句柄")
        .handle_id();

    let replacement: Arc<dyn InboundHandler> =
        Arc::new(RecordingInbound::new("beta", Arc::clone(&events)));
    let replacement = handler_from_inbound(replacement);

    let epoch_before = controller.epoch();
    assert!(
        controller.replace_handler(alpha_handle, replacement),
        "替换操作必须返回成功"
    );
    let epoch_after = controller.epoch();
    assert!(epoch_after > epoch_before, "替换后 epoch 应自增");

    let registry_labels: Vec<_> = controller
        .registry()
        .snapshot()
        .into_iter()
        .map(|entry| entry.label().to_string())
        .collect();
    assert_eq!(registry_labels, ["beta"], "注册表应仅包含新 Handler");

    controller.emit_read(PipelineMessage::from_user(TestMessage { id: 2 }));

    let recorded = events.lock();
    assert_eq!(
        recorded.as_slice(),
        ["alpha:1", "beta:2"],
        "替换后新消息只能由 beta 记录",
    );
}

/// 验证在 Handler 正在处理消息时执行插入操作，旧快照仍然保持一致，直至新 epoch 对后续消息生效。
///
/// # 教案式说明
/// - **意图 (Why)**：确认 `HotSwapPipeline` 的“变更栅栏”语义——在单条消息的处理流程中，新插入的 Handler 不会提前介入，避免读到半更新的快照。
/// - **逻辑 (How)**：
///   1. 注册带栅栏的 `alpha` Handler，并在线程 A 中触发消息，使其在 `Barrier` 处暂停；
///   2. 主线程等待进入临界区后调用 `add_handler_after` 插入 `beta`，确认 epoch 自增；
///   3. 释放栅栏继续转发消息，断言首条消息未经过 `beta`；
///   4. 对第二条消息进行检查，确认两位 Handler 均参与处理，证明新快照已对后续请求生效。
/// - **契约 (What)**：插入过程中不会出现 `beta:1` 记录；`controller.epoch()` 在插入后递增；最终注册表顺序为 `["alpha", "beta"]`。
fn hot_swap_addition_during_inflight_dispatch_awaits_epoch_barrier() {
    let (_channel, controller) = build_hot_swap_controller("hot-swap-barrier");

    let events = shared_vec();
    let start = Arc::new(Barrier::new(2));
    let release = Arc::new(Barrier::new(2));

    controller.register_inbound_handler(
        "alpha",
        Box::new(BlockingRecordingInbound::new(
            "alpha",
            Arc::clone(&events),
            Arc::clone(&start),
            Arc::clone(&release),
        )),
    );

    let alpha_handle = controller
        .registry()
        .snapshot()
        .into_iter()
        .find(|entry| entry.label() == "alpha")
        .expect("alpha 应注册成功")
        .handle_id();

    let reader_controller = Arc::clone(&controller);
    let reader = thread::spawn(move || {
        reader_controller.emit_read(PipelineMessage::from_user(TestMessage { id: 1 }));
    });

    start.wait();

    let epoch_before = controller.epoch();
    let beta_handler: Arc<dyn InboundHandler> =
        Arc::new(RecordingInbound::new("beta", Arc::clone(&events)));
    let beta_handler = handler_from_inbound(beta_handler);
    let beta_handle = controller.add_handler_after(alpha_handle, "beta", beta_handler);
    assert_ne!(beta_handle, PipelineHandleId::INBOUND_HEAD);
    let epoch_after = controller.epoch();
    assert!(epoch_after > epoch_before, "插入新 Handler 必须自增 epoch");

    release.wait();
    reader.join().expect("读线程应顺利结束");

    controller.emit_read(PipelineMessage::from_user(TestMessage { id: 2 }));

    let recorded = events.lock();
    assert_eq!(
        recorded.as_slice(),
        ["alpha:1", "alpha:2", "beta:2"],
        "首条消息不应经过 beta，而第二条应经过完整链路",
    );

    let labels: Vec<_> = controller
        .registry()
        .snapshot()
        .into_iter()
        .map(|entry| entry.label().to_string())
        .collect();
    assert_eq!(labels, ["alpha", "beta"], "注册表顺序应保持插入语义");
}

/// 构建绑定 `HotSwapPipeline` 的测试通道与控制器。
///
/// # 教案式说明
/// - **意图 (Why)**：多条测试用例均需相同的控制器初始化流程，集中封装以保证依赖一致性并降低样板代码。
/// - **逻辑 (How)**：创建无操作的运行时与观测组件，初始化 `CallContext` 与 `HotSwapPipeline`，随后将控制器绑定到测试通道。
/// - **契约 (What)**：返回 `(TestChannel, HotSwapPipeline)` 的 `Arc` 元组，调用方需在生命周期结束前保持引用，以免控制器被提前释放。
/// - **权衡 (Trade-offs)**：默认构造的缓冲池与健康探针均为空，实现目标是减少测试依赖；
///   若想验证缓冲背压或健康检查逻辑，需要对返回的 `CoreServices` 进行二次配置。
fn build_hot_swap_controller(channel_id: &str) -> (Arc<TestChannel>, Arc<HotSwapPipeline>) {
    let runtime = Arc::new(NoopRuntime::new());
    let logger = Arc::new(NoopLogger);
    let ops = Arc::new(NoopOpsBus::default());
    let metrics = Arc::new(NoopMetricsProvider);

    let services = CoreServices::with_observability_facade(
        runtime as Arc<dyn AsyncRuntime>,
        Arc::new(NoopBufferPool),
        DefaultObservabilityFacade::new(
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

    let channel = Arc::new(TestChannel::new(channel_id));
    let controller =
        HotSwapPipeline::new(channel.clone() as Arc<dyn Channel>, services, call_context);
    channel.bind_controller(controller.clone() as Arc<dyn Pipeline<HandleId = PipelineHandleId>>);

    (channel, controller)
}

/// 测试专用的 Channel，实现最小接口以绑定 `HotSwapPipeline`。
struct TestChannel {
    id: String,
    controller: OnceLock<Arc<dyn Pipeline<HandleId = PipelineHandleId>>>,
    extensions: NoopExtensions,
}

impl TestChannel {
    /// 构造带指定 ID 的测试通道。
    fn new(id: &str) -> Self {
        Self {
            id: id.to_string(),
            controller: OnceLock::new(),
            extensions: NoopExtensions,
        }
    }

    /// 绑定控制器供 Pipeline 回调使用。
    fn bind_controller(&self, controller: Arc<dyn Pipeline<HandleId = PipelineHandleId>>) {
        if self.controller.set(controller).is_err() {
            panic!("controller should only be bound once");
        }
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

    fn write(
        &self,
        _msg: PipelineMessage,
    ) -> spark_core::Result<WriteSignal, spark_core::error::CoreError> {
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

/// 仅记录事件顺序的 Inbound Handler，用于验证热插拔行为。
struct RecordingInbound {
    name: &'static str,
    events: Arc<Mutex<Vec<String>>>,
}

impl RecordingInbound {
    /// 创建记录器并持有共享事件缓冲。
    fn new(name: &'static str, events: Arc<Mutex<Vec<String>>>) -> Self {
        Self { name, events }
    }
}

impl InboundHandler for RecordingInbound {
    fn describe(&self) -> InitializerDescriptor {
        InitializerDescriptor::new(
            self.name,
            "tck.hot-swap",
            "records pipeline events for hot swap verification",
        )
    }

    fn on_channel_active(&self, _ctx: &dyn spark_router::pipeline::Context) {}

    fn on_read(&self, ctx: &dyn spark_router::pipeline::Context, msg: PipelineMessage) {
        match msg.try_into_user::<TestMessage>() {
            Ok(user) => {
                self.events
                    .lock()
                    .push(format!("{}:{}", self.name, user.id));
                ctx.forward_read(PipelineMessage::from_user(user));
            }
            Err(other) => ctx.forward_read(other),
        }
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
        _event: spark_core::observability::CoreUserEvent,
    ) {
    }

    fn on_exception_caught(
        &self,
        _ctx: &dyn spark_router::pipeline::Context,
        _error: spark_core::error::CoreError,
    ) {
    }

    fn on_channel_inactive(&self, _ctx: &dyn spark_router::pipeline::Context) {}
}

/// 在线程间同步的记录型 Handler：在写入共享事件缓冲后，通过双栅栏控制消息何时继续向后游走。
struct BlockingRecordingInbound {
    name: &'static str,
    events: Arc<Mutex<Vec<String>>>,
    start: Arc<Barrier>,
    release: Arc<Barrier>,
    first_gate_consumed: AtomicBool,
}

impl BlockingRecordingInbound {
    /// 构造带同步栅栏的 Handler。
    ///
    /// # 教案式说明
    /// - **意图 (Why)**：测试需要在 Handler 正在执行时插入新的链路节点，因此使用栅栏挂起执行线程，便于主线程观察并发窗口。
    /// - **逻辑 (How)**：`start` 栅栏用于通知主线程“已进入 Handler”；`release` 栅栏控制何时继续转发消息；
    ///   借助 `AtomicBool` 确保仅首条消息触发双栅栏同步，后续消息不再阻塞测试线程。
    /// - **契约 (What)**：调用方需保证两个栅栏的参与者均为 2（主线程 + 读线程），否则将导致永久阻塞。
    fn new(
        name: &'static str,
        events: Arc<Mutex<Vec<String>>>,
        start: Arc<Barrier>,
        release: Arc<Barrier>,
    ) -> Self {
        Self {
            name,
            events,
            start,
            release,
            first_gate_consumed: AtomicBool::new(false),
        }
    }
}

impl InboundHandler for BlockingRecordingInbound {
    fn describe(&self) -> InitializerDescriptor {
        InitializerDescriptor::new(
            self.name,
            "tck.hot-swap",
            "blocking recorder used for epoch barrier assertions",
        )
    }

    fn on_channel_active(&self, _ctx: &dyn spark_router::pipeline::Context) {}

    fn on_read(&self, ctx: &dyn spark_router::pipeline::Context, msg: PipelineMessage) {
        match msg.try_into_user::<TestMessage>() {
            Ok(user) => {
                {
                    self.events
                        .lock()
                        .push(format!("{}:{}", self.name, user.id));
                }
                if !self.first_gate_consumed.swap(true, AtomicOrdering::AcqRel) {
                    // 仅在首条消息上构造并发窗口，以验证栅栏语义；后续消息保持直通，避免测试线程再次阻塞。
                    self.start.wait();
                    self.release.wait();
                }
                ctx.forward_read(PipelineMessage::from_user(user));
            }
            Err(other) => ctx.forward_read(other),
        }
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
        _event: spark_core::observability::CoreUserEvent,
    ) {
    }

    fn on_exception_caught(
        &self,
        _ctx: &dyn spark_router::pipeline::Context,
        _error: spark_core::error::CoreError,
    ) {
    }

    fn on_channel_inactive(&self, _ctx: &dyn spark_router::pipeline::Context) {}
}

#[derive(Clone, Copy)]
struct TestMessage {
    id: usize,
}

#[derive(Default)]
struct NoopOpsBus {
    events: Mutex<Vec<OpsEvent>>,
    policies: Mutex<Vec<(OpsEventKind, EventPolicy)>>,
}

impl OpsEventBus for NoopOpsBus {
    fn broadcast(&self, event: OpsEvent) {
        self.events.lock().push(event);
    }

    fn subscribe(&self) -> spark_core::BoxStream<'static, OpsEvent> {
        Box::pin(EmptyStream)
    }

    fn set_event_policy(&self, kind: OpsEventKind, policy: EventPolicy) {
        self.policies.lock().push((kind, policy));
    }
}

struct EmptyStream;

impl spark_core::Stream for EmptyStream {
    type Item = OpsEvent;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut TaskContext<'_>) -> Poll<Option<Self::Item>> {
        Poll::Ready(None)
    }
}

struct NoopBufferPool;

impl BufferPool for NoopBufferPool {
    fn acquire(
        &self,
        _: usize,
    ) -> spark_core::Result<Box<dyn WritableBuffer>, spark_core::error::CoreError> {
        Err(spark_core::error::CoreError::new(
            "buffer.disabled",
            "buffer pool disabled",
        ))
    }

    fn shrink_to_fit(&self) -> spark_core::Result<usize, spark_core::error::CoreError> {
        Ok(0)
    }

    fn statistics(
        &self,
    ) -> spark_core::Result<spark_core::buffer::PoolStats, spark_core::error::CoreError> {
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

struct NoopRuntime;

impl NoopRuntime {
    fn new() -> Self {
        Self
    }
}

impl TaskExecutor for NoopRuntime {
    fn spawn_dyn(
        &self,
        _: &CallContext,
        _: spark_core::BoxFuture<'static, TaskResult<Box<dyn std::any::Any + Send>>>,
    ) -> spark_core::runtime::JoinHandle<Box<dyn std::any::Any + Send>> {
        spark_core::runtime::JoinHandle::from_task_handle(Box::new(NoopTaskHandle))
    }
}

#[spark_core::async_trait]
impl TimeDriver for NoopRuntime {
    fn now(&self) -> MonotonicTimePoint {
        MonotonicTimePoint::from_offset(Duration::from_millis(0))
    }

    async fn sleep(&self, _: Duration) {}
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
