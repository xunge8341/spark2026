use std::pin::Pin;
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc, Mutex,
};
use std::any::Any;
use std::panic::{self, AssertUnwindSafe};
use std::task::{Context, Poll};
use std::time::Duration;

use spark_core::async_trait;
use spark_core::buffer::BufferPool;
use spark_core::contract::{CallContext, CloseReason, Deadline};
use spark_core::error::CoreError;
use spark_core::future::{BoxFuture, Stream};
use spark_core::host::{GracefulShutdownStatus, GracefulShutdownTarget};
use spark_hosting::shutdown::GracefulShutdownCoordinator;
use spark_core::observability::{
    AttributeSet, Counter, EventPolicy, Gauge, Histogram, LogRecord, LogSeverity, Logger,
    MetricAttributeValue, MetricsProvider, OpsEvent, OpsEventBus, OpsEventKind,
};
use spark_core::runtime::{
    AsyncRuntime, CoreServices, JoinHandle, MonotonicTimePoint, TaskCancellationStrategy,
    TaskError, TaskExecutor, TaskHandle, TaskResult, TimeDriver,
};
use spark_core::{BoxStream, SparkError};
use spark_core::test_stubs::observability::{
    NoopCounter, NoopGauge, NoopHistogram, StaticObservabilityFacade,
};

use super::support::{block_on, monotonic};

/// 空事件流，满足 `OpsEventBus::subscribe` 的返回需求。
struct EmptyStream;

impl Stream for EmptyStream {
    type Item = OpsEvent;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Ready(None)
    }
}

/// 记录日志的测试实现，持久化每条 `LogRecord` 以便断言。
#[derive(Default)]
struct TestLogger {
    records: Mutex<Vec<OwnedLog>>,
}

#[derive(Debug)]
struct OwnedLog {
    severity: LogSeverity,
    message: String,
    attributes: Vec<(String, String)>,
}

impl TestLogger {
    fn take(&self) -> Vec<OwnedLog> {
        self.records.lock().unwrap().drain(..).collect()
    }
}

impl Logger for TestLogger {
    fn log(&self, record: &LogRecord<'_>) {
        let message = record.message.clone().into_owned();
        let attributes = record
            .attributes
            .iter()
            .map(|kv| (kv.key.to_string(), render_value(&kv.value)))
            .collect();
        let entry = OwnedLog {
            severity: record.severity,
            message,
            attributes,
        };
        self.records.lock().unwrap().push(entry);
    }
}

fn render_value(value: &MetricAttributeValue<'_>) -> String {
    match value {
        MetricAttributeValue::Text(text) => text.to_string(),
        MetricAttributeValue::Bool(flag) => flag.to_string(),
        MetricAttributeValue::F64(val) => val.to_string(),
        MetricAttributeValue::I64(val) => val.to_string(),
    }
}

/// 记录运维事件的测试总线。
#[derive(Default)]
struct TestOpsBus {
    events: Mutex<Vec<OpsEvent>>,
    policies: Mutex<Vec<(OpsEventKind, EventPolicy)>>,
}

impl OpsEventBus for TestOpsBus {
    fn broadcast(&self, event: OpsEvent) {
        self.events.lock().unwrap().push(event);
    }

    fn subscribe(&self) -> BoxStream<'static, OpsEvent> {
        Box::pin(EmptyStream)
    }

    fn set_event_policy(&self, kind: OpsEventKind, policy: EventPolicy) {
        self.policies.lock().unwrap().push((kind, policy));
    }
}

/// 运行时桩，实现计时语义并记录 `sleep` 调用。
#[derive(Default)]
struct TestRuntime {
    now: Mutex<Duration>,
    sleep_log: Mutex<Vec<Duration>>,
}

impl TestRuntime {
    fn new() -> Self {
        Self::default()
    }

    fn sleep_calls(&self) -> Vec<Duration> {
        self.sleep_log.lock().unwrap().clone()
    }
}

impl TaskExecutor for TestRuntime {
    fn spawn_dyn(
        &self,
        _ctx: &CallContext,
        fut: BoxFuture<'static, TaskResult<Box<dyn Any + Send>>>,
    ) -> JoinHandle<Box<dyn Any + Send>> {
        let result = panic::catch_unwind(AssertUnwindSafe(|| block_on(fut)));
        let task_result = match result {
            Ok(value) => value,
            Err(_) => Err(TaskError::Panicked),
        };
        JoinHandle::from_task_handle(Box::new(ReadyHandle::new(task_result)))
    }
}

#[async_trait]
impl TimeDriver for TestRuntime {
    fn now(&self) -> MonotonicTimePoint {
        MonotonicTimePoint::from_offset(*self.now.lock().unwrap())
    }

    async fn sleep(&self, duration: Duration) {
        self.sleep_log.lock().unwrap().push(duration);
        let mut guard = self.now.lock().unwrap();
        *guard = guard.checked_add(duration).expect("测试时钟不应溢出");
    }
}

/// 即时完成的测试句柄，将 `spawn` 的结果封装为 [`JoinHandle`]。
struct ReadyHandle<T> {
    result: Mutex<Option<TaskResult<T>>>,
    finished: AtomicBool,
    cancelled: AtomicBool,
}

impl<T> ReadyHandle<T> {
    fn new(result: TaskResult<T>) -> Self {
        Self {
            result: Mutex::new(Some(result)),
            finished: AtomicBool::new(true),
            cancelled: AtomicBool::new(false),
        }
    }
}

#[async_trait]
impl<T> TaskHandle for ReadyHandle<T>
where
    T: Send + 'static,
{
    type Output = T;

    fn cancel(&self, _strategy: TaskCancellationStrategy) {
        self.cancelled.store(true, Ordering::SeqCst);
    }

    fn is_finished(&self) -> bool {
        self.finished.load(Ordering::Acquire)
    }

    fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }

    fn id(&self) -> Option<&str> {
        None
    }

    fn detach(self: Box<Self>) {}

    async fn join(self: Box<Self>) -> TaskResult<Self::Output> {
        self.finished.store(true, Ordering::Release);
        self.result
            .lock()
            .unwrap()
            .take()
            .expect("ReadyHandle::join called multiple times")
    }
}

/// 测试用缓冲池，方法不会被调用。
#[derive(Default)]
struct TestBufferPool;

impl BufferPool for TestBufferPool {
    fn acquire(&self, _min_capacity: usize) -> spark_core::Result<Box<dyn spark_core::buffer::WritableBuffer>, CoreError> {
        Err(CoreError::new("test.buffer", "acquire not used in tests"))
    }

    fn shrink_to_fit(&self) -> spark_core::Result<usize, CoreError> {
        Ok(0)
    }

    fn statistics(&self) -> spark_core::Result<spark_core::buffer::PoolStats, CoreError> {
        Ok(Default::default())
    }
}

/// 空指标实现，避免在测试中触发额外逻辑。
#[derive(Default)]
struct TestMetrics;

impl MetricsProvider for TestMetrics {
    fn counter(&self, _descriptor: &spark_core::observability::InstrumentDescriptor<'_>) -> Arc<dyn Counter> {
        Arc::new(NoopCounter)
    }

    fn gauge(&self, _descriptor: &spark_core::observability::InstrumentDescriptor<'_>) -> Arc<dyn Gauge> {
        Arc::new(NoopGauge)
    }

    fn histogram(&self, _descriptor: &spark_core::observability::InstrumentDescriptor<'_>) -> Arc<dyn Histogram> {
        Arc::new(NoopHistogram)
    }
}

/// 构造用于测试的运行时服务集合。
///
/// # 教案式说明
/// - **意图 (Why)**：契约测试需要快速装配 `CoreServices`，以验证在 Facade 模式下，协调器能够拿到与生产环境一致的日志、
///   指标与事件句柄。
/// - **逻辑 (How)**：
///   1. 调用 [`CoreServices::with_observability_facade`]，减少逐字段填写的重复；
///   2. 使用 [`StaticObservabilityFacade`](spark_core::test_stubs::observability::StaticObservabilityFacade)
///      组合传入句柄，保持旧代码与新 Facade 之间的一致语义；
///   3. 注入空的健康探针向量，突出该测试关注停机流程而非探针健康度。
/// - **契约 (What)**：
///   - 输入：已经实现线程安全的运行时、日志、运维事件与指标桩对象；
///   - 前置：所有句柄在测试期间有效，不会提前释放；
///   - 后置：返回的 `CoreServices` 可被协调器克隆复用，并保持可观测性句柄一致。
/// - **权衡 (Trade-offs)**：函数固定使用空缓冲池；若测试需验证缓冲租借或背压行为，
///   请在调用后替换 `buffer_pool` 字段。
fn build_core_services(
    runtime: Arc<dyn AsyncRuntime>,
    logger: Arc<dyn Logger>,
    ops: Arc<dyn OpsEventBus>,
    metrics: Arc<dyn MetricsProvider>,
) -> CoreServices {
    CoreServices::with_observability_facade(
        runtime,
        Arc::new(TestBufferPool::default()),
        StaticObservabilityFacade::new(logger, metrics, ops, Arc::new(Vec::new())),
    )
}

/// 当所有目标都能在截止时间内完成时，协调器应返回 `Completed` 状态且不触发硬关闭。
///
/// # 目标（Why）
/// - 验证 `GracefulShutdownCoordinator` 在常规停机路径上会广播运维事件、输出 INFO 日志并等待所有目标完成。
///
/// # 步骤（How）
/// 1. 注册两个自定义关闭目标，记录是否收到 `graceful_close` 与 `force_close` 回调；
/// 2. 调用 `shutdown`，不设置截止时间；
/// 3. 断言报告全部为 `Completed`，日志包含启动记录，硬关闭计数为 0。
///
/// # 契约（What）
/// - `GracefulShutdownTarget::for_callbacks` 应只调用优雅关闭，不触发硬关闭；
/// - `OpsEvent::ShutdownTriggered` 的 `deadline` 应退化为 `Duration::MAX`；
/// - 报告的 `forced_count()` 返回 0。
#[test]
fn graceful_shutdown_completes_without_force_close() {
    let runtime = Arc::new(TestRuntime::new());
    let logger = Arc::new(TestLogger::default());
    let ops = Arc::new(TestOpsBus::default());
    let metrics = Arc::new(TestMetrics::default());

    let runtime_dyn: Arc<dyn AsyncRuntime> = Arc::clone(&runtime);
    let logger_dyn: Arc<dyn Logger> = Arc::clone(&logger);
    let ops_dyn: Arc<dyn OpsEventBus> = Arc::clone(&ops);
    let metrics_dyn: Arc<dyn MetricsProvider> = Arc::clone(&metrics);
    let services = build_core_services(runtime_dyn, logger_dyn, ops_dyn, metrics_dyn);
    let mut coordinator = GracefulShutdownCoordinator::new(services);

    let graceful_a = Arc::new(AtomicBool::new(false));
    let forced_a = Arc::new(AtomicBool::new(false));
    coordinator.register_target(GracefulShutdownTarget::for_callbacks(
        "alpha",
        {
            let graceful = Arc::clone(&graceful_a);
            move |_, _| {
                graceful.store(true, Ordering::SeqCst);
            }
        },
        || Box::pin(async { Ok::<(), SparkError>(()) }),
        {
            let forced = Arc::clone(&forced_a);
            move || {
                forced.store(true, Ordering::SeqCst);
            }
        },
    ));

    let graceful_b = Arc::new(AtomicBool::new(false));
    let forced_b = Arc::new(AtomicBool::new(false));
    coordinator.register_target(GracefulShutdownTarget::for_callbacks(
        "beta",
        {
            let graceful = Arc::clone(&graceful_b);
            move |_, _| {
                graceful.store(true, Ordering::SeqCst);
            }
        },
        || Box::pin(async { Ok::<(), SparkError>(()) }),
        {
            let forced = Arc::clone(&forced_b);
            move || {
                forced.store(true, Ordering::SeqCst);
            }
        },
    ));

    let reason = CloseReason::new("spark.test.shutdown", "unit test shutdown");
    let report = block_on(coordinator.shutdown(reason.clone(), None));

    assert_eq!(report.results().len(), 2, "应包含两个目标记录");
    assert_eq!(report.forced_count(), 0, "不应触发硬关闭");
    assert!(matches!(report.results()[0].status(), GracefulShutdownStatus::Completed));
    assert!(matches!(report.results()[1].status(), GracefulShutdownStatus::Completed));
    assert!(graceful_a.load(Ordering::SeqCst), "第一个目标应收到优雅关闭通知");
    assert!(graceful_b.load(Ordering::SeqCst), "第二个目标应收到优雅关闭通知");
    assert!(!forced_a.load(Ordering::SeqCst), "正常路径不应调用硬关闭");
    assert!(!forced_b.load(Ordering::SeqCst), "正常路径不应调用硬关闭");

    let events = ops.events.lock().unwrap().clone();
    assert_eq!(events.len(), 1, "应广播一次 ShutdownTriggered 运维事件");
    if let OpsEvent::ShutdownTriggered { deadline } = &events[0] {
        assert_eq!(*deadline, Duration::MAX, "默认截止时间应扩展为无限期");
    } else {
        panic!("事件类型应为 ShutdownTriggered");
    }

    let logs = logger.take();
    assert!(logs.iter().any(|log| log.severity == LogSeverity::Info && log.message.contains("graceful shutdown initiated")),
        "应输出启动优雅关闭的 INFO 日志");
    assert_eq!(report.reason().code(), reason.code(), "报告应携带原始关闭原因");
}

/// 当目标在截止时间内未完成时，协调器应记录 WARN 日志并触发硬关闭。
///
/// # 目标（Why）
/// - 验证超时路径会调用 `force_close`、输出 WARN 日志并在报告中标记 `ForcedTimeout`。
///
/// # 步骤（How）
/// 1. 注册一个始终 Pending 的关闭目标；
/// 2. 设定截止时间为 1 秒，调用 `shutdown`；
/// 3. 断言报告状态为 `ForcedTimeout`，硬关闭计数为 1，并检查 WARN 日志与运维事件中的 deadline。
///
/// # 契约（What）
/// - `TimeDriver::sleep` 的调用应记录到测试运行时；
/// - `GracefulShutdownRecord::elapsed` 至少为截止时间；
/// - WARN 日志字段包含目标标签。
#[test]
fn graceful_shutdown_triggers_force_close_on_timeout() {
    let runtime = Arc::new(TestRuntime::new());
    let logger = Arc::new(TestLogger::default());
    let ops = Arc::new(TestOpsBus::default());
    let metrics = Arc::new(TestMetrics::default());

    let runtime_dyn: Arc<dyn AsyncRuntime> = Arc::clone(&runtime);
    let logger_dyn: Arc<dyn Logger> = Arc::clone(&logger);
    let ops_dyn: Arc<dyn OpsEventBus> = Arc::clone(&ops);
    let metrics_dyn: Arc<dyn MetricsProvider> = Arc::clone(&metrics);
    let services = build_core_services(runtime_dyn, logger_dyn, ops_dyn, metrics_dyn);
    let mut coordinator = GracefulShutdownCoordinator::new(services);

    let forced = Arc::new(AtomicUsize::new(0));
    coordinator.register_target(GracefulShutdownTarget::for_callbacks(
        "stalling",
        |_reason, _deadline| {},
        || Box::pin(async {
            core::future::pending::<()>().await;
            Ok::<(), SparkError>(())
        }),
        {
            let forced = Arc::clone(&forced);
            move || {
                forced.fetch_add(1, Ordering::SeqCst);
            }
        },
    ));

    let deadline = Deadline::with_timeout(monotonic(0, 0), Duration::from_secs(1));
    let report = block_on(coordinator.shutdown(CloseReason::new("spark.test.timeout", "target stalled"), Some(deadline)));

    assert_eq!(report.results().len(), 1);
    match report.results()[0].status() {
        GracefulShutdownStatus::ForcedTimeout => {}
        other => panic!("预期 ForcedTimeout，实际为 {:?}", other),
    }
    assert_eq!(report.forced_count(), 1, "应有一个目标被硬关闭");
    assert_eq!(forced.load(Ordering::SeqCst), 1, "硬关闭回调应被调用一次");
    assert!(report.results()[0].elapsed() >= Duration::from_secs(1), "耗时应至少为截止时间");

    let events = ops.events.lock().unwrap().clone();
    assert_eq!(events.len(), 1);
    if let OpsEvent::ShutdownTriggered { deadline } = &events[0] {
        assert_eq!(*deadline, Duration::from_secs(1), "运维事件应传播剩余时间");
    } else {
        panic!("事件类型应为 ShutdownTriggered");
    }

    let logs = logger.take();
    assert!(logs.iter().any(|log| log.severity == LogSeverity::Warn && log.message.contains("forced")),
        "应输出 WARN 日志说明触发硬关闭");
}

/// 当目标返回 `SparkError` 时，协调器应记录 ERROR 日志并在报告中标记 `Failed`。
///
/// # 目标（Why）
/// - 验证失败路径仍按顺序等待，并将错误透传至报告供审计使用。
///
/// # 步骤（How）
/// 1. 注册一个立即返回 `Err(SparkError)` 的目标；
/// 2. 调用 `shutdown`，确认不会触发硬关闭；
/// 3. 断言报告状态为 `Failed`，日志包含 ERROR 级别记录。
///
/// # 契约（What）
/// - 报告的 `failure_count()` 返回 1；
/// - 日志包含错误码；
/// - 运维事件仍应广播，以便上游观测到停机动作。
#[test]
fn graceful_shutdown_reports_failures() {
    let runtime = Arc::new(TestRuntime::new());
    let logger = Arc::new(TestLogger::default());
    let ops = Arc::new(TestOpsBus::default());
    let metrics = Arc::new(TestMetrics::default());

    let runtime_dyn: Arc<dyn AsyncRuntime> = Arc::clone(&runtime);
    let logger_dyn: Arc<dyn Logger> = Arc::clone(&logger);
    let ops_dyn: Arc<dyn OpsEventBus> = Arc::clone(&ops);
    let metrics_dyn: Arc<dyn MetricsProvider> = Arc::clone(&metrics);
    let services = build_core_services(runtime_dyn, logger_dyn, ops_dyn, metrics_dyn);
    let mut coordinator = GracefulShutdownCoordinator::new(services);

    coordinator.register_target(GracefulShutdownTarget::for_callbacks(
        "failing",
        |_reason, _deadline| {},
        || {
            Box::pin(async {
                Err(SparkError::new("spark.test.failure", "synthetic error"))
            })
        },
        || {},
    ));

    let report = block_on(coordinator.shutdown(CloseReason::new("spark.test.failure", "propagate failure"), None));

    assert_eq!(report.results().len(), 1);
    assert_eq!(report.forced_count(), 0, "出现错误时不应触发硬关闭");
    assert_eq!(report.failure_count(), 1, "应统计到一个失败目标");
    match report.results()[0].status() {
        GracefulShutdownStatus::Failed(err) => {
            assert_eq!(err.code(), "spark.test.failure");
        }
        other => panic!("预期 Failed，实际为 {:?}", other),
    }

    let logs = logger.take();
    assert!(logs.iter().any(|log| log.severity == LogSeverity::Error && log.message.contains("failed")),
        "失败路径应输出 ERROR 日志");
    assert_eq!(ops.events.lock().unwrap().len(), 1, "即便失败也应广播停机事件");
}
