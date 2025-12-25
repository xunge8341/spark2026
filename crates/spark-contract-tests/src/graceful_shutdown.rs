use crate::case::{TckCase, TckSuite};
use crate::support::monotonic;
use parking_lot::Mutex;
use spark_core as spark_router;
use spark_core::contract::{CloseReason, Deadline};
use spark_core::future::Stream;
use spark_core::host::GracefulShutdownStatus;
use spark_core::observability::{
    Counter, EventPolicy, Gauge, Histogram, LogRecord, LogSeverity, Logger, MetricsProvider,
    OpsEvent, OpsEventBus, OpsEventKind,
};
use spark_core::runtime::{
    AsyncRuntime, CoreServices, JoinHandle, MonotonicTimePoint, TaskCancellationStrategy,
    TaskError, TaskExecutor, TaskHandle, TaskResult, TimeDriver,
};
use spark_core::test_stubs::observability::{NoopCounter, NoopGauge, NoopHistogram};
use spark_core::{BoxStream, SparkError};
use spark_hosting::shutdown::GracefulShutdownCoordinator;
use spark_otel::facade::DefaultObservabilityFacade;
use spark_router::pipeline::channel::ChannelState;
use spark_router::pipeline::controller::{
    HandlerRegistration, HandlerRegistry, Pipeline, PipelineHandleId,
};
use spark_router::pipeline::{Channel, ExtensionsMap};
use std::any::Any;
use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::{
    Arc, Weak,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};
use std::time::Duration;

const CASES: &[TckCase] = &[
    TckCase {
        name: "channel_fin_invocation_is_forwarded_once",
        test: channel_fin_invocation_is_forwarded_once,
    },
    TckCase {
        name: "channel_half_close_waits_for_remote_ack",
        test: channel_half_close_waits_for_remote_ack,
    },
    TckCase {
        name: "expired_deadline_triggers_immediate_force_close",
        test: expired_deadline_triggers_immediate_force_close,
    },
    TckCase {
        name: "pending_channel_times_out_and_forces_close",
        test: pending_channel_times_out_and_forces_close,
    },
    TckCase {
        name: "channel_half_close_requires_dual_ack_steps",
        test: channel_half_close_requires_dual_ack_steps,
    },
    TckCase {
        name: "channel_closed_future_error_reports_failure",
        test: channel_closed_future_error_reports_failure,
    },
];

const SUITE: TckSuite = TckSuite {
    name: "graceful_shutdown",
    cases: CASES,
};

/// 返回“优雅关闭”主题的测试套件。
///
/// # 教案式说明
/// - **意图 (Why)**：确保 `GracefulShutdownCoordinator` 在 FIN、半关闭、超时与错误路径上的契约被下游实现完整复现。
/// - **逻辑 (How)**：套件内的六个用例分别覆盖 FIN 转发、半关闭等待、超时触发、立即强制、双向确认与错误报告，
///   均基于确定性的计时器桩与手工执行器来运行。
/// - **契约 (What)**：返回 `'static` 套件描述，供 `run_graceful_shutdown_suite` 或 `#[spark_tck]` 宏直接引用。
pub const fn suite() -> &'static TckSuite {
    &SUITE
}

/// 验证通过 `GracefulShutdownTarget::for_channel` 注册的通道，协调器会在等待阶段前恰好触发一次 FIN（`close_graceful`）。
///
/// # 教案式说明
/// - **意图 (Why)**：FIN 信号必须先于 `closed()` 等待流程执行，避免直接进入等待导致 TCP/QUIC 半关闭僵死。
/// - **逻辑 (How)**：构造教学运行时与通道桩，发起 `shutdown` 并通过 `ShutdownDriver` 轮询到完成，再检查 FIN 调用次数、
///   原因与截止时间是否透传，并确认日志与运维事件。
/// - **契约 (What)**：
///   - 输入：合法的 `CloseReason` 与未来的 `Deadline`；
///   - 前置：通道 `closed()` 可立即完成；
///   - 后置：FIN 恰好调用一次、不会执行 `close()`，报告状态为 `Completed` 并产生 INFO 日志。
fn channel_fin_invocation_is_forwarded_once() {
    let runtime = Arc::new(TestRuntime::new());
    let logger = Arc::new(TestLogger::default());
    let ops = Arc::new(TestOpsBus::default());
    let metrics = Arc::new(TestMetrics);

    let services = build_core_services(
        Arc::clone(&runtime) as Arc<dyn AsyncRuntime>,
        Arc::clone(&logger) as Arc<dyn Logger>,
        Arc::clone(&ops) as Arc<dyn OpsEventBus>,
        Arc::clone(&metrics) as Arc<dyn MetricsProvider>,
    );

    let controller = Arc::new(NoopController);
    let channel = Arc::new(TestChannel::new("fin-channel", controller));
    let recorder = channel.recorder();

    let mut coordinator = GracefulShutdownCoordinator::new(services);
    coordinator.register_channel("fin", channel.clone() as Arc<dyn Channel>);

    let reason = CloseReason::new("spark.tck.shutdown.fin", "ensure FIN dispatch");
    let deadline = Deadline::with_timeout(monotonic(10, 0), Duration::from_secs(5));

    let report = ShutdownDriver::new(coordinator, reason.clone(), Some(deadline)).poll_to_ready();

    assert_eq!(report.results().len(), 1, "仅注册一个通道应生成单条记录");
    assert_eq!(report.forced_count(), 0, "FIN 成功路径不应触发硬关闭");
    assert!(matches!(
        report.results()[0].status(),
        GracefulShutdownStatus::Completed
    ));

    assert_eq!(
        recorder.graceful_calls.load(Ordering::SeqCst),
        1,
        "close_graceful 必须仅触发一次"
    );
    assert_eq!(
        recorder.force_calls.load(Ordering::SeqCst),
        0,
        "正常收敛路径不应调用 close()"
    );

    let reasons = recorder.graceful_reasons.lock();
    assert_eq!(reasons.len(), 1, "记录的 FIN 原因数量应与调用次数一致");
    assert_eq!(reasons[0].code(), reason.code());
    assert_eq!(reasons[0].message(), reason.message());

    let deadlines = recorder.graceful_deadlines.lock();
    assert_eq!(deadlines.len(), 1, "Deadline 传播记录应保持一一对应");
    assert_eq!(
        deadlines[0],
        deadline.instant(),
        "GracefulShutdownTarget::for_channel 应透传 deadline"
    );

    assert_eq!(
        ops.events.lock().len(),
        1,
        "应广播一次 ShutdownTriggered 事件"
    );
    assert!(
        logger
            .records
            .lock()
            .iter()
            .any(|entry| entry.severity == LogSeverity::Info
                && entry.message.contains("graceful shutdown initiated")),
        "INFO 日志应标记停机启动"
    );
}

/// 验证协调器在收到 FIN 后会等待 `closed()`（半关闭确认）完成，而不会提前返回或强制终止。
///
/// # 教案式说明
/// - **意图 (Why)**：半关闭阶段允许对端冲刷剩余数据；若协调器在 `close_graceful` 后立即结束，第三方实现可能丢失尾包。
/// - **逻辑 (How)**：通过 `ShutdownDriver` 首次轮询 `shutdown` Future，确认状态为 Pending 且 FIN 已到达；随后手动调用
///   `complete_closed()`，再次轮询直到返回，并核对 `close()` 未被调用。
/// - **契约 (What)**：
///   - 输入：`Deadline` 为空，表示可无限等待；
///   - 前置：`complete_closed()` 在第二次轮询前执行；
///   - 后置：报告状态为 `Completed`，FIN 调用计数为 1，强制关闭计数保持为 0。
fn channel_half_close_waits_for_remote_ack() {
    let runtime = Arc::new(TestRuntime::new());
    let logger = Arc::new(TestLogger::default());
    let ops = Arc::new(TestOpsBus::default());
    let metrics = Arc::new(TestMetrics);

    let services = build_core_services(
        Arc::clone(&runtime) as Arc<dyn AsyncRuntime>,
        Arc::clone(&logger) as Arc<dyn Logger>,
        Arc::clone(&ops) as Arc<dyn OpsEventBus>,
        Arc::clone(&metrics) as Arc<dyn MetricsProvider>,
    );

    let controller = Arc::new(NoopController);
    let channel = Arc::new(TestChannel::new("half-close", controller));
    let recorder = channel.recorder();

    let mut coordinator = GracefulShutdownCoordinator::new(services);
    coordinator.register_channel("half-close", channel.clone() as Arc<dyn Channel>);

    let reason = CloseReason::new("spark.tck.shutdown.half", "await peer confirmation");

    let mut driver = ShutdownDriver::new(coordinator, reason, None);
    assert!(
        matches!(driver.poll(), Poll::Pending),
        "未完成半关闭前应保持 Pending"
    );

    assert_eq!(
        recorder.graceful_calls.load(Ordering::SeqCst),
        1,
        "close_graceful 在首次轮询时就应触发"
    );
    assert_eq!(
        recorder.force_calls.load(Ordering::SeqCst),
        0,
        "等待阶段不应触发硬关闭"
    );

    recorder.complete_closed();

    let report = driver.poll_to_ready();

    assert_eq!(report.results().len(), 1);
    assert!(matches!(
        report.results()[0].status(),
        GracefulShutdownStatus::Completed
    ));
    assert_eq!(
        recorder.closed_completions.load(Ordering::SeqCst),
        1,
        "closed() 应完成一次"
    );
    assert_eq!(
        recorder.force_calls.load(Ordering::SeqCst),
        0,
        "整个流程未触发强制关闭"
    );
}

/// 验证当调用方提供的截止时间已过期时，协调器会立即进入强制取消路径，无需等待。
///
/// # 教案式说明
/// - **意图 (Why)**：在宿主触发“立即停止”时，不应再等待任何 FIN/半关闭流程，应马上执行 `force_close`。
/// - **逻辑 (How)**：设置零超时时间后直接运行 `shutdown`，检验报告状态、强制计数与日志等级，同时确认 `MockTimer`
///   未记录休眠。
/// - **契约 (What)**：
///   - 输入：`Deadline::with_timeout(..., Duration::ZERO)`；
///   - 前置：通道的 `closed()` 永远 Pending；
///   - 后置：报告状态为 `ForcedTimeout`，`force_close` 调用一次，计时器日志为空。
fn expired_deadline_triggers_immediate_force_close() {
    let runtime = Arc::new(TestRuntime::new());
    let logger = Arc::new(TestLogger::default());
    let ops = Arc::new(TestOpsBus::default());
    let metrics = Arc::new(TestMetrics);

    let services = build_core_services(
        Arc::clone(&runtime) as Arc<dyn AsyncRuntime>,
        Arc::clone(&logger) as Arc<dyn Logger>,
        Arc::clone(&ops) as Arc<dyn OpsEventBus>,
        Arc::clone(&metrics) as Arc<dyn MetricsProvider>,
    );

    let controller = Arc::new(NoopController);
    let channel = Arc::new(TestChannel::new("expired", controller));
    let recorder = channel.recorder();
    recorder.keep_closed_pending();

    let mut coordinator = GracefulShutdownCoordinator::new(services);
    coordinator.register_channel("expired", channel.clone() as Arc<dyn Channel>);

    let deadline = Deadline::with_timeout(monotonic(0, 0), Duration::ZERO);
    let report = ShutdownDriver::new(
        coordinator,
        CloseReason::new("spark.tck.shutdown.expired", "deadline already expired"),
        Some(deadline),
    )
    .poll_to_ready();

    assert_eq!(report.results().len(), 1);
    assert_eq!(report.forced_count(), 1, "到期场景必须统计强制关闭");
    match report.results()[0].status() {
        GracefulShutdownStatus::ForcedTimeout => {}
        other => panic!("预期 ForcedTimeout，实际为 {:?}", other),
    }

    assert_eq!(
        recorder.force_calls.load(Ordering::SeqCst),
        1,
        "force_close 应立即被调用"
    );
    assert!(
        runtime.fired_durations().is_empty(),
        "截止时间已过期时不应调用 sleep"
    );
    assert!(
        logger
            .records
            .lock()
            .iter()
            .any(|entry| entry.severity == LogSeverity::Warn && entry.message.contains("forced")),
        "WARN 日志需提示强制关闭"
    );
}

/// 验证当通道在截止时间内始终未完成时，协调器会等待超时后调用硬关闭并记录相应状态。
///
/// # 教案式说明
/// - **意图 (Why)**：确保运行时会等待完整截止时间并在超时后调用 `close()`，同时记录 WARN 日志与运维事件。
/// - **逻辑 (How)**：先让 `closed()` 永远 Pending，再通过 `ShutdownDriver` 捕获 Pending 状态；
///   之后使用 `MockTimer::expect_sleep` 获取 1 秒休眠句柄，手动触发并收集报告。
/// - **契约 (What)**：
///   - 输入：1 秒截止时间；
///   - 前置：在 Pending 阶段取得并触发计时器；
///   - 后置：报告状态为 `ForcedTimeout`，休眠日志仅包含 1 秒，`close()` 调用一次。
fn pending_channel_times_out_and_forces_close() {
    let runtime = Arc::new(TestRuntime::new());
    let logger = Arc::new(TestLogger::default());
    let ops = Arc::new(TestOpsBus::default());
    let metrics = Arc::new(TestMetrics);

    let services = build_core_services(
        Arc::clone(&runtime) as Arc<dyn AsyncRuntime>,
        Arc::clone(&logger) as Arc<dyn Logger>,
        Arc::clone(&ops) as Arc<dyn OpsEventBus>,
        Arc::clone(&metrics) as Arc<dyn MetricsProvider>,
    );

    let controller = Arc::new(NoopController);
    let channel = Arc::new(TestChannel::new("timeout", controller));
    let recorder = channel.recorder();
    recorder.keep_closed_pending();

    let mut coordinator = GracefulShutdownCoordinator::new(services);
    coordinator.register_channel("timeout", channel.clone() as Arc<dyn Channel>);

    let deadline = Deadline::with_timeout(monotonic(0, 0), Duration::from_secs(1));
    let mut driver = ShutdownDriver::new(
        coordinator,
        CloseReason::new("spark.tck.shutdown.timeout", "target stalled"),
        Some(deadline),
    );

    assert!(matches!(driver.poll(), Poll::Pending), "首次轮询应进入等待");
    let sleep = runtime.timer().expect_sleep("超时用例应安排 1 秒休眠");
    assert_eq!(sleep.duration(), Duration::from_secs(1));

    sleep.fire();
    let report = driver.poll_to_ready();

    assert_eq!(report.results().len(), 1);
    assert_eq!(report.forced_count(), 1, "超时场景应计入一次强制关闭");
    match report.results()[0].status() {
        GracefulShutdownStatus::ForcedTimeout => {}
        other => panic!("预期 ForcedTimeout，实际为 {:?}", other),
    }

    assert_eq!(
        recorder.force_calls.load(Ordering::SeqCst),
        1,
        "超时后必须调用 close()"
    );
    assert_eq!(
        runtime.fired_durations(),
        vec![Duration::from_secs(1)],
        "计时器应等待完整的截止时间"
    );
    assert!(
        logger
            .records
            .lock()
            .iter()
            .any(|entry| entry.severity == LogSeverity::Warn && entry.message.contains("forced")),
        "超时路径需记录 WARN 日志"
    );
    assert_eq!(
        recorder.closed_completions.load(Ordering::SeqCst),
        0,
        "强制关闭后 `closed()` Future 不应报告成功"
    );
    assert!(
        recorder.closed_drop_count() >= 1,
        "超时导致的取消必须释放 `closed()` Future 资源"
    );
}

/// 验证 `closed()` Future 只有在“读/写半关闭都确认”后才会完成，确保双向流水线被安全冲刷。
///
/// # 教案式说明
/// - **意图 (Why)**：强调协调器需等待所有半关闭确认步骤到齐，防止仅确认一个方向即提前返回。
/// - **逻辑 (How)**：配置通道需两步确认，轮询 `ShutdownDriver` 获取 Pending；依次调用 `ack_closed_step()` 并在第一次之后
///   验证 Future 仍 Pending，第二次后再收集报告。
/// - **契约 (What)**：
///   - 输入：无限等待（无 Deadline）；
///   - 前置：两次确认之间需插入一次轮询；
///   - 后置：报告状态为 `Completed`，未调用硬关闭，`closed()` 完成一次且释放资源。
fn channel_half_close_requires_dual_ack_steps() {
    let runtime = Arc::new(TestRuntime::new());
    let logger = Arc::new(TestLogger::default());
    let ops = Arc::new(TestOpsBus::default());
    let metrics = Arc::new(TestMetrics);

    let services = build_core_services(
        Arc::clone(&runtime) as Arc<dyn AsyncRuntime>,
        Arc::clone(&logger) as Arc<dyn Logger>,
        Arc::clone(&ops) as Arc<dyn OpsEventBus>,
        Arc::clone(&metrics) as Arc<dyn MetricsProvider>,
    );

    let controller = Arc::new(NoopController);
    let channel = Arc::new(TestChannel::new("dual-ack", controller));
    let recorder = channel.recorder();
    recorder.require_closed_steps(2);

    let mut coordinator = GracefulShutdownCoordinator::new(services);
    coordinator.register_channel("dual-ack", channel.clone() as Arc<dyn Channel>);

    let mut driver = ShutdownDriver::new(
        coordinator,
        CloseReason::new("spark.tck.shutdown.dual", "await read/write half-close"),
        None,
    );

    assert!(matches!(driver.poll(), Poll::Pending));
    assert_eq!(
        recorder.graceful_calls.load(Ordering::SeqCst),
        1,
        "FIN 应立即触发"
    );

    recorder.ack_closed_step();
    assert!(
        matches!(driver.poll(), Poll::Pending),
        "仅确认一个方向仍应等待"
    );

    recorder.ack_closed_step();
    let report = driver.poll_to_ready();

    assert_eq!(report.results().len(), 1);
    assert!(matches!(
        report.results()[0].status(),
        GracefulShutdownStatus::Completed
    ));
    assert_eq!(
        recorder.force_calls.load(Ordering::SeqCst),
        0,
        "双向确认完成后不应触发硬关闭"
    );
    assert_eq!(
        recorder.closed_completions.load(Ordering::SeqCst),
        1,
        "两个方向确认后应成功完成一次 closed()"
    );
    assert!(
        recorder.closed_drop_count() >= 1,
        "等待期间挂起的 Future 在完成后应被释放"
    );
}

/// 验证当 `closed()` 返回错误时，协调器会将状态标记为 `Failed` 并保留错误上下文。
///
/// # 教案式说明
/// - **意图 (Why)**：确保在半关闭阶段出现业务错误时，报告不会误判为成功或超时，便于宿主采取补救措施。
/// - **逻辑 (How)**：模拟 `closed()` 返回 `SparkError::internal`，轮询完成后检查报告状态、错误计数以及日志。
/// - **契约 (What)**：
///   - 输入：无截止时间；
///   - 前置：在轮询完成前调用 `complete_closed_with_error`；
///   - 后置：报告状态为 `Failed`，`closed()` 错误计数增 1，强制关闭计数保持为 0。
fn channel_closed_future_error_reports_failure() {
    let runtime = Arc::new(TestRuntime::new());
    let logger = Arc::new(TestLogger::default());
    let ops = Arc::new(TestOpsBus::default());
    let metrics = Arc::new(TestMetrics);

    let services = build_core_services(
        Arc::clone(&runtime) as Arc<dyn AsyncRuntime>,
        Arc::clone(&logger) as Arc<dyn Logger>,
        Arc::clone(&ops) as Arc<dyn OpsEventBus>,
        Arc::clone(&metrics) as Arc<dyn MetricsProvider>,
    );

    let controller = Arc::new(NoopController);
    let channel = Arc::new(TestChannel::new("failure", controller));
    let recorder = channel.recorder();
    recorder.keep_closed_pending();

    let mut coordinator = GracefulShutdownCoordinator::new(services);
    coordinator.register_channel("failure", channel.clone() as Arc<dyn Channel>);

    let mut driver = ShutdownDriver::new(
        coordinator,
        CloseReason::new("spark.tck.shutdown.failure", "closed future failed"),
        None,
    );

    assert!(matches!(driver.poll(), Poll::Pending));
    recorder.complete_closed_with_error(SparkError::new(
        "spark.tck.shutdown.failure",
        "closed future failed",
    ));

    let report = driver.poll_to_ready();

    assert_eq!(report.results().len(), 1);
    match report.results()[0].status() {
        GracefulShutdownStatus::Failed(err) => {
            assert_eq!(err.code(), "spark.tck.shutdown.failure");
            assert_eq!(err.message(), "closed future failed");
        }
        other => panic!("预期 Failed，实际为 {:?}", other),
    }

    assert_eq!(
        recorder.force_calls.load(Ordering::SeqCst),
        0,
        "失败场景不应触发硬关闭"
    );
    assert_eq!(
        recorder.closed_failure_count(),
        1,
        "应记录一次 closed() 错误"
    );
    assert_eq!(
        recorder.closed_completions.load(Ordering::SeqCst),
        0,
        "错误场景不应统计成功关闭"
    );
    assert!(
        recorder.closed_drop_count() >= 1,
        "错误返回后 Future 应被释放"
    );
}

/// 以确定性轮询方式驱动 `GracefulShutdownCoordinator::shutdown` 的辅助结构。
///
/// # 教案式说明
/// - **意图 (Why)**：测试需要在 FIN、半关闭与定时器之间插入断言，传统 `block_on` 无法在 Pending 状态下让出控制权。
/// - **逻辑 (How)**：内部保存 `ManualFuture`，每次 `poll` 使用教学版 waker 轮询；`poll_to_ready` 则循环调用直至返回
///   `Ready`，保持与真实执行器一致的语义。
/// - **契约 (What)**：
///   - 输入：`GracefulShutdownCoordinator`、停机原因与可选 `Deadline`；
///   - 前置：调用方需在 Pending 时主动推进通道状态或计时器；
///   - 后置：一旦 `poll_to_ready` 返回，协调器保证已生成最终报告。
struct ShutdownDriver {
    future: ManualFuture<spark_core::host::GracefulShutdownReport>,
}

impl ShutdownDriver {
    fn new(
        coordinator: GracefulShutdownCoordinator,
        reason: CloseReason,
        deadline: Option<Deadline>,
    ) -> Self {
        Self {
            future: ManualFuture::new(coordinator.shutdown(reason, deadline)),
        }
    }

    fn poll(&mut self) -> Poll<spark_core::host::GracefulShutdownReport> {
        self.future.poll_once()
    }

    fn poll_to_ready(self) -> spark_core::host::GracefulShutdownReport {
        self.future.poll_to_ready()
    }
}

/// 手写 Future 执行器，支持在测试中显式推进状态。
///
/// # 教案式说明
/// - **意图 (Why)**：提供比 `block_on` 更细粒度的控制，便于在 Pending 状态插入断言。
/// - **逻辑 (How)**：持有 `Pin<Box<dyn Future>>`，每次构造 no-op waker 并调用 `poll`；`poll_to_ready` 循环直至获得结果。
/// - **契约 (What)**：
///   - 输入：`Send + 'static` 的 Future；
///   - 前置：Future 需要在有限次轮询内完成；
///   - 后置：一旦返回 `Ready`，内部 Future 即被消费，不可再次调用。
struct ManualFuture<T> {
    future: Pin<Box<dyn Future<Output = T> + Send>>,
}

impl<T> ManualFuture<T> {
    fn new<F>(future: F) -> Self
    where
        F: Future<Output = T> + Send + 'static,
    {
        Self {
            future: Box::pin(future),
        }
    }

    fn poll_once(&mut self) -> Poll<T> {
        let waker = unsafe { Waker::from_raw(noop_raw_waker()) };
        let mut cx = Context::from_waker(&waker);
        self.future.as_mut().poll(&mut cx)
    }

    fn poll_to_ready(self) -> T {
        let mut future = self;
        loop {
            match future.poll_once() {
                Poll::Ready(output) => break output,
                Poll::Pending => continue,
            }
        }
    }
}

/// 基于 `RawWaker` 的 no-op waker 构造函数。
const fn noop_raw_waker() -> RawWaker {
    fn clone(_: *const ()) -> RawWaker {
        noop_raw_waker()
    }
    fn wake(_: *const ()) {}
    fn wake_by_ref(_: *const ()) {}
    fn drop(_: *const ()) {}
    const VTABLE: RawWakerVTable = RawWakerVTable::new(clone, wake, wake_by_ref, drop);
    RawWaker::new(std::ptr::null(), &VTABLE)
}

/// 可控计时器，实现确定性的 `sleep` 语义。
///
/// # 教案式说明
/// - **意图 (Why)**：真实计时器依赖系统时间，难以在测试中稳定复现；`MockTimer` 允许手动触发，配合 `ShutdownDriver`
///   可以精确重现 FIN/超时路径。
/// - **逻辑 (How)**：维护当前单调时间、待触发休眠队列与触发日志；`expect_sleep` 返回 `SleepHandle`，由测试主动调用
///   `fire` 来推进时间并唤醒等待的 Future。
/// - **契约 (What)**：
///   - 输入：`sleep(duration)` 调用由协调器触发；
///   - 前置：测试在 Pending 状态下调用 `expect_sleep`；
///   - 后置：`fire` 会累加单调时间、记录日志并唤醒休眠 Future。
#[derive(Default)]
struct MockTimer {
    now: Mutex<Duration>,
    pending: Mutex<VecDeque<Weak<SleepState>>>,
    fired_log: Mutex<Vec<Duration>>,
}

impl MockTimer {
    fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    fn timer_now(&self) -> Duration {
        *self.now.lock()
    }

    fn record_fire(&self, duration: Duration) {
        self.fired_log.lock().push(duration);
        let mut guard = self.now.lock();
        *guard = guard.checked_add(duration).expect("测试时钟不应发生溢出");
    }

    fn expect_sleep(self: &Arc<Self>, context: &str) -> SleepHandle {
        self.try_pop_sleep()
            .unwrap_or_else(|| panic!("{}: 未观察到 sleep 请求", context))
    }

    fn try_pop_sleep(self: &Arc<Self>) -> Option<SleepHandle> {
        let mut guard = self.pending.lock();
        while let Some(candidate) = guard.pop_front() {
            if let Some(state) = candidate.upgrade().filter(|state| state.consume()) {
                return Some(SleepHandle {
                    timer: Arc::clone(self),
                    state,
                });
            }
        }
        None
    }

    fn fired_durations(&self) -> Vec<Duration> {
        self.fired_log.lock().clone()
    }

    fn register_sleep(&self, state: &Arc<SleepState>) {
        self.pending.lock().push_back(Arc::downgrade(state));
    }

    async fn sleep(self: Arc<Self>, duration: Duration) {
        SleepFuture::new(self, duration).await;
    }
}

/// 手动触发的休眠句柄。
///
/// # 教案式说明
/// - **意图 (Why)**：让测试代码能够截获协调器申请的定时器，并在合适时机推进。
/// - **逻辑 (How)**：保存 `MockTimer` 与 `SleepState` 的 `Arc`，`fire` 会标记状态、写入触发日志并唤醒 Future。
/// - **契约 (What)**：
///   - 输入：`MockTimer::expect_sleep` 生成的句柄；
///   - 前置：在 Future Pending 阶段调用；
///   - 后置：`fire` 只会生效一次，多次调用被忽略。
struct SleepHandle {
    timer: Arc<MockTimer>,
    state: Arc<SleepState>,
}

impl SleepHandle {
    fn duration(&self) -> Duration {
        self.state.duration
    }

    fn fire(&self) {
        if self.state.fire() {
            self.timer.record_fire(self.state.duration);
        }
    }
}

/// `MockTimer` 内部的休眠 Future。
struct SleepFuture {
    state: Arc<SleepState>,
}

impl SleepFuture {
    fn new(timer: Arc<MockTimer>, duration: Duration) -> Self {
        let state = Arc::new(SleepState::new(duration));
        timer.register_sleep(&state);
        Self { state }
    }
}

impl Future for SleepFuture {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.state.is_fired() {
            return Poll::Ready(());
        }

        self.state.register(cx.waker());
        if self.state.is_fired() {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }
}

impl Drop for SleepFuture {
    fn drop(&mut self) {
        self.state.cancel();
    }
}

/// 休眠状态机，跟踪是否已消费或被取消。
struct SleepState {
    duration: Duration,
    fired: AtomicBool,
    consumed: AtomicBool,
    cancelled: AtomicBool,
    waker: Mutex<Option<Waker>>,
}

impl SleepState {
    fn new(duration: Duration) -> Self {
        Self {
            duration,
            fired: AtomicBool::new(false),
            consumed: AtomicBool::new(false),
            cancelled: AtomicBool::new(false),
            waker: Mutex::new(None),
        }
    }

    fn register(&self, waker: &Waker) {
        *self.waker.lock() = Some(waker.clone());
    }

    fn consume(&self) -> bool {
        !self.consumed.swap(true, Ordering::AcqRel)
    }

    fn fire(&self) -> bool {
        if self
            .fired
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Relaxed)
            .is_ok()
        {
            if let Some(waker) = self.waker.lock().take() {
                waker.wake();
            }
            true
        } else {
            false
        }
    }

    fn is_fired(&self) -> bool {
        self.fired.load(Ordering::Acquire)
    }

    fn cancel(&self) {
        if self
            .cancelled
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Relaxed)
            .is_ok()
        {
            self.waker.lock().take();
        }
    }
}

/// 运行时桩：记录 `sleep` 调用并提供同步完成的任务句柄。
///
/// # 教案式说明
/// - **意图 (Why)**：为 `GracefulShutdownCoordinator` 提供最小运行时依赖，既能捕获 `sleep` 行为，又能同步执行任务。
/// - **逻辑 (How)**：内部持有 `MockTimer`，`TaskExecutor::spawn_dyn` 直接使用教学 `block_on` 执行任务，`TimeDriver` 接口
///   则委托给计时器桩。
/// - **契约 (What)**：
///   - 输入：与真实运行时一致的 `sleep`/`spawn` 调用；
///   - 前置：测试通过 `timer()` 方法检索计时器；
///   - 后置：`fired_durations()` 返回所有被触发的休眠时长。
#[derive(Default)]
struct TestRuntime {
    timer: Arc<MockTimer>,
}

impl TestRuntime {
    fn new() -> Self {
        Self {
            timer: MockTimer::new(),
        }
    }

    fn timer(&self) -> Arc<MockTimer> {
        Arc::clone(&self.timer)
    }

    fn fired_durations(&self) -> Vec<Duration> {
        self.timer.fired_durations()
    }
}

impl TaskExecutor for TestRuntime {
    fn spawn_dyn(
        &self,
        _ctx: &spark_core::contract::CallContext,
        fut: spark_core::future::BoxFuture<'static, TaskResult<Box<dyn Any + Send>>>,
    ) -> JoinHandle<Box<dyn Any + Send>> {
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| block_on(fut)));
        let task_result = match result {
            Ok(value) => value,
            Err(_) => Err(TaskError::Panicked),
        };
        JoinHandle::from_task_handle(Box::new(ReadyHandle::new(task_result)))
    }
}

#[spark_core::async_trait]
impl TimeDriver for TestRuntime {
    fn now(&self) -> MonotonicTimePoint {
        MonotonicTimePoint::from_offset(self.timer.timer_now())
    }

    async fn sleep(&self, duration: Duration) {
        Arc::clone(&self.timer).sleep(duration).await;
    }
}

/// 即时完成的任务句柄，实现 `TaskHandle` 契约。
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

#[spark_core::async_trait]
impl<T: Send + 'static> TaskHandle for ReadyHandle<T> {
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
        self.result
            .lock()
            .take()
            .expect("ReadyHandle::join called multiple times")
    }
}

/// 教学版 `block_on`，避免引入额外依赖同时展示 Future 轮询机制。
///
/// # 教案式说明
/// - **意图 (Why)**：在无 Tokio 依赖的前提下运行异步任务，帮助读者理解 waker 与轮询语义。
/// - **逻辑 (How)**：构造空操作 waker，使用 `Pin::new_unchecked` 固定 Future，并在循环中持续 `poll` 直至返回 `Ready`。
/// - **契约 (What)**：
///   - 输入：任意 `Future`；
///   - 前置：Future 不得依赖真实唤醒；
///   - 后置：函数返回 Future 结果。
fn block_on<F: Future>(mut future: F) -> F::Output {
    fn noop_raw_waker() -> RawWaker {
        fn clone(_: *const ()) -> RawWaker {
            noop_raw_waker()
        }
        fn wake(_: *const ()) {}
        fn wake_by_ref(_: *const ()) {}
        fn drop(_: *const ()) {}
        const VTABLE: RawWakerVTable = RawWakerVTable::new(clone, wake, wake_by_ref, drop);
        RawWaker::new(std::ptr::null(), &VTABLE)
    }

    let waker = unsafe { Waker::from_raw(noop_raw_waker()) };
    let mut cx = Context::from_waker(&waker);
    let mut future = unsafe { Pin::new_unchecked(&mut future) };
    loop {
        match future.as_mut().poll(&mut cx) {
            Poll::Ready(output) => break output,
            Poll::Pending => continue,
        }
    }
}

/// 聚合运行时依赖，构造协调器所需的 `CoreServices`。
///
/// # 教案式说明
/// - **意图 (Why)**：测试桩需要快速获得与生产环境一致的 `CoreServices` 组合，
///   以验证 Facade 迁移后仍能提供日志、指标与运维事件等依赖。
/// - **逻辑 (How)**：
///   1. 使用 [`CoreServices::with_observability_facade`] 统一填充运行时与缓冲池；
///   2. 借由 [`DefaultObservabilityFacade`] 将四个观测句柄打包为 Facade；
///   3. 复用空的 `HealthChecks`，强调本套测试不关注健康探针。
/// - **契约 (What)**：
///   - 输入：具备线程安全约束的运行时、日志、运维事件与指标句柄；
///   - 前置：所有句柄需实现对应 Trait 并在测试生命周期内有效；
///   - 后置：返回的 `CoreServices` 能被协调器复用，并允许通过 Facade 继续派生旧句柄。
/// - **权衡 (Trade-offs)**：函数固定使用空缓冲池与默认健康探针，意味着若测试需要模拟
///   缓冲枯竭或探针状态，需要在调用后自行替换相关字段。
fn build_core_services(
    runtime: Arc<dyn AsyncRuntime>,
    logger: Arc<dyn Logger>,
    ops: Arc<dyn OpsEventBus>,
    metrics: Arc<dyn MetricsProvider>,
) -> CoreServices {
    CoreServices::with_observability_facade(
        runtime,
        Arc::new(NoopBufferPool),
        DefaultObservabilityFacade::new(logger, metrics, ops, Arc::new(Vec::new())),
    )
}

/// 空缓冲池桩实现。
#[derive(Default)]
struct NoopBufferPool;

impl spark_core::buffer::BufferPool for NoopBufferPool {
    fn acquire(
        &self,
        _: usize,
    ) -> spark_core::Result<Box<dyn spark_core::buffer::WritableBuffer>, spark_core::error::CoreError>
    {
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

/// 记录日志的轻量实现。
#[derive(Default)]
struct TestLogger {
    records: Mutex<Vec<OwnedLog>>,
}

#[derive(Clone)]
struct OwnedLog {
    severity: LogSeverity,
    message: String,
}

impl Logger for TestLogger {
    fn log(&self, record: &LogRecord<'_>) {
        let message = record.message.clone().into_owned();
        let entry = OwnedLog {
            severity: record.severity,
            message,
        };
        self.records.lock().push(entry);
    }
}

/// 记录运维事件的桩实现。
#[derive(Default)]
struct TestOpsBus {
    events: Mutex<Vec<OpsEvent>>,
    policies: Mutex<Vec<(OpsEventKind, EventPolicy)>>,
}

impl OpsEventBus for TestOpsBus {
    fn broadcast(&self, event: OpsEvent) {
        self.events.lock().push(event);
    }

    fn subscribe(&self) -> BoxStream<'static, OpsEvent> {
        Box::pin(EmptyStream)
    }

    fn set_event_policy(&self, kind: OpsEventKind, policy: EventPolicy) {
        self.policies.lock().push((kind, policy));
    }
}

/// 空事件流，占位满足 `subscribe` 接口。
struct EmptyStream;

impl Stream for EmptyStream {
    type Item = OpsEvent;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Ready(None)
    }
}

/// 空指标实现，避免额外依赖。
struct TestMetrics;

impl MetricsProvider for TestMetrics {
    fn counter(
        &self,
        _descriptor: &spark_core::observability::InstrumentDescriptor<'_>,
    ) -> Arc<dyn Counter> {
        Arc::new(NoopCounter)
    }

    fn gauge(
        &self,
        _descriptor: &spark_core::observability::InstrumentDescriptor<'_>,
    ) -> Arc<dyn Gauge> {
        Arc::new(NoopGauge)
    }

    fn histogram(
        &self,
        _descriptor: &spark_core::observability::InstrumentDescriptor<'_>,
    ) -> Arc<dyn Histogram> {
        Arc::new(NoopHistogram)
    }
}

/// 通道操作记录器，统计 FIN/强制调用与 `closed()` 状态。
///
/// # 教案式说明
/// - **意图 (Why)**：集中存储通道桩的调用次数、错误与 Future 状态，便于测试断言。
/// - **逻辑 (How)**：使用原子计数与互斥锁记录 FIN 原因、截止时间、`closed()` 结果及释放次数。
/// - **契约 (What)**：`take_closed_result` 只允许在 `closed()` Future 完成时调用，确保成功/失败计数互斥。
struct ChannelRecorder {
    graceful_calls: AtomicUsize,
    force_calls: AtomicUsize,
    graceful_reasons: Mutex<Vec<CloseReason>>,
    graceful_deadlines: Mutex<Vec<Option<MonotonicTimePoint>>>,
    closed_state: Arc<ManualClosedState>,
    closed_completions: AtomicUsize,
    keep_pending: AtomicBool,
    closed_failures: AtomicUsize,
    closed_drops: AtomicUsize,
    failure: Mutex<Option<SparkError>>,
    result_recorded: AtomicBool,
}

impl ChannelRecorder {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            graceful_calls: AtomicUsize::new(0),
            force_calls: AtomicUsize::new(0),
            graceful_reasons: Mutex::new(Vec::new()),
            graceful_deadlines: Mutex::new(Vec::new()),
            closed_state: Arc::new(ManualClosedState::new()),
            closed_completions: AtomicUsize::new(0),
            keep_pending: AtomicBool::new(false),
            closed_failures: AtomicUsize::new(0),
            closed_drops: AtomicUsize::new(0),
            failure: Mutex::new(None),
            result_recorded: AtomicBool::new(false),
        })
    }

    fn complete_closed(&self) {
        self.closed_state.complete();
    }

    fn keep_closed_pending(&self) {
        self.keep_pending.store(true, Ordering::SeqCst);
    }

    fn require_closed_steps(&self, steps: usize) {
        self.keep_pending.store(true, Ordering::SeqCst);
        self.closed_state.set_pending_steps(steps);
    }

    fn ack_closed_step(&self) {
        self.closed_state.ack_step();
    }

    fn complete_closed_with_error(&self, error: SparkError) {
        *self.failure.lock() = Some(error);
        self.closed_state.complete();
    }

    fn closed_failure_count(&self) -> usize {
        self.closed_failures.load(Ordering::SeqCst)
    }

    fn closed_drop_count(&self) -> usize {
        self.closed_drops.load(Ordering::SeqCst)
    }

    fn reset_result_recording(&self) {
        self.result_recorded.store(false, Ordering::SeqCst);
    }

    #[allow(clippy::result_large_err)]
    fn take_closed_result(&self) -> spark_core::Result<(), SparkError> {
        if self.result_recorded.swap(true, Ordering::SeqCst) {
            return Ok(());
        }

        if let Some(error) = self.failure.lock().take() {
            self.closed_failures.fetch_add(1, Ordering::SeqCst);
            Err(error)
        } else {
            self.closed_completions.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }
}

/// 手动控制的 `closed()` Future 状态。
///
/// # 教案式说明
/// - **意图 (Why)**：帮助测试在不同阶段控制半关闭确认，模拟读写方向的独立完成顺序。
/// - **逻辑 (How)**：通过原子计数记录剩余步骤，保存 waker 以便在最后一步唤醒等待的 Future。
/// - **契约 (What)**：调用方需在设置 Pending 步骤后按序调用 `ack_step`，`complete` 会确保 waker 在首次完成时被唤醒。
struct ManualClosedState {
    completed: AtomicBool,
    waker: Mutex<Option<Waker>>,
    pending_steps: AtomicUsize,
}

impl ManualClosedState {
    fn new() -> Self {
        Self {
            completed: AtomicBool::new(false),
            waker: Mutex::new(None),
            pending_steps: AtomicUsize::new(0),
        }
    }

    fn register(&self, waker: &Waker) {
        *self.waker.lock() = Some(waker.clone());
    }

    fn complete(&self) {
        if !self.completed.swap(true, Ordering::AcqRel) {
            self.pending_steps.store(0, Ordering::SeqCst);
            if let Some(waker) = self.waker.lock().take() {
                waker.wake();
            }
        }
    }

    fn set_pending_steps(&self, steps: usize) {
        self.pending_steps.store(steps, Ordering::SeqCst);
        self.completed.store(false, Ordering::Release);
    }

    fn ack_step(&self) {
        loop {
            let current = self.pending_steps.load(Ordering::Acquire);
            if current == 0 {
                break;
            }

            if self
                .pending_steps
                .compare_exchange(current, current - 1, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
            {
                if current == 1 {
                    self.complete();
                }
                break;
            }
        }
    }

    fn is_completed(&self) -> bool {
        self.completed.load(Ordering::Acquire)
    }
}

/// 与 `ManualClosedState` 配套的 Future，封装 `closed()` 的手动驱动逻辑。
///
/// # 教案式说明
/// - **意图 (Why)**：在不依赖真实通道的情况下重现 `closed()` Future 的完成与错误路径。
/// - **逻辑 (How)**：根据 `ChannelRecorder` 的配置决定是否立即完成，若保持 Pending 则登记 waker 等待状态改变。
/// - **契约 (What)**：每次 poll 都会尝试读取记录结果，确保错误/成功路径互斥；Drop 时统计资源释放次数。
struct ManualClosedFuture {
    state: Arc<ManualClosedState>,
    recorder: Arc<ChannelRecorder>,
}

impl Future for ManualClosedFuture {
    type Output = spark_core::Result<(), SparkError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.state.is_completed() {
            return Poll::Ready(self.recorder.take_closed_result());
        }

        if self.recorder.keep_pending.load(Ordering::Acquire) {
            self.state.register(cx.waker());
            if self.state.is_completed() {
                Poll::Ready(self.recorder.take_closed_result())
            } else {
                Poll::Pending
            }
        } else {
            self.state.complete();
            Poll::Ready(self.recorder.take_closed_result())
        }
    }
}

impl Drop for ManualClosedFuture {
    fn drop(&mut self) {
        self.recorder.closed_drops.fetch_add(1, Ordering::SeqCst);
    }
}

/// 通道桩，实现最小接口用于验证关闭语义。
///
/// # 教案式说明
/// - **意图 (Why)**：向协调器提供可控的通道行为，便于测试统计 FIN/Force 调用与 `closed()` 状态。
/// - **逻辑 (How)**：所有写入相关方法返回成功，`close_graceful`/`close` 仅更新计数器，`closed()` 返回与记录器绑定的
///   手动 Future。
/// - **契约 (What)**：调用 `recorder()` 可获取共享的统计器；通道 ID、可写状态与控制器保持静态值。
struct TestChannel {
    id: String,
    controller: Arc<NoopController>,
    extensions: NoopExtensions,
    recorder: Arc<ChannelRecorder>,
}

impl TestChannel {
    fn new(id: &str, controller: Arc<NoopController>) -> Self {
        Self {
            id: id.to_string(),
            controller,
            extensions: NoopExtensions,
            recorder: ChannelRecorder::new(),
        }
    }

    fn recorder(&self) -> Arc<ChannelRecorder> {
        Arc::clone(&self.recorder)
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

    fn close_graceful(&self, reason: CloseReason, deadline: Option<Deadline>) {
        self.recorder.graceful_calls.fetch_add(1, Ordering::SeqCst);
        self.recorder.graceful_reasons.lock().push(reason);
        self.recorder
            .graceful_deadlines
            .lock()
            .push(deadline.and_then(|d| d.instant()));
    }

    fn close(&self) {
        self.recorder.force_calls.fetch_add(1, Ordering::SeqCst);
    }

    fn closed(&self) -> spark_core::future::BoxFuture<'static, spark_core::Result<(), SparkError>> {
        self.recorder.reset_result_recording();
        Box::pin(ManualClosedFuture {
            state: Arc::clone(&self.recorder.closed_state),
            recorder: Arc::clone(&self.recorder),
        })
    }

    fn write(
        &self,
        _msg: spark_core::buffer::PipelineMessage,
    ) -> spark_core::Result<spark_router::pipeline::WriteSignal, spark_core::error::CoreError> {
        Ok(spark_router::pipeline::WriteSignal::Accepted)
    }

    fn flush(&self) {}
}

#[derive(Copy, Clone)]
struct NoopExtensions;

impl ExtensionsMap for NoopExtensions {
    fn insert(&self, _: std::any::TypeId, _: Box<dyn Any + Send + Sync>) {}
    fn get<'a>(&'a self, _: &std::any::TypeId) -> Option<&'a (dyn Any + Send + Sync + 'static)> {
        None
    }
    fn remove(&self, _: &std::any::TypeId) -> Option<Box<dyn Any + Send + Sync>> {
        None
    }
    fn contains_key(&self, _: &std::any::TypeId) -> bool {
        false
    }
    fn clear(&self) {}
}

/// 最小化控制器实现，所有方法均为 no-op。
#[derive(Default)]
struct NoopController;

impl Pipeline for NoopController {
    type HandleId = PipelineHandleId;

    fn register_inbound_handler(
        &self,
        _: &str,
        _: Box<dyn spark_router::pipeline::handler::InboundHandler>,
    ) {
    }

    fn register_outbound_handler(
        &self,
        _: &str,
        _: Box<dyn spark_router::pipeline::handler::OutboundHandler>,
    ) {
    }

    fn install_middleware(
        &self,
        _: &dyn spark_router::pipeline::PipelineInitializer,
        _: &CoreServices,
    ) -> spark_core::Result<(), spark_core::error::CoreError> {
        Ok(())
    }

    fn emit_channel_activated(&self) {}

    fn emit_read(&self, _: spark_core::buffer::PipelineMessage) {}

    fn emit_read_completed(&self) {}

    fn emit_writability_changed(&self, _: bool) {}

    fn emit_user_event(&self, _: spark_core::observability::CoreUserEvent) {}

    fn emit_exception(&self, _: spark_core::error::CoreError) {}

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
    fn snapshot(&self) -> Vec<HandlerRegistration> {
        Vec::new()
    }
}
