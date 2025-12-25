// 教案级说明：本模块仅在启用 `std` Feature 后编译。
//
// - **意图 (Why)**：在“纯契约”阶段仍需统一的时间抽象，以支撑稳定性治理、重试节律等
//   上层组件；因此保留 [`Clock`] trait，并提供可注入的系统/虚拟时钟模型。
// - **契约 (What)**：`SystemClock` 依旧作为宿主运行时的占位符而保持空实现；`MockClock`
//   则提供完全确定性的虚拟时间轴，允许测试在不依赖真实计时器的前提下驱动 Future。
// - **实现提示 (How)**：模块内部通过 `Arc<Inner>` 聚合状态，读写均以互斥锁保护，确保
//   在 `no_std + alloc` 兼容性前提下提供最小化但可重复的行为。

use alloc::boxed::Box;
use alloc::vec::Vec;
use core::marker::PhantomData;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};

/// `Sleep` 为时钟接口返回的统一延迟 Future 类型。
///
/// # 设计说明
/// - **意图 (Why)**：保持异步等待的统一形态，便于未来实现通过特化或装箱进行扩展；
/// - **契约 (What)**：返回 `Future<Output = ()>`，必须满足 `Send + 'static`；
/// - **注意 (Trade-offs)**：当前阶段不再提供默认实现，调用方需要自行注入具备实际行为
///   的实现类型。
pub type Sleep = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;

/// 抽象可注入的时钟，实现统一的“获取当前时间”与“等待指定时间”能力。
///
/// # 教案式注释
/// - **意图 (Why)**：通过 trait 限定统一契约，便于替换为宿主运行时的真实实现；
/// - **契约 (What)**：`now` 返回单调时间点，`sleep` 生成延迟 Future；
/// - **注意 (Trade-offs)**：本文件仅保留接口，不提供默认实现或回退逻辑。
pub trait Clock: Send + Sync + 'static {
    /// 返回当前的单调时间点。
    fn now(&self) -> Instant;

    /// 返回一个在指定持续时间后完成的睡眠 Future。
    fn sleep(&self, duration: Duration) -> Sleep;
}

/// `SystemClock` 为生产运行时预留的系统时间占位类型。
///
/// # 教案式注释
/// - **意图 (Why)**：保留类型名称，确保依赖方在迁移到纯契约阶段时无需改动签名；
/// - **契约 (What)**：类型实现 [`Clock`] trait，但所有方法都会在运行期触发 `unimplemented!()`；
/// - **注意 (Trade-offs)**：占位实现不会提供任何时间语义，必须由宿主运行时替换。
#[derive(Clone, Debug, Default)]
pub struct SystemClock {
    _marker: PhantomData<()>,
}

impl Clock for SystemClock {
    fn now(&self) -> Instant {
        let _ = self;
        unimplemented!("SystemClock::now 在纯契约阶段不可用；请由宿主运行时提供具体实现",)
    }

    fn sleep(&self, duration: Duration) -> Sleep {
        let _ = duration;
        unimplemented!("SystemClock::sleep 在纯契约阶段不可用；请由宿主运行时提供具体实现",)
    }
}

/// `MockClock` 提供基于手动推进的虚拟时钟，服务于单元与契约测试的确定性需求。
///
/// # 教案式注释
/// - **意图 (Why)**：测试需要在单线程、可控的时间轴下验证重试节律与延迟行为，`MockClock`
///   通过显式 `advance` 方法模拟时间流逝，避免真实计时器带来的非确定性；
/// - **契约 (What)**：
///   - `new`/`with_start` 构造起始时间与内部偏移；
///   - `advance` 推进虚拟时间并唤醒到期的 `sleep` Future；
///   - `elapsed`/`now` 返回自起点以来的偏移与绝对时间；
///   - `sleep` 创建受控的延迟 Future，直到时钟推进至截止点；
/// - **注意 (Trade-offs)**：实现依赖 `Mutex` 管理共享状态，虽牺牲并发度，但换取了语义清晰
///   与跨平台的可移植性；调试时可结合 `elapsed` 追踪推进步长。
#[derive(Clone, Debug)]
pub struct MockClock {
    inner: Arc<Inner>,
}

#[derive(Debug)]
struct Inner {
    state: Mutex<ClockState>,
}

#[derive(Debug)]
struct ClockState {
    origin: Instant,
    elapsed: Duration,
    sleepers: Vec<Arc<SleepShared>>,
}

impl Default for MockClock {
    fn default() -> Self {
        Self::new()
    }
}

impl MockClock {
    /// 创建起始时间为当前系统时间的虚拟时钟。
    ///
    /// ### 教案式说明
    /// - **意图 (Why)**：提供零样板构造函数，便于测试快速生成虚拟时钟；
    /// - **契约 (What)**：内部记录系统时间作为基准，同时将偏移量置零；
    /// - **风险 (Trade-offs)**：尽管起始点引用真实时间，但后续行为完全由 `advance` 控制。
    pub fn new() -> Self {
        Self::with_start(Instant::now())
    }

    /// 以指定起始时间构造虚拟时钟。
    ///
    /// ### 教案式说明
    /// - **意图 (Why)**：允许测试对齐特定的时间原点（例如重放记录的日志时间戳）；
    /// - **契约 (What)**：`origin` 被保存为基准，后续 `now` = `origin + elapsed`；
    /// - **注意 (How)**：初始偏移恒为零，调用者需通过 `advance` 推进虚拟时间。
    pub fn with_start(origin: Instant) -> Self {
        Self {
            inner: Arc::new(Inner {
                state: Mutex::new(ClockState {
                    origin,
                    elapsed: Duration::from_secs(0),
                    sleepers: Vec::new(),
                }),
            }),
        }
    }

    /// 手动推进虚拟时钟并唤醒所有到期的睡眠 Future。
    ///
    /// ### 教案式说明
    /// - **意图 (Why)**：测试通过显式推进控制唤醒顺序，从而验证节律是否符合预期；
    /// - **契约 (What)**：`delta` 必须为非负时长，函数会累加内部偏移并唤醒所有到期 waker；
    /// - **实现 (How)**：内部先收集到期 Future，释放锁后逐个触发，避免在锁内执行用户代码。
    pub fn advance(&self, delta: Duration) {
        let due = self.inner.advance(delta);
        for sleeper in due {
            sleeper.wake();
        }
    }

    /// 返回自起始时间以来的虚拟偏移。
    ///
    /// ### 教案式说明
    /// - **意图 (Why)**：调试或断言时需要读取当前推进到的虚拟时间；
    /// - **契约 (What)**：返回值等于累计的 `advance` 时间，单位与输入一致；
    /// - **注意 (Trade-offs)**：调用方需自行处理并发读取场景，本实现通过互斥锁保证可见性。
    pub fn elapsed(&self) -> Duration {
        self.inner.elapsed()
    }
}

impl Clock for MockClock {
    fn now(&self) -> Instant {
        self.inner.now()
    }

    fn sleep(&self, duration: Duration) -> Sleep {
        self.inner.sleep(duration)
    }
}

impl Inner {
    fn advance(&self, delta: Duration) -> Vec<Arc<SleepShared>> {
        let mut guard = self.state.lock().expect("mock clock state lock");
        guard.elapsed = guard.elapsed.saturating_add(delta);
        let current = guard.elapsed;
        let mut due = Vec::new();
        guard.sleepers.retain(|entry| {
            if entry.is_due(current) {
                entry.mark_completed();
                due.push(Arc::clone(entry));
                false
            } else {
                true
            }
        });
        due
    }

    fn elapsed(&self) -> Duration {
        let guard = self.state.lock().expect("mock clock state lock");
        guard.elapsed
    }

    fn now(&self) -> Instant {
        let guard = self.state.lock().expect("mock clock state lock");
        guard.origin + guard.elapsed
    }

    fn sleep(self: &Arc<Self>, duration: Duration) -> Sleep {
        let (shared, ready_immediately) = {
            let mut guard = self.state.lock().expect("mock clock state lock");
            let current = guard.elapsed;
            let deadline = current.saturating_add(duration);
            let shared = Arc::new(SleepShared::new(deadline));
            if shared.is_due(current) {
                shared.mark_completed();
                (shared, true)
            } else {
                guard.sleepers.push(Arc::clone(&shared));
                (shared, false)
            }
        };

        if ready_immediately {
            Box::pin(async {})
        } else {
            Box::pin(SleepFuture {
                inner: Arc::clone(self),
                shared,
            })
        }
    }
}

#[derive(Debug)]
struct SleepShared {
    deadline: Duration,
    completed: AtomicBool,
    waker: Mutex<Option<std::task::Waker>>,
}

impl SleepShared {
    fn new(deadline: Duration) -> Self {
        Self {
            deadline,
            completed: AtomicBool::new(false),
            waker: Mutex::new(None),
        }
    }

    fn is_due(&self, current: Duration) -> bool {
        current >= self.deadline
    }

    fn mark_completed(&self) {
        self.completed.store(true, Ordering::SeqCst);
    }

    fn is_completed(&self) -> bool {
        self.completed.load(Ordering::SeqCst)
    }

    fn register(&self, waker: &Waker) {
        if self.is_completed() {
            return;
        }
        let mut slot = self.waker.lock().expect("mock sleep waker lock");
        *slot = Some(waker.clone());
    }

    fn take_waker(&self) -> Option<Waker> {
        self.waker.lock().expect("mock sleep waker lock").take()
    }

    fn wake(&self) {
        if let Some(waker) = self.take_waker() {
            waker.wake();
        }
    }
}

struct SleepFuture {
    inner: Arc<Inner>,
    shared: Arc<SleepShared>,
}

impl Future for SleepFuture {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if self.shared.is_completed() || self.shared.is_due(self.inner.elapsed()) {
            self.shared.mark_completed();
            return Poll::Ready(());
        }

        self.shared.register(cx.waker());

        if self.shared.is_completed() || self.shared.is_due(self.inner.elapsed()) {
            self.shared.mark_completed();
            self.shared.take_waker();
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }
}
