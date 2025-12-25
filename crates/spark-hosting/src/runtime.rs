#![allow(clippy::module_name_repetitions)]

#[cfg(all(test, not(feature = "std")))]
extern crate std;

use alloc::{boxed::Box, sync::Arc};
use core::{
    any::Any,
    sync::atomic::{AtomicUsize, Ordering},
};

use futures_util::{future::poll_fn, task::AtomicWaker};
use spark_core::{
    BoxFuture, async_trait,
    contract::CallContext,
    runtime::{JoinHandle, TaskExecutor, TaskResult, TimeDriver},
};

/// `TaskTracker` 提供对异步任务的引用计数能力，确保即便调用方使用
/// [`JoinHandle::detach`](spark_core::runtime::JoinHandle::detach) 放弃控制权，宿主仍可在优雅停机阶段等待后台任务自然退出。
///
/// # 教案级注释
/// - **意图 (Why)**：
///   - 解决“fire-and-forget” 任务在停机时无法统计数量的问题，避免后台任务被运行时粗暴中断；
///   - 为 `spark-hosting` 的默认运行时包装器提供 WaitGroup 语义，配合停机协调器统计剩余任务量。
/// - **体系位置 (Where)**：
///   - 位于宿主层的运行时工具集，通常通过 [`TrackedRuntime`] 间接使用；
///   - 作用范围覆盖所有经 `TrackedRuntime` 提交的任务，包括已分离的句柄。
/// - **设计与逻辑 (How)**：
///   - 内部以原子计数跟踪在途任务，使用 [`AtomicWaker`] 在计数归零时唤醒等待者；
///   - `track_future` 在任务包装层生成守卫，对应任务完成或 panic 时都会自动递减计数；
///   - `wait_for_idle` 基于 `poll_fn` 轮询计数，零时立即返回，避免额外分配。
/// - **契约 (What)**：
///   - **输入**：由运行时包装的 Future，必须满足 `Send + 'static`；
///   - **前置条件**：计数器初始值为 0；调用方无需手动维护加减；
///   - **后置条件**：当所有被跟踪的任务完成后，`wait_for_idle` 返回，计数重回 0。
/// - **风险与权衡 (Trade-offs & Gotchas)**：
///   - 计数在任务进入执行队列即加一，若底层执行器丢弃任务而不执行，计数不会自动回退；
///   - 使用原子唤醒而非 channel，避免在 `no_std + alloc` 环境引入额外依赖，但仅支持“至少一次”唤醒语义。
#[derive(Clone, Default)]
pub struct TaskTracker {
    inner: Arc<TaskTrackerInner>,
}

impl TaskTracker {
    /// 创建新的任务跟踪器。
    ///
    /// - **前置条件**：无；
    /// - **后置条件**：内部计数置零，唤醒器未注册任何等待者。
    pub fn new() -> Self {
        Self::default()
    }

    /// 将待执行的 Future 包装为可计数的形式。
    ///
    /// # 教案式说明
    /// - **意图 (Why)**：在任务提交入口透明地增加生命周期计数，兼容 `detach` 和 `join` 两种消费方式；
    /// - **逻辑 (How)**：
    ///   1. 在包装前先递增计数；
    ///   2. 生成守卫并在 `async` 块末尾自动 `drop`；
    ///   3. 返回原始 Future 的输出，不更改业务语义。
    /// - **契约 (What)**：
    ///   - **输入**：`future` 为任何返回 [`TaskResult`] 的 `Future`；
    ///   - **返回**：包装后的 [`BoxFuture`]，完成后计数必减一。
    /// - **风险提示**：若底层执行器在排队前 panic，计数递增已发生，将导致计数偏高；目前假定执行器遵守契约并实际调度任务。
    pub fn track_future<T, F>(&self, future: F) -> BoxFuture<'static, TaskResult<T>>
    where
        T: Send + 'static,
        F: core::future::Future<Output = TaskResult<T>> + Send + 'static,
    {
        let guard = TaskGuard::new(self.clone());
        Box::pin(async move {
            let _guard = guard;
            future.await
        })
    }

    /// 返回当前在途任务数量。
    pub fn in_flight(&self) -> usize {
        self.inner.counter.load(Ordering::Acquire)
    }

    /// 等待所有被跟踪任务完成，提供 WaitGroup 式的停机钩子。
    ///
    /// - **意图 (Why)**：在优雅停机流程中阻塞直至所有后台任务退出，即便它们已调用 `detach`；
    /// - **前置条件**：调用方应保证不会无限新增任务，否则等待将无法返回；
    /// - **后置条件**：当返回时计数必为 0，后续新增任务需重新调用本方法等待。
    pub fn wait_for_idle(&self) -> BoxFuture<'static, ()> {
        let tracker = self.clone();
        Box::pin(async move {
            poll_fn(move |cx| {
                if tracker.in_flight() == 0 {
                    return core::task::Poll::Ready(());
                }

                tracker.inner.waker.register(cx.waker());
                if tracker.in_flight() == 0 {
                    core::task::Poll::Ready(())
                } else {
                    core::task::Poll::Pending
                }
            })
            .await
        })
    }
}

struct TaskTrackerInner {
    counter: AtomicUsize,
    waker: AtomicWaker,
}

impl Default for TaskTrackerInner {
    fn default() -> Self {
        Self {
            counter: AtomicUsize::new(0),
            waker: AtomicWaker::new(),
        }
    }
}

struct TaskGuard {
    tracker: TaskTracker,
}

impl TaskGuard {
    fn new(tracker: TaskTracker) -> Self {
        tracker.inner.counter.fetch_add(1, Ordering::Release);
        Self { tracker }
    }
}

impl Drop for TaskGuard {
    fn drop(&mut self) {
        if self.tracker.inner.counter.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.tracker.inner.waker.wake();
        }
    }
}

/// `TrackedRuntime` 为任意实现了 [`AsyncRuntime`](spark_core::runtime::AsyncRuntime) 的运行时增加任务计数能力。
///
/// # 教案式注释
/// - **意图 (Why)**：
///   - 在保持底层执行模型不变的前提下，为宿主提供“可等待的 detach” 能力，满足优雅停机的任务清点需求；
///   - 通过装饰器模式将 WaitGroup 语义与具体执行器解耦，避免侵入现有 runtime 实现。
/// - **体系位置 (Where)**：
///   - 属于 `spark-hosting` 的运行时扩展层，可在装配 `CoreServices` 时包裹宿主提供的 `AsyncRuntime`；
///   - 提供 `task_tracker` 与 `wait_for_idle` 接口供关机协调器或监控组件调用。
/// - **执行逻辑 (How)**：
///   - `spawn_dyn` 在将任务委托给内部执行器前调用 [`TaskTracker::track_future`] 包装 Future；
///   - 所有任务（包括调用 `detach` 的句柄）完成后计数归零，等待协程被唤醒；
///   - 时间驱动能力完全透传，不影响计时语义。
/// - **契约 (What)**：
///   - **输入**：构造时接收一个实现 [`TaskExecutor`] 与 [`TimeDriver`] 的运行时实例；
///   - **输出**：实现相同契约的新运行时，并额外暴露 [`task_tracker`](Self::task_tracker)；
///   - **前置条件**：调用方需确保内部运行时遵守 `spawn_dyn` 的调度契约，不会静默丢弃任务；
///   - **后置条件**：通过 [`wait_for_idle`](Self::wait_for_idle) 可等待所有经由本包装提交的任务完成。
/// - **风险与权衡 (Trade-offs & Gotchas)**：
///   - 计数器无法区分任务类型，如需按业务维度细分需在外层构造多个包装器；
///   - 若内部运行时在任务开始前即发生 panic，计数可能与实际不符，需在上层监控中结合日志审计。
pub struct TrackedRuntime<R> {
    inner: R,
    tracker: TaskTracker,
}

impl<R> TrackedRuntime<R> {
    /// 使用给定运行时构造包装器。
    pub fn new(inner: R) -> Self {
        Self {
            inner,
            tracker: TaskTracker::new(),
        }
    }

    /// 提供对底层任务计数器的访问，用于在停机时查询剩余任务或等待清空。
    pub fn task_tracker(&self) -> TaskTracker {
        self.tracker.clone()
    }

    /// 等待所有经由该运行时提交的任务完成。
    pub async fn wait_for_idle(&self) {
        self.tracker.wait_for_idle().await;
    }
}

impl<R> TaskExecutor for TrackedRuntime<R>
where
    R: TaskExecutor,
{
    fn spawn_dyn(
        &self,
        ctx: &CallContext,
        fut: BoxFuture<'static, TaskResult<Box<dyn Any + Send>>>,
    ) -> JoinHandle<Box<dyn Any + Send>> {
        let tracked_future = self.tracker.track_future(fut);
        self.inner.spawn_dyn(ctx, tracked_future)
    }
}

#[async_trait]
impl<R> TimeDriver for TrackedRuntime<R>
where
    R: TimeDriver,
{
    fn now(&self) -> spark_core::MonotonicTimePoint {
        self.inner.now()
    }

    async fn sleep(&self, duration: core::time::Duration) {
        self.inner.sleep(duration).await;
    }
}

#[cfg(test)]
mod tests {
    use super::TrackedRuntime;
    use alloc::boxed::Box;
    use core::any::Any;
    use spark_core::{
        async_trait,
        contract::CallContext,
        runtime::{
            JoinHandle, TaskCancellationStrategy, TaskExecutor, TaskHandle, TaskResult, TimeDriver,
        },
    };
    use std::sync::Mutex;

    #[test]
    fn detached_tasks_still_decrement_tracker() {
        let runtime = TrackedRuntime::new(ImmediateRuntime);
        let ctx = CallContext::builder().build();

        let handle = runtime.spawn_dyn(
            &ctx,
            Box::pin(async { Ok::<_, spark_core::TaskError>(Box::new(()) as Box<dyn Any + Send>) }),
        );
        handle.detach();

        futures::executor::block_on(runtime.wait_for_idle());
        assert_eq!(runtime.task_tracker().in_flight(), 0);
    }

    #[derive(Default)]
    struct ImmediateRuntime;

    impl TaskExecutor for ImmediateRuntime {
        fn spawn_dyn(
            &self,
            _ctx: &CallContext,
            fut: spark_core::BoxFuture<'static, TaskResult<Box<dyn Any + Send>>>,
        ) -> JoinHandle<Box<dyn Any + Send>> {
            let output = futures::executor::block_on(fut);
            JoinHandle::from_task_handle(Box::new(ImmediateHandle {
                state: Mutex::new(Some(output)),
            }))
        }
    }

    #[async_trait]
    impl TimeDriver for ImmediateRuntime {
        fn now(&self) -> spark_core::MonotonicTimePoint {
            spark_core::MonotonicTimePoint::from_offset(core::time::Duration::ZERO)
        }

        async fn sleep(&self, _duration: core::time::Duration) {}
    }

    struct ImmediateHandle {
        state: Mutex<Option<TaskResult<Box<dyn Any + Send>>>>,
    }

    #[async_trait]
    impl TaskHandle for ImmediateHandle {
        type Output = Box<dyn Any + Send>;

        fn cancel(&self, _strategy: TaskCancellationStrategy) {}

        fn is_finished(&self) -> bool {
            self.state.lock().expect("poisoned").is_none()
        }

        fn is_cancelled(&self) -> bool {
            false
        }

        fn id(&self) -> Option<&str> {
            None
        }

        fn detach(self: Box<Self>) {}

        async fn join(self: Box<Self>) -> TaskResult<Self::Output> {
            self.state
                .lock()
                .expect("poisoned")
                .take()
                .expect("join called multiple times")
        }
    }
}
