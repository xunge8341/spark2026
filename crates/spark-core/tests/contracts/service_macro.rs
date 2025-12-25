use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll, RawWaker, RawWakerVTable, Waker},
};

#[path = "../../../../tools/bench/alloc_check.rs"]
mod alloc_check;

use alloc_check::{AllocationScope, AllocationScopeGuard, CountingAllocator};
use spark::service::Service;
use spark_core as spark;

/// 在每个测试开始前创建独占作用域，并清零历史分配计数。
///
/// # 作用域说明（Why & How）
/// - 通过 [`AllocationScope::lock`] 串行化测试，防止多线程用例互相污染计数结果；
/// - 获取守卫后立即调用 [`AllocationScopeGuard::reset`]，圈定新的统计窗口；
/// - 返回的守卫在 Drop 时自动释放互斥锁，维持测试间的独立性。
fn allocation_scope() -> AllocationScopeGuard {
    let guard = AllocationScope::lock();
    guard.reset();
    guard
}

#[global_allocator]
static GLOBAL: CountingAllocator = CountingAllocator;

/// 可手动驱动的 Future，配合测试验证宏生成 Service 的就绪语义。
struct ManualFutureTask {
    state: Arc<ManualFutureState>,
}

struct ManualFutureState {
    ready: AtomicBool,
    waker: Mutex<Option<Waker>>,
}

impl ManualFutureTask {
    fn new() -> (Self, ManualFutureHandle) {
        let state = Arc::new(ManualFutureState {
            ready: AtomicBool::new(false),
            waker: Mutex::new(None),
        });
        (
            Self {
                state: Arc::clone(&state),
            },
            ManualFutureHandle { state },
        )
    }
}

impl Future for ManualFutureTask {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.state.ready.load(Ordering::Acquire) {
            Poll::Ready(())
        } else {
            let mut slot = self.state.waker.lock().expect("waker mutex poisoned");
            match slot.as_ref() {
                Some(existing) if existing.will_wake(cx.waker()) => {}
                _ => {
                    *slot = Some(cx.waker().clone());
                }
            }
            Poll::Pending
        }
    }
}

/// Future 对应的手动完成句柄。
struct ManualFutureHandle {
    state: Arc<ManualFutureState>,
}

impl ManualFutureHandle {
    fn trigger(&self) {
        self.state.ready.store(true, Ordering::Release);
        if let Some(waker) = self
            .state
            .waker
            .lock()
            .expect("waker mutex poisoned")
            .take()
        {
            waker.wake();
        }
    }
}

#[spark::service]
async fn wait_for_manual(
    _ctx: spark::CallContext,
    task: ManualFutureTask,
) -> spark_core::Result<(), spark::CoreError> {
    task.await;
    Ok(())
}

#[test]
fn poll_ready_pending_then_wake() {
    let _scope = allocation_scope();
    let mut service = wait_for_manual();
    let call_ctx = spark::CallContext::builder().build();
    let (task, handle) = ManualFutureTask::new();

    let initial_ready_waker = noop_waker();
    let mut initial_ready = Context::from_waker(&initial_ready_waker);
    assert!(matches!(
        service.poll_ready(&call_ctx.execution(), &mut initial_ready),
        Poll::Ready(spark::ReadyCheck::Ready(spark::ReadyState::Ready))
    ));

    let future_ctx_waker = noop_waker();
    let mut future_cx = Context::from_waker(&future_ctx_waker);
    let future = service.call(call_ctx.clone(), task);
    let mut future = std::pin::pin!(future);
    let future_waker = noop_waker();
    let mut future_cx_for_pending = Context::from_waker(&future_waker);
    assert!(
        future
            .as_mut()
            .poll(&mut future_cx_for_pending)
            .is_pending()
    );

    let wake_flag = Arc::new(AtomicBool::new(false));
    let waker = flag_waker(wake_flag.clone());
    let mut ready_cx = Context::from_waker(&waker);

    assert!(matches!(
        service.poll_ready(&call_ctx.execution(), &mut ready_cx),
        Poll::Pending
    ));

    assert!(!wake_flag.load(Ordering::SeqCst));

    handle.trigger();
    assert!(matches!(
        future.as_mut().poll(&mut future_cx),
        Poll::Ready(Ok(()))
    ));

    assert!(wake_flag.load(Ordering::SeqCst));

    let ready_after_waker = noop_waker();
    let mut ready_cx_after = Context::from_waker(&ready_after_waker);
    assert!(matches!(
        service.poll_ready(&call_ctx.execution(), &mut ready_cx_after),
        Poll::Ready(spark::ReadyCheck::Ready(spark::ReadyState::Ready))
    ));
}

#[test]
fn double_poll_no_ub() {
    let _scope = allocation_scope();
    let mut service = wait_for_manual();
    let call_ctx = spark::CallContext::builder().build();
    let (task, handle) = ManualFutureTask::new();

    let initial_ready_waker = noop_waker();
    let mut initial_ready = Context::from_waker(&initial_ready_waker);
    assert!(matches!(
        service.poll_ready(&call_ctx.execution(), &mut initial_ready),
        Poll::Ready(spark::ReadyCheck::Ready(spark::ReadyState::Ready))
    ));

    let future = service.call(call_ctx.clone(), task);
    let mut future = std::pin::pin!(future);
    let future_waker = noop_waker();
    let mut future_cx = Context::from_waker(&future_waker);
    assert!(future.as_mut().poll(&mut future_cx).is_pending());

    let first_flag = Arc::new(AtomicBool::new(false));
    let second_flag = Arc::new(AtomicBool::new(false));

    let first_waker = flag_waker(first_flag.clone());
    let mut first_cx = Context::from_waker(&first_waker);
    assert!(matches!(
        service.poll_ready(&call_ctx.execution(), &mut first_cx),
        Poll::Pending
    ));

    let second_waker = flag_waker(second_flag.clone());
    let mut second_cx = Context::from_waker(&second_waker);
    assert!(matches!(
        service.poll_ready(&call_ctx.execution(), &mut second_cx),
        Poll::Pending
    ));

    handle.trigger();
    assert!(matches!(
        future.as_mut().poll(&mut future_cx),
        Poll::Ready(Ok(()))
    ));

    assert!(!first_flag.load(Ordering::SeqCst));
    assert!(second_flag.load(Ordering::SeqCst));
}

/// 验证宏展开后的顺序 Service 在完整调用生命周期内保持零堆分配。
///
/// # 流程说明（How）
/// - 通过 [`allocation_scope`] 获取独占守卫并调用 [`AllocationScopeGuard::reset`]
///   清理历史数据后，依次执行 `call`、初次 `poll`（Pending）与再次 `poll_ready`（Pending）；
/// - 手动唤醒业务 Future，驱动完成并确认 waker 被触发；
/// - 最终断言分配计数保持为 0，证明调用路径未触发堆分配。
///
/// # 成功准则（What）
/// - [`AllocationScopeGuard::snapshot`] 返回的总计为 0；
/// - Pending→Ready 过程中的唤醒语义与其他测试保持一致。
#[test]
fn alloc_eq_zero() {
    let scope = allocation_scope();
    let mut service = wait_for_manual();
    let call_ctx = spark::CallContext::builder().build();
    let call_ctx_clone = call_ctx.clone();
    let (task, handle) = ManualFutureTask::new();

    let initial_ready_waker = noop_waker();
    let mut initial_ready = Context::from_waker(&initial_ready_waker);
    assert!(matches!(
        service.poll_ready(&call_ctx.execution(), &mut initial_ready),
        Poll::Ready(spark::ReadyCheck::Ready(spark::ReadyState::Ready))
    ));

    let future_waker = noop_waker();
    let mut future_cx = Context::from_waker(&future_waker);

    let wake_flag = Arc::new(AtomicBool::new(false));
    let ready_waker = flag_waker(wake_flag.clone());
    let mut ready_cx = Context::from_waker(&ready_waker);

    scope.reset();

    let future = service.call(call_ctx_clone, task);
    let mut future = std::pin::pin!(future);
    assert!(future.as_mut().poll(&mut future_cx).is_pending());
    assert!(matches!(
        service.poll_ready(&call_ctx.execution(), &mut ready_cx),
        Poll::Pending
    ));

    handle.trigger();
    assert!(matches!(
        future.as_mut().poll(&mut future_cx),
        Poll::Ready(Ok(()))
    ));
    assert!(wake_flag.load(Ordering::SeqCst));

    let snapshot = scope.snapshot();
    assert_eq!(snapshot.total(), 0, "调用路径存在额外堆分配");
    assert_eq!(alloc_check::total_allocations(), snapshot.total());
}

/// 构造一个恒定静默的 [`Waker`]，用于在测试中占位而不触发唤醒副作用。
///
/// # 设计动机（Why）
/// - 契约测试需要频繁创建 `Context` 验证 `poll_ready` 行为，但大多数场景下不需要实际唤醒；
/// - 通过固定的静默 waker 避免重复实现占位逻辑，让关注点集中在 Service 状态机上。
///
/// # 行为说明（How）
/// - 采用空指针作为原始数据指针，所有回调均为空操作；
/// - `clone` 直接复用同一 `VTABLE`，保证多次调用之间开销最小。
///
/// # 契约定义（What）
/// - **返回值**：可安全传入 [`Context::from_waker`] 的静态 [`Waker`]；
/// - **前置条件**：调用方须确保 waker 的生命周期覆盖 `Context` 的使用范围；
/// - **后置条件**：不会产生唤醒或状态变化，适合验证“保持 Pending” 的路径。
fn noop_waker() -> Waker {
    unsafe fn clone_noop(_: *const ()) -> RawWaker {
        RawWaker::new(std::ptr::null(), &VTABLE)
    }
    unsafe fn wake_noop(_: *const ()) {}
    unsafe fn wake_by_ref_noop(_: *const ()) {}
    unsafe fn drop_noop(_: *const ()) {}

    static VTABLE: RawWakerVTable =
        RawWakerVTable::new(clone_noop, wake_noop, wake_by_ref_noop, drop_noop);
    unsafe { Waker::from_raw(RawWaker::new(std::ptr::null(), &VTABLE)) }
}

/// 构造会在唤醒时置位的 [`Waker`]，用于断言 `poll_ready` 的唤醒行为。
///
/// # 设计动机（Why）
/// - 顺序 Service 必须在调用完成后唤醒等待的 waker，测试通过布尔标志来检测这一行为；
/// - 采用 `Arc<AtomicBool>` 可在多次克隆 waker 时共享状态，并避免额外锁开销。
///
/// # 实现要点（How）
/// - `clone_flag`：从裸指针恢复 `Arc`，克隆一份并立即转回裸指针，保持原始引用计数；
/// - `wake_flag`：恢复 `Arc`、置位标志，并释放一次引用，模拟一次性唤醒；
/// - `wake_by_ref_flag`：与 `wake_flag` 类似，但通过克隆转回裸指针来维持引用计数；
/// - `drop_flag`：用于 `RawWakerVTable` 的清理阶段，确保引用被正确释放。
///
/// # 契约定义（What）
/// - **输入**：共享的 `Arc<AtomicBool>`，调用方可在外层读取其值；
/// - **返回值**：可安全传入 [`Context::from_waker`] 的 [`Waker`]；
/// - **前置条件**：`flag` 必须在整个 `Waker` 生命周期内保持有效；
/// - **后置条件**：任一唤醒路径都会将 `flag` 置为 `true`，供测试断言使用。
fn flag_waker(flag: Arc<AtomicBool>) -> Waker {
    unsafe fn clone_flag(data: *const ()) -> RawWaker {
        let arc = unsafe { Arc::<AtomicBool>::from_raw(data as *const AtomicBool) };
        let cloned = Arc::clone(&arc);
        let _ = Arc::into_raw(arc);
        let cloned_ptr = Arc::into_raw(cloned);
        RawWaker::new(cloned_ptr as *const (), &VTABLE)
    }

    unsafe fn wake_flag(data: *const ()) {
        let arc = unsafe { Arc::<AtomicBool>::from_raw(data as *const AtomicBool) };
        arc.store(true, Ordering::SeqCst);
        core::mem::drop(arc);
    }

    unsafe fn wake_by_ref_flag(data: *const ()) {
        let arc = unsafe { Arc::<AtomicBool>::from_raw(data as *const AtomicBool) };
        arc.store(true, Ordering::SeqCst);
        let cloned = Arc::clone(&arc);
        let _ = Arc::into_raw(cloned);
        core::mem::drop(arc);
    }

    unsafe fn drop_flag(data: *const ()) {
        let arc = unsafe { Arc::<AtomicBool>::from_raw(data as *const AtomicBool) };
        core::mem::drop(arc);
    }

    static VTABLE: RawWakerVTable =
        RawWakerVTable::new(clone_flag, wake_flag, wake_by_ref_flag, drop_flag);
    let ptr = Arc::into_raw(flag);
    unsafe { Waker::from_raw(RawWaker::new(ptr as *const (), &VTABLE)) }
}
