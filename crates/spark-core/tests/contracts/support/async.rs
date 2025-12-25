use core::future::Future;
use core::pin::Pin;
use core::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};

/// 迷你版 `block_on`，以教学级注释阐释调度语义。
///
/// # 设计背景（Why）
/// - 合约测试需要在无需依赖 Tokio/async-std 的情况下轮询 Future，尤其是在运行时契约尚未绑定具体执行器时；
/// - 通过内联实现显式展示 waker 与 `Context` 的交互，帮助读者理解异步状态机的最小要求。
///
/// # 实现逻辑（How）
/// 1. 构造一个空操作的 [`RawWaker`]，所有唤醒操作均为 no-op，但满足编译期对 `VTABLE` 的要求；
/// 2. 以该 waker 创建 [`Context`]，随后将传入的 Future 通过 `Pin::new_unchecked` 固定在栈上；
/// 3. 在循环中调用 `Future::poll`，若返回 `Poll::Pending` 则继续轮询，直到 `Poll::Ready` 返回结果。
///
/// # 契约说明（What）
/// - **输入**：任意实现 [`Future`] 的对象；调用方需保证 Future 不依赖真实的唤醒通知，否则可能出现忙等。
/// - **返回值**：Future 的产出 `F::Output`；
/// - **前置条件**：`Future` 在 `Poll::Pending` 路径下不得存储指向临时数据的引用，否则手动固定将导致悬垂引用；
/// - **后置条件**：函数返回时 Future 已经完成，所有中间状态被丢弃。
///
/// # 风险提示（Trade-offs）
/// - 由于 waker 为 no-op，本实现适合作为教学与同步测试工具，不可用于真实异步执行环境；
/// - 若 Future 内部依赖外部唤醒（例如定时器、IO 驱动），将导致死循环；测试编写者需确保逻辑满足轮询完成的条件。
pub(crate) fn block_on<F: Future>(mut future: F) -> F::Output {
    fn noop_raw_waker() -> RawWaker {
        fn clone(_: *const ()) -> RawWaker {
            noop_raw_waker()
        }
        fn wake(_: *const ()) {}
        fn wake_by_ref(_: *const ()) {}
        fn drop(_: *const ()) {}
        const VTABLE: RawWakerVTable = RawWakerVTable::new(clone, wake, wake_by_ref, drop);
        RawWaker::new(core::ptr::null(), &VTABLE)
    }

    let waker = unsafe { Waker::from_raw(noop_raw_waker()) };
    let mut cx = Context::from_waker(&waker);
    // 安全性说明：Future 仅在本函数体内被固定，调用方不得在轮询期间持有其引用。
    let mut future = unsafe { Pin::new_unchecked(&mut future) };
    loop {
        match future.as_mut().poll(&mut cx) {
            Poll::Ready(result) => break result,
            Poll::Pending => continue,
        }
    }
}
