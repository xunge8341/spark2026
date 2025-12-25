#![allow(unsafe_code)]
// SAFETY: 顺序服务协调器需在 `Future` poll 路径上执行细粒度的 Pin 操作，
// ## 意图（Why）
// - `GuardedFuture` 以零拷贝的方式包装业务 Future，保持过程宏展开后的性能；
// - Rust 当前尚无稳定的 `Pin` 安全 API 可直接获取 `&mut Fut`，因而需要在受控环境下使用 `unsafe`。
// ## 解析逻辑（How）
// 1. `GuardedFuture::poll` 手动解 `Pin<&mut Self>`，再将内部 Future 重新 Pin，以遵循 `Pin` 合约；
// 2. 所有 `unsafe` 块前都确保 `Self` 未被移动，且字段仅在本方法内借用，避免双可变引用。
// ## 契约（What）
// - `GuardedFuture` 只会通过标准的 `Future` 驱动流程调用 `poll`；
// - 调用方不得在 `poll` 过程中移动 `GuardedFuture`，这一约束由 `Pin` API 强制；
// - Drop 保证守卫最终释放，即使 Future 被提前丢弃。
// ## 风险与权衡（Trade-offs）
// - 我们接受局部 `unsafe` 以换取过程宏兼容性，若未来标准库提供安全 API，应优先替换；
// - 如需在不同执行器之间转移该 Future，必须重新评估 `Pin` 合约与守卫生命周期。
//! Service 快速原型工具箱：提供闭包转 Service 的胶水类型，以及过程宏可复用的就绪协调器。
//!
//! # 模块定位（Why）
//! - `SimpleServiceFn`：为轻量场景提供“闭包 -> Service”桥接，并在内部复用顺序执行协调器；
//! - `SequentialService` + `AsyncFnLogic`：组合生成顺序执行的泛型 Service，实现 `#[spark::service]` 的核心；
//! - `ServiceReadyCoordinator`：封装 `poll_ready`/`call` 状态机，供过程宏在不分配堆内存的前提下实现顺序执行模型；
//! - `ReadyFutureGuard`/`GuardedFuture`：保证异步调用在正常完成或提前丢弃时都能唤醒等待者，避免背压锁死。
//!
//! # 使用指引（How）
//! - 业务代码若只需自定义 `call`，可直接使用 `SimpleServiceFn` 包裹闭包；
//! - `#[spark::service]` 宏会自动引用本模块中的协调器，实现开箱即用的 `poll_ready` 合约；
//! - 若需扩展更复杂的并发策略，可在保持 `ServiceReadyCoordinator` 语义不变的前提下另行封装。

use alloc::sync::Arc;
use core::{
    future::Future,
    marker::PhantomData,
    pin::Pin,
    sync::atomic::{AtomicBool, Ordering},
    task::{Context as TaskContext, Poll, Waker},
};

use spin::Mutex;

use crate::{
    Error,
    context::Context,
    contract::CallContext,
    service::Service,
    status::{PollReady, ReadyCheck, ReadyState},
};

/// 将闭包直接适配为顺序执行的 [`Service`]，对过程宏与测试场景一视同仁。
///
/// # 设计动机（Why）
/// - `T21` 目标要求“宏展开零堆分配 + 正确的 Pending→Wake”，因此需要一个即插即用的桥接器；
/// - 将顺序协调器封装到本类型内部，业务只需提供闭包即可获得完整的 `Service` 契约实现；
/// - 便于在过程宏、单元测试乃至快速原型中复用同一执行模型，保证行为一致。
///
/// # 行为描述（How）
/// - `new`：构造持有 [`ServiceReadyCoordinator`] 与闭包逻辑的服务实例，构造过程只进行一次 `Arc` 分配；
/// - `poll_ready`：委托给协调器，确保在前一次调用尚未完成时返回 `Pending` 并登记 waker；
/// - `call`：在占用执行权后调用闭包，并返回由 [`GuardedFuture`] 保护的业务 Future，实现完成/取消时的唤醒。
///
/// # 契约说明（What）
/// - **输入**：闭包类型 `F` 接受 [`CallContext`] 与请求体，并返回实现 [`Future`] 的对象；
/// - **输出**：满足 [`Service`] 契约的顺序执行服务，响应类型与错误类型沿用闭包定义；
/// - **前置条件**：闭包及其返回的 Future 必须满足 `Send + Sync + 'static`，确保服务本身也可跨线程安全使用；
/// - **后置条件**：每次调用结束后必定唤醒等待的 waker，恢复 `Ready` 状态，从而避免背压锁死。
///
/// # 风险提示（Trade-offs & Gotchas）
/// - 内部通过 `Arc` 共享协调器状态，以支撑 Future 跨线程移动；该分配在构造期发生一次，调用路径保持零堆分配；
/// - 若闭包捕获非 `'static` 引用或依赖非 `Send` 资源，编译阶段会直接失败，需要调用方调整所有权模型。
pub struct SimpleServiceFn<F> {
    /// 顺序执行的核心协调器，负责在 `poll_ready` 与 `call` 之间传递占用状态。
    ///
    /// - **架构位置**：复用 [`ServiceReadyCoordinator`]，确保与过程宏展开保持一致的状态机行为；
    /// - **设计考量**：使用 `Arc` 封装内部状态，允许 Future 在业务线程间自由移动，同时维持零分配热路径。
    coordinator: ServiceReadyCoordinator,
    /// 业务逻辑闭包。
    ///
    /// - **职责**：生成实际的业务 Future，由 `GuardedFuture` 在外层提供占用释放语义；
    /// - **约束**：保持 `Send + Sync + 'static`，以便整体服务满足 [`Service`] 对实现者的并发要求。
    logic: F,
}

impl<F> SimpleServiceFn<F>
where
    F: Send + Sync + 'static,
{
    /// 创建顺序执行的闭包 Service。
    ///
    /// # 实现细节（How）
    /// - 内部组合 [`ServiceReadyCoordinator::new`] 与值语义闭包，避免额外装箱或动态分派；
    /// - `logic` 字段按值存储，确保 `Service` 可在 `Send + Sync` 语境下直接移动；
    /// - 构造完成后即可立即参与 `poll_ready`/`call` 循环，无需额外初始化步骤。
    pub fn new(logic: F) -> Self {
        Self {
            coordinator: ServiceReadyCoordinator::new(),
            logic,
        }
    }
}

impl<F, Request, Fut, Response, E> Service<Request> for SimpleServiceFn<F>
where
    F: FnMut(CallContext, Request) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = crate::Result<Response, E>> + Send + 'static,
    Response: Send + 'static,
    E: Error + Send + 'static,
{
    type Response = Response;
    type Error = E;
    type Future = GuardedFuture<Fut, Response, E>;

    fn poll_ready(
        &mut self,
        _ctx: &Context<'_>,
        cx: &mut TaskContext<'_>,
    ) -> PollReady<Self::Error> {
        self.coordinator.poll_ready(cx)
    }

    fn call(&mut self, ctx: CallContext, req: Request) -> Self::Future {
        let guard = self.coordinator.begin_call();
        let fut = (self.logic)(ctx, req);
        GuardedFuture::new(guard, fut)
    }
}

/// 描述一次 Service 调用的逻辑单元，使顺序执行器可以在类型层面表达 Future 的具体形态。
///
/// # 设计目标（Why）
/// - 将“如何生成业务 Future”与“如何管理就绪状态”解耦，便于过程宏与手写 Service 共用同一套执行器；
/// - 避免使用 `BoxFuture` 等分配型包装器，保持热路径零堆分配；
/// - 为未来扩展（如并行度控制）预留插拔点。
pub trait ServiceLogic<Request>: Send + Sync + 'static {
    /// 业务响应类型。
    type Response: Send + 'static;
    /// 业务错误类型，需实现 [`Error`]。
    type Error: Error + Send + 'static;
    /// 具体的 Future 类型，实现者可返回 `async fn` 编译生成的匿名 Future。
    type Future: Future<Output = crate::Result<Self::Response, Self::Error>> + Send + 'static;

    /// 触发一次业务调用。
    ///
    /// # 契约说明
    /// - **输入**：完整的 [`CallContext`] 与请求体；
    /// - **输出**：代表本次调用的 Future；
    /// - **前置条件**：调用者需确保在获得就绪信号后再调用此函数。
    fn invoke(&mut self, ctx: CallContext, req: Request) -> Self::Future;
}

/// 将闭包适配为 [`ServiceLogic`]，供过程宏与测试复用。
pub struct AsyncFnLogic<F>(pub F);

impl<Request, F, Fut, Response, E> ServiceLogic<Request> for AsyncFnLogic<F>
where
    F: FnMut(CallContext, Request) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = crate::Result<Response, E>> + Send + 'static,
    Response: Send + 'static,
    E: Error + Send + 'static,
{
    type Response = Response;
    type Error = E;
    type Future = Fut;

    fn invoke(&mut self, ctx: CallContext, req: Request) -> Self::Future {
        (self.0)(ctx, req)
    }
}

/// 顺序执行器：结合 [`ServiceReadyCoordinator`] 与业务逻辑，生成合规的 `Service` 实现。
///
/// # 设计动机（Why）
/// - 过程宏展开时仅需注入闭包，即可获得完整的顺序执行语义；
/// - 将背压处理集中在协调器中，避免各处重复实现 waker 记录与唤醒。
pub struct SequentialService<L> {
    coordinator: ServiceReadyCoordinator,
    logic: L,
}

impl<L> SequentialService<L> {
    /// 构造顺序执行 Service。
    pub fn new(logic: L) -> Self {
        Self {
            coordinator: ServiceReadyCoordinator::new(),
            logic,
        }
    }
}

impl<L, Request> Service<Request> for SequentialService<L>
where
    L: ServiceLogic<Request>,
{
    type Response = L::Response;
    type Error = L::Error;
    type Future = GuardedFuture<L::Future, L::Response, L::Error>;

    fn poll_ready(
        &mut self,
        _ctx: &Context<'_>,
        cx: &mut TaskContext<'_>,
    ) -> PollReady<Self::Error> {
        self.coordinator.poll_ready(cx)
    }

    fn call(&mut self, ctx: CallContext, req: Request) -> Self::Future {
        let guard = self.coordinator.begin_call();
        GuardedFuture::new(guard, self.logic.invoke(ctx, req))
    }
}

/// 将业务 Future 与 `ReadyFutureGuard` 绑定，在完成或 Drop 时自动恢复就绪状态。
///
/// # 设计动机（Why）
/// - 顺序执行模型要求任何请求结束后立刻唤醒等待的 waker，否则上游会永久卡在 `poll_ready`；
/// - 直接在业务 Future 中手动调用 `release` 容易遗漏错误分支，封装为 `GuardedFuture` 可集中处理。
///
/// # 行为描述（How）
/// - `new`：接收协调器守卫与业务 Future，并记录在内部；
/// - `poll`：转调内部 Future，一旦完成即调用 `ReadyFutureGuard::release`；
/// - `Drop`：若 Future 被提前丢弃，同样触发释放，保证状态不被悬挂。
///
/// # 契约说明（What）
/// - **输入**：实现 [`Future`] 的业务类型 `Fut`；
/// - **输出**：新的 Future，输出值与原始 `Fut` 保持一致；
/// - **前置条件**：创建者需保证守卫来自对应的协调器；
/// - **后置条件**：无论 `poll` 结果如何，协调器最终都会重新进入 `Ready` 状态。
#[doc(hidden)]
pub struct GuardedFuture<Fut, Response, Error> {
    guard: Option<ReadyFutureGuard>,
    future: Fut,
    _marker: PhantomData<fn() -> (Response, Error)>,
}

impl<Fut, Response, Error> GuardedFuture<Fut, Response, Error>
where
    Fut: Future<Output = crate::Result<Response, Error>>,
{
    fn new(guard: ReadyFutureGuard, future: Fut) -> Self {
        Self {
            guard: Some(guard),
            future,
            _marker: PhantomData,
        }
    }
}

impl<Fut, Response, Error> Future for GuardedFuture<Fut, Response, Error>
where
    Fut: Future<Output = crate::Result<Response, Error>>,
{
    type Output = crate::Result<Response, Error>;

    fn poll(self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<Self::Output> {
        // SAFETY: `self` 已被 Pin 固定，`GuardedFuture` 不会实现 `Unpin`，因此 `get_unchecked_mut`
        // 仅在当前栈帧内获取可变引用，不会造成移动；
        let this = unsafe { self.get_unchecked_mut() };
        // SAFETY: `this.future` 在整个 `GuardedFuture` 生命周期内保持稳定地址，且未同时被借用，
        // 手动创建 `Pin<&mut Fut>` 与 `Pin` 的不变式一致。
        let future = unsafe { Pin::new_unchecked(&mut this.future) };
        match future.poll(cx) {
            Poll::Ready(output) => {
                if let Some(mut guard) = this.guard.take() {
                    guard.release();
                }
                Poll::Ready(output)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<Fut, Response, Error> Drop for GuardedFuture<Fut, Response, Error> {
    fn drop(&mut self) {
        if let Some(mut guard) = self.guard.take() {
            guard.release();
        }
    }
}

/// `#[spark::service]` 宏复用的就绪协调器，实现顺序调用与 waker 管理。
///
/// # 设计意图（Why）
/// - 将 `poll_ready`/`call` 的状态迁移逻辑集中管理，避免宏展开重复生成易错代码；
/// - 通过 `Arc` + 原子位表示“是否可接收下一次请求”，避免堆分配和锁的额外开销；
/// - 提供明确的接口语义，便于未来扩展（如并行度计数、排队指标）。
///
/// # 行为细节（How）
/// - `poll_ready`：若可用直接返回 `Ready`，否则记录 waker 并返回 `Pending`；
/// - `begin_call`：原子地占用执行权，违反契约（未先 `poll_ready`）时直接 panic 提醒开发者；
/// - `release_once`：在调用完成或 Future 丢弃时唤醒等待者，恢复 `Ready` 状态。
///
/// # 契约说明（What）
/// - **前置条件**：`begin_call` 必须在最近一次 `poll_ready` 返回 Ready 之后调用；
/// - **后置条件**：`ReadyFutureGuard` 被释放后，协调器保证状态回到 Ready 并唤醒一个等待者；
/// - **边界**：同一时间仅允许一个请求进行中，体现过程宏的顺序执行模型。
#[doc(hidden)]
pub struct ServiceReadyCoordinator {
    inner: Arc<ServiceReadyState>,
}

impl ServiceReadyCoordinator {
    /// 创建新的协调器实例。
    ///
    /// # 实现要点（How）
    /// - 初始状态为 `Ready`，允许立即处理一次调用；
    /// - 使用 [`Arc`] 保证宏生成的 Future 在被丢弃时也能安全访问状态。
    pub fn new() -> Self {
        Self {
            inner: Arc::new(ServiceReadyState::new()),
        }
    }

    /// 在 `poll_ready` 中查询当前状态，并在必要时登记 waker。
    pub fn poll_ready<E>(&self, cx: &mut TaskContext<'_>) -> PollReady<E>
    where
        E: Error,
    {
        if self.inner.is_ready() {
            return Poll::Ready(ReadyCheck::Ready(ReadyState::Ready));
        }

        self.inner.register(cx.waker());

        if self.inner.is_ready() {
            Poll::Ready(ReadyCheck::Ready(ReadyState::Ready))
        } else {
            Poll::Pending
        }
    }

    /// 占用执行权并返回生命周期绑定的释放守卫。
    pub fn begin_call(&self) -> ReadyFutureGuard {
        if !self.inner.try_acquire() {
            panic!("ServiceReadyCoordinator::begin_call 必须在 poll_ready 返回 Ready 后调用");
        }
        ReadyFutureGuard::new(self.clone())
    }

    fn release_once(&self) {
        self.inner.release();
    }
}

impl Clone for ServiceReadyCoordinator {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl Default for ServiceReadyCoordinator {
    fn default() -> Self {
        Self::new()
    }
}

/// 在异步调用生命周期内保持占用，并在 Drop 时自动释放。
///
/// # 设计动机（Why）
/// - 保证无论 Future 正常完成、返回错误还是提前被取消，协调器状态都能回到 Ready；
/// - 将释放逻辑集中在 `Drop`，减少宏展开时对异常分支的重复代码。
///
/// # 行为说明（How）
/// - `release`：显式释放并阻止二次释放；
/// - `Drop`：若 Future 被提前丢弃，自动执行一次释放，确保 waker 被唤醒。
#[doc(hidden)]
pub struct ReadyFutureGuard {
    coordinator: ServiceReadyCoordinator,
    armed: bool,
}

impl ReadyFutureGuard {
    fn new(coordinator: ServiceReadyCoordinator) -> Self {
        Self {
            coordinator,
            armed: true,
        }
    }

    /// 显式释放执行权，允许下一次 `poll_ready` 返回就绪。
    pub fn release(&mut self) {
        if self.armed {
            self.coordinator.release_once();
            self.armed = false;
        }
    }
}

impl Drop for ReadyFutureGuard {
    fn drop(&mut self) {
        if self.armed {
            self.coordinator.release_once();
            self.armed = false;
        }
    }
}

struct ServiceReadyState {
    ready: AtomicBool,
    waker: Mutex<Option<Waker>>,
}

impl ServiceReadyState {
    const fn new() -> Self {
        Self {
            ready: AtomicBool::new(true),
            waker: Mutex::new(None),
        }
    }

    fn is_ready(&self) -> bool {
        self.ready.load(Ordering::Acquire)
    }

    fn try_acquire(&self) -> bool {
        self.ready
            .compare_exchange(true, false, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    fn release(&self) {
        self.ready.store(true, Ordering::Release);
        if let Some(waker) = self.waker.lock().take() {
            waker.wake();
        }
    }

    fn register(&self, waker: &Waker) {
        let mut slot = self.waker.lock();
        match slot.as_ref() {
            Some(existing) if existing.will_wake(waker) => {}
            _ => {
                *slot = Some(waker.clone());
            }
        }
    }
}
