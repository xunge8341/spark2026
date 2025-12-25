#![allow(clippy::needless_return)]
//! Miri 聚焦的并发原语测试套件。
//!
//! # 教案级导览
//!
//! - **Why**：本文件聚焦 Cancellation、Budget、Channel 三个跨模块共享的并发原语，
//!   通过最小可复现场景在 Miri 下执行，确保内存可见性与状态转换不会出现未定义行为。
//! - **How**：每个测试均构造两个线程（或更多）模拟真实竞争路径，配合 `Arc` 与原子类型
//!   重建核心代码路径，并在断言阶段校验状态不变量。
//! - **What**：测试涵盖取消标记传播、预算消耗与归还的平衡、通道优雅关闭与强制关闭的竞态；
//!   所有测试均为无副作用的单元场景，可在 CI 中快速运行。

use std::sync::{
    Arc, OnceLock,
    atomic::{AtomicU8, AtomicUsize, Ordering},
};
use std::thread;

use futures::executor::block_on;
use spark_core::buffer::PipelineMessage;
use spark_core::error::{CoreError, SparkError};
use spark_core::future::BoxFuture;
use spark_core::observability::CoreUserEvent;
use spark_core::pipeline::channel::{Channel, ChannelState, WriteSignal};
use spark_core::pipeline::controller::{
    Handler as PipelineHandler, HandlerRegistration, HandlerRegistry, Pipeline, PipelineHandleId,
};
use spark_core::pipeline::extensions::ExtensionsMap;
use spark_core::runtime::CoreServices;
use spark_core::{
    contract::{Cancellation, CloseReason, Deadline},
    types::{Budget, BudgetKind},
};

/// ## 测试一：取消原语跨线程可见性
///
/// - **意图 (Why)**：验证父线程发出的取消信号能被其他线程快速捕获，避免出现“永不退出”的轮询。
/// - **逻辑 (How)**：父线程派生子令牌交由工作线程持有，工作线程循环检查 `is_cancelled()`，
///   主线程调用 `cancel()` 后等待两个线程结束，并断言所有视角均观测到取消状态。
/// - **契约 (What)**：
///   - **前置条件**：无；测试创建默认 `Cancellation`。
///   - **后置条件**：父子令牌均报告 `is_cancelled() == true`，重复 `cancel()` 返回 `false`；
///   - **风险提示**：若 `Ordering` 错误，循环可能无法终止，本测试会卡住或 panic。
#[test]
fn cancellation_cross_thread_visibility() {
    let root = Cancellation::new();
    let worker_token = root.child();
    let observer_token = root.child();

    let worker = thread::spawn(move || {
        while !worker_token.is_cancelled() {
            thread::yield_now();
        }
    });

    let observer = thread::spawn(move || {
        while !observer_token.is_cancelled() {
            thread::yield_now();
        }
    });

    assert!(root.cancel(), "首次取消应返回 true");
    worker.join().expect("工作线程必须平稳退出");
    observer.join().expect("观察线程必须平稳退出并观测到取消");
    assert!(root.is_cancelled(), "主线程应观察到取消标记");
    assert!(
        !root.cancel(),
        "重复取消应返回 false，确保比较交换的幂等语义"
    );
}

/// ## 测试二：预算并发消耗/归还不变量
///
/// - **意图 (Why)**：预算原语常用于解码/流控配额，必须在并发抢占下维持“总消耗不超过上限”。
/// - **逻辑 (How)**：两个线程分别尝试消耗不同额度，若获得额度则立即归还；
///   线程执行完毕后断言剩余额度重新等于上限，验证 `try_consume` 与 `refund` 的互斥语义。
/// - **契约 (What)**：
///   - **前置条件**：预算上限为 4。
///   - **后置条件**：测试结束后 `remaining()` == `limit()` == 4；
///   - **风险提示**：若内部原子操作未正确排序，可能出现负值或归还失败，本测试将触发断言。
#[test]
fn budget_concurrent_consume_refund_invariant() {
    let budget = Arc::new(Budget::new(BudgetKind::Decode, 4));

    let fast = {
        let budget = Arc::clone(&budget);
        thread::spawn(move || {
            let decision = budget.try_consume(1);
            if decision.is_granted() {
                budget.refund(1);
            }
        })
    };

    let greedy = {
        let budget = Arc::clone(&budget);
        thread::spawn(move || {
            let decision = budget.try_consume(4);
            if decision.is_granted() {
                budget.refund(4);
            }
        })
    };

    fast.join().expect("快速路径线程不应 panic");
    greedy.join().expect("高消耗线程不应 panic");

    assert_eq!(
        budget.remaining(),
        budget.limit(),
        "并发执行后剩余额度必须回到上限"
    );
}

/// ## 测试三：通道关闭路径竞态收敛
///
/// - **意图 (Why)**：在实际运行时，优雅关闭与强制关闭可能由不同线程触发；
///   我们需要保证多线程同时发起关闭时状态机不会回退或重复计数。
/// - **逻辑 (How)**：构造 `TestChannel`，两个线程分别调用 `close_graceful` 与 `close`；
///   第三个线程再触发一次 `close` 验证幂等性。最终检查状态为 `Closed`，且优雅关闭计数恰好一次。
/// - **契约 (What)**：
///   - **前置条件**：通道初始状态为 `Active`。
///   - **后置条件**：`state()` 返回 `ChannelState::Closed`，`graceful_count()` == 1，`force_count()` == 1；
///   - **风险提示**：若 `compare_exchange` 顺序错误，可能导致状态回退到 `Draining`，本测试将捕获该异常。
#[test]
fn channel_close_sequences_eventually_closed() {
    let channel = Arc::new(TestChannel::new("miri-channel"));
    let controller = Arc::new(NullController::default());
    channel.bind_controller(controller as Arc<dyn Pipeline<HandleId = PipelineHandleId>>);

    let graceful = {
        let channel = Arc::clone(&channel);
        thread::spawn(move || {
            channel.close_graceful(CloseReason::new("maintenance.graceful", "planned"), None);
        })
    };

    let force_once = {
        let channel = Arc::clone(&channel);
        thread::spawn(move || {
            channel.close();
        })
    };

    let force_again = {
        let channel = Arc::clone(&channel);
        thread::spawn(move || {
            channel.close();
        })
    };

    graceful.join().expect("优雅关闭线程不应 panic");
    force_once.join().expect("第一次强制关闭线程不应 panic");
    force_again.join().expect("重复关闭线程不应 panic");

    // `closed()` 在真实实现中会等待状态变更，这里同步检查状态不变量即可。
    block_on(channel.closed()).expect("等待关闭不应失败");

    assert_eq!(
        channel.state(),
        ChannelState::Closed,
        "通道最终必须进入 Closed"
    );
    assert_eq!(channel.graceful_count(), 1, "优雅关闭计数应恰好一次");
    assert_eq!(
        channel.force_count(),
        1,
        "强制关闭计数应在第一次 close 时增加"
    );
}

/// 测试用 Channel，实现必要的接口以复现关闭状态机。
struct TestChannel {
    id: String,
    controller: OnceLock<Arc<dyn Pipeline<HandleId = PipelineHandleId>>>,
    extensions: TestExtensions,
    state: AtomicU8,
    graceful_invocations: AtomicUsize,
    force_invocations: AtomicUsize,
}

impl TestChannel {
    const STATE_ACTIVE: u8 = 0;
    const STATE_DRAINING: u8 = 1;
    const STATE_CLOSED: u8 = 2;

    fn new(id: &str) -> Self {
        Self {
            id: id.to_string(),
            controller: OnceLock::new(),
            extensions: TestExtensions,
            state: AtomicU8::new(Self::STATE_ACTIVE),
            graceful_invocations: AtomicUsize::new(0),
            force_invocations: AtomicUsize::new(0),
        }
    }

    fn bind_controller(&self, controller: Arc<dyn Pipeline<HandleId = PipelineHandleId>>) {
        let _ = self.controller.set(controller);
    }

    fn graceful_count(&self) -> usize {
        self.graceful_invocations.load(Ordering::Acquire)
    }

    fn force_count(&self) -> usize {
        self.force_invocations.load(Ordering::Acquire)
    }

    fn load_state(&self) -> u8 {
        self.state.load(Ordering::Acquire)
    }

    fn swap_state(&self, value: u8) -> u8 {
        self.state.swap(value, Ordering::AcqRel)
    }

    fn compare_exchange_state(&self, current: u8, new: u8) -> spark_core::Result<u8, u8> {
        self.state
            .compare_exchange(current, new, Ordering::AcqRel, Ordering::Acquire)
    }

    fn mark_graceful(&self) {
        let _ =
            self.graceful_invocations
                .compare_exchange(0, 1, Ordering::AcqRel, Ordering::Acquire);
    }

    fn bump_force(&self) {
        self.force_invocations.fetch_add(1, Ordering::AcqRel);
    }
}

impl Channel for TestChannel {
    fn id(&self) -> &str {
        &self.id
    }

    fn state(&self) -> ChannelState {
        match self.load_state() {
            Self::STATE_ACTIVE => ChannelState::Active,
            Self::STATE_DRAINING => ChannelState::Draining,
            _ => ChannelState::Closed,
        }
    }

    fn is_writable(&self) -> bool {
        self.load_state() != Self::STATE_CLOSED
    }

    fn controller(&self) -> &dyn Pipeline<HandleId = PipelineHandleId> {
        self.controller
            .get()
            .expect("controller must be bound")
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

    fn close_graceful(&self, _reason: CloseReason, _deadline: Option<Deadline>) {
        let mut current = self.load_state();
        loop {
            match current {
                Self::STATE_ACTIVE => {
                    match self.compare_exchange_state(Self::STATE_ACTIVE, Self::STATE_DRAINING) {
                        Ok(_) => {
                            self.mark_graceful();
                            return;
                        }
                        Err(next) => current = next,
                    }
                }
                Self::STATE_DRAINING | Self::STATE_CLOSED => {
                    self.mark_graceful();
                    return;
                }
                _ => {
                    current = Self::STATE_CLOSED;
                }
            }
        }
    }

    fn close(&self) {
        let previous = self.swap_state(Self::STATE_CLOSED);
        if previous != Self::STATE_CLOSED {
            self.bump_force();
        }
    }

    fn closed(&self) -> BoxFuture<'static, spark_core::Result<(), SparkError>> {
        Box::pin(async { Ok(()) })
    }

    fn write(&self, msg: PipelineMessage) -> spark_core::Result<WriteSignal, CoreError> {
        let _ = msg;
        Ok(WriteSignal::Accepted)
    }

    fn flush(&self) {}
}

impl Default for TestChannel {
    fn default() -> Self {
        Self::new("test-channel")
    }
}

impl TestChannel {
    fn state(&self) -> ChannelState {
        Channel::state(self)
    }
}

/// 简化版 ExtensionsMap，避免测试依赖真实的扩展存储。
#[derive(Clone, Copy)]
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

/// 提供最小实现的 HandlerRegistry，用于满足 `Pipeline` 接口要求。
#[derive(Default)]
struct NullRegistry;

impl HandlerRegistry for NullRegistry {
    fn snapshot(&self) -> Vec<HandlerRegistration> {
        Vec::new()
    }
}

/// No-op 控制器，实现 `Pipeline` Trait 以满足测试的通道依赖。
#[derive(Default)]
struct NullController {
    registry: NullRegistry,
}

impl Pipeline for NullController {
    type HandleId = PipelineHandleId;

    fn register_inbound_handler(
        &self,
        _label: &str,
        _handler: Box<dyn spark_core::pipeline::handler::InboundHandler>,
    ) {
    }

    fn register_outbound_handler(
        &self,
        _label: &str,
        _handler: Box<dyn spark_core::pipeline::handler::OutboundHandler>,
    ) {
    }

    fn install_middleware(
        &self,
        _middleware: &dyn spark_core::pipeline::initializer::PipelineInitializer,
        _services: &CoreServices,
    ) -> spark_core::Result<(), CoreError> {
        Ok(())
    }

    fn emit_channel_activated(&self) {}

    fn emit_read(&self, _msg: PipelineMessage) {}

    fn emit_read_completed(&self) {}

    fn emit_writability_changed(&self, _is_writable: bool) {}

    fn emit_user_event(&self, _event: CoreUserEvent) {}

    fn emit_exception(&self, _error: CoreError) {}

    fn emit_channel_deactivated(&self) {}

    fn registry(&self) -> &dyn HandlerRegistry {
        &self.registry
    }

    fn add_handler_after(
        &self,
        anchor: Self::HandleId,
        _label: &str,
        _handler: Arc<dyn PipelineHandler>,
    ) -> Self::HandleId {
        anchor
    }

    fn remove_handler(&self, _handle: Self::HandleId) -> bool {
        false
    }

    fn replace_handler(&self, _handle: Self::HandleId, _handler: Arc<dyn PipelineHandler>) -> bool {
        false
    }

    fn epoch(&self) -> u64 {
        0
    }
}
