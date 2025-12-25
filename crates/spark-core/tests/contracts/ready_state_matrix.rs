//! ReadyState 转换矩阵契约测试
//!
//! # 教案式导航
//! - **核心目标 (Why)**：逐一驱动《docs/state_machines.md》定义的 22 条合法转换，验证泛型 Service 与对象层
//!   `DynService` 在 `poll_ready` 上对 `ReadyState` 的暴露保持等价，防止过程宏或桥接层遗漏某个状态分支。
//! - **整体定位 (Where)**：位于 `tests/contracts`，与其他契约测试协同，确保 ReadyState 语义的“文档 → 测试 → 实现”三位一体。
//! - **执行策略 (How)**：
//!   1. 使用 [`MatrixPlan`] 描述目标状态序列与 Pending 目标；
//!   2. 通过 [`MatrixReadyService`] 手写实现的 `Service` 顺序输出状态，并在 Pending 时登记 waker；
//!   3. 对每个用例分别驱动泛型层与对象层（`bridge_to_box_service`），对比实际观测的状态序列与唤醒行为。
//! - **契约声明 (What)**：每个测试用例必须至少包含一次 `poll_ready` 调用；若计划包含 Pending，必须观测到一次唤醒且
//!   在返回最终状态后清空 Pending 槽位。
//! - **风险提示 (Trade-offs & Gotchas)**：若未来 ReadyState 新增分支或允许更多 Pending → X 组合，需同步更新 `ready_state_cases()`
//!   与文档；`MatrixReadyService` 假设同一用例中 Pending 仅出现一次，若要测试多次 Pending 可额外扩展计划表达能力。

use core::{
    future::{Ready, ready},
    task::{Context as TaskContext, Poll, RawWaker, RawWakerVTable, Waker},
};

use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use spark_core::{
    SparkError,
    buffer::PipelineMessage,
    context::Context as ExecutionView,
    contract::CallContext,
    service::{Service, bridge_to_box_service},
    status::{BusyReason, ReadyCheck, ReadyState, RetryAdvice, SubscriptionBudget},
};

/// ReadyState 转换矩阵：确保泛型层与对象层都能观测到一致的状态序列。
#[test]
fn ready_state_transition_matrix_matches_contract() {
    for case in ready_state_cases() {
        drive_generic_path(&case);
        drive_dynamic_path(&case);
    }
}

/// 教案式测试用例：描述状态序列与是否包含 Pending。
struct TransitionCase {
    name: &'static str,
    plan: MatrixPlan,
    expected: Vec<ReadyState>,
    expect_pending: bool,
}

impl TransitionCase {
    fn new(
        name: &'static str,
        steps: Vec<Step>,
        expected: Vec<ReadyState>,
        expect_pending: bool,
    ) -> Self {
        Self {
            name,
            plan: MatrixPlan::new(steps),
            expected,
            expect_pending,
        }
    }
}

/// ReadyState 推演计划：复用在泛型层与对象层。
#[derive(Clone)]
struct MatrixPlan {
    steps: Arc<[Step]>,
}

impl MatrixPlan {
    fn new(steps: Vec<Step>) -> Self {
        Self {
            steps: steps.into(),
        }
    }

    fn spawn_service(&self) -> MatrixReadyService {
        MatrixReadyService::new(Arc::clone(&self.steps))
    }
}

/// 手写 Service：顺序输出 `ReadyState`，在 Pending 时登记/释放 waker。
struct MatrixReadyService {
    steps: Arc<[Step]>,
    cursor: usize,
    pending: Arc<PendingInner>,
}

impl MatrixReadyService {
    fn new(steps: Arc<[Step]>) -> Self {
        Self {
            steps,
            cursor: 0,
            pending: Arc::new(PendingInner::new()),
        }
    }

    fn pending_driver(&self) -> MatrixPendingDriver {
        MatrixPendingDriver {
            inner: Arc::clone(&self.pending),
        }
    }
}

impl Service<PipelineMessage> for MatrixReadyService {
    type Response = PipelineMessage;
    type Error = SparkError;
    type Future = Ready<spark_core::Result<Self::Response, Self::Error>>;

    fn poll_ready(
        &mut self,
        _ctx: &ExecutionView<'_>,
        cx: &mut TaskContext<'_>,
    ) -> spark_core::status::PollReady<Self::Error> {
        {
            let mut slot = self.pending.state.lock().expect("pending mutex poisoned");
            if let Some(pending) = slot.as_mut() {
                if pending.resolved {
                    let next_state = pending.target.clone();
                    *slot = None;
                    return Poll::Ready(ReadyCheck::Ready(next_state));
                }

                match pending.waker.as_ref() {
                    Some(existing) if existing.will_wake(cx.waker()) => {}
                    _ => pending.waker = Some(cx.waker().clone()),
                }
                return Poll::Pending;
            }
        }

        if self.cursor >= self.steps.len() {
            return Poll::Ready(ReadyCheck::Ready(ReadyState::Ready));
        }

        match &self.steps[self.cursor] {
            Step::Leaf(state) => {
                self.cursor += 1;
                Poll::Ready(ReadyCheck::Ready(state.clone()))
            }
            Step::Pending(target) => {
                let mut slot = self.pending.state.lock().expect("pending mutex poisoned");
                *slot = Some(PendingState {
                    target: target.clone(),
                    resolved: false,
                    waker: Some(cx.waker().clone()),
                });
                self.cursor += 1;
                Poll::Pending
            }
        }
    }

    fn call(&mut self, _ctx: CallContext, req: PipelineMessage) -> Self::Future {
        ready(Ok(req))
    }
}

/// Pending 状态的共享槽位。
struct PendingInner {
    state: Mutex<Option<PendingState>>,
}

impl PendingInner {
    fn new() -> Self {
        Self {
            state: Mutex::new(None),
        }
    }
}

struct PendingState {
    target: ReadyState,
    resolved: bool,
    waker: Option<Waker>,
}

/// 测试专用 Pending 驱动句柄：负责触发唤醒并断言槽位清理。
#[derive(Clone)]
struct MatrixPendingDriver {
    inner: Arc<PendingInner>,
}

impl MatrixPendingDriver {
    fn resolve_pending(&self) {
        let mut slot = self.inner.state.lock().expect("pending mutex poisoned");
        let state = slot
            .as_mut()
            .expect("resolve_pending called without active Pending");
        state.resolved = true;
        if let Some(waker) = state.waker.take() {
            waker.wake();
        }
    }

    fn assert_cleared(&self, case: &str) {
        let slot = self.inner.state.lock().expect("pending mutex poisoned");
        assert!(
            slot.is_none(),
            "{case}: Pending slot must be cleared after resolution"
        );
    }
}

#[derive(Clone)]
enum Step {
    Leaf(ReadyState),
    Pending(ReadyState),
}

/// 驱动泛型 Service 并核对状态序列。
fn drive_generic_path(case: &TransitionCase) {
    let mut service = case.plan.spawn_service();
    let driver = service.pending_driver();

    let call_ctx = CallContext::builder().build();
    let exec_ctx = call_ctx.execution();

    let wake_flag = Arc::new(AtomicBool::new(false));
    let waker = flag_waker(wake_flag.clone());
    let mut cx = TaskContext::from_waker(&waker);

    let mut observed = Vec::new();
    let mut pending_seen = false;

    while observed.len() < case.expected.len() {
        match service.poll_ready(&exec_ctx, &mut cx) {
            Poll::Ready(ReadyCheck::Ready(state)) => observed.push(state),
            Poll::Ready(ReadyCheck::Err(err)) => {
                panic!("{}: unexpected readiness error: {err}", case.name)
            }
            Poll::Ready(other) => panic!("{}: unexpected readiness variant: {other:?}", case.name),
            Poll::Pending => {
                assert!(
                    case.expect_pending,
                    "{}: generic service returned Pending but case forbids it",
                    case.name
                );
                assert!(
                    !pending_seen,
                    "{}: generic service should not emit multiple Pending segments",
                    case.name
                );
                pending_seen = true;
                wake_flag.store(false, Ordering::SeqCst);
                driver.resolve_pending();
                assert!(
                    wake_flag.load(Ordering::SeqCst),
                    "{}: Pending must trigger a wake in generic path",
                    case.name
                );
            }
        }
    }

    assert_eq!(
        observed, case.expected,
        "{}: generic ReadyState sequence mismatch",
        case.name
    );
    assert_eq!(
        pending_seen, case.expect_pending,
        "{}: generic Pending expectation mismatch",
        case.name
    );
    driver.assert_cleared(case.name);
}

/// 驱动对象层 DynService 并核对状态序列。
fn drive_dynamic_path(case: &TransitionCase) {
    let service = case.plan.spawn_service();
    let driver = service.pending_driver();
    let boxed = bridge_to_box_service::<_, PipelineMessage>(service);
    let mut arc = boxed.into_arc();
    let dyn_service =
        Arc::get_mut(&mut arc).expect("boxed service must be uniquely owned in tests");

    let call_ctx = CallContext::builder().build();
    let exec_ctx = call_ctx.execution();

    let wake_flag = Arc::new(AtomicBool::new(false));
    let waker = flag_waker(wake_flag.clone());
    let mut cx = TaskContext::from_waker(&waker);

    let mut observed = Vec::new();
    let mut pending_seen = false;

    while observed.len() < case.expected.len() {
        match dyn_service.poll_ready_dyn(&exec_ctx, &mut cx) {
            Poll::Ready(ReadyCheck::Ready(state)) => observed.push(state),
            Poll::Ready(ReadyCheck::Err(err)) => {
                panic!("{}: dynamic service readiness error: {err}", case.name)
            }
            Poll::Ready(other) => panic!("{}: dynamic readiness variant: {other:?}", case.name),
            Poll::Pending => {
                assert!(
                    case.expect_pending,
                    "{}: dynamic service returned Pending but case forbids it",
                    case.name
                );
                assert!(
                    !pending_seen,
                    "{}: dynamic service should not emit multiple Pending segments",
                    case.name
                );
                pending_seen = true;
                wake_flag.store(false, Ordering::SeqCst);
                driver.resolve_pending();
                assert!(
                    wake_flag.load(Ordering::SeqCst),
                    "{}: Pending must trigger a wake in dynamic path",
                    case.name
                );
            }
        }
    }

    assert_eq!(
        observed, case.expected,
        "{}: dynamic ReadyState sequence mismatch",
        case.name
    );
    assert_eq!(
        pending_seen, case.expect_pending,
        "{}: dynamic Pending expectation mismatch",
        case.name
    );
    driver.assert_cleared(case.name);
}

/// 生成 ReadyState 转换矩阵用例。
fn ready_state_cases() -> Vec<TransitionCase> {
    vec![
        TransitionCase::new(
            "ready_to_ready",
            vec![Step::Leaf(ReadyState::Ready), Step::Leaf(ReadyState::Ready)],
            vec![ReadyState::Ready, ReadyState::Ready],
            false,
        ),
        TransitionCase::new(
            "ready_to_busy",
            vec![Step::Leaf(ReadyState::Ready), Step::Leaf(busy_upstream())],
            vec![ReadyState::Ready, busy_upstream()],
            false,
        ),
        TransitionCase::new(
            "ready_to_budget",
            vec![
                Step::Leaf(ReadyState::Ready),
                Step::Leaf(budget_state(10, 0)),
            ],
            vec![ReadyState::Ready, budget_state(10, 0)],
            false,
        ),
        TransitionCase::new(
            "ready_to_retry_after",
            vec![
                Step::Leaf(ReadyState::Ready),
                Step::Leaf(retry_after_ms(40)),
            ],
            vec![ReadyState::Ready, retry_after_ms(40)],
            false,
        ),
        TransitionCase::new(
            "ready_to_pending_then_ready",
            vec![
                Step::Leaf(ReadyState::Ready),
                Step::Pending(ReadyState::Ready),
            ],
            vec![ReadyState::Ready, ReadyState::Ready],
            true,
        ),
        TransitionCase::new(
            "busy_to_ready",
            vec![Step::Leaf(busy_downstream()), Step::Leaf(ReadyState::Ready)],
            vec![busy_downstream(), ReadyState::Ready],
            false,
        ),
        TransitionCase::new(
            "busy_to_busy",
            vec![Step::Leaf(busy_upstream()), Step::Leaf(busy_queue_full())],
            vec![busy_upstream(), busy_queue_full()],
            false,
        ),
        TransitionCase::new(
            "busy_to_budget",
            vec![
                Step::Leaf(busy_downstream()),
                Step::Leaf(budget_state(8, 2)),
            ],
            vec![busy_downstream(), budget_state(8, 2)],
            false,
        ),
        TransitionCase::new(
            "busy_to_retry",
            vec![Step::Leaf(busy_upstream()), Step::Leaf(retry_after_ms(55))],
            vec![busy_upstream(), retry_after_ms(55)],
            false,
        ),
        TransitionCase::new(
            "busy_to_pending_then_busy",
            vec![
                Step::Leaf(busy_queue_full()),
                Step::Pending(busy_downstream()),
            ],
            vec![busy_queue_full(), busy_downstream()],
            true,
        ),
        TransitionCase::new(
            "budget_to_ready",
            vec![
                Step::Leaf(budget_state(4, 0)),
                Step::Leaf(ReadyState::Ready),
            ],
            vec![budget_state(4, 0), ReadyState::Ready],
            false,
        ),
        TransitionCase::new(
            "budget_to_busy",
            vec![Step::Leaf(budget_state(5, 0)), Step::Leaf(busy_upstream())],
            vec![budget_state(5, 0), busy_upstream()],
            false,
        ),
        TransitionCase::new(
            "budget_to_retry",
            vec![
                Step::Leaf(budget_state(3, 0)),
                Step::Leaf(retry_after_ms(70)),
            ],
            vec![budget_state(3, 0), retry_after_ms(70)],
            false,
        ),
        TransitionCase::new(
            "budget_to_pending_then_ready",
            vec![
                Step::Leaf(budget_state(6, 0)),
                Step::Pending(ReadyState::Ready),
            ],
            vec![budget_state(6, 0), ReadyState::Ready],
            true,
        ),
        TransitionCase::new(
            "retry_to_ready",
            vec![
                Step::Leaf(retry_after_ms(30)),
                Step::Leaf(ReadyState::Ready),
            ],
            vec![retry_after_ms(30), ReadyState::Ready],
            false,
        ),
        TransitionCase::new(
            "retry_to_busy",
            vec![
                Step::Leaf(retry_after_ms(45)),
                Step::Leaf(busy_custom("retry_busy")),
            ],
            vec![retry_after_ms(45), busy_custom("retry_busy")],
            false,
        ),
        TransitionCase::new(
            "retry_to_budget",
            vec![
                Step::Leaf(retry_after_ms(90)),
                Step::Leaf(budget_state(2, 0)),
            ],
            vec![retry_after_ms(90), budget_state(2, 0)],
            false,
        ),
        TransitionCase::new(
            "retry_to_pending_then_budget",
            vec![
                Step::Leaf(retry_after_ms(60)),
                Step::Pending(budget_state(10, 1)),
            ],
            vec![retry_after_ms(60), budget_state(10, 1)],
            true,
        ),
        TransitionCase::new(
            "pending_to_ready",
            vec![Step::Pending(ReadyState::Ready)],
            vec![ReadyState::Ready],
            true,
        ),
        TransitionCase::new(
            "pending_to_busy",
            vec![Step::Pending(busy_custom("pending_busy"))],
            vec![busy_custom("pending_busy")],
            true,
        ),
        TransitionCase::new(
            "pending_to_budget",
            vec![Step::Pending(budget_state(12, 0))],
            vec![budget_state(12, 0)],
            true,
        ),
        TransitionCase::new(
            "pending_to_retry",
            vec![Step::Pending(retry_after_ms(25))],
            vec![retry_after_ms(25)],
            true,
        ),
    ]
}

fn busy_upstream() -> ReadyState {
    ReadyState::Busy(BusyReason::upstream())
}

fn busy_downstream() -> ReadyState {
    ReadyState::Busy(BusyReason::downstream())
}

fn busy_queue_full() -> ReadyState {
    ReadyState::Busy(BusyReason::queue_full(32, 64))
}

fn busy_custom(tag: &'static str) -> ReadyState {
    ReadyState::Busy(BusyReason::custom(tag))
}

fn budget_state(limit: u32, remaining: u32) -> ReadyState {
    ReadyState::BudgetExhausted(SubscriptionBudget { limit, remaining })
}

fn retry_after_ms(delay: u64) -> ReadyState {
    ReadyState::RetryAfter(RetryAdvice::after(Duration::from_millis(delay)))
}

/// 创建触发布尔标记的自定义 Waker。
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
