//! ReadyState 状态机性质验证
//!
//! # 教案级注释概览
//!
//! - **核心目标 (Why)**：对 `ReadyState` 五态（`Ready`/`Busy`/`BudgetExhausted`/`RetryAfter`/`Pending`）进行形式化建模，
//!   验证任意“合法事件序列”在状态机中都可顺利驱动且不会落入不可达状态，同时确保一旦进入 `Pending`
//!   ，必然存在一次可观测的唤醒（`Wake`）。这些性质直接约束 `poll_ready()` 的契约，防止新实现遗漏唤醒或错用状态。
//! - **整体架构位置 (Why)**：测试位于 `crates/spark-core/tests`，与 ReadyState 契约其他测试同级。模型层仅服务于属性验证，
//!   不回写生产代码，属于“影子规格 (Shadow Spec)”——其行为必须与文档《docs/state_machines.md》保持一致。
//! - **设计手法 (Why)**：使用 Proptest 构造“合法事件序列”，通过有限状态机 + 显式唤醒记录，分别断言：
//!   1. 转换总是存在（无 `unreachable`）；2. 每个 `Pending` 区间至少有一次合法唤醒源。
//!      该手法类似 *Model-Based Testing*，以纯 Rust 结构模拟框架契约。
//!
//! # 结构说明 (How)
//!
//! - `ReadyMachine`：影子状态机，包含当前节点、`Pending` 期望唤醒集合、访问信息等。
//! - `MachineEvent`：状态机输入事件，区分 `poll_ready()` 的返回 (`PollOutcome`) 与外部唤醒 (`Wake`)。
//! - `PendingExpectation`：`Pending` 区间的合同，记录允许的唤醒源与是否已被触发。
//! - `legal_sequences()`：利用 `SequenceBuilder` 根据随机控制流构造满足合同的事件序列。
//! - `prop_legal_sequences_have_no_unreachable_state`：性质 1，确保合法序列不会触发状态机错误。
//! - `prop_pending_intervals_have_visible_wake`：性质 2，确保每个 `Pending` 区间都观测到唤醒。
//!
//! # 合同与边界 (What)
//!
//! - **输入**：随机生成的 `Vec<MachineEvent>`，必须满足：
//!   - 每次 `Pending` 事件都至少允许一个唤醒源；
//!   - 从 `Pending` 返回非 `Pending` 状态前，必须出现合法唤醒；
//!   - 最终状态不可停留在“未唤醒的 Pending”。
//! - **输出/断言**：
//!   - 性质 1：`ReadyMachine` 对序列求值返回 `Ok(())`；访问到的状态全集仅包含定义域。
//!   - 性质 2：`ReadyMachine` 记录的所有 `Pending` 区间都标记为 `wake_observed=true`。
//! - **前置条件**：测试依赖 `docs/state_machines.md` 中的转换及唤醒源定义，不涉及真实 I/O。
//! - **后置条件**：一旦模型验证失败，将给出具体事件索引与错误说明，指导实现查缺补漏。
//!
//! # 设计考量 (Trade-offs)
//!
//! - 使用影子模型而非直接调用生产代码，避免框架未来重构导致测试过度耦合；代价是需人工维持模型与文档同步。
//! - `SequenceBuilder` 在生成序列时，会在结尾强制补齐唤醒 + 收敛到非 `Pending` 状态，以免序列收敛性影响性质验证。
//! - 唤醒源只列举 `IoReady`/`Timer`/`ConfigReload`，与文档保持一致；新增来源时需同时更新文档与本测试。
//! - 性质 1 与 2 分离，便于分别定位“状态机不可达”与“唤醒缺失”两类问题。
//!
//! # 风险与跟踪任务 (Gotchas & Tracking)
//!
//! - 若未来 ReadyState 引入新分支，需要补充 `ReadyLeafState` 与转换逻辑；否则生成器会遗漏新状态。
//! - 当 Pending 区间允许多个唤醒源时，`SequenceBuilder` 当前策略是固定使用集合中的第一个源；未来可扩展为随机选择，
//!   但需确保唤醒记录正确更新。
//! - 跟踪事项：若 ReadyState 状态机引入更多上下文（例如错误码），需扩展模型记录以支持更细粒度的性质检查；
//!   详见《docs/tracking/T107.md#readystate-model-telemetry》，跟踪者负责在模型、生成器与文档间保持一致。

use std::collections::BTreeSet;

use proptest::prelude::*;

#[cfg(any(loom, spark_loom))]
mod loom_scenarios {
    //! ReadyState Pending → 唤醒 → 恢复 的 Loom 并发模型。
    //!
    //! ## 教案级导览
    //!
    //! - **核心目标 (Why)**：验证 Pending 注册与唤醒之间的内存可见性，确保“无灰区”——调度线程在离开 Pending 之前一定观测到
    //!   至少一个被允许的唤醒源。
    //! - **整体位置 (Why)**：该模块仅在 `--features loom-model` 下编译，作为文档《docs/state_machines.md》“性质三”的模型化补充，
    //!   与同文件中的 proptest 影子模型共同覆盖 ReadyState 契约。
    //! - **设计手法 (Why)**：使用 `loom::model` 穷举调度交错，三个线程分别代表 `poll_ready()` 驱动与两类唤醒源（I/O、定时器），
    //!   第三个唤醒源 `ConfigReload` 在本场景中被禁止，专门验证“未注册来源不得唤醒”。
    //!
    //! ## 结构与职责 (How)
    //!
    //! - `LoomReadySlot`：以原子变量模拟 ReadyState 的核心字段：当前节点、允许的唤醒掩码、已观测唤醒掩码。
    //!   通过严格的 Acquire/Release 顺序保证调度线程在看到 `Pending` 时必然先看到唤醒配置。
    //! - `register_pending()`：由调度线程调用，登记允许的唤醒掩码并进入 `Pending`。
    //! - `record_wake_if_allowed()`：唤醒线程根据掩码决定是否触发唤醒。
    //! - `await_wake_and_resolve()`：调度线程轮询唤醒掩码，观测到至少一个允许来源后才恢复到非 Pending。
    //! - `pending_requires_observable_wake_before_resolution()`：Loom 场景，断言最终状态一定包含至少一个允许唤醒。
    //!
    //! ## 契约与边界 (What)
    //!
    //! - **输入参数**：线程间共享 `Arc<LoomReadySlot>`，唤醒线程仅在检测到当前掩码允许自身来源时写入唤醒标记。
    //! - **前置条件**：`register_pending()` 必须在唤醒线程观察 `Pending` 之前完成掩码登记；通过 Release/Acquire 保证顺序。
    //! - **后置条件**：`await_wake_and_resolve()` 返回时，状态必为 Ready，且 `observed_mask & allowed_mask != 0`。
    //! - **设计权衡 (Trade-offs)**：采用自旋等待 + `thread::yield_now()` 以让 Loom 探索交错。牺牲一定执行效率，换取可读的模型结构。
    //!   若未来 ReadyState 引入更多唤醒源，可按位扩展掩码并补充线程。

    use super::WakeSource;
    use loom::{
        model,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        thread,
    };

    const READY: usize = 0;
    const PENDING: usize = 1;

    /// 以原子字段模拟 ReadyState Pending 契约的最小载体。
    ///
    /// ### 教案级说明
    /// - **意图 (Why)**：浓缩 Pending -> Wake -> Ready 的读写顺序，验证允许唤醒掩码在多线程下不会失效。
    /// - **逻辑 (How)**：`allowed_mask` 与 `observed_mask` 分别通过 `AtomicUsize` 维护，配合 Release/Acquire 顺序确保调度线程观测顺序正确。
    /// - **契约 (What)**：`state` 仅取 `READY` 或 `PENDING`；调用者必须在进入 `Pending` 前清空 `observed_mask`。
    /// - **注意事项 (Trade-offs)**：使用位掩码取代枚举存储，简化原子操作；若唤醒源超过 8 个需升级为更宽的整数类型。
    struct LoomReadySlot {
        state: AtomicUsize,
        allowed_mask: AtomicUsize,
        observed_mask: AtomicUsize,
    }

    impl LoomReadySlot {
        fn new() -> Self {
            Self {
                state: AtomicUsize::new(READY),
                allowed_mask: AtomicUsize::new(0),
                observed_mask: AtomicUsize::new(0),
            }
        }

        /// 将状态推进到 Pending，并登记允许的唤醒掩码。
        ///
        /// - **输入**：`mask` 表示允许唤醒源的集合，每一位对应一类来源（详见 `mask_for_source`）。
        /// - **前置条件**：调用方应确保当前状态为 Ready，且本轮 Pending 的掩码非零。
        /// - **后置条件**：返回后唤醒线程读取 `state` 为 `PENDING` 时，必能通过 Acquire 读取到最新的 `allowed_mask`。
        fn register_pending(&self, mask: usize) {
            assert!(mask != 0, "Pending 必须至少允许一个唤醒来源");
            self.allowed_mask.store(mask, Ordering::Release);
            self.observed_mask.store(0, Ordering::Release);
            self.state.store(PENDING, Ordering::Release);
        }

        /// 若唤醒源被允许，则写入唤醒标记。
        ///
        /// - **输入**：`source_mask` 为单一来源的掩码。
        /// - **逻辑**：先以 Acquire 读取 `allowed_mask` 确认自身被允许，再通过 `fetch_or` 记录唤醒。
        fn record_wake_if_allowed(&self, source_mask: usize) {
            if self.allowed_mask.load(Ordering::Acquire) & source_mask != 0 {
                self.observed_mask.fetch_or(source_mask, Ordering::AcqRel);
            }
        }

        /// 阻塞直至观测到至少一个合法唤醒，然后恢复到 Ready。
        ///
        /// - **逻辑**：循环读取 `observed_mask`，一旦与 `allowed_mask` 有交集便写入 Ready。
        /// - **前置条件**：调用前须已调用 `register_pending()`。
        /// - **后置条件**：返回时 `state == READY`，且记录的唤醒掩码与允许掩码存在交集。
        fn await_wake_and_resolve(&self) {
            loop {
                let allowed = self.allowed_mask.load(Ordering::Acquire);
                let observed = self.observed_mask.load(Ordering::Acquire);
                if allowed & observed != 0 {
                    self.state.store(READY, Ordering::Release);
                    return;
                }
                thread::yield_now();
            }
        }

        fn load_state(&self) -> usize {
            self.state.load(Ordering::Acquire)
        }

        fn load_allowed_mask(&self) -> usize {
            self.allowed_mask.load(Ordering::Acquire)
        }

        fn load_observed_mask(&self) -> usize {
            self.observed_mask.load(Ordering::Acquire)
        }
    }

    fn mask_for_source(source: WakeSource) -> usize {
        match source {
            WakeSource::IoReady => 0b001,
            WakeSource::Timer => 0b010,
            WakeSource::ConfigReload => 0b100,
        }
    }

    #[test]
    fn pending_requires_observable_wake_before_resolution() {
        model(|| {
            let slot = Arc::new(LoomReadySlot::new());
            let allowed_mask =
                mask_for_source(WakeSource::IoReady) | mask_for_source(WakeSource::Timer);

            let driver = {
                let slot = Arc::clone(&slot);
                thread::spawn(move || {
                    //
                    // 教案级说明：调度线程负责进入 Pending 并等待唤醒。
                    // - **Why**：模拟框架 `poll_ready()` 的驱动方；
                    // - **How**：登记掩码后轮询 `observed_mask`；
                    // - **What**：返回时断言状态重新变为 Ready。
                    slot.register_pending(allowed_mask);
                    slot.await_wake_and_resolve();
                })
            };

            let io_ready = {
                let slot = Arc::clone(&slot);
                thread::spawn(move || {
                    while slot.load_state() != PENDING {
                        thread::yield_now();
                    }
                    slot.record_wake_if_allowed(mask_for_source(WakeSource::IoReady));
                })
            };

            let timer = {
                let slot = Arc::clone(&slot);
                thread::spawn(move || {
                    while slot.load_state() != PENDING {
                        thread::yield_now();
                    }
                    slot.record_wake_if_allowed(mask_for_source(WakeSource::Timer));
                })
            };

            let config_reload = {
                let slot = Arc::clone(&slot);
                thread::spawn(move || {
                    while slot.load_state() != PENDING {
                        thread::yield_now();
                    }
                    //
                    // 教案级说明：该线程验证“未登记来源不得唤醒”。
                    // - 若掩码未包含 ConfigReload，则 `record_wake_if_allowed` 将保持静默。
                    slot.record_wake_if_allowed(mask_for_source(WakeSource::ConfigReload));
                })
            };

            driver.join().expect("调度线程不应 panic");
            io_ready.join().expect("I/O 唤醒线程不应 panic");
            timer.join().expect("定时器唤醒线程不应 panic");
            config_reload.join().expect("配置热更新线程不应 panic");

            let observed = slot.load_observed_mask();
            let allowed = slot.load_allowed_mask();
            assert_eq!(
                slot.load_state(),
                READY,
                "离开 Pending 后状态必须回到 Ready"
            );
            assert_ne!(
                observed & allowed,
                0,
                "Pending 结束前必须观察到至少一个被允许的唤醒来源"
            );
            assert_eq!(
                observed & mask_for_source(WakeSource::ConfigReload),
                0,
                "未登记的 ConfigReload 不应被记录为唤醒来源"
            );
        });
    }
}

/// ReadyState 的影子节点。
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum ReadyNode {
    Ready,
    Busy,
    BudgetExhausted,
    RetryAfter,
    Pending,
}

impl ReadyNode {
    /// 返回从当前节点允许进入的下一跳集合。
    ///
    /// ### 教案级说明
    /// - **意图 (Why)**：在影子模型中显式锁定允许的转换，防止实现引入文档未授权的边。
    /// - **逻辑 (How)**：根据状态机图返回静态切片，并在 `ReadyMachine::apply` 与序列生成器中复用，确保测试与文档一致。
    /// - **契约 (What)**：返回值至少包含一个状态；若允许进入 `Pending`，切片中会显式包含 `ReadyNode::Pending`。
    /// - **注意事项 (Trade-offs)**：为便于 `proptest` 复用，切片顺序与文档无关；调用方需自行决定如何使用返回结果。
    fn allowed_successors(self) -> &'static [ReadyNode] {
        match self {
            ReadyNode::Ready => &[
                ReadyNode::Ready,
                ReadyNode::Busy,
                ReadyNode::BudgetExhausted,
                ReadyNode::RetryAfter,
                ReadyNode::Pending,
            ],
            ReadyNode::Busy => &[
                ReadyNode::Ready,
                ReadyNode::Busy,
                ReadyNode::BudgetExhausted,
                ReadyNode::RetryAfter,
                ReadyNode::Pending,
            ],
            ReadyNode::BudgetExhausted => &[
                ReadyNode::Ready,
                ReadyNode::Busy,
                ReadyNode::RetryAfter,
                ReadyNode::Pending,
            ],
            ReadyNode::RetryAfter => &[
                ReadyNode::Ready,
                ReadyNode::Busy,
                ReadyNode::BudgetExhausted,
                ReadyNode::Pending,
            ],
            ReadyNode::Pending => &[
                ReadyNode::Ready,
                ReadyNode::Busy,
                ReadyNode::BudgetExhausted,
                ReadyNode::RetryAfter,
            ],
        }
    }

    fn can_transition_to(self, next: ReadyNode) -> bool {
        self.allowed_successors().contains(&next)
    }
}

/// ReadyState 的非 Pending 分支。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ReadyLeafState {
    Ready,
    Busy,
    BudgetExhausted,
    RetryAfter,
}

impl From<ReadyLeafState> for ReadyNode {
    fn from(state: ReadyLeafState) -> Self {
        match state {
            ReadyLeafState::Ready => ReadyNode::Ready,
            ReadyLeafState::Busy => ReadyNode::Busy,
            ReadyLeafState::BudgetExhausted => ReadyNode::BudgetExhausted,
            ReadyLeafState::RetryAfter => ReadyNode::RetryAfter,
        }
    }
}

/// 唤醒源，与文档中表格保持一致。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum WakeSource {
    IoReady,
    Timer,
    ConfigReload,
}

impl WakeSource {
    /// 提供稳定的枚举集合，便于构造掩码。
    fn all() -> &'static [WakeSource] {
        &[
            WakeSource::IoReady,
            WakeSource::Timer,
            WakeSource::ConfigReload,
        ]
    }
}

/// `poll_ready()` 的返回结果。
#[derive(Clone, Debug, PartialEq, Eq)]
enum PollOutcome {
    State(ReadyLeafState),
    Pending { allowed_sources: Vec<WakeSource> },
}

/// 状态机事件。
#[derive(Clone, Debug, PartialEq, Eq)]
enum MachineEvent {
    Poll(PollOutcome),
    Wake(WakeSource),
}

/// Pending 区间合同。
#[derive(Clone, Debug, PartialEq, Eq)]
struct PendingExpectation {
    allowed_sources: Vec<WakeSource>,
    wake_observed: bool,
}

impl PendingExpectation {
    fn new(allowed_sources: Vec<WakeSource>) -> Self {
        Self {
            allowed_sources,
            wake_observed: false,
        }
    }
}

/// Pending 区间历史记录，用于性质断言。
#[derive(Clone, Debug, PartialEq, Eq)]
struct PendingRecord {
    allowed_sources: Vec<WakeSource>,
    wake_observed: bool,
}

/// 状态机错误信息。
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
enum MachineError {
    #[error("pending expectation missing for wake")]
    MissingPending,
    #[error("wake source {0:?} not registered in current pending interval")]
    InvalidWake(WakeSource),
    #[error("pending interval finished without wake")]
    MissingWakeBeforeResolution,
    #[error("pending expectation must register at least one wake source")]
    EmptyWakeSet,
    #[error("transition {from:?} -> {to:?} not allowed")]
    InvalidTransition { from: ReadyNode, to: ReadyNode },
}

/// ReadyState 影子状态机。
#[derive(Debug)]
struct ReadyMachine {
    node: ReadyNode,
    pending: Option<PendingExpectation>,
    visited: BTreeSet<ReadyNode>,
    pending_history: Vec<PendingRecord>,
}

impl ReadyMachine {
    fn new() -> Self {
        let mut machine = Self {
            node: ReadyNode::Ready,
            pending: None,
            visited: BTreeSet::new(),
            pending_history: Vec::new(),
        };
        machine.visited.insert(ReadyNode::Ready);
        machine
    }

    /// 对单个事件求值。
    fn apply(&mut self, event: &MachineEvent) -> spark_core::Result<(), MachineError> {
        match event {
            MachineEvent::Poll(PollOutcome::State(state)) => {
                let current = self.node;
                let next_node = ReadyNode::from(*state);
                if !current.can_transition_to(next_node) {
                    return Err(MachineError::InvalidTransition {
                        from: current,
                        to: next_node,
                    });
                }
                if let Some(pending) = self.pending.take() {
                    if !pending.wake_observed {
                        return Err(MachineError::MissingWakeBeforeResolution);
                    }
                    self.pending_history.push(PendingRecord {
                        allowed_sources: pending.allowed_sources,
                        wake_observed: true,
                    });
                }
                self.node = next_node;
                self.visited.insert(next_node);
                Ok(())
            }
            MachineEvent::Poll(PollOutcome::Pending { allowed_sources }) => {
                if allowed_sources.is_empty() {
                    return Err(MachineError::EmptyWakeSet);
                }
                if self.node != ReadyNode::Pending
                    && !self.node.can_transition_to(ReadyNode::Pending)
                {
                    return Err(MachineError::InvalidTransition {
                        from: self.node,
                        to: ReadyNode::Pending,
                    });
                }
                self.visited.insert(ReadyNode::Pending);
                match self.pending.as_mut() {
                    Some(pending) => {
                        pending.allowed_sources = allowed_sources.clone();
                        Ok(())
                    }
                    None => {
                        self.pending = Some(PendingExpectation::new(allowed_sources.clone()));
                        self.node = ReadyNode::Pending;
                        Ok(())
                    }
                }
            }
            MachineEvent::Wake(source) => {
                if let Some(pending) = self.pending.as_mut() {
                    if pending.allowed_sources.contains(source) {
                        pending.wake_observed = true;
                        Ok(())
                    } else {
                        Err(MachineError::InvalidWake(*source))
                    }
                } else {
                    Err(MachineError::MissingPending)
                }
            }
        }
    }
}

/// 构造合法事件序列的生成器。
fn legal_sequences() -> impl Strategy<Value = Vec<MachineEvent>> {
    prop::collection::vec(any::<u8>(), 1..=64).prop_map(|controls| {
        let mut builder = SequenceBuilder::new();
        for control in controls {
            builder.push(control);
        }
        builder.finish()
    })
}

/// 仅包含至少一个 Pending 的序列，用于 Pending 唤醒性质验证。
fn legal_sequences_with_pending() -> impl Strategy<Value = Vec<MachineEvent>> {
    legal_sequences().prop_filter("sequence must contain pending", |events| {
        events
            .iter()
            .any(|event| matches!(event, MachineEvent::Poll(PollOutcome::Pending { .. })))
    })
}

#[test]
fn invalid_budget_exhausted_loop_is_rejected() {
    //
    // 教案级说明：验证 `BudgetExhausted -> BudgetExhausted` 会触发 `InvalidTransition`。
    // - **Why**：文档将该转换列为禁止项；测试确保影子模型正确报警。
    // - **How**：先驱动一次合法的 `Ready -> BudgetExhausted`，再尝试重复返回同一状态。
    // - **What**：期望第一次 `apply` 返回 `Ok(())`，第二次返回 `InvalidTransition`。
    let mut machine = ReadyMachine::new();
    let first = MachineEvent::Poll(PollOutcome::State(ReadyLeafState::BudgetExhausted));
    let second = MachineEvent::Poll(PollOutcome::State(ReadyLeafState::BudgetExhausted));

    assert_eq!(machine.apply(&first), Ok(()));
    assert_eq!(
        machine.apply(&second),
        Err(MachineError::InvalidTransition {
            from: ReadyNode::BudgetExhausted,
            to: ReadyNode::BudgetExhausted,
        })
    );
}

/// 构造事件序列的辅助状态。
struct SequenceBuilder {
    events: Vec<MachineEvent>,
    node: ReadyNode,
    pending: Option<PendingExpectation>,
}

impl SequenceBuilder {
    fn new() -> Self {
        Self {
            events: Vec::new(),
            node: ReadyNode::Ready,
            pending: None,
        }
    }

    fn push(&mut self, control: u8) {
        match self.node {
            ReadyNode::Pending => self.drive_pending(control),
            _ => self.drive_non_pending(control),
        }
    }

    fn finish(mut self) -> Vec<MachineEvent> {
        if let Some(mut pending) = self.pending.take() {
            if !pending.wake_observed {
                let source = pending.allowed_sources[0];
                self.events.push(MachineEvent::Wake(source));
                pending.wake_observed = true;
            }
            self.emit_leaf(ReadyLeafState::Ready);
        }
        self.events
    }

    fn drive_non_pending(&mut self, control: u8) {
        let successors = self.node.allowed_successors();
        let idx = (control as usize) % successors.len();
        match successors[idx] {
            ReadyNode::Ready => self.emit_leaf(ReadyLeafState::Ready),
            ReadyNode::Busy => self.emit_leaf(ReadyLeafState::Busy),
            ReadyNode::BudgetExhausted => self.emit_leaf(ReadyLeafState::BudgetExhausted),
            ReadyNode::RetryAfter => self.emit_leaf(ReadyLeafState::RetryAfter),
            ReadyNode::Pending => {
                let allowed = Self::select_sources(control.wrapping_mul(31));
                self.events.push(MachineEvent::Poll(PollOutcome::Pending {
                    allowed_sources: allowed.clone(),
                }));
                self.pending = Some(PendingExpectation::new(allowed));
                self.node = ReadyNode::Pending;
            }
        }
    }

    fn drive_pending(&mut self, control: u8) {
        let pending = self.pending.as_mut().expect("pending state must exist");
        if !pending.wake_observed {
            if control % 3 == 0 {
                let index = (control as usize / 3) % pending.allowed_sources.len();
                let source = pending.allowed_sources[index];
                self.events.push(MachineEvent::Wake(source));
                pending.wake_observed = true;
            } else {
                let allowed = Self::select_sources(control);
                self.events.push(MachineEvent::Poll(PollOutcome::Pending {
                    allowed_sources: allowed.clone(),
                }));
                pending.allowed_sources = allowed;
            }
        } else {
            let successors = ReadyNode::Pending.allowed_successors();
            let idx = (control as usize) % successors.len();
            let leaf = match successors[idx] {
                ReadyNode::Ready => ReadyLeafState::Ready,
                ReadyNode::Busy => ReadyLeafState::Busy,
                ReadyNode::BudgetExhausted => ReadyLeafState::BudgetExhausted,
                ReadyNode::RetryAfter => ReadyLeafState::RetryAfter,
                ReadyNode::Pending => unreachable!("Pending 不允许自循环"),
            };
            self.emit_leaf(leaf);
            self.pending = None;
        }
    }

    fn select_sources(seed: u8) -> Vec<WakeSource> {
        let mut mask = seed % 8;
        if mask == 0 {
            mask = 1;
        }
        let mut sources = Vec::new();
        for (bit, source) in WakeSource::all().iter().enumerate() {
            if (mask >> bit) & 1 == 1 {
                sources.push(*source);
            }
        }
        sources
    }

    /// 发射叶子节点事件，并维护内部状态指针。
    ///
    /// - **意图 (Why)**：统一封装“将叶子状态写入事件队列并更新当前节点”的重复样板，避免逻辑分散。
    /// - **逻辑 (How)**：推入 `MachineEvent::Poll(PollOutcome::State(..))`，随后借由 `ReadyNode::from` 同步内部节点。
    /// - **契约 (What)**：调用方需保证 `leaf` 不为 `Pending`，否则语义与 `ReadyNode::from` 不匹配。
    /// - **注意事项 (Trade-offs)**：保持极简以降低生成器的噪音，若未来需要附加统计信息，可在此集中扩展。
    fn emit_leaf(&mut self, leaf: ReadyLeafState) {
        self.events
            .push(MachineEvent::Poll(PollOutcome::State(leaf)));
        self.node = ReadyNode::from(leaf);
    }
}

proptest! {
    #[test]
    fn prop_legal_sequences_have_no_unreachable_state(events in legal_sequences()) {
        let mut machine = ReadyMachine::new();
        for event in &events {
            prop_assert_eq!(machine.apply(event), Ok(()));
        }
        for node in &machine.visited {
            prop_assert!(matches!(node, ReadyNode::Ready | ReadyNode::Busy | ReadyNode::BudgetExhausted | ReadyNode::RetryAfter | ReadyNode::Pending));
        }
    }

    #[test]
    fn prop_pending_intervals_have_visible_wake(events in legal_sequences_with_pending()) {
        let mut machine = ReadyMachine::new();
        for event in &events {
            prop_assert_eq!(machine.apply(event), Ok(()));
        }
        prop_assert!(!machine.pending_history.is_empty());
        for record in &machine.pending_history {
            prop_assert!(record.wake_observed);
            prop_assert!(!record.allowed_sources.is_empty());
        }
    }
}
