#![cfg(any(loom, spark_loom))]

use loom::{
    model,
    sync::{
        Arc,
        atomic::{AtomicU8, AtomicUsize, Ordering},
    },
    thread,
};
use spark_core::{
    contract::Cancellation,
    types::{Budget, BudgetKind},
};

#[test]
fn cancellation_visibility_is_sequentially_consistent() {
    //
    // 教案级说明：该测试验证 `Cancellation` 在多线程下的内存可见性。
    // - **Why**：取消信号需要被其他协程及时感知，否则会导致超时/回滚机制失效。
    // - **How**：通过 Loom 穷举线程调度，观察 `cancel` 的释放语义是否能被 `is_cancelled` 的获取语义看见。
    // - **What**：若可见性正确，观察线程最终必然退出等待循环，且后续重复取消返回 `false`。
    // - **Trade-offs**：循环中使用 `thread::yield_now()` 限制忙等，确保 Loom 能探索足够的交错而不至于无限自旋。
    model(|| {
        let root = Cancellation::new();
        let worker = root.child();
        let observer = root.child();

        let canceler = thread::spawn(move || {
            assert!(worker.cancel(), "第一次取消必须成功");
        });

        let watcher = thread::spawn(move || {
            while !observer.is_cancelled() {
                thread::yield_now();
            }
        });

        canceler.join().expect("取消线程不应 panic");
        watcher.join().expect("观察线程不应 panic");
        assert!(root.is_cancelled(), "主线程应观察到取消标记");
        assert!(
            !root.cancel(),
            "重复取消应返回 false，验证 `compare_exchange` 的幂等语义"
        );
    });
}

#[test]
fn budget_concurrent_consume_and_refund_preserves_limits() {
    //
    // 教案级说明：验证预算控制器在并发消费与归还时不会出现下溢或超限。
    // - **Why**：预算常在多线程中共享，错误的原子序列可能导致资源被重复扣减或错误归还。
    // - **How**：两个线程分别尝试消耗不同额度，随后根据返回结果决定是否归还；
    //   Loom 将探索所有执行顺序，帮助我们确认最终剩余额度始终回到上限。
    // - **What**：测试结束后 `remaining()` 必须等于 `limit()`，表明没有漏记或重复记账。
    // - **Trade-offs**：线程间未强制顺序，允许出现 `Exhausted` 分支，用以覆盖竞争场景。
    model(|| {
        let budget = Arc::new(Budget::new(BudgetKind::Decode, 2));

        let fast_path = {
            let budget = Arc::clone(&budget);
            thread::spawn(move || {
                let decision = budget.try_consume(1);
                //
                // 教案级说明：若在交错中先行被另一线程抢占，也可能返回 `Exhausted`，
                // 因此仅在成功分配时执行归还操作，保持测试对调度顺序的鲁棒性。
                if decision.is_granted() {
                    budget.refund(1);
                }
            })
        };

        let greedy_path = {
            let budget = Arc::clone(&budget);
            thread::spawn(move || {
                let decision = budget.try_consume(2);
                if decision.is_granted() {
                    budget.refund(2);
                }
            })
        };

        fast_path.join().expect("快速路径线程不应 panic");
        greedy_path.join().expect("高消耗线程不应 panic");
        assert_eq!(
            budget.remaining(),
            budget.limit(),
            "无论调度顺序如何，剩余额度必须回到上限"
        );
    });
}

/// 基于 Loom 的最小通道状态机，验证优雅关闭与强制关闭的竞态收敛到 `Closed`。
///
/// # 教案式说明
/// - **意图 (Why)**：模拟管道通道在并发触发 `close_graceful` 与 `close` 时的原子序列，
///   确认状态机不会回退或重复计数。
/// - **逻辑 (How)**：内部维护 `state`、`graceful`、`force` 三个原子；
///   `close_graceful` 仅在 `Active -> Draining` 转换时增加计数，`close` 将状态推进到 `Closed`，
///   并在首次转换时记录强制次数。
/// - **契约 (What)**：
///   - **前置条件**：初始状态为 `Active`；
///   - **后置条件**：所有线程完成后状态为 `Closed`，`graceful_count <= 1`（并发下允许强制关闭先行）、
///     `force_count == 1`；
///   - **风险提示**：若比较交换顺序错误，可能导致计数多次递增或状态回退，本场景将触发断言。
struct LoomChannelState {
    state: AtomicU8,
    graceful_count: AtomicUsize,
    force_count: AtomicUsize,
}

impl LoomChannelState {
    const ACTIVE: u8 = 0;
    const DRAINING: u8 = 1;
    const CLOSED: u8 = 2;

    fn new() -> Self {
        Self {
            state: AtomicU8::new(Self::ACTIVE),
            graceful_count: AtomicUsize::new(0),
            force_count: AtomicUsize::new(0),
        }
    }

    fn close_graceful(&self) {
        let mut current = self.state.load(Ordering::Acquire);
        loop {
            match current {
                Self::ACTIVE => match self.state.compare_exchange(
                    Self::ACTIVE,
                    Self::DRAINING,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(_) => {
                        self.graceful_count.fetch_add(1, Ordering::AcqRel);
                        return;
                    }
                    Err(next) => current = next,
                },
                Self::DRAINING | Self::CLOSED => return,
                _ => current = Self::CLOSED,
            }
        }
    }

    fn close(&self) {
        let previous = self.state.swap(Self::CLOSED, Ordering::AcqRel);
        if previous != Self::CLOSED {
            self.force_count.fetch_add(1, Ordering::AcqRel);
        }
    }

    fn is_closed(&self) -> bool {
        self.state.load(Ordering::Acquire) == Self::CLOSED
    }

    fn graceful_invocations(&self) -> usize {
        self.graceful_count.load(Ordering::Acquire)
    }

    fn force_invocations(&self) -> usize {
        self.force_count.load(Ordering::Acquire)
    }
}

#[test]
fn channel_close_paths_converge_to_closed() {
    //
    // 教案级说明：验证通道在并发关闭路径下的终态唯一性。
    // - **Why**：在运行时中通道关闭可能由两个线程同时触发，必须保证状态最终落在 `Closed`。
    // - **How**：通过 Loom 穷举 `close_graceful` 与 `close` 的调度交错，并附加一个观察线程模拟等待关闭。
    // - **What**：断言 `graceful` 计数为 1、`force` 计数为 1，且状态不可回退。
    model(|| {
        let channel = Arc::new(LoomChannelState::new());

        let graceful = {
            let channel = Arc::clone(&channel);
            thread::spawn(move || {
                channel.close_graceful();
            })
        };

        let force_first = {
            let channel = Arc::clone(&channel);
            thread::spawn(move || {
                channel.close();
            })
        };

        let force_second = {
            let channel = Arc::clone(&channel);
            thread::spawn(move || {
                channel.close();
            })
        };

        let observer = {
            let channel = Arc::clone(&channel);
            thread::spawn(move || {
                while !channel.is_closed() {
                    thread::yield_now();
                }
            })
        };

        graceful.join().expect("优雅关闭线程不应 panic");
        force_first.join().expect("第一次强制关闭线程不应 panic");
        force_second.join().expect("重复强制关闭线程不应 panic");
        observer.join().expect("观察线程不应 panic");

        assert!(channel.is_closed(), "通道最终必须进入 Closed 状态");
        assert!(
            channel.graceful_invocations() <= 1,
            "优雅关闭在并发场景下至多递增一次"
        );
        assert_eq!(
            channel.force_invocations(),
            1,
            "强制关闭计数只能在首次 close 时递增"
        );
    });
}
