#![no_main]

use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;
use spark_core::service::simple::{ReadyFutureGuard, ServiceReadyCoordinator};
use spark_core::status::ready::{ReadyCheck, ReadyState};
use spark_core::CoreError;
use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};

/// Fuzz 指令：描述一次状态机操作序列。
///
/// - **Why**：历史上 0-RTT 重放曾导致 `ServiceReadyCoordinator` 状态机卡死或重复释放，本结构显式枚举可能的调度路径，
///   帮助 Fuzzer 穷举组合并快速复现潜在 bug。
/// - **How**：操作包含 `Poll`、`Begin`、`Release`、`DropGuard`、`Clone` 五种基本动作，通过索引选择目标协调器。
/// - **What**：Fuzzer 将生成任意长度的指令流，用于验证状态机在多副本、乱序操作下依旧保持“只允许一次未释放调用”。
#[derive(Debug, Arbitrary)]
struct StateMachineCase {
    ops: Vec<CoordinatorOp>,
}

/// 具体状态机操作。
#[derive(Debug, Arbitrary)]
enum CoordinatorOp {
    /// 调用 `poll_ready` 并根据返回值更新许可标记。
    Poll { id: u8 },
    /// 若拥有许可则调用 `begin_call`，模拟业务开始执行。
    Begin { id: u8 },
    /// 显式调用 `ReadyFutureGuard::release`，模拟 Future 正常完成。
    Release { id: u8 },
    /// 丢弃当前守卫，模拟 Future 被取消或超时。
    DropGuard { id: u8 },
    /// 克隆协调器，模拟同一服务被多个任务共享。
    Clone { from: u8 },
}

fuzz_target!(|case: StateMachineCase| {
    if case.ops.is_empty() {
        return;
    }

    let mut coordinators = vec![ServiceReadyCoordinator::new()];
    let mut guards: Vec<Option<ReadyFutureGuard>> = vec![None];
    let mut permits = vec![false];

    for op in case.ops {
        match op {
            CoordinatorOp::Poll { id } => {
                let idx = map_index(&coordinators, id);
                let waker = noop_waker();
                let mut cx = Context::from_waker(&waker);
                match coordinators[idx].poll_ready::<CoreError>(&mut cx) {
                    Poll::Ready(ReadyCheck::Ready(ReadyState::Ready)) => {
                        permits[idx] = true;
                    }
                    Poll::Ready(ReadyCheck::Ready(_)) => {
                        permits[idx] = false;
                    }
                    Poll::Ready(ReadyCheck::Err(_)) => {
                        permits[idx] = false;
                    }
                    Poll::Pending => {
                        permits[idx] = false;
                    }
                    Poll::Ready(other) => {
                        // Why：`ReadyCheck` 在未来版本可能新增 `Backoff`、`TimedOut` 等分支。
                        // How：将未知分支统一视作“当前不可开始新调用”，避免 Fuzzer 因未匹配导致编译失败。
                        // What：复位许可标记，保持状态机与现有分支行为一致，确保不会误发放 permit。
                        permits[idx] = false;
                        let _ = other; // 显式使用以维持 exhaustiveness。
                    }
                }
            }
            CoordinatorOp::Begin { id } => {
                let idx = map_index(&coordinators, id);
                if permits[idx] && guards[idx].is_none() {
                    guards[idx] = Some(coordinators[idx].begin_call());
                    permits[idx] = false;
                }
            }
            CoordinatorOp::Release { id } => {
                let idx = map_index(&coordinators, id);
                if let Some(guard) = guards[idx].as_mut() {
                    guard.release();
                }
                permits[idx] = false;
            }
            CoordinatorOp::DropGuard { id } => {
                let idx = map_index(&coordinators, id);
                drop(guards[idx].take());
                permits[idx] = false;
            }
            CoordinatorOp::Clone { from } => {
                let idx = map_index(&coordinators, from);
                coordinators.push(coordinators[idx].clone());
                guards.push(None);
                permits.push(false);
            }
        }
    }

    // 最终释放所有仍持有的守卫，确保状态回到 Ready，若实现存在潜在 panic 会在此触发。
    for guard in guards.iter_mut().filter_map(|slot| slot.take()) {
        drop(guard);
    }
});

/// 根据给定 id 映射到现有协调器索引。
fn map_index<T>(items: &[T], id: u8) -> usize {
    let len = items.len();
    if len == 0 {
        0
    } else {
        (id as usize) % len
    }
}

/// 构造空操作的 `RawWaker`，用于在无真实执行器环境下驱动 `poll_ready`。
fn noop_raw_waker() -> RawWaker {
    fn clone(_: *const ()) -> RawWaker {
        noop_raw_waker()
    }
    fn wake(_: *const ()) {}
    fn wake_by_ref(_: *const ()) {}
    fn drop(_: *const ()) {}
    static VTABLE: RawWakerVTable = RawWakerVTable::new(clone, wake, wake_by_ref, drop);
    RawWaker::new(core::ptr::null(), &VTABLE)
}

/// 提供简易 `Waker`。
fn noop_waker() -> Waker {
    unsafe { Waker::from_raw(noop_raw_waker()) }
}
