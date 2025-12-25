use std::time::Duration;

use spark_core::contract::{Cancellation, Deadline};

use super::support::DeterministicClock;

/// 验证手动取消在截止点竞态中具备最高优先级，确保不会被超时逻辑饿死。
///
/// # 测试目标（Why）
/// - 模拟“截止点已到达，同时上游触发手动取消”的边界情况；
/// - 确认取消原语的幂等语义能够阻止超时逻辑重复抢占，符合“取消优先级最高”的设计约定。
///
/// # 测试步骤（How）
/// 1. 使用确定性时钟创建当前时间 `now` 与 10ms 的超时窗口；
/// 2. 将时钟直接推进到截止点，保证 `Deadline::is_expired` 返回 `true`；
/// 3. 先执行一次手动取消，随后模拟调度器的“超时检查”，确认后者不会再次成功取消；
/// 4. 断言在该顺序下最终仍只有一次取消成功，体现优先级契约。
///
/// # 输入/输出契约（What）
/// - **前置条件**：调用顺序固定为“手动取消 → 超时检查”；
/// - **后置条件**：手动取消返回 `true`，超时分支返回 `false` 且依赖 `is_cancelled()` 短路；
/// - **风险提示**：若未来修改为“超时直接调用 `cancel()` 不检查状态”，需同步更新此测试以覆盖新的并发策略。
#[test]
fn manual_cancel_preempts_timeout_at_deadline() {
    let mut clock = DeterministicClock::new();
    let start = clock.now();
    let deadline = Deadline::with_timeout(start, Duration::from_millis(10));
    let cancellation = Cancellation::new();

    // 将时间推进到截止点，形成与手动取消的竞态环境。
    clock.advance(Duration::from_millis(10));
    assert!(deadline.is_expired(clock.now()), "推进后应立即超时");

    // 手动取消必须首先成功，代表外部主动放弃任务。
    assert!(
        cancellation.cancel(),
        "手动取消是首次触发，应返回 true 以表示状态切换"
    );

    // 超时检查必须尊重已取消状态，避免重复抢占。
    let timeout_effect = if !cancellation.is_cancelled() && deadline.is_expired(clock.now()) {
        cancellation.cancel()
    } else {
        false
    };
    assert!(
        !timeout_effect,
        "当手动取消已生效时，超时逻辑不应再次成功取消"
    );
}
