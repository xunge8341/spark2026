use std::time::Duration;

use spark_core::contract::{Cancellation, Deadline};

use super::support::{DeterministicClock, Lcg64};

/// 验证取消与截止组合在确定性种子下的重放一致性。
///
/// # 测试目标（Why）
/// - 保证 T15 要求的“100 次重放一致”在取消/超时交错场景成立；
/// - 确认通过固定种子驱动的伪随机调度不会引入全局状态，从而在 CI 中稳定复现。
///
/// # 测试步骤（How）
/// 1. 使用 `Lcg64` 与 `DeterministicClock` 组合生成事件序列；
/// 2. 在每一步推进时钟并随机尝试手动取消；
/// 3. 若手动取消未触发且截止已过，则触发一次超时取消；
/// 4. 重复执行 100 次并比较事件序列，确保完全相同。
///
/// # 输入/输出契约（What）
/// - **前置条件**：种子固定为常量，所有运行共享同一基础状态；
/// - **后置条件**：每次运行至少触发一次取消事件（手动或超时），且事件序列与基线一致；
/// - **风险提示**：如未来修改 LCG 常量或截止策略需同步更新基线，避免误判。
#[test]
fn timeout_cancel_seed_replay_is_stable() {
    const SEED: u64 = 0xACED_BEEF_u64;
    const REPLAY: usize = 100;

    let baseline = execute_replay(SEED);
    assert!(
        baseline
            .iter()
            .any(|event| matches!(event.kind, ReplayEventKind::ManualCancel | ReplayEventKind::TimeoutCancel)),
        "基线序列至少需要包含一次取消事件"
    );

    for iteration in 0..REPLAY {
        let replay = execute_replay(SEED);
        assert_eq!(
            replay, baseline,
            "第 {iteration} 次重放出现偏差：期望 {baseline:?}，实际 {replay:?}"
        );
    }
}

/// 基于确定性时钟执行一次调度重放，返回事件轨迹。
fn execute_replay(seed: u64) -> Vec<ReplayEvent> {
    let mut rng = Lcg64::new(seed);
    let mut clock = DeterministicClock::new();
    let cancellation = Cancellation::new();
    let start = clock.now();
    let deadline = Deadline::with_timeout(start, Duration::from_millis(30));
    let mut events = Vec::new();

    for step in 1..=32 {
        // 通过可复现的抖动推进时钟，确保最终会跨越截止点。
        let delta_ms = 1 + rng.next_in_range(3);
        let now = clock.advance(Duration::from_millis(delta_ms));
        events.push(ReplayEvent::new(step, ReplayEventKind::Advance(now.as_duration())));

        if !cancellation.is_cancelled() {
            let manual_trigger = rng.next_in_range(5) == 0;
            if manual_trigger && cancellation.cancel() {
                events.push(ReplayEvent::new(step, ReplayEventKind::ManualCancel));
                break;
            }
        }

        if !cancellation.is_cancelled() && deadline.is_expired(clock.now()) {
            if cancellation.cancel() {
                events.push(ReplayEvent::new(step, ReplayEventKind::TimeoutCancel));
                break;
            }
        }
    }

    events
}

/// 记录一次重放中的关键事件。
#[derive(Clone, Debug, PartialEq, Eq)]
struct ReplayEvent {
    step: usize,
    kind: ReplayEventKind,
}

impl ReplayEvent {
    fn new(step: usize, kind: ReplayEventKind) -> Self {
        Self { step, kind }
    }
}

/// 重放事件类型，区分推进、手动取消与超时取消。
#[derive(Clone, Debug, PartialEq, Eq)]
enum ReplayEventKind {
    Advance(Duration),
    ManualCancel,
    TimeoutCancel,
}
