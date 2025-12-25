use crate::case::{TckCase, TckSuite};
use crate::support::{assert_budget_decision, build_custom_budget_kind};
use spark_core::types::{Budget, BudgetDecision, BudgetKind};
use spark_core::{BusyReason, ReadyState, SubscriptionBudget};
use std::sync::Arc;

const CASES: &[TckCase] = &[
    TckCase {
        name: "budget_try_consume_and_refund_contract",
        test: budget_try_consume_and_refund_contract,
    },
    TckCase {
        name: "ready_state_busy_and_budget_conversions",
        test: ready_state_busy_and_budget_conversions,
    },
];

const SUITE: TckSuite = TckSuite {
    name: "backpressure",
    cases: CASES,
};

/// 返回“背压”主题的测试套件，用于覆盖预算消费与 ReadyState 语义。
///
/// # 教案式说明
/// - **意图 (Why)**：集中验证 `Budget` 与 `ReadyState` 在消耗、回收、映射阶段的语义一致性。
/// - **逻辑 (How)**：套件包含两项用例，分别覆盖预算幂等消费与状态枚举转换。
/// - **契约 (What)**：返回 `'static` 引用，便于宏和调用方直接使用。
pub const fn suite() -> &'static TckSuite {
    &SUITE
}

/// 验证预算消费、拒绝与归还的核心契约。
///
/// # 教案式说明
/// - **意图 (Why)**：确保在高压流量下 `Budget::try_consume` 与 `refund` 行为稳定，背压信号不会误触发或遗漏。
/// - **逻辑 (How)**：依次执行“消费成功 → 超额拒绝 → 幂等拒绝 → 归还 → 再次消费”，并结合 `assert_budget_decision` 检查快照。
/// - **契约 (What)**：
///   - **前置条件**：预算初始额度 10；
///   - **后置条件**：多次消费后仍保持正确剩余，超额调用始终返回同一快照。
fn budget_try_consume_and_refund_contract() {
    let budget = Budget::new(BudgetKind::Flow, 10);

    let first = match budget.try_consume(4) {
        BudgetDecision::Granted { snapshot } => snapshot,
        other => panic!("预期首次消费成功，得到 {other:?}"),
    };
    assert_budget_decision(&first, &BudgetKind::Flow, 6, 10);

    let exhausted = match budget.try_consume(7) {
        BudgetDecision::Exhausted { snapshot } => snapshot,
        other => panic!("预期超额消费被拒绝，得到 {other:?}"),
    };
    assert_budget_decision(&exhausted, &BudgetKind::Flow, 6, 10);

    let exhausted_again = match budget.try_consume(7) {
        BudgetDecision::Exhausted { snapshot } => snapshot,
        other => panic!("幂等性失败，得到 {other:?}"),
    };
    assert_budget_decision(&exhausted_again, &BudgetKind::Flow, 6, 10);

    budget.refund(2);
    assert_eq!(budget.remaining(), 8, "归还后剩余额度应恢复");

    let resumed = match budget.try_consume(3) {
        BudgetDecision::Granted { snapshot } => snapshot,
        other => panic!("恢复后消费应成功，得到 {other:?}"),
    };
    assert_budget_decision(&resumed, &BudgetKind::Flow, 5, 10);
}

/// 验证 ReadyState 与预算快照的转换语义。
///
/// # 教案式说明
/// - **意图 (Why)**：调用方在面对 `BusyReason::QueueFull` 与 `ReadyState::BudgetExhausted` 时需获得结构化上下文，本用例确保信息未丢失。
/// - **逻辑 (How)**：构造 `QueueFull` 与自定义预算两个场景，逐一匹配枚举并检查字段。
/// - **契约 (What)**：
///   - `QueueFull` 必须保留深度与容量；
///   - 自定义预算的 `Arc<str>` 在快照中保持共享，以利于观测侧比对。
fn ready_state_busy_and_budget_conversions() {
    let queue_full = ReadyState::Busy(BusyReason::queue_full(5, 5));
    match queue_full {
        ReadyState::Busy(BusyReason::QueueFull(depth)) => {
            assert_eq!(depth.depth, 5);
            assert_eq!(depth.capacity, 5);
        }
        other => panic!("应当匹配 QueueFull，得到 {other:?}"),
    }

    let (custom_kind, arc_name) = build_custom_budget_kind("alloc.segment");
    let budget = Budget::new(custom_kind.clone(), 10);
    let _ = budget.try_consume(10);
    let exhausted_snapshot = match budget.try_consume(1) {
        BudgetDecision::Exhausted { snapshot } => snapshot,
        other => panic!("应当返回 Exhausted 快照，得到 {other:?}"),
    };
    if let BudgetKind::Custom(name) = exhausted_snapshot.kind() {
        assert!(Arc::ptr_eq(name, &arc_name), "预算种类 Arc 必须共享");
    } else {
        panic!("应当保留自定义预算种类");
    }

    let state = ReadyState::BudgetExhausted(SubscriptionBudget::from(&exhausted_snapshot));
    match state {
        ReadyState::BudgetExhausted(snapshot) => {
            assert_eq!(snapshot.limit, 10);
            assert_eq!(snapshot.remaining, 0);
        }
        other => panic!("预算耗尽应映射到 BudgetExhausted，得到 {other:?}"),
    }
}
