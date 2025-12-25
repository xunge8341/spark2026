use spark_core::types::{Budget, BudgetDecision, BudgetKind, BudgetSnapshot};
use std::sync::Arc;

/// 验证预算消费与归还的核心契约：首次消耗成功、超额请求返回耗尽、归还后额度恢复。
///
/// # 测试目标（Why）
/// - 确保 `try_consume` 对于足额请求返回 `Granted`，超额请求返回 `Exhausted` 且不改变剩余额度；
/// - 验证 `refund` 在异常回滚场景下能够正确恢复额度，且不会超过上限。
///
/// # 测试步骤（How）
/// 1. 构造上限为 10 的 Flow 预算；
/// 2. 消耗 4 单位资源，检查返回 `Granted` 且剩余额度为 6；
/// 3. 再次尝试消耗 7，预期返回 `Exhausted` 且剩余仍为 6；
/// 4. 归还 2 单位，验证余额恢复到 8 并可继续消费。
///
/// # 输入/输出契约（What）
/// - **前置条件**：预算初始额度为 10；
/// - **后置条件**：经过消费与归还后剩余额度为 8，所有决策快照保持一致；
/// - **风险提示**：若超额消费后仍减少额度，将导致背压策略被破坏。
#[test]
fn budget_try_consume_and_refund_contract() {
    let budget = Budget::new(BudgetKind::Flow, 10);

    let first = budget.try_consume(4);
    assert_budget_decision! {
        first,
        Granted,
        kind: BudgetKind::Flow,
        remaining: 6,
        limit: 10,
    };

    let exceeded = budget.try_consume(7);
    assert_budget_decision! {
        exceeded,
        Exhausted,
        kind: BudgetKind::Flow,
        remaining: 6,
        limit: 10,
    };

    assert_idempotent_effect! {
        action = || budget.try_consume(7),
        first => BudgetDecision::Exhausted {
            snapshot: BudgetSnapshot::new(BudgetKind::Flow, 6, 10),
        },
        second => BudgetDecision::Exhausted {
            snapshot: BudgetSnapshot::new(BudgetKind::Flow, 6, 10),
        },
    };

    budget.refund(2);
    assert_eq!(budget.remaining(), 8, "归还后剩余额度应恢复到 8");

    let resumed = budget.try_consume(3);
    assert_budget_decision! {
        resumed,
        Granted,
        kind: BudgetKind::Flow,
        remaining: 5,
        limit: 10,
    };
}

/// 验证克隆后的预算共享同一剩余原子值以及自定义预算标识。
///
/// # 测试目标（Why）
/// - 确保 `Budget::clone` 不会复制内部计数器，避免多副本状态不一致；
/// - 检查 `BudgetKind::custom` 的 `Arc<str>` 指针在克隆后仍共享引用，防止日志/指标枚举失真。
///
/// # 测试步骤（How）
/// 1. 使用辅助函数创建自定义预算类型及其 `Arc`；
/// 2. 构造预算并克隆一份；
/// 3. 通过克隆副本消耗额度，确认原始预算剩余同步减少；
/// 4. 对比 `Arc::ptr_eq`，验证预算类型共享同一字符串引用。
///
/// # 输入/输出契约（What）
/// - **前置条件**：预算上限设为 20，克隆次数为 1；
/// - **后置条件**：两份预算在消费后剩余均为 5，`Arc` 强引用计数为 2；
/// - **风险提示**：若克隆创建独立剩余值，将导致多线程背压判断失效。
#[test]
fn budget_clone_shares_state_and_kind_arc() {
    let (builder_kind, arc_name) = super::support::build_custom_budget_kind("alloc.segment");
    let budget = Budget::new(builder_kind, 20);
    let cloned = budget.clone();

    assert!(
        Arc::ptr_eq(
            match budget.kind() {
                BudgetKind::Custom(name) => name,
                _ => panic!("预算类型应为自定义"),
            },
            &arc_name,
        ),
        "原始预算应共享同一 Arc 名称"
    );
    assert!(
        Arc::ptr_eq(
            match cloned.kind() {
                BudgetKind::Custom(name) => name,
                _ => panic!("克隆预算类型应为自定义"),
            },
            &arc_name,
        ),
        "克隆预算也应指向相同 Arc 名称"
    );

    let decision = cloned.try_consume(15);
    assert_budget_decision! {
        decision,
        Granted,
        kind: BudgetKind::custom(Arc::clone(&arc_name)),
        remaining: 5,
        limit: 20,
    };

    assert_eq!(
        budget.remaining(),
        5,
        "克隆副本消费后原预算剩余应同步更新"
    );
    assert_eq!(Arc::strong_count(&arc_name), 3, "Arc 引用计数应统计两份预算 + 测试持有者");
}

/// 验证预算快照 API 能准确反映当前状态且独立于后续变化。
///
/// # 测试目标（Why）
/// - 确保 `snapshot()` 捕获的剩余与上限符合当前状态，用于日志与指标输出；
/// - 证明快照为值拷贝，即后续消费不会影响已获取的快照。
///
/// # 测试步骤（How）
/// 1. 构造预算并消耗部分额度；
/// 2. 调用 `snapshot()` 获取快照并断言字段；
/// 3. 再次消费预算，验证原快照保持旧值。
///
/// # 输入/输出契约（What）
/// - **前置条件**：预算上限为 5，首次消费 2；
/// - **后置条件**：快照记录剩余 3，后续消费不影响该快照；
/// - **风险提示**：若快照持有引用而非值拷贝，将导致监控数据随时间漂移。
#[test]
fn budget_snapshot_captures_current_state() {
    let budget = Budget::new(BudgetKind::Decode, 5);
    let _ = budget.try_consume(2);
    let snapshot = budget.snapshot();

    assert_eq!(snapshot.remaining(), 3, "快照应记录消耗后的剩余");
    assert_eq!(snapshot.limit(), 5, "快照应保留原始上限");
    assert_eq!(snapshot.kind(), &BudgetKind::Decode);

    let _ = budget.try_consume(1);
    assert_eq!(snapshot.remaining(), 3, "历史快照不应受后续消费影响");
}
