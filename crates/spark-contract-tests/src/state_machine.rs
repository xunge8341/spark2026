use crate::case::{TckCase, TckSuite};
use crate::support::{build_allow_all_policy, build_identity, monotonic};
use spark_core::context::Context;
use spark_core::{
    contract::{
        CallContext, Cancellation, Deadline, ObservabilityContract, SecurityContextSnapshot,
    },
    types::{Budget, BudgetKind},
};
use std::time::Duration;

const CASES: &[TckCase] = &[
    TckCase {
        name: "call_context_injects_default_flow_budget",
        test: call_context_injects_default_flow_budget,
    },
    TckCase {
        name: "call_context_preserves_inputs_and_execution_view",
        test: call_context_preserves_inputs_and_execution_view,
    },
];

const SUITE: TckSuite = TckSuite {
    name: "state_machine",
    cases: CASES,
};

/// 返回“状态机”主题的测试套件。
///
/// # 教案式说明
/// - **意图 (Why)**：验证 `CallContext` 构建器与执行视图在缺省预算与数据传播上的行为。
/// - **逻辑 (How)**：提供两个用例分别覆盖默认预算注入与自定义字段复制。
/// - **契约 (What)**：返回 `'static` 引用供宏调用。
pub const fn suite() -> &'static TckSuite {
    &SUITE
}

/// 验证构建器在未显式添加预算时会注入无限 Flow 预算。
///
/// # 教案式说明
/// - **意图 (Why)**：确保调用方未配置预算时仍具备背压控制能力。
/// - **逻辑 (How)**：构建默认 `CallContext` 并检查 `budget()` 与迭代结果。
/// - **契约 (What)**：仅存在一条 Flow 预算，上限与剩余为 `u64::MAX`。
fn call_context_injects_default_flow_budget() {
    let ctx = CallContext::builder().build();
    let flow_budget = ctx.budget(&BudgetKind::Flow).expect("默认应提供 Flow 预算");
    assert_eq!(flow_budget.limit(), u64::MAX);
    assert_eq!(flow_budget.remaining(), u64::MAX);
    assert_eq!(ctx.budgets().count(), 1);
}

/// 验证构建器能正确保留取消、截止、预算、安全与可观测性字段，并在执行视图中反映。
///
/// # 教案式说明
/// - **意图 (Why)**：`CallContext` 是跨层状态机核心，需保证字段复制与引用共享无误。
/// - **逻辑 (How)**：注入多种输入后检查上下文与执行视图的字段值，特别关注取消传播与预算引用。
/// - **契约 (What)**：执行视图与原上下文的取消状态、截止时间、预算数量完全一致。
fn call_context_preserves_inputs_and_execution_view() {
    let cancellation = Cancellation::new();
    let deadline = Deadline::with_timeout(monotonic(100, 0), Duration::from_secs(5));
    let budget_kind = BudgetKind::Decode;
    let budget = Budget::new(budget_kind.clone(), 8);
    let security = SecurityContextSnapshot::default()
        .with_identity(build_identity("catalog"))
        .with_policy(build_allow_all_policy("policy-call"));
    let observability =
        ObservabilityContract::new(&["metric.a"], &["field.a"], &["trace.a"], &["audit.a"]);

    let ctx = CallContext::builder()
        .with_cancellation(cancellation.clone())
        .with_deadline(deadline)
        .add_budget(budget.clone())
        .with_security(security.clone())
        .with_observability(observability)
        .build();

    assert!(!ctx.cancellation().is_cancelled());
    assert!(!cancellation.is_cancelled());
    cancellation.cancel();
    assert!(ctx.cancellation().is_cancelled());
    assert_eq!(ctx.deadline(), deadline);

    let fetched_budget = ctx.budget(&budget_kind).expect("应能按种类检索预算");
    assert_eq!(fetched_budget.limit(), 8);

    let exec: Context<'_> = ctx.execution();
    assert!(exec.cancellation().is_cancelled());
    assert_eq!(exec.deadline(), deadline);
    let exec_budget = exec.budget(&budget_kind).expect("执行视图也应能找到预算");
    assert_eq!(exec_budget.limit(), 8);
    assert_eq!(exec.budgets().count(), ctx.budgets().count());
}
