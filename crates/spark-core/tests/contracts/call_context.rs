use spark_core::contract::{
    Budget, BudgetKind, CallContext, Cancellation, Deadline, ObservabilityContract,
    SecurityContextSnapshot,
};
use spark_core::context::Context;

/// 验证构建器在未提供预算时会自动注入无限 Flow 预算，满足幂等契约。
///
/// # 测试目标（Why）
/// - 确保调用方即便未显式设置预算，也能在后续链路中读取到默认的 Flow 预算；
/// - 验证默认预算为无限额度，避免因缺省导致过度限流。
///
/// # 测试步骤（How）
/// 1. 使用 `CallContext::builder()` 构建上下文但不添加预算；
/// 2. 读取 `budget(BudgetKind::Flow)` 并检查 `limit`、`remaining`；
/// 3. 通过 `budgets()` 迭代确保仅存在一条默认预算。
///
/// # 输入/输出契约（What）
/// - **前置条件**：无显式预算输入；
/// - **后置条件**：上下文持有单个 Flow 预算，额度为 `u64::MAX`；
/// - **风险提示**：若默认预算缺失，依赖背压语义的组件将出现 `None`。
#[test]
fn call_context_injects_default_flow_budget() {
    let ctx = CallContext::builder().build();

    let flow_budget = ctx
        .budget(&BudgetKind::Flow)
        .expect("默认应提供 Flow 预算");
    assert_eq!(flow_budget.limit(), u64::MAX, "默认预算应为无限上限");
    assert_eq!(flow_budget.remaining(), u64::MAX, "默认预算剩余也应为无限");
    assert_eq!(ctx.budgets().count(), 1, "默认仅注入一条预算");
}

/// 验证构建器在提供取消、截止、预算、安全、可观测性信息时均能正确透传，并与执行视图保持一致。
///
/// # 测试目标（Why）
/// - 确保 `CallContext` 保留调用方提供的所有信息，便于跨模块共享；
/// - 验证 `Context` 视图引用同一取消原语与预算切片，保持零拷贝；
/// - 检查安全与可观测性契约在构建后可通过访问器读取。
///
/// # 测试步骤（How）
/// 1. 准备取消令牌、截止时间、自定义预算、安全上下文、可观测性契约；
/// 2. 使用构建器逐项注入并构建 `CallContext`；
/// 3. 断言访问器返回值与输入一致，并通过取消状态验证引用共享；
/// 4. 通过 `execution()` 获取只读视图，确保五元元数据（取消/截止/预算/追踪/身份）与原始上下文同步。
///
/// # 输入/输出契约（What）
/// - **前置条件**：调用方提供的各项输入均有效；
/// - **后置条件**：`CallContext` 与 `Context` 均可读取到相同的数据；
/// - **风险提示**：若引用未共享，将导致视图读取到过期或不一致的状态。
#[test]
fn call_context_preserves_inputs_and_execution_view() {
    let cancellation = Cancellation::new();
    let deadline = Deadline::with_timeout(super::support::monotonic(100, 0), std::time::Duration::from_secs(5));
    let budget_kind = BudgetKind::Decode;
    let budget = Budget::new(budget_kind.clone(), 8);
    let security = SecurityContextSnapshot::default()
        .with_identity(super::support::build_identity("catalog"))
        .with_policy(super::support::build_allow_all_policy("policy-call"));
    let observability = ObservabilityContract::new(
        &["metric.a"],
        &["field.a"],
        &["trace.a"],
        &["audit.a"],
    );

    let ctx = CallContext::builder()
        .with_cancellation(cancellation.clone())
        .with_deadline(deadline)
        .add_budget(budget.clone())
        .with_security(security.clone())
        .with_observability(observability)
        .build();

    assert!(
        !ctx.cancellation().is_cancelled(),
        "构建后的上下文默认应处于未取消状态"
    );
    assert!(
        !cancellation.is_cancelled(),
        "原始令牌初始也应未被取消"
    );
    cancellation.cancel();
    assert!(
        ctx.cancellation().is_cancelled(),
        "原始令牌取消后上下文应同步观察到"
    );
    assert_eq!(ctx.deadline(), deadline, "截止时间应保持不变");

    let fetched_budget = ctx
        .budget(&budget_kind)
        .expect("应能按种类检索预算");
    assert_eq!(fetched_budget.limit(), 8);
    assert_eq!(fetched_budget.remaining(), 8);

    let security_ref = ctx.security();
    assert!(security_ref.identity().is_some(), "应保留主体身份");
    assert!(security_ref.policy().is_some(), "应保留策略引用");

    let observability_ref = ctx.observability();
    assert_eq!(observability_ref.metric_names(), &["metric.a"]);

    let exec: Context<'_> = ctx.execution();
    assert!(
        exec.cancellation().is_cancelled(),
        "执行视图应共享取消状态"
    );
    assert_eq!(exec.deadline(), deadline, "执行视图的截止时间应一致");
    let exec_budget = exec
        .budget(&budget_kind)
        .expect("执行视图也应能找到预算");
    assert_eq!(exec_budget.limit(), 8);

    assert_eq!(
        exec.budgets().count(),
        ctx.budgets().count(),
        "执行视图预算迭代数量应与上下文一致"
    );
}
