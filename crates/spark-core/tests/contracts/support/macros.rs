use std::fmt::Debug;

/// 合约测试公共断言宏：验证可重入调用的幂等语义。
///
/// # 设计动机（Why）
/// - 合约测试强调“幂等契约”，例如取消标记或预算消费需要在重试/失败注入下保持状态一致。
/// - 通过统一的宏模板，确保每个测试都显式验证“首次调用 vs. 重复调用”的结果差异，避免遗漏。
///
/// # 使用方式（How）
/// ```rust
/// use spark_core::contract::Cancellation;
///
/// let token = Cancellation::new();
/// assert_idempotent_effect! {
///     action = || token.cancel(),
///     first => true,
///     second => false,
/// };
/// ```
/// - `action`：传入零捕获或捕获环境的闭包，宏内部按顺序调用两次。
/// - `first`：首次执行期望值。
/// - `second`：重复执行期望值，用于验证幂等退化。
///
/// # 契约说明（What）
/// - **前置条件**：闭包每次调用都应符合业务允许的重试行为；
/// - **后置条件**：宏会断言两次返回值分别等于 `first`、`second`，否则测试失败。
///
/// # 权衡说明（Trade-offs）
/// - 为保持灵活性，不限制返回类型，只要实现 [`Debug`] + `PartialEq`。
/// - 若需验证更多次调用，可在测试中再次调用该宏或手动扩展断言。
macro_rules! assert_idempotent_effect {
    (
        action = $action:expr,
        first => $first:expr,
        second => $second:expr $(,)?
    ) => {{
        let first_result = ($action)();
        let _: &dyn Debug = &first_result;
        assert_eq!(first_result, $first, "首次调用未达到契约期望");

        let second_result = ($action)();
        let _: &dyn Debug = &second_result;
        assert_eq!(second_result, $second, "重复调用未保持幂等语义");
    }};
}

pub(crate) use assert_idempotent_effect;

/// 合约测试公共断言宏：检查预算决策的快照一致性。
///
/// # 设计动机（Why）
/// - 预算控制的核心契约是：`Granted`/`Exhausted` 必须伴随正确的剩余额度与上限快照。
/// - 单独断言剩余额度、上限、预算类型会导致样板代码，宏化有助于集中维护。
///
/// # 使用方式（How）
/// ```rust
/// use spark_core::types::{Budget, BudgetDecision, BudgetKind};
///
/// let budget = Budget::new(BudgetKind::Flow, 10);
/// let decision = budget.try_consume(4);
/// assert_budget_decision! {
///     decision,
///     Granted,
///     kind: BudgetKind::Flow,
///     remaining: 6,
///     limit: 10,
/// };
/// ```
/// - `decision`：被验证的 [`BudgetDecision`] 实例；
/// - `Granted`/`Exhausted`：期望的枚举变体名称；
/// - `kind`：期望的 [`BudgetKind`]，可直接传入字面量或表达式；
/// - `remaining`：期望剩余额度；
/// - `limit`：期望预算上限。
///
/// # 契约说明（What）
/// - **前置条件**：预算决策来源于同一个 [`Budget`]；
/// - **后置条件**：宏会分别断言预算类型、剩余额度、上限均与期望一致。
///
/// # 设计取舍与风险（Trade-offs）
/// - 宏内部克隆期望 `BudgetKind`，以避免调用方需要提前绑定变量；
/// - 若未来 `BudgetSnapshot` 扩展字段，可在此集中增强断言逻辑，保持测试一致性。
macro_rules! assert_budget_decision {
    (
        $decision:expr,
        $variant:ident,
        kind: $kind:expr,
        remaining: $remaining:expr,
        limit: $limit:expr $(,)?
    ) => {{
        match $decision {
            spark_core::types::BudgetDecision::$variant { snapshot } => {
                let expected_kind = $kind;
                assert_eq!(snapshot.kind(), &expected_kind, "预算种类不符合契约");
                assert_eq!(snapshot.remaining(), $remaining, "预算剩余量不符合契约");
                assert_eq!(snapshot.limit(), $limit, "预算上限不符合契约");
            }
            other => panic!(
                "预算决策枚举不匹配：期待 {:?}，实际 {:?}",
                stringify!($variant),
                other
            ),
        }
    }};
}

pub(crate) use assert_budget_decision;

/// 合约测试公共断言宏：验证时间窗口的超时语义。
///
/// # 设计动机（Why）
/// - 截止时间相关的契约强调“在截止前不会误判超时，截止后必然视为超时”。
/// - 通过宏传入一组时间点，可快速覆盖边界条件（等于、略早、略晚）。
///
/// # 使用方式（How）
/// ```rust
/// use core::time::Duration;
/// use spark_core::runtime::MonotonicTimePoint;
/// use spark_core::contract::Deadline;
///
/// let now = MonotonicTimePoint::from_offset(Duration::from_secs(5));
/// let later = MonotonicTimePoint::from_offset(Duration::from_secs(10));
/// let deadline = Deadline::at(later);
/// assert_deadline_window! {
///     deadline,
///     before: now => false,
///     at: later => true,
///     after: MonotonicTimePoint::from_offset(Duration::from_secs(11)) => true,
/// };
/// ```
/// - `before`/`at`/`after`：提供代表性的时间点与期望布尔值，可按需省略某些键。
///
/// # 契约说明（What）
/// - **前置条件**：所有时间点必须来自同一计时源；
/// - **后置条件**：宏会逐项调用 [`Deadline::is_expired`] 并断言结果。
///
/// # 风险与扩展（Trade-offs）
/// - 为保持灵活性，键值是可选的：若未提供某个键，宏不会执行对应断言。
/// - 若未来需要更多边界点，可扩展为 `strict_before`、`long_after` 等语义。
macro_rules! assert_deadline_window {
    (
        $deadline:expr,
        $(before: $before:expr => $before_expected:expr,)?
        $(at: $at:expr => $at_expected:expr,)?
        $(after: $after:expr => $after_expected:expr,)?
    ) => {{
        $(
            assert_eq!(
                $deadline.is_expired($before),
                $before_expected,
                "截止前的时间点被误判为超时"
            );
        )?
        $(
            assert_eq!(
                $deadline.is_expired($at),
                $at_expected,
                "截止点本身的超时判断不符合契约"
            );
        )?
        $(
            assert_eq!(
                $deadline.is_expired($after),
                $after_expected,
                "截止后的时间点未被视为超时"
            );
        )?
    }};
}

pub(crate) use assert_deadline_window;
