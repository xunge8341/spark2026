use crate::case::{TckCase, TckSuite};
use spark_core::contract::Cancellation;

const CASES: &[TckCase] = &[TckCase {
    name: "cancellation_idempotency_and_propagation",
    test: cancellation_idempotency_and_propagation,
}];

const SUITE: TckSuite = TckSuite {
    name: "cancellation",
    cases: CASES,
};

/// 返回“取消”主题的测试套件。
///
/// # 教案式说明
/// - **意图 (Why)**：集中验证 `Cancellation` 在幂等调用与父子传播下的语义是否稳定。
/// - **逻辑 (How)**：套件目前包含单用例，用于覆盖最核心的状态机行为。
/// - **契约 (What)**：返回 `'static` 引用供宏或手动调用使用。
pub const fn suite() -> &'static TckSuite {
    &SUITE
}

/// 验证取消原语的幂等性与子令牌传播。
///
/// # 教案式说明
/// - **意图 (Why)**：防止多次取消导致状态震荡，确保子令牌能立即反映父级状态。
/// - **逻辑 (How)**：先验证父令牌默认未取消，再检查首次取消返回 `true`、重复取消返回 `false`，最后通过 `child()` 观察共享状态。
/// - **契约 (What)**：若取消逻辑正确，父子令牌最终都处于取消态且重复调用保持幂等。
fn cancellation_idempotency_and_propagation() {
    let token = Cancellation::new();
    assert!(!token.is_cancelled(), "默认构造后应处于未取消态");

    assert!(token.cancel(), "首次取消应返回 true");
    assert!(!token.cancel(), "重复取消必须保持幂等（返回 false）");

    let child = token.child();
    assert!(child.is_cancelled(), "子令牌应立即观察到父令牌的取消状态");
    assert!(!child.cancel(), "子令牌重复取消也应保持幂等");
}
