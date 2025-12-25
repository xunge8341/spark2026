use spark_core::contract::Cancellation;

/// 验证取消原语的幂等语义与子令牌一致性。
///
/// # 测试目标（Why）
/// - 保证 `Cancellation` 在重试场景下只会触发一次状态切换；
/// - 校验通过 `child` 派生的令牌能够观察到父令牌的状态并保持共享原子位。
///
/// # 测试步骤（How）
/// 1. 创建默认取消令牌，断言初始状态未被取消；
/// 2. 使用 [`assert_idempotent_effect!`] 检查首次 `cancel()` 返回 `true`，重复调用返回 `false`；
/// 3. 派生子令牌并验证其立即观测到已取消状态，且再次调用 `cancel()` 不会破坏幂等契约。
///
/// # 输入/输出契约（What）
/// - **前置条件**：无显式输入参数，使用默认构造；
/// - **后置条件**：父子令牌均处于已取消状态，重复取消保持 `false`；
/// - **风险提示**：若原子共享失效，将导致子令牌状态不同步，破坏跨模块终止语义。
#[test]
fn cancellation_idempotency_and_propagation() {
    let token = Cancellation::new();
    assert!(
        !token.is_cancelled(),
        "默认构造后应处于未取消态"
    );

    assert_idempotent_effect! {
        action = || token.cancel(),
        first => true,
        second => false,
    };

    assert!(token.is_cancelled(), "父令牌必须反映取消状态");

    let child = token.child();
    assert!(child.is_cancelled(), "子令牌应共享取消标记");
    assert!(
        !child.cancel(),
        "子令牌重复取消也应保持幂等（返回 false）"
    );
}
