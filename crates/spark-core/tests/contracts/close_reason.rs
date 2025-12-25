use spark_core::contract::CloseReason;

/// 验证关闭原因在构造后能准确返回关闭码与描述，保证优雅关闭语义。
///
/// # 测试目标（Why）
/// - 确保 `CloseReason::new` 不会篡改输入字符串，便于在审计与对端协商中使用；
/// - 验证结构支持 `Clone`，用于多方传播关闭原因。
///
/// # 测试步骤（How）
/// 1. 使用自定义关闭码与描述构造实例；
/// 2. 调用访问器断言返回值；
/// 3. 克隆实例并再次验证，确保值语义保持一致。
///
/// # 输入/输出契约（What）
/// - **前置条件**：输入字符串满足稳定命名（如 `namespace.reason`）；
/// - **后置条件**：原始与克隆实例的访问器均返回相同值；
/// - **风险提示**：若字符串被截断或拷贝异常，会导致对端无法识别关闭原因。
#[test]
fn close_reason_preserves_code_and_message() {
    let reason = CloseReason::new("spark.transport.shutdown", "peer requested drain");
    assert_eq!(reason.code(), "spark.transport.shutdown");
    assert_eq!(reason.message(), "peer requested drain");

    let cloned = reason.clone();
    assert_eq!(cloned.code(), reason.code());
    assert_eq!(cloned.message(), reason.message());
}
