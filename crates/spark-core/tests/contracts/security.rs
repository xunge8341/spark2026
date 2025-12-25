use spark_core::contract::SecurityContextSnapshot;
use std::sync::Arc;

/// 验证安全上下文在严格安全模式下的必备字段校验逻辑。
///
/// # 测试目标（Why）
/// - 确保未设置身份或策略时，`ensure_secure` 会拒绝继续执行；
/// - 验证同时配置身份与策略后能够顺利通过安全检查；
/// - 确认 `allow_insecure` 可用于显式放宽限制。
///
/// # 测试步骤（How）
/// 1. 创建默认安全上下文并调用 `ensure_secure`，预期返回错误；
/// 2. 使用 `allow_insecure` 放宽限制，验证返回 `Ok(())`；
/// 3. 构建带身份与策略的上下文，确保校验成功并可读取字段。
///
/// # 输入/输出契约（What）
/// - **前置条件**：安全上下文初始为空；
/// - **后置条件**：配置齐全后 `ensure_secure` 返回 `Ok(())`，可通过访问器读取身份/策略；
/// - **风险提示**：若校验缺失可能导致未授权访问。
#[test]
fn security_context_requires_identity_and_policy() {
    let mut insecure = SecurityContextSnapshot::default();
    assert!(
        insecure.ensure_secure().is_err(),
        "缺少身份与策略时应拒绝继续执行"
    );

    insecure = insecure.allow_insecure();
    assert!(
        insecure.ensure_secure().is_ok(),
        "显式允许不安全模式后应跳过校验"
    );

    let identity = super::support::build_identity("order-service");
    let policy = super::support::build_allow_all_policy("policy-allow-all");
    let secure = SecurityContextSnapshot::default()
        .with_identity(identity.clone())
        .with_peer_identity(identity.clone())
        .with_policy(policy.clone());

    secure
        .ensure_secure()
        .expect("身份与策略齐备时应通过校验");

    let fetched_identity = secure.identity().expect("应返回主体身份引用");
    assert_eq!(fetched_identity.name(), identity.name());
    let fetched_peer = secure
        .peer_identity()
        .expect("应返回对端身份引用");
    assert_eq!(fetched_peer.name(), identity.name());
    let fetched_policy = secure.policy().expect("应返回策略引用");
    assert_eq!(fetched_policy.id(), policy.id());
    assert!(!secure.is_insecure_allowed(), "安全模式下不应允许降级");
}

/// 验证在携带策略描述与版本时，访问器能够准确返回引用并保持只读语义。
///
/// # 测试目标（Why）
/// - 确保 `with_policy` 构造的策略引用在上下文内部存储为 `Arc`，多次读取不会发生移动；
/// - 检查当策略带有版本与描述时，访问器仍能正常透出。
///
/// # 测试步骤（How）
/// 1. 构建策略并设置版本、描述；
/// 2. 注入到安全上下文后多次读取，验证引用一致；
/// 3. 使用 `Arc::ptr_eq` 确认内部共享引用没有复制开销。
///
/// # 输入/输出契约（What）
/// - **前置条件**：策略对象在测试作用域内保持有效；
/// - **后置条件**：多次调用 `policy()` 返回同一引用；
/// - **风险提示**：若引用被复制为独立实例，可能导致策略审计与评估不一致。
#[test]
fn security_context_exposes_policy_metadata() {
    let policy = super::support::build_allow_all_policy("policy-meta")
        .with_version("v1".to_string())
        .with_description("allow everything".to_string());
    let context = SecurityContextSnapshot::default().with_policy(policy.clone());

    let first = context.policy().expect("策略应已存在");
    let second = context.policy().expect("策略引用应可重复获取");
    assert!(Arc::ptr_eq(first, second), "策略引用应共享同一 Arc 实例");
    assert_eq!(first.version().map(|v| v.as_str()), Some("v1"));
    assert_eq!(
        first.description().map(|d| d.as_str()),
        Some("allow everything")
    );
}
