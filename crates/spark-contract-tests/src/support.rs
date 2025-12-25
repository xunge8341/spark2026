use parking_lot::Mutex;
use spark_core::runtime::MonotonicTimePoint;
use spark_core::security::{
    IdentityDescriptor, IdentityKind, PolicyEffect, PolicyRule, ResourcePattern, SecurityPolicy,
    SubjectMatcher,
};
use spark_core::types::{BudgetKind, BudgetSnapshot};
use std::fmt::Write;
use std::panic;
use std::sync::Arc;
use std::time::Duration;

/// 在附加上下文的情况下重新抛出 panic。
///
/// # 教案式说明
/// - **意图 (Why)**：`case::run_suite` 捕获 panic 后，需要在原始 payload 之上追加“套件/用例”描述，
///   帮助调试者快速定位失败来源。
/// - **逻辑 (How)**：尝试将 payload 解析为 `&str` / `String` / 任意 `Any`，在格式化文本后通过
///   [`panic::resume_unwind`] 保留原始栈信息。
/// - **契约 (What)**：
///   - **输入**：`suite`、`case` 均为人类可读名称；`payload` 为原始 panic 载荷；
///   - **前置条件**：调用前必须处于 `catch_unwind` 的错误分支中；
///   - **后置条件**：函数不会正常返回，而是带上下文的 panic。
/// - **权衡 (Trade-offs)**：对 payload 进行字符串化可能产生一次小规模分配，但换取了显著的排查效率提升。
pub fn panic_with_context(suite: &str, case: &str, payload: Box<dyn std::any::Any + Send>) -> ! {
    let mut message = String::new();
    let _ = write!(&mut message, "[spark-tck::{suite}::{case}] 测试失败：");

    if let Some(text) = payload.downcast_ref::<&str>() {
        let _ = write!(&mut message, "{text}");
    } else if let Some(text) = payload.downcast_ref::<String>() {
        let _ = write!(&mut message, "{text}");
    } else {
        let _ = write!(&mut message, "<未知 panic 类型>");
    }

    panic::resume_unwind(Box::new(message));
}

/// 构造统一的单调时钟点，避免测试中硬编码时间差。
///
/// # 设计动机（Why）
/// - 多个测试需要创建相同来源的 [`MonotonicTimePoint`]，若直接在各处使用 `Duration::new`
///   容易引入溢出或语义不一致；集中封装便于维护。
///
/// # 使用方法（How）
/// - 传入秒与纳秒部分，函数内部调用 [`MonotonicTimePoint::from_offset`]，不包含额外逻辑。
///
/// # 契约（What）
/// - **输入**：`secs` 表示秒，`nanos` 为纳秒补偿；
/// - **前置条件**：需满足 `Duration::new` 的数值范围；
/// - **后置条件**：返回的时间点可直接用于 `Deadline::with_timeout` 等 API。
pub fn monotonic(secs: u64, nanos: u32) -> MonotonicTimePoint {
    MonotonicTimePoint::from_offset(Duration::new(secs, nanos))
}

/// 构造测试专用的身份描述。
///
/// # 设计目的（Why）
/// - `SecurityContextSnapshot` 相关测试需要合法身份，集中生成可复用的样例。
///
/// # 契约（What）
/// - **输入**：`name` 为服务名片段，会附加到固定 authority；
/// - **后置条件**：返回的 [`IdentityDescriptor`] 可直接注入安全上下文。
pub fn build_identity(name: &str) -> IdentityDescriptor {
    IdentityDescriptor::new(
        "spiffe://tests.spark2026".to_string(),
        format!("service/{name}"),
        IdentityKind::Service,
    )
}

/// 构造仅包含一条“允许全部”规则的安全策略。
///
/// # 设计目的（Why）
/// - 多数测试只需最小权限集合即可通过 `ensure_secure` 校验，无需搭建完整 ACL。
///
/// # 契约（What）
/// - **输入**：`policy_id` 为策略标识；
/// - **后置条件**：策略包含一条 `Allow` 规则，可直接绑定至安全上下文。
pub fn build_allow_all_policy(policy_id: &str) -> SecurityPolicy {
    let rule = PolicyRule::new(
        vec![SubjectMatcher::Any],
        vec![ResourcePattern::new("service".to_string()).add_action("*".to_string())],
        PolicyEffect::Allow,
    );
    SecurityPolicy::new(policy_id.to_string(), vec![rule])
}

/// 构造自定义预算种类，并返回底层 `Arc<str>`，便于测试引用计数。
///
/// # 设计目的（Why）
/// - 背压测试需验证 `BudgetKind::custom` 克隆后是否共享名称；集中封装避免重复代码。
///
/// # 契约（What）
/// - **输入**：`name` 为预算命名；
/// - **输出**：`(BudgetKind, Arc<str>)`，测试可使用 `Arc::strong_count` 等 API 校验。
pub fn build_custom_budget_kind(name: &str) -> (BudgetKind, Arc<str>) {
    let arc_name: Arc<str> = Arc::from(name);
    (BudgetKind::custom(Arc::clone(&arc_name)), arc_name)
}

/// 创建线程安全的 `Vec` 收集器，供需要跨线程记录事件的测试使用。
///
/// # 设计动机（Why）
/// - 热插拔与错误处理用例需要观察控制器回调，为避免重复定义结构，提供统一封装。
///
/// # 契约（What）
/// - 返回 `Arc<Mutex<Vec<T>>>`，其中 `Mutex` 采用 `parking_lot`，在测试场景下拥有更好的诊断信息与性能。
pub fn shared_vec<T>() -> Arc<Mutex<Vec<T>>> {
    Arc::new(Mutex::new(Vec::new()))
}

/// 简化 `BudgetDecision` 断言：验证种类、剩余与上限。
///
/// # 设计目的（Why）
/// - 原始仓库使用宏完成多字段断言，迁移到 crate 后使用函数保持语义一致、便于复用。
///
/// # 契约（What）
/// - `decision` 必须匹配期望种类与配额，若不满足将触发 panic。
pub fn assert_budget_decision(
    decision: &BudgetSnapshot,
    expected_kind: &BudgetKind,
    remaining: u64,
    limit: u64,
) {
    assert_eq!(decision.kind(), expected_kind, "预算种类不匹配");
    assert_eq!(decision.remaining(), remaining, "预算剩余额度不匹配");
    assert_eq!(decision.limit(), limit, "预算上限不匹配");
}
