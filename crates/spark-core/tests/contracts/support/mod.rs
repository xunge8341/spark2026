//! 合约测试支撑模块，集中维护公共宏与构造器，确保测试语义一致。
//!
//! # 模块定位（Why）
//! - T13 任务要求“按稳定面提供合约测模板与断言集合”，因此将幂等、时间窗口等通用断言抽象为宏，
//!   降低各测试文件的重复代码；
//! - 同时提供构造辅助函数，避免在测试中重复拼装 `IdentityDescriptor`、`SecurityPolicy`、`MonotonicTimePoint` 等结构。
//!
//! # 使用说明（How）
//! - 通过 `pub(crate) use` 将宏与辅助函数导出给同目录下的测试模块；
//! - 宏文件位于 `support/macros.rs`，若未来新增断言语义，只需在该文件扩展即可；
//! - 构造器函数保持最小依赖，确保 `no_std + alloc` 的编译条件下也能复用。
//!
//! # 契约说明（What）
//! - **前置条件**：测试调用方需确保传入参数满足业务契约（如时间点使用统一计时源）；
//! - **后置条件**：辅助函数返回的结构均为框架公开类型，可直接参与断言或进一步操作。
//!
//! # 风险提示（Trade-offs）
//! - 若后续核心结构字段发生变更，需要同步更新这里的构造逻辑，以免测试误判；
//! - 为保证覆盖率统计的稳定性，辅助函数不隐藏关键逻辑（如预算消费），仅负责装配测试输入。

mod r#async;
mod deterministic;
mod macros;

pub(crate) use r#async::block_on;
pub(crate) use deterministic::*;
pub(crate) use macros::*;

use spark_core::types::BudgetKind;
use spark_core::runtime::MonotonicTimePoint;
use spark_core::security::{
    IdentityDescriptor, IdentityKind, PolicyEffect, PolicyRule, ResourcePattern, SecurityPolicy,
    SubjectMatcher,
};
use std::sync::Arc;
use std::time::Duration;

/// 创建指定时间偏移的单调时钟点，确保测试时间线一致。
///
/// # 设计目的（Why）
/// - 合约测试需要模拟“当前时间/截止时间”场景，通过统一函数避免魔法数字散落各处。
///
/// # 实现说明（How）
/// - 基于 [`MonotonicTimePoint::from_offset`] 构造，接受秒与纳秒参数，便于表达精细时间点；
/// - 内部仅执行一次 `Duration::new`，保持零逻辑分支，避免干扰覆盖统计。
///
/// # 契约说明（What）
/// - **参数**：`secs` 表示秒，`nanos` 表示纳秒补充；二者组合必须符合 `Duration` 的约束；
/// - **返回值**：对应偏移的 [`MonotonicTimePoint`]；
/// - **前置条件**：调用方需确保两次调用所使用的计时源语义一致；
/// - **后置条件**：返回值可直接传入 `Deadline::with_timeout`、`Deadline::is_expired` 等 API。
///
/// # 风险提示（Trade-offs）
/// - 函数未对 `Duration::new` 的溢出进行额外包装，测试若传入极端值将直接 panic；
/// - 若未来 `MonotonicTimePoint` 构造方式调整，需要同步更新此函数以保持语义准确。
pub(crate) fn monotonic(secs: u64, nanos: u32) -> MonotonicTimePoint {
    MonotonicTimePoint::from_offset(Duration::new(secs, nanos))
}

/// 构造最小可用的身份描述，便于安全上下文相关测试。
///
/// # 设计目的（Why）
/// - 合约测试关注 `ensure_secure` 等逻辑，需要一个稳定且语义清晰的身份样例。
///
/// # 实现说明（How）
/// - 采用 `IdentityKind::Service` 作为默认类型，符合调用链常见场景；
/// - 使用 `format!` 生成名称，避免测试之间硬编码重复字符串。
///
/// # 契约说明（What）
/// - **参数**：`name` 作为服务名片段，将拼接到固定的 `authority`；
/// - **返回值**：[`IdentityDescriptor`] 实例，`version` 默认为 `None`；
/// - **前置条件**：调用方需保证 `name` 非空，以免违反身份命名规范；
/// - **后置条件**：返回对象可直接用于策略匹配或安全上下文注入。
///
/// # 风险提示（Trade-offs）
/// - 当前实现固定 `authority` 为测试命名空间，如需覆盖多命名空间场景，可根据测试需求扩展参数。
pub(crate) fn build_identity(name: &str) -> IdentityDescriptor {
    IdentityDescriptor::new(
        "spiffe://tests.spark2026".to_string(),
        format!("service/{}", name),
        IdentityKind::Service,
    )
}

/// 构建仅包含“允许全部”规则的策略，用于验证安全契约中的必备字段。
///
/// # 设计目的（Why）
/// - `SecurityContextSnapshot::ensure_secure` 需要策略存在才能通过校验，此函数提供最小化策略对象。
///
/// # 实现说明（How）
/// - 创建 `PolicyRule`，主体使用 `SubjectMatcher::Any`，资源覆盖一个带动作的通配模式；
/// - 组装为 [`SecurityPolicy`] 并以传入的标识填充 `id` 字段。
///
/// # 契约说明（What）
/// - **参数**：`policy_id` 为策略唯一标识；
/// - **返回值**：包含单条规则的 [`SecurityPolicy`]；
/// - **前置条件**：`policy_id` 应满足业务唯一性要求；
/// - **后置条件**：策略规则列表至少包含一条，足以触发上下文校验逻辑。
///
/// # 风险提示（Trade-offs）
/// - 函数默认不设置 `version` 与 `description`，如需覆盖相关分支可在测试中自行调用 `with_version` 等方法；
/// - 资源命名空间固定为 `service`，若测试涉及其他资源类别需手动修改。
pub(crate) fn build_allow_all_policy(policy_id: &str) -> SecurityPolicy {
    let rule = PolicyRule::new(
        vec![SubjectMatcher::Any],
        vec![ResourcePattern::new("service".to_string()).add_action("*".to_string())],
        PolicyEffect::Allow,
    );

    SecurityPolicy::new(policy_id.to_string(), vec![rule])
}

/// 构建自定义预算种类的引用计数器，方便测试验证 `Arc` 共享行为。
///
/// # 设计目的（Why）
/// - 某些测试需要比较 `BudgetKind::Custom` 的内部指针是否共享，此函数集中创建 `Arc<str>`，便于多处引用。
///
/// # 实现说明（How）
/// - 使用 `Arc::from` 将 `name` 转换为共享字符串，再交给 [`BudgetKind::custom`] 构造；
/// - 函数返回 `(BudgetKind, Arc<str>)`，测试可分别用于断言类型和值。
///
/// # 契约说明（What）
/// - **参数**：`name` 为预算标识；
/// - **返回值**：`(BudgetKind, Arc<str>)`，其中 `Arc` 供测试比较引用计数；
/// - **前置条件**：`name` 应遵循命名空间约定，如 `alloc.segment`；
/// - **后置条件**：返回的 `BudgetKind` 拥有独立克隆能力，可安全传递给 `Budget`。
///
/// # 风险提示（Trade-offs）
/// - 若未来 `BudgetKind::custom` 内部实现变化（例如强制小写），测试需同步调整预期结果。
pub(crate) fn build_custom_budget_kind(name: &str) -> (BudgetKind, Arc<str>) {
    let arc_name: Arc<str> = Arc::from(name);
    (BudgetKind::custom(Arc::clone(&arc_name)), arc_name)
}
