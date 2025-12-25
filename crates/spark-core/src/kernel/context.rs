use crate::{
    contract::{CallContext, Cancellation, Deadline},
    observability::TraceContext,
    security::IdentityDescriptor,
    types::{Budget, BudgetKind},
};
use core::slice;

/// `Context` 聚合一次执行路径上最关键的调用元数据：取消标记、截止时间、预算切片、追踪上下文与身份视图。
///
/// # 设计初衷（Why）
/// - 服务治理评审明确要求：所有对外公开的调用链必须显式携带“取消/截止/预算/身份/追踪”五元组，
///   以便在背压、超时、速率限制与身份审计之间保持一致语义；`Context` 即为这一约束的最小只读投影。
/// - `CallContext` 还包含安全、可观测性等重型信息。对于多数快速路径（如 `poll_ready`），
///   我们只需读取元数据五元组即可决策，故将其抽离为独立视图，避免强迫实现者依赖整套上下文。
///
/// # 架构定位（Role）
/// - 位于 `spark-core::context` 模块，被所有公共 Trait（`Service`、`DynService`、Pipeline `Context` 等）引用，
///   作为元数据只读访问口；
/// - 由 [`CallContext::execution`] 派生，保障创建时遵循构造器验证逻辑，不破坏 `CallContext` 的一致性。
///
/// # 关键逻辑（How）
/// - 内部仅保存对取消标记、追踪上下文与预算数组的不可变引用，以及按值拷贝的截止时间；
/// - 身份信息以 `Option<&IdentityDescriptor>` 的零拷贝视图提供，确保在无身份的匿名调用中不会强制分配；
/// - 通过 `budgets()` 返回 [`slice::Iter`]，确保零拷贝遍历；
/// - 提供 `budget()` 快速查找指定种类预算，便于在背压检查中即时读取剩余额度。
///
/// # 契约说明（What）
/// - **前置条件**：必须由 [`CallContext`] 或等价持有者构造，调用方需保证引用生命周期覆盖使用区间；
/// - **后置条件**：对外暴露的引用仅具只读语义，禁止通过别名写入预算或取消标记；
/// - **输入**：取消标记引用、截止时间副本、预算切片；
/// - **输出**：访问器 `cancellation()`、`deadline()`、`trace_context()`、`identity()`、`peer_identity()` 与 `budgets()`/`budget()`。
///
/// # 设计取舍与风险（Trade-offs）
/// - 选择存储引用而非克隆预算/追踪/身份，确保 `poll_ready` 等热路径零分配；但调用方需确保底层 `CallContext`
///   不会在视图仍存活时释放或突变；
/// - 截止时间按值拷贝，避免引用生命周期过于复杂；若未来转向更大结构，可考虑 `Cow`。
///
/// # 生命周期与线程安全
/// - `Context<'a>` 自身实现 `Copy`，可在单线程或多线程间按值复制；但引用字段要求调用方确保
///   源 [`CallContext`] 在 `'a` 生命周期内保持有效；
/// - 结构体未实现 `Send/Sync` 自动派生，是否跨线程共享取决于被借用的 `Cancellation`、`TraceContext`、
///   `IdentityDescriptor` 与 `Budget` 是否线程安全；默认场景下它们内部使用 `Arc` 或不可变结构，可安全跨线程，
///   但若上层引入自定义预算实现需重新审视线程安全约束；
/// - 若需要在线程池任务中长期持有上下文，请优先克隆 [`CallContext`] 后再派生新的 `Context`，
///   避免底层数据过早释放导致悬垂引用。
#[derive(Clone, Copy, Debug)]
pub struct Context<'a> {
    cancellation: &'a Cancellation,
    deadline: Deadline,
    budgets: &'a [Budget],
    trace_context: &'a TraceContext,
    identity: Option<&'a IdentityDescriptor>,
    peer_identity: Option<&'a IdentityDescriptor>,
}

impl<'a> Context<'a> {
    /// 构造执行上下文视图。
    ///
    /// # 参数说明
    /// - `cancellation`：来自父 `CallContext` 的取消原语引用，保证跨模块共享同一原子位；
    /// - `deadline`：绝对截止时间的值拷贝，若为 [`Deadline::none`] 表示未设置硬超时；
    /// - `budgets`：预算数组的只读切片，通常由 `CallContext` 内部 `Vec` 提供；
    /// - `trace_context`：分布式追踪上下文引用，确保链路采样与 span 继承一致；
    /// - `identity` / `peer_identity`：安全上下文中的主体/对端身份零拷贝视图。
    ///
    /// # 前置条件
    /// - 调用方需保证 `budgets` 生命周期不少于 `Context`；
    /// - `cancellation` 引用必须有效且指向统一的取消状态。
    ///
    /// # 后置条件
    /// - 返回结构仅提供只读视图；任何预算消费仍需通过原始 [`Budget`] 对象完成。
    pub fn new(
        cancellation: &'a Cancellation,
        deadline: Deadline,
        budgets: &'a [Budget],
        trace_context: &'a TraceContext,
        identity: Option<&'a IdentityDescriptor>,
        peer_identity: Option<&'a IdentityDescriptor>,
    ) -> Self {
        Self {
            cancellation,
            deadline,
            budgets,
            trace_context,
            identity,
            peer_identity,
        }
    }

    /// 获取取消原语引用，供调用方在热路径快速检查终止信号。
    ///
    /// # 语义说明
    /// - **返回值**：与 `CallContext` 内部共享的取消令牌引用；
    /// - **使用建议**：在长时间轮询、IO 等场景定期检查 `is_cancelled()` 并提前退出。
    pub fn cancellation(&self) -> &'a Cancellation {
        self.cancellation
    }

    /// 读取绝对截止时间。
    ///
    /// # 语义说明
    /// - **返回值**：`Deadline` 的值拷贝，允许调用方结合 [`crate::runtime::MonotonicTimePoint`] 判断是否超时；
    /// - **边界情况**：当调用方在无截止的调用中请求该值，将返回 `Deadline::none()`。
    pub fn deadline(&self) -> Deadline {
        self.deadline
    }

    /// 返回追踪上下文引用，便于 Handler/Service 衔接分布式追踪。
    ///
    /// # 使用场景
    /// - 可用于读取 `trace_id`/`span_id` 并衍生子 Span；
    /// - 在无采样场景下，该引用仍然有效，调用方可根据 `is_sampled()` 决定是否继续追踪。
    pub fn trace_context(&self) -> &'a TraceContext {
        self.trace_context
    }

    /// 获取当前调用主体身份。
    ///
    /// # 返回值
    /// - 返回 [`IdentityDescriptor`] 的只读引用，若调用在匿名或非认证模式下执行，则返回 `None`。
    pub fn identity(&self) -> Option<&'a IdentityDescriptor> {
        self.identity
    }

    /// 获取通信对端身份。
    ///
    /// # 返回值
    /// - 与 [`Self::identity`] 类似，返回对端的身份描述；若尚未协商身份则为 `None`。
    pub fn peer_identity(&self) -> Option<&'a IdentityDescriptor> {
        self.peer_identity
    }

    /// 根据预算种类检索对应预算控制器。
    ///
    /// # 参数
    /// - `kind`：预算类型标识，支持解码、流量或自定义类别。
    ///
    /// # 返回值
    /// - 若存在匹配预算，返回对 [`Budget`] 的只读引用；否则返回 `None`，提示调用方采用默认策略。
    ///
    /// # 风险提示
    /// - 预算列表通常较短，线性搜索开销可接受；若未来预算种类激增，可改为 `IndexMap` 等结构以换取查询复杂度。
    pub fn budget(&self, kind: &BudgetKind) -> Option<&'a Budget> {
        self.budgets.iter().find(|budget| budget.kind() == kind)
    }

    /// 遍历全部预算，适用于在诊断或观测场景中输出剩余额度。
    ///
    /// # 返回值
    /// - 返回 [`slice::Iter`]，可直接用于 `for` 循环或适配为 `Iterator`。
    ///
    /// # 前置条件
    /// - 调用方需保证在迭代期间不修改底层预算集合，以免违反只读约束。
    pub fn budgets(&self) -> slice::Iter<'a, Budget> {
        self.budgets.iter()
    }
}

impl<'a> From<&'a CallContext> for Context<'a> {
    /// 从完整的 [`CallContext`] 派生执行视图。
    ///
    /// # 逻辑说明
    /// - 直接引用内部的取消标记与预算切片，保持零拷贝；
    /// - 截止时间以值语义复制，避免与内部存储产生生命周期冲突。
    fn from(ctx: &'a CallContext) -> Self {
        let security = ctx.security();
        Self {
            cancellation: ctx.cancellation(),
            deadline: ctx.deadline(),
            budgets: ctx.budgets_slice(),
            trace_context: ctx.trace_context(),
            identity: security.identity().map(|identity| identity.as_ref()),
            peer_identity: security.peer_identity().map(|identity| identity.as_ref()),
        }
    }
}
