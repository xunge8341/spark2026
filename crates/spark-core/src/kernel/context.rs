use crate::{
    contract::{CallContext, Cancellation, Deadline},
    kernel::attributes::Attributes,
    observability::TraceContext,
    types::{Budget, BudgetKind},
};
use core::slice;

/// `Context` 为一次执行路径提供只读的“机制层元数据视图”。
///
/// Foundation 版本下：
/// - Core 对身份/安全策略无知；
/// - 所有治理数据必须通过 [`Attributes`] 携带；
/// - 仍保留取消/截止/预算/追踪等机制信息，供数据面热路径读取。
#[derive(Clone, Copy, Debug)]
pub struct Context<'a> {
    cancellation: &'a Cancellation,
    deadline: Deadline,
    budgets: &'a [Budget],
    attributes: &'a Attributes,
    trace_context: &'a TraceContext,
}

impl<'a> Context<'a> {
    pub fn new(
        cancellation: &'a Cancellation,
        deadline: Deadline,
        budgets: &'a [Budget],
        attributes: &'a Attributes,
        trace_context: &'a TraceContext,
    ) -> Self {
        Self {
            cancellation,
            deadline,
            budgets,
            attributes,
            trace_context,
        }
    }

    pub fn cancellation(&self) -> &'a Cancellation {
        self.cancellation
    }

    pub fn deadline(&self) -> Deadline {
        self.deadline
    }

    pub fn trace_context(&self) -> &'a TraceContext {
        self.trace_context
    }

    pub fn attributes(&self) -> &'a Attributes {
        self.attributes
    }

    pub fn budget(&self, kind: &BudgetKind) -> Option<&'a Budget> {
        self.budgets.iter().find(|b| &b.kind == kind)
    }

    pub fn budgets(&self) -> slice::Iter<'a, Budget> {
        self.budgets.iter()
    }
}

impl<'a> From<&'a CallContext> for Context<'a> {
    fn from(ctx: &'a CallContext) -> Self {
        Self::new(
            ctx.cancellation(),
            ctx.deadline(),
            ctx.budgets_slice(),
            ctx.attributes(),
            ctx.trace_context(),
        )
    }
}
