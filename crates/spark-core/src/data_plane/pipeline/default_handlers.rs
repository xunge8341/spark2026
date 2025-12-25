//! Pipeline 默认 Handler 集合，负责将结构化错误分类自动转换为就绪信号或关闭策略。
//!
//! # 设计背景（Why）
//! - T22 任务要求“错误契约机器可读”，因此框架需要根据 [`ErrorCategory`] 自动触发背压、退避或关闭动作；
//! - 将策略集中在默认 Handler 中，可确保即便业务未自定义异常处理，也能获得合理的降级行为。
//!
//! # 契约说明（What）
//! - `ExceptionAutoResponder` 实现 [`InboundHandler`]，在 `on_exception_caught` 中读取 [`CoreError::category`]；
//! - 针对 `Retryable`、`ResourceExhausted` 等分类，会向控制器广播 [`ReadyStateEvent`]，供上层调度退避；
//! - 针对 `Security`、`ProtocolViolation` 等分类，直接调用 [`Context::close_graceful`] 触发优雅关闭；
//! - `Cancelled` 与 `Timeout` 会回写 [`crate::contract::Cancellation`] 标记，确保后续逻辑及时结束。
//!
//! # 风险提示（Trade-offs）
//! - 若上层已经注册自定义异常处理器，应注意 Handler 顺序，避免默认策略覆盖业务自定义逻辑；
//! - 默认策略仅依据错误分类，不会解析错误码或消息，分类必须在构造错误时准确设置。

use crate::{
    contract::CloseReason,
    error::{CoreError, ErrorCategory},
    observability::CoreUserEvent,
    pipeline::{context::Context, handler::InboundHandler},
    status::{BusyReason, ReadyState, SubscriptionBudget},
    types::{BudgetKind, BudgetSnapshot},
};

/// 将 `ErrorCategory` 映射为自动化响应的默认入站 Handler。
#[derive(Debug, Default)]
pub struct ExceptionAutoResponder;

impl ExceptionAutoResponder {
    /// 创建新的自动响应 Handler。
    pub const fn new() -> Self {
        Self
    }
}

impl InboundHandler for ExceptionAutoResponder {
    fn on_channel_active(&self, _ctx: &dyn Context) {}

    fn on_read(&self, _ctx: &dyn Context, _msg: crate::buffer::PipelineMessage) {}

    fn on_read_complete(&self, _ctx: &dyn Context) {}

    fn on_writability_changed(&self, _ctx: &dyn Context, _is_writable: bool) {}

    fn on_user_event(&self, _ctx: &dyn Context, _event: CoreUserEvent) {}

    fn on_exception_caught(&self, ctx: &dyn Context, error: CoreError) {
        let code = error.code();
        let category = error.category();
        match category {
            ErrorCategory::Retryable(advice) => {
                if let Some(reason) = busy_reason_from_code(code) {
                    emit_ready_state(ctx, ReadyState::Busy(reason));
                }
                emit_ready_state(ctx, ReadyState::RetryAfter(advice));
            }
            ErrorCategory::ResourceExhausted(kind) => {
                let snapshot = ctx
                    .call_context()
                    .budget(&kind)
                    .map(|budget| budget.snapshot())
                    .unwrap_or_else(|| fallback_snapshot(kind.clone()));
                let budget = SubscriptionBudget::from(&snapshot);
                emit_ready_state(ctx, ReadyState::BudgetExhausted(budget));
            }
            ErrorCategory::Security(class) => {
                let reason = CloseReason::new("security.violation", class.summary());
                ctx.channel().close_graceful(reason, None);
            }
            ErrorCategory::ProtocolViolation => {
                let reason =
                    CloseReason::new("protocol.violation", "检测到协议契约违规，已触发优雅关闭");
                ctx.channel().close_graceful(reason, None);
            }
            ErrorCategory::Cancelled | ErrorCategory::Timeout => {
                ctx.call_context().cancellation().cancel();
            }
            ErrorCategory::NonRetryable => {
                // 非重试错误默认不采取额外动作，交由上层根据具体场景决定。
            }
        }
    }

    fn on_channel_inactive(&self, _ctx: &dyn Context) {}
}

/// 就绪状态广播事件，供控制器分发给上层调度器。
#[derive(Clone, Debug)]
pub struct ReadyStateEvent {
    state: ReadyState,
}

impl ReadyStateEvent {
    /// 构造事件。
    pub fn new(state: ReadyState) -> Self {
        Self { state }
    }

    /// 获取内部就绪状态。
    pub fn state(&self) -> &ReadyState {
        &self.state
    }
}

fn emit_ready_state(ctx: &dyn Context, state: ReadyState) {
    let event = ReadyStateEvent::new(state);
    ctx.pipeline()
        .emit_user_event(CoreUserEvent::from_application_event(event));
}

fn fallback_snapshot(kind: BudgetKind) -> BudgetSnapshot {
    BudgetSnapshot::new(kind, 0, 0)
}

/// 根据错误码推导默认的繁忙原因，帮助调度器生成可观测信号。
///
/// # 设计意图（Why）
/// - 与错误分类矩阵保持一致，使 `Retryable` 错误在广播 `RetryAfter` 前先给出
///   可观测的繁忙主语义（上游/下游）。
///
/// # 契约说明（What）
/// - **输入**：稳定错误码；
/// - **输出**：若存在约定的繁忙语义则返回 [`BusyReason`]，否则 `None`；
/// - **前置条件**：调用前已确认错误码遵循矩阵约定。
fn busy_reason_from_code(code: &str) -> Option<BusyReason> {
    use crate::error::category_matrix;

    category_matrix::default_autoresponse(code)
        .and_then(|response| response.retry())
        .and_then(|(_, _, busy)| busy)
        .map(|disposition| disposition.to_busy_reason())
}
