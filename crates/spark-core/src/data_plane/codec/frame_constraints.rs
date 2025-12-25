use alloc::format;
use core::{convert::TryFrom, num::NonZeroU16};

use crate::CoreError;
use crate::contract::{Budget, BudgetDecision};
use crate::error::codes;

/// `FrameRole` 区分约束在编码端还是解码端生效，用于生成具象化的错误消息与遥测标签。
///
/// # 设计缘由（Why）
/// - 既有的 `EncodeContext` 与 `DecodeContext` 在处理帧预算、递归深度时拥有完全相同的逻辑，仅在错误文案与指标命名上存在“Encoder/Decoder”差异；
/// - 将角色抽象为枚举，可在共享逻辑中轻松选择正确的文案，避免复制粘贴导致的不一致。
///
/// # 行为描述（How）
/// - `limit_label`/`depth_label` 返回用于错误消息的角色前缀；
/// - 其余方法为生成带上下文的信息字符串提供辅助。
///
/// # 契约说明（What）
/// - **前置条件**：枚举仅包含 `Encoder` 与 `Decoder` 两个分支；
/// - **后置条件**：调用方可放心地将其用于区分日志、错误消息或指标标签。
#[derive(Clone, Copy, Debug)]
pub(crate) enum FrameRole {
    Encoder,
    Decoder,
}

impl FrameRole {
    fn limit_label(self) -> &'static str {
        match self {
            FrameRole::Encoder => "encoder",
            FrameRole::Decoder => "decoder",
        }
    }

    fn depth_label(self) -> &'static str {
        match self {
            FrameRole::Encoder => "encoder",
            FrameRole::Decoder => "decoder",
        }
    }
}

/// `FrameCharge` 负责安全地把 `usize` 长度映射为预算扣减所需的 `u64` 数值。
///
/// # 设计动机（Why）
/// - 在 32/64 位架构混用的情况下直接转换可能溢出，集中在一个类型中处理错误可以避免调用方重复编写样板逻辑；
/// - 统一返回 `protocol.budget_exceeded` 错误码，便于上层捕获并触发限流或断连策略。
///
/// # 逻辑解析（How）
/// - `new` 尝试执行 `u64::try_from(len)`，失败时生成具备稳定错误码与解释文案的 [`CoreError`]；
/// - `amount` 返回转换成功后的值供预算系统消费。
///
/// # 契约说明（What）
/// - **输入**：`len` 代表即将消费或解析的帧长度；
/// - **输出**：成功返回 `FrameCharge`，可多次读取但应只用于一次扣减；
/// - **前置条件**：无；**后置条件**：若返回错误，预算状态保持不变。
struct FrameCharge(u64);

impl FrameCharge {
    fn new(len: usize) -> crate::Result<Self, CoreError> {
        u64::try_from(len).map(FrameCharge).map_err(|_| {
            CoreError::new(
                codes::PROTOCOL_BUDGET_EXCEEDED,
                "frame length exceeds budget accounting capacity (u64::MAX)",
            )
        })
    }

    fn amount(&self) -> u64 {
        self.0
    }
}

/// `FrameConstraints` 将帧长预算、递归深度限制与预算引用封装为可复用的组合件。
///
/// # 设计初心（Why）
/// - 原有 `EncodeContext`/`DecodeContext` 在帧预算、深度控制逻辑上完全重复，影响维护；
/// - 通过该结构共享核心算法，可确保错误语义、指标标签的统一，并降低未来变更成本。
///
/// # 行为拆解（How）
/// - 构造函数 `empty`/`with_frame_size`/`with_limits` 提供不同复杂度的初始化方案；
/// - `check_frame` 负责帧长与预算扣减；
/// - `enter_frame`/`FrameGuard` 组合实现 RAII 风格的递归层级控制；
/// - `refund_budget` 在失败时回滚预算消耗。
///
/// # 契约定义（What）
/// - **前置条件**：若提供预算引用，必须在 `FrameConstraints` 生命周期内保持有效；
/// - **后置条件**：所有方法保证在错误时不修改内部状态，成功时更新 `current_depth` 或预算剩余量。
///
/// # 风险与权衡（Trade-offs）
/// - 结构体不维护并发安全：由引用方（上下文）保证同一实例不会在多线程同时使用；
/// - `refund_budget` 对超出 `u64` 范围的输入静默忽略，以避免 fuzz 测试触发 panic。
pub(crate) struct FrameConstraints<'a> {
    max_frame_size: Option<usize>,
    budget: Option<&'a Budget>,
    max_recursion_depth: Option<NonZeroU16>,
    current_depth: u16,
}

impl<'a> FrameConstraints<'a> {
    /// 创建不携带任何约束的配置。
    pub(crate) fn empty() -> Self {
        Self {
            max_frame_size: None,
            budget: None,
            max_recursion_depth: None,
            current_depth: 0,
        }
    }

    /// 仅设置最大帧长度。
    pub(crate) fn with_frame_size(max_frame_size: Option<usize>) -> Self {
        Self {
            max_frame_size,
            ..Self::empty()
        }
    }

    /// 同时配置预算、帧长度与递归深度限制。
    pub(crate) fn with_limits(
        budget: Option<&'a Budget>,
        max_frame_size: Option<usize>,
        max_recursion_depth: Option<NonZeroU16>,
    ) -> Self {
        Self {
            max_frame_size,
            budget,
            max_recursion_depth,
            current_depth: 0,
        }
    }

    /// 返回帧长上限。
    pub(crate) fn max_frame_size(&self) -> Option<usize> {
        self.max_frame_size
    }

    /// 返回可选预算引用。
    pub(crate) fn budget(&self) -> Option<&'a Budget> {
        self.budget
    }

    /// 返回递归深度上限。
    pub(crate) fn max_recursion_depth(&self) -> Option<NonZeroU16> {
        self.max_recursion_depth
    }

    /// 返回当前深度计数。
    pub(crate) fn current_depth(&self) -> u16 {
        self.current_depth
    }

    /// 校验帧长度并在成功时扣减预算。
    pub(crate) fn check_frame(
        &self,
        frame_len: usize,
        role: FrameRole,
    ) -> crate::Result<(), CoreError> {
        if let Some(max) = self.max_frame_size
            && frame_len > max
        {
            return Err(CoreError::new(
                codes::PROTOCOL_BUDGET_EXCEEDED,
                format!(
                    "frame length {} exceeds configured {} limit {} bytes",
                    frame_len,
                    role.limit_label(),
                    max
                ),
            ));
        }

        if let Some(budget) = self.budget {
            let charge = FrameCharge::new(frame_len)?;
            match budget.try_consume(charge.amount()) {
                BudgetDecision::Granted { .. } => {}
                BudgetDecision::Exhausted { snapshot } => {
                    return Err(CoreError::new(
                        codes::PROTOCOL_BUDGET_EXCEEDED,
                        format!(
                            "budget {:?} exhausted: required {}, remaining {} of limit {}",
                            snapshot.kind(),
                            charge.amount(),
                            snapshot.remaining(),
                            snapshot.limit()
                        ),
                    ));
                }
            }
        }

        Ok(())
    }

    /// 进入下一层帧处理递归，返回 RAII 守卫确保离开时回退计数。
    pub(crate) fn enter_frame(
        &mut self,
        role: FrameRole,
    ) -> crate::Result<FrameGuard<'_, 'a>, CoreError> {
        if let Some(limit) = self.max_recursion_depth
            && self.current_depth >= limit.get()
        {
            return Err(CoreError::new(
                codes::PROTOCOL_BUDGET_EXCEEDED,
                format!(
                    "{} recursion depth {} exceeds configured limit {}",
                    role.depth_label(),
                    self.current_depth + 1,
                    limit
                ),
            ));
        }

        self.current_depth = self.current_depth.checked_add(1).ok_or_else(|| {
            CoreError::new(
                codes::PROTOCOL_BUDGET_EXCEEDED,
                format!("{} recursion depth counter overflow", role.depth_label()),
            )
        })?;

        Ok(FrameGuard {
            constraints: self,
            active: true,
        })
    }

    /// 在失败或重试路径上返还预算。
    pub(crate) fn refund_budget(&self, amount: usize) {
        if let (Some(budget), Ok(amount)) = (self.budget, u64::try_from(amount)) {
            budget.refund(amount);
        }
    }
}

/// `FrameGuard` 是递归深度限制的共享 RAII 实现。
///
/// # 设计背景（Why）
/// - 先前编码器与解码器分别实现几乎一致的守卫，抽取公共类型可确保两端逻辑严格一致；
/// - 通过 `Drop` 自动回退深度，防止忘记手动减计数导致逻辑锁死。
///
/// # 契约说明（What）
/// - 只能通过 [`FrameConstraints::enter_frame`] 构造；
/// - **前置条件**：构造前已经通过 `enter_frame` 检查成功；
/// - **后置条件**：离开作用域必定回退一次深度，除非调用者显式 `core::mem::forget`。
pub(crate) struct FrameGuard<'ctx, 'a> {
    constraints: &'ctx mut FrameConstraints<'a>,
    active: bool,
}

impl Drop for FrameGuard<'_, '_> {
    fn drop(&mut self) {
        if self.active {
            self.constraints.current_depth = self.constraints.current_depth.saturating_sub(1);
        }
    }
}
