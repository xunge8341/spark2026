#![allow(clippy::module_name_repetitions)]

use core::fmt;

use crate::error::SipTransactionError;

/// SIP INVITE 服务端事务的有限状态机表示。
///
/// # 教案级注释
/// - **意图 (Why)**：RFC 3261 §17 将 INVITE 事务抽象为 Trying → Proceeding → Completed →
///   Terminated 四阶段。为了在实现与测试中复用该语义，本结构使用枚举显式记录状态，
///   并在取消（CANCEL）竞态场景下提供可预测的过渡行为。
/// - **体系位置 (Where)**：该状态机位于 `spark-codec-sip`，作为编解码层与上层事务处理之间
///   的桥梁；`spark-tck` 的 CANCEL 竞态测试会直接驱动本状态机验证分支正确性。
/// - **契约 (What)**：状态从 `Trying` 起步；收到临时响应进入 `Proceeding`；产生最终响应后
///   进入 `Completed`；资源释放后进入 `Terminated`。在终态之外的任何状态都必须保持幂等的
///   CANCEL 处理能力。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InviteServerTransactionState {
    /// 初始状态：已收到 INVITE，但尚未向 UAS 报告任何响应。
    Trying,
    /// 进行中：已向对端发送 1xx 临时响应，等待最终响应或取消。
    Proceeding,
    /// 已完成：最终响应已经确定（可能由 CANCEL 触发 487）。
    Completed,
    /// 终止：事务资源已释放，任何额外输入都视为错误。
    Terminated,
}

impl fmt::Display for InviteServerTransactionState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Trying => f.write_str("Trying"),
            Self::Proceeding => f.write_str("Proceeding"),
            Self::Completed => f.write_str("Completed"),
            Self::Terminated => f.write_str("Terminated"),
        }
    }
}

/// CANCEL 处理后的最终响应处置。
///
/// # 教案式说明
/// - **Why**：在 CANCEL 与原事务最终响应竞速时，需要向上层报告“最终响应是否由 CANCEL 生成”，
///   以便决定是否广播 487 或复用既有响应。
/// - **How**：枚举区分“新生成的 487”与“已有响应被保留”两种语义，便于测试断言。
/// - **What**：`Generated(code)` 表示本次 CANCEL 新生成的最终响应；`AlreadySent(code)` 表示
///   原事务此前已经发送过对应的最终响应，CANCEL 只负责返回 200 OK。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FinalResponseDisposition {
    /// CANCEL 触发了新的最终响应（通常为 487 Request Terminated）。
    Generated(u16),
    /// 事务早已发送最终响应，CANCEL 仅回送 200，不再修改历史。
    AlreadySent(u16),
}

/// 处理 CANCEL 的结果描述。
///
/// # 教案式拆解
/// - **意图 (Why)**：对 CANCEL 的处理不仅要回送 200，还要决定原始 INVITE 是否需要生成
///   487；本结构一次性打包状态机新状态、是否生成最终响应等信息。
/// - **逻辑 (How)**：`cancel_response` 始终为 200；`final_response` 记录最终响应的去向；
///   `cancelled_invite` 指示是否实际终止了原始事务。
/// - **契约 (What)**：调用方根据 `state` 确认最新状态，根据 `final_response` 决定是否发送
///   487，若 `cancelled_invite` 为真则需触发上游的取消传播。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CancelOutcome {
    /// 应答 CANCEL 请求所使用的状态码（RFC 3261 要求为 200）。
    pub cancel_response: u16,
    /// 对原始 INVITE 最终响应的处置情况。
    pub final_response: Option<FinalResponseDisposition>,
    /// CANCEL 是否真正终止了原始事务（意味着需要向业务传播取消）。
    pub cancelled_invite: bool,
    /// 状态机在本次处理后的状态。
    pub state: InviteServerTransactionState,
}

impl CancelOutcome {
    /// 构造帮助函数，保证 `cancel_response` 恒为 200。
    const fn accepted(
        final_response: Option<FinalResponseDisposition>,
        cancelled_invite: bool,
        state: InviteServerTransactionState,
    ) -> Self {
        Self {
            cancel_response: 200,
            final_response,
            cancelled_invite,
            state,
        }
    }
}

/// INVITE 服务端事务，提供最小化的状态推进与 CANCEL 竞态处理。
///
/// # 教案式说明
/// - **动机 (Why)**：实际实现会在 I/O 线程中并发处理 INVITE 最终响应与 CANCEL。若缺乏显式
///   状态机，很难复现“CANCEL 抢先生成 487，而后续业务仍试图发送 200”的竞态。该结构是 TCK
///   与实现共享的参考模型。
/// - **流程 (How)**：
///   1. `new` 初始化为 `Trying`；
///   2. `advance_to_proceeding` 表示发送 1xx 临时响应；
///   3. `record_final_response` 记录业务产生的最终响应；
///   4. `handle_cancel` 处理 CANCEL，必要时生成 487 并拒绝后续最终响应；
///   5. `terminate` 表示事务完全结束。
/// - **契约 (What)**：所有方法皆为同步调用，不持有外部资源；返回的错误统一为
///   [`SipTransactionError`]，调用方需结合其 `ToErrorCategory` 实现驱动自动响应。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InviteServerTransaction {
    state: InviteServerTransactionState,
    final_response: Option<u16>,
    cancel_applied: bool,
}

impl InviteServerTransaction {
    /// 创建新的 INVITE 事务，初始状态为 `Trying`。
    pub const fn new() -> Self {
        Self {
            state: InviteServerTransactionState::Trying,
            final_response: None,
            cancel_applied: false,
        }
    }

    /// 返回当前状态，便于调用方断言或记录。
    pub const fn state(&self) -> InviteServerTransactionState {
        self.state
    }

    /// 返回已记录的最终响应状态码（若存在）。
    pub const fn final_response(&self) -> Option<u16> {
        self.final_response
    }

    /// 指示 CANCEL 是否已经实际终止了原始 INVITE。
    pub const fn cancel_applied(&self) -> bool {
        self.cancel_applied
    }

    /// 记录 1xx 临时响应，驱动状态进入 `Proceeding`。
    pub fn advance_to_proceeding(&mut self) {
        if matches!(self.state, InviteServerTransactionState::Trying) {
            self.state = InviteServerTransactionState::Proceeding;
        }
    }

    /// 记录业务生成的最终响应。
    ///
    /// # 契约说明
    /// - **前置条件**：状态必须为 `Trying`/`Proceeding`，或在 `Completed` 且重复写入同一状态码；
    /// - **后置条件**：成功后状态进入 `Completed`；若 CANCEL 曾生成 487，再次写入其它状态码
    ///   会返回 [`SipTransactionError::FinalResponseConflict`]。
    pub fn record_final_response(
        &mut self,
        status: u16,
    ) -> spark_core::Result<(), SipTransactionError> {
        match self.state {
            InviteServerTransactionState::Trying | InviteServerTransactionState::Proceeding => {
                self.final_response = Some(status);
                self.state = InviteServerTransactionState::Completed;
                Ok(())
            }
            InviteServerTransactionState::Completed => {
                let existing = self.final_response;
                if existing == Some(status) {
                    Ok(())
                } else {
                    Err(SipTransactionError::FinalResponseConflict {
                        existing: existing.unwrap_or(status),
                        attempted: status,
                    })
                }
            }
            InviteServerTransactionState::Terminated => {
                Err(SipTransactionError::TransactionTerminated)
            }
        }
    }

    /// 处理 CANCEL 请求，根据当前状态决定是否生成 487。
    ///
    /// # 返回值
    /// - 成功时返回 [`CancelOutcome`]，其中 `cancelled_invite` 表示是否需要对 INVITE 发送 487；
    /// - 若事务已经终止，则返回 [`SipTransactionError::TransactionTerminated`]。
    pub fn handle_cancel(&mut self) -> spark_core::Result<CancelOutcome, SipTransactionError> {
        match self.state {
            InviteServerTransactionState::Trying | InviteServerTransactionState::Proceeding => {
                if self.final_response.is_none() {
                    self.final_response = Some(487);
                    self.cancel_applied = true;
                    self.state = InviteServerTransactionState::Completed;
                    Ok(CancelOutcome::accepted(
                        Some(FinalResponseDisposition::Generated(487)),
                        true,
                        self.state,
                    ))
                } else {
                    self.state = InviteServerTransactionState::Completed;
                    Ok(CancelOutcome::accepted(
                        self.final_response
                            .map(FinalResponseDisposition::AlreadySent),
                        false,
                        self.state,
                    ))
                }
            }
            InviteServerTransactionState::Completed => Ok(CancelOutcome::accepted(
                self.final_response
                    .map(FinalResponseDisposition::AlreadySent),
                false,
                self.state,
            )),
            InviteServerTransactionState::Terminated => {
                Err(SipTransactionError::TransactionTerminated)
            }
        }
    }

    /// 将事务标记为终止，表明资源已经释放。
    pub fn terminate(&mut self) {
        self.state = InviteServerTransactionState::Terminated;
    }
}

impl Default for InviteServerTransaction {
    fn default() -> Self {
        Self::new()
    }
}
