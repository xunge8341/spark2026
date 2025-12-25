#![cfg(test)]

//! CANCEL 与最终响应竞速场景测试。
//!
//! # 教案式概要
//! - **Why**：RFC 3261 §9 要求 CANCEL 能够立即终止正在处理的 INVITE，同时在已有最终响应时
//!   应保持事务稳定。本文件覆盖这两类竞态，作为实现者调试的锚点。
//! - **How**：使用 `spark-codec-sip` 暴露的状态机模型驱动测试，通过断言状态、响应与错误分类，
//!   验证 CANCEL 的可重入性与错误映射。
//! - **What**：提供五个核心用例，涵盖“CANCEL 抢占 487”、“CANCEL 遇到既有响应”、“CANCEL 后
//!   的响应冲突”、“终止态 CANCEL”与“解析错误分类”场景。

use spark_codec_sip::error::{SipTransactionError, ToErrorCategory};
use spark_codec_sip::{
    CancelOutcome, FinalResponseDisposition, InviteServerTransaction, InviteServerTransactionState,
    SipParseError,
};
use spark_core::error::ErrorCategory;

/// CANCEL 抢在最终响应前到达，应当生成 487 并标记为已取消。
#[test]
fn cancel_before_final_response_generates_487_and_marks_cancelled() {
    let mut transaction = InviteServerTransaction::new();
    transaction.advance_to_proceeding();

    let outcome = transaction
        .handle_cancel()
        .expect("CANCEL 在 Trying/Proceeding 阶段必须被接受");

    assert_eq!(outcome.cancel_response, 200, "CANCEL 响应码必须为 200 OK");
    assert_eq!(
        outcome.final_response,
        Some(FinalResponseDisposition::Generated(487)),
        "应生成 487 Request Terminated"
    );
    assert!(outcome.cancelled_invite, "标记 INVITE 已被取消");
    assert_eq!(
        outcome.state,
        InviteServerTransactionState::Completed,
        "状态机应进入 Completed"
    );
    assert_eq!(transaction.final_response(), Some(487));
    assert!(transaction.cancel_applied());
}

/// CANCEL 在最终响应之后到达，应保持既有响应不变。
#[test]
fn cancel_after_final_response_preserves_existing_state() {
    let mut transaction = InviteServerTransaction::new();
    transaction
        .record_final_response(200)
        .expect("记录首个最终响应应成功");

    let outcome = transaction
        .handle_cancel()
        .expect("已完成事务仍需对 CANCEL 返回 200");

    assert_eq!(outcome.cancel_response, 200);
    assert_eq!(
        outcome.final_response,
        Some(FinalResponseDisposition::AlreadySent(200)),
        "CANCEL 不得覆盖既有的 200 OK"
    );
    assert!(!outcome.cancelled_invite, "原事务未被取消，只是回送 200");
    assert_eq!(transaction.final_response(), Some(200));
    assert!(!transaction.cancel_applied());
}

/// CANCEL 生成 487 后，若业务仍尝试写入其他最终响应，应产生冲突错误并映射到 Cancelled。
#[test]
fn cancel_then_conflicting_final_response_returns_error() {
    let mut transaction = InviteServerTransaction::new();
    transaction.advance_to_proceeding();
    let outcome = transaction
        .handle_cancel()
        .expect("CANCEL 必须成功触发 487");
    assert!(matches!(
        outcome,
        CancelOutcome {
            final_response: Some(FinalResponseDisposition::Generated(487)),
            ..
        }
    ));

    let error = transaction
        .record_final_response(200)
        .expect_err("再次写入最终响应应被拒绝");
    assert!(matches!(
        error,
        SipTransactionError::FinalResponseConflict {
            existing: 487,
            attempted: 200
        }
    ));
    assert_eq!(error.to_error_category(), ErrorCategory::Cancelled);
}

/// 事务终止后收到 CANCEL，应返回终止错误并映射到 Cancelled，提醒上层保持取消语义。
#[test]
fn cancel_after_termination_is_categorized_as_cancelled() {
    let mut transaction = InviteServerTransaction::new();
    transaction.terminate();

    let error = transaction
        .handle_cancel()
        .expect_err("终止态不应再接受 CANCEL");
    assert!(matches!(error, SipTransactionError::TransactionTerminated));
    assert_eq!(error.to_error_category(), ErrorCategory::Cancelled);
}

/// 解析阶段的错误必须映射到协议违规类别，驱动默认关闭策略。
#[test]
fn parse_errors_map_to_protocol_violation() {
    let parse_error = SipParseError::InvalidRequestLine;
    assert_eq!(
        parse_error.to_error_category(),
        ErrorCategory::ProtocolViolation
    );
}

/// 未匹配的 CANCEL 需映射为协议违规，提示实现者校验分支。
#[test]
fn unmatched_cancel_maps_to_protocol_violation() {
    let error = SipTransactionError::NoMatchingInvite;
    assert_eq!(error.to_error_category(), ErrorCategory::ProtocolViolation);
}
