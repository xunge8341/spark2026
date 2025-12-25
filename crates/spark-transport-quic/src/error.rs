use quinn::{ConnectError, ConnectionError, ReadError, WriteError};
use spark_core::prelude::{CoreError, ErrorCategory, RetryAdvice};
use std::borrow::Cow;
use std::io;
use std::time::Duration;

/// QUIC 传输层错误映射工具。
///
/// # 教案式注释
///
/// ## 意图（Why）
/// - **统一错误码**：将 `quinn` 与标准库 IO 错误转换为框架稳定的 `CoreError`，
///   便于日志、指标与自动化治理使用统一语义。
/// - **分类驱动治理**：为每个错误关联 [`ErrorCategory`]，让上层可据此决定是否重试、
///   退避或直接失败。
/// - **复用契约**：与 TCP 传输保持一致的 `OperationKind` 命名体系，降低认知成本。
///
/// ## 逻辑（How）
/// - `OperationKind` 记录错误码与默认文案；
/// - `map_*` 系列函数根据错误来源生成 `CoreError`，并设置合适的分类与提示；
/// - `cancelled_error`、`timeout_error` 为上下文控制提供统一封装；
/// - `invalid_endpoint_mode`、`closed_error` 用于语义异常（如在客户端调用 `accept`）。
///
/// ## 契约（What）
/// - 所有函数返回的 `CoreError` 均遵循 `spark.transport.quic.*` 命名约定；
/// - **前置条件**：调用方需传入与操作对应的 `OperationKind`，保证错误码语义稳定；
/// - **后置条件**：返回值的 `ErrorCategory` 与错误信息能够驱动上层正确的容错策略。
///
/// ## 风险与注意（Trade-offs）
/// - 分类策略基于通用经验，若部署环境有特殊 SLA，可调整重试时长常量；
/// - `quinn` 错误枚举未来可能扩展，需要同步更新映射函数；
/// - 未对错误消息做本地化，日志中统一使用英文描述，以便跨团队排障。
#[derive(Clone, Copy)]
pub(crate) struct OperationKind {
    pub code: &'static str,
    pub message: &'static str,
}

pub(crate) const BIND: OperationKind = OperationKind {
    code: "spark.transport.quic.bind_failed",
    message: "quic bind",
};

pub(crate) const ACCEPT: OperationKind = OperationKind {
    code: "spark.transport.quic.accept_failed",
    message: "quic accept",
};

pub(crate) const CONNECT: OperationKind = OperationKind {
    code: "spark.transport.quic.connect_failed",
    message: "quic connect",
};

pub(crate) const OPEN_STREAM: OperationKind = OperationKind {
    code: "spark.transport.quic.open_stream_failed",
    message: "quic open_stream",
};

pub(crate) const READ: OperationKind = OperationKind {
    code: "spark.transport.quic.read_failed",
    message: "quic read",
};

pub(crate) const WRITE: OperationKind = OperationKind {
    code: "spark.transport.quic.write_failed",
    message: "quic write",
};

pub(crate) const FLUSH: OperationKind = OperationKind {
    code: "spark.transport.quic.flush_failed",
    message: "quic flush",
};

pub(crate) const SHUTDOWN: OperationKind = OperationKind {
    code: "spark.transport.quic.shutdown_failed",
    message: "quic shutdown",
};

pub(crate) const POLL_READY: OperationKind = OperationKind {
    code: "spark.transport.quic.poll_ready_failed",
    message: "quic poll_ready",
};

pub(crate) fn map_io_error(kind: OperationKind, error: io::Error) -> CoreError {
    let category = categorize_io_error(&error);
    CoreError::new(
        kind.code,
        Cow::Owned(format!("{}: {}", kind.message, error)),
    )
    .with_category(category)
}

pub(crate) fn map_connect_error(kind: OperationKind, error: ConnectError) -> CoreError {
    let category = match error {
        ConnectError::UnsupportedVersion => retryable(Duration::from_millis(200)),
        ConnectError::EndpointStopping
        | ConnectError::CidsExhausted
        | ConnectError::InvalidServerName(_)
        | ConnectError::InvalidRemoteAddress(_)
        | ConnectError::NoDefaultClientConfig => ErrorCategory::NonRetryable,
    };
    CoreError::new(
        kind.code,
        Cow::Owned(format!("{}: {}", kind.message, error)),
    )
    .with_category(category)
}

pub(crate) fn map_connection_error(kind: OperationKind, error: ConnectionError) -> CoreError {
    let category = match error {
        ConnectionError::TimedOut => ErrorCategory::Timeout,
        ConnectionError::LocallyClosed | ConnectionError::CidsExhausted => {
            ErrorCategory::NonRetryable
        }
        ConnectionError::Reset | ConnectionError::ConnectionClosed(_) => {
            retryable(Duration::from_millis(80))
        }
        ConnectionError::ApplicationClosed(_) => ErrorCategory::NonRetryable,
        ConnectionError::TransportError(_) => retryable(Duration::from_millis(120)),
        ConnectionError::VersionMismatch => ErrorCategory::NonRetryable,
    };
    CoreError::new(
        kind.code,
        Cow::Owned(format!("{}: {}", kind.message, error)),
    )
    .with_category(category)
}

pub(crate) fn map_write_error(kind: OperationKind, error: WriteError) -> CoreError {
    let category = match error {
        WriteError::Stopped(_) => retryable(Duration::from_millis(40)),
        WriteError::ConnectionLost(_) => retryable(Duration::from_millis(100)),
        WriteError::ClosedStream => ErrorCategory::NonRetryable,
        WriteError::ZeroRttRejected => retryable(Duration::from_millis(20)),
    };
    CoreError::new(
        kind.code,
        Cow::Owned(format!("{}: {}", kind.message, error)),
    )
    .with_category(category)
}

pub(crate) fn map_read_error(kind: OperationKind, error: ReadError) -> CoreError {
    let category = match error {
        ReadError::Reset(_) => retryable(Duration::from_millis(40)),
        ReadError::ConnectionLost(_) => retryable(Duration::from_millis(100)),
        ReadError::ClosedStream | ReadError::IllegalOrderedRead => ErrorCategory::NonRetryable,
        ReadError::ZeroRttRejected => retryable(Duration::from_millis(20)),
    };
    CoreError::new(
        kind.code,
        Cow::Owned(format!("{}: {}", kind.message, error)),
    )
    .with_category(category)
}

pub(crate) fn cancelled_error(kind: OperationKind) -> CoreError {
    CoreError::new(
        "spark.transport.quic.cancelled",
        format!("{} cancelled", kind.message),
    )
    .with_category(ErrorCategory::Cancelled)
}

pub(crate) fn timeout_error(kind: OperationKind) -> CoreError {
    CoreError::new(
        "spark.transport.quic.timeout",
        format!("{} timed out", kind.message),
    )
    .with_category(ErrorCategory::Timeout)
}

pub(crate) fn invalid_endpoint_mode(kind: OperationKind) -> CoreError {
    CoreError::new(
        kind.code,
        format!("{}: invalid endpoint mode", kind.message),
    )
    .with_category(ErrorCategory::NonRetryable)
}

pub(crate) fn closed_error(kind: OperationKind, detail: &'static str) -> CoreError {
    CoreError::new(kind.code, format!("{}: {}", kind.message, detail))
        .with_category(ErrorCategory::NonRetryable)
}

fn categorize_io_error(error: &io::Error) -> ErrorCategory {
    use io::ErrorKind;
    match error.kind() {
        ErrorKind::TimedOut => ErrorCategory::Timeout,
        ErrorKind::WouldBlock | ErrorKind::Interrupted => retryable(Duration::from_millis(5)),
        ErrorKind::ConnectionRefused
        | ErrorKind::ConnectionReset
        | ErrorKind::ConnectionAborted
        | ErrorKind::NotConnected
        | ErrorKind::AddrInUse
        | ErrorKind::AddrNotAvailable => retryable(Duration::from_millis(60)),
        ErrorKind::PermissionDenied | ErrorKind::Unsupported => ErrorCategory::NonRetryable,
        _ => ErrorCategory::NonRetryable,
    }
}

fn retryable(delay: Duration) -> ErrorCategory {
    ErrorCategory::Retryable(RetryAdvice::after(delay))
}
