use spark_core::prelude::{CoreError, ErrorCategory, RetryAdvice};
use std::borrow::Cow;
use std::io;
use std::time::Duration;

/// 描述一次底层操作对应的稳定错误码与默认文案。
#[derive(Clone, Copy)]
pub(crate) struct OperationKind {
    pub code: &'static str,
    pub message: &'static str,
}

pub(crate) const BIND: OperationKind = OperationKind {
    code: "spark.transport.tcp.bind_failed",
    message: "tcp bind",
};
pub(crate) const ACCEPT: OperationKind = OperationKind {
    code: "spark.transport.tcp.accept_failed",
    message: "tcp accept",
};
pub(crate) const CONNECT: OperationKind = OperationKind {
    code: "spark.transport.tcp.connect_failed",
    message: "tcp connect",
};
pub(crate) const READ: OperationKind = OperationKind {
    code: "spark.transport.tcp.read_failed",
    message: "tcp read",
};
pub(crate) const WRITE: OperationKind = OperationKind {
    code: "spark.transport.tcp.write_failed",
    message: "tcp write",
};
pub(crate) const WRITE_VECTORED: OperationKind = OperationKind {
    code: "spark.transport.tcp.writev_failed",
    message: "tcp write_vectored",
};
pub(crate) const FLUSH: OperationKind = OperationKind {
    code: "spark.transport.tcp.flush_failed",
    message: "tcp flush",
};
pub(crate) const SHUTDOWN: OperationKind = OperationKind {
    code: "spark.transport.tcp.shutdown_failed",
    message: "tcp shutdown",
};
pub(crate) const CONFIGURE: OperationKind = OperationKind {
    code: "spark.transport.tcp.configure_failed",
    message: "tcp configure",
};
pub(crate) const POLL_READY: OperationKind = OperationKind {
    code: "spark.transport.tcp.poll_ready_failed",
    message: "tcp poll_ready",
};
pub(crate) const PIPELINE_ATTACH: OperationKind = OperationKind {
    code: "spark.transport.tcp.pipeline_attach_failed",
    message: "tcp pipeline_attach",
};
pub(crate) const PIPELINE_MISSING: OperationKind = OperationKind {
    code: "spark.transport.tcp.pipeline_initializer_missing",
    message: "tcp pipeline_initializer_missing",
};

const CANCEL_CODE: &str = "spark.transport.tcp.cancelled";
const TIMEOUT_CODE: &str = "spark.transport.tcp.timeout";

/// 将 IO 错误映射为框架级 CoreError，并附带错误分类。
pub(crate) fn map_io_error(kind: OperationKind, error: io::Error) -> CoreError {
    let category = categorize_io_error(&error);
    CoreError::new(
        kind.code,
        Cow::Owned(format!("{}: {}", kind.message, error)),
    )
    .with_category(category)
}

/// 构造取消错误。
pub(crate) fn cancelled_error(kind: OperationKind) -> CoreError {
    let message = format!("{} cancelled", kind.message);
    CoreError::new(CANCEL_CODE, message).with_category(ErrorCategory::Cancelled)
}

/// 构造超时错误。
pub(crate) fn timeout_error(kind: OperationKind) -> CoreError {
    let message = format!("{} timed out", kind.message);
    CoreError::new(TIMEOUT_CODE, message).with_category(ErrorCategory::Timeout)
}

/// 当通道已绑定 Pipeline 元数据时返回冲突错误。
pub(crate) fn pipeline_attach_conflict() -> CoreError {
    CoreError::new(
        PIPELINE_ATTACH.code,
        format!("{} already bound", PIPELINE_ATTACH.message),
    )
    .with_category(ErrorCategory::NonRetryable)
}

/// 当互斥锁被毒化时返回通用错误，提醒调用方重新初始化监听器。
pub(crate) fn pipeline_attach_poisoned() -> CoreError {
    CoreError::new(
        PIPELINE_ATTACH.code,
        format!("{} poisoned mutex", PIPELINE_ATTACH.message),
    )
    .with_category(ErrorCategory::NonRetryable)
}

/// 当监听器尚未配置 PipelineInitializer 选择器时返回错误。
pub(crate) fn pipeline_initializer_missing() -> CoreError {
    CoreError::new(
        PIPELINE_MISSING.code,
        format!("{} selector not configured", PIPELINE_MISSING.message),
    )
    .with_category(ErrorCategory::NonRetryable)
}

fn categorize_io_error(error: &io::Error) -> ErrorCategory {
    use io::ErrorKind;
    match error.kind() {
        ErrorKind::TimedOut => ErrorCategory::Timeout,
        ErrorKind::WouldBlock | ErrorKind::Interrupted => {
            ErrorCategory::Retryable(RetryAdvice::after(Duration::from_millis(5)))
        }
        ErrorKind::ConnectionRefused
        | ErrorKind::ConnectionReset
        | ErrorKind::ConnectionAborted
        | ErrorKind::NotConnected
        | ErrorKind::AddrInUse
        | ErrorKind::AddrNotAvailable
        | ErrorKind::BrokenPipe => {
            ErrorCategory::Retryable(RetryAdvice::after(Duration::from_millis(50)))
        }
        ErrorKind::PermissionDenied | ErrorKind::Unsupported => ErrorCategory::NonRetryable,
        ErrorKind::WriteZero => {
            ErrorCategory::Retryable(RetryAdvice::after(Duration::from_millis(10)))
        }
        _ => ErrorCategory::NonRetryable,
    }
}
