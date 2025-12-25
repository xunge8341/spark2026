use std::{borrow::Cow, io, time::Duration};

use rustls::{AlertDescription, Error as RustlsError};
use spark_core::security::class::SecurityClass;
use spark_core::{BudgetKind, CoreError, ErrorCategory, RetryAdvice};

/// TLS 传输错误映射模块。
///
/// # 教案级注释
///
/// ## 意图（Why）
/// - 将 `rustls`/IO 层的错误统一映射到框架的 [`ErrorCategory`]，便于自动化决策（重试、熔断、
///   安全告警）。
/// - 提供稳定错误码与文案，使 TCK 与运维脚本能够根据错误定位问题，而无需解析底层库细节。
///
/// ## 逻辑（How）
/// - `OperationKind` 描述一类操作（握手/读写/关闭）的错误码与默认文案；
/// - `map_handshake_error`/`map_stream_error` 根据 `io::Error`（可能嵌套 `rustls::Error`）推导
///   [`ErrorCategory`]，并生成 `CoreError`；
/// - `categorize_rustls_error`/`categorize_io_error` 将具体错误细分为 `Security`、`ResourceExhausted`
///   或 `Retryable`；
/// - `alert_to_category` 针对 TLS Alert 做进一步细化，确保安全相关告警具备语义化分类。
///
/// ## 契约（What）
/// - 所有映射函数均保证返回的 [`CoreError`] 携带稳定错误码，并遵循“握手 → 安全/资源/重试”
///   的分类约定；
/// - 取消/超时使用 `cancelled_error` 与 `timeout_error` 保持与 TCP 层一致；
/// - `exclusive_channel_error` 表示缺少独占 `TcpChannel` 所需的资源。
///
/// ## 风险与权衡（Trade-offs）
/// - `rustls::Error::General` 等泛型错误默认映射为重试类别，避免误判为安全事件；
/// - 未穷举的 Alert 会被视作可重试错误，后续若需更精确分类可在此集中扩展。
///
///   描述一次 TLS 操作的错误码及默认文案。
#[derive(Clone, Copy)]
pub(crate) struct OperationKind {
    pub code: &'static str,
    pub message: &'static str,
}

pub(crate) const HANDSHAKE: OperationKind = OperationKind {
    code: "spark.transport.tls.handshake_failed",
    message: "tls handshake",
};

pub(crate) const READ: OperationKind = OperationKind {
    code: "spark.transport.tls.read_failed",
    message: "tls read",
};

pub(crate) const WRITE: OperationKind = OperationKind {
    code: "spark.transport.tls.write_failed",
    message: "tls write",
};

pub(crate) const FLUSH: OperationKind = OperationKind {
    code: "spark.transport.tls.flush_failed",
    message: "tls flush",
};

pub(crate) const SHUTDOWN: OperationKind = OperationKind {
    code: "spark.transport.tls.shutdown_failed",
    message: "tls shutdown",
};

const CANCEL_CODE: &str = "spark.transport.tls.cancelled";
const TIMEOUT_CODE: &str = "spark.transport.tls.timeout";

/// 将握手阶段的 `io::Error` 映射为框架级 [`CoreError`]。
pub(crate) fn map_handshake_error(kind: OperationKind, error: io::Error) -> CoreError {
    let category = categorize_with_rustls(&error);
    CoreError::new(
        kind.code,
        Cow::Owned(format!("{}: {}", kind.message, error)),
    )
    .with_category(category)
}

/// 将读写阶段的 `io::Error` 映射为框架级 [`CoreError`]。
pub(crate) fn map_stream_error(kind: OperationKind, error: io::Error) -> CoreError {
    let category = categorize_with_rustls(&error);
    CoreError::new(
        kind.code,
        Cow::Owned(format!("{}: {}", kind.message, error)),
    )
    .with_category(category)
}

/// 构造取消错误，保持与 `spark-transport-tcp` 一致的语义。
pub(crate) fn cancelled_error(kind: OperationKind) -> CoreError {
    let message = format!("{} cancelled", kind.message);
    CoreError::new(CANCEL_CODE, message).with_category(ErrorCategory::Cancelled)
}

/// 构造超时错误。
pub(crate) fn timeout_error(kind: OperationKind) -> CoreError {
    let message = format!("{} timed out", kind.message);
    CoreError::new(TIMEOUT_CODE, message).with_category(ErrorCategory::Timeout)
}

/// 当 TLS 握手需要独占 `TcpChannel` 但被多处持有时返回的错误。
pub(crate) fn exclusive_channel_error() -> CoreError {
    CoreError::new(
        "spark.transport.tls.channel_not_exclusive",
        "tls handshake requires exclusive TcpChannel ownership",
    )
    .with_category(ErrorCategory::ResourceExhausted(BudgetKind::Flow))
}

fn categorize_with_rustls(error: &io::Error) -> ErrorCategory {
    if let Some(source) = error.get_ref()
        && let Some(rustls_error) = source.downcast_ref::<RustlsError>()
    {
        return categorize_rustls_error(rustls_error);
    }
    categorize_io_error(error)
}

fn categorize_rustls_error(error: &RustlsError) -> ErrorCategory {
    use RustlsError::*;
    match error {
        InappropriateMessage { .. }
        | InappropriateHandshakeMessage { .. }
        | InvalidEncryptedClientHello(_)
        | InvalidMessage(_)
        | PeerMisbehaved(_)
        | DecryptError
        | EncryptError
        | PeerSentOversizedRecord => ErrorCategory::Security(SecurityClass::Integrity),
        NoCertificatesPresented
        | InvalidCertificate(_)
        | InvalidCertRevocationList(_)
        | UnsupportedNameType => ErrorCategory::Security(SecurityClass::Authentication),
        PeerIncompatible(_) | HandshakeNotComplete | General(_) | Other(_) => {
            retryable(Duration::from_millis(80))
        }
        FailedToGetCurrentTime
        | FailedToGetRandomBytes
        | BadMaxFragmentSize
        | InconsistentKeys(_) => ErrorCategory::ResourceExhausted(BudgetKind::Flow),
        AlertReceived(alert) => alert_to_category(alert),
        NoApplicationProtocol => ErrorCategory::Security(SecurityClass::Unknown),
        _ => retryable(Duration::from_millis(60)),
    }
}

fn alert_to_category(alert: &AlertDescription) -> ErrorCategory {
    use AlertDescription::*;
    match alert {
        BadCertificate
        | UnsupportedCertificate
        | CertificateRevoked
        | CertificateExpired
        | CertificateUnknown
        | UnknownCA
        | NoCertificate
        | CertificateUnobtainable
        | CertificateRequired => ErrorCategory::Security(SecurityClass::Authentication),
        AccessDenied => ErrorCategory::Security(SecurityClass::Authorization),
        CloseNotify | UserCanceled | NoRenegotiation => retryable(Duration::from_millis(30)),
        DecodeError
        | DecryptError
        | DecryptionFailed
        | HandshakeFailure
        | IllegalParameter
        | RecordOverflow
        | BadRecordMac
        | UnexpectedMessage
        | InsufficientSecurity
        | InternalError
        | InappropriateFallback
        | MissingExtension
        | UnsupportedExtension
        | BadCertificateStatusResponse
        | BadCertificateHashValue
        | UnknownPSKIdentity
        | UnrecognisedName
        | NoApplicationProtocol
        | EncryptedClientHelloRequired
        | ExportRestriction
        | ProtocolVersion => ErrorCategory::Security(SecurityClass::Integrity),
        DecompressionFailure => ErrorCategory::ResourceExhausted(BudgetKind::Flow),
        _ => retryable(Duration::from_millis(40)),
    }
}

fn categorize_io_error(error: &io::Error) -> ErrorCategory {
    use io::ErrorKind;
    match error.kind() {
        ErrorKind::WouldBlock | ErrorKind::Interrupted => retryable(Duration::from_millis(5)),
        ErrorKind::TimedOut | ErrorKind::UnexpectedEof => retryable(Duration::from_millis(40)),
        ErrorKind::ConnectionReset
        | ErrorKind::ConnectionAborted
        | ErrorKind::ConnectionRefused
        | ErrorKind::NotConnected
        | ErrorKind::BrokenPipe => retryable(Duration::from_millis(60)),
        ErrorKind::WriteZero | ErrorKind::OutOfMemory => {
            ErrorCategory::ResourceExhausted(BudgetKind::Flow)
        }
        _ => retryable(Duration::from_millis(25)),
    }
}

fn retryable(wait: Duration) -> ErrorCategory {
    ErrorCategory::Retryable(RetryAdvice::after(wait))
}
