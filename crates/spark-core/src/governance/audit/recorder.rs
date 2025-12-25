use alloc::borrow::Cow;
use core::fmt;

use super::event::AuditEventV1;

/// Recorder 接口定义。
///
/// ## 设计动机（Why）
/// - 框架只关心“事件被可靠写入”，至于写到哪里由业务定制，例如本地文件、Kafka、对象存储等。
///
/// ## 契约说明（What）
/// - `record`：同步写入事件。若返回错误，上游会视为此次变更失败并触发重试或熔断。
/// - `flush`：可选的刷新钩子，默认返回成功；文件 Recorder 可在此同步落盘。
pub trait AuditRecorder: Send + Sync {
    fn record(&self, event: AuditEventV1) -> crate::Result<(), AuditError>;

    fn flush(&self) -> crate::Result<(), AuditError> {
        Ok(())
    }
}

/// Recorder 返回的错误类型。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuditError {
    message: Cow<'static, str>,
}

impl AuditError {
    /// 创建带上下文的错误实例。
    pub fn new<M>(message: M) -> Self
    where
        M: Into<Cow<'static, str>>,
    {
        Self {
            message: message.into(),
        }
    }

    /// 返回错误描述。
    pub fn message(&self) -> &str {
        &self.message
    }
}

impl fmt::Display for AuditError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

#[cfg(feature = "std")]
impl std::error::Error for AuditError {}
