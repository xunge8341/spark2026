use crate::Error;
use alloc::borrow::Cow;
use core::fmt;

/// 配置体系的统一错误类型。
///
/// ### 设计目标（Why）
/// - 将数据源交互、解析、合并等环节的错误映射为稳定枚举，便于跨语言桥接与监控上报。
/// - 借鉴 AWS AppConfig、Envoy xDS 的错误分层（来源错误 / 验证错误 / 变更冲突）。
///
/// ### 逻辑解析（How）
/// - `Source`：数据源读取失败，通常由网络、权限等引起。
/// - `Decode`：原始数据解析失败，提供上下文说明。
/// - `Validation`：业务规则校验失败，例如缺失必填字段。
/// - `Conflict`：多源合并发生冲突，需要人工介入。
/// - `Audit`：审计链路写入失败，需要上层触发重试或人工干预。
///
/// ### 契约说明（What）
/// - 实现 [`crate::Error`]，在 `no_std` 环境中保持错误链的可追踪性。
/// - `context` 使用 `Cow`，允许常量与动态拼接并存。
///
/// ### 设计权衡（Trade-offs）
/// - 未内置错误码，避免强加企业内部规范；建议在上层约定错误字典。
#[derive(Debug)]
#[non_exhaustive]
pub enum ConfigurationError {
    Source { context: Cow<'static, str> },
    Decode { context: Cow<'static, str> },
    Validation { context: Cow<'static, str> },
    Conflict { context: Cow<'static, str> },
    Audit { context: Cow<'static, str> },
}

impl ConfigurationError {
    /// 创建带上下文的错误。
    pub fn with_context<C>(kind: ConfigurationErrorKind, context: C) -> Self
    where
        C: Into<Cow<'static, str>>,
    {
        match kind {
            ConfigurationErrorKind::Source => Self::Source {
                context: context.into(),
            },
            ConfigurationErrorKind::Decode => Self::Decode {
                context: context.into(),
            },
            ConfigurationErrorKind::Validation => Self::Validation {
                context: context.into(),
            },
            ConfigurationErrorKind::Conflict => Self::Conflict {
                context: context.into(),
            },
            ConfigurationErrorKind::Audit => Self::Audit {
                context: context.into(),
            },
        }
    }

    /// 返回错误类别。
    pub fn kind(&self) -> ConfigurationErrorKind {
        match self {
            Self::Source { .. } => ConfigurationErrorKind::Source,
            Self::Decode { .. } => ConfigurationErrorKind::Decode,
            Self::Validation { .. } => ConfigurationErrorKind::Validation,
            Self::Conflict { .. } => ConfigurationErrorKind::Conflict,
            Self::Audit { .. } => ConfigurationErrorKind::Audit,
        }
    }

    /// 获取上下文字符串。
    pub fn context(&self) -> &str {
        match self {
            Self::Source { context }
            | Self::Decode { context }
            | Self::Validation { context }
            | Self::Conflict { context }
            | Self::Audit { context } => context,
        }
    }
}

impl fmt::Display for ConfigurationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.kind(), self.context())
    }
}

impl Error for ConfigurationError {
    #[allow(unused_parens)]
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        None
    }
}

/// 辅助枚举，用于构造统一的错误上下文。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum ConfigurationErrorKind {
    Source,
    Decode,
    Validation,
    Conflict,
    Audit,
}

impl fmt::Display for ConfigurationErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            Self::Source => "source",
            Self::Decode => "decode",
            Self::Validation => "validation",
            Self::Conflict => "conflict",
            Self::Audit => "audit",
        };
        f.write_str(name)
    }
}

/// 注册配置源时可能出现的错误。
///
/// ### 设计目的（Why）
/// - 约束 Builder 在注册阶段即可发现问题，而非推迟到加载阶段。
/// - 对齐 Envoy LDS/CDS 的注册校验流程，提前阻断潜在冲突。
///
/// ### 契约说明（What）
/// - `Duplicate`：同一标识的源被重复注册。
/// - `Capacity`：超出事先声明的容量限制。
#[derive(Debug)]
#[non_exhaustive]
pub enum SourceRegistrationError {
    Duplicate,
    Capacity,
}

impl fmt::Display for SourceRegistrationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Duplicate => f.write_str("duplicate configuration source"),
            Self::Capacity => f.write_str("configuration source capacity exceeded"),
        }
    }
}

impl Error for SourceRegistrationError {
    #[allow(unused_parens)]
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        None
    }
}

const _: fn() = || {
    fn assert_error_traits<T: Error + Send + Sync + 'static>() {}

    assert_error_traits::<ConfigurationError>();
    assert_error_traits::<SourceRegistrationError>();
};
