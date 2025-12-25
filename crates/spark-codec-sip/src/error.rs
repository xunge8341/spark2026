//! 错误类型模块。
//!
//! ## 模块目的（Why）
//! - 将解析/格式化阶段的所有失败分门别类，便于调用方通过模式匹配采取补救措施。
//! - 统一错误展示文案，配合 `Display`/`Error` trait，方便在 `anyhow` 等错误栈中串联。
//!
//! ## 使用契约（What）
//! - 解析相关 API 返回 [`SipParseError`]；格式化相关 API 返回 [`SipFormatError`]。
//! - 错误枚举不携带对输入缓冲的引用，避免生命周期难题，且可在日志中安全复制。
//!
//! ## 实现策略（How）
//! - `SipParseError` 聚焦于语法层失败，按照起始行、头部、URI 等分类，便于快速定位。
//! - `SipFormatError` 目前仅涵盖写入失败，未来扩展复杂格式化逻辑时可继续细分。
//!
//! ## 风险提示（Trade-offs）
//! - 若未来需要记录原始错误上下文（例如具体的 header 名称），需权衡与 no_std 场景下分配的取舍。
//! - 当前实现偏重于分类而非详细定位，调用方若需精确位置，可在更高层自行补充。

use core::fmt;

use spark_codecs::error::{self, ErrorCategory};

/// 将 SIP 解析或事务错误映射为结构化的 [`ErrorCategory`]。
///
/// # 教案式说明
/// - **意图 (Why)**：框架的自动响应器依据 [`ErrorCategory`] 决定是触发取消、退避还是关闭。
///   若解析层或事务层错误无法与错误矩阵对齐，将导致默认策略失效。
/// - **实现 (How)**：各错误类型根据与矩阵中稳定错误码的对应关系，调用内部的
///   `category_for_code` 查表函数得到分类；这样可以与 `contracts/error_matrix.toml`
///   中的声明保持同步。
/// - **契约 (What)**：实现者必须保证返回值稳定且无副作用；分类结果主要用于日志、指标与
///   自动化处理器，调用方无需额外清理资源。
pub trait ToErrorCategory {
    /// 返回与错误码矩阵一致的分类信息。
    fn to_error_category(&self) -> ErrorCategory;
}

/// 统一的错误码查表辅助函数。
fn category_for_code(code: &str) -> ErrorCategory {
    error::category_matrix::entry_for_code(code)
        .map(error::category_matrix::CategoryMatrixEntry::category)
        .unwrap_or(ErrorCategory::NonRetryable)
}

/// SIP 解析阶段可能出现的错误枚举。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SipParseError {
    /// 请求行不符合 `METHOD SP URI SP SIP/2.0` 格式。
    InvalidRequestLine,
    /// 响应行不符合 `SIP/2.0 SP Status-Code SP Reason-Phrase` 格式。
    InvalidStatusLine,
    /// 版本号既非 `SIP/2.0` 也不符合兼容要求。
    UnsupportedVersion,
    /// URI 文本无法根据 §19 解析。
    InvalidUri,
    /// 头部名称存在非法字符或缺失冒号。
    InvalidHeaderName,
    /// 头部值为空或违反 §7.3.1 折行规则。
    InvalidHeaderValue,
    /// `Via` 头无法按照 §18.1 解析（如缺失 sent-by）。
    InvalidViaHeader,
    /// `From`/`To` 头无法解析为 name-addr。
    InvalidNameAddrHeader,
    /// `Call-ID` 头无有效 token。
    InvalidCallId,
    /// `CSeq` 头格式不符合 `<number> SP <method>`。
    InvalidCSeq,
    /// `Max-Forwards` 头不是合法的 32 位整数。
    InvalidMaxForwards,
    /// `Contact` 头缺少 URI 或参数格式错误。
    InvalidContact,
    /// 在 header 区域前过早遇到 EOF 或 CRLF 分隔缺失。
    UnexpectedEof,
}

impl fmt::Display for SipParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidRequestLine => {
                write!(f, "请求行格式错误，期望 `METHOD SP URI SP SIP/2.0`")
            }
            Self::InvalidStatusLine => {
                write!(f, "状态行格式错误，期望 `SIP/2.0 SP Status-Code SP Reason`")
            }
            Self::UnsupportedVersion => write!(f, "仅支持 SIP/2.0 版本号"),
            Self::InvalidUri => write!(f, "URI 解析失败，不符合 RFC 3261 §19"),
            Self::InvalidHeaderName => write!(f, "头部名称非法或缺失冒号"),
            Self::InvalidHeaderValue => write!(f, "头部值违反 RFC 3261 §7.3.1 约束"),
            Self::InvalidViaHeader => write!(f, "Via 头部无法按照 RFC 3261 §18.1 解析"),
            Self::InvalidNameAddrHeader => write!(f, "From/To 头部不是合法的 name-addr 形式"),
            Self::InvalidCallId => write!(f, "Call-ID 头部缺少有效 token"),
            Self::InvalidCSeq => write!(f, "CSeq 头部格式错误，需形如 `<number> <method>`"),
            Self::InvalidMaxForwards => write!(f, "Max-Forwards 需为 0-2^32-1 范围内整数"),
            Self::InvalidContact => write!(f, "Contact 头部缺少 URI 或参数非法"),
            Self::UnexpectedEof => write!(f, "头部或报文意外结束"),
        }
    }
}

#[cfg(feature = "std")]
impl std::error::Error for SipParseError {}

/// SIP 文本序列化阶段的错误。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SipFormatError {
    /// 底层 `fmt::Write` 写入失败。
    Io,
    /// body 非 UTF-8 编码，当前写入器仅支持文本输出。
    NonUtf8Body,
}

impl fmt::Display for SipFormatError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io => write!(f, "写入器返回错误，序列化失败"),
            Self::NonUtf8Body => write!(f, "body 不是合法的 UTF-8 文本"),
        }
    }
}

#[cfg(feature = "std")]
impl std::error::Error for SipFormatError {}

impl From<fmt::Error> for SipFormatError {
    fn from(_: fmt::Error) -> Self {
        Self::Io
    }
}

impl From<core::str::Utf8Error> for SipFormatError {
    fn from(_: core::str::Utf8Error) -> Self {
        Self::NonUtf8Body
    }
}

impl ToErrorCategory for SipParseError {
    fn to_error_category(&self) -> ErrorCategory {
        // 所有解析错误均映射到协议违规，驱动默认的关闭策略。
        category_for_code("protocol.decode")
    }
}

/// SIP 事务阶段可能出现的错误枚举。
///
/// # 教案式注释
/// - **Why**：CANCEL 竞态会暴露“事务已终止”“最终响应冲突”等边界，若没有统一错误枚举，
///   上层无法根据错误类型判断是否传播取消或回滚。
/// - **Where**：位于编解码层，供 TCK 与实现共享，确保竞态处理分支保持一致。
/// - **What**：`TransactionTerminated` 表示事务资源已释放；`FinalResponseConflict` 表示在 CANCEL
///   生成 487 后仍尝试发送其它最终响应；`NoMatchingInvite` 则代表 CANCEL 无对应事务。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SipTransactionError {
    /// CANCEL 无匹配的 INVITE 事务。
    NoMatchingInvite,
    /// 事务已终止，无法再处理 CANCEL 或最终响应。
    TransactionTerminated,
    /// 事务已生成最终响应（通常因 CANCEL 生成 487），但仍有新的响应写入。
    FinalResponseConflict {
        /// 已经登记的最终响应状态码。
        existing: u16,
        /// 试图再次写入的状态码。
        attempted: u16,
    },
}

impl fmt::Display for SipTransactionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoMatchingInvite => write!(f, "找不到与 CANCEL 对应的 INVITE 事务"),
            Self::TransactionTerminated => write!(f, "事务已终止，禁止追加事件"),
            Self::FinalResponseConflict {
                existing,
                attempted,
            } => write!(f, "最终响应冲突：已记录 {existing}，拒绝新的 {attempted}",),
        }
    }
}

#[cfg(feature = "std")]
impl std::error::Error for SipTransactionError {}

impl ToErrorCategory for SipTransactionError {
    fn to_error_category(&self) -> ErrorCategory {
        match self {
            Self::NoMatchingInvite => category_for_code("protocol.decode"),
            Self::TransactionTerminated => category_for_code("runtime.shutdown"),
            Self::FinalResponseConflict { .. } => category_for_code("runtime.shutdown"),
        }
    }
}
