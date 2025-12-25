//! RTCP 解析错误类型定义。
//!
//! # 教案定位（Why）
//! - 解析 RTCP 报文时需要向上游明确失败原因，以便 TCK 或业务方在回归测试中快速定位异常字段。
//! - 与 `parse` 模块解耦，避免解析逻辑与错误枚举相互污染，保持模块职责单一。
//!
//! # 使用契约（What）
//! - `RtcpError` 每个分支对应 RFC3550 中的校验规则，调用方可以据此判断是输入数据损坏、报文类型尚未支持，还是实现 bug。
//! - 所有变体均实现 `Clone`/`PartialEq`，便于测试直接断言具体错误类型。
//!
//! # 设计考量（How）
//! - 错误枚举仅存储简单的整型数据或静态字符串，确保在 `no_std` 环境中也能正常使用。
//! - 通过实现 `Display` 输出友好提示，配合 `#[cfg(feature = "std")]` 的 `Error` 实现接入常规错误栈。

use core::fmt;

/// RTCP 解析过程中可能出现的错误。
///
/// ## 教案解读（Why）
/// - 每个分支对应协议的关键约束：例如版本号固定为 2、长度字段以 32-bit 为单位、SDES 需要 0 结束符等。
/// - 明确的错误类型能够帮助排查外部系统的协议偏差，同时在编写解析逻辑时形成自检清单。
///
/// ## 契约定义（What）
/// - 所有错误均表示“当前报文无法成功解析”，调用方不得继续使用输出结构。
/// - 触发错误后不应假定解析函数已经消费全部输入；调用方如果需要，可记录原始字节以供诊断。
///
/// ## 注意事项（Trade-offs）
/// - 目前仅覆盖 SR/RR/SDES/BYE 相关的错误分类。后续扩展新报文类型时，应新增对应分支，以免错误落入过于笼统的分类。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RtcpError {
    /// 输入缓冲为空或在读取头部前已经耗尽。
    EmptyCompound,
    /// 剩余字节不足以解析 RTCP 头部（固定 4 字节）。
    PacketTooShort {
        /// 当前缓冲中剩余的字节数。
        remaining: usize,
    },
    /// 报文版本号不是 RFC3550 规定的 2。
    InvalidVersion {
        /// 实际读到的版本号。
        version: u8,
    },
    /// 长度字段与剩余数据不匹配，无法得到完整报文。
    InvalidLengthField {
        /// 报文声明的 32-bit 字长数（不含头部的隐式 +1）。
        declared_words: usize,
        /// 输入缓冲剩余字节数，用于定位截断问题。
        available_bytes: usize,
    },
    /// Padding 标志位开启，但末尾字节声明的填充长度无效。
    InvalidPadding {
        /// 声明的填充字节数。
        padding: u8,
        /// 实际 payload 长度（不含头部）。
        payload_len: usize,
    },
    /// 第一个 RTCP 分组不是 SR 或 RR，违反复合报文约定。
    FirstPacketNotReport {
        /// 第一个分组的类型值。
        packet_type: u8,
    },
    /// 当前实现尚未支持的 RTCP 报文类型。
    UnsupportedPacketType {
        /// 未识别的类型值。
        packet_type: u8,
    },
    /// `report count` 与剩余数据不匹配，导致接收报告块被截断。
    ReceptionReportTruncated {
        /// 声明的接收报告块数量。
        expected_blocks: usize,
        /// 剩余可用字节数。
        available_bytes: usize,
    },
    /// SR/RR 在报告块之后仍存在无法按 32-bit 对齐的扩展字段。
    ProfileExtensionMisaligned {
        /// 扩展字段的原始字节长度。
        remaining_bytes: usize,
    },
    /// SDES 分块在解析过程中被截断（缺少 SSRC、长度或终止标记）。
    SdesChunkTruncated {
        /// 发生问题的分块索引（从 0 开始）。
        chunk_index: usize,
        /// 静态说明原因的标签，帮助诊断具体缺失的部分。
        reason: &'static str,
    },
    /// SDES 分块在终止标记之后出现非零字节，说明 padding 被破坏。
    SdesPaddingCorrupted {
        /// 发生问题的分块索引。
        chunk_index: usize,
    },
    /// BYE 报文的 SSRC/CSRC 列表被截断。
    ByeSourcesTruncated {
        /// 声明的源数量。
        expected_sources: usize,
        /// 剩余字节数。
        available_bytes: usize,
    },
    /// BYE 原因字符串长度超过剩余字节数。
    ByeReasonTruncated {
        /// 字符串声明长度。
        declared: usize,
        /// 实际剩余字节数。
        available: usize,
    },
    /// 报文剩余字节不是零填充，意味着报文格式被破坏。
    TrailingNonZeroPadding {
        /// 首个非零填充字节的分组类型。
        packet_type: u8,
    },
}

impl fmt::Display for RtcpError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyCompound => f.write_str("RTCP 复合报文为空"),
            Self::PacketTooShort { remaining } => {
                write!(f, "剩余字节 ({remaining}) 不足以解析 RTCP 头部")
            }
            Self::InvalidVersion { version } => {
                write!(f, "RTCP 版本号 {version} 非法，期望值为 2")
            }
            Self::InvalidLengthField {
                declared_words,
                available_bytes,
            } => {
                write!(
                    f,
                    "长度字段声明 {declared_words} 个 32-bit 单元，但仅剩 {available_bytes} 字节"
                )
            }
            Self::InvalidPadding {
                padding,
                payload_len,
            } => {
                write!(
                    f,
                    "Padding 标志开启但声明填充 {padding} 超过 payload 长度 {payload_len}"
                )
            }
            Self::FirstPacketNotReport { packet_type } => {
                write!(
                    f,
                    "复合报文首分组类型 {packet_type} 不是 Sender/Receiver Report"
                )
            }
            Self::UnsupportedPacketType { packet_type } => {
                write!(f, "当前实现不支持的 RTCP 类型 {packet_type}")
            }
            Self::ReceptionReportTruncated {
                expected_blocks,
                available_bytes,
            } => {
                write!(
                    f,
                    "接收报告数量 {expected_blocks} 与剩余 {available_bytes} 字节不匹配"
                )
            }
            Self::ProfileExtensionMisaligned { remaining_bytes } => {
                write!(f, "SR/RR 扩展字段长度 {remaining_bytes} 不是 32-bit 对齐")
            }
            Self::SdesChunkTruncated {
                chunk_index,
                reason,
            } => {
                write!(f, "SDES 分块 {chunk_index} 被截断，缺少 {reason}")
            }
            Self::SdesPaddingCorrupted { chunk_index } => {
                write!(f, "SDES 分块 {chunk_index} 的填充区出现非零字节")
            }
            Self::ByeSourcesTruncated {
                expected_sources,
                available_bytes,
            } => {
                write!(
                    f,
                    "BYE 声明 {expected_sources} 个源，但仅剩 {available_bytes} 字节"
                )
            }
            Self::ByeReasonTruncated {
                declared,
                available,
            } => {
                write!(
                    f,
                    "BYE 原因字段声明长度 {declared}，但仅剩 {available} 字节"
                )
            }
            Self::TrailingNonZeroPadding { packet_type } => {
                write!(
                    f,
                    "RTCP 类型 {packet_type} 的尾部填充包含非零字节，违反规范"
                )
            }
        }
    }
}

#[cfg(feature = "std")]
impl std::error::Error for RtcpError {}
