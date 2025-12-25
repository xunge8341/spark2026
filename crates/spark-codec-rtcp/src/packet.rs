//! RTCP 报文的结构化数据模型。
//!
//! # 教案定位（Why）
//! - 在解析过程中将二进制字段映射为具名结构，方便业务侧或测试用例直接读取语义化信息。
//! - 拆分出独立的数据模型模块，降低 `parse` 子模块的复杂度，使解析流程专注于字节处理。
//!
//! # 契约说明（What）
//! - 所有结构均面向“解析产物”，不承担编码职责；若未来需要生成 RTCP 报文，可在此基础上扩展构建器。
//! - 字段类型尽量贴近 RFC3550 中的定义，例如 NTP 时间戳使用 `u64`、`fraction_lost` 使用 `u8` 等。
//!
//! # 设计考量（How）
//! - 模型使用 `Vec`/`SmallVec` 等可在 `alloc` 环境中运行的数据结构，以兼容 `no_std`。
//! - 结构派生 `Clone`/`PartialEq`，便于测试直接断言解析结果；字段附带详细注释帮助理解协议语义。

use alloc::{string::String, vec::Vec};

/// 发送端统计信息，来源于 SR 报文固定的 20 字节字段。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SenderInfo {
    /// 64-bit NTP 时间戳，组合 `most significant word` 与 `least significant word`。
    pub ntp_timestamp: u64,
    /// RTP 时间戳，用于与媒体时钟对齐。
    pub rtp_timestamp: u32,
    /// 自上次报告以来累计发送的 RTP 包个数。
    pub sender_packet_count: u32,
    /// 自上次报告以来累计发送的载荷字节数。
    pub sender_octet_count: u32,
}

/// 接收报告块，既可出现在 SR 也可出现在 RR 中。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReceptionReport {
    /// 被报告的源（SSRC 或 CSRC）。
    pub source_ssrc: u32,
    /// 8-bit 的瞬时丢包比例。
    pub fraction_lost: u8,
    /// 累计丢失的 RTP 包数量（24-bit 有符号整数）。
    pub cumulative_lost: i32,
    /// 接收方观测到的最高序列号（含扩展序列）。
    pub extended_highest_sequence: u32,
    /// 估计的抖动值，单位为 RTP 时间戳刻度。
    pub interarrival_jitter: u32,
    /// 上一次接收到的 SR 的中间时间戳（LSR）。
    pub last_sr_timestamp: u32,
    /// 自上次 SR 到本次 RR 之间的延迟（单位：1/65536 秒）。
    pub delay_since_last_sr: u32,
}

/// Sender Report 报文的结构化表示。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SenderReport {
    /// 发送端自身的 SSRC。
    pub sender_ssrc: u32,
    /// 固定的发送统计信息。
    pub sender_info: SenderInfo,
    /// 跟随在报文后的接收报告块集合。
    pub reports: Vec<ReceptionReport>,
    /// 可选的 Profile-Specific 扩展字段（保持原始字节顺序）。
    pub profile_extensions: Vec<u8>,
}

/// Receiver Report 报文的结构化表示。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReceiverReport {
    /// 报告方的 SSRC。
    pub reporter_ssrc: u32,
    /// 接收报告块集合。
    pub reports: Vec<ReceptionReport>,
    /// Profile-Specific 扩展字段，遵循 32-bit 对齐约束。
    pub profile_extensions: Vec<u8>,
}

/// 单个 SDES 条目，由 item type 与值组成。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SdesItem {
    /// SDES item 的类型值（例如 1 对应 CNAME）。
    pub item_type: u8,
    /// 条目内容的原始字节。
    pub value: Vec<u8>,
}

/// SDES 分块，对应一个 SSRC 以及若干条目。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SdesChunk {
    /// 该分块对应的 SSRC 或 CSRC。
    pub source: u32,
    /// 条目列表，顺序与报文中一致。
    pub items: Vec<SdesItem>,
}

/// Source Description (SDES) 报文。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceDescription {
    /// 报文内包含的分块集合。
    pub chunks: Vec<SdesChunk>,
}

/// Goodbye (BYE) 报文。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Goodbye {
    /// 离开会话的 SSRC/CSRC 列表。
    pub sources: Vec<u32>,
    /// 可选的离开原因，使用 UTF-8 字符串存储；若原始数据不是合法 UTF-8，则保持为 `None`。
    pub reason: Option<String>,
}

/// RTCP 报文的枚举类型，涵盖当前解析器支持的全部变体。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RtcpPacket {
    /// Sender Report。
    SenderReport(SenderReport),
    /// Receiver Report。
    ReceiverReport(ReceiverReport),
    /// Source Description。
    SourceDescription(SourceDescription),
    /// Goodbye。
    Goodbye(Goodbye),
}
