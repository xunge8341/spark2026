#![cfg_attr(not(feature = "std"), no_std)]
#![warn(missing_docs)]

//! # spark-codec-rtcp
//!
//! ## 教案目的（Why）
//! - **定位**：Real-time Transport Control Protocol (RTCP) 控制平面的编解码骨架，负责媒体质量反馈与会话控制消息。
//! - **架构角色**：与 RTP 数据面配对，提供统计、拥塞控制和同步信息，保障实时会话体验。
//! - **设计策略**：在占位结构之上补充 RTCP 复合包解析能力，使 `spark-tck` 可以加载真实测试载荷。
//!
//! ## 交互契约（What）
//! - **依赖输入**：依托 `spark-codecs` 聚合的 [`BufView`](spark_codecs::buffer::BufView) 实现，从零拷贝缓冲视图中提取字节流。
//! - **输出职责**：提供 `parse_rtcp` 函数，将 SR/RR/SDES/BYE 等报文解析为结构化的 [`RtcpPacket`] 列表。
//! - **前置约束**：运行环境需具备与 RTP 相同的时钟/缓冲支持；当 `no_std` 启用时必须提供 `alloc` 支持以保存解析结果。
//!
//! ## 实现策略（How）
//! - **实施步骤**：
//!   1. 通过 `RtcpCodecScaffold` 固定 API 入口；
//!   2. 新增 `packet`/`error`/`parse` 子模块，定义结构化报文模型与诊断错误；
//!   3. 在解析过程中将 `BufView` 分片折叠为临时缓冲，并基于 RFC3550 的字段布局提取语义信息。
//! - **技术考量**：解析流程遵守 32-bit 对齐、Padding 标志、Report Count 等协议约束，并在异常时返回精确错误类型。
//!
//! ## 风险提示（Trade-offs）
//! - **内存权衡**：解析阶段需将分片复制到临时缓冲以便随机访问，牺牲部分内存以换取实现直观性与可维护性。
//! - **演进风险**：控制报文格式复杂，后续扩展需严谨验证字段长度与对齐；当前仅覆盖常见的 SR/RR/SDES/BYE 报文。
//! - **维护建议**：新增报文类型时务必同步更新测试与文档，防止协议偏差，同时补充 `RtcpError` 分支以便诊断。

extern crate alloc;

mod error;
mod packet;
mod parse;
#[cfg(feature = "std")]
mod stats;

pub use crate::{
    error::RtcpError,
    packet::{
        Goodbye, ReceiverReport, ReceptionReport, RtcpPacket, SdesChunk, SdesItem, SenderInfo,
        SenderReport, SourceDescription,
    },
    parse::{DEFAULT_COMPOUND_CAPACITY, parse_rtcp},
};

#[cfg(feature = "std")]
pub use crate::stats::{
    NtpTime, ReceiverStatistics, ReceptionStatistics, RtpClock, SenderStatistics, build_rr,
    build_sr,
};

#[cfg(feature = "std")]
pub use crate::stats::raw::{
    BuildError, ReceiverStat, ReceptionStat, RtpClockMapper, SenderStat, build_rr_raw, build_sr_raw,
};

/// RTCP 编解码占位结构，约定控制平面实现入口。
///
/// ### 设计意图（Why）
/// - 让调用方可以立即引用 RTCP 模块，提前规划依赖结构。
/// - 与 RTP/SIP/SDP 形成完整的实时通讯协议栈。
///
/// ### 契约描述（What）
/// - 结构体当前不包含任何状态，纯粹作为类型锚点。
/// - 后续将扩展统计窗口、时钟同步参数等字段。
///
/// ### 实现细节（How）
/// - 维持零尺寸类型，结合 `Default`/`Copy` 简化测试注入。
/// - `const fn` 构造器保证可在静态上下文中创建实例。
///
/// ### 风险提示（Trade-offs）
/// - 随着控制逻辑复杂化，可能需要拆分为多个组件或引入内部引用计数。
#[derive(Debug, Default, Clone, Copy)]
pub struct RtcpCodecScaffold;

impl RtcpCodecScaffold {
    /// 构造 RTCP 编解码占位实例。
    ///
    /// ### 设计动机（Why）
    /// - 为 `spark-tck` 即将添加的控制面测试准备构造入口。
    /// - 统一向调用方暴露实例化方法，减少后续 breaking change 的概率。
    ///
    /// ### 契约定义（What）
    /// - **输入**：无。
    /// - **输出**：返回 `RtcpCodecScaffold`，代表一个可用的占位对象。
    /// - **前置条件**：仅需完成编译期链接，无运行时依赖。
    /// - **后置条件**：调用方持有一个可复制的标记类型，可用于占位实现或泛型约束。
    ///
    /// ### 实现说明（How）
    /// - 选择 `const fn` 提升可用性，在常量上下文或静态变量中均可使用。
    ///
    /// ### 风险提示（Trade-offs）
    /// - 若未来构造函数需要校验参数，请将返回类型调整为 `Result` 并补充错误语义。
    #[must_use]
    pub const fn new() -> Self {
        Self
    }
}

/// 构造解析返回值的小型向量。
///
/// ### 教案说明（Why）
/// - 复合 RTCP 报文通常仅包含少量控制分组（SR、SDES、BYE 等），使用 `SmallVec` 能将前几个分组直接存储在栈上，避免频繁堆分配。
/// - 与 `parse_rtcp` 返回类型共享，调用方可以统一引用该别名简化类型签名阅读。
///
/// ### 合同约束（What）
/// - `RtcpPacketVec` 的容量与 `DEFAULT_COMPOUND_CAPACITY` 相同，确保至少容纳典型的 3~4 个分组。
/// - 超过内联容量时自动回退至堆分配，语义与 `SmallVec` 一致。
///
/// ### 注意事项（Trade-offs）
/// - 若未来复合包显著增多，需要酌情调整默认容量或改用 `Vec`。
pub type RtcpPacketVec = smallvec::SmallVec<[RtcpPacket; DEFAULT_COMPOUND_CAPACITY]>;

/// 构造空的 [`RtcpPacketVec`]，便于无调用方直接引用 `smallvec` 依赖。
#[must_use]
pub fn new_packet_vec() -> RtcpPacketVec {
    smallvec::SmallVec::new()
}
