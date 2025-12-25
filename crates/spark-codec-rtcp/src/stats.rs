//! RTCP 报告生成与统计映射辅助工具。
//!
//! # 教案定位（Why）
//! - 将“统计数据 → RTCP 报文”这一算法性逻辑集中管理，便于协议实现和测试套件复用相同的构建流程。
//! - 通过对 NTP/RTP 时间轴的显式描述，帮助读者理解 Sender Report 与 Receiver Report 之间的时间相关字段来源。
//!
//! # 模块契约（What）
//! - 暴露 `build_sr`/`build_rr` 两个构建函数，分别生成带统计字段的 Sender Report、Receiver Report 报文。
//! - 定义 `NtpTime`、`SenderStatistics`、`ReceptionStatistics` 等结构，明确上层调用方需要提供的统计指标格式。
//! - 约束调用方在输入时满足 RFC3550 对字段范围、对齐的要求；若数据超界，本模块会按规范执行饱和或默认降级处理。
//!
//! # 使用指引（How）
//! - 在采集统计后，构造 `SenderStatistics`/`ReceiverStatistics` 并调用对应的 `build_*` 函数，将结果推入 `RtcpPacketVec`。
//! - 若没有可用的 RTP 时钟，可提供自定义实现的 `RtpClock` 以完成 NTP→RTP 的时间映射。
//! - 对延迟、丢包等字段，本模块提供了单位换算和边界裁剪逻辑，调用方无需重复实现。
//!
//! # 风险提示（Trade-offs）
//! - 为兼顾 `no_std` 环境，模块使用 `alloc` 中的容器并避免标准库依赖；这要求调用方在 `no_std` 场景下启用 `alloc` 特性。
//! - `build_sr`/`build_rr` 会复制输入的切片数据，若报告量较大需留意堆分配开销；可在上层复用缓冲或启用对象池优化。

use alloc::vec::Vec;
use core::{cmp, time::Duration};

use crate::{ReceiverReport, ReceptionReport, RtcpPacket, RtcpPacketVec, SenderInfo, SenderReport};

/// RTP 媒体时钟抽象，用于在生成 SR 时完成 NTP 时间到 RTP 时间戳的映射。
///
/// ## 教案说明（Why）
/// - RFC3550 §6.3.1 要求 SR 同时携带 NTP 与 RTP 时间戳，用于音视频同步；实现者往往已经掌握一套“墙钟 → RTP”转换逻辑。
/// - 通过 trait 约定，将该转换逻辑注入构建流程，避免在 `build_sr` 内部硬编码采样率或时间基。
///
/// ## 契约约束（What）
/// - `to_rtp_timestamp` 输入为 NTP 时间（单位同 RFC3550，64-bit 秒+分数）；输出应为与媒体时钟同源的 RTP 时间戳。
/// - 调用前置条件：调用者需保证提供的 `NtpTime` 与当前媒体时钟处于同一时间轴；否则 RTCP 同步字段将出现偏差。
/// - 后置条件：返回的 `u32` 将直接写入 SR 报文，范围超界将按 `u32` 自然截断，调用方需自行保证合法性。
///
/// ## 风险提示（Trade-offs）
/// - 若媒体时钟支持跳变（例如重新同步），请在实现中自行处理 wrap-around，以保持 RTP 时间戳单调递增。
pub trait RtpClock {
    /// 将 NTP 表示的时间映射为 RTP 时间戳。
    fn to_rtp_timestamp(&self, ntp: &NtpTime) -> u32;
}

/// NTP 时间戳（64-bit，32 bit 秒 + 32 bit 分数）的小型封装。
///
/// ## 教案说明（Why）
/// - RTCP 在 SR 与 RR 中多次引用 NTP 时间字段，该结构帮助调用方以类型安全的方式传递值，同时隐藏位运算细节。
/// - 封装 `as_u64`/`lsr` 等转换方法，便于生成 SenderInfo 与接收报告块。
///
/// ## 契约定义（What）
/// - `seconds` 与 `fraction` 均为 32-bit 无符号整数，遵循 RFC3550 的布局约定。
/// - 可通过 `from_parts` 构造，也可从 `Duration` 等高层时间类型换算后传入。
/// - 前置条件：`seconds`、`fraction` 应来自同一时钟基准；若跨时钟混用，SR/RR 中的同步信息将失效。
/// - 后置条件：`as_u64` 返回的 64-bit 值可直接写入网络序字段；`lsr` 提供 compact 表示（低 32-bit）。
///
/// ## 风险提示（Trade-offs）
/// - 结构不主动校验 `fraction` 是否小于 `2^32`（由类型保证），调用方需自行负责单位换算的正确性。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NtpTime {
    seconds: u32,
    fraction: u32,
}

impl NtpTime {
    /// 依据给定的整数秒与小数部分构造 NTP 时间戳。
    pub const fn from_parts(seconds: u32, fraction: u32) -> Self {
        Self { seconds, fraction }
    }

    /// 返回整数秒部分。
    pub const fn seconds(&self) -> u32 {
        self.seconds
    }

    /// 返回小数部分。
    pub const fn fraction(&self) -> u32 {
        self.fraction
    }

    /// 以 64-bit 表示完整的 NTP 时间戳。
    pub const fn as_u64(&self) -> u64 {
        ((self.seconds as u64) << 32) | self.fraction as u64
    }

    /// 生成 RFC3550 中定义的 `LSR`（Last SR，32-bit Compact NTP）。
    pub const fn lsr(&self) -> u32 {
        ((self.seconds & 0xFFFF) << 16) | (self.fraction >> 16)
    }
}

/// 单个接收报告块的原始统计数据。
///
/// ## 意图阐述（Why）
/// - 提供 SR/RR 中 `reception report` 所需字段的载体，让调用者可以直接填入测量值。
/// - 聚合延迟、丢包、最高序列等指标，避免构建函数对上层逻辑有过多耦合。
///
/// ## 契约描述（What）
/// - `source_ssrc`：被报告方 SSRC；`fraction_lost`：8-bit 瞬时丢包率（0~255）。
/// - `cumulative_lost` 必须位于 `[-8_388_608, 8_388_607]` 范围；超界将被截断以遵循 24-bit 有符号整数约束。
/// - `last_sr`/`delay_since_last_sr`：若尚未接收到对端 SR，可填 `None`，构建器会在报文中写入 0。
/// - 前置条件：`delay_since_last_sr` 采用 `Duration`，其值不应超过可编码的 36 小时（`u32::MAX / 65536` 秒）；若超出将饱和至最大值。
/// - 后置条件：构建出的 `ReceptionReport` 会完全复制这些字段，并完成单位换算。
///
/// ## 风险提示（Trade-offs）
/// - 若上游统计窗口产生的 `cumulative_lost` 超出 24-bit 范围，本结构会饱和裁剪，可能导致信息损失；必要时可在上层拆分多份报告。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReceptionStatistics {
    /// 被观测的源 SSRC，用于对齐报告块与实际媒体流。
    pub source_ssrc: u32,
    /// 最近一个测量窗口的丢包比例，范围 0~255（即 0~100%）。
    pub fraction_lost: u8,
    /// 测量起点以来累计丢失的 RTP 包数量，会在构建时裁剪到 24-bit 有符号区间。
    pub cumulative_lost: i32,
    /// 接收方观测到的最高扩展序列号，用于估算发包进度。
    pub extended_highest_sequence: u32,
    /// 接收链路估算的抖动值，单位为 RTP 时间戳刻度。
    pub interarrival_jitter: u32,
    /// 最近一次接收到的 SR 的 NTP 时间戳；缺失时填 `None`。
    pub last_sr: Option<NtpTime>,
    /// 自上次 SR 至本次报告的等待时间，单位为真实时间；缺失时填 `None`。
    pub delay_since_last_sr: Option<Duration>,
}

/// 发送端在生成 SR 时需要提供的统计快照。
///
/// ## 教案定位（Why）
/// - 将 SR 固定字段（发送者 SSRC、数据量统计）与可变部分（接收报告、Profile 扩展）组合，清晰描述调用方的职责范围。
/// - `capture_ntp` 与 `clock` 搭配，实现 SR 中 NTP 与 RTP 时间戳的同步。
///
/// ## 契约定义（What）
/// - `sender_ssrc`：当前发送者的 SSRC 标识。
/// - `capture_ntp`：生成报告时刻的 NTP 时间戳；必须与 `clock` 使用同一时间基准。
/// - `rtp_timestamp_override`：若上层已计算对应 RTP 时间戳，可在此覆盖；否则 `build_sr` 会调用 `clock` 推算。
/// - `reports`：跟随在 SR 后的接收报告块集合；可为空。
/// - `profile_extensions`：可选的 profile-specific 扩展数据，长度需满足 32-bit 对齐要求（本模块不强制检查，调用方需自律）。
/// - 前置条件：统计计数应遵循无符号 32-bit 限制（SR 规范要求）。
/// - 后置条件：`build_sr` 将把数据复制进新的 `SenderReport`，并推入外部提供的输出缓冲。
///
/// ## 风险提示（Trade-offs）
/// - Profile 扩展会产生堆分配；若频繁构建 SR，可重用缓冲或在更上层聚合复合报文以减少分配次数。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SenderStatistics<'a> {
    /// 当前发送者的 SSRC 标识。
    pub sender_ssrc: u32,
    /// 生成报告时刻的 NTP 时间戳。
    pub capture_ntp: NtpTime,
    /// 若上层已计算出的 RTP 时间戳，可在此覆盖默认映射逻辑。
    pub rtp_timestamp_override: Option<u32>,
    /// 自上次报告以来累计发送的包数。
    pub sender_packet_count: u32,
    /// 自上次报告以来累计发送的载荷字节数。
    pub sender_octet_count: u32,
    /// 跟随在 SR 之后的接收报告块集合。
    pub reports: &'a [ReceptionStatistics],
    /// 可选的 profile-specific 扩展字节序列。
    pub profile_extensions: &'a [u8],
}

/// Receiver Report 所需的统计快照。
///
/// ## 教案说明（Why）
/// - 将报告方 SSRC、接收报告列表、profile 扩展打包，便于 `build_rr` 直接消费。
/// - 与 `SenderStatistics` 保持一致的字段组织，降低调用方心智负担。
///
/// ## 契约描述（What）
/// - `reporter_ssrc`：生成该 RR 的实体 SSRC。
/// - `reports`：接收报告块集合；可为空。
/// - `profile_extensions`：保留 profile-specific 字节序列，同样要求 32-bit 对齐（本模块不做强检）。
/// - 前置条件：上层需保证报告块指标已按 RFC3550 范围换算。
/// - 后置条件：生成的 `ReceiverReport` 会复制所有字段并推入输出缓冲。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReceiverStatistics<'a> {
    /// 报告方（接收端）的 SSRC。
    pub reporter_ssrc: u32,
    /// 接收报告块集合，可为空数组表示“无下游统计”。
    pub reports: &'a [ReceptionStatistics],
    /// Profile-specific 扩展字段原始字节序列。
    pub profile_extensions: &'a [u8],
}

/// 构建带统计字段的 Sender Report 并写入输出集合。
///
/// ## 教案讲解（Why）
/// - 将 SR 构建流程模块化，确保测试与实际实现共享同一套逻辑，降低重复代码与偏差风险。
/// - 在构建过程中完成 NTP → RTP 时间戳映射，保证跨媒体流同步的准确性。
///
/// ## 契约细节（What）
/// - `clock`：实现了 `RtpClock` 的时间映射器。
/// - `stats`：发送端统计快照。
/// - `out`：输出缓冲（通常为 `RtcpPacketVec`），函数会向其中追加一个 `RtcpPacket::SenderReport`。
/// - 前置条件：`stats.capture_ntp` 与 `clock` 使用同一时基；`stats.profile_extensions` 已按 32-bit 对齐。
/// - 后置条件：`out` 中追加的报文满足 RFC3550 §6.3.1 的字段要求；若传入的统计为空，报文仍包含基本 SenderInfo。
///
/// ## 实现步骤（How）
/// 1. 计算 RTP 时间戳：优先使用 `rtp_timestamp_override`，否则调用 `clock` 进行映射；
/// 2. 将 `SenderStatistics` 中的计数直接填入 `SenderInfo`；
/// 3. 遍历 `reports` 列表，调用内部辅助函数生成 `ReceptionReport`；
/// 4. 复制 profile 扩展后推入输出缓冲。
///
/// ## 风险提示（Trade-offs）
/// - 若 `clock` 与统计时间轴不一致，会导致 RTP 时间戳偏移，从而破坏同步；调用方在集成前需确保两者协同。
/// - 接收报告数量较多时会分配新的 `Vec`，可视需求复用缓冲以降低开销。
pub fn build_sr<C>(clock: &C, stats: &SenderStatistics<'_>, out: &mut RtcpPacketVec)
where
    C: RtpClock,
{
    let rtp_timestamp = stats
        .rtp_timestamp_override
        .unwrap_or_else(|| clock.to_rtp_timestamp(&stats.capture_ntp));

    let sender_info = SenderInfo {
        ntp_timestamp: stats.capture_ntp.as_u64(),
        rtp_timestamp,
        sender_packet_count: stats.sender_packet_count,
        sender_octet_count: stats.sender_octet_count,
    };

    let reports = stats
        .reports
        .iter()
        .map(reception_from_stats)
        .collect::<Vec<_>>();

    out.push(RtcpPacket::SenderReport(SenderReport {
        sender_ssrc: stats.sender_ssrc,
        sender_info,
        reports,
        profile_extensions: stats.profile_extensions.to_vec(),
    }));
}

/// 构建带统计字段的 Receiver Report 并写入输出集合。
///
/// ## 教案讲解（Why）
/// - Receiver Report 与 Sender Report 在接收报告块生成逻辑上高度一致，抽象函数便于复用并减少逻辑分叉。
/// - 通过统一入口，测试可以轻松验证 RR 中延迟、LSR 等字段的换算是否正确。
///
/// ## 契约细节（What）
/// - `stats`：RR 统计快照。
/// - `out`：输出缓冲，函数会向其中追加 `RtcpPacket::ReceiverReport`。
/// - 前置条件：`stats.profile_extensions` 已满足 32-bit 对齐要求。
/// - 后置条件：生成的 `ReceiverReport` 满足 RFC3550 §6.4.1 的字段约束，未提供的 LSR/DLSR 字段会按规范写零。
///
/// ## 实现步骤（How）
/// 1. 遍历 `stats.reports`，调用共享的 `reception_from_stats` 构造接收报告块；
/// 2. 复制 profile 扩展字节；
/// 3. 以 `RtcpPacket::ReceiverReport` 形式压入输出缓冲。
///
/// ## 风险提示（Trade-offs）
/// - 若输入统计缺失 `last_sr` 信息，RR 中的 LSR 与 DLSR 会是 0；确保接收链路在统计数据齐备时再调用可提高同步精度。
pub fn build_rr(stats: &ReceiverStatistics<'_>, out: &mut RtcpPacketVec) {
    let reports = stats
        .reports
        .iter()
        .map(reception_from_stats)
        .collect::<Vec<_>>();

    out.push(RtcpPacket::ReceiverReport(ReceiverReport {
        reporter_ssrc: stats.reporter_ssrc,
        reports,
        profile_extensions: stats.profile_extensions.to_vec(),
    }));
}

fn reception_from_stats(stats: &ReceptionStatistics) -> ReceptionReport {
    ReceptionReport {
        source_ssrc: stats.source_ssrc,
        fraction_lost: stats.fraction_lost,
        cumulative_lost: clamp_cumulative_lost(stats.cumulative_lost),
        extended_highest_sequence: stats.extended_highest_sequence,
        interarrival_jitter: stats.interarrival_jitter,
        last_sr_timestamp: stats.last_sr.map_or(0, |ntp| ntp.lsr()),
        delay_since_last_sr: stats
            .delay_since_last_sr
            .map_or(0, encode_delay_since_last_sr),
    }
}

pub(super) const MIN_CUMULATIVE_LOST: i32 = -0x80_0000; // -8_388_608
pub(super) const MAX_CUMULATIVE_LOST: i32 = 0x7F_FFFF; // 8_388_607

fn clamp_cumulative_lost(value: i32) -> i32 {
    value.clamp(MIN_CUMULATIVE_LOST, MAX_CUMULATIVE_LOST)
}

fn encode_delay_since_last_sr(delay: Duration) -> u32 {
    let secs_component = delay.as_secs();
    let nanos_component = delay.subsec_nanos();

    let coarse = secs_component.saturating_mul(65_536);
    let fine = ((nanos_component as u64) * 65_536) / 1_000_000_000;
    let total = coarse.saturating_add(fine);

    cmp::min(total, u32::MAX as u64) as u32
}

#[cfg(feature = "std")]
pub mod raw {
    //! 针对字节级 RTCP 报文构建的工具集，仅在 `std` 环境中启用。
    //!
    //! # 教案级导航
    //! - **定位 (Why)**：补充高阶 `Vec<u8>` 编码入口，方便终端或网络栈直接生成可发送的 SR/RR 报文；
    //! - **协作 (How)**：复用上层采集的统计数据与 [`RtpClockMapper`] 提供的时间映射，将结构化信息编码为网络序字节；
    //! - **契约 (What)**：输出缓冲需由调用方提供并复用，函数在成功路径下仅追加数据；
    //! - **风险提示**：计算过程涉及时间换算与边界检查，若输入不满足约束会返回 [`BuildError`]。

    use alloc::vec::Vec;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    use super::{MAX_CUMULATIVE_LOST, MIN_CUMULATIVE_LOST};

    /// RTCP 的固定版本号（RFC3550 §6.1）。
    const RTCP_VERSION: u8 = 2;

    /// RTCP Sender Report 的分组类型常量（RFC3550 §6.4.1）。
    const RTCP_SR_PACKET_TYPE: u8 = 200;

    /// RTCP Receiver Report 的分组类型常量（RFC3550 §6.4.2）。
    const RTCP_RR_PACKET_TYPE: u8 = 201;

    /// NTP 时间戳与 Unix 纪元之间的秒差（1970-01-01 与 1900-01-01 的间隔）。
    const NTP_UNIX_EPOCH_DELTA_SECS: u64 = 2_208_988_800;

    /// 构造 RTCP 统计报文过程中可能出现的错误。
    ///
    /// ### Why
    /// - 发送/接收报告的编码存在多处协议约束，直接 `panic` 会给上层带来不可控的崩溃风险。
    /// - 将错误枚举化后，调用方可以在调试阶段快速定位违反协议的根因。
    ///
    /// ### What
    /// - 每个变体对应一类契约违规：报告块数量超限、扩展字段未按 32-bit 对齐、时间回退等。
    /// - 错误在 [`build_sr_raw`] / [`build_rr_raw`] 中返回，调用方可据此选择回滚或丢弃统计。
    ///
    /// ### How
    /// - 仅使用 `#[derive(Debug, Clone, PartialEq, Eq)]`，便于单元测试直接断言；
    /// - 未实现 `std::error::Error`，因为该模块面向嵌入式/测试环境，不强制依赖标准库错误体系。
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub enum BuildError {
        /// 报告块数量超出 5 bit 所能表示的范围（RFC3550 上限 31）。
        TooManyReports {
            /// 实际提供的报告块数量。
            count: usize,
        },
        /// Profile 扩展长度不是 32-bit 对齐，违反 §6.4.1/§6.4.2 的对齐约束。
        MisalignedProfileExtension {
            /// Profile 扩展字段的原始字节长度。
            len: usize,
        },
        /// `SystemTime` 早于 1900-01-01，无法映射为合法的 NTP 时间戳。
        SystemTimeBeforeNtpEpoch,
        /// 采样时间早于 [`RtpClockMapper`] 的参考起点，意味着时钟倒退。
        ClockWentBackwards,
        /// 接收报告中的 `cumulative_lost` 超出 24-bit 有符号整数的表示范围。
        CumulativeLostOutOfRange {
            /// 调用方传入的累计丢包值。
            value: i32,
        },
    }
    /// RTP 与 NTP 时间映射器。
    ///
    /// ### 意图说明（Why）
    /// - SR 需要同时携带 64-bit NTP 时间戳与 RTP 时钟刻度，二者必须严格对应同一采样时刻。
    /// - 通过集中管理起始时间与时钟频率，可以确保所有调用者共享一致的换算规则。
    ///
    /// ### 契约（What）
    /// - `origin_time`：对应 `origin_rtp` 的绝对时间，用于之后的差值计算；
    /// - `origin_rtp`：RTP 时间戳零点（或任意参考点），允许在 32-bit 范围内回绕；
    /// - `clock_rate`：RTP 时钟频率（单位：ticks/秒），必须为正数；
    /// - `snapshot`：将任意 `SystemTime` 映射为 `(ntp_timestamp, rtp_timestamp)`。
    ///
    /// ### 实现细节（How）
    /// - 差值计算基于整数运算：`ticks = secs * clock_rate + nanos * clock_rate / 1e9`；
    /// - NTP 时间戳通过向 Unix 时间加上固定偏移（2208988800 秒）获得；
    /// - `rtp_timestamp` 使用 `wrapping_add` 处理 32-bit 回绕。
    ///
    /// ### 风险提示（Trade-offs）
    /// - 假设调用方提供的 `SystemTime` 精度不低于纳秒；若底层平台精度更低可能导致 RTP tick 取整误差；
    /// - 当 `clock_rate` 极大时（> 4GHz）`u128` 计算仍然安全，但输出转换为 `u32` 时会发生截断，符合 RTP 模 2^32 的语义。
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct RtpClockMapper {
        origin_time: SystemTime,
        origin_rtp: u32,
        clock_rate: u32,
    }

    impl RtpClockMapper {
        /// 使用给定的起点与时钟频率构造映射器。
        ///
        /// ### 输入说明
        /// - `origin_time`：定义 RTP 时间戳 `origin_rtp` 对应的绝对时间。
        /// - `origin_rtp`：选择的参考 RTP 时间戳，可从媒体初始化阶段获取。
        /// - `clock_rate`：媒体时钟频率；若为 0 表示缺失配置，将在生成时直接返回错误。
        ///
        /// ### 行为描述
        /// - 构造函数不执行额外校验，仅保存参数；
        /// - `clock_rate = 0` 不会立即报错，但在调用 `snapshot` 时会导致 RTP 时间差始终为 0。
        #[must_use]
        pub const fn new(origin_time: SystemTime, origin_rtp: u32, clock_rate: u32) -> Self {
            Self {
                origin_time,
                origin_rtp,
                clock_rate,
            }
        }

        /// 返回内部记录的 RTP 时钟频率。
        #[must_use]
        pub const fn clock_rate(&self) -> u32 {
            self.clock_rate
        }

        /// 将指定的采样时间映射为 NTP 与 RTP 时间戳快照。
        ///
        /// ### 合同说明
        /// - **输入**：任意 `SystemTime`，通常来自媒体帧的采样时间；
        /// - **输出**：`Ok((ntp_timestamp, rtp_timestamp))`，两者均已经过 mod 处理；
        /// - **前置条件**：`capture_time >= origin_time`；否则返回 [`BuildError::ClockWentBackwards`]；
        /// - **后置条件**：返回的 NTP/RTP 时间戳指向相同的物理时刻，可直接写入 SR。
        pub fn snapshot(
            &self,
            capture_time: SystemTime,
        ) -> spark_core::Result<(u64, u32), BuildError> {
            let ntp = system_time_to_ntp(capture_time)?;
            let delta = capture_time
                .duration_since(self.origin_time)
                .map_err(|_| BuildError::ClockWentBackwards)?;
            let ticks = duration_to_rtp_ticks(delta, self.clock_rate);
            let rtp_timestamp = self.origin_rtp.wrapping_add(ticks as u32);
            Ok((ntp, rtp_timestamp))
        }
    }

    /// 发送端统计输入，描述 SR 报文需要携带的所有字段。
    ///
    /// ### Why
    /// - 将 SR 所需的元数据聚合成结构体，可在调用处显式声明每个字段的含义，避免魔法常量。
    ///
    /// ### What
    /// - `sender_ssrc`：发送端自身的同步源标识；
    /// - `capture_time`：用于计算 NTP/RTP 时间戳的采样时间；
    /// - `sender_packet_count`/`sender_octet_count`：RFC3550 §6.4.1 的统计字段；
    /// - `reports`：SR 中携带的接收报告块集合；
    /// - `profile_extensions`：可选的 Profile-Specific 扩展，需要由 32-bit 边界对齐。
    ///
    /// ### 前置/后置条件
    /// - **前置**：`reports.len() <= 31`、`profile_extensions.len()` 必须是 4 的倍数；
    /// - **后置**：成功生成的 SR 将把所有字段按照网络字节序写入输出缓冲。
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct SenderStat<'a> {
        /// 发送端 SSRC。
        pub sender_ssrc: u32,
        /// 采样时间，使用 [`RtpClockMapper`] 映射为 NTP/RTP 时间戳。
        pub capture_time: SystemTime,
        /// 自上次报告以来累计发送的 RTP 包个数。
        pub sender_packet_count: u32,
        /// 自上次报告以来累计发送的负载字节数。
        pub sender_octet_count: u32,
        /// 接收报告块集合。
        pub reports: &'a [ReceptionStat],
        /// Profile-Specific 扩展字段（已对齐到 32-bit）。
        pub profile_extensions: &'a [u8],
    }

    /// 接收端统计输入，描述 RR 报文所需的字段。
    ///
    /// ### Why
    /// - RR 不含发送统计，但仍需要聚合报告块与扩展字段，结构体保持与 SR 对称便于复用测试。
    ///
    /// ### 契约（What）
    /// - `reporter_ssrc`：上报该 RR 的节点；
    /// - `reports`：针对不同源的接收报告块；
    /// - `profile_extensions`：Profile-Specific 扩展，同样要求 32-bit 对齐。
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct ReceiverStat<'a> {
        /// 报告方 SSRC。
        pub reporter_ssrc: u32,
        /// 接收报告块集合。
        pub reports: &'a [ReceptionStat],
        /// Profile 扩展原始字节。
        pub profile_extensions: &'a [u8],
    }

    /// 接收报告块的输入结构，映射为 24 字节的 `ReceptionReport`。
    ///
    /// ### 契约说明
    /// - 字段与 [`crate::packet::ReceptionReport`] 一一对应；
    /// - `cumulative_lost` 采用 24-bit 有符号整数语义，范围 `[-8388608, 8388607]`。
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct ReceptionStat {
        /// 被观测源的同步源标识。
        pub source_ssrc: u32,
        /// 上一统计间隔的瞬时丢包率。
        pub fraction_lost: u8,
        /// 累计丢包数量（24-bit 有符号整数范围）。
        pub cumulative_lost: i32,
        /// 接收方观测到的最高扩展序列号。
        pub extended_highest_sequence: u32,
        /// 接收端估计的到达抖动。
        pub interarrival_jitter: u32,
        /// 最近一次接收到的 SR 时间戳（LSR）。
        pub last_sr_timestamp: u32,
        /// 从 LSR 到当前 RR 的间隔，单位 1/65536 秒。
        pub delay_since_last_sr: u32,
    }

    /// 构造 Sender Report 报文并写入输出缓冲。
    ///
    /// ### 使用说明（How）
    /// 1. 调用方准备 [`RtpClockMapper`] 与 [`SenderStat`]；
    /// 2. 函数计算 NTP/RTP 时间戳、写入 header、固定字段、报告块以及扩展字段；
    /// 3. 若任一步骤违反契约则返回 [`BuildError`]。
    ///
    /// ### 前置条件（What）
    /// - `sender_stat.reports.len() <= 31`；
    /// - `sender_stat.profile_extensions.len()` 为 4 的倍数；
    /// - `sender_stat.capture_time` 不早于 `clock.origin_time` 且不早于 1900-01-01。
    ///
    /// ### 后置条件
    /// - `out` 追加完整的 SR 报文，长度等于 `(length + 1) * 4`；
    /// - 输出缓冲未进行清空操作，允许调用方复用同一个 `Vec` 生成多个报文。
    pub fn build_sr_raw(
        clock: &RtpClockMapper,
        sender_stat: &SenderStat<'_>,
        out: &mut Vec<u8>,
    ) -> spark_core::Result<(), BuildError> {
        validate_report_constraints(sender_stat.reports, sender_stat.profile_extensions)?;
        let (ntp_timestamp, rtp_timestamp) = clock.snapshot(sender_stat.capture_time)?;

        let report_count = sender_stat.reports.len();
        let payload_len = 4  // sender_ssrc
            + 20 // sender info
            + report_count * 24
            + sender_stat.profile_extensions.len();
        let total_len = 4 + payload_len;
        let length_field = bytes_to_rtcp_length(total_len);

        write_header(out, report_count as u8, RTCP_SR_PACKET_TYPE, length_field);
        out.extend_from_slice(&sender_stat.sender_ssrc.to_be_bytes());
        out.extend_from_slice(&ntp_timestamp.to_be_bytes());
        out.extend_from_slice(&rtp_timestamp.to_be_bytes());
        out.extend_from_slice(&sender_stat.sender_packet_count.to_be_bytes());
        out.extend_from_slice(&sender_stat.sender_octet_count.to_be_bytes());
        write_reception_reports(out, sender_stat.reports)?;
        out.extend_from_slice(sender_stat.profile_extensions);
        Ok(())
    }
    /// 构造 Receiver Report 报文并写入输出缓冲。
    ///
    /// ### 契约摘要
    /// - 校验报告块数量与扩展字段对齐；
    /// - 无需进行 NTP/RTP 映射；
    /// - 输出缓冲追加完整 RR 报文。
    pub fn build_rr_raw(
        recv_stat: &ReceiverStat<'_>,
        out: &mut Vec<u8>,
    ) -> spark_core::Result<(), BuildError> {
        validate_report_constraints(recv_stat.reports, recv_stat.profile_extensions)?;
        let report_count = recv_stat.reports.len();
        let payload_len = 4 // reporter_ssrc
            + report_count * 24
            + recv_stat.profile_extensions.len();
        let total_len = 4 + payload_len;
        let length_field = bytes_to_rtcp_length(total_len);

        write_header(out, report_count as u8, RTCP_RR_PACKET_TYPE, length_field);
        out.extend_from_slice(&recv_stat.reporter_ssrc.to_be_bytes());
        write_reception_reports(out, recv_stat.reports)?;
        out.extend_from_slice(recv_stat.profile_extensions);
        Ok(())
    }

    /// 校验报告块数量与 Profile 扩展长度是否满足协议约束。
    fn validate_report_constraints(
        reports: &[ReceptionStat],
        profile_extensions: &[u8],
    ) -> spark_core::Result<(), BuildError> {
        if reports.len() > 31 {
            return Err(BuildError::TooManyReports {
                count: reports.len(),
            });
        }
        if profile_extensions.len() % 4 != 0 {
            return Err(BuildError::MisalignedProfileExtension {
                len: profile_extensions.len(),
            });
        }
        Ok(())
    }

    /// 将字节长度转换为 RTCP 头部的 `length` 字段值。
    fn bytes_to_rtcp_length(total_len: usize) -> u16 {
        debug_assert_eq!(total_len % 4, 0, "RTCP 报文长度必须按 32-bit 对齐");
        ((total_len / 4) - 1) as u16
    }

    /// 写入 RTCP 报文头部。
    fn write_header(out: &mut Vec<u8>, report_count: u8, packet_type: u8, length_field: u16) {
        let v_p_count = (RTCP_VERSION << 6) | (report_count & 0x1F);
        out.extend_from_slice(&[
            v_p_count,
            packet_type,
            (length_field >> 8) as u8,
            (length_field & 0xFF) as u8,
        ]);
    }

    /// 写入接收报告块列表。
    fn write_reception_reports(
        out: &mut Vec<u8>,
        reports: &[ReceptionStat],
    ) -> spark_core::Result<(), BuildError> {
        for report in reports {
            out.extend_from_slice(&report.source_ssrc.to_be_bytes());
            out.push(report.fraction_lost);
            let cumulative_lost = encode_cumulative_lost(report.cumulative_lost)?;
            out.extend_from_slice(&cumulative_lost);
            out.extend_from_slice(&report.extended_highest_sequence.to_be_bytes());
            out.extend_from_slice(&report.interarrival_jitter.to_be_bytes());
            out.extend_from_slice(&report.last_sr_timestamp.to_be_bytes());
            out.extend_from_slice(&report.delay_since_last_sr.to_be_bytes());
        }
        Ok(())
    }

    /// 将 24-bit 的 `cumulative_lost` 编码为网络字节序字节数组。
    fn encode_cumulative_lost(value: i32) -> spark_core::Result<[u8; 3], BuildError> {
        if !(MIN_CUMULATIVE_LOST..=MAX_CUMULATIVE_LOST).contains(&value) {
            return Err(BuildError::CumulativeLostOutOfRange { value });
        }
        let encoded = (value as i64) & 0x00FF_FFFF;
        Ok([
            ((encoded >> 16) & 0xFF) as u8,
            ((encoded >> 8) & 0xFF) as u8,
            (encoded & 0xFF) as u8,
        ])
    }

    /// 将 `SystemTime` 转换为 64-bit NTP 时间戳。
    fn system_time_to_ntp(time: SystemTime) -> spark_core::Result<u64, BuildError> {
        let since_unix = time
            .duration_since(UNIX_EPOCH)
            .map_err(|_| BuildError::SystemTimeBeforeNtpEpoch)?;
        let total_secs = since_unix
            .as_secs()
            .saturating_add(NTP_UNIX_EPOCH_DELTA_SECS);
        let fractional = ((since_unix.subsec_nanos() as u128) << 32) / 1_000_000_000u128;
        Ok((total_secs << 32) | (fractional as u64))
    }

    /// 将持续时间转换为 RTP tick 数量。
    fn duration_to_rtp_ticks(duration: Duration, clock_rate: u32) -> u128 {
        if clock_rate == 0 {
            return 0;
        }
        let rate = clock_rate as u128;
        let whole = duration.as_secs() as u128 * rate;
        let frac = (duration.subsec_nanos() as u128 * rate) / 1_000_000_000u128;
        whole + frac
    }
}
