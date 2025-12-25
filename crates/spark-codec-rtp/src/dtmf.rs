#![allow(clippy::module_name_repetitions)]

//! # RFC 4733 电话事件（DTMF）编解码模块
//!
//! ## 设计意图（Why）
//! - **场景定位**：RTP 在承载 DTMF（Dual-tone Multi-frequency）信号时会将事件封装为 RFC 4733 定义的 4 字节单元，
//!   编解码器需要将原始 payload 与业务侧「事件/结束标志/音量/持续时间」信息互相转换；
//! - **架构角色**：该模块隶属于 `spark-codec-rtp`，补足 RTP 层对音视频负载以外的电话事件支持，
//!   供实现 TCK 与实际信令通路共享；
//! - **设计理念**：强调契约式校验——除了返回解析出的字段，还必须在遇到保留位、动态负载类型等违反 RFC 的情况时立刻报错，
//!   以免错误数据进一步污染业务流程。
//!
//! ## 交互契约（What）
//! - **输入来源**：来自 RTP Header 的 payload type (`pt`) 以及负载字节切片；
//! - **输出职责**：提供 `decode_dtmf`/`encode_dtmf` 双向接口，与 `DtmfEvent` 数据结构搭配使用；
//! - **前置条件**：调用方需保证 `pt` 处于 RFC 3551 规定的动态负载区间（96-127），payload 至少包含一个完整的 4 字节事件单元；
//! - **后置条件**：成功解析后返回结构化事件，编码成功则保证输出缓冲前 4 字节写入符合 RFC 4733 §2.3 的格式。
//!
//! ## 实现策略（How）
//! - **解析流程**：采用 `chunks_exact(4)` 逐块读取事件单元，仅保留最后一个块作为最终状态（与规范要求一致，最后一个块才携带最新的 `E`/`duration` 信息），
//!   期间校验所有块的事件号与保留位；
//! - **编码流程**：严格按照位级别打包 `event`、`E` 位、`volume` 与 `duration`，并返回写入的字节数以便上层进行偏移管理；
//! - **防御式校验**：针对非法输入（payload 不足、音量越界、保留位被置位等）分别暴露显式错误枚举，方便上层决定是否丢包或告警。
//!
//! ## 风险提示（Trade-offs）
//! - **多事件聚合**：规范允许在一个 RTP 包内重复发送相同事件，本实现会遍历所有块并断言事件号一致；
//! - **性能考量**：遍历 payload 时仅作按块读取与基本位运算，仍保持常数时间复杂度；
//! - **扩展可能**：若未来需要支持其他 Named Telephone Event，可在 `DtmfEvent` 上扩展新的语义字段，但需同步更新契约注释。

use core::fmt;

/// RFC 4733 电话事件的结构化表示。
///
/// ### 意图说明（Why）
/// - 调用方需要以强类型方式获取 DTMF 事件的各个字段，以便做拨号匹配或事件转发；
/// - 避免直接操作位域与字节切片，提高可维护性。
///
/// ### 功能契约（What）
/// - `event_code`：8 bit 的事件号，DTMF 0-15、扩展事件亦可映射在 0-255；
/// - `end`：指示该事件是否结束（E bit）；
/// - `volume`：瞬时音量（0-63，单位 dB）；
/// - `duration`：采样持续时间（单位：采样点，16 bit）。
///
/// ### 交互假设与结果（Contract）
/// - **前置条件**：构造时需保证 `volume <= 63`，`duration` 已按采样率转换为采样点数；
/// - **后置条件**：结构体仅保存原始字段，不包含额外推导信息，编码函数需再次校验 `volume` 范围。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DtmfEvent {
    /// 电话事件号（0-255）。
    pub event_code: u8,
    /// 是否携带事件结束标记（E bit）。
    pub end: bool,
    /// 原始音量值（0-63 dB）。
    pub volume: u8,
    /// 事件持续时间（采样点）。
    pub duration: u16,
}

impl DtmfEvent {
    /// 构造辅助函数，避免重复字段赋值。
    #[must_use]
    pub const fn new(event_code: u8, end: bool, volume: u8, duration: u16) -> Self {
        Self {
            event_code,
            end,
            volume,
            duration,
        }
    }
}

/// DTMF 解码错误，覆盖 RFC 4733 字节级校验的潜在失败场景。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DtmfDecodeError {
    /// Payload Type 不在动态负载区间。
    InvalidPayloadType(u8),
    /// 输入 payload 字节数不足以构成至少一个 4 字节事件块。
    PayloadTooShort,
    /// payload 长度不是 4 的整数倍，无法按块解析。
    MisalignedPayload,
    /// 任一事件块的保留位（bit7）被置位。
    ReservedBitSet,
    /// 同一 RTP 包内的事件号不一致，违反规范。
    InconsistentEvent,
}

impl fmt::Display for DtmfDecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidPayloadType(pt) => {
                write!(f, "DTMF payload type 必须位于 96-127 范围，实际为 {}", pt)
            }
            Self::PayloadTooShort => f.write_str("DTMF payload 长度不足 4 字节"),
            Self::MisalignedPayload => f.write_str("DTMF payload 长度必须是 4 的倍数"),
            Self::ReservedBitSet => f.write_str("DTMF payload 的保留位 (bit7) 被置位"),
            Self::InconsistentEvent => f.write_str("同一 RTP 包内出现多个不同事件号"),
        }
    }
}

/// DTMF 编码错误。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DtmfEncodeError {
    /// Payload Type 不在动态负载区间。
    InvalidPayloadType(u8),
    /// 输出缓冲区长度不足以容纳 4 字节事件块。
    BufferTooSmall,
    /// 音量字段超出 6 bit 限制。
    VolumeOutOfRange(u8),
}

impl fmt::Display for DtmfEncodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidPayloadType(pt) => {
                write!(f, "DTMF payload type 必须位于 96-127 范围，实际为 {}", pt)
            }
            Self::BufferTooSmall => f.write_str("输出缓冲区不足 4 字节"),
            Self::VolumeOutOfRange(v) => write!(f, "DTMF 音量字段必须位于 0-63，实际为 {}", v),
        }
    }
}

const DTMF_DYNAMIC_PAYLOAD_MIN: u8 = 96;
const DTMF_DYNAMIC_PAYLOAD_MAX: u8 = 127;
const DTMF_BLOCK_LEN: usize = 4;
const RESERVED_BIT_MASK: u8 = 0x80;
const END_BIT_MASK: u8 = 0x40;
const VOLUME_MASK: u8 = 0x3F;

fn ensure_payload_type(pt: u8) -> spark_core::Result<(), DtmfDecodeError> {
    if !(DTMF_DYNAMIC_PAYLOAD_MIN..=DTMF_DYNAMIC_PAYLOAD_MAX).contains(&pt) {
        return Err(DtmfDecodeError::InvalidPayloadType(pt));
    }
    Ok(())
}

fn ensure_payload_type_for_encode(pt: u8) -> spark_core::Result<(), DtmfEncodeError> {
    if !(DTMF_DYNAMIC_PAYLOAD_MIN..=DTMF_DYNAMIC_PAYLOAD_MAX).contains(&pt) {
        return Err(DtmfEncodeError::InvalidPayloadType(pt));
    }
    Ok(())
}

/// 解析 RFC 4733 电话事件负载。
///
/// ### 功能说明（What）
/// - **输入参数**：
///   - `payload_type`：RTP Header 的 PT 字段，必须是 96-127 的动态类型；
///   - `payload`：RTP 包内的负载切片，长度需为 4 的倍数；
/// - **返回值**：成功时输出 `DtmfEvent`，代表同一个事件的最终状态。
///
/// ### 行为契约（Contract）
/// - **前置条件**：payload 至少包含一个事件块，且所有块的事件号必须一致；
/// - **后置条件**：返回的事件体保证 `volume <= 63`，`duration` 为解析自最后一个块；
/// - **失败场景**：若触发任何 `DtmfDecodeError`，调用方应视为报文不合规并丢弃。
///
/// ### 实现要点（How）
/// - 使用 `chunks_exact(4)` 遍历所有事件块，逐块校验保留位与事件号一致性；
/// - 仅保留最后一个块的 `end`/`volume`/`duration` 作为最终输出；
/// - 解析过程中不复制 payload，完全基于引用与位运算。
pub fn decode_dtmf(
    payload_type: u8,
    payload: &[u8],
) -> spark_core::Result<DtmfEvent, DtmfDecodeError> {
    ensure_payload_type(payload_type)?;

    if payload.len() < DTMF_BLOCK_LEN {
        return Err(DtmfDecodeError::PayloadTooShort);
    }

    let chunks = payload.chunks_exact(DTMF_BLOCK_LEN);
    if !chunks.remainder().is_empty() {
        return Err(DtmfDecodeError::MisalignedPayload);
    }

    let mut last_event: Option<DtmfEvent> = None;

    for chunk in chunks {
        let [event_code, flags, hi, lo] = [chunk[0], chunk[1], chunk[2], chunk[3]];

        if (flags & RESERVED_BIT_MASK) != 0 {
            return Err(DtmfDecodeError::ReservedBitSet);
        }

        let parsed = DtmfEvent {
            event_code,
            end: (flags & END_BIT_MASK) != 0,
            volume: flags & VOLUME_MASK,
            duration: u16::from_be_bytes([hi, lo]),
        };

        if let Some(prev) = last_event
            && prev.event_code != parsed.event_code
        {
            return Err(DtmfDecodeError::InconsistentEvent);
        }

        last_event = Some(parsed);
    }

    last_event.ok_or(DtmfDecodeError::PayloadTooShort)
}

/// 编码单个 RFC 4733 电话事件块。
///
/// ### 功能说明（What）
/// - **输入参数**：
///   - `payload_type`：RTP Header 的 PT 字段，用于合法性校验；
///   - `event`：需要序列化的 `DtmfEvent`；
///   - `output`：可写的字节缓冲，至少 4 字节；
/// - **返回值**：成功时返回写入的字节数（固定为 4）。
///
/// ### 契约约束（Contract）
/// - **前置条件**：`payload_type` 位于 96-127，`event.volume <= 63`，`output.len() >= 4`；
/// - **后置条件**：缓冲前 4 字节写入 RFC 4733 §2.3 定义的字段，其他字节保持不变；
/// - **失败场景**：若不满足前置条件，则返回 `DtmfEncodeError`。
///
/// ### 实现要点（How）
/// - 通过位运算将 `end`/`volume` 合并到第二个字节；
/// - 使用 `u16::to_be_bytes` 写入持续时间，确保网络字节序；
/// - 返回写入长度以供上层偏移管理。
pub fn encode_dtmf(
    payload_type: u8,
    event: &DtmfEvent,
    output: &mut [u8],
) -> spark_core::Result<usize, DtmfEncodeError> {
    ensure_payload_type_for_encode(payload_type)?;

    if output.len() < DTMF_BLOCK_LEN {
        return Err(DtmfEncodeError::BufferTooSmall);
    }

    if event.volume > VOLUME_MASK {
        return Err(DtmfEncodeError::VolumeOutOfRange(event.volume));
    }

    let mut flags = event.volume & VOLUME_MASK;
    if event.end {
        flags |= END_BIT_MASK;
    }

    output[0] = event.event_code;
    output[1] = flags;
    let [hi, lo] = event.duration.to_be_bytes();
    output[2] = hi;
    output[3] = lo;

    Ok(DTMF_BLOCK_LEN)
}
