//! RTCP 复合报文解析逻辑。
//!
//! # 教案定位（Why）
//! - 将零拷贝视图 (`BufView`) 转换为结构化的 [`RtcpPacket`](crate::packet::RtcpPacket) 列表，支撑 `spark-tck` 的协议契约测试。
//! - 解析流程在遇到异常时返回 [`RtcpError`](crate::error::RtcpError)，帮助实现者快速定位协议字段问题。
//!
//! # 契约说明（What）
//! - 函数入口仅依赖 `&impl BufView`，既可接收内存中单片缓冲，也可处理多分片 scatter/gather 视图。
//! - 输出为 `SmallVec`，默认在栈上容纳 4 个报文，超出时自动回退到堆。
//! - 当前实现覆盖 SR (200)、RR (201)、SDES (202)、BYE (203) 四类分组；其他类型会返回 `UnsupportedPacketType`。
//!
//! # 实现策略（How）
//! - 解析前先将 `BufView` 分片复制到连续缓冲，换取简洁的随机访问逻辑（详见函数注释中的权衡说明）。
//! - 对每个分组执行头部校验（版本号、长度、padding），随后分发到对应的解析器。
//! - 子解析函数复用辅助读取方法，确保对齐/长度检查与数据提取紧密绑定，减少遗漏的边界条件。

use alloc::{string::String, vec::Vec};
use core::{convert::TryInto, str};

use spark_codecs::buffer::BufView;

use crate::{
    RtcpPacketVec,
    error::RtcpError,
    new_packet_vec,
    packet::{
        Goodbye, ReceiverReport, ReceptionReport, RtcpPacket, SdesChunk, SdesItem, SenderInfo,
        SenderReport, SourceDescription,
    },
};

/// 解析结果默认的内联容量。
pub const DEFAULT_COMPOUND_CAPACITY: usize = 4;

/// 解析 RTCP 复合报文，将字节序列拆解成语义化的分组列表。
///
/// # 设计动机（Why）
/// - RTCP 复合报文常同时携带 SR/RR、SDES、BYE 等信息。将其解析为结构化枚举，便于上层逻辑在一次 IO 中获取所有控制状态。
/// - `spark-tck` 的契约测试需要验证解析对齐、字段含义与错误处理，因此该函数提供了详尽的校验与错误分类。
///
/// # 调用契约（What）
/// - **输入参数**：任何实现了 [`BufView`] 的只读缓冲视图。要求该视图生命周期至少覆盖解析过程，且 `len()` 为实际字节数。
/// - **返回值**：若解析成功，返回按出现顺序排列的 [`RtcpPacket`] 列表；否则返回 [`RtcpError`] 描述具体失败原因。
/// - **前置条件**：输入缓冲必须遵循 RFC3550：头部分片齐全、长度字段准确、首个分组为 SR 或 RR。
/// - **后置条件**：成功解析后，所有字节均被消费；调用方可直接按照 `SmallVec` 中的结构读取字段。
///
/// # 实现细节（How）
/// 1. 调用 [`BufView::as_chunks`] 将所有分片拷贝到临时 `Vec<u8>`，以便后续基于索引读取字段。（为何复制：RTCP 报文长度通常小于 MTU，复制开销可控；换取逻辑直观、易于审查。）
/// 2. 迭代缓冲，针对每个分组解析头部：校验版本、计算长度、处理 padding。
/// 3. 根据 `packet type` 派发至专用解析函数，并将结果推入 `SmallVec`。
/// 4. 若任一分组解析失败，立即返回对应错误，保证失败可溯源。
///
/// # 注意事项（Trade-offs & Gotchas）
/// - 目前仅支持常见的四类分组；若上游发送 APP/FIR 等类型会返回 `UnsupportedPacketType`。
/// - Padding 校验会检查末尾字节必须为零（符合规范推荐），否则返回 `TrailingNonZeroPadding`。
/// - 若输入为空或长度不足 4 字节，会分别触发 `EmptyCompound` 与 `PacketTooShort` 错误。
#[allow(clippy::too_many_lines)]
pub fn parse_rtcp(view: &(impl BufView + ?Sized)) -> spark_core::Result<RtcpPacketVec, RtcpError> {
    let buffer = flatten_view(view);

    if buffer.is_empty() {
        return Err(RtcpError::EmptyCompound);
    }

    let mut offset = 0usize;
    let mut packets: RtcpPacketVec = new_packet_vec();
    let mut first_packet = true;

    while offset < buffer.len() {
        let remaining = buffer.len() - offset;
        if remaining < 4 {
            return Err(RtcpError::PacketTooShort { remaining });
        }

        let first_byte = buffer[offset];
        let version = first_byte >> 6;
        if version != 2 {
            return Err(RtcpError::InvalidVersion { version });
        }

        let padding_flag = (first_byte & 0x20) != 0;
        let report_count = (first_byte & 0x1f) as usize;
        let packet_type = buffer[offset + 1];
        let length_words =
            u16::from_be_bytes(buffer[offset + 2..offset + 4].try_into().unwrap()) as usize;
        let packet_len = (length_words + 1) * 4;

        if packet_len > remaining {
            return Err(RtcpError::InvalidLengthField {
                declared_words: length_words,
                available_bytes: remaining,
            });
        }

        let payload_start = offset + 4;
        let mut payload_end = offset + packet_len;

        if padding_flag {
            let padding_size = buffer[payload_end - 1] as usize;
            let payload_len = packet_len.saturating_sub(4);
            if padding_size == 0 || padding_size > payload_len {
                return Err(RtcpError::InvalidPadding {
                    padding: padding_size as u8,
                    payload_len,
                });
            }

            let new_end = payload_end - padding_size;
            if buffer[new_end..payload_end].iter().any(|&byte| byte != 0) {
                return Err(RtcpError::TrailingNonZeroPadding { packet_type });
            }
            payload_end = new_end;
        }

        if payload_end < payload_start {
            return Err(RtcpError::InvalidPadding {
                padding: 0,
                payload_len: 0,
            });
        }

        let payload = &buffer[payload_start..payload_end];

        if first_packet && packet_type != 200 && packet_type != 201 {
            return Err(RtcpError::FirstPacketNotReport { packet_type });
        }

        first_packet = false;

        let packet = match packet_type {
            200 => RtcpPacket::SenderReport(parse_sender_report(report_count, payload)?),
            201 => RtcpPacket::ReceiverReport(parse_receiver_report(report_count, payload)?),
            202 => RtcpPacket::SourceDescription(parse_sdes(report_count, payload)?),
            203 => RtcpPacket::Goodbye(parse_bye(report_count, payload)?),
            _ => return Err(RtcpError::UnsupportedPacketType { packet_type }),
        };

        packets.push(packet);
        offset += packet_len;
    }

    Ok(packets)
}

fn flatten_view(view: &(impl BufView + ?Sized)) -> Vec<u8> {
    let mut buffer = Vec::with_capacity(view.len());
    for chunk in view.as_chunks() {
        buffer.extend_from_slice(chunk);
    }
    buffer
}

fn parse_sender_report(
    count: usize,
    payload: &[u8],
) -> spark_core::Result<SenderReport, RtcpError> {
    const FIXED_LEN: usize = 24;
    if payload.len() < FIXED_LEN {
        return Err(RtcpError::ReceptionReportTruncated {
            expected_blocks: count,
            available_bytes: payload.len(),
        });
    }

    let sender_ssrc = read_u32(&payload[0..4]);
    let ntp_high = read_u32(&payload[4..8]);
    let ntp_low = read_u32(&payload[8..12]);
    let rtp_timestamp = read_u32(&payload[12..16]);
    let sender_packet_count = read_u32(&payload[16..20]);
    let sender_octet_count = read_u32(&payload[20..24]);

    let mut cursor = FIXED_LEN;
    let required = count.saturating_mul(24);
    if payload.len().saturating_sub(cursor) < required {
        return Err(RtcpError::ReceptionReportTruncated {
            expected_blocks: count,
            available_bytes: payload.len().saturating_sub(cursor),
        });
    }

    let mut reports = Vec::with_capacity(count);
    for _ in 0..count {
        let block = &payload[cursor..cursor + 24];
        reports.push(parse_reception_report_block(block));
        cursor += 24;
    }

    let remaining = payload.len().saturating_sub(cursor);
    if remaining % 4 != 0 {
        return Err(RtcpError::ProfileExtensionMisaligned {
            remaining_bytes: remaining,
        });
    }

    let profile_extensions = payload[cursor..].to_vec();

    Ok(SenderReport {
        sender_ssrc,
        sender_info: SenderInfo {
            ntp_timestamp: ((ntp_high as u64) << 32) | ntp_low as u64,
            rtp_timestamp,
            sender_packet_count,
            sender_octet_count,
        },
        reports,
        profile_extensions,
    })
}

fn parse_receiver_report(
    count: usize,
    payload: &[u8],
) -> spark_core::Result<ReceiverReport, RtcpError> {
    if payload.len() < 4 {
        return Err(RtcpError::ReceptionReportTruncated {
            expected_blocks: count,
            available_bytes: payload.len(),
        });
    }

    let reporter_ssrc = read_u32(&payload[0..4]);
    let mut cursor = 4usize;
    let required = count.saturating_mul(24);
    if payload.len().saturating_sub(cursor) < required {
        return Err(RtcpError::ReceptionReportTruncated {
            expected_blocks: count,
            available_bytes: payload.len().saturating_sub(cursor),
        });
    }

    let mut reports = Vec::with_capacity(count);
    for _ in 0..count {
        let block = &payload[cursor..cursor + 24];
        reports.push(parse_reception_report_block(block));
        cursor += 24;
    }

    let remaining = payload.len().saturating_sub(cursor);
    if remaining % 4 != 0 {
        return Err(RtcpError::ProfileExtensionMisaligned {
            remaining_bytes: remaining,
        });
    }

    Ok(ReceiverReport {
        reporter_ssrc,
        reports,
        profile_extensions: payload[cursor..].to_vec(),
    })
}

fn parse_sdes(count: usize, payload: &[u8]) -> spark_core::Result<SourceDescription, RtcpError> {
    let mut cursor = 0usize;
    let mut chunks = Vec::with_capacity(count);

    for chunk_index in 0..count {
        if payload.len().saturating_sub(cursor) < 4 {
            return Err(RtcpError::SdesChunkTruncated {
                chunk_index,
                reason: "missing-ssrc",
            });
        }

        let source = read_u32(&payload[cursor..cursor + 4]);
        cursor += 4;

        let chunk_start = cursor;
        let mut items = Vec::new();

        loop {
            if cursor >= payload.len() {
                return Err(RtcpError::SdesChunkTruncated {
                    chunk_index,
                    reason: "missing-terminator",
                });
            }

            let item_type = payload[cursor];
            cursor += 1;

            if item_type == 0 {
                while (cursor - chunk_start) % 4 != 0 {
                    if cursor >= payload.len() {
                        return Err(RtcpError::SdesChunkTruncated {
                            chunk_index,
                            reason: "padding-out-of-range",
                        });
                    }
                    if payload[cursor] != 0 {
                        return Err(RtcpError::SdesPaddingCorrupted { chunk_index });
                    }
                    cursor += 1;
                }
                break;
            }

            if cursor >= payload.len() {
                return Err(RtcpError::SdesChunkTruncated {
                    chunk_index,
                    reason: "missing-item-length",
                });
            }

            let item_len = payload[cursor] as usize;
            cursor += 1;

            if payload.len().saturating_sub(cursor) < item_len {
                return Err(RtcpError::SdesChunkTruncated {
                    chunk_index,
                    reason: "item-value-truncated",
                });
            }

            let value = payload[cursor..cursor + item_len].to_vec();
            cursor += item_len;

            items.push(SdesItem { item_type, value });
        }

        chunks.push(SdesChunk { source, items });
    }

    if payload[cursor..].iter().any(|&byte| byte != 0) {
        return Err(RtcpError::TrailingNonZeroPadding { packet_type: 202 });
    }

    Ok(SourceDescription { chunks })
}

fn parse_bye(count: usize, payload: &[u8]) -> spark_core::Result<Goodbye, RtcpError> {
    let required = count.saturating_mul(4);
    if payload.len() < required {
        return Err(RtcpError::ByeSourcesTruncated {
            expected_sources: count,
            available_bytes: payload.len(),
        });
    }

    let mut cursor = 0usize;
    let mut sources = Vec::with_capacity(count);
    for _ in 0..count {
        sources.push(read_u32(&payload[cursor..cursor + 4]));
        cursor += 4;
    }

    let mut reason = None;
    if cursor < payload.len() {
        let reason_len = payload[cursor] as usize;
        cursor += 1;
        if payload.len().saturating_sub(cursor) < reason_len {
            return Err(RtcpError::ByeReasonTruncated {
                declared: reason_len,
                available: payload.len().saturating_sub(cursor),
            });
        }
        let text = &payload[cursor..cursor + reason_len];
        cursor += reason_len;
        if let Ok(decoded) = str::from_utf8(text) {
            reason = Some(String::from(decoded));
        }
    }

    if payload[cursor..].iter().any(|&byte| byte != 0) {
        return Err(RtcpError::TrailingNonZeroPadding { packet_type: 203 });
    }

    Ok(Goodbye { sources, reason })
}

fn parse_reception_report_block(block: &[u8]) -> ReceptionReport {
    let source_ssrc = read_u32(&block[0..4]);
    let fraction_lost = block[4];
    let cumulative_lost = parse_i24(&block[5..8]);
    let extended_highest_sequence = read_u32(&block[8..12]);
    let interarrival_jitter = read_u32(&block[12..16]);
    let last_sr_timestamp = read_u32(&block[16..20]);
    let delay_since_last_sr = read_u32(&block[20..24]);

    ReceptionReport {
        source_ssrc,
        fraction_lost,
        cumulative_lost,
        extended_highest_sequence,
        interarrival_jitter,
        last_sr_timestamp,
        delay_since_last_sr,
    }
}

fn read_u32(bytes: &[u8]) -> u32 {
    u32::from_be_bytes(bytes.try_into().expect("slice 长度必须为 4"))
}

fn parse_i24(bytes: &[u8]) -> i32 {
    let value = ((bytes[0] as i32) << 16) | ((bytes[1] as i32) << 8) | (bytes[2] as i32);
    if (value & 0x800000) != 0 {
        value | !0xFFFFFF
    } else {
        value
    }
}
