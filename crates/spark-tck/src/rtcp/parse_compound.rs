//! RTCP 复合报文解析测试。
//!
//! # 教案定位（Why）
//! - 验证 `spark-codec-rtcp::parse_rtcp` 能够正确拆解 Sender Report、SDES、BYE 等典型组合。
//! - 同时校验错误分支（例如首分组不是报告类时应立即失败），确保实现与 RFC3550 的约束保持一致。
//!
//! # 覆盖范围（What）
//! - `parse_sr_sdes_bye_compound`：解析一个包含 SR、SDES、BYE 的复合报文，逐字段断言解析结果。
//! - `reject_compound_without_report`：验证首分组不是 SR/RR 时返回 `FirstPacketNotReport` 错误。
//!
//! # 测试技巧（How）
//! - 报文字节通过 Python 脚本生成，避免手动计算长度字段时出错；固定在常量中便于复用。
//! - 使用 `assert_eq!` 搭配结构体字段断言，保证解析输出的契约稳定可靠。

use spark_codec_rtcp::{
    Goodbye, RtcpError, RtcpPacket, SenderReport, SourceDescription, parse_rtcp,
};

/// 包含 SR + SDES + BYE 的复合 RTCP 报文。
const COMPOUND_PACKET: [u8; 96] = [
    0x81, 0xC8, 0x00, 0x0C, 0x01, 0x02, 0x03, 0x04, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88,
    0x99, 0xAA, 0xBB, 0xCC, 0x00, 0x00, 0x00, 0x10, 0x00, 0x00, 0x01, 0x00, 0x0A, 0x0B, 0x0C, 0x0D,
    0x05, 0x00, 0x01, 0x02, 0x12, 0x34, 0x56, 0x78, 0x00, 0x00, 0x10, 0x00, 0x45, 0x67, 0x89, 0xAB,
    0x00, 0x00, 0x12, 0x34, 0x81, 0xCA, 0x00, 0x05, 0x01, 0x02, 0x03, 0x04, 0x01, 0x0A, 0x61, 0x6C,
    0x69, 0x63, 0x65, 0x40, 0x76, 0x6F, 0x69, 0x70, 0x00, 0x00, 0x00, 0x00, 0x81, 0xCB, 0x00, 0x04,
    0x0A, 0x0B, 0x0C, 0x0D, 0x08, 0x73, 0x68, 0x75, 0x74, 0x64, 0x6F, 0x77, 0x6E, 0x00, 0x00, 0x00,
];

/// 首分组非报告类型的最小化 RTCP 报文。
const BYE_ONLY: [u8; 4] = [0x80, 0xCB, 0x00, 0x00];

/// 解析 SR+SDES+BYE 复合报文并逐字段校验，验证复合报文处理逻辑。
///
/// # 教案式说明
/// - **意图（Why）**：确保 `parse_rtcp` 能正确拆解符合 RFC 3550 的复合报文，
///   否则统计数据将因字段解析错误而产生偏差。
/// - **流程（How）**：调用解析函数得到分组数组，针对 SR、SDES、BYE 各自断言关键字段。
/// - **契约（What）**：函数无参，若断言失败会 panic；成功表示解析逻辑满足契约。
///   - 前置条件：调用方需在 `std` 环境运行（复用解析器依赖的分配）；
///   - 后置条件：若无 panic，则保证解析器至少支持包含 SR、SDES、BYE 的复合报文。
/// - **边界考量（Trade-offs）**：示例报文覆盖常见字段，若要验证扩展项需要额外测试。
pub fn assert_parse_sr_sdes_bye_compound() {
    let packets = parse_rtcp(&COMPOUND_PACKET[..]).expect("解析 SR+SDES+BYE 复合报文应成功");
    assert_eq!(packets.len(), 3, "复合报文应包含 3 个分组");

    match &packets[0] {
        RtcpPacket::SenderReport(SenderReport {
            sender_ssrc,
            sender_info,
            reports,
            profile_extensions,
        }) => {
            assert_eq!(*sender_ssrc, 0x0102_0304);
            assert_eq!(sender_info.ntp_timestamp, 0x1122_3344_5566_7788);
            assert_eq!(sender_info.rtp_timestamp, 0x99AA_BBCC);
            assert_eq!(sender_info.sender_packet_count, 0x10);
            assert_eq!(sender_info.sender_octet_count, 0x100);
            assert_eq!(reports.len(), 1);
            let report = &reports[0];
            assert_eq!(report.source_ssrc, 0x0A0B_0C0D);
            assert_eq!(report.fraction_lost, 0x05);
            assert_eq!(report.cumulative_lost, 0x0000_0102);
            assert_eq!(report.extended_highest_sequence, 0x1234_5678);
            assert_eq!(report.interarrival_jitter, 0x0000_1000);
            assert_eq!(report.last_sr_timestamp, 0x4567_89AB);
            assert_eq!(report.delay_since_last_sr, 0x0000_1234);
            assert!(profile_extensions.is_empty(), "示例报文未携带扩展字段");
        }
        other => panic!("首分组应解析为 SenderReport，实际为 {other:?}"),
    }

    match &packets[1] {
        RtcpPacket::SourceDescription(SourceDescription { chunks }) => {
            assert_eq!(chunks.len(), 1);
            let chunk = &chunks[0];
            assert_eq!(chunk.source, 0x0102_0304);
            assert_eq!(chunk.items.len(), 1);
            let item = &chunk.items[0];
            assert_eq!(item.item_type, 0x01, "示例使用 CNAME 条目");
            let text = String::from_utf8(item.value.clone()).expect("CNAME 应为合法 UTF-8");
            assert_eq!(text, "alice@voip");
        }
        other => panic!("第二个分组应为 SDES，实际为 {other:?}"),
    }

    match &packets[2] {
        RtcpPacket::Goodbye(Goodbye { sources, reason }) => {
            assert_eq!(sources, &[0x0A0B_0C0D]);
            assert_eq!(reason.as_deref(), Some("shutdown"));
        }
        other => panic!("第三个分组应为 BYE，实际为 {other:?}"),
    }
}

/// 检验当复合报文首分组不是报告类时是否返回 `FirstPacketNotReport` 错误。
///
/// # 教案式说明
/// - **意图（Why）**：RFC 3550 要求复合报文以 SR/RR 开头，若实现忽略该规则可能导致安全漏洞。
/// - **流程（How）**：将仅包含 BYE 的报文输入解析函数，并断言错误类型。
/// - **契约（What）**：出错时应准确返回 `RtcpError::FirstPacketNotReport`，便于调用方据此拒绝报文。
///   - 前置条件：解析器应处于默认配置；
///   - 后置条件：若 `parse_rtcp` 返回其他错误，TCK 将 panic，提醒实现者补齐校验分支。
/// - **边界考量（Trade-offs）**：测试仅覆盖首分组违规场景，后续可扩展至长度/对齐异常。
pub fn assert_reject_compound_without_report() {
    let error = parse_rtcp(&BYE_ONLY[..]).expect_err("首分组不是报告时应触发错误");
    assert_eq!(
        error,
        RtcpError::FirstPacketNotReport { packet_type: 203 },
        "需返回明确的 FirstPacketNotReport 错误"
    );
}

#[cfg(test)]
mod tests {
    use super::{assert_parse_sr_sdes_bye_compound, assert_reject_compound_without_report};

    #[test]
    fn parse_sr_sdes_bye_compound() {
        assert_parse_sr_sdes_bye_compound();
    }

    #[test]
    fn reject_compound_without_report() {
        assert_reject_compound_without_report();
    }
}
