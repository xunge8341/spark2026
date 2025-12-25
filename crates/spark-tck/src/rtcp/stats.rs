//! SR/RR 生成逻辑的契约测试。
//!
//! # 教案定位（Why）
//! - 验证 `spark-codec-rtcp::stats` 模块在构造 Sender Report / Receiver Report 时对 NTP、RTP、统计字段的映射是否符合 RFC3550 要求。
//! - 为后续媒体栈实现提供回归基线，确保边界条件（延迟饱和、LSR 计算等）不会因重构而回退。
//!
//! # 测试策略（How）
//! - 构造伪造的 RTP 时钟实现，模拟 90 kHz 媒体时钟与 NTP 时间之间的映射关系。
//! - 针对 SR/RR 分别断言：SenderInfo、接收报告块、Profile 扩展等字段与输入统计一致。
//! - 覆盖异常情况，例如 `cumulative_lost` 超界及延迟溢出，确认构建函数采取饱和策略。

use core::time::Duration;
use std::time::{Duration as StdDuration, UNIX_EPOCH};

use spark_codec_rtcp::{
    BuildError, NtpTime, ReceiverStat, ReceiverStatistics, ReceptionStat, ReceptionStatistics,
    RtcpPacket, RtcpPacketVec, RtpClock, RtpClockMapper, SenderStat, SenderStatistics, build_rr,
    build_rr_raw, build_sr, build_sr_raw,
};

/// 90 kHz RTP 媒体时钟：RTP 时间戳 = 秒 * 90000 + (fraction >> 16)。
struct NinetyKhzClock;

impl RtpClock for NinetyKhzClock {
    fn to_rtp_timestamp(&self, ntp: &NtpTime) -> u32 {
        ntp.seconds().saturating_mul(90_000) + (ntp.fraction() >> 16)
    }
}

/// 验证高阶构建器能够复制发送者统计与接收报告字段。
#[test]
fn build_sr_populates_sender_and_reports() {
    let clock = NinetyKhzClock;
    let last_sr = NtpTime::from_parts(0x0203, 0x0405_0000);
    let stats = SenderStatistics {
        sender_ssrc: 0x1122_3344,
        capture_ntp: NtpTime::from_parts(0x0123_4567, 0x89AB_CDEF),
        rtp_timestamp_override: None,
        sender_packet_count: 123_456,
        sender_octet_count: 654_321,
        reports: &[ReceptionStatistics {
            source_ssrc: 0x5566_7788,
            fraction_lost: 42,
            cumulative_lost: 12_345,
            extended_highest_sequence: 0x9ABC_DEF0,
            interarrival_jitter: 0x0102_0304,
            last_sr: Some(last_sr),
            delay_since_last_sr: Some(Duration::new(3, 500_000_000)),
        }],
        profile_extensions: &[0xDE, 0xAD, 0xBE, 0xEF],
    };

    let mut packets = RtcpPacketVec::new();
    build_sr(&clock, &stats, &mut packets);

    let packet = packets.pop().expect("SR builder must output one packet");
    let RtcpPacket::SenderReport(report) = packet else {
        panic!("expected SenderReport")
    };

    assert_eq!(report.sender_ssrc, stats.sender_ssrc);
    assert_eq!(report.profile_extensions, stats.profile_extensions);
    assert_eq!(report.sender_info.ntp_timestamp, stats.capture_ntp.as_u64());
    let expected_rtp = clock.to_rtp_timestamp(&stats.capture_ntp);
    assert_eq!(report.sender_info.rtp_timestamp, expected_rtp);
    assert_eq!(
        report.sender_info.sender_packet_count,
        stats.sender_packet_count
    );
    assert_eq!(
        report.sender_info.sender_octet_count,
        stats.sender_octet_count
    );

    assert_eq!(report.reports.len(), 1);
    let rr = &report.reports[0];
    assert_eq!(rr.source_ssrc, 0x5566_7788);
    assert_eq!(rr.fraction_lost, 42);
    assert_eq!(rr.cumulative_lost, 12_345);
    assert_eq!(rr.extended_highest_sequence, 0x9ABC_DEF0);
    assert_eq!(rr.interarrival_jitter, 0x0102_0304);
    assert_eq!(rr.last_sr_timestamp, last_sr.lsr());
    // 3.5 秒 => 3 * 65536 + 0.5 * 65536 = 229376
    assert_eq!(rr.delay_since_last_sr, 229_376);
}

/// 验证高阶构建器对延迟饱和与 profile 扩展复制的处理。
#[test]
fn build_rr_respects_overrides_and_saturation() {
    let reception = ReceptionStatistics {
        source_ssrc: 0xCAFEBABE,
        fraction_lost: 255,
        cumulative_lost: 10_000_000, // 超界应被裁剪至 24-bit 上限
        extended_highest_sequence: 0x0102_0304,
        interarrival_jitter: 0x0506_0708,
        last_sr: None,
        delay_since_last_sr: Some(Duration::new(u64::from(u16::MAX) + 10, 900_000_000)),
    };

    let recv_stats = ReceiverStatistics {
        reporter_ssrc: 0xA0B1_C2D3,
        reports: &[reception],
        profile_extensions: &[1, 2, 3, 4, 5, 6, 7, 8],
    };

    let mut packets = RtcpPacketVec::new();
    build_rr(&recv_stats, &mut packets);

    match packets.pop().expect("RR builder must output one packet") {
        RtcpPacket::ReceiverReport(report) => {
            assert_eq!(report.reporter_ssrc, recv_stats.reporter_ssrc);
            assert_eq!(report.profile_extensions, recv_stats.profile_extensions);
            assert_eq!(report.reports.len(), 1);
            let block = &report.reports[0];
            assert_eq!(block.source_ssrc, reception.source_ssrc);
            assert_eq!(block.fraction_lost, 255);
            assert_eq!(block.cumulative_lost, 0x7F_FFFF);
            assert_eq!(block.last_sr_timestamp, 0);
            assert_eq!(block.delay_since_last_sr, u32::MAX);
        }
        other => panic!("unexpected packet variant: {other:?}"),
    }
}

/// 验证高阶构建器优先使用 `rtp_timestamp_override`。
#[test]
fn build_sr_honors_rtp_override() {
    let stats = SenderStatistics {
        sender_ssrc: 0xFEED_CAFE,
        capture_ntp: NtpTime::from_parts(10, 0),
        rtp_timestamp_override: Some(0xDEAD_BEEF),
        sender_packet_count: 1,
        sender_octet_count: 2,
        reports: &[],
        profile_extensions: &[],
    };

    let mut packets = RtcpPacketVec::new();
    build_sr(&NinetyKhzClock, &stats, &mut packets);

    let packet = packets.pop().expect("SR builder must output one packet");
    let RtcpPacket::SenderReport(report) = packet else {
        panic!("expected SenderReport");
    };

    assert_eq!(report.sender_info.rtp_timestamp, 0xDEAD_BEEF);
}
/// 生成包含单个接收报告与 Profile 扩展的 Sender Report，并校验完整字节序列。
#[test]
fn raw_build_sender_report_with_statistics() {
    let clock = RtpClockMapper::new(UNIX_EPOCH, 0, 90_000);
    let capture_time = UNIX_EPOCH + StdDuration::from_millis(1500);
    let reports = [ReceptionStat {
        source_ssrc: 0x0102_0304,
        fraction_lost: 0x05,
        cumulative_lost: 0x0000_0607,
        extended_highest_sequence: 0x1020_3040,
        interarrival_jitter: 0x1122_3344,
        last_sr_timestamp: 0x5566_7788,
        delay_since_last_sr: 0x99AA_BBCC,
    }];
    let sender_stat = SenderStat {
        sender_ssrc: 0x5566_7788,
        capture_time,
        sender_packet_count: 0x0001_0203,
        sender_octet_count: 0x0002_0304,
        reports: &reports,
        profile_extensions: &[0xDE, 0xAD, 0xBE, 0xEF],
    };

    let mut out = Vec::new();
    build_sr_raw(&clock, &sender_stat, &mut out).expect("SR 生成应成功");

    let expected = hex::decode(
        "81c8000d5566778883aa7e818000000000020f580001020300020304010203040500060710203040112233445566778899aabbccdeadbeef",
    )
    .expect("固定的十六进制字符串应合法");
    assert_eq!(out, expected);
}

/// 生成包含两个接收报告的 Receiver Report，并验证负累计丢包的编码逻辑。
#[test]
fn raw_build_receiver_report_with_signed_loss() {
    let reports = [
        ReceptionStat {
            source_ssrc: 0x0102_0304,
            fraction_lost: 0x05,
            cumulative_lost: 0x0000_0607,
            extended_highest_sequence: 0x1020_3040,
            interarrival_jitter: 0x1122_3344,
            last_sr_timestamp: 0x5566_7788,
            delay_since_last_sr: 0x99AA_BBCC,
        },
        ReceptionStat {
            source_ssrc: 0x0A0B_0C0D,
            fraction_lost: 0xFE,
            cumulative_lost: -5,
            extended_highest_sequence: 0x2222_2222,
            interarrival_jitter: 0x3333_3333,
            last_sr_timestamp: 0x4444_4444,
            delay_since_last_sr: 0x5555_5555,
        },
    ];
    let recv_stat = ReceiverStat {
        reporter_ssrc: 0xCAFE_BABE,
        reports: &reports,
        profile_extensions: &[],
    };

    let mut out = Vec::new();
    build_rr_raw(&recv_stat, &mut out).expect("RR 生成应成功");

    let expected = hex::decode(
        "82c9000dcafebabe010203040500060710203040112233445566778899aabbcc0a0b0c0dfefffffb22222222333333334444444455555555",
    )
    .expect("固定的十六进制字符串应合法");
    assert_eq!(out, expected);
}

/// 当采样时间早于映射器起点时应返回 `ClockWentBackwards` 错误，避免生成无效 NTP/RTP 时间戳。
#[test]
fn raw_sr_rejects_clock_rewind() {
    let clock = RtpClockMapper::new(UNIX_EPOCH + StdDuration::from_secs(10), 0, 90_000);
    let sender_stat = SenderStat {
        sender_ssrc: 0x1234_5678,
        capture_time: UNIX_EPOCH,
        sender_packet_count: 0,
        sender_octet_count: 0,
        reports: &[],
        profile_extensions: &[],
    };

    let mut out = Vec::new();
    let error = build_sr_raw(&clock, &sender_stat, &mut out).expect_err("采样时间回退应触发错误");
    assert!(matches!(error, BuildError::ClockWentBackwards));
}

/// Profile 扩展长度必须按 32-bit 对齐，否则返回 `MisalignedProfileExtension`。
#[test]
fn raw_rr_rejects_misaligned_extension() {
    let recv_stat = ReceiverStat {
        reporter_ssrc: 0xCAFE_BABE,
        reports: &[],
        profile_extensions: &[0xAA],
    };
    let mut out = Vec::new();
    let error = build_rr_raw(&recv_stat, &mut out).expect_err("未对齐的扩展应触发错误");
    assert!(matches!(
        error,
        BuildError::MisalignedProfileExtension { len: 1 }
    ));
}

/// `cumulative_lost` 超过 24-bit 表示范围时应触发 `CumulativeLostOutOfRange` 错误。
#[test]
fn raw_reception_stat_rejects_out_of_range_loss() {
    let reports = [ReceptionStat {
        source_ssrc: 0,
        fraction_lost: 0,
        cumulative_lost: 1 << 24,
        extended_highest_sequence: 0,
        interarrival_jitter: 0,
        last_sr_timestamp: 0,
        delay_since_last_sr: 0,
    }];
    let recv_stat = ReceiverStat {
        reporter_ssrc: 0,
        reports: &reports,
        profile_extensions: &[],
    };
    let mut out = Vec::new();
    let error = build_rr_raw(&recv_stat, &mut out).expect_err("超范围累计丢包应触发错误");
    assert!(matches!(
        error,
        BuildError::CumulativeLostOutOfRange { value } if value == (1 << 24)
    ));
}
