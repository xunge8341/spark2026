//! Offer/Answer 协商及 DTMF 属性的兼容性测试。
//!
//! ## 设计意图（Why）
//! - 验证 `spark-codec-sdp` 提供的 `apply_offer_answer` 是否能够根据本地能力筛选 PCMU/PCMA；
//! - 确保当 offer 提供 RFC 4733 `telephone-event` 时，本地在开启能力后能正确解析 `a=rtpmap` 与 `a=fmtp`；
//! - 为后续引入更复杂的媒体协商逻辑打下回归测试基础。
//!
//! ## 结构安排（How）
//! - `basic_pcm`：单元测试本地同时支持 PCMU/PCMA 时的最小交集选择；
//! - `negotiate_dtmf`：验证在开启 DTMF 能力时解析电话事件负载与事件范围。

use spark_codec_sdp::{
    offer_answer::{AnswerCapabilities, AudioAnswer, AudioCaps, AudioCodec, apply_offer_answer},
    parse_sdp,
};

/// 当本地偏好 PCMU 且 offer 同时给出 PCMU/PCMA 时，应优先选择 PCMU。
///
/// # 教案式说明
/// - **意图（Why）**：确认 `apply_offer_answer` 在面对多种常用语音编解码时遵循“本地偏好优先”的协商策略，
///   避免在生产中选择错误的负载类型导致媒体兼容问题。
/// - **流程（How）**：解析示例 offer，使用同时开启 PCMU/PCMA 的本地能力运行协商，断言最终计划中的音频
///   分支被接受且匹配 PCMU（负载 0）。
/// - **契约（What）**：函数无输入参数、无返回值；若断言失败将 panic，从而使集成该函数的 TCK 立即报错。
///   - 前置条件：调用方需保证 SDP 编解码器以 std 环境编译（用于字符串处理）。
///   - 后置条件：若无 panic，则保证协商结果中的音频分支被接受且 `payload_type == 0`。
/// - **边界考量（Trade-offs）**：采用最小报文示例，若未来支持更多 codec，需要扩展示例以覆盖权衡。
#[cfg_attr(not(test), allow(dead_code))]
pub fn assert_basic_pcm() {
    let offer = parse_sdp(
        "v=0\r\n\
         o=- 0 0 IN IP4 192.0.2.1\r\n\
         s=Test\r\n\
         t=0 0\r\n\
         m=audio 49170 RTP/AVP 0 8\r\n\
         a=rtpmap:0 PCMU/8000\r\n\
         a=rtpmap:8 PCMA/8000\r\n",
    )
    .expect("offer parsing");

    let caps = AnswerCapabilities::audio_only(AudioCaps::new(AudioCodec::Pcmu, true, true, false));

    let plan = apply_offer_answer(&offer, &caps);
    let audio_plan = plan.audio.expect("audio m-line must exist");

    match audio_plan {
        AudioAnswer::Accepted(accept) => {
            assert_eq!(accept.codec, AudioCodec::Pcmu);
            assert_eq!(accept.payload_type, 0);
            assert_eq!(accept.rtpmap.encoding, "PCMU");
            assert_eq!(accept.rtpmap.clock_rate, 8000);
            assert!(accept.telephone_event.is_none());
        }
        AudioAnswer::Rejected => panic!("audio should be accepted"),
    }
}

/// 当本地愿意协商 DTMF 时，应能够解析 `telephone-event` 的 `rtpmap` 与 `fmtp`。
///
/// # 教案式说明
/// - **意图（Why）**：验证实现正确识别 RFC 4733 的电话事件协商，确保开启 DTMF 能力后不会遗漏 `fmtp`
///   中的事件范围配置。
/// - **流程（How）**：构造包含动态负载 101 的 offer，启用本地 DTMF 能力，随后断言回答计划记录了 payload、
///   采样率与事件范围。
/// - **契约（What）**：函数 panic 代表协商逻辑违反契约；无返回值。
///   - 前置条件：需在测试环境中启用 `dtmf` 能力，且本地至少支持 PCMU。
///   - 后置条件：成功返回意味着回答计划中的 DTMF 部分拥有正确的 payload、采样率与事件范围。
/// - **边界考量（Trade-offs）**：场景忽略 `ptime` 等附加属性，若实现依赖这些字段需在 TCK 中补充。
#[cfg_attr(not(test), allow(dead_code))]
pub fn assert_negotiate_dtmf() {
    let offer = parse_sdp(
        "v=0\r\n\
         o=- 0 0 IN IP4 198.51.100.1\r\n\
         s=DTMF\r\n\
         t=0 0\r\n\
         m=audio 5004 RTP/AVP 0 101\r\n\
         a=rtpmap:0 PCMU/8000\r\n\
         a=rtpmap:101 telephone-event/8000\r\n\
         a=fmtp:101 0-15\r\n",
    )
    .expect("offer parsing");

    let caps = AnswerCapabilities::audio_only(AudioCaps::new(AudioCodec::Pcmu, true, false, true));

    let plan = apply_offer_answer(&offer, &caps);
    let audio_plan = plan.audio.expect("audio m-line must exist");

    match audio_plan {
        AudioAnswer::Accepted(accept) => {
            let dtmf = accept.telephone_event.expect("dtmf should be negotiated");
            assert_eq!(accept.codec, AudioCodec::Pcmu);
            assert_eq!(accept.payload_type, 0);
            assert_eq!(dtmf.payload_type, 101);
            assert_eq!(dtmf.clock_rate, 8000);
            assert_eq!(dtmf.events, "0-15");
        }
        AudioAnswer::Rejected => panic!("audio should be accepted"),
    }
}

#[cfg(test)]
mod tests {
    use super::{assert_basic_pcm, assert_negotiate_dtmf};

    #[test]
    fn basic_pcm() {
        assert_basic_pcm();
    }

    #[test]
    fn negotiate_dtmf() {
        assert_negotiate_dtmf();
    }
}
