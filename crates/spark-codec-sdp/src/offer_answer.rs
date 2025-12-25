//! RFC 3264 Offer/Answer 协商与 RFC 4733 DTMF 属性解析的最小实现。
//!
//! ## 模块定位（Why）
//! - **承上启下**：协助上层根据解析出的 `SessionDesc` 与本地能力决定应答内容；
//! - **风险兜底**：在 TCK 阶段通过集中实现最小逻辑，防止其它 crate 反复实现临时解析器；
//! - **扩展前瞻**：结构体与返回值以“计划（Plan）”形式存在，可在后续追加媒体方向、ICE 参数等。
//!
//! ## 契约说明（What）
//! - 输入：由 `parse_sdp` 得到的 `SessionDesc` 引用，以及本地能力声明 `AnswerCapabilities`；
//! - 输出：`AnswerPlan`，描述是否接受音频媒体、选中的编解码器及 DTMF 配置；
//! - 错误处理：最小实现阶段不返回错误，而是通过 `AudioAnswer::Rejected` 表达拒绝。
//!
//! ## 逻辑蓝图（How）
//! 1. 从 offer 的首个 `m=audio` 块收集 `m=` 格式、`a=rtpmap` 与 `a=fmtp`；
//! 2. 识别 PCMU/PCMA 编解码能力，与本地能力求交集并按照本地偏好排序；
//! 3. 若本地愿意支持 DTMF，解析 `telephone-event` 的 `rtpmap/fmtp` 组合并写入计划；
//! 4. 若找不到可接受的编解码器，则返回 `Rejected`，由上层决定是否在 SDP 中写入 `m=audio 0`。

use alloc::vec::Vec;

use crate::{MediaDesc, SessionDesc};

/// 音频编解码器的枚举。
///
/// ### 设计动机（Why）
/// - 明确区分 PCMU/PCMA，便于在能力声明与协商结果之间进行匹配；
/// - 保持 `Copy` 以方便在状态机与测试断言中传递。
///
/// ### 契约说明（What）
/// - 枚举值对应 IANA RTP Payload Types 中的静态负载：`0` → PCMU，`8` → PCMA；
/// - 后续若引入新的编解码器，可按需扩展枚举。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AudioCodec {
    /// G.711 μ-law，对应 RTP 负载类型 0。
    Pcmu,
    /// G.711 A-law，对应 RTP 负载类型 8。
    Pcma,
}

impl AudioCodec {
    /// 返回该编解码器在静态负载表中的编号。
    ///
    /// ### 意图说明（Why）
    /// - 便于在未提供 `a=rtpmap` 时根据静态编号推导 `rtpmap` 默认值；
    /// - TCK 在断言选中编解码器时亦可复用该接口。
    pub const fn static_payload_type(self) -> u8 {
        match self {
            Self::Pcmu => 0,
            Self::Pcma => 8,
        }
    }

    /// 返回推荐的编码名称。
    ///
    /// ### 契约说明（What）
    /// - 返回值遵循 RFC 3551 大写命名（`PCMU`/`PCMA`），用于生成 `a=rtpmap`。
    pub const fn encoding_name(self) -> &'static str {
        match self {
            Self::Pcmu => "PCMU",
            Self::Pcma => "PCMA",
        }
    }
}

/// 描述音频协商能力。
///
/// ### 设计动机（Why）
/// - 通过一个紧凑结构体表达“本地愿意启用哪些编解码器、优先级为何以及是否接受 DTMF”；
/// - 与测试用例共享，实现“只关心 PCMU/PCMA + DTMF”这一阶段性目标。
///
/// ### 契约说明（What）
/// - `prefer`：本地偏好的编解码器；
/// - `allow_pcmu` / `allow_pcma`：显式开关，避免因偏好设定导致误选未授权的编码；
/// - `accept_dtmf`：是否愿意在应答中保留 `telephone-event` 描述；
/// - 前置条件：调用方需确保偏好与允许集合至少包含一个重叠编解码器；
/// - 后置条件：结构体本身不执行校验，交由协商流程处理。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AudioCaps {
    /// 本地首选编解码器。
    pub prefer: AudioCodec,
    /// 是否允许 PCMU。
    pub allow_pcmu: bool,
    /// 是否允许 PCMA。
    pub allow_pcma: bool,
    /// 是否接受 DTMF `telephone-event`。
    pub accept_dtmf: bool,
}

impl AudioCaps {
    /// 构造音频能力声明。
    ///
    /// ### 实现逻辑（How）
    /// - 该函数仅收集参数并返回结构体，不做任何推导或校验；
    /// - 之所以显式提供，而非直接使用字面量，是为了在未来加入断言或默认值时保持向后兼容。
    pub const fn new(
        prefer: AudioCodec,
        allow_pcmu: bool,
        allow_pcma: bool,
        accept_dtmf: bool,
    ) -> Self {
        Self {
            prefer,
            allow_pcmu,
            allow_pcma,
            accept_dtmf,
        }
    }

    /// 判断某个编解码器是否被允许。
    #[allow(clippy::match_like_matches_macro)]
    pub const fn permits(&self, codec: AudioCodec) -> bool {
        match codec {
            AudioCodec::Pcmu => self.allow_pcmu,
            AudioCodec::Pcma => self.allow_pcma,
        }
    }
}

/// 描述本地所有媒体的能力，目前仅包含音频。
///
/// ### 设计动机（Why）
/// - 为未来扩展视频或数据通道预留统一入口；
/// - 让 `apply_offer_answer` 的签名稳定，不因新增媒体类型而频繁变更。
///
/// ### 契约说明（What）
/// - 当前阶段仅包含单一字段 `audio`；
/// - 预期调用方始终提供音频能力，即使暂时拒绝所有编解码器也需显式声明。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AnswerCapabilities {
    /// 音频能力声明。
    pub audio: AudioCaps,
}

impl AnswerCapabilities {
    /// 构造仅含音频的能力声明。
    pub const fn audio_only(audio: AudioCaps) -> Self {
        Self { audio }
    }
}

/// 协商后的整体回答计划。
///
/// ### 设计动机（Why）
/// - 让调用方可以在不立即生成 SDP 的情况下观察协商结论；
/// - 为测试提供断言入口，验证所选编解码器与属性。
///
/// ### 契约说明（What）
/// - `audio`：若 offer 中存在 `m=audio` 块，则给出对应的处理决策；
/// - 若 `audio` 为 `None`，意味着 offer 未提供音频媒体。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AnswerPlan<'a> {
    /// 音频媒体的处理计划。
    pub audio: Option<AudioAnswer<'a>>,
}

/// 音频媒体的回答决策。
///
/// ### 设计动机（Why）
/// - 区分“接受并选定编解码器”与“整体拒绝”两种结论；
/// - 避免使用 `Result`，以便未来扩充更多分支（例如“待协商”）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AudioAnswer<'a> {
    /// 接受音频媒体，并指明采用的具体参数。
    Accepted(AudioAcceptPlan<'a>),
    /// 拒绝音频媒体。
    Rejected,
}

/// 对被接受音频媒体的细节描述。
///
/// ### 设计动机（Why）
/// - 在一处集中呈现回答所需的全部关键信息（负载类型、编解码器、`rtpmap`、DTMF）；
/// - 便于后续生成 SDP `m=` 与 `a=` 行。
///
/// ### 契约说明（What）
/// - `payload_type`：来自 offer `m=` 行的负载编号；
/// - `codec`：本地选中的编解码器；
/// - `rtpmap`：用于生成 `a=rtpmap` 行的参考数据，即使 offer 未提供亦会填充默认值；
/// - `telephone_event`：若成功协商 DTMF，则包含事件范围与采样率。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AudioAcceptPlan<'a> {
    /// 采用的负载类型编号。
    pub payload_type: u8,
    /// 选中的编解码器。
    pub codec: AudioCodec,
    /// 生成 `a=rtpmap` 所需的信息。
    pub rtpmap: RtpmapRef<'a>,
    /// 若协商成功则包含 DTMF 配置。
    pub telephone_event: Option<TelephoneEvent<'a>>,
}

/// `a=rtpmap` 行的解析结果视图。
///
/// ### 设计动机（Why）
/// - 在 Answer 计划中复用 offer 提供的参数，减少重复计算；
/// - 若 offer 缺失 `a=rtpmap`，则使用该结构承载内建默认值。
///
/// ### 契约说明（What）
/// - `encoding`：编码名称，遵循大小写不敏感比对；
/// - `clock_rate`：采样率，以 Hz 为单位；
/// - `encoding_params`：附加参数（如声道数），可能为空。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RtpmapRef<'a> {
    /// 编码名称。
    pub encoding: &'a str,
    /// 采样率。
    pub clock_rate: u32,
    /// 附加参数。
    pub encoding_params: Option<&'a str>,
}

/// `telephone-event` 的协商结果。
///
/// ### 设计动机（Why）
/// - RFC 4733 要求回答端复用 offer 的负载编号与事件范围，故需要单独结构体记录；
/// - 方便未来扩展更多事件或自定义参数。
///
/// ### 契约说明（What）
/// - `payload_type`：DTMF 使用的动态负载编号；
/// - `clock_rate`：采样率（通常为 8000）；
/// - `events`：`a=fmtp` 中声明的事件范围文本。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TelephoneEvent<'a> {
    /// 使用的负载编号。
    pub payload_type: u8,
    /// 采样率。
    pub clock_rate: u32,
    /// 支持的事件范围。
    pub events: &'a str,
}

/// 根据 offer 与本地能力生成回答计划。
///
/// ### 设计动机（Why）
/// - 在协商早期阶段提供一个“纯计算”接口，后续可在 SIP/SDP 层按需渲染；
/// - 将 Offer/Answer 协议中的核心决策点集中在单个函数中，便于测试覆盖。
///
/// ### 契约说明（What）
/// - **输入**：`offer` 为已解析的 `SessionDesc` 引用，`caps` 为本地能力；
/// - **输出**：`AnswerPlan`，若无音频或无可接受编码，则 `audio` 分支分别为 `None`/`Rejected`；
/// - **前置条件**：`offer` 必须来源于合法 SDP 文本；`caps` 至少允许一个编解码器；
/// - **后置条件**：不会修改 `offer`，也不会分配除辅助容器外的大量内存。
///
/// ### 实现逻辑（How）
/// - 寻找第一个 `m=audio` 媒体描述并交由 `negotiate_audio` 处理；
/// - 若未找到音频媒体，则返回仅含 `None` 的计划。
///
/// ### 风险提示（Trade-offs & Gotchas）
/// - 当前仅处理单一音频块，若 offer 存在多个音频流将只取第一个；
/// - 未对不匹配的能力返回错误，交由上层依据业务策略处理拒绝。
#[must_use]
pub fn apply_offer_answer<'a>(
    offer: &'a SessionDesc<'a>,
    caps: &AnswerCapabilities,
) -> AnswerPlan<'a> {
    let audio = offer
        .media
        .iter()
        .find(|media| media.media.eq_ignore_ascii_case("audio"))
        .map(|media| negotiate_audio(media, caps.audio));

    AnswerPlan { audio }
}

fn negotiate_audio<'a>(media: &'a MediaDesc<'a>, caps: AudioCaps) -> AudioAnswer<'a> {
    let rtpmap_entries = collect_rtpmap(media);
    let fmtp_entries = collect_fmtp(media);

    let mut pcmu_candidate: Option<AudioCandidate<'a>> = None;
    let mut pcma_candidate: Option<AudioCandidate<'a>> = None;

    for &fmt in &media.formats {
        let rtpmap = rtpmap_entries
            .iter()
            .find(|entry| entry.payload_type == fmt)
            .copied();
        let codec = identify_codec(fmt, rtpmap);

        if let Some(codec) = codec
            && let Ok(payload_type) = fmt.parse::<u8>()
        {
            let candidate = AudioCandidate {
                payload_type,
                rtpmap,
            };
            match codec {
                AudioCodec::Pcmu => pcmu_candidate.get_or_insert(candidate),
                AudioCodec::Pcma => pcma_candidate.get_or_insert(candidate),
            };
        }
    }

    let order = match caps.prefer {
        AudioCodec::Pcmu => [AudioCodec::Pcmu, AudioCodec::Pcma],
        AudioCodec::Pcma => [AudioCodec::Pcma, AudioCodec::Pcmu],
    };

    for codec in order {
        if !caps.permits(codec) {
            continue;
        }

        let candidate = match codec {
            AudioCodec::Pcmu => pcmu_candidate,
            AudioCodec::Pcma => pcma_candidate,
        };

        if let Some(candidate) = candidate {
            let rtpmap = candidate
                .rtpmap
                .map(|entry| RtpmapRef {
                    encoding: entry.encoding,
                    clock_rate: entry.clock_rate,
                    encoding_params: entry.encoding_params,
                })
                .unwrap_or_else(|| default_rtpmap(codec));

            let telephone_event = if caps.accept_dtmf {
                negotiate_dtmf(&rtpmap_entries, &fmtp_entries)
            } else {
                None
            };

            return AudioAnswer::Accepted(AudioAcceptPlan {
                payload_type: candidate.payload_type,
                codec,
                rtpmap,
                telephone_event,
            });
        }
    }

    AudioAnswer::Rejected
}

fn collect_rtpmap<'a>(media: &'a MediaDesc<'a>) -> Vec<ParsedRtpmap<'a>> {
    media
        .attributes
        .iter()
        .filter_map(|attr| match attr.value {
            Some(value) if attr.key.eq_ignore_ascii_case("rtpmap") => parse_rtpmap(value),
            _ => None,
        })
        .collect()
}

fn collect_fmtp<'a>(media: &'a MediaDesc<'a>) -> Vec<ParsedFmtp<'a>> {
    media
        .attributes
        .iter()
        .filter_map(|attr| match attr.value {
            Some(value) if attr.key.eq_ignore_ascii_case("fmtp") => parse_fmtp(value),
            _ => None,
        })
        .collect()
}

fn identify_codec<'a>(fmt: &'a str, rtpmap: Option<ParsedRtpmap<'a>>) -> Option<AudioCodec> {
    match fmt {
        "0" => Some(AudioCodec::Pcmu),
        "8" => Some(AudioCodec::Pcma),
        _ => rtpmap.and_then(|entry| {
            if entry.encoding.eq_ignore_ascii_case("PCMU") {
                Some(AudioCodec::Pcmu)
            } else if entry.encoding.eq_ignore_ascii_case("PCMA") {
                Some(AudioCodec::Pcma)
            } else {
                None
            }
        }),
    }
}

fn default_rtpmap(codec: AudioCodec) -> RtpmapRef<'static> {
    RtpmapRef {
        encoding: codec.encoding_name(),
        clock_rate: 8000,
        encoding_params: None,
    }
}

fn negotiate_dtmf<'a>(
    rtpmap_entries: &[ParsedRtpmap<'a>],
    fmtp_entries: &[ParsedFmtp<'a>],
) -> Option<TelephoneEvent<'a>> {
    let rtpmap = rtpmap_entries
        .iter()
        .find(|entry| entry.encoding.eq_ignore_ascii_case("telephone-event"))?;
    let fmtp = fmtp_entries
        .iter()
        .find(|entry| entry.payload_type == rtpmap.payload_type)?;
    let payload_type = rtpmap.payload_type.parse().ok()?;

    Some(TelephoneEvent {
        payload_type,
        clock_rate: rtpmap.clock_rate,
        events: fmtp.params,
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ParsedRtpmap<'a> {
    payload_type: &'a str,
    encoding: &'a str,
    clock_rate: u32,
    encoding_params: Option<&'a str>,
}

fn parse_rtpmap(value: &str) -> Option<ParsedRtpmap<'_>> {
    let mut parts = value.split_whitespace();
    let payload_type = parts.next()?;
    let descriptor = parts.next()?;
    let mut descriptor_parts = descriptor.split('/');
    let encoding = descriptor_parts.next()?;
    let clock_rate = descriptor_parts.next()?.parse().ok()?;
    let encoding_params = descriptor_parts.next();

    Some(ParsedRtpmap {
        payload_type,
        encoding,
        clock_rate,
        encoding_params,
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ParsedFmtp<'a> {
    payload_type: &'a str,
    params: &'a str,
}

fn parse_fmtp(value: &str) -> Option<ParsedFmtp<'_>> {
    let mut parts = value.split_whitespace();
    let payload_type = parts.next()?;
    let params = parts.next()?;

    Some(ParsedFmtp {
        payload_type,
        params,
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct AudioCandidate<'a> {
    payload_type: u8,
    rtpmap: Option<ParsedRtpmap<'a>>,
}
