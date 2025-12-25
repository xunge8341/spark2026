#![cfg_attr(not(feature = "std"), no_std)]
#![warn(missing_docs)]

//! # spark-codec-sdp
//!
//! ## 教案目的（Why）
//! - **定位**：该 crate 为 Session Description Protocol (SDP) 的编解码骨架，是媒体参数协商（编解码器、带宽、网络候选等）的核心环节。
//! - **架构角色**：承上启下地连接 SIP 信令与 RTP/RTCP 数据面，负责解释和生成多媒体会话描述。
//! - **设计策略**：在零拷贝约束下实现核心行（`v/o/s/c/t/m/a`）的解析与生成，为后续扩展留出接口。
//!
//! ## 交互契约（What）
//! - **依赖输入**：基于 `spark-core` 的 Codec trait，后续会消费 SIP 层传递的 SDP 负载。
//! - **输出职责**：提供 `parse_sdp`/`format_sdp`，在不复制字符串的前提下读取并生成基础 SDP。
//! - **前置条件**：默认假设 UTF-8 编码、支持多媒体描述（m=）行；后续需扩展对 ICE/DTLS 参数的验证。
//!
//! ## 实现策略（How）
//! - **路线规划**：
//!   1. 定义 `SessionDesc`/`MediaDesc`/`Attribute` 结构，并保持生命周期与输入绑定；
//!   2. 解析时单次扫描、按序归档媒体块；
//!   3. 生成时严格遵循 SDP 行顺序，满足往返测试；
//! - **技术选择**：使用 `alloc` 的 `String`/`Vec` 在 `no_std` 环境下也可运行；保留零拷贝切片，避免重复分配。
//!
//! ## 风险提示（Trade-offs）
//! - **功能范围**：当前仅支持核心行，其余行会被忽略；扩展行需后续补充。
//! - **性能考量**：单次扫描、零拷贝切片，可在长 SDP 中保持线性性能；若需多线程解析可另行封装。
//! - **维护提醒**：新增行类型时需同时更新解析与生成函数及契约测试。

extern crate alloc;

use alloc::string::String;
use alloc::vec::Vec;

/// Offer/Answer 能力协商与媒体属性解析模块。
///
/// ### 设计定位（Why）
/// - 将 RFC 3264 Offer/Answer 与 RFC 4733 DTMF 事件的最小实现集中在独立模块中，
///   避免核心 SDP 结构体被业务逻辑污染；
/// - 为 `spark-tck` 的互操作测试提供具名入口，确保未来扩展（如多编解码器、方向属性）
///   有明确的代码落点。
///
/// ### 契约约束（What）
/// - 暴露 `apply_offer_answer` 等函数，输入为解析后的 `SessionDesc`，输出为回答计划；
/// - 模块内部默认依赖 `alloc`，遵守 `no_std` 约束。
pub mod offer_answer;
use core::fmt;

/// SDP 编解码骨架，明确媒体协商入口位置。
///
/// ### 设计意图（Why）
/// - 对上保持与 SIP 层的耦合最小化，对下为 RTP/RTCP 提供能力描述。
/// - 预留结构体以统一管理 SDP 解析配置（如多流支持、加密要求）。
///
/// ### 使用契约（What）
/// - 当前结构体不存储状态，仅表示“SDP 功能即将到位”的承诺。
/// - 后续将引入字段描述媒体流、加密算法、候选地址等细节。
///
/// ### 实现说明（How）
/// - 采用 `Default`/`Copy`，方便测试场景中快速生成样例。
/// - 结合 `#[must_use]` 的构造函数，提醒调用者显式处理结果。
///
/// ### 风险提示（Trade-offs）
/// - 若未来需要维护内部缓冲，需撤销 `Copy` 并引入自定义 Drop 逻辑。
#[derive(Debug, Default, Clone, Copy)]
pub struct SdpCodecScaffold;

impl SdpCodecScaffold {
    /// 构造 SDP 编解码占位实例。
    ///
    /// ### 设计动机（Why）
    /// - 在项目早期确保调用方可以引用固定的构造入口。
    /// - 为 `spark-tck` 预留依赖，后续可以直接在测试中注入。
    ///
    /// ### 契约定义（What）
    /// - **输入**：无。无需外部配置即可返回占位对象。
    /// - **输出**：`SdpCodecScaffold` 零尺寸实例，代表未来 SDP 功能的锚点。
    /// - **前置条件**：调用环境只需具备编译期依赖即可，无运行时约束。
    /// - **后置条件**：调用方获得一个可拷贝的标记对象，可用于类型推断或测试桩。
    ///
    /// ### 实现细节（How）
    /// - 使用 `const fn` 便于在静态上下文中构造实例（如全局常量、测试 fixtures）。
    ///
    /// ### 风险提示（Trade-offs）
    /// - 随着功能扩展可能需要引入错误返回；请在修改时同步更新测试与文档。
    #[must_use]
    pub const fn new() -> Self {
        Self
    }
}

/// SDP 解析错误的枚举。
///
/// ### 设计动机（Why）
/// - 向调用者精确说明是哪一类核心行缺失或格式不合规，便于排查信令错误。
/// - 支持 TCK 在匹配具体错误类型时给出针对性的诊断。
///
/// ### 契约定义（What）
/// - 每个枚举值都代表一类确定的失败场景，例如缺少必须行（`v=`、`o=` 等）或语法切分失败。
/// - 解析函数保证只返回该枚举，方便 `match` 处理。
///
/// ### 风险提示（Trade-offs）
/// - 当前未包含位置信息；如果后续需要高精度错误提示，可扩展为包含行号的结构体。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SdpParseError {
    /// 缺少 `v=` 行。
    MissingVersion,
    /// 缺少 `o=` 行。
    MissingOrigin,
    /// `o=` 行字段不足或格式错误。
    InvalidOrigin,
    /// 缺少 `s=` 行。
    MissingSessionName,
    /// 缺少 `t=` 行。
    MissingTiming,
    /// `t=` 行字段不足或格式错误。
    InvalidTiming,
    /// `c=` 行字段不足或格式错误。
    InvalidConnection,
    /// `m=` 行字段不足或格式错误。
    InvalidMedia,
}

impl fmt::Display for SdpParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingVersion => write!(f, "SDP 缺少 v= 版本行"),
            Self::MissingOrigin => write!(f, "SDP 缺少 o= 会话发起人行"),
            Self::InvalidOrigin => write!(f, "o= 行字段不足，需提供 6 个片段"),
            Self::MissingSessionName => write!(f, "SDP 缺少 s= 会话名称行"),
            Self::MissingTiming => write!(f, "SDP 缺少 t= 时间行"),
            Self::InvalidTiming => write!(f, "t= 行需要包含开始与结束时间"),
            Self::InvalidConnection => write!(f, "c= 行需要提供网络/地址类型与地址"),
            Self::InvalidMedia => write!(f, "m= 行需要包含媒体、端口、传输与至少一个格式"),
        }
    }
}

#[cfg(feature = "std")]
impl std::error::Error for SdpParseError {}

/// 描述 SDP 会话级别的 Origin 字段。
///
/// ### 设计动机（Why）
/// - `o=` 行是 SDP 会话的全局标识，包含协商双方识别所需的核心元数据。
/// - 零拷贝存储切片，避免在解析时分配额外字符串。
///
/// ### 契约定义（What）
/// - 记录用户名、会话 ID、版本、网络类型、地址类型与实际地址，共 6 个字段。
/// - 所有字段直接引用原始 SDP 文本切片，生命周期与输入字符串一致。
///
/// ### 风险提示（Trade-offs）
/// - 字段未做进一步语义验证（例如数值范围），后续如需严格校验可在上层补充。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Origin<'a> {
    /// 发起用户名。
    pub username: &'a str,
    /// 会话标识。
    pub session_id: &'a str,
    /// 会话版本号。
    pub session_version: &'a str,
    /// 网络类型，例如 `IN`。
    pub net_type: &'a str,
    /// 地址类型，例如 `IP4` 或 `IP6`。
    pub addr_type: &'a str,
    /// 实际地址内容。
    pub address: &'a str,
}

/// 描述 SDP 的连接信息。
///
/// ### 设计动机（Why）
/// - `c=` 行为媒体流提供网络地址，是媒体传输可达性的基础。
/// - 会话级与媒体级可共用结构，减少重复实现。
///
/// ### 契约定义（What）
/// - 包含网络类型、地址类型与具体地址三部分，全部为零拷贝切片。
/// - 可直接复用在会话或媒体级别。
///
/// ### 风险提示（Trade-offs）
/// - 未处理地址中的附加参数（如 TTL、地址数），如后续需要需扩展结构。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Connection<'a> {
    /// 网络类型，常见为 `IN`。
    pub net_type: &'a str,
    /// 地址类型，常见为 `IP4` 或 `IP6`。
    pub addr_type: &'a str,
    /// 实际连接地址。
    pub address: &'a str,
}

/// 描述 SDP 的时间窗口。
///
/// ### 设计动机（Why）
/// - `t=` 行定义媒体会话的起止时间，是基础协商要素。
/// - 提供零拷贝访问，便于上层按需解析或转换为数值。
///
/// ### 契约定义（What）
/// - 存储开始（`start`）与结束（`stop`）两个时间片段。
/// - 解析层仅保证两个片段存在，不强加语义约束。
///
/// ### 风险提示（Trade-offs）
/// - 若需支持重复时间（`r=`）等扩展，需要后续扩展结构和解析流程。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Timing<'a> {
    /// 会话开始时间。
    pub start: &'a str,
    /// 会话结束时间。
    pub stop: &'a str,
}

/// 描述 SDP 的属性行。
///
/// ### 设计动机（Why）
/// - `a=` 行大量出现于会话与媒体层，承载编解码能力、方向等细节。
/// - 抽象统一结构，方便追加、遍历或生成文本。
///
/// ### 契约定义（What）
/// - `key` 必填，`value` 可选，对应 `a=key:value` 语法。
/// - 所有字段保持零拷贝，引用原始 SDP 切片。
///
/// ### 风险提示（Trade-offs）
/// - 未对值内的具体语法（如 `rtpmap`）做细分，后续可在上层实现类型安全封装。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Attribute<'a> {
    /// 属性键名。
    pub key: &'a str,
    /// 属性值，可能为空。
    pub value: Option<&'a str>,
}

/// 描述 SDP 中的媒体块。
///
/// ### 设计动机（Why）
/// - `m=` 行及其随后的 `c=`、`a=` 行共同定义某个媒体流的协商参数。
/// - 将属性与连接信息内聚在结构体中，方便上层一次性读取。
///
/// ### 契约定义（What）
/// - `media`：媒体类型，如 `audio`、`video`。
/// - `port`：媒体端口，以字符串形式保留原始值。
/// - `proto`：传输协议描述，如 `RTP/AVP`。
/// - `formats`：编码格式列表，保持零拷贝切片集合。
/// - `connection`：可选的媒体级连接信息。
/// - `attributes`：媒体级属性集合。
///
/// ### 风险提示（Trade-offs）
/// - 未处理多端口或端口范围；如需支持需在解析阶段额外拆解。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MediaDesc<'a> {
    /// 媒体类型。
    pub media: &'a str,
    /// 媒体端口。
    pub port: &'a str,
    /// 传输协议描述。
    pub proto: &'a str,
    /// 可用编码格式列表。
    pub formats: Vec<&'a str>,
    /// 媒体级连接信息。
    pub connection: Option<Connection<'a>>,
    /// 媒体级属性集合。
    pub attributes: Vec<Attribute<'a>>,
}

/// 表示完整的 SDP 会话描述。
///
/// ### 设计动机（Why）
/// - 聚合会话级信息（版本、发起人、时间窗口、属性）与多个媒体块，构成 SDP 的核心抽象。
/// - 以零拷贝方式提供读取视图，避免重复分配。
///
/// ### 契约定义（What）
/// - `version`：`v=` 行内容。
/// - `origin`：`o=` 解析后的结构。
/// - `session_name`：`s=` 行内容。
/// - `connection`：可选会话级连接信息。
/// - `timing`：`t=` 行内容。
/// - `attributes`：会话级属性集合。
/// - `media`：媒体块列表。
///
/// ### 风险提示（Trade-offs）
/// - 当前仅覆盖基础行；对于 `b=`、`k=`、`r=` 等扩展行暂不支持，解析时会忽略。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionDesc<'a> {
    /// SDP 版本号，通常为 `0`。
    pub version: &'a str,
    /// 会话发起人信息。
    pub origin: Origin<'a>,
    /// 会话名称。
    pub session_name: &'a str,
    /// 会话级连接信息。
    pub connection: Option<Connection<'a>>,
    /// 时间窗口描述。
    pub timing: Timing<'a>,
    /// 会话级属性集合。
    pub attributes: Vec<Attribute<'a>>,
    /// 媒体块列表。
    pub media: Vec<MediaDesc<'a>>,
}

/// 解析 SDP 文本。
///
/// ### 设计动机（Why）
/// - 为传输实现提供零拷贝的 SDP 读取能力，支持后续能力协商。
/// - 满足 `spark-tck` 在 `basic_core_lines` 中对核心行的解析验证。
///
/// ### 契约定义（What）
/// - **输入**：`input` 为完整 SDP 文本，支持 `\n` 或 `\r\n` 行分隔。
/// - **输出**：若解析成功返回 `SessionDesc`，其内部字符串全部借用 `input` 切片。
/// - **错误**：当必需行缺失或关键字段不足时返回 `SdpParseError`。
/// - **前置条件**：文本需符合 SDP 基本语法；未知或扩展行会被忽略。
/// - **后置条件**：返回结构包含会话级信息与媒体列表，媒体级属性按出现顺序归属各自媒体。
///
/// ### 实现逻辑（How）
/// 1. 顺序遍历每一行，利用首字符判定类型，实现“单通道”解析避免多次扫描。
/// 2. 针对必需行调用专门的解析函数（如 `parse_origin`、`parse_connection`）。
/// 3. 当遇到媒体行时，将后续属性/连接归属当前媒体。
/// 4. 其余未知标识符按 §5 要求忽略，实现向前兼容。
///
/// ### 风险与注意事项（Trade-offs & Gotchas）
/// - 暂未处理重复的必需行，若出现将覆盖前值；若需严格校验可在后续增强。
/// - 不解析 `r=`、`b=` 等扩展行，确保忽略策略符合“忽略未知行”的要求。
/// - 属性行若在任何媒体块之前出现，将归属于会话级。
pub fn parse_sdp<'a>(input: &'a str) -> spark_core::Result<SessionDesc<'a>, SdpParseError> {
    let mut version: Option<&'a str> = None;
    let mut origin: Option<Origin<'a>> = None;
    let mut session_name: Option<&'a str> = None;
    let mut session_connection: Option<Connection<'a>> = None;
    let mut timing: Option<Timing<'a>> = None;
    let mut session_attributes: Vec<Attribute<'a>> = Vec::new();
    let mut media_list: Vec<MediaDesc<'a>> = Vec::new();
    let mut current_media_index: Option<usize> = None;

    for raw_line in input.split('\n') {
        let line = raw_line.trim_end_matches('\r');
        if line.is_empty() {
            continue;
        }
        if line.len() < 2 || line.as_bytes()[1] != b'=' {
            // 安全性：要求形如 "x=..." 的结构，否则忽略。
            continue;
        }

        match line.as_bytes()[0] {
            b'v' => {
                version = Some(line[2..].trim());
            }
            b'o' => {
                origin = Some(parse_origin(line[2..].trim())?);
            }
            b's' => {
                session_name = Some(line[2..].trim());
            }
            b'c' => {
                let connection = parse_connection(line[2..].trim())?;
                if let Some(index) = current_media_index {
                    if let Some(media) = media_list.get_mut(index) {
                        media.connection = Some(connection);
                    }
                } else {
                    session_connection = Some(connection);
                }
            }
            b't' => {
                timing = Some(parse_timing(line[2..].trim())?);
            }
            b'm' => {
                let media = parse_media(line[2..].trim())?;
                media_list.push(media);
                current_media_index = Some(media_list.len() - 1);
            }
            b'a' => {
                let attribute = parse_attribute(line[2..].trim());
                if let Some(index) = current_media_index {
                    if let Some(media) = media_list.get_mut(index) {
                        media.attributes.push(attribute);
                    }
                } else {
                    session_attributes.push(attribute);
                }
            }
            _ => {
                // 忽略未知或暂未实现的行，保持 §5 要求的前向兼容。
            }
        }
    }

    let version = version.ok_or(SdpParseError::MissingVersion)?;
    let origin = origin.ok_or(SdpParseError::MissingOrigin)?;
    let session_name = session_name.ok_or(SdpParseError::MissingSessionName)?;
    let timing = timing.ok_or(SdpParseError::MissingTiming)?;

    Ok(SessionDesc {
        version,
        origin,
        session_name,
        connection: session_connection,
        timing,
        attributes: session_attributes,
        media: media_list,
    })
}

/// 根据 SDP 结构生成文本表示。
///
/// ### 设计动机（Why）
/// - 与 `parse_sdp` 成对提供能力，允许调用者在修改结构后重新序列化。
/// - 为 TCK 提供回写能力，验证“解析-生成”往返一致性。
///
/// ### 契约定义（What）
/// - **输入**：`desc` 为会话结构引用，字段需满足 SDP 基本约束。
/// - **输出**：返回使用 CRLF 结尾的 SDP 文本字符串。
/// - **前置条件**：调用者需保证结构中存在至少一个媒体块，以便符合常规 SDP；若为空则输出仅包含会话级行。
/// - **后置条件**：生成文本的字段顺序遵循标准：会话级核心行 → 会话属性 → 各媒体块及其附属行。
///
/// ### 实现逻辑（How）
/// 1. 使用 `String` 累加并显式写入行前缀，避免 `format!` 多余分配。
/// 2. 媒体行的格式列表逐个追加，确保零拷贝利用原始切片。
/// 3. 属性行根据是否存在值选择 `a=key:value` 或 `a=key` 形式。
///
/// ### 风险与注意事项（Trade-offs & Gotchas）
/// - 当前不会自动排序属性，完全保留输入顺序，保证往返测试一致。
/// - 若未来需要支持 `b=`、`k=` 等行，应在此函数中同步追加。
pub fn format_sdp(desc: &SessionDesc<'_>) -> String {
    let mut output = String::new();

    push_line(&mut output, "v=", desc.version);
    push_origin(&mut output, &desc.origin);
    push_line(&mut output, "s=", desc.session_name);

    if let Some(connection) = &desc.connection {
        push_connection(&mut output, connection);
    }

    push_timing(&mut output, &desc.timing);

    for attribute in &desc.attributes {
        push_attribute(&mut output, attribute);
    }

    for media in &desc.media {
        push_media(&mut output, media);
    }

    output
}

fn push_line(buffer: &mut String, prefix: &str, content: &str) {
    buffer.push_str(prefix);
    buffer.push_str(content);
    buffer.push_str("\r\n");
}

fn push_origin(buffer: &mut String, origin: &Origin<'_>) {
    buffer.push_str("o=");
    buffer.push_str(origin.username);
    buffer.push(' ');
    buffer.push_str(origin.session_id);
    buffer.push(' ');
    buffer.push_str(origin.session_version);
    buffer.push(' ');
    buffer.push_str(origin.net_type);
    buffer.push(' ');
    buffer.push_str(origin.addr_type);
    buffer.push(' ');
    buffer.push_str(origin.address);
    buffer.push_str("\r\n");
}

fn push_connection(buffer: &mut String, connection: &Connection<'_>) {
    buffer.push_str("c=");
    buffer.push_str(connection.net_type);
    buffer.push(' ');
    buffer.push_str(connection.addr_type);
    buffer.push(' ');
    buffer.push_str(connection.address);
    buffer.push_str("\r\n");
}

fn push_timing(buffer: &mut String, timing: &Timing<'_>) {
    buffer.push_str("t=");
    buffer.push_str(timing.start);
    buffer.push(' ');
    buffer.push_str(timing.stop);
    buffer.push_str("\r\n");
}

fn push_attribute(buffer: &mut String, attribute: &Attribute<'_>) {
    buffer.push_str("a=");
    buffer.push_str(attribute.key);
    if let Some(value) = attribute.value {
        buffer.push(':');
        buffer.push_str(value);
    }
    buffer.push_str("\r\n");
}

fn push_media(buffer: &mut String, media: &MediaDesc<'_>) {
    buffer.push_str("m=");
    buffer.push_str(media.media);
    buffer.push(' ');
    buffer.push_str(media.port);
    buffer.push(' ');
    buffer.push_str(media.proto);
    for fmt in &media.formats {
        buffer.push(' ');
        buffer.push_str(fmt);
    }
    buffer.push_str("\r\n");

    if let Some(connection) = &media.connection {
        push_connection(buffer, connection);
    }

    for attribute in &media.attributes {
        push_attribute(buffer, attribute);
    }
}

fn parse_origin<'a>(value: &'a str) -> spark_core::Result<Origin<'a>, SdpParseError> {
    let mut parts = value.split_whitespace();
    let username = parts.next();
    let session_id = parts.next();
    let session_version = parts.next();
    let net_type = parts.next();
    let addr_type = parts.next();
    let address = parts.next();

    if let (
        Some(username),
        Some(session_id),
        Some(session_version),
        Some(net_type),
        Some(addr_type),
        Some(address),
    ) = (
        username,
        session_id,
        session_version,
        net_type,
        addr_type,
        address,
    ) {
        Ok(Origin {
            username,
            session_id,
            session_version,
            net_type,
            addr_type,
            address,
        })
    } else {
        Err(SdpParseError::InvalidOrigin)
    }
}

fn parse_connection<'a>(value: &'a str) -> spark_core::Result<Connection<'a>, SdpParseError> {
    let mut parts = value.split_whitespace();
    let net_type = parts.next();
    let addr_type = parts.next();
    let address = parts.next();

    if let (Some(net_type), Some(addr_type), Some(address)) = (net_type, addr_type, address) {
        Ok(Connection {
            net_type,
            addr_type,
            address,
        })
    } else {
        Err(SdpParseError::InvalidConnection)
    }
}

fn parse_timing<'a>(value: &'a str) -> spark_core::Result<Timing<'a>, SdpParseError> {
    let mut parts = value.split_whitespace();
    let start = parts.next();
    let stop = parts.next();

    if let (Some(start), Some(stop)) = (start, stop) {
        Ok(Timing { start, stop })
    } else {
        Err(SdpParseError::InvalidTiming)
    }
}

fn parse_media<'a>(value: &'a str) -> spark_core::Result<MediaDesc<'a>, SdpParseError> {
    let mut parts = value.split_whitespace();
    let media = parts.next();
    let port = parts.next();
    let proto = parts.next();
    let formats: Vec<&'a str> = parts.collect();

    if let (Some(media), Some(port), Some(proto)) = (media, port, proto) {
        if formats.is_empty() {
            return Err(SdpParseError::InvalidMedia);
        }
        Ok(MediaDesc {
            media,
            port,
            proto,
            formats,
            connection: None,
            attributes: Vec::new(),
        })
    } else {
        Err(SdpParseError::InvalidMedia)
    }
}

fn parse_attribute<'a>(value: &'a str) -> Attribute<'a> {
    if let Some((key, val)) = value.split_once(':') {
        Attribute {
            key,
            value: Some(val),
        }
    } else {
        Attribute {
            key: value,
            value: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_and_format_basic() {
        let text = "v=0\r\no=- 0 0 IN IP4 127.0.0.1\r\ns=Test\r\nc=IN IP4 0.0.0.0\r\nt=0 0\r\na=tool:unit\r\nm=audio 49170 RTP/AVP 0 96\r\nc=IN IP4 203.0.113.1\r\na=rtpmap:0 PCMU/8000\r\na=rtpmap:96 opus/48000/2\r\n";
        let parsed = parse_sdp(text).expect("should parse");
        assert_eq!(parsed.version, "0");
        assert_eq!(parsed.origin.address, "127.0.0.1");
        assert_eq!(parsed.session_name, "Test");
        assert_eq!(parsed.connection.as_ref().unwrap().address, "0.0.0.0");
        assert_eq!(parsed.timing.start, "0");
        assert_eq!(parsed.attributes[0].key, "tool");
        assert_eq!(parsed.media.len(), 1);
        let media = &parsed.media[0];
        assert_eq!(media.media, "audio");
        assert_eq!(media.formats, vec!["0", "96"]);
        assert_eq!(media.attributes.len(), 2);

        let formatted = format_sdp(&parsed);
        assert!(formatted.contains("m=audio 49170 RTP/AVP 0 96\r\n"));
    }
}
