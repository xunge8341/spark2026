//! 基础类型定义模块。
//!
//! ## 模块目标（Why）
//! - 为解析与序列化提供统一的零拷贝数据模型，避免在业务层重复描述 SIP 报文结构。
//! - 明确各字段与 RFC 章节之间的映射关系，后续扩展时可快速定位对应规则。
//!
//! ## 结构概览（What）
//! - [`Method`]：描述请求方法，支持识别常见标准方法并保留原始 token；
//! - [`SipUri`] / [`NameAddr`]：封装 URI 与带显示名的地址段；
//! - [`RequestLine`] / [`StatusLine`]：请求与响应的起始行；
//! - [`Header`]：核心头部（Via/From/To/Call-ID/CSeq/Max-Forwards/Contact）及扩展头的统一表示；
//! - [`SipMessage`]：组合起始行、头部与 body 的零拷贝报文结构。
//!
//! ## 实现策略（How）
//! - 所有切片均指向输入缓冲，保证无额外分配；
//! - 通过 `HeaderName::kind` 标记头部类型，实现大小写不敏感匹配；
//! - 对于参数字符串（如 URI 参数、未知 header 值），使用原文字符串延迟解析。
//!
//! ## 风险与扩展（Trade-offs）
//! - 当前 `Header::Extension` 仅保留原文，未来如需更丰富的解析需新增枚举分支；
//! - `SipMessage` 中 body 以 `&[u8]` 表示，若调用方需保持文本语义，需自行转换编码。

#[cfg(feature = "alloc")]
use alloc::vec::Vec;

use core::fmt;

/// SIP 方法枚举，保留常见标准方法并支持自定义扩展。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Method<'a> {
    /// RFC 3261 定义的 `INVITE`。
    Invite,
    /// RFC 3261 定义的 `ACK`。
    Ack,
    /// RFC 3261 定义的 `OPTIONS`。
    Options,
    /// RFC 3261 定义的 `BYE`。
    Bye,
    /// RFC 3261 定义的 `CANCEL`。
    Cancel,
    /// RFC 3261 定义的 `REGISTER`。
    Register,
    /// RFC 3262 定义的 `PRACK`。
    Prack,
    /// RFC 3265 定义的 `SUBSCRIBE`。
    Subscribe,
    /// RFC 3265 定义的 `NOTIFY`。
    Notify,
    /// RFC 3903 定义的 `PUBLISH`。
    Publish,
    /// RFC 2976 定义的 `INFO`。
    Info,
    /// RFC 3515 定义的 `REFER`。
    Refer,
    /// RFC 3428 定义的 `MESSAGE`。
    Message,
    /// RFC 3311 定义的 `UPDATE`。
    Update,
    /// 未被标准枚举覆盖的其它方法，使用原始 token。
    Extension(&'a str),
}

impl<'a> Method<'a> {
    /// 根据输入 token 构造方法枚举。
    pub fn from_token(token: &'a str) -> Self {
        match token {
            "INVITE" => Self::Invite,
            "ACK" => Self::Ack,
            "OPTIONS" => Self::Options,
            "BYE" => Self::Bye,
            "CANCEL" => Self::Cancel,
            "REGISTER" => Self::Register,
            "PRACK" => Self::Prack,
            "SUBSCRIBE" => Self::Subscribe,
            "NOTIFY" => Self::Notify,
            "PUBLISH" => Self::Publish,
            "INFO" => Self::Info,
            "REFER" => Self::Refer,
            "MESSAGE" => Self::Message,
            "UPDATE" => Self::Update,
            other => Self::Extension(other),
        }
    }

    /// 将方法枚举转换回文本表示。
    pub fn as_str(self) -> &'a str {
        match self {
            Self::Invite => "INVITE",
            Self::Ack => "ACK",
            Self::Options => "OPTIONS",
            Self::Bye => "BYE",
            Self::Cancel => "CANCEL",
            Self::Register => "REGISTER",
            Self::Prack => "PRACK",
            Self::Subscribe => "SUBSCRIBE",
            Self::Notify => "NOTIFY",
            Self::Publish => "PUBLISH",
            Self::Info => "INFO",
            Self::Refer => "REFER",
            Self::Message => "MESSAGE",
            Self::Update => "UPDATE",
            Self::Extension(token) => token,
        }
    }
}

/// SIP URI scheme。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SipScheme {
    /// 非加密 `sip:`。
    Sip,
    /// 加密 `sips:`。
    Sips,
}

impl fmt::Display for SipScheme {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Sip => f.write_str("sip"),
            Self::Sips => f.write_str("sips"),
        }
    }
}

/// SIP URI，根据 RFC 3261 §19 解析后的零拷贝结构。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SipUri<'a> {
    /// scheme 信息。
    pub scheme: SipScheme,
    /// `userinfo`（可包含用户名与密码），若缺失则为 `None`。
    pub userinfo: Option<&'a str>,
    /// 主机或域名。
    pub host: &'a str,
    /// 端口号，可选。
    pub port: Option<u16>,
    /// URI 参数（包含分号）；保留原文供上层解析。
    pub params: Option<&'a str>,
    /// header 参数（`?` 之后），原文切片。
    pub headers: Option<&'a str>,
}

impl<'a> SipUri<'a> {
    /// 判断是否显式携带了 `transport=` 参数。
    pub fn has_transport_param(&self) -> bool {
        self.params
            .map(|p| {
                p.split(';')
                    .any(|kv| kv.trim_start().starts_with("transport="))
            })
            .unwrap_or(false)
    }
}

/// 带显示名的地址（适用于 From/To/Contact）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NameAddr<'a> {
    /// 可选显示名，保留引号内文本。
    pub display_name: Option<&'a str>,
    /// 地址部分的 URI。
    pub uri: SipUri<'a>,
    /// 附加参数（例如 `;tag=`），原文切片。
    pub params: Option<&'a str>,
    /// 是否使用尖括号包裹 URI。
    pub enclosed: bool,
}

/// 请求起始行。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RequestLine<'a> {
    /// 请求方法。
    pub method: Method<'a>,
    /// 请求目标 URI。
    pub uri: SipUri<'a>,
    /// 协议版本文本（当前固定为 `SIP/2.0`）。
    pub version: &'a str,
}

/// 响应起始行。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StatusLine<'a> {
    /// 协议版本，预期为 `SIP/2.0`。
    pub version: &'a str,
    /// 三位状态码。
    pub status_code: u16,
    /// 原因短语（可为空）。
    pub reason: &'a str,
}

/// Header 名称种类，用于大小写无关匹配。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HeaderKind {
    /// `Via` 头。
    Via,
    /// `From` 头。
    From,
    /// `To` 头。
    To,
    /// `Call-ID` 头。
    CallId,
    /// `CSeq` 头。
    CSeq,
    /// `Max-Forwards` 头。
    MaxForwards,
    /// `Contact` 头。
    Contact,
    /// 未知或暂不处理的头。
    Other,
}

/// Header 名称，保留原文与语义。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HeaderName<'a> {
    /// 报文中出现的原始大小写形式。
    pub raw: &'a str,
    /// 归一化后的枚举分类。
    pub kind: HeaderKind,
}

impl<'a> HeaderName<'a> {
    /// 根据名称文本构造 [`HeaderName`]。
    pub fn new(name: &'a str) -> Self {
        let kind = if name.eq_ignore_ascii_case("via") {
            HeaderKind::Via
        } else if name.eq_ignore_ascii_case("from") {
            HeaderKind::From
        } else if name.eq_ignore_ascii_case("to") {
            HeaderKind::To
        } else if name.eq_ignore_ascii_case("call-id") || name.eq_ignore_ascii_case("callid") {
            HeaderKind::CallId
        } else if name.eq_ignore_ascii_case("cseq") {
            HeaderKind::CSeq
        } else if name.eq_ignore_ascii_case("max-forwards")
            || name.eq_ignore_ascii_case("maxforwards")
        {
            HeaderKind::MaxForwards
        } else if name.eq_ignore_ascii_case("contact") {
            HeaderKind::Contact
        } else {
            HeaderKind::Other
        };
        Self { raw: name, kind }
    }
}

/// `Via` 头中的 `rport` 表达形式。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ViaParamRport {
    /// 未携带 `rport` 参数。
    NotPresent,
    /// 以 `;rport` 请求回填。
    Requested,
    /// 已携带或被赋值的端口号。
    Value(u16),
}

/// `Via` 头的核心字段。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ViaHeader<'a> {
    /// `SIP/2.0/UDP` 等 sent-protocol 字段。
    pub sent_protocol: &'a str,
    /// `sent-by` 主机。
    pub host: &'a str,
    /// 可选端口。
    pub port: Option<u16>,
    /// `branch` 参数。
    pub branch: Option<&'a str>,
    /// `received` 参数。
    pub received: Option<&'a str>,
    /// `rport` 参数。
    pub rport: ViaParamRport,
    /// 其他参数原文，以分号分隔。
    pub params: Option<&'a str>,
}

/// `From`/`To` 头的别名。
pub type FromToHeader<'a> = NameAddr<'a>;

/// `Call-ID` 头。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CallIdHeader<'a> {
    /// call-id token。
    pub value: &'a str,
}

/// `CSeq` 头。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CSeqHeader<'a> {
    /// 序列号。
    pub sequence: u32,
    /// 方法。
    pub method: Method<'a>,
}

/// `Max-Forwards` 头。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MaxForwardsHeader {
    /// 剩余跳数。
    pub hops: u32,
}

/// `Contact` 头。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ContactHeader<'a> {
    /// 地址信息；当 `is_wildcard` 为 `true` 时为 `None`。
    pub address: Option<NameAddr<'a>>,
    /// 是否为 `*` 特殊值。
    pub is_wildcard: bool,
}

/// Header 枚举。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Header<'a> {
    /// `Via`。
    Via(ViaHeader<'a>),
    /// `From`。
    From(FromToHeader<'a>),
    /// `To`。
    To(FromToHeader<'a>),
    /// `Call-ID`。
    CallId(CallIdHeader<'a>),
    /// `CSeq`。
    CSeq(CSeqHeader<'a>),
    /// `Max-Forwards`。
    MaxForwards(MaxForwardsHeader),
    /// `Contact`。
    Contact(ContactHeader<'a>),
    /// 未解析的扩展头，保留原名与原值。
    Extension {
        /// 扩展头名称，保持原始大小写以便上层自定义处理。
        name: HeaderName<'a>,
        /// 扩展头值，包含折行还原后的内容。
        value: &'a str,
    },
}

/// 起始行的联合表示。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StartLine<'a> {
    /// 请求。
    Request(RequestLine<'a>),
    /// 响应。
    Response(StatusLine<'a>),
}

/// SIP 报文的统一表示。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SipMessage<'a> {
    /// 起始行。
    pub start_line: StartLine<'a>,
    /// 解析出的头部集合。
    pub headers: Vec<Header<'a>>,
    /// 报文主体，原始字节切片。
    pub body: &'a [u8],
}

impl<'a> SipMessage<'a> {
    /// 按顺序查找第一个匹配的 header。
    pub fn find_header(&self, kind: HeaderKind) -> Option<&Header<'a>> {
        self.headers.iter().find(|h| match h {
            Header::Via(_) => kind == HeaderKind::Via,
            Header::From(_) => kind == HeaderKind::From,
            Header::To(_) => kind == HeaderKind::To,
            Header::CallId(_) => kind == HeaderKind::CallId,
            Header::CSeq(_) => kind == HeaderKind::CSeq,
            Header::MaxForwards(_) => kind == HeaderKind::MaxForwards,
            Header::Contact(_) => kind == HeaderKind::Contact,
            Header::Extension { name, .. } => name.kind == kind,
        })
    }
}
