//! 公共解析工具集合。
//!
//! ## 模块目的（Why）
//! - 避免在请求/响应/头部解析中重复实现同样的字符串处理逻辑。
//! - 将 RFC 3261 中关于空白字符与 URI 的规则集中管理，降低后续维护成本。
//!
//! ## 能力概览（What）
//! - 行切分：[`split_first_line`]、[`split_headers_body`]；
//! - 线性空白处理：[`skip_lws`] 与 [`trim_lws_end`]；
//! - URI 解析：[`parse_sip_uri`] 将文本转换为 [`SipUri`](crate::types::SipUri)。
//!
//! ## 实现策略（How）
//! - 所有函数均保持零拷贝，只返回原始切片；
//! - 对 `CRLF` 与制表符统一视作 linear white space (LWS)，符合 §7.3.1。
//!
//! ## 风险提示（Trade-offs）
//! - 当前 URI 解析未覆盖 IPv6 zone-id 等高级语法，后续可在 `parse_sip_uri` 中扩展。
//! - 若输入包含非 ASCII 字符，本模块按字节处理，需确保上层约束合法性。

use crate::{
    error::SipParseError,
    types::{SipScheme, SipUri},
};

/// 查找首行并返回剩余部分。
pub(crate) fn split_first_line(input: &str) -> spark_core::Result<(&str, &str), SipParseError> {
    match input.find("\r\n") {
        Some(idx) => Ok((&input[..idx], &input[idx + 2..])),
        None => Err(SipParseError::UnexpectedEof),
    }
}

/// 将 header 与 body 分离。
pub(crate) fn split_headers_body(input: &str) -> spark_core::Result<(&str, &str), SipParseError> {
    match input.find("\r\n\r\n") {
        Some(idx) => Ok((&input[..idx], &input[idx + 4..])),
        None => Err(SipParseError::UnexpectedEof),
    }
}

/// 将字节流拆分为 header 与 body，以支持二进制负载透传。
///
/// # 教案式说明
/// - **意图 (Why)**：为上层提供“header 仍按 UTF-8 解析、body 保持原始字节” 的切分能力，避免因正文中出现非 UTF-8 数据而导致整包拒收。
/// - **契约 (What)**：
///   - 输入：完整的 SIP 报文字节切片；
///   - 输出：`(header_bytes, body_bytes)`，均为原切片的子切片，不发生复制；
///   - 前置条件：报文必须包含 `\r\n\r\n` 分隔符；
///   - 后置条件：返回的两个切片生命周期与输入一致，可直接用于后续 UTF-8 校验或二进制透传。
/// - **风险提示 (Trade-offs & Gotchas)**：
///   - 若分隔符缺失，返回 `UnexpectedEof`，调用方需决定是记录告警还是直接拒绝；
///   - 函数不校验 `Content-Length`，仅负责物理切分，语义层校验需在上层完成。
pub(crate) fn split_headers_body_bytes(
    input: &[u8],
) -> spark_core::Result<(&[u8], &[u8]), SipParseError> {
    match input.windows(4).position(|window| window == b"\r\n\r\n") {
        Some(idx) => Ok((&input[..idx], &input[idx + 4..])),
        None => Err(SipParseError::UnexpectedEof),
    }
}

/// 跳过线性空白（包含 CRLF 后紧跟的空白）。
pub(crate) fn skip_lws(input: &str, mut offset: usize) -> usize {
    let bytes = input.as_bytes();
    while offset < bytes.len() {
        match bytes[offset] {
            b' ' | b'\t' => offset += 1,
            b'\r' => {
                if offset + 1 < bytes.len() && bytes[offset + 1] == b'\n' {
                    offset += 2;
                    while offset < bytes.len() && (bytes[offset] == b' ' || bytes[offset] == b'\t')
                    {
                        offset += 1;
                    }
                } else {
                    break;
                }
            }
            _ => break,
        }
    }
    offset
}

/// 去除结尾的线性空白并返回新的结尾下标。
pub(crate) fn trim_lws_end(input: &str, mut end: usize) -> usize {
    let bytes = input.as_bytes();
    while end > 0 {
        match bytes[end - 1] {
            b' ' | b'\t' => end -= 1,
            b'\n' => {
                if end >= 2 && bytes[end - 2] == b'\r' {
                    end -= 2;
                } else {
                    break;
                }
            }
            _ => break,
        }
    }
    end
}

/// 从 `start` 开始读取 token，遇到 `stop` 条件或 LWS 停止。
pub(crate) fn take_token_until<F>(input: &str, start: usize, stop: F) -> (&str, usize)
where
    F: Fn(u8) -> bool,
{
    let bytes = input.as_bytes();
    let mut end = start;
    while end < bytes.len() {
        let b = bytes[end];
        if b == b'\r' || b == b' ' || b == b'\t' || stop(b) {
            break;
        }
        end += 1;
    }
    (&input[start..end], end)
}

/// 解析 SIP URI。
pub(crate) fn parse_sip_uri<'a>(input: &'a str) -> spark_core::Result<SipUri<'a>, SipParseError> {
    let trimmed_start = skip_lws(input, 0);
    let trimmed_end = trim_lws_end(input, input.len());
    if trimmed_start >= trimmed_end {
        return Err(SipParseError::InvalidUri);
    }
    let slice = &input[trimmed_start..trimmed_end];

    let colon = slice.find(':').ok_or(SipParseError::InvalidUri)?;
    let scheme_text = &slice[..colon];
    let scheme = if scheme_text.eq_ignore_ascii_case("sip") {
        SipScheme::Sip
    } else if scheme_text.eq_ignore_ascii_case("sips") {
        SipScheme::Sips
    } else {
        return Err(SipParseError::InvalidUri);
    };

    let mut rest = &slice[colon + 1..];
    if rest.is_empty() {
        return Err(SipParseError::InvalidUri);
    }

    let mut headers = None;
    if let Some(pos) = rest.find('?') {
        headers = Some(&rest[pos + 1..]);
        rest = &rest[..pos];
    }

    let mut params = None;
    if let Some(pos) = rest.find(';') {
        params = Some(&rest[pos..]);
        rest = &rest[..pos];
    }

    let userinfo_split_limit = rest.len();
    let mut userinfo = None;
    if let Some(at_pos) = rest[..userinfo_split_limit].rfind('@') {
        userinfo = Some(&rest[..at_pos]);
        rest = &rest[at_pos + 1..];
    }

    if rest.is_empty() {
        return Err(SipParseError::InvalidUri);
    }

    let (host, port) = if rest.starts_with('[') {
        let closing = rest.find(']').ok_or(SipParseError::InvalidUri)?;
        let host = &rest[1..closing];
        let mut port = None;
        if rest.len() > closing + 1 {
            if rest.as_bytes()[closing + 1] != b':' {
                return Err(SipParseError::InvalidUri);
            }
            let port_text = &rest[closing + 2..];
            if port_text.is_empty() {
                return Err(SipParseError::InvalidUri);
            }
            port = Some(parse_port(port_text)?);
        }
        (host, port)
    } else if let Some(pos) = rest.rfind(':') {
        let port_candidate = &rest[pos + 1..];
        if !port_candidate.is_empty() && port_candidate.bytes().all(|b| b.is_ascii_digit()) {
            let host = &rest[..pos];
            let port = Some(parse_port(port_candidate)?);
            (host, port)
        } else {
            (rest, None)
        }
    } else {
        (rest, None)
    };

    if host.is_empty() {
        return Err(SipParseError::InvalidUri);
    }

    Ok(SipUri {
        scheme,
        userinfo,
        host,
        port,
        params,
        headers,
    })
}

fn parse_port(text: &str) -> spark_core::Result<u16, SipParseError> {
    text.parse::<u16>().map_err(|_| SipParseError::InvalidUri)
}
