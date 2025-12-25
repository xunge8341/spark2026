//! 头部解析逻辑。
//!
//! ## 模块目标（Why）
//! - 按照 RFC 3261 §7.3.1 处理大小写与折行，并解析核心头部字段。
//! - 为传输与会话控制提供结构化数据（如 `Via.rport`、`From.tag` 等）。
//!
//! ## 设计思路（How）
//! 1. 逐行扫描 header 段，将折行后的多行视作一个逻辑头；
//! 2. 根据头部名称匹配具体解析器，返回强类型结构；
//! 3. 未覆盖的头部保留原文，以备上层自定义解析。

#[cfg(feature = "alloc")]
use alloc::vec::Vec;

use crate::{
    error::SipParseError,
    types::{
        CSeqHeader, CallIdHeader, ContactHeader, Header, HeaderKind, HeaderName, MaxForwardsHeader,
        Method, NameAddr, ViaHeader, ViaParamRport,
    },
};

use super::common::{parse_sip_uri, skip_lws, take_token_until, trim_lws_end};

/// 解析 header 区域。
pub(crate) fn parse_headers<'a>(
    input: &'a str,
) -> spark_core::Result<Vec<Header<'a>>, SipParseError> {
    if input.is_empty() {
        return Ok(Vec::new());
    }

    let mut headers = Vec::new();
    let mut offset = 0usize;
    let bytes = input.as_bytes();

    let mut current_name: Option<HeaderName<'a>> = None;
    let mut value_start = 0usize;
    let mut last_line_end = 0usize;

    while offset < bytes.len() {
        let (line_end, has_crlf) = match input[offset..].find("\r\n") {
            Some(rel) => (offset + rel, true),
            None => (bytes.len(), false),
        };
        let line = &input[offset..line_end];
        let next_offset = if has_crlf { line_end + 2 } else { bytes.len() };

        // 教案级提示：SIP 报文最后一个头部允许直接跟随空行，这意味着 `split_headers_body`
        // 切分后末尾可能缺少 `CRLF`。若不做额外处理，解析器会误判为 `UnexpectedEof`，
        // 导致像本次测试中的 REGISTER 请求无法被接受。此处允许尾部无 `CRLF` 的最后一行，
        // 以符合 RFC 3261 §7.3.1 对 header 折行的描述，同时保持对其他行的严格校验。

        if line.is_empty() {
            return Err(SipParseError::InvalidHeaderValue);
        }

        if matches!(line.as_bytes()[0], b' ' | b'\t') {
            if current_name.is_none() {
                return Err(SipParseError::InvalidHeaderValue);
            }
            last_line_end = line_end;
            offset = next_offset;
            continue;
        }

        if let Some(name) = current_name.take() {
            let end = trim_lws_end(input, last_line_end);
            let value = &input[value_start..end];
            headers.push(build_header(name, value)?);
        }

        let colon = line.find(':').ok_or(SipParseError::InvalidHeaderName)?;
        let name_text = line[..colon].trim_end();
        if name_text.is_empty() {
            return Err(SipParseError::InvalidHeaderName);
        }
        let header_name = HeaderName::new(name_text);

        let mut start = offset + colon + 1;
        while start < line_end && matches!(input.as_bytes()[start], b' ' | b'\t') {
            start += 1;
        }

        current_name = Some(header_name);
        value_start = start;
        last_line_end = line_end;
        offset = next_offset;
    }

    if let Some(name) = current_name.take() {
        let end = trim_lws_end(input, last_line_end);
        let value = &input[value_start..end];
        headers.push(build_header(name, value)?);
    }

    Ok(headers)
}

fn build_header<'a>(
    name: HeaderName<'a>,
    value: &'a str,
) -> spark_core::Result<Header<'a>, SipParseError> {
    match name.kind {
        HeaderKind::Via => parse_via(value).map(Header::Via),
        HeaderKind::From => parse_name_addr(value).map(Header::From),
        HeaderKind::To => parse_name_addr(value).map(Header::To),
        HeaderKind::CallId => parse_call_id(value).map(Header::CallId),
        HeaderKind::CSeq => parse_cseq(value).map(Header::CSeq),
        HeaderKind::MaxForwards => parse_max_forwards(value).map(Header::MaxForwards),
        HeaderKind::Contact => parse_contact(value).map(Header::Contact),
        HeaderKind::Other => Ok(Header::Extension { name, value }),
    }
}

fn parse_via<'a>(value: &'a str) -> spark_core::Result<ViaHeader<'a>, SipParseError> {
    let mut idx = skip_lws(value, 0);
    let (protocol, next_idx) = take_token_until(value, idx, |b| matches!(b, b' ' | b'\t' | b';'));
    if protocol.is_empty() {
        return Err(SipParseError::InvalidViaHeader);
    }
    idx = skip_lws(value, next_idx);

    let sent_by_start = idx;
    let (sent_by_token, after_sent_by) = take_token_until(value, idx, |b| matches!(b, b';'));
    if sent_by_token.is_empty() {
        return Err(SipParseError::InvalidViaHeader);
    }
    idx = after_sent_by;

    let sent_by_end = trim_lws_end(value, idx);
    let sent_by = &value[sent_by_start..sent_by_end];
    let (host, port) = split_host_port(sent_by)?;

    let trimmed_end = trim_lws_end(value, value.len());
    let mut params_slice = None;
    let cursor = skip_lws(value, idx);
    if cursor < trimmed_end && value.as_bytes()[cursor] == b';' {
        params_slice = Some(&value[cursor..trimmed_end]);
    }

    let mut branch = None;
    let mut received = None;
    let mut rport = ViaParamRport::NotPresent;

    if let Some(params) = params_slice {
        let mut pos = 0usize;
        while pos < params.len() {
            if params.as_bytes()[pos] != b';' {
                pos += 1;
                continue;
            }
            pos += 1;
            pos = skip_lws(params, pos);
            let (name, mut next) = take_token_until(params, pos, |b| matches!(b, b'=' | b';'));
            if name.is_empty() {
                pos = next;
                continue;
            }
            let mut value_opt = None;
            if next < params.len() && params.as_bytes()[next] == b'=' {
                next += 1;
                next = skip_lws(params, next);
                let value_start = next;
                let (_, new_next) = take_token_until(params, next, |b| matches!(b, b';'));
                let trimmed = trim_lws_end(params, new_next);
                value_opt = Some(&params[value_start..trimmed]);
                next = trimmed;
            }
            if name.eq_ignore_ascii_case("branch") {
                if let Some(v) = value_opt {
                    branch = Some(v);
                }
            } else if name.eq_ignore_ascii_case("received") {
                if let Some(v) = value_opt {
                    received = Some(v);
                }
            } else if name.eq_ignore_ascii_case("rport") {
                match value_opt {
                    Some(v) if !v.is_empty() => {
                        let parsed = v
                            .parse::<u16>()
                            .map_err(|_| SipParseError::InvalidViaHeader)?;
                        rport = ViaParamRport::Value(parsed);
                    }
                    Some(_) | None => {
                        rport = ViaParamRport::Requested;
                    }
                }
            }
            pos = skip_lws(params, next);
        }
    }

    Ok(ViaHeader {
        sent_protocol: protocol,
        host,
        port,
        branch,
        received,
        rport,
        params: params_slice,
    })
}

fn split_host_port(input: &str) -> spark_core::Result<(&str, Option<u16>), SipParseError> {
    if input.is_empty() {
        return Err(SipParseError::InvalidViaHeader);
    }
    if let Some(colon) = input.rfind(':') {
        let port_part = &input[colon + 1..];
        if !port_part.is_empty() && port_part.bytes().all(|b| b.is_ascii_digit()) {
            let host = &input[..colon];
            let port = port_part
                .parse::<u16>()
                .map_err(|_| SipParseError::InvalidViaHeader)?;
            return Ok((host, Some(port)));
        }
    }
    Ok((input, None))
}

fn parse_name_addr<'a>(value: &'a str) -> spark_core::Result<NameAddr<'a>, SipParseError> {
    let mut idx = skip_lws(value, 0);
    let bytes = value.as_bytes();
    let mut display_name = None;

    if idx < bytes.len() && bytes[idx] == b'"' {
        idx += 1;
        let start = idx;
        while idx < bytes.len() && bytes[idx] != b'"' {
            idx += 1;
        }
        if idx >= bytes.len() {
            return Err(SipParseError::InvalidNameAddrHeader);
        }
        display_name = Some(&value[start..idx]);
        idx += 1;
        idx = skip_lws(value, idx);
    }

    let uri;
    let mut params = None;
    let mut enclosed = false;
    if idx < bytes.len() && bytes[idx] == b'<' {
        idx += 1;
        let uri_start = idx;
        while idx < bytes.len() && bytes[idx] != b'>' {
            idx += 1;
        }
        if idx >= bytes.len() {
            return Err(SipParseError::InvalidNameAddrHeader);
        }
        uri = parse_sip_uri(&value[uri_start..idx])?;
        idx += 1;
        idx = skip_lws(value, idx);
        if idx < bytes.len() {
            params = Some(&value[idx..]);
        }
        enclosed = true;
    } else {
        let (uri_token, next) = take_token_until(value, idx, |b| matches!(b, b';'));
        if uri_token.is_empty() {
            return Err(SipParseError::InvalidNameAddrHeader);
        }
        uri = parse_sip_uri(uri_token)?;
        idx = next;
        idx = skip_lws(value, idx);
        if idx < bytes.len() {
            params = Some(&value[idx..]);
        }
    }

    Ok(NameAddr {
        display_name,
        uri,
        params,
        enclosed,
    })
}

fn parse_call_id<'a>(value: &'a str) -> spark_core::Result<CallIdHeader<'a>, SipParseError> {
    let start = skip_lws(value, 0);
    let end = trim_lws_end(value, value.len());
    if start >= end {
        return Err(SipParseError::InvalidCallId);
    }
    Ok(CallIdHeader {
        value: &value[start..end],
    })
}

fn parse_cseq<'a>(value: &'a str) -> spark_core::Result<CSeqHeader<'a>, SipParseError> {
    let mut idx = skip_lws(value, 0);
    let (number_part, next) = take_token_until(value, idx, |b| matches!(b, b' ' | b'\t'));
    if number_part.is_empty() {
        return Err(SipParseError::InvalidCSeq);
    }
    let sequence = number_part
        .parse::<u32>()
        .map_err(|_| SipParseError::InvalidCSeq)?;
    idx = skip_lws(value, next);
    let method_part = &value[idx..trim_lws_end(value, value.len())];
    if method_part.is_empty() {
        return Err(SipParseError::InvalidCSeq);
    }
    Ok(CSeqHeader {
        sequence,
        method: Method::from_token(method_part),
    })
}

fn parse_max_forwards(value: &str) -> spark_core::Result<MaxForwardsHeader, SipParseError> {
    let start = skip_lws(value, 0);
    let end = trim_lws_end(value, value.len());
    let slice = &value[start..end];
    if slice.is_empty() {
        return Err(SipParseError::InvalidMaxForwards);
    }
    let hops = slice
        .parse::<u32>()
        .map_err(|_| SipParseError::InvalidMaxForwards)?;
    Ok(MaxForwardsHeader { hops })
}

fn parse_contact<'a>(value: &'a str) -> spark_core::Result<ContactHeader<'a>, SipParseError> {
    let start = skip_lws(value, 0);
    let end = trim_lws_end(value, value.len());
    if start >= end {
        return Err(SipParseError::InvalidContact);
    }
    let slice = &value[start..end];
    if slice == "*" {
        return Ok(ContactHeader {
            address: None,
            is_wildcard: true,
        });
    }
    let addr = parse_name_addr(slice)?;
    Ok(ContactHeader {
        address: Some(addr),
        is_wildcard: false,
    })
}
