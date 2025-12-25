//! Header 序列化实现。
//!
//! ## 设计动机（Why）
//! - 为响应回写提供精确的头部输出控制，尤其是 `Via` 中的 `rport` 等参数。
//! - 保留未知头部的原文，以便上层扩展。
//!
//! ## 使用方式（What & How）
//! - 调用 [`write_header`] 序列化单个头部；
//! - 调用 [`write_headers`] 依次输出多个头部，函数会自动添加 `\r\n` 结尾；
//! - 生成的文本遵循大小写规范（核心头部使用标准名称，其余沿用原文）。

use core::fmt;

use crate::{
    error::SipFormatError,
    parse::common::{skip_lws, take_token_until, trim_lws_end},
    types::{Header, HeaderName, NameAddr, SipUri, ViaParamRport},
};

/// 写出一组头部。
pub fn write_headers<W: fmt::Write>(
    writer: &mut W,
    headers: &[Header<'_>],
) -> spark_core::Result<(), SipFormatError> {
    for header in headers {
        write_header(writer, header)?;
        writer.write_str("\r\n")?;
    }
    Ok(())
}

/// 写出单个头部。
pub fn write_header<W: fmt::Write>(
    writer: &mut W,
    header: &Header<'_>,
) -> spark_core::Result<(), SipFormatError> {
    match header {
        Header::Via(via) => {
            writer.write_str("Via: ")?;
            writer.write_str(via.sent_protocol)?;
            writer.write_char(' ')?;
            writer.write_str(via.host)?;
            if let Some(port) = via.port {
                write!(writer, ":{}", port)?;
            }
            if let Some(branch) = via.branch {
                write!(writer, ";branch={}", branch)?;
            }
            if let Some(received) = via.received {
                write!(writer, ";received={}", received)?;
            }
            match via.rport {
                ViaParamRport::NotPresent => {}
                ViaParamRport::Requested => writer.write_str(";rport")?,
                ViaParamRport::Value(port) => write!(writer, ";rport={}", port)?,
            }
            if let Some(extra) = via.params {
                write_via_extras(writer, extra)?;
            }
        }
        Header::From(name) => {
            writer.write_str("From: ")?;
            write_name_addr(writer, name)?;
        }
        Header::To(name) => {
            writer.write_str("To: ")?;
            write_name_addr(writer, name)?;
        }
        Header::CallId(call_id) => {
            writer.write_str("Call-ID: ")?;
            writer.write_str(call_id.value)?;
        }
        Header::CSeq(cseq) => {
            write!(writer, "CSeq: {} {}", cseq.sequence, cseq.method.as_str())?;
        }
        Header::MaxForwards(max) => {
            write!(writer, "Max-Forwards: {}", max.hops)?;
        }
        Header::Contact(contact) => {
            writer.write_str("Contact: ")?;
            if contact.is_wildcard {
                writer.write_char('*')?;
            } else if let Some(addr) = contact.address {
                write_name_addr(writer, &addr)?;
            }
        }
        Header::Extension { name, value } => {
            write_extension(writer, name, value)?;
        }
    }
    Ok(())
}

fn write_extension<W: fmt::Write>(
    writer: &mut W,
    name: &HeaderName<'_>,
    value: &str,
) -> spark_core::Result<(), SipFormatError> {
    writer.write_str(name.raw)?;
    writer.write_str(":")?;
    if !value.is_empty() {
        writer.write_char(' ')?;
        writer.write_str(value)?;
    }
    Ok(())
}

fn write_name_addr<W: fmt::Write>(
    writer: &mut W,
    name: &NameAddr<'_>,
) -> spark_core::Result<(), SipFormatError> {
    if let Some(display) = name.display_name {
        write!(writer, "\"{}\" ", display)?;
    }
    let use_brackets = name.enclosed || name.display_name.is_some();
    if use_brackets {
        writer.write_char('<')?;
    }
    write_uri(writer, &name.uri)?;
    if use_brackets {
        writer.write_char('>')?;
    }
    if let Some(params) = name.params {
        writer.write_str(params)?;
    }
    Ok(())
}

pub(crate) fn write_uri<W: fmt::Write>(
    writer: &mut W,
    uri: &SipUri<'_>,
) -> spark_core::Result<(), SipFormatError> {
    write!(writer, "{}:", uri.scheme)?;
    if let Some(userinfo) = uri.userinfo {
        write!(writer, "{}@", userinfo)?;
    }
    writer.write_str(uri.host)?;
    if let Some(port) = uri.port {
        write!(writer, ":{}", port)?;
    }
    if let Some(params) = uri.params {
        writer.write_str(params)?;
    }
    if let Some(headers) = uri.headers {
        writer.write_char('?')?;
        writer.write_str(headers)?;
    }
    Ok(())
}

fn write_via_extras<W: fmt::Write>(
    writer: &mut W,
    raw: &str,
) -> spark_core::Result<(), SipFormatError> {
    let mut pos = 0usize;
    while pos < raw.len() {
        if raw.as_bytes()[pos] != b';' {
            pos += 1;
            continue;
        }
        let segment_start = pos;
        pos += 1;
        pos = skip_lws(raw, pos);
        let (name, mut next) = take_token_until(raw, pos, |b| matches!(b, b'=' | b';'));
        let mut end = next;
        if next < raw.len() && raw.as_bytes()[next] == b'=' {
            next += 1;
            next = skip_lws(raw, next);
            let (_, value_end) = take_token_until(raw, next, |b| matches!(b, b';'));
            end = value_end;
            pos = value_end;
        } else {
            pos = next;
        }
        pos = skip_lws(raw, pos);
        let trimmed_end = trim_lws_end(raw, end);
        let segment = &raw[segment_start..trimmed_end];
        if name.eq_ignore_ascii_case("branch")
            || name.eq_ignore_ascii_case("received")
            || name.eq_ignore_ascii_case("rport")
        {
            continue;
        }
        writer.write_str(segment)?;
    }
    Ok(())
}
