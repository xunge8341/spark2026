//! 响应报文序列化。
//!
//! ## 模块目标（Why）
//! - 将 [`StatusLine`](crate::types::StatusLine) 与头部、主体重新编码为 SIP 响应文本，支持回写 `Via` 等字段后发送。
//!
//! ## 使用方式（How）
//! - 调用 [`write_response`] 输出完整响应；
//! - 若仅需状态行，可调用 [`write_status_line`]。

use core::{fmt, str};

use crate::{
    error::SipFormatError,
    fmt::headers::write_headers,
    types::{Header, StatusLine},
};

/// 写出完整响应报文。
pub fn write_response<W: fmt::Write>(
    writer: &mut W,
    line: &StatusLine<'_>,
    headers: &[Header<'_>],
    body: &[u8],
) -> spark_core::Result<(), SipFormatError> {
    write_status_line(writer, line)?;
    writer.write_str("\r\n")?;
    write_headers(writer, headers)?;
    writer.write_str("\r\n")?;
    write_body(writer, body)?;
    Ok(())
}

/// 写出响应状态行。
pub fn write_status_line<W: fmt::Write>(
    writer: &mut W,
    line: &StatusLine<'_>,
) -> spark_core::Result<(), SipFormatError> {
    if line.reason.is_empty() {
        write!(writer, "{} {}", line.version, line.status_code)?;
    } else {
        write!(
            writer,
            "{} {} {}",
            line.version, line.status_code, line.reason
        )?;
    }
    Ok(())
}

fn write_body<W: fmt::Write>(
    writer: &mut W,
    body: &[u8],
) -> spark_core::Result<(), SipFormatError> {
    if body.is_empty() {
        return Ok(());
    }
    let text = str::from_utf8(body).map_err(|_| SipFormatError::NonUtf8Body)?;
    writer.write_str(text)?;
    Ok(())
}
