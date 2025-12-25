//! 请求报文序列化。
//!
//! ## 模块目标（Why）
//! - 将 [`RequestLine`](crate::types::RequestLine) 与头部、body 重组为文本，支持代理或测试工具回发请求。
//! - 输出遵循 `CRLF` 行结尾，并保持核心字段大小写规范。
//!
//! ## 使用场景（What）
//! - 生成新的 SIP 请求：调用 [`write_request`] 填充方法/URI/头部/主体；
//! - 仅需起始行可调用 [`write_request_line`]。

use core::{fmt, str};

use crate::{
    error::SipFormatError,
    fmt::headers::{write_headers, write_uri},
    types::{Header, RequestLine},
};

/// 写出完整请求报文。
pub fn write_request<W: fmt::Write>(
    writer: &mut W,
    line: &RequestLine<'_>,
    headers: &[Header<'_>],
    body: &[u8],
) -> spark_core::Result<(), SipFormatError> {
    write_request_line(writer, line)?;
    writer.write_str("\r\n")?;
    write_headers(writer, headers)?;
    writer.write_str("\r\n")?;
    write_body(writer, body)?;
    Ok(())
}

/// 写出请求行。
pub fn write_request_line<W: fmt::Write>(
    writer: &mut W,
    line: &RequestLine<'_>,
) -> spark_core::Result<(), SipFormatError> {
    writer.write_str(line.method.as_str())?;
    writer.write_char(' ')?;
    write_uri(writer, &line.uri)?;
    writer.write_char(' ')?;
    writer.write_str(line.version)?;
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
