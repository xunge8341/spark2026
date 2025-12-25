//! 响应解析逻辑。
//!
//! ## 模块目的（Why）
//! - 将 RFC 3261 §7.2 中描述的状态行与头部解析为强类型结构。
//! - 支持 `spark-tck` 对响应头部进行断言，特别是 `Via` 回写 `rport` 的场景。
//!
//! ## 处理步骤（How）
//! 1. 校验状态行版本与状态码；
//! 2. 重用头部解析逻辑；
//! 3. 返回零拷贝的 [`SipMessage`](crate::types::SipMessage)。

use crate::{
    error::SipParseError,
    types::{SipMessage, StartLine, StatusLine},
};

use super::{parse_headers, split_first_line, split_headers_body};

/// 解析 SIP 响应文本。
///
/// ### 契约说明（What）
/// - **输入**：完整的响应报文，行结尾必须为 CRLF。
/// - **返回**：成功时给出 [`SipMessage`]，失败时返回 [`SipParseError`]。
/// - **前置条件**：状态行需符合 `SIP/2.0 <code> <reason>` 格式。
/// - **后置条件**：返回的切片均指向输入缓冲，未发生复制。
pub fn parse_response<'a>(input: &'a str) -> spark_core::Result<SipMessage<'a>, SipParseError> {
    let (line, rest) = split_first_line(input)?;
    let status_line = parse_status_line(line)?;
    let (header_block, body_block) = split_headers_body(rest)?;
    let headers = parse_headers(header_block)?;
    Ok(SipMessage {
        start_line: StartLine::Response(status_line),
        headers,
        body: body_block.as_bytes(),
    })
}

fn parse_status_line<'a>(line: &'a str) -> spark_core::Result<StatusLine<'a>, SipParseError> {
    let first_space = line.find(' ').ok_or(SipParseError::InvalidStatusLine)?;
    let version = &line[..first_space];
    if !version.eq_ignore_ascii_case("SIP/2.0") {
        return Err(SipParseError::UnsupportedVersion);
    }

    let rest = line[first_space + 1..].trim_start();
    let second_space = rest.find(' ');
    let (status_text, reason) = match second_space {
        Some(idx) => (&rest[..idx], rest[idx + 1..].trim_start()),
        None => (rest, ""),
    };

    if status_text.len() != 3 || !status_text.chars().all(|c| c.is_ascii_digit()) {
        return Err(SipParseError::InvalidStatusLine);
    }

    let status_code = status_text
        .parse::<u16>()
        .map_err(|_| SipParseError::InvalidStatusLine)?;

    Ok(StatusLine {
        version,
        status_code,
        reason,
    })
}
