//! 请求解析逻辑。
//!
//! ## 模块目的（Why）
//! - 将 RFC 3261 §7.1 描述的请求行与后续头部解析为零拷贝结构。
//! - 为 `spark-tck` 提供对请求解析的核心能力，便于后续针对 `rport` 等字段做断言。
//!
//! ## 关键流程（How）
//! 1. 切分首行并校验 `SIP/2.0` 版本；
//! 2. 调用 [`parse_headers`](super::headers::parse_headers) 解析头部；
//! 3. 将主体部分按字节切片返回，避免复制。

use crate::{
    error::SipParseError,
    types::{Method, RequestLine, SipMessage, StartLine},
};

use super::{
    parse_headers, parse_sip_uri, split_first_line, split_headers_body, split_headers_body_bytes,
};
use core::str;

/// 解析 SIP 请求文本。
///
/// ### 设计动机（Why）
/// - 将文本转换为 [`SipMessage`] 结构，方便后续业务逻辑基于类型系统进行处理。
///
/// ### 契约说明（What）
/// - **输入**：`input` 必须是包含 CRLF 行结尾的完整请求报文。
/// - **返回**：成功时返回零拷贝的 [`SipMessage`]；失败时给出具体的 [`SipParseError`]。
/// - **前置条件**：文本需符合 RFC 3261 §7.1 格式；
/// - **后置条件**：返回的 `SipMessage` 中所有切片引用原始 `input` 的生命周期。
///
/// ### 实现细节（How）
/// - 使用 `split_first_line` 与 `split_headers_body` 切分文本；
/// - 请求行解析完成后委托 `parse_headers` 处理折行与核心头部；
/// - body 以 `&[u8]` 形式返回，保留原始编码。
pub fn parse_request<'a>(input: &'a str) -> spark_core::Result<SipMessage<'a>, SipParseError> {
    let (line, rest) = split_first_line(input)?;
    let request_line = parse_request_line(line)?;
    let (header_block, body_block) = split_headers_body(rest)?;
    let headers = parse_headers(header_block)?;
    Ok(SipMessage {
        start_line: StartLine::Request(request_line),
        headers,
        body: body_block.as_bytes(),
    })
}

/// 解析 SIP 请求字节流，仅对 header 执行 UTF-8 校验。
///
/// ### 教案式说明
/// - **意图（Why）**：允许携带非 UTF-8 正文（例如 ISUP 隧道或二进制扩展）时仍能完成 INVITE/REGISTER 等起始行与头部解析。
/// - **契约（What）**：
///   - 输入：包含 CRLF 行结尾与 `\r\n\r\n` 分隔符的完整请求字节流；
///   - 返回：零拷贝 [`SipMessage`]，其中 body 直接引用原始二进制切片；
///   - 前置条件：header 必须是合法 UTF-8（符合 RFC 3261 的 ASCII/UTF-8 约束）；
///   - 后置条件：若分隔符缺失或 header 无法通过 UTF-8 校验，将返回 [`SipParseError::UnexpectedEof`] 或 [`SipParseError::InvalidHeaderValue`]。
/// - **实现要点（How）**：
///   1. 调用 [`split_headers_body_bytes`] 快速定位 `\r\n\r\n` 分隔符，避免对正文进行 UTF-8 校验；
///   2. 仅对 header 部分执行 `from_utf8`，随后复用 `parse_request_line` 与 `parse_headers` 完成语法解析；
///   3. 将 body 保留为 `&[u8]` 直接塞入 [`SipMessage`]，保证零拷贝透传。
/// - **风险提示（Trade-offs & Gotchas）**：
///   - 函数不校验 `Content-Length` 与正文编码，调用方在解析 SDP 或其它负载时需自行校验；
///   - 若调用方期望严格 UTF-8，可继续使用 [`parse_request`] 保持原有行为。
pub fn parse_request_bytes<'a>(
    input: &'a [u8],
) -> spark_core::Result<SipMessage<'a>, SipParseError> {
    let (header_block, body_block) = split_headers_body_bytes(input)?;
    let headers_text =
        str::from_utf8(header_block).map_err(|_| SipParseError::InvalidHeaderValue)?;
    let (line, header_block) = split_first_line(headers_text)?;
    let request_line = parse_request_line(line)?;
    let headers = parse_headers(header_block)?;
    Ok(SipMessage {
        start_line: StartLine::Request(request_line),
        headers,
        body: body_block,
    })
}

fn parse_request_line<'a>(line: &'a str) -> spark_core::Result<RequestLine<'a>, SipParseError> {
    let mut parts = line.split_whitespace();
    let method = parts.next().ok_or(SipParseError::InvalidRequestLine)?;
    let uri_text = parts.next().ok_or(SipParseError::InvalidRequestLine)?;
    let version = parts.next().ok_or(SipParseError::InvalidRequestLine)?;

    if !version.eq_ignore_ascii_case("SIP/2.0") {
        return Err(SipParseError::UnsupportedVersion);
    }

    if parts.next().is_some() {
        return Err(SipParseError::InvalidRequestLine);
    }

    Ok(RequestLine {
        method: Method::from_token(method),
        uri: parse_sip_uri(uri_text)?,
        version,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{Header, Method, StartLine};

    /// 教案级说明：验证 `parse_request` 可接受以 CRLF 空行结尾的常见 REGISTER 报文。
    ///
    /// - **意图（Why）**：回归测试本模块对 RFC 3261 §7.3.1 的支持，避免再次出现末尾 `CRLF`
    ///   缺失时误报 `UnexpectedEof` 的缺陷。
    /// - **流程（How）**：
    ///   1. 构造包含标准头部（Via/Max-Forwards/To/From/Contact/Call-ID/CSeq/Content-Length）的示例报文；
    ///   2. 调用 `parse_request` 并断言起始行、头部枚举解析以及扩展头部值；
    ///   3. 确认解析结果 body 为空切片，契合 `Content-Length: 0`。
    /// - **契约（What）**：若函数返回错误或缺失任一关键头部，测试即失败。
    #[test]
    fn parse_register_with_standard_headers() {
        let register = "REGISTER sip:spark.invalid SIP/2.0\r\n\
Via: SIP/2.0/UDP client.invalid;branch=z9hG4bK-1;rport\r\n\
Max-Forwards: 70\r\n\
To: <sip:alice@client.invalid>\r\n\
From: <sip:alice@client.invalid>;tag=1928301774\r\n\
Contact: <sip:alice@client.invalid>\r\n\
Call-ID: a84b4c76e66710\r\n\
CSeq: 314159 REGISTER\r\n\
Content-Length: 0\r\n\r\n";

        let message = parse_request(register).expect("REGISTER 报文应成功解析");

        match &message.start_line {
            StartLine::Request(line) => {
                assert_eq!(line.method, Method::Register);
                assert_eq!(line.uri.host, "spark.invalid");
            }
            other => panic!("解析结果应为请求，实际: {other:?}"),
        }

        assert!(
            message
                .headers
                .iter()
                .any(|header| matches!(header, Header::Via(_))),
            "应解析出 Via 头"
        );
        assert!(
            message
                .headers
                .iter()
                .any(|header| matches!(header, Header::MaxForwards(_))),
            "应解析出 Max-Forwards 头"
        );
        assert!(
            message
                .headers
                .iter()
                .any(|header| matches!(header, Header::Contact(_))),
            "应解析出 Contact 头"
        );
        assert!(
            message.headers.iter().any(|header| matches!(
                header,
                Header::Extension { name, value }
                    if name.raw.eq_ignore_ascii_case("Content-Length") && value.trim() == "0"
            )),
            "Content-Length 扩展头解析失败"
        );

        assert!(message.body.is_empty(), "Content-Length:0 时 body 应为空");
    }
}
