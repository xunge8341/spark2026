//! 报文级序列化入口。
//!
//! ## 模块目的（Why）
//! - 结合请求/响应两类起始行，提供统一的写入函数，简化调用代码。

use core::fmt;

use crate::{
    error::SipFormatError,
    types::{SipMessage, StartLine},
};

use super::{request::write_request, response::write_response};

/// 写出完整的 [`SipMessage`]。
pub fn write_message<W: fmt::Write>(
    writer: &mut W,
    message: &SipMessage<'_>,
) -> spark_core::Result<(), SipFormatError> {
    match &message.start_line {
        StartLine::Request(line) => write_request(writer, line, &message.headers, message.body),
        StartLine::Response(line) => write_response(writer, line, &message.headers, message.body),
    }
}
