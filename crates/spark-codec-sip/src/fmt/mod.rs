//! 序列化模块入口。
//!
//! ## 模块目标（Why）
//! - 将解析后的 [`SipMessage`](crate::types::SipMessage) 再次转化为文本，便于发送响应或回放测试。
//! - 提供请求/响应/头部粒度的写入函数，方便上层按需组合。
//!
//! ## 结构概览（What）
//! - `request`：负责请求起始行及整体序列化；
//! - `response`：负责响应序列化；
//! - `headers`：负责核心头部的格式化。
//!
//! ## 使用方式（How）
//! - 常用场景可调用 [`write_message`] 输出完整报文；
//! - 若需自定义 header 顺序，可分别调用 `write_request_line`、`write_headers` 等函数组合。

mod headers;
mod message;
mod request;
mod response;

pub use headers::{write_header, write_headers};
pub use message::write_message;
pub use request::write_request;
pub use response::write_response;
