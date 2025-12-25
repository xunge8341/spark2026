//! WebSocket 帧与 SIP 文本报文之间的转换工具集。
//!
//! # 教案级总览
//! - **定位**：该模块实现 RFC 7118 §5 要求的 WebSocket 帧聚合/拆分逻辑，是 `spark-codec-sip`
//!   与传输实现之间的衔接层。
//! - **职责**：提供 `ws_to_sip`、`sip_to_ws` 两个方向的转换函数，并定义帧视图与错误类型。
//! - **扩展性**：通过 [`crate::ws::SipWebSocketFrame`] trait 允许调用方自定义帧载体，只要满足 `BufView`
//!   契约即可实现零拷贝复用；同时提供 `frame_text_binary` 子模块承载 WebSocket 文本/二进制帧的聚合
//!   与拆分逻辑，便于 TCK 与传输实现共享同一套参考实现。

#![allow(clippy::module_name_repetitions)]

#[cfg(feature = "alloc")]
use alloc::{string::String, vec::Vec};

#[cfg(feature = "alloc")]
/// WebSocket 文本/二进制帧聚合与拆分的教学示例实现，详见子模块内的逐步讲解。
pub mod frame_text_binary;

use core::{fmt, str};

use spark_codecs::buffer::{BufView, Chunks};

use crate::{
    SipFormatError, SipParseError,
    fmt::write_message,
    parse::{parse_request, parse_response},
    types::SipMessage,
};

/// WebSocket 帧的语义标签，用于区分文本与二进制承载。
///
/// # 教案级说明
/// - **意图（Why）**：RFC 6455 允许 SIP over WebSocket 既可以使用 `Text` 帧，也可以使用 `Binary`
///   帧传输同样的文本内容。本枚举显式记录帧类别，供聚合与拆分逻辑选择正确的 UTF-8 校验策略。
/// - **体系位置（Where）**：该类型位于 `ws` 模块的 API 表面，被 `ws_to_sip`/`sip_to_ws` 及帧视图
///   类型共享，调用方可据此声明目标帧类型。
/// - **契约（What）**：取值仅限 `Text` 与 `Binary` 两种；枚举实现 `Copy`/`Eq` 以便在测试中直接比较。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WsFrameFormat {
    /// WebSocket 文本帧（RFC 6455 §5.6）。
    Text,
    /// WebSocket 二进制帧。
    Binary,
}

impl fmt::Display for WsFrameFormat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Text => f.write_str("text"),
            Self::Binary => f.write_str("binary"),
        }
    }
}

/// 为 SIP over WebSocket 提供统一的帧视图接口。
///
/// # 教案级注释
/// - **意图（Why）**：抽象出帧视图 trait，允许调用方用现有 `BufView` 实现包装自身缓冲，
///   而无需显式复制；
/// - **架构角色（How）**：继承 `BufView`，额外暴露 `frame_format` 方法，使 `ws_to_sip` 在泛型上下文中
///   能够访问帧字节与类型信息；
/// - **契约（What）**：实现者必须返回稳定的帧类型；`BufView` 契约要求分片的生命周期至少与 `self`
///   等长。
pub trait SipWebSocketFrame: BufView {
    /// 返回当前帧的类别，用于驱动 UTF-8 校验与错误报告。
    fn frame_format(&self) -> WsFrameFormat;
}

/// 为引用语义的帧构造器，便于在零拷贝场景下包装现有缓冲。
///
/// - **Why**：测试或上层协议通常以 `Vec<u8>` / `Box<[u8]>` 储存 WebSocket payload；通过该结构可以直接
///   借用底层缓冲而无需复制；
/// - **How**：内部仅保存帧类型与对底层 `BufView` 的引用，实现 `SipWebSocketFrame` 时简单委托给被借用对象；
/// - **What**：`format` 字段定义帧类型，`payload` 持有底层只读缓冲引用。
pub struct WsFrameRef<'a, B: BufView + ?Sized> {
    format: WsFrameFormat,
    payload: &'a B,
}

impl<'a, B> WsFrameRef<'a, B>
where
    B: BufView + ?Sized,
{
    /// 构建新的帧引用。
    ///
    /// # 契约说明
    /// - **输入**：`format` 表示帧类型；`payload` 必须满足 `BufView` 契约（生命周期覆盖整个帧视图）。
    /// - **前置条件**：调用方需保证 `payload` 在返回的 `WsFrameRef` 生命周期内保持只读。
    /// - **后置条件**：返回结构仅借用 `payload`，不会修改或复制底层数据。
    pub fn new(format: WsFrameFormat, payload: &'a B) -> Self {
        Self { format, payload }
    }

    /// 返回帧类型标签，便于调试或自定义分支。
    pub fn format(&self) -> WsFrameFormat {
        self.format
    }

    /// 暴露底层 `BufView`，供高级场景直接访问。
    pub fn payload(&self) -> &'a B {
        self.payload
    }
}

impl<B> BufView for WsFrameRef<'_, B>
where
    B: BufView + ?Sized,
{
    fn as_chunks(&self) -> Chunks<'_> {
        self.payload.as_chunks()
    }

    fn len(&self) -> usize {
        self.payload.len()
    }
}

impl<B> SipWebSocketFrame for WsFrameRef<'_, B>
where
    B: BufView + ?Sized,
{
    fn frame_format(&self) -> WsFrameFormat {
        self.format
    }
}

/// 拥有型 WebSocket 帧，适合在 `sip_to_ws` 输出路径中使用。
///
/// # 说明
/// - **Why**：当需要生成新的帧发送到网络时，必须持有 payload；
/// - **How**：内部存储 `Vec<u8>`，并实现 `BufView` 以便再次零拷贝解析；
/// - **What**：通过 `format()`/`payload()` 方法可分别获取类型与字节视图。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WsFrameOwned {
    format: WsFrameFormat,
    payload: Vec<u8>,
}

impl WsFrameOwned {
    /// 基于给定类型与 payload 构造帧。
    ///
    /// - **前置条件**：`payload` 必须为 UTF-8 字节序列以便 `Text` 帧正常传输；
    /// - **后置条件**：内部复制输入向量所有权，由调用方负责生命周期管理。
    pub fn new(format: WsFrameFormat, payload: Vec<u8>) -> Self {
        Self { format, payload }
    }

    /// 返回帧类型，便于调用方根据类型进行处理。
    pub fn format(&self) -> WsFrameFormat {
        self.format
    }

    /// 返回帧 payload 的只读切片视图。
    pub fn payload(&self) -> &[u8] {
        &self.payload
    }

    /// 将内部 payload 所有权转移给调用方。
    pub fn into_payload(self) -> Vec<u8> {
        self.payload
    }
}

impl BufView for WsFrameOwned {
    fn as_chunks(&self) -> Chunks<'_> {
        Chunks::from_single(self.payload.as_slice())
    }

    fn len(&self) -> usize {
        self.payload.len()
    }
}

impl SipWebSocketFrame for WsFrameOwned {
    fn frame_format(&self) -> WsFrameFormat {
        self.format
    }
}

/// WebSocket ↔ SIP 文本转换过程中可能出现的错误类型。
///
/// # 教案式说明
/// - **Why**：明确列举 UTF-8 校验失败、SIP 解析失败、帧分片异常等场景，便于调用方做精细化错误处理；
/// - **How**：使用枚举承载上下文信息（例如帧索引、原始错误），并实现 `Display`/`Error`；
/// - **What**：所有错误均为终态，调用方需按需重试或记录日志。
#[derive(Debug)]
pub enum WsSipError {
    /// 帧 payload 为空，不符合 SIP 报文最小要求。
    EmptyPayload {
        /// 触发错误的帧索引，便于日志定位。
        index: usize,
    },
    /// 帧 payload 存在多个分片，当前实现无法保持零拷贝语义。
    FragmentedPayload {
        /// 触发错误的帧索引。
        index: usize,
        /// 实际观察到的分片数量，帮助实现者评估是否需要额外聚合。
        observed_chunks: usize,
    },
    /// 文本或二进制帧的字节序列不是合法 UTF-8。
    Utf8 {
        /// 触发错误的帧索引。
        index: usize,
        /// 帧的类型标签，用于区分文本与二进制场景。
        format: WsFrameFormat,
        /// 具体的 UTF-8 解码错误来源。
        source: str::Utf8Error,
    },
    /// SIP 报文解析失败。
    Parse {
        /// 触发解析失败的帧索引。
        index: usize,
        /// 解析器返回的原始错误，包含失败原因。
        source: SipParseError,
    },
    /// SIP 报文格式化失败（理论上只会在头部不满足约束时出现）。
    Format {
        /// SIP 序列化过程中产生的错误。
        source: SipFormatError,
    },
}

impl fmt::Display for WsSipError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyPayload { index } => {
                write!(
                    f,
                    "frame #{index} payload is empty, cannot build SIP message"
                )
            }
            Self::FragmentedPayload {
                index,
                observed_chunks,
            } => write!(
                f,
                "frame #{index} is composed of {observed_chunks} chunks; zero-copy SIP mapping requires a single contiguous slice"
            ),
            Self::Utf8 { index, format, .. } => {
                write!(f, "{format} frame #{index} contains non UTF-8 bytes")
            }
            Self::Parse { index, source } => {
                write!(
                    f,
                    "failed to parse SIP message from frame #{index}: {source}"
                )
            }
            Self::Format { source } => {
                write!(f, "failed to format SIP message into text: {source}")
            }
        }
    }
}

#[cfg(feature = "std")]
impl std::error::Error for WsSipError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Utf8 { source, .. } => Some(source),
            Self::Parse { source, .. } => Some(source),
            Self::Format { source } => Some(source),
            _ => None,
        }
    }
}

/// 提取帧中唯一的分片切片，若出现多分片则返回错误。
fn borrow_single_chunk<F>(frame: &F, index: usize) -> spark_core::Result<&[u8], WsSipError>
where
    F: SipWebSocketFrame,
{
    let mut chunks = frame.as_chunks();
    let Some(chunk) = chunks.next() else {
        return Err(WsSipError::EmptyPayload { index });
    };
    if chunks.next().is_some() {
        let observed = frame.chunk_count();
        return Err(WsSipError::FragmentedPayload {
            index,
            observed_chunks: observed,
        });
    }
    Ok(chunk)
}

/// 将一组 WebSocket 帧聚合为 SIP 文本报文并解析为结构化对象。
///
/// # 教案级注释
/// - **意图（Why）**：实现 RFC 7118 §5 要求的帧聚合流程，使上层可以直接对 SIP 报文进行结构化处理；
/// - **逻辑步骤（How）**：
///   1. 逐帧提取唯一分片并校验 UTF-8；
///   2. 优先尝试按照 SIP 请求解析，失败后回退到响应解析；
///   3. 将解析成功的 [`SipMessage`] 收集到返回向量中，保持对原始缓冲的零拷贝引用；
/// - **契约（What）**：
///   - **前置条件**：输入帧的 payload 必须为完整的 SIP 文本；
///   - **后置条件**：返回的 `SipMessage` 生命周期与输入帧相同，调用方需保证帧在消息使用期间保持有效。
pub fn ws_to_sip<'a, F>(frames: &'a [F]) -> spark_core::Result<Vec<SipMessage<'a>>, WsSipError>
where
    F: SipWebSocketFrame,
{
    let mut messages = Vec::with_capacity(frames.len());

    for (index, frame) in frames.iter().enumerate() {
        let bytes = borrow_single_chunk(frame, index)?;
        let text = str::from_utf8(bytes).map_err(|err| WsSipError::Utf8 {
            index,
            format: frame.frame_format(),
            source: err,
        })?;

        // 先尝试按照请求解析，若失败则回退到响应解析。
        let message = match parse_request(text) {
            Ok(request) => request,
            Err(request_err) => match parse_response(text) {
                Ok(response) => response,
                Err(_) => {
                    return Err(WsSipError::Parse {
                        index,
                        source: request_err,
                    });
                }
            },
        };

        messages.push(message);
    }

    Ok(messages)
}

/// 将单个 SIP 报文序列化为文本，并同时生成文本帧与二进制帧表示。
///
/// # 教案级注释
/// - **意图（Why）**：为传输实现提供统一出口，确保无论底层通道选择 WebSocket 文本帧还是二进制帧，
///   都可复用同一段序列化逻辑；
/// - **实现步骤（How）**：
///   1. 使用 [`write_message`] 将结构化 `SipMessage` 写入字符串缓冲；
///   2. 将字符串转换为字节向量，并生成 `Text`/`Binary` 两种帧；
///   3. 依次压入 `out_frames`，供调用方发送；
/// - **契约（What）**：
///   - **输入**：`message` 必须是完整的 SIP 报文；
///   - **输出**：`out_frames` 末尾会追加两个拥有型帧；
///   - **失败条件**：若写入过程中违反 SIP 格式约束，则返回 [`WsSipError::Format`]。
pub fn sip_to_ws(
    message: &SipMessage<'_>,
    out_frames: &mut Vec<WsFrameOwned>,
) -> spark_core::Result<(), WsSipError> {
    let mut buffer = String::new();
    write_message(&mut buffer, message).map_err(|err| WsSipError::Format { source: err })?;
    let bytes = buffer.into_bytes();

    // 文本帧需要单独拷贝，确保 UTF-8 编码独立存储。
    out_frames.push(WsFrameOwned::new(WsFrameFormat::Text, bytes.clone()));
    out_frames.push(WsFrameOwned::new(WsFrameFormat::Binary, bytes));
    Ok(())
}
