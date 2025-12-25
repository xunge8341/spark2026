//! WebSocket 文本/二进制帧与 SIP 报文之间的映射工具集。
//!
//! # 教案级总览
//! - **定位 (Why)**：负责在传输层与 SIP 编解码之间搬运“帧 ↔ 文本”转换，是 `spark-tck`
//!   验证互通性时的基准实现；
//! - **流程 (How)**：封装帧解析、掩码解码、分片聚合与长度编码细节，并暴露 `ws_to_sip` / `sip_to_ws`
//!   两个方向的高层函数；
//! - **契约 (What)**：输入输出均采用拥有型 `Vec<u8>` 或零拷贝 `BufView`，要求调用方在 `alloc`
//!   特性启用的环境中使用；
//! - **风险提示**：实现以可读性优先，采用缓冲拷贝换取简化逻辑，重度实时场景需关注分配开销。

use alloc::vec::Vec;
use core::{fmt, mem, str};

use spark_codecs::buffer::BufView;

/// WebSocket 数据帧在 SIP 映射中的语义分类。
///
/// # 教案级说明
/// - **意图 (Why)**：RFC 7118 §5 要求 SIP 报文被封装在 WebSocket 文本消息中，但在互通
///   实践中仍可能遇到供应商使用二进制帧承载文本。因此枚举区分 Text/Binary，以便转换逻辑
///   在聚合与拆分阶段采取相同的处理流程。
/// - **体系位置 (Architecture)**：供 [`SipMessage`] 表示底层载荷的来源类型，进一步影响
///   `sip_to_ws` 生成帧时选择的 `opcode`。
/// - **契约 (What)**：`Text` 表示 opcode=0x1，`Binary` 表示 opcode=0x2；其余 opcode 由
///   `ws_to_sip` 拒绝。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrameKind {
    /// WebSocket 文本帧。
    Text,
    /// WebSocket 二进制帧。
    Binary,
}

/// 聚合后的 SIP 文本报文。
///
/// # 教案级说明
/// - **意图 (Why)**：封装从一个或多个 WebSocket 帧还原出的 SIP 报文正文，同时记录原始
///   帧类型，便于后续拆分时保持对称性。
/// - **整体位置 (Architecture)**：作为 `ws_to_sip` 的输出与 `sip_to_ws` 的输入，承上启下
///   地连接“帧级”与“文本报文级”处理流程。
/// - **构造契约 (What)**：
///   - `frame_kind` 指示在还原为 WebSocket 帧时应选择的 opcode；
///   - `payload` 保存完整的 SIP 文本字节（UTF-8），若来自二进制帧仍保持原始字节序列；
///   - `text` 构造函数会验证 UTF-8，有效性不满足即返回错误；
///   - `binary` 构造函数不做额外校验，用于容忍厂商的非标准封装。
/// - **风险提示 (Gotchas)**：`payload` 为拥有型向量，意味着生成 `SipMessage` 会分配新
///   内存；这是在解掩码/聚合时不可避免的成本，需要调用方在批量处理场景中复用缓冲区。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SipMessage {
    frame_kind: FrameKind,
    payload: Vec<u8>,
}

impl SipMessage {
    /// 以 UTF-8 文本构建 SIP 报文。
    ///
    /// - **输入参数**：`payload` 为任意可转为 `Vec<u8>` 的类型（如 `String`、`&[u8]`）。
    /// - **前置条件**：字节序列必须是合法的 UTF-8，以满足 RFC 7118 对文本消息的约束。
    /// - **后置条件**：成功返回的实例保证 `frame_kind == FrameKind::Text` 且内部字节与输入
    ///   完全一致。
    /// - **逻辑摘要 (How)**：收集字节 -> 验证 UTF-8 -> 构建结构体。
    pub fn text<P>(payload: P) -> spark_core::Result<Self, WsSipError>
    where
        P: Into<Vec<u8>>,
    {
        let bytes = payload.into();
        str::from_utf8(&bytes).map_err(WsSipError::InvalidTextPayload)?;
        Ok(Self {
            frame_kind: FrameKind::Text,
            payload: bytes,
        })
    }

    /// 以二进制载荷构建 SIP 报文。
    ///
    /// - **适用场景**：兼容某些遗留实现使用 opcode=0x2 发送文本的情况。
    /// - **输入输出契约**：不校验内容编码，调用方需在必要时自行进行 UTF-8 检查。
    pub fn binary<P>(payload: P) -> Self
    where
        P: Into<Vec<u8>>,
    {
        Self {
            frame_kind: FrameKind::Binary,
            payload: payload.into(),
        }
    }

    /// 返回底层字节视图。
    pub fn payload(&self) -> &[u8] {
        &self.payload
    }

    /// 返回帧类型，用于指导逆向编码。
    pub fn frame_kind(&self) -> FrameKind {
        self.frame_kind
    }
}

/// WebSocket ↔ SIP 映射过程中的错误类型。
///
/// # 教案级说明
/// - **Why**：为调用方提供可判定的失败原因，便于在 TCK 中断言具体违反了哪条 RFC 约束。
/// - **How**：细分长度解析、opcode 校验、UTF-8 检查等阶段的失败分支。
/// - **What**：所有枚举变体均实现 `Display`，便于日志记录；`InvalidTextPayload` 持有
///   `Utf8Error` 以输出失败偏移。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WsSipError {
    /// 帧长度不足两个字节，无法解析基础头部。
    FrameTooShort {
        /// 实际收到的总字节数，用于向上层报告缺失量估算。
        /// 实际观测到的帧字节数，帮助定位截断来源。
        actual: usize,
    },
    /// 扩展长度字段或掩码键不完整。
    IncompleteHeader {
        /// 期望的最小头部长度（含扩展字段），供重试时预估缓冲大小。
        expected: usize,
        /// 实际可用的字节数，辅助判断数据截断或解码器错误。
        /// 解析头部所需的完整字节数（含扩展长度与掩码键）。
        actual: usize,
    },
    /// payload 长度超出当前平台可表示范围。
    PayloadLengthOverflow {
        /// 帧宣称的 payload 长度，后续可记录到日志以排查协议兼容性问题。
        declared: u64,
    },
    /// 帧声明的 payload 长度与实际字节数不一致。
    PayloadLengthMismatch {
        /// 头部中宣告的 payload 字节数。
        expected: usize,
        /// 根据缓冲长度推导出的实际 payload 字节数。
        actual: usize,
    },
    /// 控制帧被错误地拆分（RFC 6455 §5.5）。
    FragmentedControl {
        /// 导致异常的 opcode，便于在日志中定位问题帧类型。
        opcode: u8,
    },
    /// 收到未知或不支持的 opcode。
    UnsupportedOpcode {
        /// 原始 opcode 数值，可帮助调用方快速定位需要扩展的帧类型。
        opcode: u8,
    },
    /// 在未开始数据帧的情况下收到 continuation 帧。
    UnexpectedContinuation,
    /// 在前一条消息尚未 FIN 的情况下收到新的数据帧。
    MessageInterleaving {
        /// 触发乱序的 opcode，结合日志可还原出问题序列。
        opcode: u8,
    },
    /// 输入结束时仍存在未完成的分片序列。
    DanglingFragment,
    /// 文本帧载荷未通过 UTF-8 校验。
    InvalidTextPayload(str::Utf8Error),
}

impl fmt::Display for WsSipError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WsSipError::FrameTooShort { actual } => {
                write!(f, "WebSocket 帧长度不足，实际 {actual} 字节")
            }
            WsSipError::IncompleteHeader { expected, actual } => write!(
                f,
                "WebSocket 帧头部不完整，期望 {expected} 字节但仅收到 {actual}"
            ),
            WsSipError::PayloadLengthOverflow { declared } => {
                write!(f, "payload 长度 {declared} 超出平台 usize 上限")
            }
            WsSipError::PayloadLengthMismatch { expected, actual } => write!(
                f,
                "payload 长度不匹配，声明 {expected} 字节，实际收到 {actual}"
            ),
            WsSipError::FragmentedControl { opcode } => {
                write!(f, "控制帧 opcode={opcode:#x} 不允许分片")
            }
            WsSipError::UnsupportedOpcode { opcode } => {
                write!(f, "不支持的 WebSocket opcode={opcode:#x}")
            }
            WsSipError::UnexpectedContinuation => {
                write!(f, "收到 continuation 帧但不存在待续消息")
            }
            WsSipError::MessageInterleaving { opcode } => {
                write!(f, "上一条消息尚未 FIN，却收到新的数据帧 opcode={opcode:#x}")
            }
            WsSipError::DanglingFragment => {
                write!(f, "输入结束时存在未完成的帧分片")
            }
            WsSipError::InvalidTextPayload(err) => {
                write!(f, "文本帧 payload 不是合法 UTF-8：{err}")
            }
        }
    }
}

impl From<str::Utf8Error> for WsSipError {
    fn from(value: str::Utf8Error) -> Self {
        WsSipError::InvalidTextPayload(value)
    }
}

/// 将 WebSocket 帧序列聚合成 SIP 文本报文。
///
/// # 教案级说明
/// - **意图 (Why)**：实现 RFC 7118 §5 所述“每条 WebSocket 消息对应一条 SIP 报文”的聚合逻辑，
///   支持处理分片（fragmentation）、掩码（masking）以及文本/二进制两种数据帧。
/// - **流程 (How)**：
///   1. 逐帧解析头部，校验 opcode、FIN 与长度字段；
///   2. 对携带掩码的 payload 执行原地解掩码；
///   3. 若为数据帧（Text/Binary），累积到当前消息缓存；遇到 continuation 时继续追加；
///   4. 当 FIN=1 时构建 [`SipMessage`] 并推入结果；控制帧则忽略但校验不得分片；
///   5. 输入结束后若仍存在未完成的分片，则返回 [`WsSipError::DanglingFragment`]。
/// - **契约 (What)**：
///   - `frames` 中的每个元素必须是完整的单个 WebSocket 帧缓冲；
///   - 返回的 `Vec` 中元素顺序与帧序列中的消息出现顺序一致；
///   - 任意解析错误将立即返回 `Err`，不产生部分结果。
/// - **注意事项 (Trade-offs)**：实现为了保持可读性，将 `BufView` flatten 为连续缓冲后再解析，
///   牺牲了极端场景的零拷贝；若后续需要提升性能，可在解析阶段直接遍历分片。
pub fn ws_to_sip(frames: &[&dyn BufView]) -> spark_core::Result<Vec<SipMessage>, WsSipError> {
    let mut messages = Vec::new();
    let mut assembling = Vec::new();
    let mut current_kind: Option<FrameKind> = None;

    for view in frames {
        let frame = parse_frame(*view)?;
        match frame.opcode {
            OPCODE_CONTINUATION => {
                let kind = current_kind.ok_or(WsSipError::UnexpectedContinuation)?;
                assembling.extend_from_slice(&frame.payload);
                if frame.fin {
                    let payload = mem::take(&mut assembling);
                    let message = match kind {
                        FrameKind::Text => SipMessage::text(payload)?,
                        FrameKind::Binary => SipMessage::binary(payload),
                    };
                    messages.push(message);
                    current_kind = None;
                }
            }
            OPCODE_TEXT | OPCODE_BINARY => {
                if current_kind.is_some() {
                    return Err(WsSipError::MessageInterleaving {
                        opcode: frame.opcode,
                    });
                }
                let kind = if frame.opcode == OPCODE_TEXT {
                    FrameKind::Text
                } else {
                    FrameKind::Binary
                };
                current_kind = Some(kind);
                assembling.extend_from_slice(&frame.payload);
                if frame.fin {
                    let payload = mem::take(&mut assembling);
                    let message = match kind {
                        FrameKind::Text => SipMessage::text(payload)?,
                        FrameKind::Binary => SipMessage::binary(payload),
                    };
                    messages.push(message);
                    current_kind = None;
                }
            }
            OPCODE_CLOSE | OPCODE_PING | OPCODE_PONG => {
                if !frame.fin {
                    return Err(WsSipError::FragmentedControl {
                        opcode: frame.opcode,
                    });
                }
                // 控制帧与 SIP 报文无关，直接忽略。
            }
            _ => {
                return Err(WsSipError::UnsupportedOpcode {
                    opcode: frame.opcode,
                });
            }
        }
    }

    if current_kind.is_some() || !assembling.is_empty() {
        return Err(WsSipError::DanglingFragment);
    }

    Ok(messages)
}

/// 将 SIP 文本报文拆分为 WebSocket 帧。
///
/// # 教案级说明
/// - **意图 (Why)**：为 TCK 及未来的 WS 传输实现提供统一的编码逻辑，确保生成的帧满足
///   RFC 6455 的基本格式要求。
/// - **流程 (How)**：
///   1. 根据 `frame_kind` 选择 opcode（0x1 或 0x2）；
///   2. 计算 payload 长度并写入基础/扩展长度字段；
///   3. 由于本方法面向服务器侧使用，因此不对 payload 施加掩码；
///   4. 将 FIN 位置 1，产生单帧消息并写入 `out_frames`。
/// - **契约 (What)**：
///   - `message` 必须来源于本模块构造函数，保证文本帧 payload 已通过 UTF-8 校验；
///   - `out_frames` 将追加一条新的帧缓冲，调用方可选择在外层进行 further fragmentation。
/// - **权衡 (Trade-offs)**：当前实现总是生成单帧消息，若未来需要切分大型 payload，可
///   在外层改造或扩展本函数的参数以支持自定义分片策略。
pub fn sip_to_ws(message: &SipMessage, out_frames: &mut Vec<Vec<u8>>) {
    let opcode = match message.frame_kind {
        FrameKind::Text => OPCODE_TEXT,
        FrameKind::Binary => OPCODE_BINARY,
    };
    let payload = message.payload();
    let mut frame = Vec::with_capacity(2 + payload.len());
    frame.push(FLAG_FIN | opcode);
    encode_length(payload.len(), &mut frame);
    frame.extend_from_slice(payload);
    out_frames.push(frame);
}

/// 解析单个 WebSocket 帧。
fn parse_frame(view: &dyn BufView) -> spark_core::Result<Frame, WsSipError> {
    let raw = flatten_view(view);
    if raw.len() < 2 {
        return Err(WsSipError::FrameTooShort { actual: raw.len() });
    }

    let fin = (raw[0] & FLAG_FIN) != 0;
    let opcode = raw[0] & 0x0F;
    let masked = (raw[1] & FLAG_MASK) != 0;
    let mut payload_len = (raw[1] & 0x7F) as u64;
    let mut offset = 2usize;

    if payload_len == 126 {
        if raw.len() < offset + 2 {
            return Err(WsSipError::IncompleteHeader {
                expected: offset + 2,
                actual: raw.len(),
            });
        }
        payload_len = u16::from_be_bytes([raw[offset], raw[offset + 1]]) as u64;
        offset += 2;
    } else if payload_len == 127 {
        if raw.len() < offset + 8 {
            return Err(WsSipError::IncompleteHeader {
                expected: offset + 8,
                actual: raw.len(),
            });
        }
        payload_len = u64::from_be_bytes([
            raw[offset],
            raw[offset + 1],
            raw[offset + 2],
            raw[offset + 3],
            raw[offset + 4],
            raw[offset + 5],
            raw[offset + 6],
            raw[offset + 7],
        ]);
        offset += 8;
    }

    let mask_key = if masked {
        if raw.len() < offset + 4 {
            return Err(WsSipError::IncompleteHeader {
                expected: offset + 4,
                actual: raw.len(),
            });
        }
        let key = [
            raw[offset],
            raw[offset + 1],
            raw[offset + 2],
            raw[offset + 3],
        ];
        offset += 4;
        Some(key)
    } else {
        None
    };

    let expected_len =
        usize::try_from(payload_len).map_err(|_| WsSipError::PayloadLengthOverflow {
            declared: payload_len,
        })?;

    if raw.len() < offset + expected_len {
        return Err(WsSipError::PayloadLengthMismatch {
            expected: expected_len,
            actual: raw.len().saturating_sub(offset),
        });
    }

    let mut payload = raw[offset..offset + expected_len].to_vec();
    if let Some(key) = mask_key {
        for (index, byte) in payload.iter_mut().enumerate() {
            *byte ^= key[index % 4];
        }
    }

    if raw.len() != offset + expected_len {
        return Err(WsSipError::PayloadLengthMismatch {
            expected: expected_len,
            actual: raw.len().saturating_sub(offset),
        });
    }

    Ok(Frame {
        fin,
        opcode,
        payload,
    })
}

/// 将 `BufView` 展平成连续缓冲，便于后续解析。
fn flatten_view(view: &dyn BufView) -> Vec<u8> {
    let mut flat = Vec::with_capacity(view.len());
    for chunk in view.as_chunks() {
        flat.extend_from_slice(chunk);
    }
    flat
}

/// 写入 WebSocket 长度字段（含扩展长度）。
fn encode_length(len: usize, out: &mut Vec<u8>) {
    if len <= 125 {
        out.push(len as u8);
    } else if len <= u16::MAX as usize {
        out.push(126);
        out.extend_from_slice(&(len as u16).to_be_bytes());
    } else {
        out.push(127);
        out.extend_from_slice(&(len as u64).to_be_bytes());
    }
}

struct Frame {
    fin: bool,
    opcode: u8,
    payload: Vec<u8>,
}

const FLAG_FIN: u8 = 0x80;
const FLAG_MASK: u8 = 0x80;
const OPCODE_CONTINUATION: u8 = 0x0;
const OPCODE_TEXT: u8 = 0x1;
const OPCODE_BINARY: u8 = 0x2;
const OPCODE_CLOSE: u8 = 0x8;
const OPCODE_PING: u8 = 0x9;
const OPCODE_PONG: u8 = 0xA;
