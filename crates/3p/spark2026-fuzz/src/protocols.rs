use alloc::{collections::BTreeMap, string::String, vec::Vec};
use core::str;
use std::sync::Arc;

use spark_codec_line::LineDelimitedCodec;
use spark_codec_rtp::parse_rtp;
use spark_codec_sdp::parse_sdp;
use spark_codec_sip::{parse_request, parse_response};
use spark_codecs::codec::{Codec, DecodeContext, DecodeOutcome, EncodeContext};

use crate::support::{FuzzBufferPool, FragmentedView, LinearReadable};

/// 支持的协议枚举。
///
/// - **Why**：将脚本首行的协议类型解析为强类型枚举，避免后续使用字符串匹配导致的笔误。 
/// - **What**：包含本次需求关注的四类协议：`LINE`/`RTP`/`SIP`/`SDP`。
/// - **How**：通过 `Protocol::from_tag` 将大小写无关的标签映射为枚举值。
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum Protocol {
    Line,
    Rtp,
    Sip,
    Sdp,
}

impl Protocol {
    fn from_tag(tag: &str) -> Option<Self> {
        match tag.trim().to_ascii_uppercase().as_str() {
            "LINE" => Some(Self::Line),
            "RTP" => Some(Self::Rtp),
            "SIP" => Some(Self::Sip),
            "SDP" => Some(Self::Sdp),
            _ => None,
        }
    }
}

/// 执行单条协议脚本。
///
/// - **Why**：在 fuzz target、命令行测试之间共享解析逻辑，使得极限样本既能作为回归用例，也能被
///   LibFuzzer 继续变异扩展。
/// - **How**：
///   1. 将输入字节解析为 UTF-8 文本，过滤空行/注释后定位首个协议声明；
///   2. 解析键值对选项并根据协议类型分派到专用执行函数；
///   3. 每类执行函数内部复用 `spark-core` 的上下文/解码 API，对错误保持宽容，仅在 panic 时视为失败。
/// - **What**：输入为脚本文本的字节序列；输出无返回值但可能触发断言或 panic 以指出异常。
/// - **前置条件**：脚本首行需声明协议类型；剩余行使用 `key:value` 或文本正文描述极限样本。
/// - **后置条件**：若无 panic，则视为成功覆盖了预期的极限场景。
pub fn execute_protocol_script(data: &[u8]) {
    let text = match str::from_utf8(data) {
        Ok(text) => text,
        Err(_) => return,
    };

    let mut lines = text.lines();
    let mut header_line: Option<&str> = None;
    while let Some(line) = lines.next() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        header_line = Some(line);
        break;
    }

    let header = match header_line {
        Some(line) => line,
        None => return,
    };

    let (protocol, options) = match parse_header(header) {
        Some(spec) => spec,
        None => return,
    };

    let body: Vec<&str> = lines.collect();

    match protocol {
        Protocol::Line => run_line_case(&options, &body),
        Protocol::Rtp => run_rtp_case(&options, &body),
        Protocol::Sip => run_sip_case(&body),
        Protocol::Sdp => run_sdp_case(&body),
    }
}

/// 解析脚本头部，返回协议类型与键值对选项。
fn parse_header(line: &str) -> Option<(Protocol, BTreeMap<String, String>)> {
    let mut parts = line.split_whitespace();
    let protocol = Protocol::from_tag(parts.next()?)?;
    let mut options = BTreeMap::new();
    for token in parts {
        if token.starts_with('#') {
            break;
        }
        if let Some((key, value)) = token.split_once('=') {
            options.insert(key.trim().to_ascii_lowercase(), value.trim().to_string());
        }
    }
    Some((protocol, options))
}

/// LINE 协议脚本执行：覆盖编码预算、解码预算、半帧等极限路径。
fn run_line_case(options: &BTreeMap<String, String>, lines: &[&str]) {
    let encode_limit = options
        .get("encode")
        .and_then(|value| value.parse::<usize>().ok());
    let decode_limit = options
        .get("decode")
        .and_then(|value| value.parse::<usize>().ok());

    let pool = Arc::new(FuzzBufferPool::default());
    let mut encode_ctx = EncodeContext::with_max_frame_size(pool.as_ref(), encode_limit);
    let mut decode_ctx = DecodeContext::with_max_frame_size(pool.as_ref(), decode_limit);
    let codec = LineDelimitedCodec::new();

    let mut encoded_stream = Vec::new();
    let mut expected_frames = Vec::new();
    let mut saw_truncated = false;

    for action in parse_line_actions(lines) {
        match action {
            LineAction::Emit(text) => {
                match codec.encode(&text, &mut encode_ctx) {
                    Ok(payload) => {
                        if let Ok(mut bytes) = payload.into_buffer().try_into_vec() {
                            expected_frames.push(text);
                            encoded_stream.append(&mut bytes);
                        }
                    }
                    Err(_err) => {
                        // 预算拒绝被视为合法分支，直接跳过，保持 fuzz 环境稳定。
                    }
                }
            }
            LineAction::Truncate(text) => {
                if let Ok(payload) = codec.encode(&text, &mut encode_ctx) {
                    if let Ok(mut bytes) = payload.into_buffer().try_into_vec() {
                        if !bytes.is_empty() {
                            bytes.pop();
                            saw_truncated = true;
                        }
                        encoded_stream.append(&mut bytes);
                    }
                }
            }
        }
    }

    let mut decoded_frames = Vec::new();
    let mut decode_failed = false;
    let mut source_buf = LinearReadable::from(encoded_stream).into_box();
    let source = source_buf.as_mut();
    loop {
        let before = source.remaining();
        if before == 0 {
            break;
        }
        match codec.decode(source, &mut decode_ctx) {
            Ok(DecodeOutcome::Complete(frame)) => {
                decoded_frames.push(frame);
            }
            Ok(DecodeOutcome::Incomplete) => {
                if source.remaining() == before {
                    break;
                }
            }
            Ok(_) => {}
            Err(_err) => {
                decode_failed = true;
                break;
            }
        }
        if source.remaining() == before {
            break;
        }
    }

    if !decode_failed && !saw_truncated {
        assert_eq!(decoded_frames, expected_frames);
    } else {
        assert!(
            decoded_frames.len() <= expected_frames.len(),
            "LINE 解码结果数量异常：{decoded_frames:?} 超出期望 {expected_frames:?}",
        );
    }
}

/// RTP 脚本执行：解析带分片、扩展、填充的极限报文。
fn run_rtp_case(options: &BTreeMap<String, String>, lines: &[&str]) {
    let bytes = collect_hex_payload(lines);
    if bytes.is_empty() {
        return;
    }
    let splits = options
        .get("split")
        .map(|raw| parse_usize_list(raw))
        .unwrap_or_default();
    let view = FragmentedView::from_splits(bytes.clone(), &splits);
    let Ok(packet) = parse_rtp(&view) else {
        return;
    };

    let header = packet.header();
    let _ = header.sequence_number;
    let _ = header.timestamp;
    let _ = header.payload_type;

    if let Some(extension) = packet.extension() {
        let mut consumed = 0usize;
        for chunk in extension.data.as_chunks() {
            consumed += chunk.len();
        }
        assert_eq!(consumed, extension.data.len());
    }

    let mut payload_len = 0usize;
    for chunk in packet.payload().as_chunks() {
        payload_len += chunk.len();
    }
    let padding = packet.padding_len() as usize;
    assert!(payload_len + padding <= view.len());
}

/// SIP 脚本执行：分别尝试按请求与响应解析，并读取关键头部。
fn run_sip_case(lines: &[&str]) {
    let mut text = decode_escaped(&join_lines(lines));
    if text.trim().is_empty() {
        return;
    }

    if !text.contains('\r') {
        text = text.replace('\n', "\r\n");
    }

    if let Ok(message) = parse_request(&text) {
        // 遍历头部以覆盖线性化逻辑。
        let mut via_count = 0usize;
        for header in &message.headers {
            let rendered = format!("{header:?}");
            if rendered.to_ascii_lowercase().contains("via") {
                via_count += 1;
            }
        }
        assert!(via_count >= 1, "SIP 请求样本应至少包含一个 Via 头");
        return;
    }

    if let Ok(message) = parse_response(&text) {
        assert!(message.headers.len() >= 2, "SIP 响应应包含若干头部");
    }
}

/// SDP 脚本执行：解析后遍历媒体与属性，验证必填字段。
fn run_sdp_case(lines: &[&str]) {
    let text = decode_escaped(&join_lines(lines));
    if text.trim().is_empty() {
        return;
    }

    if let Ok(desc) = parse_sdp(&text) {
        assert!(!desc.media.is_empty(), "SDP 样本需至少包含一个媒体描述");
        for media in &desc.media {
            assert!(
                !media.formats.is_empty(),
                "媒体描述必须提供至少一个编码格式",
            );
        }
    }
}

/// 解析 LINE 行为描述。
fn parse_line_actions(lines: &[&str]) -> Vec<LineAction> {
    let mut actions = Vec::new();
    for raw in lines {
        let trimmed = raw.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        if let Some(payload) = trimmed.strip_prefix("frame:") {
            actions.push(LineAction::Emit(decode_escaped(payload.trim())));
        } else if let Some(payload) = trimmed.strip_prefix("truncate:") {
            actions.push(LineAction::Truncate(decode_escaped(payload.trim())));
        }
    }
    actions
}

/// 描述 LINE 行为的枚举。
enum LineAction {
    Emit(String),
    Truncate(String),
}

/// 将脚本正文按换行重新拼接，保留空行。
fn join_lines(lines: &[&str]) -> String {
    let mut out = String::new();
    for (index, line) in lines.iter().enumerate() {
        if index > 0 {
            out.push('\n');
        }
        out.push_str(line);
    }
    out
}

/// 解析十六进制描述的 RTP 报文。
fn collect_hex_payload(lines: &[&str]) -> Vec<u8> {
    let mut bytes = Vec::new();
    for raw in lines {
        let trimmed = raw.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        if let Some(data) = trimmed.strip_prefix("hex:") {
            bytes.extend(decode_hex(data));
        }
    }
    bytes
}

/// 将 `size1,size2,...` 形式的文本解析为 usize 列表。
fn parse_usize_list(text: &str) -> Vec<usize> {
    text.split(',')
        .filter_map(|segment| segment.trim().parse::<usize>().ok())
        .collect()
}

/// 文本转义：支持 `\n`/`\r`/`\t`/`\\` 以及 `\xNN`。
fn decode_escaped(input: &str) -> String {
    let mut output = String::new();
    let mut chars = input.chars();
    while let Some(ch) = chars.next() {
        if ch != '\\' {
            output.push(ch);
            continue;
        }
        match chars.next() {
            Some('n') => output.push('\n'),
            Some('r') => output.push('\r'),
            Some('t') => output.push('\t'),
            Some('\\') => output.push('\\'),
            Some('x') => {
                let hi = chars.next();
                let lo = chars.next();
                if let (Some(hi), Some(lo)) = (hi, lo) {
                    if let (Some(hi), Some(lo)) = (hi.to_digit(16), lo.to_digit(16)) {
                        let byte = ((hi << 4) | lo) as u8;
                        output.push(byte as char);
                        continue;
                    }
                }
            }
            Some(other) => output.push(other),
            None => {}
        }
    }
    output
}

/// 将十六进制字符串解析为字节数组，忽略空白与无效片段。
fn decode_hex(text: &str) -> Vec<u8> {
    let mut cleaned = String::new();
    for ch in text.chars() {
        if ch.is_ascii_hexdigit() {
            cleaned.push(ch);
        }
    }
    let mut bytes = Vec::new();
    let mut chars = cleaned.chars();
    while let (Some(hi), Some(lo)) = (chars.next(), chars.next()) {
        if let (Some(hi), Some(lo)) = (hi.to_digit(16), lo.to_digit(16)) {
            bytes.push(((hi << 4) | lo) as u8);
        }
    }
    bytes
}
