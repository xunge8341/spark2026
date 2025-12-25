#![no_main]

use arbitrary::Arbitrary;
use core::mem::MaybeUninit;
use libfuzzer_sys::fuzz_target;
use spark2026_fuzz::support::{FuzzBufferPool, LinearReadable};
use spark_codec_line::LineDelimitedCodec;
use spark_core::codec::{Codec, DecodeContext, DecodeOutcome, EncodeContext};
use std::sync::Arc;

/// Fuzz 用例：描述需要编码的帧序列以及可选的长度预算。
///
/// - **Why**：将结构化输入显式建模，便于差分比较“成功编码的帧”与“解码得到的帧”是否一致。
/// - **How**：每个 `FrameSpec` 控制帧内容与是否模拟“缺失换行”的异常场景；全局 `encode_limit`/`decode_limit`
///   则复用 `EncodeContext`/`DecodeContext` 的帧尺寸约束。
/// - **What**：Fuzzer 会自动生成任意组合，帮助覆盖多种预算、分片与异常输入。
#[derive(Debug, Arbitrary)]
struct CodecFuzzCase {
    frames: Vec<FrameSpec>,
    encode_limit: Option<u16>,
    decode_limit: Option<u16>,
}

/// 单帧规格，控制文本内容与是否构造“缺少结尾换行”以触发半帧场景。
#[derive(Debug, Arbitrary)]
struct FrameSpec {
    text: String,
    force_truncate: bool,
}

/// 归一化帧内容，移除编码阶段不允许出现的换行控制符。
///
/// - **Why**：`LineDelimitedCodec` 将首个 `\n` 视为帧分隔符，若原文中包含换行会被提前截断。
///   归一化后才可与解码结果进行差分比较。
/// - **How**：过滤 `\n` 与 `\r`，其余字符原样保留，确保 fuzz 仍能探索多样的 Unicode 文本。
/// - **What**：返回可安全用于编码的业务字符串。
fn sanitize_frame_text(input: &str) -> String {
    input
        .chars()
        .filter(|ch| *ch != '\n' && *ch != '\r')
        .collect()
}

fuzz_target!(|case: CodecFuzzCase| {
    if case.frames.is_empty() {
        return;
    }

    // === Why === 保证所有编码与解码上下文共享同一缓冲池，模拟真实运行时“租借-冻结-释放”的流程。
    let pool = Arc::new(FuzzBufferPool::default());
    let mut encode_ctx = EncodeContext::with_max_frame_size(
        pool.as_ref(),
        case.encode_limit.map(|limit| limit as usize),
    );
    let mut decode_ctx = DecodeContext::with_max_frame_size(
        pool.as_ref(),
        case.decode_limit.map(|limit| limit as usize),
    );
    let codec = LineDelimitedCodec::new();

    let mut encoded_stream = Vec::new();
    let mut expected_frames = Vec::new();
    let mut saw_truncated = false;

    for frame in &case.frames {
        let normalized = sanitize_frame_text(&frame.text);
        match codec.encode(&normalized, &mut encode_ctx) {
            Ok(payload) => {
                let mut bytes = match payload.into_buffer().try_into_vec() {
                    Ok(bytes) => bytes,
                    Err(_) => continue,
                };
                if frame.force_truncate && !bytes.is_empty() {
                    bytes.pop();
                    saw_truncated = true;
                } else {
                    expected_frames.push(normalized);
                }
                encoded_stream.extend_from_slice(&bytes);
            }
            Err(_) => {
                // 预算受限等异常视为“拒绝发送该帧”，不纳入期望列表。
            }
        }
    }

    let mut decoded_frames = Vec::new();
    let mut decode_failed = false;

    {
        let mut source_buf = LinearReadable::from(encoded_stream).into_box();
        let source = source_buf.as_mut();
        loop {
            let before = source.remaining();
            if before == 0 {
                break;
            }

            let outcome = codec.decode(source, &mut decode_ctx);
            match outcome {
                Ok(DecodeOutcome::Complete(text)) => {
                    decoded_frames.push(text);
                }
                Ok(DecodeOutcome::Incomplete) => {
                    if source.remaining() == before {
                        // 没有消费任何字节，说明当前帧仍缺数据，直接停止，避免死循环。
                        break;
                    }
                }
                Ok(_) => {
                    // 未来若新增解码状态，保持保守策略：仅当读取进度推进时继续循环。
                }
                Err(_) => {
                    decode_failed = true;
                    break;
                }
            }

            if source.remaining() == before {
                break;
            }
        }
    }

    // 差分断言：
    // - 当不存在解码失败或强制截断时，解码结果必须与期望帧序列完全一致；
    // - 若出现截断或解码错误，仅要求未产生额外帧，确保状态机未失控膨胀。
    if !decode_failed && !saw_truncated {
        assert_eq!(decoded_frames, expected_frames);
    } else {
        assert!(
            decoded_frames.len() <= expected_frames.len(),
            "解码结果数量异常：{decoded_frames:?} 超过期望帧 {expected_frames:?}",
        );
    }
});
