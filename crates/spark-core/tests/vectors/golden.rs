//! 协议无关向量化用例黄金对照测试。
//!
//! # 设计目的（Why）
//! - 通过 JSON 测试向量描述 Codec 与 Service 的预期行为，帮助第三方协议适配在不依赖 Rust 源码的情况下复现核心语义；
//! - 将运行结果固化为黄金文件式字符串，作为回归基线，一旦行为漂移即可在测试中察觉；
//! - 验证请求成功、非重试错误、带重试成功三类典型时序，覆盖请求/响应/错误/重试路径。
//!
//! # 执行策略（How）
//! 1. 从 `tests/vectors/codec_service_roundtrip.json` 读取结构化向量；
//! 2. 使用测试专用的 `VectorEchoCodec` 模拟编解码；
//! 3. 使用顺序执行的 `SimpleServiceFn` 驱动 `VectorEchoServiceState`，按照向量中声明的时序触发业务逻辑；
//! 4. 将实际行为与期望对比，记录到人类可读的报表字符串；
//! 5. 断言报表与黄金字符串 `EXPECTED_REPORT` 一致，以检测未来变更对协议契约的影响。
//!
//! # 契约说明（What）
//! - 测试限定在 `std` 环境执行；
//! - JSON 中的十六进制字符串使用小写 ASCII 表达；
//! - 黄金报表必须保持排序与缩进一致，否则会触发断言失败。

use core::{
    pin::Pin,
    task::{Context as TaskContext, Poll, RawWaker, RawWakerVTable, Waker},
    time::Duration,
};
use serde::Deserialize;
use spark_core::{
    BufferPool, CoreError, Service,
    buffer::{ErasedSparkBuf, PoolStats, ReadableBuffer, WritableBuffer},
    codec::{
        Codec, CodecDescriptor, ContentEncoding, ContentType, DecodeContext, DecodeOutcome,
        EncodeContext, EncodedPayload,
    },
    contract::CallContext,
    error::{ErrorCategory, codes},
    service::SimpleServiceFn,
    status::{ReadyCheck, ReadyState, RetryAdvice},
};
use std::fmt::Write as _;

/// JSON 向量文件的顶层结构。
///
/// ## 意图（Why）
/// - 将多场景测试数据聚合在一个文档中，简化仓库同步与第三方复现成本；
/// - 约束字段命名与含义，避免读取逻辑中出现魔法字符串。
///
/// ## 解析逻辑（How）
/// - `codec` 描述编解码元信息；
/// - `cases` 为独立的调用脚本，保持顺序执行；
/// - 所有字符串使用 UTF-8，与 `VectorEchoCodec` 的实现一致。
///
/// ## 契约约束（What）
/// - `suite` 用于黄金报表头部标识；
/// - `cases` 至少包含一个场景；
/// - 当 `decode_frames` 为空时表示无需额外解码验证。
#[derive(Debug, Deserialize)]
struct VectorSuite {
    suite: String,
    codec: CodecSection,
    cases: Vec<CaseSection>,
}

/// 编解码描述符片段。
///
/// ## Why
/// 明确告知外部协议适配层当前示例所采用的内容类型与编码算法。
///
/// ## How
/// 单纯承载字符串，不参与运行时逻辑。
///
/// ## What
/// 所有字段均为必填，必须与 `VectorEchoCodec` 提供的描述符保持一致。
#[derive(Debug, Deserialize)]
struct CodecSection {
    descriptor_name: String,
    content_type: String,
    content_encoding: String,
}

/// 单个测试场景描述。
///
/// ## Why
/// 表达一次请求在不同阶段的期望观察结果，包括编码、服务执行与重试结果。
///
/// ## How
/// - `request` 定义输入消息与编码期望；
/// - `service_attempts` 依序描述每次 `call` 的结果；
/// - `decode_frames` 额外校验从字节还原业务对象的行为。
///
/// ## What
/// 至少包含一次服务尝试，且请求消息为 UTF-8 字符串。
#[derive(Debug, Deserialize)]
struct CaseSection {
    label: String,
    request: RequestSection,
    service_attempts: Vec<ServiceAttemptSection>,
    #[serde(default)]
    decode_frames: Vec<DecodeFrameSection>,
}

/// 请求片段。
///
/// ## Why
/// 固化请求侧编码与解码的契约。
///
/// ## How
/// - `expected_encoded_hex` 与实际编码结果比对；
/// - `expected_decoded_roundtrip` 验证解码能还原原始字符串。
///
/// ## What
/// `message` 必须为 UTF-8；若未提供 `expected_decoded_roundtrip`，测试将跳过解码校验。
#[derive(Debug, Deserialize)]
struct RequestSection {
    message: String,
    expected_encoded_hex: String,
    #[serde(default)]
    expected_decoded_roundtrip: Option<String>,
}

/// 服务调用尝试描述。
///
/// ## Why
/// 定义成功与失败分支的期望，便于验证重试逻辑与错误分类。
///
/// ## How
/// 使用 `serde` 的枚举内部标签模式，基于 `outcome` 字段区分成功/失败。
///
/// ## What
/// - 成功时必须给出响应字符串；
/// - 若附带 `encoded_hex`，将额外比较响应编码结果；
/// - 失败时需要提供稳定错误码、描述及分类。
#[derive(Debug, Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case")]
enum ServiceAttemptSection {
    Ok {
        response: String,
        #[serde(default)]
        encoded_hex: Option<String>,
    },
    Err {
        error_code: String,
        error_message: String,
        category: ServiceErrorCategory,
    },
}

/// 错误分类片段。
///
/// ## Why
/// 区分重试策略，确保 `CoreError` 分类符合设计文档。
///
/// ## How
/// 通过枚举映射到 [`ErrorCategory`]。
///
/// ## What
/// 目前覆盖可重试与不可重试两种语义。
#[derive(Debug, Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum ServiceErrorCategory {
    Retryable,
    NonRetryable,
}

/// 解码帧描述。
///
/// ## Why
/// 验证入站字节序列在不同方向上都能正确还原业务对象。
///
/// ## How
/// 按序对每个帧执行一次解码，比较预期字符串。
///
/// ## What
/// `hex` 字段使用小写十六进制；`expected_message` 必须与解码结果完全一致。
#[derive(Debug, Deserialize)]
struct DecodeFrameSection {
    direction: FrameDirection,
    hex: String,
    expected_message: String,
}

/// 帧方向，仅用于报表展示。
#[derive(Debug, Deserialize, Clone, Copy)]
#[serde(rename_all = "snake_case")]
enum FrameDirection {
    Inbound,
    Outbound,
}

/// 测试使用的简单请求类型。
#[derive(Debug, Clone, PartialEq, Eq)]
struct VectorRequest {
    body: String,
}

/// 测试使用的简单响应类型。
#[derive(Debug, Clone, PartialEq, Eq)]
struct VectorResponse {
    body: String,
}

/// Vector 协议的编解码器测试实现。
///
/// ## Why
/// - 提供稳定、无状态的 UTF-8 编解码器，方便围绕十六进制断言；
/// - 将 `CodecDescriptor` 固化为测试数据中的字段，帮助校验元信息是否漂移。
///
/// ## How
/// - `encode` 直接拷贝字符串字节并包装为只读缓冲；
/// - `decode` 读取全部剩余字节并尝试按 UTF-8 还原；
/// - 遇到非法 UTF-8 时返回 `protocol.decode` 错误，防止 silent failure。
///
/// ## What
/// 输入输出均为 `VectorMessage`，仅包含业务字符串。
struct VectorEchoCodec {
    descriptor: CodecDescriptor,
}

impl VectorEchoCodec {
    /// 构造测试编解码器。
    fn new() -> Self {
        let descriptor = CodecDescriptor::new(
            ContentType::new("application/vnd.spark.vector-echo"),
            ContentEncoding::identity(),
        )
        .with_schema(
            spark_core::codec::SchemaDescriptor::with_name("vector.echo").with_version("1.0"),
        );
        Self { descriptor }
    }
}

impl Codec for VectorEchoCodec {
    type Incoming = VectorMessage;
    type Outgoing = VectorMessage;

    fn descriptor(&self) -> &CodecDescriptor {
        &self.descriptor
    }

    fn encode(
        &self,
        item: &Self::Outgoing,
        _ctx: &mut EncodeContext<'_>,
    ) -> spark_core::Result<EncodedPayload, CoreError> {
        let buffer = OwnedReadableBuffer::new(item.body.as_bytes().to_vec());
        Ok(EncodedPayload::from_buffer(Box::new(buffer)))
    }

    fn decode(
        &self,
        src: &mut ErasedSparkBuf,
        _ctx: &mut DecodeContext<'_>,
    ) -> spark_core::Result<DecodeOutcome<Self::Incoming>, CoreError> {
        let remaining = src.remaining();
        let mut buf = vec![0u8; remaining];
        src.copy_into_slice(&mut buf)?;
        let text = String::from_utf8(buf).map_err(|_| {
            CoreError::new(codes::PROTOCOL_DECODE, "vector payload must be valid utf-8")
        })?;
        Ok(DecodeOutcome::Complete(VectorMessage { body: text }))
    }
}

/// 业务消息封装。
#[derive(Debug, Clone, PartialEq, Eq)]
struct VectorMessage {
    body: String,
}

impl From<VectorRequest> for VectorMessage {
    fn from(req: VectorRequest) -> Self {
        Self { body: req.body }
    }
}

impl From<VectorResponse> for VectorMessage {
    fn from(resp: VectorResponse) -> Self {
        Self { body: resp.body }
    }
}

/// 服务状态机：根据请求字符串返回响应或错误。
///
/// ## Why
/// - 模拟真实服务对不同命令的处理，以验证重试与错误分类行为；
/// - 状态仅包含“retry”场景的剩余失败次数，保持可预测性。
///
/// ## How
/// - `retry_budget` 初值为 1：第一次收到 `"retry"` 返回可重试错误，第二次返回成功；
/// - 其他命令按硬编码规则返回结果。
///
/// ## What
/// 所有输出均为 `CoreError` 或 `VectorResponse`，不会触发异步等待。
#[derive(Debug)]
struct VectorEchoServiceState {
    retry_budget: u8,
}

impl VectorEchoServiceState {
    fn handle(&mut self, req: VectorRequest) -> spark_core::Result<VectorResponse, CoreError> {
        match req.body.as_str() {
            "ping" => Ok(VectorResponse {
                body: "pong".to_string(),
            }),
            "fail-auth" => Err(CoreError::new("app.unauthorized", "auth token rejected")
                .with_category(ErrorCategory::NonRetryable)),
            "retry" => {
                if self.retry_budget > 0 {
                    self.retry_budget -= 1;
                    Err(CoreError::new("app.unavailable", "temporal failure")
                        .with_category(retryable_category()))
                } else {
                    Ok(VectorResponse {
                        body: "recovered".to_string(),
                    })
                }
            }
            other => Ok(VectorResponse {
                body: format!("echo:{other}"),
            }),
        }
    }
}

impl Default for VectorEchoServiceState {
    fn default() -> Self {
        Self { retry_budget: 1 }
    }
}

/// 静态分配器：用于构造编解码上下文，防止在测试中意外分配缓冲。
///
/// ## Why
/// 本测试不依赖实际缓冲池，但 `EncodeContext`/`DecodeContext` 需要一个实现契约的分配器实例。
///
/// ## How
/// 实现 [`BufferPool`]，所有方法返回静态值或错误，当编解码器尝试实际分配时将立即失败，提醒开发者补充上下文。
///
/// ## What
/// - `acquire` 返回错误，防止遗漏的分配路径；
/// - `shrink_to_fit` 和 `statistics` 返回零值快照。
struct DeterministicAllocator;

impl BufferPool for DeterministicAllocator {
    fn acquire(
        &self,
        _min_capacity: usize,
    ) -> spark_core::Result<Box<dyn WritableBuffer>, CoreError> {
        Err(CoreError::new(
            "test.alloc_disabled",
            "allocator disabled for golden vector test",
        ))
    }

    fn shrink_to_fit(&self) -> spark_core::Result<usize, CoreError> {
        Ok(0)
    }

    fn statistics(&self) -> spark_core::Result<PoolStats, CoreError> {
        Ok(PoolStats::default())
    }
}

/// 只读缓冲实现：封装 `Vec<u8>` 以满足 `ReadableBuffer` 契约。
///
/// ## Why
/// 测试场景需要在不依赖生产缓冲实现的情况下完成编码/解码循环。
///
/// ## How
/// 维护一个字节向量与游标，按照 `ReadableBuffer` 要求实现拆分、前进与复制操作。
///
/// ## What
/// 所有方法均保证在越界时返回 `protocol.decode` 错误。
#[derive(Debug, Clone)]
struct OwnedReadableBuffer {
    data: Vec<u8>,
    cursor: usize,
}

impl OwnedReadableBuffer {
    fn new(data: Vec<u8>) -> Self {
        Self { data, cursor: 0 }
    }
}

impl ReadableBuffer for OwnedReadableBuffer {
    fn remaining(&self) -> usize {
        self.data.len().saturating_sub(self.cursor)
    }

    fn chunk(&self) -> &[u8] {
        &self.data[self.cursor..]
    }

    fn split_to(&mut self, len: usize) -> spark_core::Result<Box<dyn ReadableBuffer>, CoreError> {
        if len > self.remaining() {
            return Err(CoreError::new(
                codes::PROTOCOL_DECODE,
                "split length exceeds remaining bytes",
            ));
        }
        let end = self.cursor + len;
        let slice = self.data[self.cursor..end].to_vec();
        self.cursor = end;
        Ok(Box::new(OwnedReadableBuffer::new(slice)))
    }

    fn advance(&mut self, len: usize) -> spark_core::Result<(), CoreError> {
        if len > self.remaining() {
            return Err(CoreError::new(
                codes::PROTOCOL_DECODE,
                "advance exceeds remaining bytes",
            ));
        }
        self.cursor += len;
        Ok(())
    }

    fn copy_into_slice(&mut self, dst: &mut [u8]) -> spark_core::Result<(), CoreError> {
        if dst.len() > self.remaining() {
            return Err(CoreError::new(
                codes::PROTOCOL_DECODE,
                "copy exceeds remaining bytes",
            ));
        }
        let end = self.cursor + dst.len();
        dst.copy_from_slice(&self.data[self.cursor..end]);
        self.cursor = end;
        Ok(())
    }

    fn try_into_vec(self: Box<Self>) -> spark_core::Result<Vec<u8>, CoreError> {
        let remaining = self.data[self.cursor..].to_vec();
        Ok(remaining)
    }
}

/// 构造 noop waker，供 `poll_ready` 与 Future 驱动使用。
fn noop_waker() -> Waker {
    fn clone(_: *const ()) -> RawWaker {
        RawWaker::new(core::ptr::null(), &VTABLE)
    }
    fn wake(_: *const ()) {}
    fn wake_by_ref(_: *const ()) {}
    fn drop(_: *const ()) {}
    static VTABLE: RawWakerVTable = RawWakerVTable::new(clone, wake, wake_by_ref, drop);
    let raw = RawWaker::new(core::ptr::null(), &VTABLE);
    unsafe { Waker::from_raw(raw) }
}

/// 将字节数组转换为小写十六进制字符串。
fn to_hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(&mut out, "{:02x}", byte).expect("write to string should succeed");
    }
    out
}

/// 将小写十六进制解析为字节。
fn from_hex(hex: &str) -> spark_core::Result<Vec<u8>, String> {
    if hex.len() % 2 != 0 {
        return Err(format!("hex string length must be even, got {}", hex.len()));
    }
    let mut bytes = Vec::with_capacity(hex.len() / 2);
    let chars: Vec<char> = hex.chars().collect();
    for chunk in chars.chunks(2) {
        let hi = chunk[0]
            .to_digit(16)
            .ok_or_else(|| format!("invalid hex character '{}' in {}", chunk[0], hex))?;
        let lo = chunk[1]
            .to_digit(16)
            .ok_or_else(|| format!("invalid hex character '{}' in {}", chunk[1], hex))?;
        bytes.push(((hi << 4) | lo) as u8);
    }
    Ok(bytes)
}

/// 将 `ServiceErrorCategory` 转换为框架内枚举，便于比较。
fn to_error_category(category: ServiceErrorCategory) -> ErrorCategory {
    match category {
        ServiceErrorCategory::Retryable => retryable_category(),
        ServiceErrorCategory::NonRetryable => ErrorCategory::NonRetryable,
    }
}

/// 构造测试使用的重试分类，保持向量与实际实现一致。
fn retryable_category() -> ErrorCategory {
    ErrorCategory::Retryable(RetryAdvice::after(Duration::from_millis(50)))
}

/// 将错误分类转为人类可读字符串。
fn display_category(category: &ErrorCategory) -> String {
    match category {
        ErrorCategory::Retryable(_) => "Retryable".to_string(),
        ErrorCategory::NonRetryable => "NonRetryable".to_string(),
        other => format!("{:?}", other),
    }
}

/// 黄金报表内容，用于断言输出稳定性。
const EXPECTED_REPORT: &str = r#"suite: codec-service-roundtrip
codec.descriptor: vector.echo.v1 | application/vnd.spark.vector-echo | identity
- case: happy_path
  request.message: "ping"
    encode.hex: actual=70696e67 expected=70696e67 -> OK
    decode.roundtrip: actual=ping expected=ping -> OK
  service.attempt[1]: poll_ready -> Ready | call -> Ok(response="pong") expected_body="pong" body_check=OK -> encode=706f6e67 expected=Some(706f6e67) -> OK
  decode.frame[1]: direction=inbound hex=706f6e67 -> message="pong" expected="pong" -> OK
- case: non_retryable_error
  request.message: "fail-auth"
    encode.hex: actual=6661696c2d61757468 expected=6661696c2d61757468 -> OK
    decode.roundtrip: actual=fail-auth expected=fail-auth -> OK
  service.attempt[1]: poll_ready -> Ready | call -> Err(code=app.unauthorized, category=NonRetryable) expected=(code=app.unauthorized, category=NonRetryable) -> OK
- case: retry_then_success
  request.message: "retry"
    encode.hex: actual=7265747279 expected=7265747279 -> OK
    decode.roundtrip: actual=retry expected=retry -> OK
  service.attempt[1]: poll_ready -> Ready | call -> Err(code=app.unavailable, category=Retryable) expected=(code=app.unavailable, category=Retryable) -> OK
  service.attempt[2]: poll_ready -> Ready | call -> Ok(response="recovered") expected_body="recovered" body_check=OK -> encode=7265636f7665726564 expected=Some(7265636f7665726564) -> OK
  decode.frame[1]: direction=inbound hex=7265636f7665726564 -> message="recovered" expected="recovered" -> OK
"#;

/// 读取 JSON 向量并生成黄金报表，对外部包装测试复用。
pub fn golden_vectors_match() {
    let raw = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../tests/vectors/codec_service_roundtrip.json"
    ));
    let suite: VectorSuite = serde_json::from_str(raw).expect("向量文件必须可解析");
    let codec = VectorEchoCodec::new();
    let allocator = DeterministicAllocator;

    let mut report = String::new();
    writeln!(&mut report, "suite: {}", suite.suite).unwrap();
    writeln!(
        &mut report,
        "codec.descriptor: {} | {} | {}",
        suite.codec.descriptor_name, suite.codec.content_type, suite.codec.content_encoding
    )
    .unwrap();

    for case in &suite.cases {
        writeln!(&mut report, "- case: {}", case.label).unwrap();
        writeln!(
            &mut report,
            "  request.message: \"{}\"",
            case.request.message
        )
        .unwrap();

        let request_message = VectorMessage {
            body: case.request.message.clone(),
        };
        let mut encode_ctx = EncodeContext::new(&allocator);
        let encoded = codec
            .encode(&request_message, &mut encode_ctx)
            .expect("编码应成功");
        let encoded_vec = encoded
            .into_buffer()
            .try_into_vec()
            .expect("缓冲应可转 Vec");
        let encoded_hex = to_hex(&encoded_vec);
        let encode_status = if encoded_hex == case.request.expected_encoded_hex {
            "OK"
        } else {
            "MISMATCH"
        };
        writeln!(
            &mut report,
            "    encode.hex: actual={} expected={} -> {}",
            encoded_hex, case.request.expected_encoded_hex, encode_status
        )
        .unwrap();

        if let Some(expected_text) = &case.request.expected_decoded_roundtrip {
            let bytes = from_hex(&case.request.expected_encoded_hex).expect("期望十六进制应合法");
            let mut buffer: Box<ErasedSparkBuf> = Box::new(OwnedReadableBuffer::new(bytes));
            let mut decode_ctx = DecodeContext::new(&allocator);
            let decoded = codec
                .decode(&mut *buffer, &mut decode_ctx)
                .expect("解码应成功");
            let actual = match decoded {
                DecodeOutcome::Complete(msg) => msg.body,
                other => format!("unexpected outcome: {:?}", other),
            };
            let status = if &actual == expected_text {
                "OK"
            } else {
                "MISMATCH"
            };
            writeln!(
                &mut report,
                "    decode.roundtrip: actual={} expected={} -> {}",
                actual, expected_text, status
            )
            .unwrap();
        }

        let mut service_state = VectorEchoServiceState::default();
        let mut service = SimpleServiceFn::new(move |_ctx: CallContext, req: VectorRequest| {
            let outcome = service_state.handle(req);
            core::future::ready(outcome)
        });

        for (index, attempt) in case.service_attempts.iter().enumerate() {
            let attempt_index = index + 1;
            let call_ctx = CallContext::builder().build();
            let exec_ctx = call_ctx.execution();
            let waker = noop_waker();
            let mut ready_cx = TaskContext::from_waker(&waker);
            let ready = Service::poll_ready(&mut service, &exec_ctx, &mut ready_cx);
            let ready_status = match ready {
                Poll::Ready(ReadyCheck::Ready(ReadyState::Ready)) => "Ready".to_string(),
                Poll::Ready(ReadyCheck::Ready(other)) => {
                    format!("Unexpected({:?})", other)
                }
                Poll::Ready(ReadyCheck::Err(err)) => {
                    panic!("poll_ready 返回错误: {:?}", err)
                }
                Poll::Pending => "Pending".to_string(),
                Poll::Ready(_) => "Ready(unknown)".to_string(),
            };

            let request = VectorRequest {
                body: case.request.message.clone(),
            };
            let mut future = Service::call(&mut service, call_ctx.clone(), request);
            let mut future = Pin::new(&mut future);
            let mut future_cx = TaskContext::from_waker(&waker);
            let result = match future.as_mut().poll(&mut future_cx) {
                Poll::Ready(res) => res,
                Poll::Pending => panic!("测试服务不应返回 Pending"),
            };

            match (attempt, result) {
                (
                    ServiceAttemptSection::Ok {
                        response,
                        encoded_hex: expected_hex,
                    },
                    Ok(actual_resp),
                ) => {
                    let response_status = if actual_resp.body == *response {
                        "OK"
                    } else {
                        "MISMATCH"
                    };
                    let mut encode_ctx = EncodeContext::new(&allocator);
                    let payload = codec
                        .encode(&VectorMessage::from(actual_resp.clone()), &mut encode_ctx)
                        .expect("响应编码不应失败");
                    let bytes = payload
                        .into_buffer()
                        .try_into_vec()
                        .expect("响应缓冲应可转 Vec");
                    let hex = to_hex(&bytes);
                    let status = match expected_hex {
                        Some(expected) if expected == &hex => "OK",
                        None => "OK",
                        _ => "MISMATCH",
                    };
                    let expected_hex_display = match expected_hex {
                        Some(expected) => format!("Some({})", expected),
                        None => "None".to_string(),
                    };
                    writeln!(
                        &mut report,
                        "  service.attempt[{}]: poll_ready -> {} | call -> Ok(response=\"{}\") expected_body=\"{}\" body_check={} -> encode={} expected={} -> {}",
                        attempt_index,
                        ready_status,
                        actual_resp.body,
                        response,
                        response_status,
                        hex,
                        expected_hex_display,
                        status
                    )
                    .unwrap();
                }
                (
                    ServiceAttemptSection::Err {
                        error_code,
                        error_message,
                        category,
                    },
                    Err(actual_err),
                ) => {
                    let actual_category = actual_err.category();
                    let expected_category = to_error_category(*category);
                    let status = if actual_err.code() == error_code
                        && actual_err.message() == error_message
                        && actual_category == expected_category
                    {
                        "OK"
                    } else {
                        "MISMATCH"
                    };
                    let actual_category_display = display_category(&actual_category);
                    let expected_category_display = display_category(&expected_category);
                    writeln!(
                        &mut report,
                        "  service.attempt[{}]: poll_ready -> {} | call -> Err(code={}, category={}) expected=(code={}, category={}) -> {}",
                        attempt_index,
                        ready_status,
                        actual_err.code(),
                        actual_category_display,
                        error_code,
                        expected_category_display,
                        status
                    )
                    .unwrap();
                }
                (expected, actual) => {
                    panic!(
                        "服务结果与向量类型不匹配: expected {:?}, actual {:?}",
                        expected, actual
                    );
                }
            }
        }

        for (index, frame) in case.decode_frames.iter().enumerate() {
            let bytes = from_hex(&frame.hex).expect("帧十六进制应合法");
            let mut buffer: Box<ErasedSparkBuf> = Box::new(OwnedReadableBuffer::new(bytes));
            let mut decode_ctx = DecodeContext::new(&allocator);
            let decoded = codec
                .decode(&mut *buffer, &mut decode_ctx)
                .expect("帧解码应成功");
            let actual = match decoded {
                DecodeOutcome::Complete(msg) => msg.body,
                other => format!("unexpected outcome: {:?}", other),
            };
            let status = if actual == frame.expected_message {
                "OK"
            } else {
                "MISMATCH"
            };
            writeln!(
                &mut report,
                "  decode.frame[{}]: direction={} hex={} -> message=\"{}\" expected=\"{}\" -> {}",
                index + 1,
                match frame.direction {
                    FrameDirection::Inbound => "inbound",
                    FrameDirection::Outbound => "outbound",
                },
                frame.hex,
                actual,
                frame.expected_message,
                status
            )
            .unwrap();
        }
    }

    assert_eq!(report, EXPECTED_REPORT);
}
