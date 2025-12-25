// SparkError 内含调用链与可观测性上下文，体积较大且为领域要求，因此允许相关 Clippy 告警。
#![allow(clippy::result_large_err)]

use alloc::{boxed::Box, format, string::String, sync::Arc, vec::Vec};
use core::task::{Context as TaskContext, Poll};

use spark_codec_sip::types::StartLine;
use spark_codec_sip::{
    Aor, AorRef, ContactUri, ContactUriRef, Header, HeaderName, Method, RequestLine, SipMessage,
    SipParseError, StatusLine, fmt::write_response, parse_request,
};
#[cfg(feature = "router-factory")]
use spark_core::service::{BoxService, auto_dyn::bridge_to_box_service};
use spark_core::{
    BoxFuture, CallContext, Context, CoreError, PipelineMessage, SparkError,
    buffer::{ErasedSparkBuf, ReadableBuffer},
    error::codes,
    service::Service,
    status::{PollReady, ReadyCheck, ReadyState},
};
#[cfg(feature = "router-factory")]
use spark_router::ServiceFactory;

use super::location::LocationStore;

const CODE_UNSUPPORTED_MESSAGE: &str = "app.registrar.unsupported_message";
const CODE_INTERNAL_FAILURE: &str = "app.registrar.internal_failure";

/// SIP Registrar 服务的最小实现。
///
/// # 设计摘要
/// - **职责定位（Why）**：将 REGISTER 报文转换为 Location Store 中的 AOR → Contact 映射，并同步返回 200 OK 响应。
/// - **架构位置（Where）**：运行于 `spark-switch` 的对象层 Service，通常通过 `spark_router` 注册到 `sip.registrar` 路由。
/// - **核心流程（How）**：
///   1. 解包 `PipelineMessage::Buffer`，将字节转换为 UTF-8 文本；
///   2. 借助 `spark-codec-sip` 解析请求，提取 `Aor` 与 `ContactUri`；
///   3. 调用 [`LocationStore::register`] 更新最新 Contact；
///   4. 基于请求头拼接 200 OK，并以新的 `PipelineMessage::Buffer` 返回。
#[derive(Debug)]
pub struct RegistrarService {
    store: Arc<LocationStore>,
}

impl RegistrarService {
    /// 创建使用共享 Location Store 的 Registrar 实例。
    #[must_use]
    pub fn new(store: Arc<LocationStore>) -> Self {
        Self { store }
    }

    fn handle_request(
        &self,
        message: PipelineMessage,
    ) -> spark_core::Result<PipelineMessage, SparkError> {
        // 统一从流水线消息中获取只读缓冲；Registrar 目前只消费原始字节，若未来扩展需在此处集中处理。
        let mut buffer = match message {
            PipelineMessage::Buffer(buf) => buf,
            _ => return Err(unsupported_message_error()),
        };

        // 教案式说明：优先零拷贝读取缓冲，避免在高并发 REGISTER 场景下为每个请求分配 Vec。
        // - 若底层缓冲为连续内存（chunk 覆盖 remaining），直接借用切片并按 UTF-8 解析；
        // - 若数据跨越多段（chunk < remaining），仅在必要时复制一次以获取平坦视图，保持性能与健壮性平衡。
        let remaining = buffer.remaining();
        let chunk = buffer.chunk();
        let owned_bytes;
        let text = if chunk.len() == remaining {
            core::str::from_utf8(chunk).map_err(|err| {
                SparkError::new(
                    codes::APP_INVALID_ARGUMENT,
                    format!("REGISTER 报文必须为 UTF-8：{err}"),
                )
            })?
        } else {
            owned_bytes = {
                let mut bytes = alloc::vec![0u8; remaining];
                buffer.copy_into_slice(&mut bytes).map_err(|err| {
                    SparkError::new(
                        codes::APP_INVALID_ARGUMENT,
                        format!("无法读取 REGISTER 请求字节：{err}"),
                    )
                    .with_cause(err)
                })?;
                bytes
            };
            core::str::from_utf8(&owned_bytes).map_err(|err| {
                SparkError::new(
                    codes::APP_INVALID_ARGUMENT,
                    format!("REGISTER 报文必须为 UTF-8：{err}"),
                )
            })?
        };

        let message = parse_request(text).map_err(map_parse_error)?;
        let request_line = extract_register_line(&message)?;
        let aor = extract_aor(&message, request_line);
        let contact = extract_contact(&message)?;

        self.store.register(aor, contact);

        build_success_response(&message)
    }
}

impl Service<PipelineMessage> for RegistrarService {
    type Response = PipelineMessage;
    type Error = SparkError;
    ///
    /// # 教案级意图说明（Why / What）
    /// - 采用 [`BoxFuture`] 而非 `future::Ready`：当前 `LocationStore` 为内存实现，可同步返回；但契约需预留
    ///   接入分布式存储（如 Redis、Consul）的演进空间，使用 `BoxFuture` 可在不变更上层类型签名的前提下接入
    ///   `async fn` 或远程调用。
    /// - 架构位置：该 Future 与数据平面 [`Service`] 契约对齐，保证对象层调用者在切换执行器时无需额外适配。
    /// - 前置/后置条件：
    ///   - **前置**：调用方需确保在 `poll_ready` 返回 `ReadyState::Ready` 后再调用 `call`；
    ///   - **后置**：Future 完成即意味着一次 REGISTER 请求处理完毕，返回的 `PipelineMessage` 可安全写回下游。
    type Future = BoxFuture<'static, spark_core::Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _: &Context<'_>, _: &mut TaskContext<'_>) -> PollReady<Self::Error> {
        Poll::Ready(ReadyCheck::Ready(ReadyState::Ready))
    }

    fn call(&mut self, _ctx: CallContext, req: PipelineMessage) -> Self::Future {
        // 教案式逻辑拆解：
        // 1. 将同步的 `handle_request` 结果暂存，避免在 async 块中持有 `&mut self`，确保 `BoxFuture<'static>` 满足
        //    生命周期约束；
        // 2. 使用 `Box::pin(async move { ... })` 统一返回异步接口，为未来接入分布式 Registrar 实现留出扩展点，
        //    但对当前纯内存路径零额外语义更改。
        let result = self.handle_request(req);
        Box::pin(async move { result })
    }
}

/// 为路由器提供 Registrar Service 的工厂。
#[cfg(feature = "router-factory")]
#[derive(Clone, Debug)]
pub struct RegistrarServiceFactory {
    store: Arc<LocationStore>,
}

#[cfg(feature = "router-factory")]
impl RegistrarServiceFactory {
    /// 使用共享状态创建工厂。
    #[must_use]
    pub fn new(store: Arc<LocationStore>) -> Self {
        Self { store }
    }
}

#[cfg(feature = "router-factory")]
impl ServiceFactory for RegistrarServiceFactory {
    fn create(&self) -> spark_core::Result<BoxService, SparkError> {
        let service = RegistrarService::new(Arc::clone(&self.store));
        Ok(bridge_to_box_service(service))
    }
}

fn extract_register_line<'a>(
    message: &'a SipMessage<'a>,
) -> spark_core::Result<&'a RequestLine<'a>, SparkError> {
    match &message.start_line {
        StartLine::Request(line) if line.method == Method::Register => Ok(line),
        StartLine::Request(line) => Err(SparkError::new(
            codes::APP_INVALID_ARGUMENT,
            format!("当前仅支持 REGISTER，收到 {}", line.method.as_str()),
        )),
        StartLine::Response(_) => Err(SparkError::new(
            codes::APP_INVALID_ARGUMENT,
            "Registrar 期望请求而非响应",
        )),
    }
}

fn extract_aor(message: &SipMessage<'_>, request_line: &RequestLine<'_>) -> Aor {
    message
        .headers
        .iter()
        .find_map(|header| match header {
            Header::To(to) => Some(AorRef::from(to).into_owned()),
            _ => None,
        })
        .unwrap_or_else(|| AorRef::from(&request_line.uri).into_owned())
}

fn extract_contact(message: &SipMessage<'_>) -> spark_core::Result<ContactUri, SparkError> {
    message
        .headers
        .iter()
        .find_map(|header| match header {
            Header::Contact(contact) if !contact.is_wildcard => contact
                .address
                .map(|addr| ContactUriRef::from(&addr).into_owned()),
            _ => None,
        })
        .ok_or_else(|| {
            SparkError::new(
                codes::APP_INVALID_ARGUMENT,
                "REGISTER 缺少可用的 Contact 头部",
            )
        })
}

fn build_success_response(
    message: &SipMessage<'_>,
) -> spark_core::Result<PipelineMessage, SparkError> {
    let mut headers: Vec<Header<'_>> = Vec::new();
    collect_headers(message, HeaderSelector::Via, &mut headers);
    collect_headers(message, HeaderSelector::From, &mut headers);
    collect_headers(message, HeaderSelector::To, &mut headers);
    collect_headers(message, HeaderSelector::CallId, &mut headers);
    collect_headers(message, HeaderSelector::CSeq, &mut headers);
    collect_headers(message, HeaderSelector::Contact, &mut headers);
    headers.push(Header::Extension {
        name: HeaderName::new("Content-Length"),
        value: "0",
    });

    let status_line = StatusLine {
        version: "SIP/2.0",
        status_code: 200,
        reason: "OK",
    };

    let mut text = String::new();
    write_response(&mut text, &status_line, &headers, &[]).map_err(|err| {
        SparkError::new(CODE_INTERNAL_FAILURE, format!("序列化 SIP 响应失败：{err}"))
    })?;
    Ok(PipelineMessage::from_buffer(Box::new(
        OwnedReadableBuffer::new(text.into_bytes()),
    )))
}

fn map_parse_error(err: SipParseError) -> SparkError {
    SparkError::new(
        codes::APP_INVALID_ARGUMENT,
        format!("无法解析 REGISTER 报文：{err}"),
    )
}

/// 构造“消息类型不受支持”的领域错误。
///
/// - **Why**：Registrar 当前仅支持对原始字节流执行解析，若收到 Pipeline 侧未来扩展的其它消息类型，应显式拒绝并提示调用方。
/// - **What**：返回使用固定错误码 [`CODE_UNSUPPORTED_MESSAGE`] 的 [`SparkError`]，调用前无额外前置条件。
/// - **How**：复用稳定文案，避免在多个匹配分支中重复创建字符串，后续若扩展支持类型只需调整此处。
fn unsupported_message_error() -> SparkError {
    SparkError::new(CODE_UNSUPPORTED_MESSAGE, "Registrar 仅支持字节缓冲消息")
}

#[derive(Clone, Copy)]
enum HeaderSelector {
    Via,
    From,
    To,
    CallId,
    CSeq,
    Contact,
}

fn collect_headers<'a>(
    message: &'a SipMessage<'a>,
    selector: HeaderSelector,
    output: &mut Vec<Header<'a>>,
) {
    output.extend(message.headers.iter().filter_map(|header| {
        matches!(
            (selector, header),
            (HeaderSelector::Via, Header::Via(_))
                | (HeaderSelector::From, Header::From(_))
                | (HeaderSelector::To, Header::To(_))
                | (HeaderSelector::CallId, Header::CallId(_))
                | (HeaderSelector::CSeq, Header::CSeq(_))
                | (HeaderSelector::Contact, Header::Contact(_))
        )
        .then_some(*header)
    }));
}

#[derive(Debug)]
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

    fn split_to(&mut self, len: usize) -> spark_core::Result<Box<ErasedSparkBuf>, CoreError> {
        if len > self.remaining() {
            return Err(CoreError::new(
                codes::PROTOCOL_DECODE,
                "split length exceeds remaining bytes",
            ));
        }
        let end = self.cursor + len;
        let slice = self.data[self.cursor..end].to_vec();
        self.cursor = end;
        Ok(Box::new(Self::new(slice)))
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
        Ok(self.data[self.cursor..].to_vec())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_register() -> String {
        "REGISTER sip:spark.invalid SIP/2.0\r\n\
Via: SIP/2.0/UDP client.invalid;branch=z9hG4bK-1;rport\r\n\
Max-Forwards: 70\r\n\
To: <sip:alice@spark.invalid>\r\n\
From: <sip:alice@spark.invalid>;tag=1928301774\r\n\
Contact: <sip:alice@client.invalid>\r\n\
Call-ID: a84b4c76e66710\r\n\
CSeq: 314159 REGISTER\r\n\
Content-Length: 0\r\n\r\n"
            .to_string()
    }

    #[test]
    fn registrar_updates_store_and_returns_response() {
        let store = Arc::new(LocationStore::new());
        let service = RegistrarService::new(Arc::clone(&store));
        let request = PipelineMessage::from_buffer(Box::new(OwnedReadableBuffer::new(
            sample_register().into_bytes(),
        )));
        let response = service.handle_request(request).expect("REGISTER 应成功");

        let raw = sample_register();
        let parsed = parse_request(&raw).expect("示例 REGISTER 应可解析");
        let aor = parsed
            .headers
            .iter()
            .find_map(|header| match header {
                Header::To(to) => Some(Aor::from_name_addr(to)),
                _ => None,
            })
            .expect("示例 REGISTER 应包含 To 头");
        let contact = store.lookup(&aor).expect("应写入 Contact");
        assert_eq!(contact.as_ref(), "sip:alice@client.invalid");

        match response {
            PipelineMessage::Buffer(buf) => {
                let text = String::from_utf8(buf.try_into_vec().unwrap()).unwrap();
                assert!(text.contains("200 OK"));
                assert!(text.contains("Contact: <sip:alice@client.invalid>"));
            }
            PipelineMessage::User(_) => panic!("Registrar 应返回字节缓冲"),
            _ => panic!("Registrar 应返回字节缓冲"),
        }
    }
}
