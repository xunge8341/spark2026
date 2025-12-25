# Spark 契约索引（P0-04）

> 本索引映射 **文档 → Rustdoc → 代码 → TCK**，用于回答“某个公开契约在哪里定义、如何示例、由哪些测试守护”。
> - 每个词条与《Spark 契约术语表》保持一致，便于在文档之间互跳；
> - “Rustdoc” 提供官方 API 文档入口；“示例” 指向仓库内可运行的用例或教学代码；
> - “TCK” 指向 `spark-contract-tests` 或 `spark-impl-tck` 中的契约测试，确保任何公开 trait/类型都能追溯到验证用例；
> - 页尾的 `toml` 数据块由 CI 守门脚本解析，统计公开 API 覆盖率并在缺失时阻断。

## Buffer（缓冲池与视图）
| 维度 | 链接 |
| --- | --- |
| Rustdoc | [`spark_core::buffer`](https://docs.rs/spark-core/latest/spark_core/buffer/index.html) · [`spark_buffer`](https://docs.rs/spark-buffer/latest/spark_buffer/) |
| 示例 | [`SlabBufferPool::acquire`](../crates/spark-buffer/src/pool.rs#L122-L179) |
| TCK | [`resource_exhaustion::thread_pool_starvation_emits_busy_then_retry_after`](../crates/spark-contract-tests/src/resource_exhaustion.rs#L292-L415) |

## Frame（帧与消息封装）
| 维度 | 链接 |
| --- | --- |
| Rustdoc | [`spark_core::protocol::Frame`](https://docs.rs/spark-core/latest/spark_core/protocol/struct.Frame.html) |
| 示例 | [`Frame::try_new` 单元测试](../crates/spark-core/src/data_plane/protocol/frame.rs) |
| TCK | [`spark-impl-tck::ws_sip::frame_text_binary`](../crates/spark-impl-tck/tests/ws_sip/frame_text_binary.rs#L8-L155) |

## Codec（编解码）
| 维度 | 链接 |
| --- | --- |
| Rustdoc | [`spark_core::codec`](https://docs.rs/spark-core/latest/spark_core/codec/index.html) · [`spark-codecs`](https://docs.rs/spark-codecs/latest/spark_codecs/) |
| 示例 | [`LineDelimitedCodec`](../crates/spark-codec-line/src/line.rs#L170-L210) |
| TCK | [`slowloris::slow_reader_exceeding_budget_is_rejected`](../crates/spark-contract-tests/src/slowloris.rs#L70-L115) |

## Transport（传输与握手）
| 维度 | 链接 |
| --- | --- |
| Rustdoc | [`spark_core::transport`](https://docs.rs/spark-core/latest/spark_core/transport/index.html) |
| 示例 | [`TransportParams`](../crates/spark-core/src/data_plane/transport/params.rs#L20-L136) |
| TCK | [`graceful_shutdown::draining_connections_respect_fin`](../crates/spark-contract-tests/src/graceful_shutdown.rs#L640-L796) |

## Pipeline（处理链）
| 维度 | 链接 |
| --- | --- |
| Rustdoc | [`spark_core::pipeline`](https://docs.rs/spark-core/latest/spark_core/pipeline/index.html) · [`spark_router::pipeline`](https://docs.rs/spark-router/latest/spark_router/pipeline/index.html) |
| 示例 | [`hot_swap` 集成测试](../crates/spark-core/tests/pipeline/hot_swap.rs#L40-L420) |
| TCK | [`hot_swap::pipeline_state_transitions_are_ordered`](../crates/spark-contract-tests/src/hot_swap.rs#L430-L585) |

## PipelineInitializer（管线初始化器）
| 维度 | 链接 |
| --- | --- |
| Rustdoc | [`spark_core::pipeline::initializer`](https://docs.rs/spark-core/latest/spark_core/pipeline/initializer/index.html) |
| 示例 | [`ChainBuilder::register_inbound`](../crates/spark-core/src/data_plane/pipeline/initializer.rs#L52-L118) |
| TCK | [`observability::middleware_emits_expected_attributes`](../crates/spark-contract-tests/src/observability.rs#L210-L332) |

## Service（服务接口）
| 维度 | 链接 |
| --- | --- |
| Rustdoc | [`spark_core::service`](https://docs.rs/spark-core/latest/spark_core/service/index.html) · [`spark-middleware`](https://docs.rs/spark-middleware/latest/spark_middleware/) |
| 示例 | [`ServiceLogic::call`](../crates/spark-core/src/data_plane/service/simple.rs#L120-L210) |
| TCK | [`graceful_shutdown::coordinator_waits_for_service_draining`](../crates/spark-contract-tests/src/graceful_shutdown.rs#L820-L1012) |

## Router（路由控制）
| 维度 | 链接 |
| --- | --- |
| Rustdoc | [`spark-router`](https://docs.rs/spark-router/latest/spark_router/) |
| 示例 | [`DefaultRouter::route_dyn`](../crates/spark-router/src/lib.rs#L303-L361) |
| TCK | [`route_dyn_returns_new_service`](../crates/spark-router/src/lib.rs#L538-L611) |

## Context（调用上下文）
| 维度 | 链接 |
| --- | --- |
| Rustdoc | [`spark_core::context`](https://docs.rs/spark-core/latest/spark_core/context/index.html) · [`spark_core::contract`](https://docs.rs/spark-core/latest/spark_core/contract/index.html) |
| 示例 | [`CallContext::with_deadline`](../crates/spark-core/src/kernel/contract.rs#L120-L198) |
| TCK | [`state_machine::child_context_inherits_cancellation`](../crates/spark-contract-tests/src/state_machine.rs#L60-L214) |

## Error（错误语义）
| 维度 | 链接 |
| --- | --- |
| Rustdoc | [`spark_core::error`](https://docs.rs/spark-core/latest/spark_core/error/index.html) |
| 示例 | [`CategoryMatrixEntry`](../crates/spark-core/src/error/generated/category_matrix.rs#L20-L210) |
| TCK | [`errors::domain_error_is_mapped_to_matrix`](../crates/spark-contract-tests/src/errors.rs#L250-L360) |

## State（状态与节流）
| 维度 | 链接 |
| --- | --- |
| Rustdoc | [`spark_core::model`](https://docs.rs/spark-core/latest/spark_core/model/index.html) · [`spark_core::status`](https://docs.rs/spark-core/latest/spark_core/status/index.html) |
| 示例 | [`RetryAdvice`](../crates/spark-core/src/kernel/status/retry.rs#L15-L140) |
| TCK | [`backpressure::channel_queue_exhaustion_emits_busy_then_retry_after`](../crates/spark-contract-tests/src/backpressure.rs#L60-L198) |

## Backpressure·Budget·Shutdown（统一信号骨架）
| 维度 | 链接 |
| --- | --- |
| Rustdoc | [`spark_core::contract::BackpressureSignal`](https://docs.rs/spark-core/latest/spark_core/contract/enum.BackpressureSignal.html) |
| 示例 | _占位：待 P0-15 案例补充_ |
| TCK | _占位：待 spark-contract-tests::backpressure-shell 覆盖_ |

<!-- contracts-index:start -->
```toml
[[contract]]
term = "Buffer"
rustdoc = [
  { name = "spark_core::buffer", url = "https://docs.rs/spark-core/latest/spark_core/buffer/index.html" },
  { name = "spark_buffer", url = "https://docs.rs/spark-buffer/latest/spark_buffer/" },
]
examples = [
  { name = "SlabBufferPool::acquire", path = "../crates/spark-buffer/src/pool.rs#L122-L179" },
]
tck = [
  { name = "resource_exhaustion::thread_pool_starvation_emits_busy_then_retry_after", path = "../crates/spark-contract-tests/src/resource_exhaustion.rs#L292-L415" },
]
covers = [
  "spark_core::buffer::BufView",
  "spark_core::buffer::BufferAllocator",
  "spark_core::buffer::pool::BufferAllocator",
  "spark_core::buffer::pool::BufferPoolAllocatorAdapter",
  "spark_core::buffer::BufferPool",
  "spark_core::buffer::Chunks",
  "spark_core::buffer::PoolStatDimension",
  "spark_core::buffer::PoolStats",
  "spark_core::buffer::ReadableBuffer",
  "spark_core::buffer::UserMessage",
  "spark_core::buffer::WritableBuffer",
  "spark_core::buffer::buf_view::BufView",
  "spark_core::buffer::buf_view::Chunks",
  "spark_core::buffer::message::UserMessage",
  "spark_core::buffer::pool::BufferPool",
  "spark_core::buffer::pool::PoolStatDimension",
  "spark_core::buffer::pool::PoolStats",
  "spark_core::buffer::readable::ReadableBuffer",
  "spark_core::buffer::writable::WritableBuffer",
]

[[contract]]
term = "Frame"
rustdoc = [
  { name = "spark_core::protocol::Frame", url = "https://docs.rs/spark-core/latest/spark_core/protocol/struct.Frame.html" },
]
examples = [
  { name = "Frame::try_new", path = "../crates/spark-core/src/data_plane/protocol/frame.rs" },
]
tck = [
  { name = "spark-impl-tck::ws_sip::frame_text_binary", path = "../crates/spark-impl-tck/tests/ws_sip/frame_text_binary.rs#L8-L155" },
]
covers = [
  "spark_core::protocol::Frame",
]

[[contract]]
term = "Codec"
rustdoc = [
  { name = "spark_core::codec", url = "https://docs.rs/spark-core/latest/spark_core/codec/index.html" },
  { name = "spark-codecs", url = "https://docs.rs/spark-codecs/latest/spark_codecs/" },
]
examples = [
  { name = "LineDelimitedCodec", path = "../crates/spark-codec-line/src/line.rs#L170-L210" },
]
tck = [
  { name = "slowloris::slow_reader_exceeding_budget_is_rejected", path = "../crates/spark-contract-tests/src/slowloris.rs#L70-L115" },
]
covers = [
  "spark_core::codec::Codec",
  "spark_core::codec::CodecDescriptor",
  "spark_core::codec::CodecMetricsHook",
  "spark_core::codec::CodecPhase",
  "spark_core::codec::CodecRegistry",
  "spark_core::codec::ContentEncoding(_)",
  "spark_core::codec::ContentType(_)",
  "spark_core::codec::DecodeContext",
  "spark_core::codec::DecodeFrameGuard",
  "spark_core::codec::Decoder",
  "spark_core::codec::DynCodec",
  "spark_core::codec::DynCodecFactory",
  "spark_core::codec::EncodeContext",
  "spark_core::codec::EncodeFrameGuard",
  "spark_core::codec::EncodedPayload",
  "spark_core::codec::Encoder",
  "spark_core::codec::NegotiatedCodec",
  "spark_core::codec::SchemaDescriptor",
  "spark_core::codec::TypedCodecAdapter",
  "spark_core::codec::TypedCodecFactory",
  "spark_core::codec::metrics::CodecMetricsHook",
  "spark_core::codec::metrics::CodecPhase",
]

[[contract]]
term = "Transport"
rustdoc = [
  { name = "spark_core::transport", url = "https://docs.rs/spark-core/latest/spark_core/transport/index.html" },
]
examples = [
  { name = "TransportParams", path = "../crates/spark-core/src/data_plane/transport/params.rs#L20-L136" },
]
tck = [
  { name = "graceful_shutdown::draining_connections_respect_fin", path = "../crates/spark-contract-tests/src/graceful_shutdown.rs#L640-L796" },
]
covers = [
  "spark_core::transport::Capability",
  "spark_core::transport::CapabilityBitmap",
  "spark_core::transport::ConnectionIntent",
  "spark_core::transport::DowngradeReport",
  "spark_core::transport::DynServerChannel",
  "spark_core::transport::DynTransportFactory",
  "spark_core::transport::Endpoint",
  "spark_core::transport::HandshakeError",
  "spark_core::transport::HandshakeErrorKind",
  "spark_core::transport::HandshakeOffer",
  "spark_core::transport::HandshakeOutcome",
  "spark_core::transport::LinkDirection",
  "spark_core::transport::ListenerConfig",
  "spark_core::transport::ListenerShutdown",
  "spark_core::transport::NegotiationAuditContext",
  "spark_core::transport::ServerChannel",
  "spark_core::transport::ServerChannelObject",
  "spark_core::transport::TransportBuilder",
  "spark_core::transport::Channel",
  "spark_core::transport::ServerChannel",
  "spark_core::transport::DatagramEndpoint",
  "spark_core::transport::TransportFactory",
  "spark_core::transport::TransportFactoryObject",
  "spark_core::transport::BackpressureDecision",
  "spark_core::transport::BackpressureMetrics",
  "spark_core::transport::ShutdownDirection",
  "spark_core::transport::TransportSocketAddr",
  "spark_core::transport::AddressFamily",
  "spark_core::transport::TransportMetricsHook",
  "spark_core::transport::TransportParams(_)",
  "spark_core::transport::Version",
  "spark_core::transport::builder::TransportBuilder",
  "spark_core::transport::endpoint::Endpoint",
  "spark_core::transport::endpoint::EndpointBuilder",
  "spark_core::transport::factory::ListenerConfig",
  "spark_core::transport::handshake::Capability",
  "spark_core::transport::handshake::CapabilityBitmap",
  "spark_core::transport::handshake::DowngradeReport",
  "spark_core::transport::handshake::HandshakeError",
  "spark_core::transport::handshake::HandshakeErrorKind",
  "spark_core::transport::handshake::HandshakeOffer",
  "spark_core::transport::handshake::HandshakeOutcome",
  "spark_core::transport::handshake::NegotiationAuditContext",
  "spark_core::transport::handshake::Version",
  "spark_core::transport::intent::ConnectionIntent",
  "spark_core::transport::metrics::LinkDirection",
  "spark_core::transport::metrics::TransportMetricsHook",
  "spark_core::transport::channel::BackpressureDecision",
  "spark_core::transport::channel::BackpressureMetrics",
  "spark_core::transport::channel::Channel",
  "spark_core::transport::datagram::DatagramEndpoint",
  "spark_core::transport::server_channel::ServerChannel",
  "spark_core::transport::address::AddressFamily",
  "spark_core::transport::address::TransportSocketAddr",
  "spark_core::transport::shutdown::ShutdownDirection",
  "spark_core::transport::params::TransportParams(_)",
  "spark_core::transport::server::ListenerShutdown",
  "spark_core::transport::DynServerChannel",
  "spark_core::transport::DynTransportFactory",
  "spark_core::transport::ServerChannel",
  "spark_core::transport::ServerChannelObject",
  "spark_core::transport::TransportFactory",
  "spark_core::transport::TransportFactoryObject",
  "spark_core::transport::factory::DynTransportFactory",
  "spark_core::transport::factory::TransportFactory",
  "spark_core::transport::factory::TransportFactoryObject",
  "spark_core::transport::server_channel::DynServerChannel",
  "spark_core::transport::server_channel::ServerChannelObject",
]

[[contract]]
term = "Pipeline"
rustdoc = [
  { name = "spark_core::pipeline", url = "https://docs.rs/spark-core/latest/spark_core/pipeline/index.html" },
  { name = "spark_router::pipeline", url = "https://docs.rs/spark-router/latest/spark_router/pipeline/index.html" },
]
examples = [
  { name = "pipeline hot_swap integration test", path = "../crates/spark-core/tests/pipeline/hot_swap.rs#L40-L420" },
]
tck = [
  { name = "hot_swap::pipeline_state_transitions_are_ordered", path = "../crates/spark-contract-tests/src/hot_swap.rs#L430-L585" },
]
covers = [
  "spark_core::pipeline::ChainBuilder",
  "spark_core::pipeline::Channel",
  "spark_core::pipeline::Context",
  "spark_core::pipeline::Controller",
  "spark_core::pipeline::ControllerEvent",
  "spark_core::pipeline::ControllerEventKind",
  "spark_core::pipeline::ControllerFactory",
  "spark_core::pipeline::ControllerHandle",
  "spark_core::pipeline::ControllerHandleId(_)",
  "spark_core::pipeline::Handler",
  "spark_core::pipeline::HandlerDirection",
  "spark_core::pipeline::HandlerRegistration",
  "spark_core::pipeline::Pipeline",
  "spark_core::pipeline::PipelineEvent",
  "spark_core::pipeline::PipelineFactory",
  "spark_core::pipeline::PipelineFactoryObject",
  "spark_core::pipeline::PipelineHandle",
  "spark_core::pipeline::PipelineHandleId(_)",
  "spark_core::pipeline::HotSwapPipeline",
  "spark_core::pipeline::DuplexHandler",
  "spark_core::pipeline::DynControllerFactory",
  "spark_core::pipeline::DynControllerFactoryAdapter",
  "spark_core::pipeline::DynPipelineFactory",
  "spark_core::pipeline::DynPipelineFactoryAdapter",
  "spark_core::pipeline::ExceptionAutoResponder",
  "spark_core::pipeline::ExtensionsMap",
  "spark_core::pipeline::HandlerRegistry",
  "spark_core::pipeline::InboundHandler",
  "spark_core::pipeline::PipelineInitializer",
  "spark_core::pipeline::InitializerDescriptor",
  "spark_core::pipeline::OutboundHandler",
  "spark_core::pipeline::Pipeline",
  "spark_core::pipeline::PipelineFactory",
  "spark_core::pipeline::ReadyStateEvent",
  "spark_core::pipeline::channel::Channel",
  "spark_core::pipeline::context::Context",
  "spark_core::pipeline::controller::Pipeline",
  "spark_core::pipeline::controller::PipelineEvent",
  "spark_core::pipeline::controller::PipelineHandleId(_)",
  "spark_core::pipeline::controller::Handler",
  "spark_core::pipeline::controller::HandlerRegistration",
  "spark_core::pipeline::controller::HandlerRegistry",
  "spark_core::pipeline::controller::HotSwapPipeline",
  "spark_core::pipeline::default_handlers::ExceptionAutoResponder",
  "spark_core::pipeline::default_handlers::ReadyStateEvent",
  "spark_core::pipeline::extensions::ExtensionsMap",
  "spark_core::pipeline::factory::PipelineFactory",
  "spark_core::pipeline::factory::PipelineFactoryObject",
  "spark_core::pipeline::factory::PipelineHandle",
  "spark_core::pipeline::factory::DynPipelineFactory",
  "spark_core::pipeline::factory::DynPipelineFactoryAdapter",
  "spark_core::pipeline::handler::DuplexHandler",
  "spark_core::pipeline::handler::InboundHandler",
  "spark_core::pipeline::handler::OutboundHandler",
  "spark_core::pipeline::pipeline::Handler",
  "spark_core::pipeline::pipeline::HandlerRegistration",
  "spark_core::pipeline::pipeline::HandlerRegistry",
  "spark_core::pipeline::pipeline::HotSwapPipeline",
  "spark_core::pipeline::pipeline::Pipeline",
  "spark_core::pipeline::pipeline::PipelineEvent",
  "spark_core::pipeline::pipeline::PipelineEventKind",
  "spark_core::pipeline::pipeline::PipelineHandleId(_)",
  "spark_core::pipeline::instrument::HandlerSpan",
  "spark_core::pipeline::instrument::HandlerSpanGuard",
  "spark_core::pipeline::instrument::HandlerSpanParams",
  "spark_core::pipeline::instrument::HandlerSpanTracer",
  "spark_core::pipeline::instrument::HandlerTracerError",
  "spark_core::pipeline::instrument::InstrumentedLogger",
  "spark_core::pipeline::initializer::ChainBuilder",
  "spark_core::pipeline::initializer::PipelineInitializer",
  "spark_core::pipeline::initializer::InitializerDescriptor",
]

[[contract]]
term = "PipelineInitializer"
rustdoc = [
  { name = "spark_core::pipeline::initializer", url = "https://docs.rs/spark-core/latest/spark_core/pipeline/initializer/index.html" },
]
examples = [
  { name = "ChainBuilder::register_inbound", path = "../crates/spark-core/src/data_plane/pipeline/initializer.rs#L52-L118" },
]
tck = [
  { name = "observability::middleware_emits_expected_attributes", path = "../crates/spark-contract-tests/src/observability.rs#L210-L332" },
]
covers = [
  "spark_core::pipeline::PipelineInitializer",
  "spark_core::pipeline::InitializerDescriptor",
  "spark_core::pipeline::initializer::ChainBuilder",
  "spark_core::pipeline::initializer::PipelineInitializer",
  "spark_core::pipeline::initializer::InitializerDescriptor",
]

[[contract]]
term = "Service"
rustdoc = [
  { name = "spark_core::service", url = "https://docs.rs/spark-core/latest/spark_core/service/index.html" },
  { name = "spark-middleware", url = "https://docs.rs/spark-middleware/latest/spark_middleware/" },
]
examples = [
  { name = "ServiceLogic::call", path = "../crates/spark-core/src/data_plane/service/simple.rs#L120-L210" },
]
tck = [
  { name = "graceful_shutdown::coordinator_waits_for_service_draining", path = "../crates/spark-contract-tests/src/graceful_shutdown.rs#L820-L1012" },
]
covers = [
  "spark_core::service::AutoDynBridge",
  "spark_core::service::BoxService",
  "spark_core::service::Decode",
  "spark_core::service::DynBridge",
  "spark_core::service::DynService",
  "spark_core::service::Encode",
  "spark_core::service::Layer",
  "spark_core::service::PayloadDirection",
  "spark_core::service::Service",
  "spark_core::service::ServiceMetricsHook",
  "spark_core::service::ServiceObject",
  "spark_core::service::ServiceOutcome",
  "spark_core::service::SimpleServiceFn",
  "spark_core::service::auto_dyn::AutoDynBridge",
  "spark_core::service::auto_dyn::Decode",
  "spark_core::service::auto_dyn::DynBridge",
  "spark_core::service::auto_dyn::Encode",
  "spark_core::service::metrics::PayloadDirection",
  "spark_core::service::metrics::ServiceMetricsHook",
  "spark_core::service::metrics::ServiceOutcome",
  "spark_core::service::simple::AsyncFnLogic",
  "spark_core::service::simple::SequentialService",
  "spark_core::service::simple::ServiceLogic",
  "spark_core::service::simple::SimpleServiceFn",
  "spark_core::service::BoxService",
  "spark_core::service::DynService",
  "spark_core::service::Layer",
  "spark_core::service::Service",
  "spark_core::service::ServiceObject",
  "spark_core::service::generic::Layer",
  "spark_core::service::generic::Service",
  "spark_core::service::object::BoxService",
  "spark_core::service::object::DynService",
  "spark_core::service::object::ServiceObject",
]

[[contract]]
term = "Router"
rustdoc = [
  { name = "spark-router", url = "https://docs.rs/spark-router/latest/spark_router/" },
]
examples = [
  { name = "DefaultRouter::route_dyn", path = "../crates/spark-router/src/lib.rs#L303-L361" },
]
tck = [
  { name = "route_dyn_returns_new_service", path = "../crates/spark-router/src/lib.rs#L538-L611" },
]
covers = [
  "spark_router::DefaultRouter",
  "spark_router::RouteRegistration",
  "spark_router::ServiceFactory",
  "spark_router::pipeline::ApplicationRouter",
  "spark_router::pipeline::RoutingContextState",
]

[[contract]]
term = "Context"
rustdoc = [
  { name = "spark_core::context", url = "https://docs.rs/spark-core/latest/spark_core/context/index.html" },
  { name = "spark_core::contract", url = "https://docs.rs/spark-core/latest/spark_core/contract/index.html" },
]
examples = [
  { name = "CallContext::with_deadline", path = "../crates/spark-core/src/kernel/contract.rs#L120-L198" },
]
tck = [
  { name = "state_machine::child_context_inherits_cancellation", path = "../crates/spark-contract-tests/src/state_machine.rs#L60-L214" },
]
covers = [
  "spark_core::context::Context",
  "spark_core::contract::CallContext",
  "spark_core::contract::CallContextBuilder",
  "spark_core::contract::Cancellation",
  "spark_core::contract::Deadline",
  "spark_core::contract::SecurityContextSnapshot",
]

[[contract]]
term = "Error"
rustdoc = [
  { name = "spark_core::error", url = "https://docs.rs/spark-core/latest/spark_core/error/index.html" },
]
examples = [
  { name = "CategoryMatrixEntry (generated)", path = "../crates/spark-core/src/error/generated/category_matrix.rs#L20-L210" },
]
tck = [
  { name = "errors::domain_error_is_mapped_to_matrix", path = "../crates/spark-contract-tests/src/errors.rs#L250-L360" },
]
covers = [
  "spark_core::error::CoreError",
  "spark_core::error::DomainError",
  "spark_core::error::ImplError",
  "spark_core::error::IntoCoreError",
  "spark_core::error::IntoDomainError",
  "spark_core::error::SparkError",
  "spark_core::error::ErrorTelemetryView",
  "spark_core::error::observability::ErrorTelemetryView",
  "spark_core::error::category_matrix::BudgetDisposition",
  "spark_core::error::category_matrix::BusyDisposition",
  "spark_core::error::category_matrix::CategoryMatrixEntry",
  "spark_core::error::category_matrix::CategoryTemplate",
  "spark_core::error::category_matrix::DefaultAutoResponse",
]

[[contract]]
term = "State"
rustdoc = [
  { name = "spark_core::model", url = "https://docs.rs/spark-core/latest/spark_core/model/index.html" },
  { name = "spark_core::status", url = "https://docs.rs/spark-core/latest/spark_core/status/index.html" },
]
examples = [
  { name = "RetryAdvice", path = "../crates/spark-core/src/kernel/status/retry.rs#L15-L140" },
]
tck = [
  { name = "backpressure::channel_queue_exhaustion_emits_busy_then_retry_after", path = "../crates/spark-contract-tests/src/backpressure.rs#L60-L198" },
]
covers = [
  "spark_core::model::State",
  "spark_core::model::Status",
  "spark_core::status::RetryAdvice",
  "spark_core::status::RetryAfterThrottle",
  "spark_core::status::RetryRhythm",
  "spark_core::status::SubscriptionBudget",
  "spark_core::status::ready::QueueDepth",
  "spark_core::status::ready::RetryAdvice",
  "spark_core::status::ready::RetryAfterThrottle",
  "spark_core::status::ready::RetryRhythm",
  "spark_core::status::ready::SubscriptionBudget",
]

[[contract]]
term = "BackpressureBudgetShutdown"
rustdoc = [
  { name = "spark_core::contract::BackpressureSignal", url = "https://docs.rs/spark-core/latest/spark_core/contract/enum.BackpressureSignal.html" },
]
examples = [
  { name = "TODO", path = "(pending)" },
]
tck = [
  { name = "TODO", path = "(pending)" },
]
covers = [
  "spark_core::contract::BackpressureSignal",
  "spark_core::contract::ShutdownGraceful",
  "spark_core::contract::ShutdownImmediate",
  "spark_core::contract::StateAdvance",
  "spark_core::contract::ContractStateMachine",
]

```
<!-- contracts-index:end -->
