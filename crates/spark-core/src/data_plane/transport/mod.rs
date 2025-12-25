//! 传输子系统的跨平台契约集合。
//!
//! ## 设计蓝图（Why）
//! - **生产级参考**：融合 gRPC, QUIC, NATS, MQTT, Apache Pulsar 等稳定框架的接口抽象——统一 Endpoint 模型、显式 QoS、会话生命周期与安全模式。
//! - **前沿探索**：借鉴 WebTransport、KCP/QUIC 混合拥塞控制、学术界对 Intent-Based Networking 的研究成果，将“连接意图”建模为一等公民，便于策略推理。
//! - **跨平台目标**：严格限制 `std` 依赖，所有异步能力通过 `BoxFuture` 等 `alloc` 友好工具提供，以满足嵌入式、裸机与云原生的统一部署需求。
//!
//! ## 模块概览（How）
//! - [`address`]：定义跨平台的 Socket 地址表示。
//! - [`params`]：描述传输相关的参数键值集合及类型化读取方法。
//! - [`endpoint`]：统一逻辑与物理地址的层级结构。
//! - [`intent`]：对连接/监听意图进行结构化建模，承载 QoS、安全、可用性约束。
//! - [`server`]：服务端监听契约，规范优雅关闭语义。
//! - [`factory`]：抽象传输工厂的构建、连接流程，与服务发现组件协作。
//!
//! ## 命名约定（What）
//! - 禁用 `Spark*` 前缀，遵循行业共识命名（`Endpoint`、`SocketAddr`、`QoS` 等）。
//! - 避免晦涩缩写，除 QoS 等广泛接受的术语外，均使用完整单词。

pub mod address;
pub mod builder;
pub mod channel;
pub mod datagram;
pub mod endpoint;
pub mod factory;
pub mod handshake;
pub mod intent;
pub mod metrics;
pub mod params;
pub mod server;
pub mod server_channel;
pub mod shutdown;

pub use address::{AddressFamily, TransportSocketAddr};
pub use builder::TransportBuilder;
pub use channel::{BackpressureDecision, BackpressureMetrics, Channel};
pub use datagram::DatagramEndpoint;
pub use endpoint::{Endpoint, EndpointKind};
pub use factory::{DynTransportFactory, ListenerConfig, TransportFactory, TransportFactoryObject};
pub use handshake::{
    Capability, CapabilityBitmap, DowngradeReport, HandshakeError, HandshakeErrorKind,
    HandshakeOffer, HandshakeOutcome, NegotiationAuditContext, Version, negotiate,
};
pub use intent::{
    AvailabilityRequirement, ConnectionIntent, QualityOfService, SecurityMode, SessionLifecycle,
};
pub use metrics::{LinkDirection, TransportMetricsHook};
pub use params::TransportParams;
pub use server::{ListenerShutdown, describe_shutdown_target};
pub use server_channel::{
    DynServerChannel, PipelineInitializerSelector, ServerChannel, ServerChannelObject,
};
pub use shutdown::ShutdownDirection;
