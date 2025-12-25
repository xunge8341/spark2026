//! `codec` 模块定义跨平台通用通信中的编解码契约。
//!
//! # 模块设计（Why）
//! - 借鉴 gRPC、Apache Arrow Flight、NATS、Kafka、Netty 等业内头部产品的编解码抽象，强调**内容协商**、**类型安全**与**流式增量处理**。
//! - 面向 `spark-core` 的无协议假设，我们将编解码拆分为元信息（`metadata`）、编码上下文（`encoder`）、解码上下文（`decoder`）以及运行时协商（`registry`），降低单文件复杂度，便于在 `no_std` 环境维护。
//! - 模块中所有接口均避免携带 `Spark` 命名前缀，遵循跨项目复用的一致命名习惯。
//!
//! # 使用指引（How）
//! - 实现者需首先定义 `CodecDescriptor` 描述内容类型、压缩算法与 schema 信息，再依据业务需求实现 `Encoder` 与 `Decoder`。
//! - 若需要在运行时根据客户端声明协商具体编解码方案，可实现 `CodecRegistry` 并注册多个 `DynCodecFactory`。
//! - `TypedCodecAdapter` 为典型的桥接器：它把带有静态泛型保证的 `Codec` 转换为可存放在注册中心的对象安全 `DynCodec`，此模式源自 Netty `ChannelHandler` 的 type adapter。
//!
//! # 契约说明（What）
//! - 模块导出编码、解码所需的上下文结构（`EncodeContext`、`DecodeContext`），以及统一的 `DecodeOutcome` 状态枚举。
//! - 除 `CodecRegistry` 等接口返回 `Result` 外，其余结构为纯数据，调用者可以在 `no_std` + `alloc` 环境直接使用。
//!
//! # 风险提示（Trade-offs）
//! - 由于对象安全的需要，`DynCodec` 在运行时执行类型擦除并依赖 `Any` 做下转型。适配器在失败时会返回稳定错误码 `protocol.type_mismatch`，请在调用侧处理。
//! - 注册中心只定义契约，未绑定具体数据结构；实现者需自行关注并发安全与生命周期管理。

mod contract;
mod decoder;
mod dyn_codec;
mod encoder;
mod frame_constraints;
mod metadata;
pub mod metrics;
mod registry;

pub use contract::Codec;
pub use decoder::{DecodeContext, DecodeFrameGuard, DecodeOutcome, Decoder};
pub use dyn_codec::{DynCodec, TypedCodecAdapter};
pub use encoder::{EncodeContext, EncodeFrameGuard, EncodedPayload, Encoder};
pub use metadata::{CodecDescriptor, ContentEncoding, ContentType, SchemaDescriptor};
pub use metrics::{CodecMetricsHook, CodecPhase};
pub use registry::{CodecRegistry, DynCodecFactory, NegotiatedCodec, TypedCodecFactory};
