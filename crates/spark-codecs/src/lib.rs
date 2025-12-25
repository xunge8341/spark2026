#![cfg_attr(not(feature = "std"), no_std)]
#![warn(missing_docs)]

//! # spark-codecs
//!
//! ## 教案意图（Why）
//! - **职责定位**：为各类 `spark-codec-*` 协议实现提供统一、稳定的编解码契约与预算限额接口，
//!   避免每个协议 crate 直接依赖庞大的 `spark-core`。
//! - **架构价值**：通过集中 re-export `spark-core` 的 codec/预算/缓冲等稳定面，实现在协议实现层面的
//!   插拔替换，同时维持核心 crate 的演进节奏。
//! - **团队协作**：简化协议 crate 的依赖拓扑，使并行开发时仅需关注本协议逻辑即可。
//!
//! ## 使用方式（How）
//! - 在协议 crate 中引入 `spark-codecs`，即可访问 `DecodeContext`、`EncodeContext`、`Codec`、`Budget`
//!   等核心接口，并沿用 `spark-core` 的错误与缓冲类型；
//! - Feature `alloc`/`std`/`compat_v0` 直接透传到 `spark-core`，保持二者行为一致；
//! - 如需访问更底层的 `spark-core` 能力，可通过本 crate re-export 的模块进行扩展。
//!
//! ## 契约说明（What）
//! - 对外暴露的所有类型均来源于 `spark-core`，确保语义一致；
//! - 不额外引入状态或逻辑，纯粹扮演“接口整合层”；
//! - **前置条件**：调用方需遵循 `spark-core` 对各接口的使用契约；
//! - **后置条件**：协议 crate 仅依赖 `spark-codecs` 亦可完整实现编解码逻辑。
//!
//! ## 风险提示（Trade-offs）
//! - `spark-codecs` 当前为 re-export 形态，后续若 core 层重构需同步更新此处映射；
//! - 若协议实现仍需访问 `spark-core` 的实验性模块，需额外声明依赖并关注兼容性。

extern crate alloc;

/// 统一暴露核心错误类型。
pub use spark_core::CoreError;
/// 重新导出 `spark-core` 中的缓冲契约，供协议编解码器复用。
pub use spark_core::buffer;
/// 重新导出编解码契约模块，保持原有路径结构。
pub use spark_core::codec;
/// 重新导出 `spark-core` 的兼容层接口（受 `compat_v0` 控制）。
#[cfg(feature = "compat_v0")]
pub use spark_core::compat;
/// 暴露完整的错误模块，便于协议实现引用分类枚举等类型。
pub use spark_core::error;
/// 暴露错误码常量命名空间。
pub use spark_core::error::codes;
/// 暴露预算相关类型，支撑帧大小与资源限额控制。
pub use spark_core::types::{Budget, BudgetDecision, BudgetKind, BudgetSnapshot};

/// 指标相关类型与 RAII 守卫通过 `codec` 命名空间导出，保持与核心 crate 的一致性。
pub use spark_core::codec::{
    DecodeFrameGuard, EncodeFrameGuard,
    metrics::{CodecMetricsHook, CodecPhase},
};
/// 便捷 re-export：直接在 crate 根访问常用编解码接口。
pub use spark_core::{
    Codec, CodecDescriptor, CodecRegistry, ContentEncoding, ContentType, DecodeContext,
    DecodeOutcome, DynCodec, DynCodecFactory, EncodeContext, EncodedPayload, Encoder,
    NegotiatedCodec, SchemaDescriptor, TypedCodecAdapter, TypedCodecFactory,
};
