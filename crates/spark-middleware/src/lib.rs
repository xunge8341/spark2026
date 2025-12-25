#![cfg_attr(not(feature = "std"), no_std)]

//! spark-middleware: 面向 `spark-core` Pipeline 的可复用中间件组件库。
//!
//! # 教案式概览
//! - **意图（Why）**：为应用和平台团队提供现成的日志、指标、编解码中间件，避免在每个工程重复编写 Handler 样板代码。
//! - **结构（How）**：按关注点拆分为 [`logging`], [`metrics`] 与 [`codec`] 三个模块，每个模块都遵循
//!   [`spark_core::pipeline::PipelineInitializer`] 契约，可直接注入 `Pipeline`。
//! - **契约（What）**：所有中间件均在 `configure` 阶段向 [`spark_core::pipeline::ChainBuilder`] 注册
//!   [`spark_core::pipeline::handler::InboundHandler`] 与 [`OutboundHandler`](spark_core::pipeline::handler::OutboundHandler)
//!   实现，并复用 [`spark_core::runtime::CoreServices`] 暴露的能力（日志、指标、缓冲池等）。
//! - **风险提示（Trade-offs）**：模块以 `no_std + alloc` 为默认目标，若依赖宿主运行时额外能力（如 I/O），需在上层扩展。

extern crate alloc;

pub mod codec;
pub mod logging;
pub mod metrics;

pub use codec::{CodecMiddleware, CodecTransform};
pub use logging::{LoggingMiddleware, LoggingMiddlewareConfig};
pub use metrics::{MetricsMiddleware, MetricsMiddlewareConfig};
