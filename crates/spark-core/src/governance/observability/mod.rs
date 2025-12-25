//! 可观测性契约模块，提供跨平台通信框架在指标、日志、追踪与运维事件方面的统一接口。
//!
//! # 设计背景（Why）
//! - 借鉴 OpenTelemetry、AWS OTEL Distro、Datadog、New Relic、Google Cloud Operations 等业内头部平台的稳定契约，
//!   以 Rust `no_std + alloc` 可运行的方式进行抽象，确保框架在多云、多 Runtime 环境下保持一致的可观测性语义。
//! - 结合前沿研究（分布式系统可观测性中的可组合事件模型、Self-Describing Telemetry）提供可扩展、可推理的契约，
//!   便于后续学术原型或实验性特性在不破坏现有实现的前提下接入。
//!
//! # 模块概览（How）
//! - [`trace`]：围绕 W3C Trace Context 的追踪上下文建模。
//! - [`logging`]：统一日志事件结构，支持结构化字段与 Trace 关联。
//! - [`metrics`]：覆盖计数器、测量仪表、直方图等核心指标的契约与属性模型。
//! - [`keys`]：由合约生成的指标/日志/追踪键名常量，提供单一事实来源。
//! - [`health`]：描述健康状态与异步探针能力。
//! - [`events`]：定义核心用户事件与运维事件总线，适配跨组件事件流。
//! - [`attributes`]：提供指标与日志的键值对建模，确保高基数控制与类型安全。
//!
//! # 使用契约（What）
//! - 每个子模块都遵循对象安全约束，可在运行时通过 `Arc<dyn Trait>` 注入。
//! - 调用方需确保满足各模块文档中声明的前置条件，并正确处理返回值承诺的后置条件。
//!
//! # 风险与权衡（Trade-offs）
//! - 为适配 `no_std` 场景，未引入 `std::time::SystemTime` 等类型；如需时间戳请由上层在 `LogRecord` 等结构中补齐。
//! - `TraceState` 与 `Attribute` 模块采用可扩展 `Vec` 存储，使用者需在高频路径中复用缓冲以避免重复分配。

pub mod attributes;
pub mod events;
pub mod facade;
pub mod health;
pub mod keys;
pub mod logging;
pub mod metrics;
pub mod resource;
pub mod trace;

pub use attributes::{
    AttributeKey, AttributeSet, KeyValue, MetricAttributeValue, OwnedAttributeSet,
};
pub use events::{
    ApplicationEvent, CoreUserEvent, EventPolicy, IdleDirection, IdleTimeout, OpsEvent,
    OpsEventBus, OpsEventKind, RateDirection, RateLimited, TlsInfo,
};
pub use facade::ObservabilityFacade;
pub use health::{ComponentHealth, HealthCheckProvider, HealthChecks, HealthState};
pub use logging::{LogField, LogRecord, LogSeverity, Logger};
pub use metrics::{Counter, Gauge, Histogram, InstrumentDescriptor, MetricsProvider};
pub use resource::{OwnedResourceAttrs, ResourceAttr, ResourceAttrSet};
pub use trace::{
    SpanId, TraceContext, TraceContextError, TraceFlags, TraceId, TraceState, TraceStateEntry,
    TraceStateError,
};
