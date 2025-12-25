//! 运维事件契约的便捷出口。
//!
//! # 教案式说明
//! - **意图（Why）**：通过 `spark-otel` 统一暴露事件枚举与事件总线 Trait，方便在构建 OpenTelemetry 集成时同时引入事件语义；
//! - **逻辑（How）**：直接 re-export `spark-core::observability` 内的枚举与 Trait，保持契约完整性；
//! - **契约（What）**：类型定义完全沿用核心实现，调用方可在此基础上实现自定义事件总线。

pub use spark_core::observability::{
    CoreUserEvent, EventPolicy, OpsEvent, OpsEventBus, OpsEventKind,
};
