//! 健康探针契约的便捷出口。
//!
//! # 教案式说明
//! - **意图（Why）**：让依赖 `spark-otel` 的集成层可以直接访问健康状态与探针 Trait，无需额外引入 `spark-core`；
//! - **逻辑（How）**：重导出 `spark-core::observability` 中的健康相关类型，保持语义一致；
//! - **契约（What）**：类型定义与核心一致，可直接复用或扩展。

pub use spark_core::observability::{
    ComponentHealth, HealthCheckProvider, HealthChecks, HealthState,
};
