//! 指标契约的便捷出口。
//!
//! # 教案式说明
//! - **意图（Why）**：在引入 `spark-otel` 时集中访问计数器、Gauge 与直方图等契约类型；
//! - **逻辑（How）**：直接 re-export `spark-core` 中的 Trait 与描述符常量，避免调用方跨 crate 跳转；
//! - **契约（What）**：不改变类型定义，确保与核心契约保持二进制兼容。

pub use spark_core::observability::{
    Counter, Gauge, Histogram, InstrumentDescriptor, MetricsProvider,
};
