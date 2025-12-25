//! 日志契约的便捷出口。
//!
//! # 教案式说明
//! - **意图（Why）**：提供 `spark-otel` 统一入口，方便集成方在依赖 OpenTelemetry 实现时，直接通过本 crate 引入日志契约类型；
//! - **逻辑（How）**：简单地转 re-export `spark-core::observability` 中的稳定类型，无需额外封装；
//! - **契约（What）**：保持类型定义不变，确保与核心契约完全一致。

pub use spark_core::observability::{LogField, LogRecord, LogSeverity, Logger};
