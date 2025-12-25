//! Spark 合约稳定面测试入口，聚焦取消/截止/预算/安全等核心契约。
//!
//! # 设计说明（Why）
//! - 以“稳定面”为维度划分测试文件，确保每个公共 API 都具备明确的契约验证；
//! - 引入统一的支撑模块与断言宏，保证测试在失败注入或重试场景下仍能稳定复现。
//!
//! # 结构概览（How）
//! - `support`：提供宏与构造器；
//! - 其余子模块按契约主题划分，例如 `cancellation`, `deadline`, `budget` 等；
//! - 每个子模块内均包含覆盖正常路径、异常路径与幂等性的测试用例。
//!
//! # 合约范围（What）
//! - 覆盖 `spark_core::contract` 暴露的稳定 API，包括 `Cancellation`, `Deadline`, `Budget`,
//!   `CallContext`, `SecurityContextSnapshot`, `ObservabilityContract`, `CloseReason`；
//! - 追加针对 `Context` 的一致性校验，确保三元组视图与原始上下文保持同步。
//!
//! # 风险提示（Trade-offs）
//! - 若新增契约类型，请同步扩展本入口，以便在覆盖率统计中自动纳入；
//! - 顶层模块不直接定义测试函数，避免 `cargo test` 输出冗长路径。

mod support;

use support::*;

mod cancellation;
mod deadline;
mod budget;
mod security;
mod call_context;
mod observability;
mod close_reason;
mod timeout_cancel_replay;
mod timeout_cancel_priority;
mod shutdown;
mod handshake;
mod context_propagation;
mod context_propagation_spawn;
mod retry_after;
mod service_macro;
mod error_category_autoresponse;
