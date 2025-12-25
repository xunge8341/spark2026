//! 治理命名空间：统一描述策略、审计、安全与可观测性等“控制面”能力。
//!
//! # 模块定位（Why）
//! - **策略集中**：把限流、重试、安全策略等模块放在一起，帮助读者快速理解治理面职责；
//! - **运行视角**：配置、审计、弃用公告、观测指标在生命周期上紧密耦合，共置后便于协调演进；
//! - **拆分准备**：未来若将治理能力抽成独立 crate，可整体迁移此目录并保持对 kernel 的单向依赖。
//!
//! # 结构概览（What）
//! - [`configuration`]：配置中心契约与变更管道；
//! - [`timeout`]：将配置模型（`profile`）与运行时热更新容器（`runtime`）归档于同一命名空间；
//! - [`limits`]、[`retry`]、[`security`]：治理策略族；
//! - [`audit`]、[`observability`]、[`deprecation`]：变更记录、指标/日志契约与弃用治理。
//!
//! # 使用指南（How）
//! - 数据面与平台通过 `use crate::governance::...` 引入所需策略；
//! - 若需要对外兼容旧路径，可在 `lib.rs` 中使用 `pub use governance::module;` 进行重导出；
//! - 添加新策略时请补充模块注释，明确策略目的、输入输出及与其他模块的交互。
//!
//! # 风险提示（Trade-offs）
//! - 治理层往往与外部系统对接（配置中心、监控、密钥管理），修改接口需评估跨 crate 影响；
//! - 避免在此目录引入对数据面/平台的反向依赖，以防止循环；必要时通过 trait 或事件桥接。

pub mod audit;
pub mod configuration;
pub mod deprecation;
pub mod limits;
pub mod observability;
pub mod retry;
pub mod security;
pub mod timeout;
