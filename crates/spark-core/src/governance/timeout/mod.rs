//! 超时治理命名空间：对齐“配置侧档案”与“运行时热更新容器”的组合模型。
//!
//! # 模块定位（Why）
//! - 将原 `config.rs` 与 `runtime/timeouts.rs` 合并，统一描述超时策略的声明、解析与动态分发；
//! - 为运行时提供单一入口，避免在平台与治理间重复导出相同类型。
//!
//! # 结构说明（What）
//! - [`profile`]：定义 `Timeout`、`TimeoutProfile` 等声明性配置；
//! - [`runtime`]：定义运行时可感知的热更新容器与纪元观测接口。
//!
//! # 使用方式（How）
//! - 配置解析层：`use crate::governance::timeout::profile::*;` 构建策略档案；
//! - 运行时：`use crate::governance::timeout::runtime::*;` 维护动态配置，并在平台层注入读写栅栏；
//! - 平台若需要兼容旧路径，可继续通过 `crate::Timeout` 等 re-export 访问。
//!
//! # 维护提示（Trade-offs）
//! - `runtime` 模块依赖平台的热更新原语（`HotReload*`），请确保仅通过 trait/轻量别名访问，避免形成循环；
//! - 若未来扩展新的超时族（如连接、批处理超时），应在此目录下增设子模块保持一致性。

pub mod profile;
pub mod runtime;

pub use profile::{Timeout, TimeoutProfile};
pub use runtime::{TimeoutConfigError, TimeoutRuntimeConfig, TimeoutSettings};
