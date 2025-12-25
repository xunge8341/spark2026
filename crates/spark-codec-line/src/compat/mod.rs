#![allow(missing_docs)]

//! # 模块综述（Why）
//! - `spark-codec-line` 作为外部扩展 crate，也需要在 `compat_v0` 阶段提供旧版就绪信号到 `ReadyState` 的过渡工具。
//! - 该目录仅在兼容功能开启时编译，确保主路径保持轻量。
//!
//! # 与核心库的关系（How）
//! - 绝大部分逻辑直接复用 `spark-core` 提供的适配函数，本模块的职责是向扩展 crate 的调用者暴露统一入口。
//! - 当未来移除兼容层时，仅需删除该目录，不会影响 `line` 模块的核心功能。

pub mod flow_ready;
