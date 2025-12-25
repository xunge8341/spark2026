//! 核心命名空间：集中承载 spark-core 最底层的执行契约与共享数据结构。
//!
//! # 模块定位（Why）
//! - **统一契约入口**：将调用上下文、基础类型、状态枚举等散落文件聚合，明确“kernel = 最小可用内核”；
//! - **跨子系统共享**：治理、平台、数据面全部依赖这些抽象，通过单一点维护降低交叉依赖复杂度；
//! - **后续拆分准备**：未来若拆分成多个 crate，可直接将 `kernel` 目录作为独立 crate 的主体，无需重新整理文件。
//!
//! # 结构概览（What）
//! - [`contract`]：请求生命周期、取消/背压语义；
//! - [`context`]：运行时传播的上下文容器；
//! - [`common`]、[`types`]、[`ids`]：常用基础类型、ID 生成辅助；
//! - [`future`]：对象安全的 Future/Stream 类型别名；
//! - [`status`]、[`model`]：就绪态/状态机抽象，为平台与数据面提供统一状态语言；
//! - [`arc_swap`]、[`sealed`]：共享内部设施，分别处理热更新指针交换与 trait sealing。
//!
//! # 集成提示（How）
//! - 其他命名空间通过 `use crate::kernel::...` 引入契约；
//! - `lib.rs` 会对其中关键模块做 re-export，保持 `spark_core::contract::CallContext` 等路径兼容；
//! - 调整或新增内核契约时，务必更新对应的治理/平台模块注释，确保整体架构说明同步。
//!
//! # 风险与维护要点（Trade-offs）
//! - 内核代码的任何破坏性变更都会影响整个生态，提交前需运行全量契约测试；
//! - 若引入新的基础类型，请评估 `no_std + alloc` 兼容性，避免依赖 `std`；
//! - 为保持命名一致性，请遵循现有文件命名风格（snake_case）与注释规范。

pub mod arc_swap;
pub mod common;
pub mod context;
pub mod contract;
pub mod future;
pub mod ids;
pub mod model;
pub mod sealed;
pub mod status;
pub mod types;
