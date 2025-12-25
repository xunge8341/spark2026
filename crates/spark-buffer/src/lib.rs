#![cfg_attr(not(feature = "std"), no_std)]

//! `spark-buffer` 提供面向 `BufferPool` 的具体缓冲实现。
//!
//! # 模块定位（Why）
//! - 为 `spark-core` 的抽象缓冲契约提供基于 `bytes::BytesMut` 的高性能实现，
//!   支撑零拷贝流水线与运行时内存池任务。
//! - 补足 `spark-core` 仅定义 trait、不落地实体的问题，
//!   使控制器、传输栈可以在不关心底层内存策略的情况下获取池化缓冲。
//!
//! # 设计概要（How）
//! - `pooled_buffer` 模块实现 `PooledBuffer`，负责持有可写 `BytesMut` 与冻结后的只读 `Bytes` 视图。
//! - 通过 `BufferRecycler` trait 将生命周期回收钩子显式化，
//!   在 `Drop` 阶段通知所属池归还容量，实现与池的松耦合协作。
//! - 所有公开类型均满足 `Send + Sync + 'static`，契合 `spark-core` 在跨线程流水线中的对象安全要求。
//!
//! # 命名约定（Consistency）
//! - 延续 `spark-core::buffer` 的术语，采用 `ReadableBuffer`/`WritableBuffer` 等通用命名，
//!   避免引入额外前缀，确保调用端体验一致。

extern crate alloc;

mod pool;
mod pooled_buffer;

pub use pool::SlabBufferPool;
pub use pooled_buffer::{BufferRecycler, PooledBuffer, ReclaimedBuffer};
