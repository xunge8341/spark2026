//! 缓冲区契约模块。
//!
//! # 模块架构（Why）
//! - 将读取、写入、分配与消息桥接拆分为独立子模块，对齐 Netty、Tokio Bytes 等主流框架的职责分离实践。
//! - 通过统一的 `ReadableBuffer`/`WritableBuffer` 契约隐藏底层实现差异，让上层管线与运行时解耦具体内存策略。
//!
//! # 设计总览（How）
//! - [`buf_view`] 提供轻量级的零拷贝只读视图，用于在不获取所有权的场景下遍历分片。
//! - [`readable`] 定义高性能只读缓冲协议，涵盖 `split_to`、`peek`、`advance` 等核心操作。
//! - [`writable`] 提供可写缓冲协议，强调与只读视图之间的“冻结”转换。
//! - [`pool`] 约束缓冲池接口，使运行时能够租借/归还缓冲以控制内存峰值。
//! - [`message`] 描述流水线消息体，支持零拷贝字节与高层业务消息并存。
//!
//! # 命名共识（Consistency）
//! - 所有类型均避免使用特定业务前缀，遵循 Rust 与异步生态的惯用术语，便于与 Bytes/Tonic/Tokio 等生态互操作。

pub mod buf_view;
pub mod message;
pub mod pool;
pub mod readable;
pub mod writable;

pub use buf_view::{BufView, Chunks};
pub use message::{Bytes, PipelineMessage, UserMessage};
pub use pool::{BufferAllocator, BufferPool, PoolStatDimension, PoolStats};
pub use readable::{ErasedSparkBuf, ReadableBuffer};
pub use writable::{ErasedSparkBufMut, WritableBuffer};
