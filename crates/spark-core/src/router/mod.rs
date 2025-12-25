//! Router 契约模块，集中定义路由匹配与决策的核心数据结构。
//!
//! # 教案级说明（Why）
//! - 将对象层路由契约沉淀到 `spark-core`，为 `spark-router`、宿主框架与示例代码提供统一依赖面。
//! - 以最小实现填补缺失的模块占位，避免破坏既有核心设计，同时为后续演进保留清晰扩展点。
//!
//! # 模块结构（How）
//! - `binding`：描述路由决策产物（绑定对象与警告）。
//! - `catalog`：提供可枚举的路由目录快照，用于观察与调试。
//! - `context`：承载路由意图、运行时快照与上下文借用。
//! - `metadata`：定义路由元数据键值对，负责静态/动态标签合并。
//! - `route`：建模路由模式、段与稳定 ID。
//!
//! # 使用契约（What）
//! - 调用方可通过 [`DynRouter`](trait@crate::router::DynRouter) 对象安全接口进行路由判定；
//! - 所有结构均支持 `no_std + alloc`，并默认避免额外的堆分配与锁。
//!
//! # 设计权衡（Trade-offs）
//! - 本次补齐遵循“最小可用实现”原则，不在核心契约上引入破坏性变更；
//! - 元数据使用 `BTreeMap` 保持迭代顺序确定性，便于测试与日志；
//! - 若未来需要更复杂的匹配算法（例如基于前缀树），可在不改变对外接口的前提下替换内部实现。

extern crate alloc;

pub mod binding;
pub mod catalog;
pub mod context;
pub mod metadata;
pub mod route;

pub use binding::{DynRouter, RouteBindingObject, RouteDecisionObject, RouteError};
pub use catalog::{RouteCatalog, RouteDescriptor};
pub use context::{RoutingContext, RoutingIntent, RoutingSnapshot};
pub use metadata::{MetadataKey, MetadataValue, RouteMetadata};
pub use route::{RouteId, RouteKind, RoutePattern, RouteSegment};
