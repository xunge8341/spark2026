//! 审计事件 Schema 及其辅助工具集。
//!
//! # 设计目标（Why）
//! - 提供跨组件共享的事件 Schema，使配置、路由等核心资源的变更能够形成不可篡改的哈希链。
//! - 暴露统一的事件记录接口，便于在运行期挂载文件、消息队列或 TSA（Time-Stamping Authority）等实现。
//! - 内置状态哈希工具与内存回放 Recorder，为 `T12｜审计事件 Schema v1.0` 的验收提供可脚本化样例。
//!
//! # 契约概览（What）
//! - [`AuditEventV1`]：事件载荷，涵盖事件 ID、实体信息、动作、前/后状态哈希、操作者、发生时间以及可选 TSA 锚点。
//! - [`AuditRecorder`]：抽象事件写入器；框架在关键资源变更处调用 `record` 推送事件。
//! - [`AuditStateHasher`]：将配置状态映射为稳定哈希值，保证链路完整性检测的一致性。
//! - `InMemoryAuditRecorder`：仅在 `std` 环境可用的参考实现，用于测试与本地回放验证。
//!
//! # 风险与注意事项（Trade-offs）
//! - 模块默认依赖 `alloc`，因此在纯 `no_std` 且无分配器的环境下不可用；若需进一步精简，请在上层提供裁剪版本。
//! - 事件链完整性依赖调用方保证所有写入都经过 [`AuditRecorder`]；若 Recorder 忽略错误，链式检测将失效。

mod actor;
mod changes;
mod context;
mod entity;
mod event;
mod hasher;
#[cfg(feature = "std")]
mod in_memory;
mod recorder;
mod tag;
mod tsa;

pub use actor::AuditActor;
pub use changes::{AuditChangeEntry, AuditChangeSet, AuditDeletedEntry};
pub use context::{AuditContext, AuditPipeline};
pub use entity::AuditEntityRef;
pub use event::AuditEventV1;
pub use hasher::AuditStateHasher;
#[cfg(feature = "std")]
pub use in_memory::InMemoryAuditRecorder;
pub use recorder::{AuditError, AuditRecorder};
pub use tag::AuditTag;
pub use tsa::TsaEvidence;
