#![allow(clippy::module_name_repetitions)]

//! # spark-core Prelude
//!
//! ## 教案级说明（Why）
//! - **统一导入面**：为上层 crate 提供一个稳定、浅路径的导入入口，
//!   避免在业务代码中出现大量 `spark_core::contract::...` 等深层次路径，
//!   从而降低“复制粘贴 + 临时重定义”带来的契约分叉风险。
//! - **体系定位**：该模块位于 `spark-core` 的最外层，面向使用者暴露“常用契约组合包”，
//!   是 `pipeline`、`buffers`、`transport` 等上层组件构建的必经入口。
//! - **设计思路**：遵循 Rust 社区常见的 Prelude 模式，通过精选的 re-export 集合，
//!   让依赖方仅需 `use spark_core::prelude::*;` 即可获取常用类型。
//!
//! ## 逻辑拆解（How）
//! 1. **常量/别名 re-export**：直接透出核心错误类型与 `Result` 别名，
//!    保持错误语义统一；
//! 2. **契约类型聚合**：集中导出调用上下文（`CallContext` 等）、预算（`Budget*` 系列）、
//!    管道抽象（`Pipeline`、`Channel`、`PipelineInitializer`、`PipelineMessage` 等）以及运行时能力
//!    （`Context`、`CoreServices` 等）；
//! 3. **状态机语义**：包含 `status::ready` 中的核心判定枚举，支撑背压与管道调度；
//! 4. **传输层契约**：将 `TransportSocketAddr`、`ShutdownDirection` 等常用结构一并暴露，
//!    便于传输实现 crate 直接复用；
//! 5. **异步工具**：预置 `BoxFuture`、`BoxStream`，方便在无 `std` 环境下进行动态分发。
//!
//! ## 契约定义（What）
//! - **输入前置**：调用方应基于 `spark-core` 公布的稳定接口使用本 Prelude，
//!   不应假设其包含试验性或内部模块；
//! - **输出保证**：成功导入后，可稳定访问下列 re-export 的类型与函数；
//! - **版本策略**：Prelude 仅收录稳定契约；新增导出将遵循 SemVer，可向后兼容。
//!
//! ## 设计考量与权衡（Trade-offs）
//! - **范围控制**：为防止 Prelude 无限膨胀，仅纳入“跨模块高频依赖”的类型。
//!   对于边缘模块（例如观测、审计）仍建议使用明确命名空间以提升可读性；
//! - **性能影响**：纯 re-export 不引入额外代码路径，对编译与运行时零开销；
//! - **维护成本**：若核心契约迁移，需要同步更新此处映射，
//!   因此在代码评审中需重点关注 Prelude 的变更是否与稳定性策略一致。

/// `spark_core::prelude`：契约级常用类型一站式导入。
///
/// # 设计意图（Why）
/// `spark_core::prelude`：契约级常用类型一站式导入。
///
/// # 设计意图（Why）
/// - 降低新接入方的学习门槛，仅需 `use spark_core::prelude::*;` 即可获取调用上下文五元组、错误、ID 等核心概念；
/// - 避免业务侧错误导出内部模块（例如 `configuration`），确保依赖面受控；
/// - 支持 `no_std + alloc` 环境，全部类型均来源于本 crate。
///
/// # 收录内容（What）
/// - 调用上下文：[`CallContext`]、[`CallContextBuilder`]、[`Cancellation`]、[`Context`]、[`Deadline`]、[`TraceContext`];
/// - 错误体系：[`CoreError`]、[`Result`];
/// - 预算与协议：[`crate::contract::Budget`]、[`crate::contract::BudgetDecision`]、
///   [`crate::protocol::Event`]、[`crate::protocol::Frame`]、[`crate::protocol::Message`];
/// - 标识与配置：[`RequestId`]、[`CorrelationId`]、[`IdempotencyKey`]、[`Timeout`];
/// - 状态语义：[`State`]、[`Status`];
/// - 管线调度：[`crate::pipeline::Pipeline`]、[`crate::pipeline::PipelineInitializer`],
///   [`crate::pipeline::Channel`]、[`crate::transport::ServerChannel`];
/// - 辅助类型：[`NonEmptyStr`]、[`CloseReason`]、[`BudgetSet`]、[`TimeoutProfile`].
///
/// ## 导出明细（How）
/// - **入口综述（Why）**：`spark_core::prelude` 面向业务侧提供“调用上下文五元组 + 错误语义 + 预算”一站式导入，
///   上层仅需 `use spark_core::prelude::*;` 便可获得常见契约与辅助工具；
/// - **调用上下文（What）**：[`crate::CallContext`]、[`crate::CallContextBuilder`]、[`crate::Cancellation`],
///   [`crate::Context`]、[`crate::Deadline`] 用于描述一次 RPC 生命周期与取消/截止语义；
/// - **预算协同**：[`crate::contract::Budget`]、[`crate::contract::BudgetDecision`],
///   [`crate::contract::BudgetKind`]、[`crate::contract::BudgetSnapshot`] 抽象跨层背压资源；
/// - **错误体系**：[`crate::CoreError`]、[`crate::SparkError`]、[`crate::Result`] 及错误枚举确保统一的诊断与恢复策略；
/// - **状态/标识**：[`crate::State`]、[`crate::Status`]、[`crate::RequestId`]、[`crate::ids::CorrelationId`],
///   [`crate::IdempotencyKey`]、[`crate::Timeout`] 维护状态机与请求追踪；
/// - **观测与运行时**：[`crate::runtime::CoreServices`]、[`crate::runtime::MonotonicTimePoint`],
///   [`crate::observability::Logger`]、[`crate::observability::TraceContext`] 提供运行期调度与观测能力；
/// - **管线调度**：[`crate::pipeline::Pipeline`]、[`crate::pipeline::PipelineInitializer`],
///   [`crate::pipeline::Channel`]、[`crate::buffer::PipelineMessage`] 描述连接生命周期与消息编解码；
/// - **传输抽象**：[`crate::transport::ServerChannel`]、[`crate::transport::TransportSocketAddr`],
///   [`crate::transport::ShutdownDirection`] 让监听器在协议协商后即可完成装配；
/// - **缓冲与服务装配**：[`crate::buffer::ReadableBuffer`]、[`crate::buffer::WritableBuffer`],
///   [`crate::service::BoxService`]、[`crate::service::Layer`] 等支撑流水线与服务组合。
pub use crate::{
    async_trait,
    buffer::{
        BufView, BufferAllocator, BufferPool, Bytes, PipelineMessage, PoolStats, ReadableBuffer,
        WritableBuffer,
    },
    context::Context,
    contract::DEFAULT_OBSERVABILITY_CONTRACT,
    error::{
        CoreError, DomainError, DomainErrorKind, ErrorCategory, ErrorCause, ImplError,
        ImplErrorKind, IntoCoreError, IntoDomainError, Result, SparkError,
    },
    future::{BoxFuture, BoxStream, Stream},
    observability::{CoreUserEvent, LogRecord, Logger, TraceContext},
    // 教案级说明：统一透出“Pipeline 三件套”。
    // Why: 对外暴露重构后的 `Pipeline` 栈，而非旧的 Controller/Middleware/Router 名称，
    //       让依赖方在导入 Prelude 时即可与新版控制面语义对齐。
    // How: 依次 re-export `Channel`（连接生命周期抽象）、`Pipeline`（事件调度核心）
    //       以及 `PipelineInitializer`（装配期配置驱动入口），维持与 data_plane::pipeline
    //       模块的命名一致性，从而避免二次封装导致的概念漂移。
    // What: 在调用方执行 `use spark_core::prelude::*;` 时，可直接获取上述类型构建管线；
    //       前置条件为 pipeline 模块已由上游装配实现，后置条件是调用方可依赖这些契约
    //       完成 Handler/Transport 的组合。
    // Trade-offs: 牺牲对旧名称的向后兼容导出，换取语义明确的 API 面；边界场景为旧版仍依赖
    //             Controller 名称的业务，需要转向 `spark_core::pipeline::controller` 兼容映射。
    pipeline::{Channel, Pipeline, PipelineInitializer},
    runtime::{CoreServices, JoinHandle, MonotonicTimePoint},
    service::{
        AutoDynBridge, BoxService, Decode, DynService, Encode, Layer, Service, ServiceObject,
        type_mismatch_error,
    },
    status::ready::{BusyReason, PollReady, ReadyCheck, ReadyState, RetryAdvice},
    transport::{ServerChannel, ShutdownDirection, TransportSocketAddr},
    types::BudgetSet,
};

pub use crate::{
    CallContext, CallContextBuilder, Cancellation, CloseReason, Deadline, Event, Frame,
    IdempotencyKey, Message, NonEmptyStr, RequestId, State, Status, Timeout, TimeoutProfile,
};

pub use crate::ids::CorrelationId;
