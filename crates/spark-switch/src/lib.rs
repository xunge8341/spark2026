#![cfg_attr(not(feature = "std"), no_std)]

//! # spark-switch
//!
//! ## 定位与职责（Why）
//! - 作为 SIP/SDP 等信令协议的中枢调度层，负责协调 `spark-core` 的会话契约与 `spark-router`
//!   的服务工厂，实现多协议会话在统一路由器中的组装、拆解与生命周期管理。
//! - 通过将各类对象层 Service 的构造逻辑集中在一个 Crate 中，为未来的信令路由、协议转换
//!   与状态同步提供统一扩展点。
//!
//! ## 架构嵌入（Where）
//! - `applications` 模块承载不同信令应用的 Service 实现，用于对接上层业务流程；
//! - `core` 模块负责会话状态管理、拓扑编排与协议互操作控制面；
//! - `error` 模块用于集中定义错误类型，统一向外暴露 `thiserror` 风格的诊断信息。
//!
//! ## Feature 策略（Trade-offs）
//! - `std` 特性开启后依赖 `dashmap` 与 `spark-router`，提供生产环境所需的并发调度能力；
//! - `alloc` 特性为后续在受限运行时中重用核心契约铺路，可在无 `std` 的情形下完成基础编译。
//! - 特别强调运行时去耦：`std` 仅引入通用并发与错误依赖，不会强制绑定 Tokio 等具体异步运行时，
//!   以避免下游默认启用时形成“幽灵依赖”，让调用方自选 runtime（如 async-std 或自定义执行器）。

extern crate alloc;

/// 应用层对象 Service 的集合入口。
///
/// - **意图说明 (Why)**：抽象出协议/业务绑定的对象层服务，便于统一挂载至信令交换机；
/// - **契约定位 (What)**：各子模块应实现 `spark_core::service::Service` 家族契约；
/// - **架构位置 (Where)**：面向业务的 Service 注册点，将通过 `core` 模块与会话流程协同。
pub mod applications;

/// 会话态管理与拓扑调度逻辑的核心入口。
///
/// - **意图说明 (Why)**：集中处理信令会话的装配、状态同步以及与路由层的交互；
/// - **契约定位 (What)**：负责协调 `spark_core` 的上下文 API 与 `spark_router::ServiceFactory` 的实例；
/// - **扩展指引 (How)**：建议采用状态机/编排器子模块进行拆分，避免核心流程膨胀。
pub mod core;

/// 错误类型与诊断信息集中声明处。
///
/// - **意图说明 (Why)**：统一描述交换机运行时可能出现的协议错误、资源异常等；
/// - **契约定位 (What)**：使用 `thiserror::Error` 派生统一向外暴露 `spark_core::SparkError` 兼容表示；
/// - **风险提示 (Trade-offs)**：应注意区分可恢复与不可恢复错误，避免误导上层重试策略。
pub mod error;
