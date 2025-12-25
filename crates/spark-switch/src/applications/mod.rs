//! # applications 模块说明
//!
//! ## 设计定位（Why）
//! - 聚合所有对象层 Service 的实现，以适配不同信令协议或业务场景；
//! - 为 `core` 模块提供可热插拔的服务列表，实现路由与会话调度分离。
//!
//! ## 扩展提示（How）
//! - 建议按协议或业务域拆分子模块，例如 `sip`, `webrtc` 等；
//! - 每个子模块应暴露构造器或 `ServiceFactory`，以便被交换机注册。
//!
//! ## 契约边界（What）
//! - 需要实现 `spark_core::service::Service` 族契约；
//! - 如需共享状态，优先通过 `core` 模块提供的会话管理接口获取，以避免状态散落。

#[cfg(feature = "std")]
pub mod location;

#[cfg(feature = "std")]
pub mod registrar;

#[cfg(feature = "std")]
pub mod proxy;
