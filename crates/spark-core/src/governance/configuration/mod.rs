//! Configuration API 契约模块。
//!
//! # 设计目标概述
//! - 提供跨平台、协议无关的配置抽象，以支撑通信框架的核心组件。
//! - 通过分层（Layered）、可观察（Observable）、与环境感知（Profile-aware）的模型，吸收业界领先框架（如 Envoy、gRPC、NATS、Redis 企业版、Consul）
//!   的设计经验，并结合 Rust 安全抽象进行转译。
//! - 模块拆分以便演进：`key` 负责命名契约、`value` 抽象配置值、`profile` 表达环境、`source` 与 `builder`
//!   提供加载与组装、`change` 描述变更事件、`error` 统一错误。
//!
//! # 使用路线图
//! 1. 业务侧定义一组 [`ConfigKey`] 常量，绑定业务域。
//! 2. 注册一个或多个实现 [`ConfigurationSource`] 的数据源（例如静态文件、集中式配置中心）。
//! 3. 使用 [`ConfigurationBuilder`] 聚合多源配置，
//!    最终得到 [`ConfigurationHandle`] 供运行时组件读取与监听。

mod builder;
mod change;
mod error;
mod events;
mod events_runtime;
mod key;
mod profile;
mod snapshot;
mod source;
mod value;

pub use builder::{
    BuildError, BuildErrorKind, BuildErrorStage, BuildOutcome, BuildReport, ConfigurationBuilder,
    ConfigurationHandle, ConfigurationUpdate, ConfigurationUpdateKind, ConfigurationWatch,
    LayeredConfiguration, ResolvedConfiguration, ValidationFinding, ValidationReport,
    ValidationState,
};
pub use change::{ChangeEvent, ChangeNotification, ChangeSet};
pub use error::{ConfigurationError, ConfigurationErrorKind, SourceRegistrationError};
pub use events::*;
pub use events_runtime::{
    DriftAggregationContext, DriftAggregationError, DriftAggregationOutcome, DriftNodeReport,
    aggregate_configuration_drift,
};
pub(crate) use key::ConfigKeyRepr;
pub use key::{ConfigKey, ConfigScope};
pub use profile::{ProfileDescriptor, ProfileId, ProfileLayering};
pub use snapshot::{
    ConfigurationSnapshot, SnapshotEntry, SnapshotLayer, SnapshotMetadata, SnapshotProfile,
    SnapshotValue,
};
pub use source::{
    ConfigDelta, ConfigurationLayer, ConfigurationSource, DynConfigurationSource, NoopConfigStream,
    SourceMetadata,
};
pub(crate) use value::ConfigValueRepr;
pub use value::{ConfigMetadata, ConfigValue};
