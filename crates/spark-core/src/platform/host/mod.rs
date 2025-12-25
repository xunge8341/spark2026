//! Host 模块定义跨平台宿主环境必须实现的核心契约。
//!
//! # 设计导引（Why）
//! - 借鉴 gRPC、Envoy、NATS、Dapr、Wasmtime 五个成熟项目的宿主抽象，强调“环境能力 + 组件生命周期”的分层职责。
//! - `spark-core` 需要能够被嵌入到多语言、多操作系统的宿主之中，因此统一宿主契约是跨平台通信栈的基石。
//!
//! # 模块分层（How）
//! - [`capability`]：声明宿主可以暴露的网络、安全、限流等硬性能力，用于运行时协商。
//! - [`context`]：描述宿主元数据，作为所有回调的输入上下文。
//! - [`component`]：定义组件注册与实例化流程，让业务模块与宿主解耦。
//! - [`lifecycle`]：给出宿主启动、就绪、下线的生命周期钩子，吸纳 Envoy Hot Restart、Kubernetes Liveness 等最佳实践。
//! - [`provisioning`]：定义配置下发与动态变更契约，参考 Dapr Configuration API 以及 Service Mesh xDS 模型。
//!
//! # 命名约定（Trade-offs）
//! - 避免出现 `Spark` 前缀，保持对宿主实现的中性态度，方便复用到其他项目。
//! - 不引入项目外的非共识缩写，全部类型命名尽量与业内主流术语对齐（如 Capability、Component、Lifecycle 等）。

pub mod capability;
pub mod component;
pub mod context;
pub mod lifecycle;
pub mod provisioning;
pub mod shutdown;

pub use capability::{
    CapabilityDescriptor, CapabilityLevel, NetworkAddressFamily, NetworkProtocol, SecurityFeature,
    ThroughputClass,
};
pub use component::{ComponentDescriptor, ComponentFactory, ComponentHealthState, ComponentKind};
pub use context::{HostContext, HostMetadata, HostRuntimeProfile};
pub use lifecycle::{HostLifecycle, ShutdownReason, StartupPhase};
pub use provisioning::{
    ConfigChange, ConfigConsumer, ConfigEnvelope, ConfigQuery, ProvisioningOutcome,
};
pub use shutdown::{
    GracefulShutdownRecord, GracefulShutdownReport, GracefulShutdownStatus, GracefulShutdownTarget,
};
