#![cfg_attr(not(feature = "std"), no_std)]
#![deny(unsafe_code)]
#![allow(clippy::result_large_err)]
#![allow(private_bounds)]
#![doc = "spark-core: 高性能、协议无关、分布式原生的异步通信框架核心契约。"]
#![doc = ""]
#![doc = "== 兼容性与版本治理 (P1.10) =="]
#![doc = "本 Crate 遵守语义化版本 2.0 (SemVer)。"]
#![doc = "1. 破坏性变更 (Breaking Change): 仅允许在 MAJOR 版本（如 11.x -> 12.0）中引入。"]
#![doc = "2. 弃用 (Deprecation): API 弃用必须至少提前 1 个 MINOR 版本（如 11.2 弃用，11.3 保留，11.4 可移除）公告，并保留运行时告警。"]
#![doc = "3. 契约测试: 任何对 `spark-core` 契约的实现或变更，必须同步更新 `spark-contract-tests` 并确保 100% 通过。"]
#![doc = ""]
#![doc = "== 内存分配依赖 (P2.4) =="]
#![doc = "`spark-core` 目前定位于 `no_std + alloc` 场景：核心契约大量依赖 [`alloc`] 中的 `Box`、`Arc`、`Vec` 等类型来支撑 Pipeline 事件分发、缓冲池租借与异步运行时对象安全。"]
#![doc = "纯 `no_std`（无分配器）环境暂不支持；若在无堆平台使用，需由调用方提供等价的内存与调度设施。最新的可行性研究（参见 docs/no-std-compatibility-report.md）已探索通过泛型化消息体、外部 Arena Trait、静态容量容器等思路，引入以 feature flag 控制的“极简契约”作为长期演进方向。现阶段该能力仍处于调研期，我们会在确定迁移策略后再发布实验性接口。"]

extern crate alloc;

/// 教案级说明：`alloc` 绑定策略。
///
/// # 意图（Why）
/// - `spark-core` 的所有异步契约都依赖 `Vec`、`Box`、`Arc` 等堆分配结构；
/// - 在 `no_std` 轨道中仍需维持这些能力，否则 Pipeline/Router trait 将无法对象安全。
///
/// # 合同（What）
/// - 无论 Feature 开关如何，均显式引入 [`alloc`] 命名空间，确保 `cargo check -p spark-core --no-default-features`
///   时依旧具备分配器符号；
/// - 上层若显式关闭 `alloc` Feature，只是避免递归地向依赖传播，但本 crate 仍然要求编译目标提供分配器。
///
/// # 执行逻辑（How）
/// - 通过 `extern crate alloc;` 直接暴露堆分配 API；
/// - `std` Feature 只是增量开启时间、IO 等扩展模块，与 `alloc` 引入解耦。
///
/// # 注意事项（Trade-offs）
/// - 这样做牺牲了“纯 no_std（无堆）”场景，但换取了与现有调用方完全兼容的最小改动；
/// - 若未来提供真正的无堆实现，需要重新拆分模块并在文档中更新使用约束。
pub use async_trait::async_trait;
pub use spark_macros::service;

mod macros;

/// 框架级过程宏命名空间。
///
/// # 设计目标（Why）
/// - 统一向外暴露 `#[spark::service]` 等过程宏，避免业务代码直接依赖内部 crate 名称；
/// - 将宏视为框架 API 的一部分，便于在文档和教程中使用 `use spark_core as spark;` 的统一写法。
///
/// # 使用方式（How）
/// - 在调用方执行 `use spark_core as spark;` 后，可直接书写 `#[spark::service]`；
/// - 若未来增加更多过程宏，只需在此模块追加 re-export 即可。
pub mod spark {
    pub use spark_macros::service;
}

pub mod kernel;
pub use kernel::{arc_swap, common, context, contract, future, ids, model, sealed, status, types};

pub mod governance;
pub use governance::timeout::profile as config;
pub use governance::{
    audit, configuration, deprecation, limits, observability, retry, security, timeout,
};

pub mod data_plane;
pub use data_plane::{buffer, codec, initializer, pipeline, protocol, service, transport};

pub mod router;
pub use router::{
    DynRouter, MetadataKey, MetadataValue, RouteBindingObject, RouteCatalog, RouteDecisionObject,
    RouteDescriptor, RouteError, RouteId, RouteKind, RouteMetadata, RoutePattern, RouteSegment,
    RoutingContext, RoutingIntent, RoutingSnapshot,
};

pub mod platform;
#[cfg(feature = "std")]
pub use platform::time;
pub use platform::{cluster, host, runtime};

#[cfg(feature = "compat_v0")]
pub mod compat;
pub mod error;
pub mod prelude;
/// 测试桩命名空间，集中暴露框架官方维护的 `Noop`/`Mock` 实现，供集成测试与示例复用。
///
/// # 设计背景（Why）
/// - 统一维护常见桩对象，避免在各处重复定义零尺寸结构体；
/// - 当核心契约演进时，通过单点更新保证所有测试同步适配。
///
/// # 使用方式（How）
/// - 通过 `use spark_core::test_stubs::observability::*;` 等语句引入需要的桩类型；
/// - 所有桩对象在 `no_std + alloc` 环境同样可用，便于运行最小化集成测试。
pub mod test_stubs;

pub use audit::{
    AuditActor, AuditChangeEntry, AuditChangeSet, AuditContext, AuditDeletedEntry, AuditEntityRef,
    AuditError, AuditEventV1, AuditPipeline, AuditRecorder, AuditStateHasher, AuditTag,
    TsaEvidence,
};
pub use buffer::{
    BufView, BufferAllocator, BufferPool, Bytes, Chunks, ErasedSparkBuf, ErasedSparkBufMut,
    PipelineMessage, PoolStatDimension, PoolStats, ReadableBuffer, UserMessage, WritableBuffer,
};
pub use cluster::{
    ClusterConsistencyLevel, ClusterEpoch, ClusterMembership, ClusterMembershipEvent,
    ClusterMembershipScope, ClusterMembershipSnapshot, ClusterNodeProfile, ClusterNodeState,
    ClusterRevision, ClusterScopeSelector, DiscoveryEvent, DiscoverySnapshot, FlowControlMode,
    NodeId, OverflowPolicy, RoleDescriptor, ServiceDiscovery, ServiceInstance, ServiceName,
    SubscriptionFlowControl, SubscriptionQueueProbe, SubscriptionQueueSnapshot, SubscriptionStream,
};
pub use codec::{
    Codec, CodecDescriptor, CodecRegistry, ContentEncoding, ContentType, DecodeContext,
    DecodeOutcome, DynCodec, DynCodecFactory, EncodeContext, EncodedPayload, Encoder,
    NegotiatedCodec, SchemaDescriptor, TypedCodecAdapter, TypedCodecFactory,
};
/// 重新导出 `common` 模块的核心契约与临时兼容函数。
///
/// - `legacy_loopback_outbound` 已进入弃用流程，但在两版过渡期内仍需暴露；
///   因此使用 `#[allow(deprecated)]` 抑制内部警告，保留对外提示能力。
#[allow(deprecated)]
pub use common::{Empty, IntoEmpty, Loopback, legacy_loopback_outbound};
pub use config::{Timeout, TimeoutProfile};
pub use configuration::{
    BuildError, BuildErrorKind, BuildErrorStage, BuildOutcome, BuildReport, ChangeEvent,
    ChangeNotification, ChangeSet, ConfigDelta, ConfigKey, ConfigMetadata, ConfigScope,
    ConfigValue, ConfigurationBuilder, ConfigurationError, ConfigurationHandle, ConfigurationLayer,
    ConfigurationSnapshot, ConfigurationSource, ConfigurationUpdate, ConfigurationUpdateKind,
    ConfigurationWatch, DynConfigurationSource, LayeredConfiguration, NoopConfigStream,
    ProfileDescriptor, ProfileId, ProfileLayering, ResolvedConfiguration, SnapshotEntry,
    SnapshotLayer, SnapshotMetadata, SnapshotProfile, SnapshotValue, SourceMetadata,
    ValidationFinding, ValidationReport, ValidationState,
};
pub use context::Context;
pub use contract::{
    BackpressureSignal, CallContext, CallContextBuilder, Cancellation, ContractStateMachine,
    DEFAULT_OBSERVABILITY_CONTRACT, Deadline, ObservabilityContract, SecurityContextSnapshot,
    ShutdownGraceful, ShutdownImmediate, StateAdvance,
};
pub use error::{
    CoreError, DomainError, DomainErrorKind, ErrorCategory, ErrorCause, ImplError, ImplErrorKind,
    IntoCoreError, IntoDomainError, Result, SparkError,
};
pub use future::{BoxFuture, BoxStream, LocalBoxFuture, Stream};
pub use host::{
    CapabilityDescriptor, CapabilityLevel, ComponentDescriptor, ComponentFactory,
    ComponentHealthState, ComponentKind, ConfigChange, ConfigConsumer, ConfigEnvelope, ConfigQuery,
    HostContext, HostLifecycle, HostMetadata, HostRuntimeProfile, NetworkAddressFamily,
    NetworkProtocol, ProvisioningOutcome, SecurityFeature, ShutdownReason, StartupPhase,
    ThroughputClass,
};
pub use ids::{CorrelationId, IdempotencyKey, RequestId};
pub use limits::{
    LimitAction, LimitConfigError, LimitDecision, LimitMetricsHook, LimitPlan, LimitSettings,
    ResourceKind, config_error_to_spark, decision_queue_snapshot,
};
pub use model::{State, Status};
pub use observability::{
    ApplicationEvent, AttributeKey, AttributeSet, ComponentHealth, CoreUserEvent, Counter,
    EventPolicy, Gauge, HealthCheckProvider, HealthChecks, HealthState, Histogram, IdleDirection,
    IdleTimeout, InstrumentDescriptor, KeyValue, LogField, LogRecord, LogSeverity, Logger,
    MetricAttributeValue, MetricsProvider, ObservabilityFacade, OpsEvent, OpsEventBus,
    OpsEventKind, OwnedAttributeSet, OwnedResourceAttrs, RateDirection, RateLimited, ResourceAttr,
    ResourceAttrSet, SpanId, TlsInfo, TraceContext, TraceContextError, TraceFlags, TraceId,
    TraceState, TraceStateEntry, TraceStateError,
};
pub use pipeline::{
    ChainBuilder, Channel, ChannelState, Context as PipelineContext, Controller, ControllerEvent,
    ControllerEventKind, ControllerFactory, ControllerHandle, DuplexHandler, DynControllerFactory,
    DynControllerFactoryAdapter, ExtensionsMap, HandlerRegistry, InboundHandler,
    InitializerDescriptor, OutboundHandler, Pipeline, PipelineEvent, PipelineEventKind,
    PipelineFactory, PipelineHandle, PipelineHandleId, PipelineInitializer, WriteSignal,
};
pub use protocol::{Event, Frame, Message};
pub use runtime::{
    AsyncRuntime, BlockingTaskSubmission, CoreServices, JoinHandle, LocalTaskSubmission,
    ManagedBlockingTask, ManagedLocalTask, ManagedSendTask, MonotonicTimePoint, SendTaskSubmission,
    SloPolicyAction, SloPolicyConfigError, SloPolicyDirective, SloPolicyManager,
    SloPolicyReloadReport, SloPolicyRule, SloPolicyTrigger, TaskCancellationStrategy, TaskError,
    TaskExecutor, TaskHandle, TaskLaunchOptions, TaskPriority, TaskResult, TimeDriver,
    TimeoutConfigError, TimeoutRuntimeConfig, TimeoutSettings, slo_policy_table_key,
};
pub use security::{
    Credential, CredentialDescriptor, CredentialMaterial, CredentialScope, CredentialState,
    IdentityDescriptor, IdentityKind, IdentityProof, KeyMaterial, KeyPurpose, KeyRequest,
    KeyResponse, KeyRetrievalError, KeySource, NegotiationContext, NegotiationError,
    NegotiationOutcome, NegotiationResult, PolicyAttachment, PolicyEffect, PolicyRule,
    ResourcePattern, SecurityClass, SecurityNegotiationPlan, SecurityNegotiator, SecurityPolicy,
    SecurityProtocol, SecurityProtocolOffer, SubjectMatcher,
};
pub use service::{
    AutoDynBridge, BoxService, ClientFactory, Decode, DynService, Encode, Layer, Service,
    ServiceObject, type_mismatch_error,
};
pub use types::{
    Budget, BudgetDecision, BudgetKind, BudgetSet, BudgetSnapshot, CloseReason, NonEmptyStr,
};
#[rustfmt::skip]
pub use status::{
    BusyReason,
    PollReady,
    ReadyCheck,
    ReadyState,
    RetryAdvice,
    RetryRhythm,
    SubscriptionBudget,
};
#[cfg(feature = "std")]
pub use status::RetryAfterThrottle;
pub use transport::{
    AddressFamily, AvailabilityRequirement, BackpressureDecision, BackpressureMetrics, Capability,
    CapabilityBitmap, ConnectionIntent, DatagramEndpoint, DowngradeReport, DynServerChannel,
    DynTransportFactory, Endpoint, EndpointKind, HandshakeError, HandshakeErrorKind,
    HandshakeOffer, HandshakeOutcome, ListenerConfig, ListenerShutdown, NegotiationAuditContext,
    QualityOfService, SecurityMode, ServerChannel, ServerChannelObject, SessionLifecycle,
    TransportBuilder, TransportFactory, TransportFactoryObject, TransportParams,
    TransportSocketAddr, Version, describe_shutdown_target, negotiate,
};

use alloc::boxed::Box;
use core::fmt;

/// `spark-core` 中所有错误必须实现的 `no_std` 基础 Trait。
///
/// # 设计背景（Why）
/// - `std::error::Error` 在 `no_std` 环境中不可用，因此我们需要一个对象安全、与平台无关的错误抽象来串联底层错误链。
/// - 该 Trait 作为所有错误类型的“最小公共接口”，帮助框架在 `alloc` 场景下完成跨模块错误传递。
///
/// # 逻辑解析（How）
/// - 约束实现者提供 `Debug` 与 `Display`，便于日志与可观测性收集。
/// - 通过 `source` 方法递归返回链路上的上游错误，保持与 `std::error::Error::source` 一致的语义，从而兼容现有生态的错误处理约定。
///
/// # 契约说明（What）
/// - **输入/前置条件**：实现类型必须是 `'static` 生命周期并可安全跨线程共享（若需包装进 `ErrorCause`）。
/// - **返回/后置条件**：`source` 返回的引用生命周期受限于 `self`，以防悬垂引用。
///
/// # 设计取舍与风险（Trade-offs）
/// - 我们没有引入 `Send + Sync` 约束，避免对 `no_std` 设备强加多余负担；需要线程安全时请使用 `ErrorCause` 类型别名。
/// - 调用方需注意：若底层错误不提供 `source`，则错误链会在此处终止，这是设计上允许的边界情况。
pub trait Error: fmt::Debug + fmt::Display + crate::sealed::Sealed {
    /// 返回当前错误的上游来源。
    #[allow(unused_parens)]
    fn source(&self) -> Option<&(dyn Error + 'static)>;
}

impl<E> Error for Box<E>
where
    E: Error + ?Sized,
{
    #[allow(unused_parens)]
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        (**self).source()
    }
}
pub use deprecation::{DeprecationNotice, LEGACY_LOOPBACK_OUTBOUND};
