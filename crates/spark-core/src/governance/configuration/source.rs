use alloc::{borrow::Cow, boxed::Box, vec::Vec};
use core::{
    pin::Pin,
    task::{Context, Poll},
};

use crate::{
    future::{BoxStream, Stream},
    sealed::Sealed,
};

use super::{ChangeNotification, ConfigKey, ConfigValue, ConfigurationError, ProfileId};

/// 配置源的元数据。
///
/// ### 设计目的（Why）
/// - 记录来源名称、优先级等信息，帮助排查冲突与审计。
/// - 吸收 Envoy xDS 资源版本（resource version）的概念，通过 `version` 字段实现增量对比。
///
/// ### 契约说明（What）
/// - `name`：来源的稳定标识，例如 `file:///etc/app/config.yaml`。
/// - `priority`：数值越大优先级越高，决定合并顺序。
/// - `version`：可选版本号，变更时必须递增或变化。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SourceMetadata {
    pub name: Cow<'static, str>,
    pub priority: u16,
    pub version: Option<Cow<'static, str>>,
}

impl SourceMetadata {
    /// 构造函数，供配置源实现者快速返回元数据。
    pub fn new<N>(name: N, priority: u16, version: Option<Cow<'static, str>>) -> Self
    where
        N: Into<Cow<'static, str>>,
    {
        Self {
            name: name.into(),
            priority,
            version,
        }
    }
}

/// 单个配置层。
///
/// ### 设计目的（Why）
/// - 在 Layer 模型中，每个数据源返回一个逻辑层，便于后续合并时进行优先级排序。
/// - 提供 `metadata` 与 `entries`，与 AWS AppConfig、Consul Watch 的 `items + revision` 对齐。
///
/// ### 契约说明（What）
/// - `entries`：由配置键值对组成。若同一键出现多次，以最后一次为准。
/// - `metadata`：描述该层的来源、版本等信息。
#[derive(Clone, Debug, PartialEq)]
pub struct ConfigurationLayer {
    pub metadata: SourceMetadata,
    pub entries: Vec<(ConfigKey, ConfigValue)>,
}

/// 配置增量事件。
///
/// ### 设计目的（Why）
/// - 将配置源产生的变更统一抽象为“增量”与“全量刷新”两种语义，便于构建上层热更新管道。
/// - 兼容文件轮询、远程推送等多种来源，实现者只需在适当时机发送对应事件。
///
/// ### 架构定位（How）
/// - `Change`：承载一次 [`ChangeNotification`]，通常来源于增量推送或差异计算。
/// - `Refresh`：表示底层源需要替换全部 [`ConfigurationLayer`]，常见于文件重新加载。
///
/// ### 契约说明（What）
/// - 事件必须按发生顺序发送；调用方需保证同一来源的有序性。
/// - `Refresh` 事件中的 `layers` 应包含完整层列表，调用方接收到后需丢弃旧层。
#[derive(Clone, Debug, PartialEq)]
pub enum ConfigDelta {
    Change(ChangeNotification),
    Refresh(Vec<ConfigurationLayer>),
}

/// 空实现的配置流，用于不支持热更新的数据源。
///
/// ### 设计目的（Why）
/// - 提供零成本的默认流，实现 `watch` 返回值要求的 `Stream` 契约。
/// - 避免调用方必须为每个静态源手写“空流”样板代码，降低实现负担。
///
/// ### 行为说明（How）
/// - `poll_next` 永远返回 `Poll::Ready(None)`，表示流立即结束。
/// - 类型实现 `Send + Sync`，可安全跨线程共享，兼容所有源实现。
pub struct NoopConfigStream;

impl NoopConfigStream {
    /// 构造空流实例。
    pub const fn new() -> Self {
        Self
    }
}

impl Default for NoopConfigStream {
    fn default() -> Self {
        Self::new()
    }
}

impl Stream for NoopConfigStream {
    type Item = ConfigDelta;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Ready(None)
    }
}

/// 配置源契约。
///
/// ### 设计目的（Why）
/// - 抽象不同后端（文件、环境变量、远程配置中心）的加载与监听能力。
/// - 兼容 Envoy xDS 的请求/响应模型，同时适应 Rust `no_std` 场景。
///
/// ### 逻辑解析（How）
/// - `load`：按 Profile 返回完整配置层列表。
/// - `watch`：返回增量通知的流式接口，可选实现。
///
/// ### 契约说明（What）
/// - **前置条件**：`profile` 必须是源支持的档案，否则返回 `ConfigurationError::Validation`。
/// - **后置条件**：`load` 成功时必须至少返回一个 Layer；若无数据，可返回空向量但须保持元数据一致。
/// - 返回的流必须遵循线程安全语义：实现者需保证在并发环境下也能安全地 `poll_next` 或触发唤醒。
///
/// ### 设计权衡（Trade-offs）
/// - 使用 `Vec` 而非 `Iterator`，简化 FFI 场景下的跨语言传递。
/// - `watch` 默认返回空流，避免对不支持热更新的数据源施加负担。
///
/// # 线程安全与生命周期说明
/// - Trait 仅要求 `Send + Sync`，刻意**不**附加 `'static`：配置源通常与底层连接句柄或缓存绑定，其生命周期可能短于进程；
/// - 若需要跨进程级全局共享，可配合 `boxed_static_source` 借用适配器在 Builder 侧托管 `'static` 引用。
pub trait ConfigurationSource: Send + Sync + Sealed {
    /// 热更新流的具体类型。
    ///
    /// ### 设计动机（Why）
    /// - 允许实现者暴露最贴近底层的流类型（如 `ReceiverStream`、`WatchStream`），在高频变更场景降低装箱与虚调用开销；
    /// - 与 [`watch`](ConfigurationSource::watch) 的生命周期绑定，支持在流中安全捕获对 `self` 的借用。
    type Stream<'a>: Stream<Item = ConfigDelta> + Send + 'a
    where
        Self: 'a;

    /// 返回指定 Profile 的配置层集合。
    fn load(
        &self,
        profile: &ProfileId,
    ) -> crate::Result<Vec<ConfigurationLayer>, ConfigurationError>;

    /// 订阅增量通知。
    fn watch<'a>(
        &'a self,
        profile: &ProfileId,
    ) -> crate::Result<Self::Stream<'a>, ConfigurationError>;

    /// 将实现者返回的流装箱为统一的 [`BoxStream`]，供多路聚合使用。
    fn watch_boxed<'a>(
        &'a self,
        profile: &ProfileId,
    ) -> crate::Result<BoxStream<'a, ConfigDelta>, ConfigurationError>
    where
        Self::Stream<'a>: Sized,
    {
        Ok(Box::pin(self.watch(profile)?))
    }
}

/// 对象安全的配置源包装，供 Builder 储存与调度。
pub trait DynConfigurationSource: Send + Sync + Sealed {
    /// 对应 [`ConfigurationSource::load`] 的对象安全版本。
    fn load_dyn(
        &self,
        profile: &ProfileId,
    ) -> crate::Result<Vec<ConfigurationLayer>, ConfigurationError>;

    /// 对应 [`ConfigurationSource::watch_boxed`] 的对象安全版本。
    fn watch_dyn<'a>(
        &'a self,
        profile: &ProfileId,
    ) -> crate::Result<BoxStream<'a, ConfigDelta>, ConfigurationError>;
}

impl<T> DynConfigurationSource for T
where
    T: ConfigurationSource,
    for<'a> T::Stream<'a>: Sized,
{
    fn load_dyn(
        &self,
        profile: &ProfileId,
    ) -> crate::Result<Vec<ConfigurationLayer>, ConfigurationError> {
        ConfigurationSource::load(self, profile)
    }

    fn watch_dyn<'a>(
        &'a self,
        profile: &ProfileId,
    ) -> crate::Result<BoxStream<'a, ConfigDelta>, ConfigurationError> {
        ConfigurationSource::watch_boxed(self, profile)
    }
}

/// 将 `'static` 配置源引用转换为拥有型 `Box`，用于桥接借用/拥有双入口。
pub(crate) fn boxed_static_source(
    source: &'static dyn DynConfigurationSource,
) -> Box<dyn DynConfigurationSource> {
    Box::new(BorrowedConfigurationSource { inner: source })
}

struct BorrowedConfigurationSource {
    inner: &'static dyn DynConfigurationSource,
}

impl DynConfigurationSource for BorrowedConfigurationSource {
    fn load_dyn(
        &self,
        profile: &ProfileId,
    ) -> crate::Result<Vec<ConfigurationLayer>, ConfigurationError> {
        self.inner.load_dyn(profile)
    }

    fn watch_dyn<'a>(
        &'a self,
        profile: &ProfileId,
    ) -> crate::Result<BoxStream<'a, ConfigDelta>, ConfigurationError> {
        self.inner.watch_dyn(profile)
    }
}
