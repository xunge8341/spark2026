use super::{
    Endpoint, TransportParams,
    intent::ConnectionIntent,
    server_channel::{DynServerChannel, ServerChannel, ServerChannelObject},
};
use alloc::{boxed::Box, sync::Arc};
use core::{future::Future, time::Duration};

use crate::{
    CoreError, async_trait,
    cluster::ServiceDiscovery,
    context::Context,
    contract::CallContext,
    pipeline::factory::{
        DynPipelineFactory, DynPipelineFactoryAdapter, PipelineFactory as GenericPipelineFactory,
    },
    pipeline::{Channel as PipelineChannel, Pipeline},
    sealed::Sealed,
};

/// 监听配置，描述传输工厂如何在目标端点上暴露服务。
///
/// # 设计背景（Why）
/// - 综合 Envoy Listener、Netty ServerBootstrap、Tokio Listener Options，将常见配置项（并发、积压、参数）标准化。
/// - 支撑科研实验：`params` 可注入拥塞控制、负载调度策略标识，方便对比试验。
///
/// # 契约说明（What）
/// - `endpoint`：监听的物理端点，必须是 [`EndpointKind::Physical`](super::EndpointKind::Physical)。
/// - `params`：额外的键值参数（如 `tcp_backlog`、`quic_stateless_retry`）。
/// - `concurrency_limit`：建议的最大并发连接数，`None` 表示交由实现决定。
/// - `accept_backoff`：接受新连接的退避策略（如限流或防御攻击）。
/// - **前置条件**：`endpoint` 应包含可解析的主机与端口；调用前应完成权限校验。
/// - **后置条件**：当 `bind` 返回成功时，监听器应按照配置开始接收连接。
///
/// # 风险提示（Trade-offs）
/// - 配置未对参数合法性做严格校验，需由上层或实现方验证。
/// - 并发限制过低可能导致连接饥饿；过高则可能耗尽资源。
#[derive(Clone, Debug)]
pub struct ListenerConfig {
    endpoint: Endpoint,
    params: TransportParams,
    concurrency_limit: Option<u32>,
    accept_backoff: Option<Duration>,
}

impl ListenerConfig {
    /// 以必需的端点构造配置。
    pub fn new(endpoint: Endpoint) -> Self {
        Self {
            endpoint,
            params: TransportParams::new(),
            concurrency_limit: None,
            accept_backoff: None,
        }
    }

    /// 指定最大并发连接数。
    pub fn with_concurrency_limit(mut self, limit: u32) -> Self {
        self.concurrency_limit = Some(limit);
        self
    }

    /// 指定接受退避时间。
    pub fn with_accept_backoff(mut self, backoff: Duration) -> Self {
        self.accept_backoff = Some(backoff);
        self
    }

    /// 覆盖参数表。
    pub fn with_params(mut self, params: TransportParams) -> Self {
        self.params = params;
        self
    }

    /// 访问端点。
    pub fn endpoint(&self) -> &Endpoint {
        &self.endpoint
    }

    /// 访问参数。
    pub fn params(&self) -> &TransportParams {
        &self.params
    }

    /// 获取并发上限。
    pub fn concurrency_limit(&self) -> Option<u32> {
        self.concurrency_limit
    }

    /// 获取退避时间。
    pub fn accept_backoff(&self) -> Option<Duration> {
        self.accept_backoff
    }
}

/// 泛型层传输工厂，统一建连与监听流程。
///
/// # 教案级注释
///
/// ## 意图（Why）
/// - 内建实现依靠泛型接口避免 `BoxFuture` 与 trait object 的运行时开销；
/// - 通过与对象层适配器互转，保证插件/脚本仍可复用同一实现。
///
/// ## 逻辑（How）
/// - `scheme` 返回协议标识；
/// - `bind` 按照 [`ListenerConfig`] 与 Pipeline 工厂构造监听器，返回 [`ServerChannel`]；
/// - `connect` 基于 [`ConnectionIntent`] 建立客户端通道；
/// - 全流程与 [`Context`]、[`ServiceDiscovery`] 协同执行取消、超时与服务发现。
///
/// ## 契约（What）
/// - `Channel`/`Server` 关联类型必须满足 [`PipelineChannel`]、[`ServerChannel`] 契约；
/// - **前置条件**：调用方需保证配置与协议标识匹配；
/// - **后置条件**：返回的监听器/通道在语义上等同于对象层版本；
/// - **错误处理**：失败需返回 [`CoreError`] 并附带结构化错误码。
///
/// ## 风险提示（Trade-offs）
/// - 泛型 Future 类型通常较复杂，如需在对象层复用需配合类型擦除适配器；
/// - 建连过程中依赖 `ServiceDiscovery` 时，应处理网络分区等异常并返回语义化错误。
pub trait TransportFactory: Send + Sync + 'static + Sealed {
    /// 客户端通道类型。
    type Channel: PipelineChannel;
    /// 服务端监听器类型。
    type Server: for<'ctx> ServerChannel<
            Error = CoreError,
            Connection = Self::Channel,
            AcceptCtx<'ctx> = CallContext,
            ShutdownCtx<'ctx> = Context<'ctx>,
        >;

    /// 绑定流程返回的 Future 类型。
    type BindFuture<'a, P>: Future<Output = crate::Result<Self::Server, CoreError>> + Send + 'a
    where
        Self: 'a,
        P: GenericPipelineFactory + Send + Sync + 'static,
        P::Pipeline: Pipeline;

    /// 建连流程返回的 Future 类型。
    type ConnectFuture<'a>: Future<Output = crate::Result<Self::Channel, CoreError>> + Send + 'a
    where
        Self: 'a;

    /// 返回支持的协议标识。
    ///
    /// # 教案级注释
    ///
    /// ## 意图（Why）
    /// - 提供统一入口读取传输协议字符串，同时携带 [`Context`] 以便未来根据调用预算动态裁剪协议集合。
    ///
    /// ## 逻辑（How）
    /// - 当前实现通常直接返回静态字符串；`ctx` 仅作为占位，保持接口一致。
    ///
    /// ## 契约（What）
    /// - `ctx`：执行上下文；
    /// - 返回：协议标识。
    ///
    /// ## 考量（Trade-offs）
    /// - 统一签名避免上层特判；未来若需按预算区分主/备协议可直接利用 `ctx`。
    fn scheme(&self, ctx: &Context<'_>) -> &'static str;

    /// 根据配置绑定监听器。
    ///
    /// # 教案级注释
    ///
    /// ## 意图（Why）
    /// - 将 [`Context`] 上的取消/截止约束下沉至监听器创建过程，避免长时间阻塞资源初始化。
    ///
    /// ## 逻辑（How）
    /// - 在 Future 内部应周期性检查 `ctx` 的取消标记或剩余预算；
    /// - 其余逻辑与协议实现一致，仅多传递 `ctx`。
    ///
    /// ## 契约（What）
    /// - `ctx`：执行上下文；
    /// - `config`：监听配置；
    /// - `pipeline_factory`：Pipeline 装配工厂；
    /// - 返回：构建完成的 [`ServerChannel`]。
    ///
    /// ## 考量（Trade-offs）
    /// - 若绑定耗时，及时响应 `ctx` 可提升系统缩容/回滚时的敏捷性。
    fn bind<P>(
        &self,
        ctx: &Context<'_>,
        config: ListenerConfig,
        pipeline_factory: Arc<P>,
    ) -> Self::BindFuture<'_, P>
    where
        P: GenericPipelineFactory + Send + Sync + 'static,
        P::Pipeline: Pipeline;

    /// 建立客户端通道。
    ///
    /// # 教案级注释
    ///
    /// ## 意图（Why）
    /// - 将调用方的取消/截止/预算贯穿至建连过程，确保连接尝试在资源受限或超时时及时退出。
    ///
    /// ## 逻辑（How）
    /// - 实现应在等待 DNS、握手等步骤时关注 `ctx` 的状态，必要时取消操作。
    ///
    /// ## 契约（What）
    /// - `ctx`：执行上下文；
    /// - `intent`：连接意图；
    /// - `discovery`：可选服务发现；
    /// - 返回：满足约束的 [`PipelineChannel`]。
    ///
    /// ## 考量（Trade-offs）
    /// - 融合 `ctx` 可避免重复传参并与 Service 层保持一致。
    fn connect(
        &self,
        ctx: &Context<'_>,
        intent: ConnectionIntent,
        discovery: Option<Arc<dyn ServiceDiscovery>>,
    ) -> Self::ConnectFuture<'_>;
}

/// 对象层传输工厂，统一封装建连与监听流程。
///
/// # 教案级注释
///
/// ## 意图（Why）
/// - 控制面、脚本插件可通过对象层注册自定义传输；
/// - 与泛型层 [`TransportFactory`] 互转，满足双层语义等价目标。
///
/// ## 逻辑（How）
/// - `bind_dyn` 将对象层 Pipeline 工厂适配为泛型 [`DynPipelineFactoryAdapter`] 并类型擦除；
/// - `connect_dyn` 调用泛型实现，返回 `Box<dyn PipelineChannel>`；
/// - 通过 [`TransportFactoryObject`] 在两层之间完成互转。
///
/// ## 契约（What）
/// - 输入输出与泛型层保持一致；
/// - **错误处理**：返回结构化 [`CoreError`]，调用方据此决定重试或降级策略。
///
/// ## 风险提示（Trade-offs）
/// - 对象层存在 `Box` 分配与虚表跳转，在热路径建议优先使用泛型接口；
/// - `async_trait` 装箱路径相较泛型 Future 略有开销，但仍在延迟预算内。
#[async_trait]
pub trait DynTransportFactory: Send + Sync + Sealed {
    /// 返回支持的协议标识。
    fn scheme_dyn(&self, ctx: &Context<'_>) -> &'static str;

    /// 绑定监听器。
    async fn bind_dyn(
        &self,
        ctx: &Context<'_>,
        config: ListenerConfig,
        pipeline_factory: Arc<dyn DynPipelineFactory>,
    ) -> crate::Result<Box<dyn DynServerChannel>, CoreError>;

    /// 建立客户端通道。
    async fn connect_dyn(
        &self,
        ctx: &Context<'_>,
        intent: ConnectionIntent,
        discovery: Option<Arc<dyn ServiceDiscovery>>,
    ) -> crate::Result<Box<dyn PipelineChannel>, CoreError>;
}

/// 将泛型传输工厂适配为对象层实现。
pub struct TransportFactoryObject<F>
where
    F: TransportFactory,
{
    inner: Arc<F>,
}

impl<F> TransportFactoryObject<F>
where
    F: TransportFactory,
{
    /// 使用泛型实现构造对象层适配器。
    pub fn new(inner: F) -> Self {
        Self {
            inner: Arc::new(inner),
        }
    }

    /// 访问内部泛型实现的共享引用计数包装。
    pub fn into_inner(self) -> Arc<F> {
        self.inner
    }
}

#[async_trait]
impl<F> DynTransportFactory for TransportFactoryObject<F>
where
    F: TransportFactory,
{
    fn scheme_dyn(&self, ctx: &Context<'_>) -> &'static str {
        self.inner.scheme(ctx)
    }

    async fn bind_dyn(
        &self,
        ctx: &Context<'_>,
        config: ListenerConfig,
        pipeline_factory: Arc<dyn DynPipelineFactory>,
    ) -> crate::Result<Box<dyn DynServerChannel>, CoreError> {
        let adapter = DynPipelineFactoryAdapter::new(pipeline_factory);
        let server = TransportFactory::bind(&*self.inner, ctx, config, Arc::new(adapter)).await?;
        Ok(Box::new(ServerChannelObject::new(server)) as Box<dyn DynServerChannel>)
    }

    async fn connect_dyn(
        &self,
        ctx: &Context<'_>,
        intent: ConnectionIntent,
        discovery: Option<Arc<dyn ServiceDiscovery>>,
    ) -> crate::Result<Box<dyn PipelineChannel>, CoreError> {
        let channel = TransportFactory::connect(&*self.inner, ctx, intent, discovery).await?;
        Ok(Box::new(channel) as Box<dyn PipelineChannel>)
    }
}
