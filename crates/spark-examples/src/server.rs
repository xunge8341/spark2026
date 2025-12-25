use alloc::{boxed::Box, format, string::String, sync::Arc};

use spark_core::{
    buffer::PipelineMessage,
    contract::{CallContext, CloseReason},
    error::CoreError,
    pipeline::{
        ChainBuilder, Channel, Context as PipelineContext, InboundHandler,
        InitializerDescriptor, OutboundHandler, PipelineInitializer, WriteSignal,
    },
    runtime::CoreServices,
    service::BoxService,
    transport::{
        Capability, CapabilityBitmap, DowngradeReport, DynServerChannel, DynTransportFactory,
        HandshakeOutcome, ListenerConfig, PipelineInitializerSelector, Version,
    },
    Result as CoreResult,
};
use spark_hosting::{Host, MiddlewareRegistry, ServiceEntry, ServiceRegistry};
use spin::Mutex;

use crate::transport::TransportFactoryExt;

/// `PipelineFactory`：为对象层 [`DynPipelineFactory`](spark_core::pipeline::DynPipelineFactory)
/// 提供语义更贴近示例场景的别名。
///
/// # 教案级说明
/// - **意图 (Why)**：隐藏 `DynPipelineFactory` 的冗长名称，提醒读者“任何对象层工厂都可以放入 `Arc<dyn PipelineFactory>`”。
/// - **逻辑 (How)**：通过空 trait 继承语法让实现自动覆盖；调用方只需引入本别名即可获得对象安全接口。
/// - **契约 (What)**：类型若已实现 `DynPipelineFactory`，即可自动满足本 trait；别名不添加任何新行为。
pub trait PipelineFactory: spark_core::pipeline::DynPipelineFactory {}

impl<T> PipelineFactory for T where T: spark_core::pipeline::DynPipelineFactory {}

/// `ExampleServer` 演示新版启动链路如何串联 `Host`、传输监听器与 Pipeline 初始
/// 化器选择逻辑。
///
/// # 教案级概览
/// - **意图 (Why)**：统一展示“构建 Host → 注册 Service/中间件 → 运行传输监听 →
///   设置协议协商 (L1) → 安装 Handler (L2/L3)”的端到端流程；
/// - **整体位置**：位于教学示例的核心入口，调用者只需构造该结构体并执行
///   [`run`](Self::run) 即可体验完整启动链路；
/// - **关键角色**：
///   - `services`：传递给 Handler 的运行时能力，如执行器、时钟等；
///   - `transport_factory`：负责根据监听配置启动底层 `ServerChannel`；
///   - `host`：提供已注册的 Service 与中间件；
///   - `full_stack_label`/`minimal_stack_label`：在 Host 中检索不同 Handler 组合的标签；
///   - `service_name`：业务 Service 在注册表中的名称；
///   - `listener`：缓存运行中的 `ServerChannel`，以便后续关闭或观测。
/// - **契约约束**：
///   - *前置条件*：`host` 内已注册 `service_name` 对应的 Service，以及两个可选的链路标签；
///   - *后置条件*：`run` 成功后，监听器已设置好 L1 协商闭包并处于就绪状态；
/// - **权衡 (Trade-offs)**：示例使用 `spin::Mutex` 以兼容 `no_std + alloc` 环境；对热路径
///   影响有限但在高并发场景需注意自旋锁占用 CPU。
pub struct ExampleServer {
    services: CoreServices,
    transport_factory: Arc<dyn DynTransportFactory>,
    config: ListenerConfig,
    host: Host,
    full_stack_label: String,
    minimal_stack_label: String,
    service_name: String,
    listener: Mutex<Option<Box<dyn DynServerChannel>>>,
}

impl ExampleServer {
    /// 构造 `ExampleServer`，在启动前绑定所有必需的运行时资源。
    ///
    /// # 教案级注释
    /// - **意图 (Why)**：强调“构造阶段注入依赖，运行阶段专注执行”；
    /// - **逻辑 (How)**：简单存储传入参数，并将链路标签/服务名称转换为 `String` 以利日志输出；
    /// - **契约 (What)**：
    ///   - *参数*：
    ///     - `services`：`CoreServices` 聚合体；
    ///     - `transport_factory`：对象层传输工厂；
    ///     - `config`：监听配置；
    ///     - `host`：已完成注册的宿主；
    ///     - `full_stack_label` / `minimal_stack_label`：在 Host 中查找 Pipeline 初始化器的标签；
    ///     - `service_name`：业务 Service 的注册名；
    ///   - *前置条件*：调用方需确保标签与服务已注册；
    ///   - *后置条件*：返回的实例即可用于 [`run`](Self::run)。
    /// - **权衡**：构造函数不做额外校验，以免掩盖示例重点；真实系统应在此阶段验证配置完整性。
    pub fn new(
        services: CoreServices,
        transport_factory: Arc<dyn DynTransportFactory>,
        config: ListenerConfig,
        host: Host,
        full_stack_label: impl Into<String>,
        minimal_stack_label: impl Into<String>,
        service_name: impl Into<String>,
    ) -> Self {
        Self {
            services,
            transport_factory,
            config,
            host,
            full_stack_label: full_stack_label.into(),
            minimal_stack_label: minimal_stack_label.into(),
            service_name: service_name.into(),
            listener: Mutex::new(None),
        }
    }

    /// 启动传输监听器并注入协议协商逻辑（L1 路由）。
    ///
    /// # 教案级注释
    /// - **意图 (Why)**：演示如何将握手结果映射为不同的 `PipelineInitializer`，并在启动时绑定到 `ServerChannel`；
    /// - **逻辑 (How)**：
    ///   1. 通过 `CallContext` 获取执行上下文；
    ///   2. 调用传输工厂的 [`listen`](TransportFactoryExt::listen) 启动监听；
    ///   3. 使用 [`InitializerSelectorContext`] 从 Host 提取初始化器与 Service；
    ///   4. 调用 `set_initializer_selector_dyn` 完成 L1 协商闭包装配；
    ///   5. 将监听器句柄缓存至 `listener`，方便后续关闭或观测；
    /// - **契约 (What)**：
    ///   - *参数*：`pipeline_factory` 为传输实现构建 Pipeline 所需的对象层工厂；
    ///   - *返回*：`CoreResult<()>`，失败时传播底层 `CoreError`；
    ///   - *前置条件*：宿主已注册必要组件；
    ///   - *后置条件*：监听器完成协议协商闭包设置并缓存成功。
    /// - **风险与权衡**：示例在缺少依赖时使用 `panic!` 直观暴露问题，生产环境应改为结构化错误。
    pub async fn run(&self, pipeline_factory: Arc<dyn PipelineFactory>) -> CoreResult<()> {
        let call_context = CallContext::builder().build();
        let execution = call_context.execution();

        let mut listener = self
            .transport_factory
            .listen(&execution, self.config.clone(), pipeline_factory)
            .await?;

        let selectors = InitializerSelectorContext::new(
            &self.host,
            &self.full_stack_label,
            &self.minimal_stack_label,
            &self.service_name,
        );

        let selector = selectors.into_selector();
        listener.set_initializer_selector_dyn(selector);

        let mut slot = self.listener.lock();
        *slot = Some(listener);
        Ok(())
    }

    /// 暴露内部的 `CoreServices`，方便测试或示例代码读取运行时能力。
    pub fn services(&self) -> &CoreServices {
        &self.services
    }
}

/// `InitializerSelectorContext` 封装从 Host 抽取初始化器与 Service 的细节，并构造协议
/// 协商所需的闭包。
///
/// # 教案级说明
/// - **意图 (Why)**：在监听器启动阶段一次性收集所有依赖，避免闭包运行时再访问注册表造成竞争；
/// - **体系位置**：服务于 [`ExampleServer::run`]，负责 L1 路由的准备工作；
/// - **关键流程 (How)**：
///   1. 解析业务 Service；
///   2. 基于标签查找或即时构造 `MyInitializer`；
///   3. 生成根据能力位图选择初始化器的闭包；
/// - **契约 (What)**：
///   - *前置条件*：Host 至少注册了 `service_name` 对应的 Service；
///   - *后置条件*：[`into_selector`](Self::into_selector) 返回的闭包可在线程间安全共享；
/// - **权衡**：若标签缺失，示例会即时构造默认初始化器，牺牲配置显式性换取教学连贯性。
struct InitializerSelectorContext {
    full_stack: Arc<dyn PipelineInitializer>,
    minimal_stack: Arc<dyn PipelineInitializer>,
}

impl InitializerSelectorContext {
    fn new(host: &Host, full_label: &str, minimal_label: &str, service_name: &str) -> Self {
        let service = resolve_service_instance(host.services(), service_name);
        let full_stack = resolve_initializer(host.middleware(), full_label, service.clone(), true);
        let minimal_stack = resolve_initializer(host.middleware(), minimal_label, service, false);

        Self {
            full_stack,
            minimal_stack,
        }
    }

    fn into_selector(self) -> Arc<PipelineInitializerSelector> {
        let Self {
            full_stack,
            minimal_stack,
        } = self;

        let mut requires_router = CapabilityBitmap::empty();
        requires_router.insert(Capability::MULTIPLEXING);

        Arc::new(move |outcome: &HandshakeOutcome| {
            if requires_router.is_subset_of(outcome.capabilities()) {
                Ok(Arc::clone(&full_stack))
            } else {
                Ok(Arc::clone(&minimal_stack))
            }
        })
    }
}

/// 根据标签从宿主的中间件注册表解析或构造 `PipelineInitializer`。
fn resolve_initializer(
    registry: &MiddlewareRegistry,
    label: &str,
    service: BoxService,
    enable_router: bool,
) -> Arc<dyn PipelineInitializer> {
    if let Some(initializer) = registry.get(label) {
        Arc::clone(initializer)
    } else {
        Arc::new(MyInitializer::new(
            format!(
                "spark.examples.pipeline.{}",
                if enable_router { "full" } else { "minimal" }
            ),
            enable_router,
            service,
        ))
    }
}

/// 根据服务名称解析业务 Service 实例。
fn resolve_service_instance(registry: &ServiceRegistry, name: &str) -> BoxService {
    match registry.get(name) {
        Some(ServiceEntry::Instance(service)) => service.clone(),
        Some(ServiceEntry::Factory(factory)) => factory
            .create()
            .expect("service factory returned error in spark example"),
        None => panic!("missing service `{name}` in host service registry"),
    }
}

/// `MyInitializer` 演示如何在 `configure` 阶段串联 Codec、可选 L2 Router 与 Service 适配器。
/// 当启用 `std` 时，路由阶段直接复用 `spark_router::pipeline::ApplicationRouter`，以便向读者
/// 展示真实的 Handler 安装流程；在 `no_std` 场景下则回退为教学桩以维持文档连贯性。
///
/// # 教案级注释
/// - **意图 (Why)**：强调 PipelineInitializer 的职责是“描述并装配 Handler 链”；
/// - **体系位置**：被协议协商闭包选中后在连接建立时执行；
/// - **契约 (What)**：
///   - *构造参数*：`label` 标识链路，`enable_router` 控制是否安装 L2 Handler，`service` 为业务入口；
///   - *前置条件*：`service` 可在多线程环境下安全克隆；
///   - *后置条件*：`configure` 成功后 Pipeline 完成 Codec → (Router) → Service Adapter 装配。
struct MyInitializer {
    descriptor: InitializerDescriptor,
    enable_router: bool,
    service: BoxService,
}

impl MyInitializer {
    fn new(label: impl Into<String>, enable_router: bool, service: BoxService) -> Self {
        let name = label.into();
        let descriptor = InitializerDescriptor::new(
            name.clone(),
            if enable_router { "routing" } else { "codec" }.into(),
            if enable_router {
                "全功能链路：编解码 + 应用路由 + Service 适配"
            } else {
                "精简链路：仅包含编解码与 Service 适配"
            }
            .into(),
        );

        Self {
            descriptor,
            enable_router,
            service,
        }
    }
}

impl PipelineInitializer for MyInitializer {
    fn descriptor(&self) -> InitializerDescriptor {
        self.descriptor.clone()
    }

    fn configure(
        &self,
        chain: &mut dyn ChainBuilder,
        _channel: &dyn Channel,
        _services: &CoreServices,
    ) -> CoreResult<(), CoreError> {
        let codec_descriptor = InitializerDescriptor::new(
            "spark.examples.codec",
            "codec",
            "负责协议层编解码并桥接 Pipeline 消息格式",
        );
        let codec_handler = CodecHandler::new(codec_descriptor.clone());
        chain.register_inbound("codec.inbound", Box::new(codec_handler.clone()));
        chain.register_outbound("codec.outbound", Box::new(codec_handler));


        let router_flow = if self.enable_router {
            let router_descriptor = InitializerDescriptor::new(
                "spark.examples.router",
                "routing",
                "根据帧头元数据选择业务路由",
            );
            Some(router_stage::register_router(
                chain,
                router_descriptor,
                self.service.clone(),
            ))
        } else {
            None
        };

        if !matches!(
            router_flow,
            Some(router_stage::RouterFlow::ConsumesInbound)
        ) {
            let service_descriptor = InitializerDescriptor::new(
                "spark.examples.service_adapter",
                "service",
                "将 Pipeline 消息交给业务 Service，并在完成后继续写出响应",
            );
            chain.register_inbound(
                "service.adapter",
                Box::new(ServiceAdapterHandler::new(
                    service_descriptor,
                    self.service.clone(),
                )),
            );
        }

        Ok(())
    }
}

/// `CodecHandler` 演示如何同时实现入站与出站 Handler，以在单个结构体内部处理编解码。
#[derive(Clone)]
struct CodecHandler {
    descriptor: InitializerDescriptor,
}

impl CodecHandler {
    fn new(descriptor: InitializerDescriptor) -> Self {
        Self { descriptor }
    }

    fn describe_stage(&self, stage: &'static str) -> InitializerDescriptor {
        InitializerDescriptor::new(
            format!("{}.{stage}", self.descriptor.name()),
            self.descriptor.category(),
            format!("{stage} 阶段的教学编解码 Handler"),
        )
    }
}

impl InboundHandler for CodecHandler {
    fn describe(&self) -> InitializerDescriptor {
        self.describe_stage("inbound")
    }

    fn on_channel_active(&self, ctx: &dyn PipelineContext) {
        ctx.logger().info(
            "codec handler activated",
            None,
            Some(ctx.trace_context()),
        );
    }

    fn on_read(&self, ctx: &dyn PipelineContext, msg: PipelineMessage) {
        ctx.forward_read(msg);
    }

    fn on_read_complete(&self, _ctx: &dyn PipelineContext) {}

    fn on_writability_changed(&self, _ctx: &dyn PipelineContext, _is_writable: bool) {}

    fn on_user_event(&self, _ctx: &dyn PipelineContext, _event: spark_core::observability::CoreUserEvent) {}

    fn on_exception_caught(&self, ctx: &dyn PipelineContext, error: CoreError) {
        ctx.logger().error(
            "codec handler encountered error",
            Some(&error),
            Some(ctx.trace_context()),
        );
        ctx.close_graceful(
            CloseReason::new("spark.examples.codec.error", "codec error"),
            None,
        );
    }

    fn on_channel_inactive(&self, ctx: &dyn PipelineContext) {
        ctx.logger().debug(
            "codec handler channel inactive",
            None,
            Some(ctx.trace_context()),
        );
    }
}

impl OutboundHandler for CodecHandler {
    fn describe(&self) -> InitializerDescriptor {
        self.describe_stage("outbound")
    }

    fn on_write(
        &self,
        _ctx: &dyn PipelineContext,
        msg: PipelineMessage,
    ) -> CoreResult<WriteSignal> {
        Ok(if msg.is_user() {
            WriteSignal::AcceptedAndFlushed
        } else {
            WriteSignal::Accepted
        })
    }

    fn on_flush(&self, _ctx: &dyn PipelineContext) -> CoreResult<()> {
        Ok(())
    }

    fn on_close_graceful(
        &self,
        _ctx: &dyn PipelineContext,
        _deadline: Option<core::time::Duration>,
    ) -> CoreResult<()> {
        Ok(())
    }
}

mod router_stage {
    use super::{BoxService, ChainBuilder, InitializerDescriptor, PipelineMessage};

    /// `RouterFlow` 描述路由 Handler 对入站消息的消费语义。
    ///
    /// # 教案级说明
    /// - **意图 (Why)**：在 `MyInitializer` 中根据不同实现（真实 Router 或教学桩）
    ///   决定是否继续注册 `ServiceAdapterHandler`；
    /// - **契约 (What)**：`ConsumesInbound` 表示 Router 会终止消息并负责调用业务
    ///   Service；`ForwardsToNext` 表示仍需后续 Handler 处理；
    /// - **风险提示 (Trade-offs)**：抽象为枚举后，未来若增添更多 Router 变体，只需
    ///   新增分支即可保持流程清晰，避免布尔标志失语化。
    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    pub enum RouterFlow {
        /// Router 自行消费入站消息并写出响应。
        ConsumesInbound,
        /// Router 仅记录或补充上下文，消息继续流向后续 Handler。
        ForwardsToNext,
    }

    #[cfg(feature = "std")]
    mod std_impl {
        use super::RouterFlow;
        use alloc::{borrow::Cow, format, sync::Arc};
        use spark_core::{
            SparkError,
            pipeline::Context as PipelineContext,
            router::{
                DynRouter,
                context::{RoutingIntent, RoutingSnapshot},
                metadata::{MetadataKey, MetadataValue, RouteMetadata},
                route::{RouteKind, RoutePattern, RouteSegment},
            },
        };
        use spark_router::{DefaultRouter, ServiceFactory};
        use spark_router::pipeline::{
            ApplicationRouter, RoutingContextBuilder, RoutingContextParts,
        };

        use super::{BoxService, ChainBuilder, InitializerDescriptor, PipelineMessage};

        /// 将真实 `ApplicationRouter` 装配进教学 Pipeline。
        ///
        /// # 教案级说明
        /// - **意图 (Why)**：示例展示“握手 → Router → Service” 的真实路径，帮助读者理解
        ///   `spark_router::pipeline` 新命名空间的使用方式；
        /// - **流程 (How)**：
        ///   1. 构建只含单一路由的 [`DefaultRouter`] 并注册静态 Service 工厂；
        ///   2. 构造 `ExampleRoutingContextBuilder`，根据 Channel 上下文拼装路由意图；
        ///   3. 以 [`ApplicationRouter::new`] 生成 Handler 并注册到入站链路；
        /// - **契约 (What)**：返回 [`RouterFlow::ConsumesInbound`]，告知上游不再需要
        ///   `ServiceAdapterHandler`；
        /// - **权衡 (Trade-offs)**：默认路由模式和元数据均写死为教学用途，若要复用需结合
        ///   实际业务扩展模式枚举与上下文构造器。
        pub fn register_router(
            chain: &mut dyn ChainBuilder,
            descriptor: InitializerDescriptor,
            service: BoxService,
        ) -> RouterFlow {
            let pattern = RoutePattern::new(
                RouteKind::Rpc,
                vec![
                    RouteSegment::Literal(Cow::Borrowed("examples")),
                    RouteSegment::Literal(Cow::Borrowed("service")),
                ],
            );

            let router = Arc::new(DefaultRouter::new());
            router.add_route(
                pattern.clone(),
                Arc::new(StaticServiceFactory::new(service.clone())),
            );

            let router: Arc<dyn DynRouter> = router;
            let context_builder: Arc<dyn RoutingContextBuilder> = Arc::new(
                ExampleRoutingContextBuilder::new(pattern, descriptor.name().to_owned()),
            );

            let handler = ApplicationRouter::new(router, context_builder, descriptor);
            chain.register_inbound("router.l2", Box::new(handler));

            RouterFlow::ConsumesInbound
        }

        /// `StaticServiceFactory`：始终返回克隆后的教学 Service。
        ///
        /// # 教案级注释
        /// - **意图 (Why)**：向默认路由器提供满足 `ServiceFactory` 契约的最小实现，
        ///   便于示例通过 `ApplicationRouter` 调用业务逻辑；
        /// - **契约 (What)**：每次 `create` 调用都会克隆内部的 [`BoxService`]；
        /// - **风险 (Trade-offs)**：克隆开销与业务 Service 实现有关，真实系统应按需缓存或
        ///   使用对象池。
        struct StaticServiceFactory {
            service: BoxService,
        }

        impl StaticServiceFactory {
            fn new(service: BoxService) -> Self {
                Self { service }
            }
        }

        impl ServiceFactory for StaticServiceFactory {
            fn create(&self) -> spark_core::Result<BoxService, SparkError> {
                Ok(self.service.clone())
            }
        }

        /// `ExampleRoutingContextBuilder`：从 Channel 与意图模板构建路由上下文。
        ///
        /// # 教案级注释
        /// - **意图 (Why)**：示范如何自定义 [`RoutingContextBuilder`]，将教学用的静态模式
        ///   与运行时动态信息结合；
        /// - **流程 (How)**：
        ///   1. 复制预设的 `RoutePattern` 生成 [`RoutingIntent`]；
        ///   2. 将 Channel trace 信息写入动态元数据，方便路由器记录；
        ///   3. 返回 [`RoutingContextParts`] 供 Router 组装完整上下文；
        /// - **契约 (What)**：若需要扩展更多动态属性，可在此函数中访问 `msg` 或其他扩展。
        struct ExampleRoutingContextBuilder {
            pattern: RoutePattern,
            label: String,
        }

        impl ExampleRoutingContextBuilder {
            fn new(pattern: RoutePattern, label: String) -> Self {
                Self { pattern, label }
            }
        }

        impl RoutingContextBuilder for ExampleRoutingContextBuilder {
            fn build(
                &self,
                ctx: &dyn PipelineContext,
                _msg: &PipelineMessage,
                snapshot: RoutingSnapshot<'_>,
            ) -> spark_core::Result<RoutingContextParts, SparkError> {
                let message = format!(
                    "router builder constructing routing context (catalog_revision={} label={})",
                    snapshot.revision(),
                    self.label
                );
                ctx.logger()
                    .debug(&message, None, Some(ctx.trace_context()));

                let mut metadata = RouteMetadata::new();
                metadata.insert(
                    MetadataKey::new(Cow::Borrowed("spark.examples.router.label")),
                    MetadataValue::Text(Cow::Owned(self.label.clone())),
                );

                let intent = RoutingIntent::new(self.pattern.clone())
                    .with_metadata(metadata.clone());

                Ok(RoutingContextParts::new(intent, None, metadata))
            }
        }
    }

    #[cfg(not(feature = "std"))]
    mod no_std_impl {
        use super::RouterFlow;
        use spark_core::{
            CoreError,
            observability::CoreUserEvent,
            pipeline::{Context as PipelineContext, InboundHandler},
        };

        use super::{ChainBuilder, InitializerDescriptor, PipelineMessage};

        /// 教学桩实现：保持旧版“记录后转发”的行为，以兼容 `no_std` 环境。
        pub fn register_router(
            chain: &mut dyn ChainBuilder,
            descriptor: InitializerDescriptor,
            _service: super::BoxService,
        ) -> RouterFlow {
            chain.register_inbound(
                "router.l2",
                Box::new(ApplicationRouter::new(descriptor)),
            );
            RouterFlow::ForwardsToNext
        }

        /// `ApplicationRouter`：no_std 桩实现，负责记录并继续转发消息。
        ///
        /// # 教案式说明
        /// - **意图（Why）**：在缺失标准库的环境中，以最小依赖保留“记录+透传”能力，
        ///   让示例能够演示控制面如何挂载路由 Handler；
        /// - **契约（What）**：实现 [`InboundHandler`]，输入来自上一层的 [`PipelineMessage`]，
        ///   输出通过 `ctx.forward_read` 原样交给下游，无内部状态变更；
        /// - **流程（How）**：
        ///   1. `new` 保存初始化描述符，便于运行时查询；
        ///   2. `on_channel_active` 记录启动日志，帮助定位链路；
        ///   3. `on_read` 打印调试信息并继续透传消息；
        ///   4. 其他回调保持空实现，表明此桩不干预额外阶段；
        /// - **前置条件/后置条件（Contract）**：调用前需提供合法的 `InitializerDescriptor`，
        ///   成功执行后保证消息被继续转发且未修改；
        /// - **权衡与风险（Trade-offs）**：牺牲真实路由能力换取编译通过，若在生产误用，将缺失动态路由；
        ///   如需增强，可替换为完整的 [`super::register_router`] `std` 版本；
        /// - **边界情况（Gotchas）**：日志记录依赖 `PipelineContext` 提供的追踪上下文，
        ///   若上游未注入将退化为 `None`，但不会影响消息流。
        struct ApplicationRouter {
            descriptor: InitializerDescriptor,
        }

        impl ApplicationRouter {
            fn new(descriptor: InitializerDescriptor) -> Self {
                Self { descriptor }
            }
        }

        impl InboundHandler for ApplicationRouter {
            fn describe(&self) -> InitializerDescriptor {
                self.descriptor.clone()
            }

            fn on_channel_active(&self, ctx: &dyn PipelineContext) {
                ctx.logger().info(
                    "app router activated",
                    None,
                    Some(ctx.trace_context()),
                );
            }

            fn on_read(&self, ctx: &dyn PipelineContext, msg: PipelineMessage) {
                ctx.logger().debug(
                    "app router received frame; forwarding to next handler",
                    None,
                    Some(ctx.trace_context()),
                );
                ctx.forward_read(msg);
            }

            fn on_read_complete(&self, _ctx: &dyn PipelineContext) {}

            fn on_writability_changed(
                &self,
                _ctx: &dyn PipelineContext,
                _is_writable: bool,
            ) {
            }

            fn on_user_event(
                &self,
                _ctx: &dyn PipelineContext,
                _event: CoreUserEvent,
            ) {
            }

            fn on_exception_caught(&self, ctx: &dyn PipelineContext, error: CoreError) {
                ctx.logger().error(
                    "app router handler failed",
                    Some(&error),
                    Some(ctx.trace_context()),
                );
            }

            fn on_channel_inactive(&self, _ctx: &dyn PipelineContext) {}
        }
    }

    #[cfg(feature = "std")]
    pub use std_impl::register_router;
    #[cfg(not(feature = "std"))]
    pub use no_std_impl::register_router;

    pub use RouterFlow;
}

/// `ServiceAdapterHandler` 在示例中代表 L3/业务阶段：将消息传递给业务 Service。
/// 当启用 `std` 并安装真实 `ApplicationRouter` 时，该 Handler 会被跳过，因为路由器
/// 已直接调用业务 Service 并写回响应；在 `no_std` 教学桩或禁用 Router 场景下仍负责
/// 收尾处理。
struct ServiceAdapterHandler {
    descriptor: InitializerDescriptor,
    service: BoxService,
}

impl ServiceAdapterHandler {
    fn new(descriptor: InitializerDescriptor, service: BoxService) -> Self {
        Self { descriptor, service }
    }
}

impl InboundHandler for ServiceAdapterHandler {
    fn describe(&self) -> InitializerDescriptor {
        self.descriptor.clone()
    }

    fn on_channel_active(&self, ctx: &dyn PipelineContext) {
        ctx.logger().info(
            "service adapter activated",
            None,
            Some(ctx.trace_context()),
        );
    }

    fn on_read(&self, ctx: &dyn PipelineContext, msg: PipelineMessage) {
        ctx.logger().info(
            "service adapter received request; forwarding to next handler",
            None,
            Some(ctx.trace_context()),
        );
        // 真正的业务调用在教学示例中省略，保留透传流程以突出 Handler 排布。
        ctx.forward_read(msg);
    }

    fn on_read_complete(&self, _ctx: &dyn PipelineContext) {}

    fn on_writability_changed(&self, _ctx: &dyn PipelineContext, _is_writable: bool) {}

    fn on_user_event(&self, _ctx: &dyn PipelineContext, _event: spark_core::observability::CoreUserEvent) {}

    fn on_exception_caught(&self, ctx: &dyn PipelineContext, error: CoreError) {
        ctx.logger().error(
            "service adapter observed pipeline error",
            Some(&error),
            Some(ctx.trace_context()),
        );
    }

    fn on_channel_inactive(&self, ctx: &dyn PipelineContext) {
        ctx.logger().debug(
            "service adapter shutting down",
            None,
            Some(ctx.trace_context()),
        );
    }
}

/// 构造一个“未发生降级”的 `DowngradeReport`，便于快速创建握手结果。
fn downgrade_report() -> DowngradeReport {
    DowngradeReport::new(CapabilityBitmap::empty(), CapabilityBitmap::empty())
}

/// 生成教学用的 `HandshakeOutcome`，方便文档或测试模拟不同能力组合。
pub fn example_handshake(capabilities: &[Capability]) -> HandshakeOutcome {
    let mut bitmap = CapabilityBitmap::empty();
    for capability in capabilities {
        bitmap.insert(*capability);
    }
    HandshakeOutcome::new(Version::new(1, 0, 0), bitmap, downgrade_report())
}

#[cfg(all(test, feature = "std"))]
mod router_pipeline_migration_tests {
    use core::mem;

    use spark_router::pipeline::{ApplicationRouter, ApplicationRouterInitializer};

    /// 教案级测试：确保示例代码默认面向 `spark_router::pipeline` 新命名空间，而不会在未来
    /// 回退到任何已经弃用的旧版路由 Handler 命名空间。
    ///
    /// # 教案级说明
    /// - **意图 (Why)**：通过编译期访问关键类型的方式，提醒维护者“示例应引用新版命名空
    ///   间”；一旦误引入旧 crate，测试将在链接阶段失败，阻止错误传播到文档或教学代码中。
    /// - **逻辑 (How)**：
    ///   1. 使用 [`core::mem::size_of`] 强制编译器解析 `ApplicationRouterInitializer` 与
    ///      `ApplicationRouter`，验证类型来自 `spark_router::pipeline`；
    ///   2. 将两者尺寸求和并断言结果大于零，避免优化器删除代码路径；
    /// - **契约 (What)**：
    ///   - *前置条件*：编译单元启用了 `std` 特性，使 `spark-router` 暴露完整 Pipeline API；
    ///   - *后置条件*：若新命名空间可见，断言成立并返回 `()`；若类型被移动或改名，则编译
    ///     或运行阶段立即失败，向维护者发出警示。
    /// - **设计考量 (Trade-offs & Gotchas)**：测试不实例化 Router，保持零运行时依赖；虽然
    ///   未覆盖行为层面的验证，但换取了编译快、定位准的保障，适合作为命名空间迁移的守护
    ///   网。未来若 API 再次迁移，只需调整 `use spark_router::pipeline::{..}` 即可。
    #[test]
    fn ensure_new_pipeline_namespace_is_linked() {
        let initializer_size = mem::size_of::<ApplicationRouterInitializer>();
        let handler_size = mem::size_of::<ApplicationRouter>();

        assert!(initializer_size + handler_size > 0);
    }
}
