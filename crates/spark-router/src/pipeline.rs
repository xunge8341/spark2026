#![allow(clippy::items_after_test_module)]

use alloc::{borrow::Cow, boxed::Box, format, sync::Arc, vec::Vec};
use core::any::TypeId;

use spark_core::{
    CoreError, SparkError,
    buffer::PipelineMessage,
    error::codes,
    observability::{Logger, OwnedAttributeSet},
    pipeline::{
        ChainBuilder, Channel, Context, Handler, HandlerDirection, InboundHandler,
        PipelineInitializer, extensions::ExtensionsMap, initializer::InitializerDescriptor,
    },
    router::{
        DynRouter, RouteDecisionObject, RouteError,
        context::{RoutingContext, RoutingIntent, RoutingSnapshot},
        metadata::RouteMetadata,
    },
    runtime::{CoreServices, TaskExecutor},
    service::BoxService,
    transport::intent::ConnectionIntent,
};

/// ApplicationRouter 模块内使用的扩展存储键类型，占位以提供稳定的 `TypeId`。
struct RouterContextSlot;

/// `RouterContextState` 聚合路由判定所需的上下文快照。
///
/// # 教案式说明
/// - **意图（Why）**：将跨组件收集的路由意图、连接属性与动态元数据集中管理，避免 Handler 彼此约定魔法字段。
/// - **定位（Where）**：存放于 `Channel` 的 [`ExtensionsMap`] 中，供 [`ApplicationRouter`] 在入站阶段读取。
/// - **逻辑（How）**：提供快照式只读访问接口，并在需要时克隆出独立副本，确保在异步路由判定期间不会悬垂引用。
/// - **契约（What）**：
///   - `intent`：请求方声明的路由意图，必须完整描述目标模式；
///   - `connection`：传输层意图（可选），用于 QoS、安全等协商；
///   - `dynamic_metadata`：运行时追加的标签集合，在路由决策时与静态元数据合并。
/// - **权衡（Trade-offs）**：结构体保持不可变，若需动态更新应在上游生成新的快照重新写入扩展存储，以换取读路径的无锁访问。
#[derive(Clone, Debug)]
pub struct RouterContextState {
    intent: RoutingIntent,
    connection: Option<ConnectionIntent>,
    dynamic_metadata: RouteMetadata,
}

impl RouterContextState {
    /// 基于必需的路由意图构造状态。
    ///
    /// # 教案式说明
    /// - **前置条件**：`intent` 必须指向合法的 [`RoutePattern`](spark_core::router::RoutePattern)，调用方应在上游完成语义校验；
    /// - **执行步骤**：初始化时动态元数据置空，留待后续组件补充；
    /// - **后置条件**：返回的状态可立即存入扩展映射，并被 [`ApplicationRouter`] 读取。
    pub fn new(intent: RoutingIntent) -> Self {
        Self {
            intent,
            connection: None,
            dynamic_metadata: RouteMetadata::new(),
        }
    }

    /// 附加传输层意图，用于在路由阶段融合 QoS/安全偏好。
    pub fn with_connection(mut self, connection: ConnectionIntent) -> Self {
        self.connection = Some(connection);
        self
    }

    /// 访问动态元数据的可变引用，便于在上游组件中填充运行时标签。
    pub fn dynamic_metadata_mut(&mut self) -> &mut RouteMetadata {
        &mut self.dynamic_metadata
    }

    /// 读取内部路由意图。
    pub fn intent(&self) -> &RoutingIntent {
        &self.intent
    }

    /// 读取可选的连接意图。
    pub fn connection(&self) -> Option<&ConnectionIntent> {
        self.connection.as_ref()
    }

    /// 读取动态元数据。
    pub fn dynamic_metadata(&self) -> &RouteMetadata {
        &self.dynamic_metadata
    }

    /// 克隆出一次性消费的快照，供 Handler 构建 [`RoutingContext`]。
    pub fn snapshot(&self) -> RouterContextSnapshot {
        RouterContextSnapshot {
            intent: self.intent.clone(),
            connection: self.connection.clone(),
            dynamic_metadata: self.dynamic_metadata.clone(),
        }
    }
}

/// 供 [`ApplicationRouter`] 使用的只读快照。
#[derive(Clone, Debug)]
pub struct RouterContextSnapshot {
    pub intent: RoutingIntent,
    pub connection: Option<ConnectionIntent>,
    pub dynamic_metadata: RouteMetadata,
}

/// 将路由上下文状态写入通道扩展存储。
///
/// # 教案式说明
/// - **意图（Why）**：提供统一入口，避免调用方直接操作 [`ExtensionsMap`] 时出现键冲突或类型漂移。
/// - **逻辑（How）**：以 `Arc` 包裹状态对象后写入，保证多 Handler 并发读取时的线程安全；
/// - **契约（What）**：后续可调用 [`load_router_context`] 获取同一份 `Arc`。若需更新，调用方应先移除再写入新值。
pub fn store_router_context(extensions: &dyn ExtensionsMap, state: RouterContextState) {
    extensions.insert(TypeId::of::<RouterContextSlot>(), Box::new(Arc::new(state)));
}

/// 从通道扩展存储中读取路由上下文状态。
pub fn load_router_context(extensions: &dyn ExtensionsMap) -> Option<Arc<RouterContextState>> {
    extensions
        .get(&TypeId::of::<RouterContextSlot>())
        .and_then(|entry| entry.downcast_ref::<Arc<RouterContextState>>())
        .map(Arc::clone)
}

/// 描述构建 [`RoutingContext`] 所需的全部要素。
///
/// # 教案式说明
/// - **意图（Why）**：`RoutingContext` 要求引用型字段（意图、连接、动态元数据）在路由判定期间保持有效，
///   因此提前聚合这些所有权数据，便于在 Handler 内部安全借用。
/// - **结构（How）**：持有 [`RoutingIntent`]、可选的 [`ConnectionIntent`] 与动态 [`RouteMetadata`]；
///   Handler 将在调用 [`RoutingContext::new`] 时临时借用这些字段。
/// - **契约（What）**：
///   - `intent`：描述目标路由模式及调用方偏好；
///   - `connection`：可选的传输层意图，用于 QoS/安全策略协同；
///   - `dynamic_metadata`：运行时附带的标签，例如请求头或租户信息。
#[derive(Debug, Clone)]
pub struct RoutingContextParts {
    /// 路由意图，承载目标模式与偏好参数。
    pub intent: RoutingIntent,
    /// 可选的连接意图，用于路由决策结合网络因素。
    pub connection: Option<ConnectionIntent>,
    /// 动态路由元数据，供策略引擎读取。
    pub dynamic_metadata: RouteMetadata,
}

impl RoutingContextParts {
    /// 构造便捷函数，供上层在已知意图场景快速创建上下文材料。
    pub fn new(
        intent: RoutingIntent,
        connection: Option<ConnectionIntent>,
        dynamic_metadata: RouteMetadata,
    ) -> Self {
        Self {
            intent,
            connection,
            dynamic_metadata,
        }
    }
}

/// 为 [`ApplicationRouter`] 提供请求上下文所需材料的构造器契约。
///
/// # 教案式说明
/// - **意图（Why）**：不同接入层在 [`PipelineMessage`] 中承载上下文的方式各异，
///   通过扩展点让调用者自定义“如何从 `ctx` 与 `msg` 中抽取路由素材”，以避免 Handler 对具体协议写死假设。
/// - **逻辑（How）**：实现方可选择从 Channel 扩展、调用上下文或消息体中提取意图、连接与动态元数据，
///   并返回 [`RoutingContextParts`]；Handler 随后会将其装配成 [`RoutingContext`]。
/// - **契约（What）**：
///   - 输入为当前 Pipeline 上下文 `ctx`、待路由的消息 `msg` 及 Router 快照 `snapshot`；
///   - 成功时返回完整的 [`RoutingContextParts`]；失败时返回 [`SparkError`] 以便 Handler 记录并放弃处理。
pub trait RoutingContextBuilder: Send + Sync + 'static {
    /// 根据 Pipeline 状态与消息构建路由上下文所需材料。
    #[allow(clippy::result_large_err)]
    fn build(
        &self,
        ctx: &dyn Context,
        msg: &PipelineMessage,
        snapshot: RoutingSnapshot<'_>,
    ) -> spark_core::Result<RoutingContextParts, SparkError>;
}

/// 默认的上下文构造器，实现从 [`ExtensionsMap`] 中提取 [`RouterContextState`]。
///
/// # 教案式说明
/// - **意图（Why）**：为沿用旧版“控制面先写入 `RouterContextState`，Handler 直接消费” 的调用模式提供兼容层，
///   降低本次路由模块合并的迁移成本。
/// - **逻辑（How）**：
///   1. 调用 [`load_router_context`] 读取共享状态；
///   2. 若缺失则返回 `SparkError`，提示调用方补充意图；
///   3. 对读取到的状态执行 `snapshot()`，生成 [`RoutingContextParts`]。
/// - **契约（What）**：要求在 Handler 触发前由上游组件通过 [`store_router_context`] 写入状态；否则将返回
///   `codes::APP_ROUTING_FAILED` 错误并终止处理。
#[derive(Clone, Debug, Default)]
pub struct ExtensionsRoutingContextBuilder;

impl RoutingContextBuilder for ExtensionsRoutingContextBuilder {
    fn build(
        &self,
        ctx: &dyn Context,
        _msg: &PipelineMessage,
        _snapshot: RoutingSnapshot<'_>,
    ) -> spark_core::Result<RoutingContextParts, SparkError> {
        let Some(state) = load_router_context(ctx.channel().extensions()) else {
            return Err(SparkError::new(
                codes::APP_ROUTING_FAILED,
                "router context missing on channel",
            ));
        };
        let snapshot = state.snapshot();
        Ok(RoutingContextParts::new(
            snapshot.intent,
            snapshot.connection,
            snapshot.dynamic_metadata,
        ))
    }
}

/// 封装服务调用所需的调度句柄集合。
///
/// # 教案式说明
/// - **意图（Why）**：将执行器、调用上下文、写通道与日志器等多项依赖打包，
///   既便于 [`ApplicationRouter::spawn_service_task`] 复用，也让参数列表控制在 Clippy 建议范围内。
/// - **逻辑（How）**：在 Handler 中就地构造 `ServiceDispatchContext`，
///   持有 [`spark_core::contract::CallContext`] 所有权，并克隆通道 `Arc` 与追踪上下文；随后交由异步任务消费。
/// - **契约（What）**：
///   - `executor`：运行时执行器引用，必须保证在任务生命周期内有效；
///   - `call_ctx`：任务所属的调用上下文，要求克隆自当前 Pipeline；
///   - `channel`：写响应所需的通道引用，由 [`Arc`] 管理生命周期；
///   - `logger`：用于记录任务执行失败的日志器指针；
///   - `trace`：追踪上下文，用于日志注入；
/// - **风险提示（Trade-offs）**：`logger` 通过 `unsafe` 指针延长生命周期，假设 `HotSwapContext` 以 `Arc`
///   持有底层实现；若未来上下文模型变化，需要同步调整此结构以维持内存安全。
struct ServiceDispatchContext<'exec> {
    executor: &'exec dyn TaskExecutor,
    call_ctx: spark_core::contract::CallContext,
    channel: Arc<dyn spark_core::pipeline::channel::Channel>,
    logger: &'static dyn Logger,
    trace: spark_core::observability::TraceContext,
}

/// `ApplicationRouter` 负责终止入站事件并将请求转交给对象层 Router。
///
/// # 教案式说明
/// - **意图（Why）**：将 Pipeline 最终的业务请求映射到 [`DynRouter`]，实现“Handler → Router → Service” 的桥接，
///   使得 Handler 链可以专注于编解码、鉴权等前置步骤。
/// - **逻辑（How）**：
///   1. 通过 [`RoutingContextBuilder`] 获取构建 [`RoutingContext`] 所需的材料；
///   2. 调用注入的 [`DynRouter::route_dyn`] 执行路由决策；
///   3. 将命中的 [`BoxService`] 托付给运行时执行器，异步调用业务逻辑；
///   4. 调用成功后写回响应，触发出站链路。
/// - **契约（What）**：
///   - Handler 不再调用 `ctx.forward_read`，意味着它必须位于入站链路尾部；
///   - 路由或服务调用失败会记录 ERROR 日志，但不会重复抛给后续 Handler。
/// - **风险（Trade-offs）**：
///   - 通过 `unsafe` 延长 `Logger` 引用生命周期以便在异步任务中使用，
///     假定 `HotSwapContext` 内部使用 `Arc` 持有该资源；若未来上下文实现发生变化，需同步评估安全性。
pub struct ApplicationRouter {
    router: Arc<dyn DynRouter>,
    context_builder: Arc<dyn RoutingContextBuilder>,
    descriptor: InitializerDescriptor,
}

/// 默认的 Handler 注册标签，供 [`ApplicationRouterInitializer`] 在装配阶段复用。
const DEFAULT_HANDLER_LABEL: &str = "application-router";

/// `ApplicationRouterInitializer` 负责在 Pipeline 初始化阶段注册 [`ApplicationRouter`] Handler。
///
/// # 教案级说明
/// - **意图（Why）**：将 L2 路由 Handler 的装配逻辑封装成可复用的 [`PipelineInitializer`]，
///   以便宿主或管道工厂无需重复编写样板代码即可安装应用层路由能力。
/// - **体系位置（Where）**：处于 PipelineInitializer 链的末端，通常由连接握手或协议编解码
///   中间件在完成上下文准备后按需注册；
/// - **执行逻辑（How）**：
///   1. 构造阶段捕获对象层 [`DynRouter`] 与上下文构造器；
///   2. `configure` 调用时克隆内部的 [`ApplicationRouter`]，以 [`ChainBuilder::register_inbound`]
///      注册到入站链路；
///   3. 采用 teach-plan 注释明确输入输出及幂等约束，帮助调用方在多次装配场景下复用。
/// - **契约（What）**：
///   - `descriptor` 字段描述 PipelineInitializer 自身的元信息；
///   - `handler_label` 控制注册到 Pipeline 时使用的标识；
///   - `handler_descriptor` 通过 [`ApplicationRouter::describe`] 对外报告 Handler 元信息；
/// - **权衡（Trade-offs & Gotchas）**：
///   - 默认使用 `"application-router"` 作为 Handler 标签，若链路中存在多路路由器需显式调用
///     [`Self::with_label`] 自定义；
///   - 初始化器持有 [`ApplicationRouter`] 的克隆副本，每次 `configure` 调用都会产生一次结构体
///     拷贝，代价可忽略但仍需注意幂等性；
///   - 初始化器不直接访问 [`CoreServices`]，保留未来在装配阶段引入观测或运行时协作的扩展点。
#[derive(Clone)]
pub struct ApplicationRouterInitializer {
    descriptor: InitializerDescriptor,
    handler_label: Cow<'static, str>,
    handler: ApplicationRouter,
}

impl ApplicationRouterInitializer {
    /// 基于自定义上下文构造器创建初始化器。
    ///
    /// # 教案级注释
    /// - **前置条件（Preconditions）**：
    ///   - `router` 必须实现 [`DynRouter`] 并满足 `Arc` 共享语义；
    ///   - `context_builder` 负责从 [`ExtensionsMap`] 或其他介质拼装路由上下文，应在 Handler
    ///     执行前由上游组件准备所需材料；
    ///   - `initializer_descriptor` 与 `handler_descriptor` 需准确反映组件职责，便于观测与调试。
    /// - **执行步骤（How）**：
    ///   1. 调用 [`ApplicationRouter::new`] 生成 Handler；
    ///   2. 将 Handler 与默认标签封装到初始化器中；
    ///   3. 返回可直接用于 [`PipelineInitializer`] 链的对象。
    /// - **后置条件（Postconditions）**：返回的初始化器可多次调用 `configure`，每次都会以
    ///   独立的 Handler 副本注册到 Pipeline。
    pub fn new(
        router: Arc<dyn DynRouter>,
        context_builder: Arc<dyn RoutingContextBuilder>,
        initializer_descriptor: InitializerDescriptor,
        handler_descriptor: InitializerDescriptor,
    ) -> Self {
        Self {
            descriptor: initializer_descriptor,
            handler_label: Cow::Borrowed(DEFAULT_HANDLER_LABEL),
            handler: ApplicationRouter::new(router, context_builder, handler_descriptor),
        }
    }

    /// 使用默认的 [`ExtensionsRoutingContextBuilder`] 构造初始化器，简化常见场景。
    ///
    /// # 教案级注释
    /// - **意图（Why）**：复用扩展存储驱动的上下文构造逻辑，避免调用方手动封装 `Arc`；
    /// - **契约（What）**：`router` 与 `descriptor` 语义同 [`Self::new`]，返回对象即可直接用于
    ///   `PipelineInitializer` 链；
    /// - **风险提示（Trade-offs）**：默认构造器假设上游已通过 [`store_router_context`] 写入上下文，
    ///   否则 Handler 会记录诊断日志并终止请求。
    pub fn with_extensions_builder(
        router: Arc<dyn DynRouter>,
        initializer_descriptor: InitializerDescriptor,
        handler_descriptor: InitializerDescriptor,
    ) -> Self {
        Self::new(
            router,
            Arc::new(ExtensionsRoutingContextBuilder),
            initializer_descriptor,
            handler_descriptor,
        )
    }

    /// 自定义 Handler 注册标签，便于在同一 Pipeline 内区分多路路由器。
    pub fn with_label(mut self, label: impl Into<Cow<'static, str>>) -> Self {
        self.handler_label = label.into();
        self
    }

    /// 读取内部 Handler 的描述信息，协助在管线装配前准备观测配置。
    pub fn handler_descriptor(&self) -> InitializerDescriptor {
        self.handler.describe()
    }

    /// 暴露 Handler 契约对象，供运行时热插拔或自定义装配流程复用。
    ///
    /// # 教案级注释
    /// - **意图（Why）**：当调用方希望绕过 `ChainBuilder::register_inbound`，直接使用
    ///   [`Pipeline::add_handler_after`](spark_core::pipeline::Pipeline::add_handler_after)
    ///   等 Handler 契约接口时，需要获得 `Arc<dyn Handler>`；
    ///   `handler_arc` 提供统一出口，避免外部重复编写克隆逻辑；
    /// - **体系位置（Where）**：运行在 `PipelineInitializer::configure` 或控制器热更新
    ///   场景中，通常在判断是否启用应用路由后调用；
    /// - **执行逻辑（How）**：
    ///   1. 克隆内部的 [`ApplicationRouter`] 实例（保持幂等语义）；
    ///   2. 调用 [`ApplicationRouter::handler_arc`] 将其转换为 `Arc<dyn Handler>`；
    /// - **契约（What）**：
    ///   - **返回值**：`Arc<dyn Handler>`，满足 `Send + Sync + 'static`，可安全传递至任意线程；
    ///   - **前置条件**：初始化器内部的路由器与上下文构造器均保持有效；
    ///   - **后置条件**：返回对象不会与初始化器共享可变状态，重复调用安全；
    /// - **设计权衡（Trade-offs）**：每次调用都会克隆一次 Handler，代价为常数级 `Arc`/`Descriptor`
    ///   拷贝，但换取了按需安装的灵活度；调用方若频繁使用，可自行缓存结果。
    pub fn handler_arc(&self) -> Arc<dyn Handler> {
        self.handler.handler_arc()
    }

    /// 将 Handler 注册到 [`ChainBuilder`]，供测试与装配流程复用。
    fn install_handler(&self, chain: &mut dyn ChainBuilder) {
        chain.register_inbound(&self.handler_label, Box::new(self.handler.clone()));
    }
}

impl PipelineInitializer for ApplicationRouterInitializer {
    fn descriptor(&self) -> InitializerDescriptor {
        self.descriptor.clone()
    }

    fn configure(
        &self,
        chain: &mut dyn ChainBuilder,
        _channel: &dyn Channel,
        _services: &CoreServices,
    ) -> spark_core::Result<(), CoreError> {
        self.install_handler(chain);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use spark_core::pipeline::{
        ChainBuilder, HandlerDirection, InboundHandler, handler::OutboundHandler,
    };

    #[derive(Default)]
    struct RecordingChainBuilder {
        registrations: alloc::vec::Vec<(alloc::string::String, InitializerDescriptor)>,
    }

    impl ChainBuilder for RecordingChainBuilder {
        fn register_inbound(&mut self, label: &str, handler: Box<dyn InboundHandler>) {
            self.registrations
                .push((label.to_owned(), handler.describe()));
        }

        fn register_outbound(&mut self, _label: &str, _handler: Box<dyn OutboundHandler>) {}
    }

    #[test]
    fn initializer_registers_application_router_with_defaults() {
        let router = Arc::new(crate::DefaultRouter::new());
        let initializer_desc = InitializerDescriptor::new(
            "test.router_initializer",
            "routing",
            "installs application router handler",
        );
        let handler_desc = InitializerDescriptor::new(
            "test.application_router",
            "routing",
            "application router handler",
        );
        let initializer = ApplicationRouterInitializer::with_extensions_builder(
            router,
            initializer_desc.clone(),
            handler_desc.clone(),
        );

        let mut builder = RecordingChainBuilder::default();
        initializer.install_handler(&mut builder);

        assert_eq!(builder.registrations.len(), 1);
        let (label, descriptor) = &builder.registrations[0];
        assert_eq!(label, DEFAULT_HANDLER_LABEL);
        assert_eq!(descriptor, &handler_desc);
        assert_eq!(initializer.descriptor(), initializer_desc);
        assert_eq!(initializer.handler_descriptor(), handler_desc);
    }

    #[test]
    fn initializer_allows_custom_labels() {
        let router = Arc::new(crate::DefaultRouter::new());
        let initializer_desc = InitializerDescriptor::new(
            "test.router_initializer.custom",
            "routing",
            "installs router with custom label",
        );
        let handler_desc = InitializerDescriptor::new(
            "test.application_router.custom",
            "routing",
            "application router handler",
        );
        let initializer = ApplicationRouterInitializer::with_extensions_builder(
            router,
            initializer_desc,
            handler_desc.clone(),
        )
        .with_label("custom-router");

        let mut builder = RecordingChainBuilder::default();
        initializer.install_handler(&mut builder);

        assert_eq!(builder.registrations.len(), 1);
        let (label, descriptor) = &builder.registrations[0];
        assert_eq!(label, "custom-router");
        assert_eq!(descriptor, &handler_desc);
    }

    #[test]
    fn initializer_exposes_handler_arc_for_dynamic_installation() {
        // 教案式说明：验证 `handler_arc` 所返回的 `Arc<dyn Handler>` 可直接暴露给运行时，
        // 以满足 Pipeline 热插拔 API 对 Handler 契约的期待。
        let router = Arc::new(crate::DefaultRouter::new());
        let initializer_desc = InitializerDescriptor::new(
            "test.router_initializer.arc",
            "routing",
            "installs router handler via handler_arc",
        );
        let handler_desc = InitializerDescriptor::new(
            "test.application_router.arc",
            "routing",
            "application router handler arc",
        );
        let initializer = ApplicationRouterInitializer::with_extensions_builder(
            router,
            initializer_desc,
            handler_desc.clone(),
        );

        let handler_arc = initializer.handler_arc();

        assert_eq!(handler_arc.direction(), HandlerDirection::Inbound);
        assert_eq!(handler_arc.descriptor(), handler_desc);

        let inbound = handler_arc
            .clone_inbound()
            .expect("application router must expose inbound handler");
        assert_eq!(inbound.describe(), handler_arc.descriptor());
    }
}

impl Clone for ApplicationRouter {
    fn clone(&self) -> Self {
        Self {
            router: Arc::clone(&self.router),
            context_builder: Arc::clone(&self.context_builder),
            descriptor: self.descriptor.clone(),
        }
    }
}

impl ApplicationRouter {
    /// 构造新的路由处理器实例。
    ///
    /// # 参数说明
    /// - `router`：对象层路由器实现，负责根据 [`RoutingContext`] 返回服务绑定；
    /// - `context_builder`：提取路由上下文材料的扩展点；
    /// - `descriptor`：用于链路 introspection 的元信息，如无特殊需求可传入 [`InitializerDescriptor::anonymous`] 结果。
    pub fn new(
        router: Arc<dyn DynRouter>,
        context_builder: Arc<dyn RoutingContextBuilder>,
        descriptor: InitializerDescriptor,
    ) -> Self {
        Self {
            router,
            context_builder,
            descriptor,
        }
    }

    /// 基于默认的 [`ExtensionsRoutingContextBuilder`] 构造 Handler 实例。
    ///
    /// # 教案式说明
    /// - **意图（Why）**：为常见的“PipelineInitializer 先在 `ExtensionsMap` 写入路由上下文，再交由
    ///   Handler 消费” 场景提供零样板构造函数，避免调用方重复显式创建 `Arc<dyn RoutingContextBuilder>`。
    /// - **定位（Where）**：位于 L2 Handler 构造阶段，通常在 PipelineInitializer 的 `configure`
    ///   方法内调用，并紧跟在业务自定义的编解码/鉴权 Handler 之后注册。
    /// - **逻辑（How）**：内部复用 [`Self::new`]，传入默认的
    ///   [`ExtensionsRoutingContextBuilder::default`]，随后返回完全体的 [`ApplicationRouter`]。
    /// - **契约（What）**：
    ///   - `router`：对象层路由器实例，需满足 `DynRouter` 契约；
    ///   - `descriptor`：PipelineInitializer 用于观测与热插拔的元数据描述；
    ///   - 返回值：即可在 `spark_core::pipeline::ChainBuilder::register_inbound` 中以
    ///     `Box::new(...)` 安装的 Handler；
    /// - **风险与权衡（Trade-offs & Gotchas）**：默认构造器假设路由上下文已写入
    ///   `ExtensionsMap`，若调用方采用其他携带方式，应改用 [`Self::new`] 自行注入专用
    ///   `RoutingContextBuilder`，否则会触发 `APP_ROUTING_FAILED` 错误并被记录到日志。
    pub fn with_extensions_builder(
        router: Arc<dyn DynRouter>,
        descriptor: InitializerDescriptor,
    ) -> Self {
        Self::new(
            router,
            Arc::new(ExtensionsRoutingContextBuilder),
            descriptor,
        )
    }

    fn spawn_service_task(
        &self,
        dispatch: ServiceDispatchContext<'_>,
        service: BoxService,
        request: PipelineMessage,
    ) {
        let fut_ctx = dispatch.call_ctx.clone();
        let channel = Arc::clone(&dispatch.channel);
        let logger = dispatch.logger;
        let trace_for_log = dispatch.trace.clone();
        let service_task = async move {
            let mut dyn_service = into_dyn_service(service);
            let response = dyn_service.call_dyn(fut_ctx, request).await?;
            channel.write(response).map(|_| ()).map_err(|err| {
                SparkError::new(codes::APP_ROUTING_FAILED, "failed to write response")
                    .with_cause(err)
            })
        };

        let log_future = async move {
            if let Err(err) = service_task.await {
                logger.error(
                    "application-router encountered error while invoking service",
                    Some(&err),
                    Some(&trace_for_log),
                );
            }

            Ok::<Box<dyn core::any::Any + Send>, spark_core::TaskError>(Box::new(()))
        };

        let handle = dispatch
            .executor
            .spawn_dyn(&dispatch.call_ctx, Box::pin(log_future));
        handle.detach();
    }

    fn handle_decision(
        &self,
        ctx: &dyn Context,
        decision: RouteDecisionObject,
        request: PipelineMessage,
        trace: spark_core::observability::TraceContext,
    ) {
        let binding = decision.binding().clone();
        let service = binding.service().clone();
        let call_ctx = ctx.call_context().clone();
        let executor = ctx.executor();
        let channel = unsafe_clone_channel(ctx);
        let logger_ptr = ctx.logger() as *const dyn Logger;
        drop(binding);
        drop(decision);
        let dispatch = ServiceDispatchContext {
            executor,
            call_ctx,
            channel,
            logger: unsafe { &*logger_ptr },
            trace,
        };
        self.spawn_service_task(dispatch, service, request);
    }

    /// 将当前 Handler 实例转化为 `Arc<dyn Handler>`，方便 PipelineInitializer 直接注册。
    ///
    /// # 教案式说明
    /// - **意图（Why）**：减少调用方在装配链路时的样板代码，直接得到框架契约期望的对象类型；
    /// - **契约（What）**：返回的 `Arc` 既实现 [`Handler`] 又能通过 [`Handler::clone_inbound`] 克隆入站实例；
    /// - **前置条件（Contract）**：当前结构体持有的 Router、上下文构造器均已满足 `Arc` 克隆语义；
    /// - **后置条件（Contract）**：调用方可将返回值直接交给 [`Pipeline::add_handler_after`](spark_core::pipeline::Pipeline::add_handler_after)。
    pub fn into_handler(self) -> Arc<dyn Handler> {
        Arc::new(self)
    }

    /// 生成具备对象安全语义的 Handler 引用，供 PipelineInitializer 或热插拔逻辑复用。
    ///
    /// # 教案级注释
    /// - **意图（Why）**：`ChainBuilder` 在旧版 API 中仅支持 `Box<dyn InboundHandler>` 注册；
    ///   当调用方希望直接借助 [`Pipeline::add_handler_after`](spark_core::pipeline::Pipeline::add_handler_after)
    ///   或其他 Handler 契约接口装配链路时，需要显式获取 `Arc<dyn Handler>`；
    /// - **体系位置（Where）**：位于初始化器与运行时之间的桥接层，通常在 `PipelineInitializer::configure`
    ///   内根据条件决定是否安装路由 Handler；
    /// - **执行逻辑（How）**：
    ///   1. 克隆内部的 [`ApplicationRouter`] 状态，确保返回值拥有独立所有权；
    ///   2. 调用 [`Self::into_handler`] 将克隆体封装为 `Arc<dyn Handler>`；
    ///   3. 将结果交给控制器或 `ChainBuilder`，由其负责写入管线拓扑；
    /// - **契约（What）**：
    ///   - **输入**：无需额外参数，方法在现有实例上工作；
    ///   - **返回值**：满足 `Send + Sync + 'static` 的 `Arc<dyn Handler>`，可跨线程复用；
    ///   - **前置条件**：内部的 Router、上下文构造器均须保持有效的 `Arc` 语义；
    ///   - **后置条件**：调用方可多次调用本方法，每次都会得到独立的 Handler 副本；
    /// - **设计权衡（Trade-offs）**：克隆 Handler 会产生常数次 `Arc::clone` 与 `InitializerDescriptor::clone`
    ///   的开销，换取了按需装配的灵活性；若对性能极度敏感，可预先缓存返回值复用。
    pub fn handler_arc(&self) -> Arc<dyn Handler> {
        self.clone().into_handler()
    }
}

impl Handler for ApplicationRouter {
    fn direction(&self) -> HandlerDirection {
        HandlerDirection::Inbound
    }

    fn descriptor(&self) -> InitializerDescriptor {
        self.descriptor.clone()
    }

    fn clone_inbound(&self) -> Option<Arc<dyn InboundHandler>> {
        let handler: Arc<dyn InboundHandler> = Arc::new(self.clone());
        Some(handler)
    }
}

impl InboundHandler for ApplicationRouter {
    fn describe(&self) -> InitializerDescriptor {
        self.descriptor.clone()
    }

    fn on_channel_active(&self, _ctx: &dyn Context) {}

    fn on_read(&self, ctx: &dyn Context, msg: PipelineMessage) {
        let snapshot = self.router.snapshot();
        let trace_clone = ctx.trace_context().clone();
        let parts = match self.context_builder.build(ctx, &msg, snapshot) {
            Ok(parts) => parts,
            Err(err) => {
                ctx.logger().error(
                    "application-router failed to build routing context",
                    Some(&err),
                    Some(ctx.trace_context()),
                );
                return;
            }
        };

        // --- 请求元数据审查逻辑 --------------------------------------------------------
        // 教案式说明：
        // 1. **意图 (Why)**：业务路由往往依赖帧头/动态标签等元数据。若消息缺乏这类字段，路由器
        //    可能只得依赖默认路由或直接拒绝，为了在热路径中即时发现这类异常，我们在 Handler 内
        //    主动检测并输出诊断日志。
        // 2. **逻辑 (How)**：
        //    - 调用 [`PipelineMessage::user_kind`] 获取业务帧类型；
        //    - 读取 `RoutingContextParts::dynamic_metadata` 判断是否已填充标签；
        //    - 若两者任一缺失，则输出 DEBUG 日志，帮助排查上游编解码/上下文构造器是否遗漏。
        // 3. **契约 (What)**：
        //    - 仅在 `message_kind` 或 `metadata` 缺失时记录日志，避免为健康流量增加噪音；
        //    - 日志附带 `router.metadata_present`（布尔）与 `router.message_kind`（字符串）两个观测字段。
        // 4. **权衡 (Trade-offs)**：
        //    - 选择 DEBUG 级别以降低噪音，但依然为排查“路由意图缺失”问题提供第一手线索；
        //    - 诊断逻辑只读数据，不会在消息路径上复制缓冲或修改上下文，确保性能损耗可忽略。
        let message_kind = msg.user_kind();
        let metadata_empty = parts.dynamic_metadata.iter().next().is_none();
        if message_kind.is_none() || metadata_empty {
            let mut attributes = OwnedAttributeSet::new();
            attributes.push_owned("router.metadata_present", !metadata_empty);
            attributes.push_owned(
                "router.message_kind",
                message_kind.unwrap_or("<unknown>").to_owned(),
            );
            ctx.logger().debug_with_fields(
                "application-router inspected inbound message metadata",
                attributes.as_slice(),
                Some(ctx.trace_context()),
            );
        }

        let routing_ctx = RoutingContext::new(
            ctx.execution_context(),
            &msg,
            &parts.intent,
            parts.connection.as_ref(),
            &parts.dynamic_metadata,
            snapshot,
        );

        match self.router.route_dyn(routing_ctx) {
            Ok(decision) => {
                if !decision.warnings().is_empty() {
                    let mut attributes = OwnedAttributeSet::new();
                    let joined = decision
                        .warnings()
                        .iter()
                        .map(|warn| warn.as_ref())
                        .collect::<Vec<_>>()
                        .join("; ");
                    attributes.push_owned("router.warnings", joined);
                    ctx.logger().warn_with_fields(
                        "application-router received decision warnings",
                        attributes.as_slice(),
                        Some(ctx.trace_context()),
                    );
                }
                self.handle_decision(ctx, decision, msg, trace_clone);
            }
            Err(err) => {
                let spark_error = match err {
                    RouteError::NotFound { pattern, .. } => SparkError::new(
                        codes::APP_ROUTING_FAILED,
                        format!("route pattern `{pattern:?}` not found"),
                    ),
                    RouteError::PolicyDenied { reason } => SparkError::new(
                        codes::APP_ROUTING_FAILED,
                        format!("route rejected by policy: {reason}"),
                    ),
                    RouteError::ServiceUnavailable { id, source } => SparkError::new(
                        codes::APP_ROUTING_FAILED,
                        format!("service bound to route `{id:?}` is unavailable"),
                    )
                    .with_cause(source),
                    RouteError::Internal(inner) => inner,
                    other => SparkError::new(
                        codes::APP_ROUTING_FAILED,
                        format!("unexpected route error: {other:?}"),
                    ),
                };
                ctx.logger().error(
                    "application-router failed to resolve route",
                    Some(&spark_error),
                    Some(ctx.trace_context()),
                );
            }
        }
    }

    fn on_read_complete(&self, _ctx: &dyn Context) {}

    fn on_writability_changed(&self, _ctx: &dyn Context, _is_writable: bool) {}

    fn on_user_event(&self, _ctx: &dyn Context, _event: spark_core::observability::CoreUserEvent) {}

    fn on_exception_caught(&self, _ctx: &dyn Context, _error: CoreError) {}

    fn on_channel_inactive(&self, _ctx: &dyn Context) {}
}

/// 将 [`Context`] 暂借的通道引用转换为拥有型 [`Arc`]。
///
/// # 教案式说明
/// - **意图（Why）**：`ApplicationRouter` 在异步任务中需要写回响应，必须延长 [`Channel`](spark_core::pipeline::channel::Channel)
///   的生命周期；
/// - **逻辑（How）**：利用 `HotSwapContext` 内部以 [`Arc`] 保存通道的事实，先手动增加强引用计数，再通过
///   [`Arc::from_raw`] 恢复拥有权，确保未来在任务结束时安全释放；
/// - **契约（What）**：调用者需保证传入的 [`Context`] 实现确实由 [`Arc`] 创建通道引用（当前 `HotSwapContext`
///   满足该条件）；若未来更换实现，应同步调整此函数避免破坏内存安全。
fn unsafe_clone_channel(ctx: &dyn Context) -> Arc<dyn spark_core::pipeline::channel::Channel> {
    let channel_ref = ctx.channel();
    let ptr = channel_ref as *const dyn spark_core::pipeline::channel::Channel;
    unsafe {
        Arc::increment_strong_count(ptr);
        Arc::from_raw(ptr)
    }
}

/// 将 [`BoxService`] 拆箱为可变的对象层服务引用。
///
/// # 教案式说明
/// - **意图（Why）**：[`spark_core::service::DynService`] 的调用接口要求可变引用，需消除外层 [`Arc`] 封装以便执行一次性的业务调用；
/// - **逻辑（How）**：利用“路由器每次返回独占实例”的前提，直接通过 [`Arc::into_raw`] 获取裸指针，再恢复为 `Box` 管理；
/// - **契约（What）**：调用前必须确保没有额外的句柄克隆，否则将导致双重释放；本实现依赖路由器工厂按需创建服务满足该假设。
fn into_dyn_service(service: BoxService) -> Box<dyn spark_core::service::DynService> {
    let arc = service.into_arc();
    debug_assert_eq!(
        Arc::strong_count(&arc),
        1,
        "router factory must yield unique services",
    );
    let raw = Arc::into_raw(arc) as *mut dyn spark_core::service::DynService;
    unsafe { Box::from_raw(raw) }
}
