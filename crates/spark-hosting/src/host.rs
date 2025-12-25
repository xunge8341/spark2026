use spark_core::configuration::{BuildReport, ConfigurationHandle, ResolvedConfiguration};
#[cfg(feature = "switch")]
use spark_core::router::{
    metadata::RouteMetadata,
    route::{RouteId, RouteKind, RoutePattern, RouteSegment},
};
#[cfg(feature = "switch")]
use spark_router::{DefaultRouter, RouteRegistration};

#[cfg(feature = "switch")]
use crate::builder::SwitchServiceFactories;
use crate::{MiddlewareRegistry, ServiceRegistry};
#[cfg(feature = "switch")]
use alloc::{borrow::Cow, sync::Arc, vec::Vec};

/// `Host` 封装宿主运行时所需的核心构件。
///
/// # 教案级注释
/// - **设计目的 (Why)**
///   - 将配置句柄、服务注册表与中间件目录集中保存在一个结构体中，
///     便于宿主在完成装配后以单一入口向下传递依赖。
///   - 提供对初始配置与构建报告的访问，方便审计与可观测组件在启动时记录状态。
/// - **体系位置 (Where)**
///   - 该结构由 [`HostBuilder`](crate::builder::HostBuilder) 生成，
///     并在运行时启动阶段交由宿主主循环持有。
/// - **关键要素 (How)**
///   - `configuration_handle`：用于后续读取/订阅配置；
///   - `initial_configuration`：首个解析结果，便于快速渲染诊断信息；
///   - `configuration_report`：校验 & 装配报告，帮助定位构建失败原因；
///   - `services` 与 `middleware`：提供对数据平面组件的有序访问。
///   - `switch_services`（仅在启用 `switch` feature 时存在）：缓存 spark-switch 提供的
///     SIP 服务工厂，便于一键注册到 `spark-router`。
/// - **契约说明 (What)**
///   - 结构体自身不执行任何 I/O，仅存放装配阶段的产物；
///   - 调用方在读取配置句柄的可变引用时，需确保不会在多线程环境下产生数据竞争。
/// - **风险提示 (Trade-offs)**
///   - 当前未内置生命周期管理或热更新逻辑；若宿主需要在运行时动态调整注册表，
///     请自行在外部加锁或复制必要的数据结构。
pub struct Host {
    configuration_handle: ConfigurationHandle,
    initial_configuration: ResolvedConfiguration,
    configuration_report: BuildReport,
    services: ServiceRegistry,
    middleware: MiddlewareRegistry,
    #[cfg(feature = "switch")]
    switch_services: Option<SwitchServiceFactories>,
}

impl core::fmt::Debug for Host {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        let profile = self.configuration_handle.profile();
        let service_count = self.services.iter().count();
        let middleware_count = self.middleware.iter().count();
        #[cfg(feature = "switch")]
        let has_switch_routes = self.switch_services.is_some();
        let mut debug = f.debug_struct("Host");
        debug
            .field("profile", &profile.identifier.as_str())
            .field("service_count", &service_count)
            .field("middleware_count", &middleware_count);
        #[cfg(feature = "switch")]
        {
            debug.field("switch_routes", &has_switch_routes);
        }
        debug.finish()
    }
}

impl Host {
    /// 以构建成果创建宿主实例。
    pub(crate) fn new(
        configuration_handle: ConfigurationHandle,
        initial_configuration: ResolvedConfiguration,
        configuration_report: BuildReport,
        services: ServiceRegistry,
        middleware: MiddlewareRegistry,
        #[cfg(feature = "switch")] switch_services: Option<SwitchServiceFactories>,
    ) -> Self {
        Self {
            configuration_handle,
            initial_configuration,
            configuration_report,
            services,
            middleware,
            #[cfg(feature = "switch")]
            switch_services,
        }
    }

    /// 获取配置句柄的可变引用，以便读取快照或订阅增量更新。
    ///
    /// - **前置条件**：调用方在多线程环境中必须保证互斥访问；
    /// - **后置条件**：返回的引用在使用期间保持有效，宿主仍拥有所有权。
    pub fn configuration_handle(&mut self) -> &mut ConfigurationHandle {
        &mut self.configuration_handle
    }

    /// 返回初始化阶段解析出的配置快照。
    ///
    /// - **语义说明**：该快照在 Host 构建完成时即固定，不随后续增量更新而变化；
    /// - **使用建议**：可用于启动日志或健康检查的静态基线。
    pub fn initial_configuration(&self) -> &ResolvedConfiguration {
        &self.initial_configuration
    }

    /// 获取配置构建报告，包含校验记录与脱敏快照摘要。
    pub fn configuration_report(&self) -> &BuildReport {
        &self.configuration_report
    }

    /// 访问服务注册表的只读视图。
    ///
    /// - **风险提示**：返回值可被克隆并独立使用，但不会自动跟踪运行时状态变化。
    pub fn services(&self) -> &ServiceRegistry {
        &self.services
    }

    /// 访问中间件注册表。
    pub fn middleware(&self) -> &MiddlewareRegistry {
        &self.middleware
    }

    /// 构建默认 Router，并在启用 `switch` 时注册 SIP 相关 ServiceFactory。
    ///
    /// # 教案级注释
    /// - **意图 (Why)**：
    ///   - 将 `spark-switch` 提供的 Registrar/Proxy 服务自动挂载到 `spark-router`，
    ///     避免调用方手动拼装路由注册逻辑；
    ///   - 确保宿主在构建路由器时即可获得 SIP REGISTER / INVITE 的最小可用能力；
    /// - **体系位置 (Where)**：
    ///   - 该方法位于 [`Host`] 实例上，通常在完成配置装配后由运行时调用，
    ///     以便生成 `ArcSwap` 驱动的 `DefaultRouter`；
    /// - **执行逻辑 (How)**：
    ///   1. 创建空的 [`DefaultRouter`]；
    ///   2. 若 `HostBuilder` 先前调用了 [`HostBuilder::with_switch_services`](crate::builder::HostBuilder::with_switch_services)，
    ///      则组装对应的 [`RouteRegistration`] 列表；
    ///   3. 通过 [`DefaultRouter::update`] 将路由批量写入，revision 暂固定为 `1`；
    /// - **契约 (What)**：
    ///   - 始终返回可用的 `DefaultRouter`；若未启用 `switch` 或未注册工厂，则返回空路由表；
    /// - **风险提示 (Trade-offs)**：
    ///   - 当前 revision 写死，未来若需要多次更新可由调用方在返回的路由器上继续调用 `add_route`；
    ///   - 路由匹配模式为纯字面量，不支持参数/通配符；如需拓展需同步调整路由常量。
    #[cfg(feature = "switch")]
    pub fn build_router(&self) -> DefaultRouter {
        let router = DefaultRouter::new();
        if let Some(factories) = &self.switch_services {
            let registrations = assemble_switch_registrations(factories);
            if !registrations.is_empty() {
                router.update(1, registrations);
            }
        }
        router
    }
}

#[cfg(feature = "switch")]
const SIP_REGISTER_ROUTE: [&str; 3] = ["sip", "registrar", "register"];
#[cfg(feature = "switch")]
const SIP_INVITE_ROUTE: [&str; 3] = ["sip", "proxy", "invite"];

/// 依据宿主缓存的工厂集合构建 SIP 路由注册信息。
///
/// # 教案式说明
/// - **Why**：集中维护 REGISTER/INVITE 的路由拓扑，避免在多个调用点重复构造。
/// - **How**：克隆工厂引用后生成 [`RouteRegistration`]，当前固定产出两条路由。
/// - **What**：返回稳定顺序的 `Vec<RouteRegistration>`，供 [`DefaultRouter::update`] 直接消费。
#[cfg(feature = "switch")]
fn assemble_switch_registrations(factories: &SwitchServiceFactories) -> Vec<RouteRegistration> {
    let mut registrations = Vec::with_capacity(2);
    registrations.push(build_route_registration(
        &SIP_REGISTER_ROUTE,
        factories.registrar_factory(),
    ));
    registrations.push(build_route_registration(
        &SIP_INVITE_ROUTE,
        factories.proxy_factory(),
    ));
    registrations
}

/// 基于给定的字面量段创建路由注册条目。
///
/// - **前置条件**：`segments` 需按业务约定给出稳定顺序的字面量；
/// - **执行逻辑**：
///   1. 将字面量转换为 [`RouteSegment::Literal`] 构建 [`RoutePattern`]；
///   2. 同步生成对应的 [`RouteId`]；
///   3. 以空的 [`RouteMetadata`] 组装 [`RouteRegistration`]；
/// - **后置条件**：返回的路由注册条目可直接交由 Router 更新。
#[cfg(feature = "switch")]
fn build_route_registration(
    segments: &[&'static str],
    factory: Arc<dyn spark_router::ServiceFactory>,
) -> RouteRegistration {
    let mut literal_segments = Vec::with_capacity(segments.len());
    let mut literal_ids = Vec::with_capacity(segments.len());
    for &segment in segments {
        literal_segments.push(RouteSegment::Literal(Cow::Borrowed(segment)));
        literal_ids.push(Cow::Borrowed(segment));
    }

    let pattern = RoutePattern::new(RouteKind::Rpc, literal_segments);
    let id = RouteId::new(RouteKind::Rpc, literal_ids);

    RouteRegistration {
        id,
        pattern,
        metadata: RouteMetadata::new(),
        factory,
    }
}
