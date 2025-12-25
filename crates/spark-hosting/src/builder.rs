use core::fmt;

#[cfg(feature = "alloc")]
use alloc::boxed::Box;
#[cfg(feature = "switch")]
use alloc::sync::Arc;

use crate::{
    host::Host,
    pipeline::{MiddlewareRegistrationError, MiddlewareRegistry},
    service::{ServiceRegistrationError, ServiceRegistry},
};
use spark_core::configuration::{
    BuildError, ConfigurationBuilder, DynConfigurationSource, SourceRegistrationError,
};
#[cfg(feature = "switch")]
use spark_switch::{
    applications::{
        location::LocationStore, proxy::ProxyServiceFactory, registrar::RegistrarServiceFactory,
    },
    core::SessionManager,
};

/// 构建宿主时出现的配置阶段错误。
#[derive(Debug)]
pub enum HostBuilderError {
    /// 配置源注册失败。
    ConfigurationSource(SourceRegistrationError),
    /// 服务注册冲突。
    Service(ServiceRegistrationError),
    /// 中间件注册冲突。
    Middleware(MiddlewareRegistrationError),
}

impl fmt::Display for HostBuilderError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            HostBuilderError::ConfigurationSource(error) => {
                write!(f, "configuration source registration failed: {error}")
            }
            HostBuilderError::Service(error) => {
                write!(f, "service registration failed: {error}")
            }
            HostBuilderError::Middleware(error) => {
                write!(f, "middleware registration failed: {error}")
            }
        }
    }
}

impl spark_core::Error for HostBuilderError {
    fn source(&self) -> Option<&(dyn spark_core::Error + 'static)> {
        match self {
            HostBuilderError::ConfigurationSource(error) => Some(error),
            HostBuilderError::Service(error) => Some(error),
            HostBuilderError::Middleware(error) => Some(error),
        }
    }
}

impl From<SourceRegistrationError> for HostBuilderError {
    fn from(value: SourceRegistrationError) -> Self {
        Self::ConfigurationSource(value)
    }
}

impl From<ServiceRegistrationError> for HostBuilderError {
    fn from(value: ServiceRegistrationError) -> Self {
        Self::Service(value)
    }
}

impl From<MiddlewareRegistrationError> for HostBuilderError {
    fn from(value: MiddlewareRegistrationError) -> Self {
        Self::Middleware(value)
    }
}

/// 构建宿主最终失败时的错误。
#[derive(Debug)]
pub enum HostBuildError {
    /// 配置构建流程失败。
    Configuration(BuildError),
}

impl fmt::Display for HostBuildError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            HostBuildError::Configuration(error) => {
                write!(f, "configuration build failed: {error}")
            }
        }
    }
}

impl spark_core::Error for HostBuildError {
    fn source(&self) -> Option<&(dyn spark_core::Error + 'static)> {
        match self {
            HostBuildError::Configuration(error) => Some(error),
        }
    }
}

impl From<BuildError> for HostBuildError {
    fn from(value: BuildError) -> Self {
        Self::Configuration(value)
    }
}

/// `HostBuilder` 聚合配置、服务与中间件的装配步骤。
///
/// # 教案级注释
/// - **设计目标 (Why)**
///   - 为宿主提供一个统一的装配入口，避免在应用层重复处理配置加载、
///     服务注册与 Pipeline 中间件链路的杂务；
///   - 借鉴 .NET Generic Host、Envoy Bootstrap Builder 等实践，确保宿主可以以声明式方式逐步配置所需组件。
/// - **体系位置 (Where)**
///   - 属于 `spark-hosting` crate 的核心类型；
///   - 在应用启动阶段初始化，并在 `build` 之后产生 [`Host`] 供运行时使用。
/// - **关键流程 (How)**
///   1. `configure_configuration`：装载 [`ConfigurationBuilder`]，注册多个 [`DynConfigurationSource`]；
///   2. `configure_services`：向 [`ServiceRegistry`] 写入对象层服务或工厂；
///   3. `configure_pipeline`：登记链路中间件；
///   4. `build`：执行配置构建并打包整体状态。
/// - **契约说明 (What)**
///   - 所有配置步骤均返回 `Result<&mut Self, HostBuilderError>`，便于链式调用与错误传播；
///   - `build` 可能返回 [`HostBuildError::Configuration`]，用于揭示配置加载失败的详细原因。
/// - **风险提示 (Trade-offs)**
///   - Builder 本身并不持有生命周期管理逻辑，若宿主需要延迟释放或热更新，需要在 `Host` 层自行处理；
///   - 当前 `configure_*` 方法同步执行闭包，若闭包内部执行耗时操作，需注意不要阻塞启动流程。
#[derive(Default)]
pub struct HostBuilder {
    configuration: ConfigurationBuilder,
    services: ServiceRegistry,
    middleware: MiddlewareRegistry,
    #[cfg(feature = "switch")]
    switch_services: Option<SwitchServiceFactories>,
}

impl fmt::Debug for HostBuilder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let service_count = self.services.iter().count();
        let middleware_count = self.middleware.iter().count();
        #[cfg(feature = "switch")]
        let has_switch_services = self.switch_services.is_some();
        let mut debug = f.debug_struct("HostBuilder");
        debug
            .field("configuration", &"ConfigurationBuilder{...}")
            .field("service_count", &service_count)
            .field("middleware_count", &middleware_count);
        #[cfg(feature = "switch")]
        {
            debug.field("switch_services", &has_switch_services);
        }
        debug.finish()
    }
}

impl HostBuilder {
    /// 创建空的 Builder。
    ///
    /// - **前置条件**：无；
    /// - **后置条件**：内部的 `ConfigurationBuilder` 尚未设置 Profile，也未注册任何配置源。
    pub fn new() -> Self {
        Self::default()
    }

    /// 配置配置子系统，常用于注册配置源或调整 Profile。
    ///
    /// # 教案级注释
    /// - **输入参数**：`configure` 闭包接收 [`ConfigurationBuilder`] 的可变引用；
    /// - **前置条件**：闭包需保持幂等性，避免多次调用导致重复注册；
    /// - **执行逻辑 (How)**：立即执行闭包，若返回错误则中止后续链式调用；
    /// - **返回值 (What)**：链式返回 `&mut Self`，方便连续调用其他配置方法。
    pub fn configure_configuration<F>(
        &mut self,
        configure: F,
    ) -> Result<&mut Self, HostBuilderError>
    where
        F: FnOnce(&mut ConfigurationBuilder) -> Result<(), HostBuilderError>,
    {
        configure(&mut self.configuration)?;
        Ok(self)
    }

    /// 配置服务注册表，支持注册对象层服务或工厂。
    ///
    /// # 教案级注释
    /// - **意图 (Why)**：在启动阶段集中登记所有业务服务，让路由或控制器在运行时能够按名称查找；
    /// - **执行逻辑 (How)**：立即调用传入闭包，闭包内部可使用 [`ServiceRegistry`] 的注册接口；
    /// - **契约 (What)**：闭包需返回 `Result` 以显式传播注册错误；若失败，Builder 状态保持在调用前的值；
    /// - **风险提示**：闭包应避免进行耗时 I/O，防止阻塞装配流程；重复调用需自行确保幂等。
    pub fn configure_services<F>(&mut self, configure: F) -> Result<&mut Self, HostBuilderError>
    where
        F: FnOnce(&mut ServiceRegistry) -> Result<(), HostBuilderError>,
    {
        configure(&mut self.services)?;
        Ok(self)
    }

    /// 配置 Pipeline 中间件注册表。
    ///
    /// # 教案级注释
    /// - **动机 (Why)**：将鉴权、观测、限流等中间件在装配阶段集中登记，确保后续链路按预期顺序执行；
    /// - **执行方式 (How)**：同步执行闭包，闭包可调用 [`MiddlewareRegistry`] 的 `register` 方法添加条目；
    /// - **契约 (What)**：闭包返回错误时整体调用失败，避免部分注册成功导致状态不一致；
    /// - **风险提示**：若需要根据配置决定顺序，请在闭包内部进行排序后再注册，以保持确定性。
    pub fn configure_pipeline<F>(&mut self, configure: F) -> Result<&mut Self, HostBuilderError>
    where
        F: FnOnce(&mut MiddlewareRegistry) -> Result<(), HostBuilderError>,
    {
        configure(&mut self.middleware)?;
        Ok(self)
    }

    /// 初始化 spark-switch 运行所需的共享状态，并缓存 ServiceFactory。
    ///
    /// # 教案级注释
    /// - **意图 (Why)**：
    ///   1. 集中创建 [`SessionManager`] 与 [`LocationStore`]，避免在应用层重复拼装；
    ///   2. 为 REGISTER/INVITE 等 SIP 路由准备 `ServiceFactory`，以便后续在 `spark-router`
    ///      中一次性注册；
    /// - **体系位置 (Where)**：
    ///   - 属于 `HostBuilder` 的可选配置步骤，仅在启用 `switch` feature 时有效；
    ///   - 产物会保存在内部的 [`SwitchServiceFactories`]，并在 `Host::build_router`
    ///     过程中被消费；
    /// - **执行逻辑 (How)**：
    ///   1. 构造共享的 `LocationStore` 与 `SessionManager`；
    ///   2. 以上述共享状态创建 `RegistrarServiceFactory` 与 `ProxyServiceFactory`；
    ///   3. 将工厂打包到 `SwitchServiceFactories` 并存入 Builder；
    /// - **契约 (What)**：
    ///   - 返回 `&mut Self` 以支持链式调用；
    ///   - 重复调用会覆盖旧的工厂缓存，便于在测试中注入新的共享状态；
    /// - **风险提示 (Trade-offs)**：
    ///   - 当前实现使用内存内 `DashMap` 保存状态，若宿主需要持久化，请在更高层构造器中
    ///     替换为自定义的 `LocationStore`/`SessionManager`；
    ///   - 该方法假定运行在 `std` 环境下，若在 `no_std` 场景调用会因缺少 `switch` feature 而编译失败。
    #[cfg(feature = "switch")]
    pub fn with_switch_services(&mut self) -> &mut Self {
        let location_store = Arc::new(LocationStore::new());
        let session_manager = Arc::new(SessionManager::new());

        let registrar_factory = Arc::new(RegistrarServiceFactory::new(Arc::clone(&location_store)));
        let proxy_factory = Arc::new(ProxyServiceFactory::new(
            Arc::clone(&location_store),
            Arc::clone(&session_manager),
        ));

        self.switch_services = Some(SwitchServiceFactories::new(
            registrar_factory,
            proxy_factory,
        ));
        self
    }

    /// 最终构建宿主实例。
    ///
    /// # 教案级注释
    /// - **执行步骤**
    ///   1. 调用内部的 [`ConfigurationBuilder::build`]，生成 `ConfigurationHandle`；
    ///   2. 若配置构建失败，错误会被映射为 [`HostBuildError::Configuration`]；
    ///   3. 将服务与中间件注册表移动到 [`Host`]，保持装配产物的一致性。
    /// - **前置条件**：必须在此前通过 `configure_configuration` 指定 Profile 并注册至少一个配置源；
    /// - **后置条件**：成功返回的 [`Host`] 拥有配置句柄与注册表的所有权。
    pub fn build(self) -> Result<Host, HostBuildError> {
        let HostBuilder {
            configuration,
            services,
            middleware,
            #[cfg(feature = "switch")]
            switch_services,
        } = self;
        let outcome = configuration.build().map_err(HostBuildError::from)?;
        Ok(Host::new(
            outcome.handle,
            outcome.initial,
            outcome.report,
            services,
            middleware,
            #[cfg(feature = "switch")]
            switch_services,
        ))
    }

    /// 方便注册 `'static` 配置源引用的辅助函数。
    ///
    /// # 教案级注释
    /// - **场景 (Why)**：对于编译期常驻的静态配置源（如内存快照），业务往往只持有 `'static` 引用；
    /// - **操作步骤 (How)**：内部直接复用 [`ConfigurationBuilder::register_source`]，并将错误转换为 [`HostBuilderError`]；
    /// - **契约 (What)**：调用成功后配置源被纳入 Builder 管理，失败会返回重复/容量超限等结构化错误；
    /// - **风险提示**：若配置源实际并非 `'static`，请避免通过此方法注册，以免造成悬垂引用。
    pub fn register_configuration_source(
        &mut self,
        source: Box<dyn DynConfigurationSource>,
    ) -> Result<&mut Self, HostBuilderError> {
        self.configuration
            .register_source(source)
            .map_err(HostBuilderError::from)?;
        Ok(self)
    }
}

#[cfg(feature = "switch")]
#[derive(Clone, Debug)]
pub(crate) struct SwitchServiceFactories {
    /// Registrar 路由对应的 ServiceFactory。使用 `Arc` 以便 router 在多线程中克隆。
    registrar: Arc<dyn spark_router::ServiceFactory>,
    /// Proxy 路由对应的 ServiceFactory。
    proxy: Arc<dyn spark_router::ServiceFactory>,
}

#[cfg(feature = "switch")]
impl SwitchServiceFactories {
    /// 构造包装后的工厂集合。
    fn new(
        registrar: Arc<dyn spark_router::ServiceFactory>,
        proxy: Arc<dyn spark_router::ServiceFactory>,
    ) -> Self {
        Self { registrar, proxy }
    }

    /// 克隆 Registrar 工厂引用，供路由注册使用。
    pub(crate) fn registrar_factory(&self) -> Arc<dyn spark_router::ServiceFactory> {
        Arc::clone(&self.registrar)
    }

    /// 克隆 Proxy 工厂引用。
    pub(crate) fn proxy_factory(&self) -> Arc<dyn spark_router::ServiceFactory> {
        Arc::clone(&self.proxy)
    }
}
