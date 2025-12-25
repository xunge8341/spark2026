use core::fmt;

#[cfg(feature = "alloc")]
use alloc::{collections::BTreeMap, string::String, sync::Arc, vec::Vec};

use spark_core::{
    Error as SparkErrorTrait, Result as SparkResult, SparkError, service::BoxService,
};

/// `ServiceFactory` 抽象出“按需创建对象层服务实例”的契约。
///
/// # 教案级注释
/// - **意图 (Why)**
///   - 宿主在启动阶段通常只注册服务描述，而不立即实例化全部服务，以便后续按需惰性创建或结合运行时依赖。
///   - 统一的工厂接口帮助宿主在构建 Pipeline、路由或组件时复用相同的装配流程。
/// - **角色定位 (Where)**
///   - 位于 `spark-hosting` 的服务注册子系统，供 [`HostBuilder`](crate::builder::HostBuilder) 和运行时协调逻辑调用。
/// - **关键设计 (How)**
///   - `create` 方法接收运行时依赖（目前留空，后续可扩展），返回框架内置的 [`BoxService`]。
///   - Trait 需满足 `Send + Sync + 'static`，以支持跨线程缓存与延迟实例化。
/// - **契约说明 (What)**
///   - 返回结果使用 `spark_core::Result`，错误类型统一为 [`SparkError`]，方便和现有监控/日志体系对齐。
///   - 未来若需要注入 `CoreServices` 等依赖，可在保持向后兼容的情况下为 `create` 增加参数。
/// - **风险提示 (Trade-offs)**
///   - 工厂默认每次调用都需返回全新实例；若需共享实例，可返回内部使用 `Arc` 复用的 [`BoxService`]。
pub trait ServiceFactory: Send + Sync + 'static {
    /// 生成一个新的服务实例。
    fn create(&self) -> SparkResult<BoxService, SparkError>;
}

/// 服务注册项，封装直接实例或惰性工厂的两种形态。
#[derive(Clone)]
pub enum ServiceEntry {
    /// 立即可用的对象层服务实例。
    Instance(BoxService),
    /// 延迟创建的服务工厂。
    Factory(Arc<dyn ServiceFactory>),
}

impl fmt::Debug for ServiceEntry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ServiceEntry::Instance(_) => f
                .debug_tuple("ServiceEntry::Instance")
                .field(&"BoxService")
                .finish(),
            ServiceEntry::Factory(_) => f
                .debug_tuple("ServiceEntry::Factory")
                .field(&"ServiceFactory")
                .finish(),
        }
    }
}

/// 注册服务时可能遇到的错误。
#[derive(Debug)]
pub enum ServiceRegistrationError {
    /// 名称已被占用，禁止重复注册。
    Duplicate { name: String },
}

impl fmt::Display for ServiceRegistrationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ServiceRegistrationError::Duplicate { name } => {
                write!(f, "service `{name}` already registered")
            }
        }
    }
}

impl SparkErrorTrait for ServiceRegistrationError {
    fn source(&self) -> Option<&(dyn SparkErrorTrait + 'static)> {
        None
    }
}

/// `ServiceRegistry` 维护宿主可见的服务目录。
///
/// # 教案级注释
/// - **目标 (Why)**
///   - 在 Host 构建阶段集中登记所有数据平面服务，方便后续根据名称查找或批量实例化。
///   - 避免不同组件各自维护散乱的 `HashMap`，保证命名冲突在注册时即被捕获。
/// - **架构位置 (Where)**
///   - 位于 `spark-hosting` crate 内部，被 [`HostBuilder`](crate::builder::HostBuilder)、[`Host`](crate::Host) 以及测试场景共享。
/// - **设计要点 (How)**
///   - 内部使用 `BTreeMap<String, ServiceEntry>`：
///     - `BTreeMap` 保证遍历顺序稳定，便于配置快照或文档输出；
///     - 值类型使用 [`ServiceEntry`]，同时支持实例与工厂两类注册。
///   - 对外暴露 `register_instance` 与 `register_factory`，均返回结构化错误，便于上层处理。
/// - **契约 (What)**
///   - 名称使用 `String`，默认区分大小写；调用方需自行约束命名规范（如 `service.rpc.echo`）。
///   - 若尝试重复注册同名服务，会返回 [`ServiceRegistrationError::Duplicate`]。
/// - **风险与注意事项 (Trade-offs)**
///   - 注册后返回的 `ServiceEntry` 仍可被克隆；若想阻止后续修改，可在构建完成后将注册表包装进 `Arc`。
///   - 未内置生命周期管理，宿主需在合适时机触发工厂创建并负责回收。
#[derive(Debug, Default, Clone)]
pub struct ServiceRegistry {
    entries: BTreeMap<String, ServiceEntry>,
}

impl ServiceRegistry {
    /// 创建空的注册表。
    pub fn new() -> Self {
        Self::default()
    }

    /// 注册已实例化的对象层服务。
    ///
    /// # 教案级注释
    /// - **输入参数**
    ///   - `name`：服务的稳定标识，通常采用 `domain.component` 命名；
    ///   - `service`：已经准备好的 [`BoxService`]，内部通常包裹 `Arc<dyn DynService>`；
    /// - **前置条件**
    ///   - 调用点应确保尚未注册同名服务；
    ///   - 服务实例需要满足线程安全要求，能够在后续运行时阶段被多个组件共享。
    /// - **执行逻辑 (How)**
    ///   1. 将名称标准化为 `String`；
    ///   2. 检查映射表是否存在冲突；
    ///   3. 插入 `ServiceEntry::Instance`。
    /// - **返回值 (What)**
    ///   - 成功返回 `()`；
    ///   - 若重名则返回 [`ServiceRegistrationError::Duplicate`]。
    pub fn register_instance(
        &mut self,
        name: impl Into<String>,
        service: BoxService,
    ) -> SparkResult<(), ServiceRegistrationError> {
        let name = name.into();
        if self.entries.contains_key(&name) {
            return Err(ServiceRegistrationError::Duplicate { name });
        }
        self.entries.insert(name, ServiceEntry::Instance(service));
        Ok(())
    }

    /// 注册惰性创建的服务工厂。
    ///
    /// # 教案级注释
    /// - **输入参数**
    ///   - `name`：服务标识；
    ///   - `factory`：实现 [`ServiceFactory`] 的对象，通常捕获配置或运行时依赖。
    /// - **设计考虑**
    ///   - 工厂使用 `Arc` 封装以便在多个宿主组件间复用；
    ///   - 注册时同样检测命名冲突，避免后续惰性创建阶段才发现异常。
    /// - **返回值/错误处理**
    ///   - 返回值与 [`register_instance`](Self::register_instance) 一致。
    pub fn register_factory(
        &mut self,
        name: impl Into<String>,
        factory: Arc<dyn ServiceFactory>,
    ) -> SparkResult<(), ServiceRegistrationError> {
        let name = name.into();
        if self.entries.contains_key(&name) {
            return Err(ServiceRegistrationError::Duplicate { name });
        }
        self.entries.insert(name, ServiceEntry::Factory(factory));
        Ok(())
    }

    /// 按名称读取注册项。
    ///
    /// # 教案级注释
    /// - **意图 (Why)**：允许宿主在运行时根据名称查找具体服务或工厂，实现延迟实例化与依赖注入；
    /// - **契约 (What)**：返回引用只读，不会改变注册状态；未命中时返回 `None`；
    /// - **风险提示**：调用方需自行处理 `None`，避免在运行时出现未注册却被引用的名称。
    pub fn get(&self, name: &str) -> Option<&ServiceEntry> {
        self.entries.get(name)
    }

    /// 返回所有注册项的有序列表。
    ///
    /// # 教案级注释
    /// - **用途 (Why)**：在调试或生成文档时，需要遍历全部注册内容并保持确定顺序；
    /// - **输出 (What)**：返回迭代器，条目顺序与 `BTreeMap` 顺序一致；
    /// - **注意事项**：迭代器借用注册表，使用期间不可同时调用可变方法。
    pub fn iter(&self) -> impl Iterator<Item = (&String, &ServiceEntry)> {
        self.entries.iter()
    }

    /// 将注册表消费为内部向量，便于一次性移动所有权。
    ///
    /// # 教案级注释
    /// - **场景 (Why)**：宿主构建 `Host` 后，可能希望将注册信息交给运行时其他组件接管；
    /// - **行为 (How)**：消费 `self`，将 `BTreeMap` 转换为拥有所有权的 `Vec`；
    /// - **后置条件 (What)**：调用后原注册表失效，确保不会遗留重复持有的状态。
    pub fn into_entries(self) -> Vec<(String, ServiceEntry)> {
        self.entries.into_iter().collect()
    }
}
