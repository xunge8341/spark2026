use crate::host::context::HostContext;
use crate::{Error, sealed::Sealed};
use alloc::string::String;
use alloc::vec::Vec;

/// 宿主可注册的组件类别。
///
/// # 背景（Why）
/// - 参考 Dapr Building Block、Envoy Filter 分类以及 CNCF Operator 模式，将组件按照职责拆分，便于宿主做权限与资源隔离。
///
/// # 契约（What）
/// - `TransportAdapter`：处理底层传输协议转换。
/// - `ServiceRuntime`：承载业务服务生命周期，如请求路由或执行器。
/// - `ObservabilityExporter`：日志、指标、追踪导出组件。
/// - `ConfigurationProvider`：提供配置与密钥的动态拉取能力。
/// - `Extension`：轻量插件或实验特性。
/// - `Custom`：宿主自定义分类。
///
/// # 实现责任 (Implementation Responsibility)
/// - **命名约定**：推荐以组织或反向域名前缀命名，如 `acme.feature_toggle`，确保跨平台唯一。
/// - **错误处理**：当宿主（如 `Router`、`ComponentFactory`）无法识别 `Custom` 值时，必须拒绝加载并返回
///   [`crate::error::codes::ROUTER_VERSION_CONFLICT`]，同时输出告警日志帮助诊断。
/// - **禁止降级**：严禁静默忽略或回退至默认组件类别，避免以未知权限运行造成安全风险。
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum ComponentKind {
    TransportAdapter,
    ServiceRuntime,
    ObservabilityExporter,
    ConfigurationProvider,
    Extension,
    Custom(String),
}

/// 组件描述信息，用于注册与依赖声明。
///
/// # 背景（Why）
/// - 结合 Kubernetes CRD 与 HashiCorp Terraform Provider 的实践，通过结构化描述组件可观测性与依赖关系。
///
/// # 字段说明（What）
/// - `name`：组件唯一名称。
/// - `kind`：组件类别，指导宿主在正确阶段挂载。
/// - `version`：组件版本，方便热升级判断兼容性。
/// - `dependencies`：组件依赖的其他组件名称列表。
/// - `description`：人类可读描述，辅助诊断。
///
/// # 合同（Contract）
/// - **前置条件**：`name` 必须唯一；`dependencies` 中的名称由宿主负责校验存在性。
/// - **后置条件**：宿主在注册后应将描述信息纳入健康检查与可观测性事件中。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ComponentDescriptor {
    /// 组件名称。
    pub name: String,
    /// 组件类别。
    pub kind: ComponentKind,
    /// 组件版本。
    pub version: String,
    /// 依赖的其他组件。
    pub dependencies: Vec<String>,
    /// 描述信息。
    pub description: Option<String>,
}

impl ComponentDescriptor {
    /// 创建基础组件描述。
    pub fn new(name: String, kind: ComponentKind, version: String) -> Self {
        Self {
            name,
            kind,
            version,
            dependencies: Vec::new(),
            description: None,
        }
    }
}

/// 组件健康状态，参考 Kubernetes `Probe` 与 Dapr Health API。
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum ComponentHealthState {
    /// 组件已就绪，可对外提供服务。
    Ready,
    /// 组件正在初始化或等待依赖。
    Initializing,
    /// 组件处于降级模式但仍保持部分功能。
    Degraded,
    /// 组件失败，需要宿主触发恢复逻辑。
    Failed,
}

/// 组件工厂接口，定义初始化流程。
///
/// # 背景（Why）
/// - 借鉴 OSGi、Dapr Component Model 与 Wasmtime Linker 的模式，将组件实例化与宿主解耦。
///
/// # 契约（What）
/// - `Instance`：组件实例类型，宿主可自由定义。
/// - `Error`：初始化失败时返回的错误，需实现 `crate::Error`。
/// - `descriptor`：返回组件元数据，宿主用来登记与观测。
/// - `initialize`：根据宿主上下文构建组件实例。
///
/// # 前置/后置条件
/// - **前置条件**：`initialize` 调用时宿主保证传入的 `HostContext` 已包含完整能力。
/// - **后置条件**：成功返回的实例必须满足组件自己的契约，例如监听端口、注册路由等；若失败应确保未对宿主造成不可逆副作用。
///
/// # 风险提示（Trade-offs）
/// - 该接口默认同步返回实例，若组件初始化需要异步步骤，可在实例内部暴露 `Future`。
pub trait ComponentFactory: Sealed {
    /// 组件实例类型。
    type Instance;
    /// 错误类型。
    type Error: Error;

    /// 返回组件描述信息。
    fn descriptor(&self) -> &ComponentDescriptor;

    /// 初始化组件实例。
    fn initialize(&self, ctx: &HostContext) -> crate::Result<Self::Instance, Self::Error>;
}
