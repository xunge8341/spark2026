use alloc::{
    borrow::Cow,
    string::{String, ToString},
};
use core::{cmp::Ordering, fmt};

/// 描述配置项作用域的枚举。
///
/// ## 设计目的（Why）
/// - 对齐业内成熟配置系统（Kubernetes ConfigMap、Envoy xDS、Consul KV）的“作用域分层”理念，
///   允许调用方以统一契约表达不同部署粒度。
/// - 通过枚举而非自由字符串，确保跨语言实现时可以进行枚举校验与兼容测试。
///
/// ## 逻辑说明（How）
/// - `Global`：适用于所有节点或实例的全局默认值。
/// - `Cluster`：限定在某个逻辑集群或分组内的共享配置。
/// - `Node`：针对单个节点或主机。
/// - `Runtime`：聚焦当前进程或容器实例。
/// - `Session`：面向单个连接 / 会话的临时配置，例如限流令牌。
///
/// ## 契约定义（What）
/// - 无输入参数；作为 [`ConfigKey`] 的一个字段。
/// - 枚举值可安全转换为稳定字符串传递给其它语言实现。
///
/// ## 设计权衡与注意事项（Trade-offs）
/// - 未额外引入 `Tenant` 等领域专用枚举，避免降低通用性；若需扩展，请在上层自定义。
/// - `Session` 配置通常具备高频更新特性，使用者需评估缓存或推送策略，以免产生性能瓶颈。
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum ConfigScope {
    Global,
    Cluster,
    Node,
    Runtime,
    Session,
}

impl ConfigScope {
    /// 返回该作用域的稳定字符串描述。
    #[inline]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Global => "global",
            Self::Cluster => "cluster",
            Self::Node => "node",
            Self::Runtime => "runtime",
            Self::Session => "session",
        }
    }

    /// 根据字符串解析作用域。
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "global" => Some(Self::Global),
            "cluster" => Some(Self::Cluster),
            "node" => Some(Self::Node),
            "runtime" => Some(Self::Runtime),
            "session" => Some(Self::Session),
            _ => None,
        }
    }
}

impl fmt::Display for ConfigScope {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// 配置项的稳定标识符。
///
/// ## 设计目的（Why）
/// - 遵循 "domain + name" 的命名法，吸收 gRPC、Envoy 等对配置键的模块化实践。
/// - 提供作用域、版本与可本地化的描述信息，支撑跨团队协作与自动化文档。
///
/// ## 逻辑说明（How）
/// - `domain`：代表业务域或协议模块，例如 `transport`、`runtime`。
/// - `name`：在域内唯一的配置项名称。
/// - `scope`：参考 [`ConfigScope`]，约束该配置项的影响范围。
/// - `summary`：供 UI 或 CLI 展示的简短说明，使用 `Cow` 避免不必要拷贝。
///
/// ## 契约定义（What）
/// - **前置条件**：`domain` 与 `name` 必须符合跨语言的标识规范（推荐 `[a-z0-9_.-]`）。
/// - **输入**：构造函数接受静态或动态字符串，调用方负责确保唯一性。
/// - **返回值**：`ConfigKey` 实例本身，可拷贝或克隆以便在 `no_std` 场景重用。
///
/// ## 设计权衡与注意事项（Trade-offs）
/// - 采用 `Cow<'static, str>` 允许常量与动态注册并存，兼顾性能与灵活性。
/// - 没有在类型层面加入版本字段，避免增加使用复杂度；版本治理建议通过上层元数据实现。
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ConfigKey {
    domain: Cow<'static, str>,
    name: Cow<'static, str>,
    scope: ConfigScope,
    summary: Cow<'static, str>,
}

impl ConfigKey {
    /// 构造一个新的配置键。
    ///
    /// ### 参数（Inputs）
    /// - `domain`: 所属业务域，通常对应 crate 或服务模块名。
    /// - `name`: 配置项在域内的唯一名称。
    /// - `scope`: 配置项影响范围。
    /// - `summary`: 面向人类的简短描述。
    ///
    /// ### 前置条件（Preconditions）
    /// - `domain` 与 `name` 不能为空字符串。
    /// - 调用方需确保 `(domain, name, scope)` 组合在全局内唯一。
    ///
    /// ### 后置条件（Postconditions）
    /// - 返回的 `ConfigKey` 可安全用于 HashMap 键值、配置注册表等场景。
    pub fn new<D, N, S>(domain: D, name: N, scope: ConfigScope, summary: S) -> Self
    where
        D: Into<Cow<'static, str>>,
        N: Into<Cow<'static, str>>,
        S: Into<Cow<'static, str>>,
    {
        Self {
            domain: domain.into(),
            name: name.into(),
            scope,
            summary: summary.into(),
        }
    }

    /// 返回配置项所属业务域。
    #[inline]
    pub fn domain(&self) -> &str {
        &self.domain
    }

    /// 返回配置项名称。
    #[inline]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// 返回配置项作用域。
    #[inline]
    pub fn scope(&self) -> ConfigScope {
        self.scope
    }

    /// 返回摘要描述。
    #[inline]
    pub fn summary(&self) -> &str {
        &self.summary
    }

    /// 将配置键转换为内部可序列化表示。
    pub(crate) fn to_repr(&self) -> ConfigKeyRepr {
        ConfigKeyRepr {
            domain: self.domain().to_string(),
            name: self.name().to_string(),
            scope: self.scope().as_str().to_string(),
            summary: self.summary().to_string(),
        }
    }

    /// 根据中间表示还原配置键。
    pub(crate) fn from_repr(repr: ConfigKeyRepr) -> crate::Result<Self, ConfigKeyReprError> {
        let scope = ConfigScope::parse(&repr.scope)
            .ok_or_else(|| ConfigKeyReprError::InvalidScope(repr.scope.clone()))?;
        Ok(Self::new(repr.domain, repr.name, scope, repr.summary))
    }
}

impl fmt::Display for ConfigKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}::{}@{}", self.domain, self.name, self.scope)
    }
}

impl From<&ConfigKey> for String {
    fn from(value: &ConfigKey) -> Self {
        value.to_string()
    }
}

impl PartialOrd for ConfigKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ConfigKey {
    fn cmp(&self, other: &Self) -> Ordering {
        self.domain
            .cmp(&other.domain)
            .then_with(|| self.name.cmp(&other.name))
            .then_with(|| self.scope.as_str().cmp(other.scope.as_str()))
    }
}

/// 表示配置键在序列化往返过程中的错误。
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ConfigKeyReprError {
    InvalidScope(String),
}

impl fmt::Display for ConfigKeyReprError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidScope(scope) => write!(f, "invalid config scope: {}", scope),
        }
    }
}

pub(crate) use serde_repr::ConfigKeyRepr;

mod serde_repr {
    //! `ConfigKey` 的内部序列化表示，避免公共 API 直接依赖 `serde`。
    use super::*;
    use serde::{Deserialize, Serialize};

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    pub(crate) struct ConfigKeyRepr {
        pub domain: String,
        pub name: String,
        pub scope: String,
        pub summary: String,
    }
}
