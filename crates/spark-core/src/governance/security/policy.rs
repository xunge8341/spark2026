//! # 策略评估引擎解耦指引（Why）
//! - **定位说明**：本模块仅定义 `SecurityPolicy` 及其相关数据结构，负责描述“要评估的策略内容”，而不内置任何执行引擎。
//! - **架构考量**：Spark Core 旨在保持轻量与可移植性，因而把决策逻辑托管给上层组件，可自由对接 OPA Rego、Google CEL 等策略引擎或自研求值器。
//!
//! # 集成建议（How）
//! 1. **外部引擎桥接**：在业务层创建一个适配器，把 `SecurityPolicy` 序列化或转换为目标引擎的输入格式，再由该引擎返回最终 `PolicyEffect`。
//! 2. **统一契约接口**：推荐为所有策略引擎实现者提供如下 Trait，统一“输入身份、资源和上下文，输出授权结果”的交互契约：
//!
//! ```rust,ignore
//! use spark_core::security::identity::IdentityDescriptor;
//! use spark_core::security::policy::{PolicyEffect, ResourcePattern, SecurityPolicy};
//!
//! /// 统一的策略评估接口，约束上层如何调用任意策略引擎。
//! pub trait PolicyEvaluator {
//!     /// - `subject`：调用者身份，如终端用户、服务账户或计算节点。
//!     /// - `resource`：被访问的资源模式，与当前策略绑定的资源应一致。
//!     /// - `context`：外部上下文（如环境变量、时区、请求标签等），其结构由上层自定义。
//!     /// - 返回值：若评估成功则返回策略效果 `PolicyEffect`，失败则返回 `EvaluationError`。
//!     fn evaluate(
//!         &self,
//!         policy: &SecurityPolicy,
//!         subject: &IdentityDescriptor,
//!         resource: &ResourcePattern,
//!         context: &EvaluationContext,
//!     ) -> crate::Result<PolicyEffect, EvaluationError>;
//! }
//!
//! /// 上层可自定义的求值上下文与错误类型示例。
//! pub struct EvaluationContext;
//! pub struct EvaluationError;
//! ```
//!
//! # 使用提醒（What & Trade-offs）
//! - **前置条件**：上层在调用策略评估接口前，应完成身份验证并准备好必要的上下文信息。
//! - **后置条件**：接口返回 `PolicyEffect` 后，需要结合业务语义落实“拒绝”“允许”或“质询”动作。
//! - **权衡提示**：当策略复杂度高时，建议缓存已编译的策略或评估结果，以平衡性能与实时性，同时确保缓存刷新机制与审计要求兼容。
use alloc::string::String;
use alloc::vec::Vec;

use super::identity::IdentityDescriptor;

/// 安全策略，聚合一组访问控制规则。
///
/// # 背景（Why）
/// - **行业范式**：融合 Kubernetes RBAC、OPA/Rego 策略与 AWS IAM Policy 的核心思想，使用显式的规则集合表达授权逻辑。
/// - **科研动机**：为属性基访问控制（ABAC）与基于策略的零信任网络提供契约，便于在后续研究中替换评估引擎。
///
/// # 字段契约（What）
/// - `id`：策略唯一标识。
/// - `version`：策略版本或修订号。
/// - `rules`：策略规则列表，顺序即优先级。
/// - `description`：可选描述，用于审计或控制台展示。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SecurityPolicy {
    id: String,
    version: Option<String>,
    rules: Vec<PolicyRule>,
    description: Option<String>,
}

impl SecurityPolicy {
    /// 构建策略。
    ///
    /// # 契约
    /// - `id` 必须唯一；`rules` 按优先级从高到低排列。
    pub fn new(id: String, rules: Vec<PolicyRule>) -> Self {
        Self {
            id,
            version: None,
            rules,
            description: None,
        }
    }

    /// 设置版本。
    pub fn with_version(mut self, version: String) -> Self {
        self.version = Some(version);
        self
    }

    /// 设置描述。
    pub fn with_description(mut self, description: String) -> Self {
        self.description = Some(description);
        self
    }

    /// 获取策略标识。
    pub fn id(&self) -> &str {
        &self.id
    }

    /// 获取版本。
    pub fn version(&self) -> Option<&String> {
        self.version.as_ref()
    }

    /// 获取规则列表。
    pub fn rules(&self) -> &Vec<PolicyRule> {
        &self.rules
    }

    /// 获取描述。
    pub fn description(&self) -> Option<&String> {
        self.description.as_ref()
    }
}

/// 策略规则，描述主体-资源-条件的组合。
///
/// # 字段契约
/// - `subjects`：匹配主体集合。
/// - `resources`：匹配资源集合。
/// - `effect`：允许、拒绝或发起质询。
/// - `conditions`：额外键值条件，如时间段、区域等。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PolicyRule {
    subjects: Vec<SubjectMatcher>,
    resources: Vec<ResourcePattern>,
    effect: PolicyEffect,
    conditions: Vec<(String, String)>,
}

impl PolicyRule {
    /// 构造规则。
    pub fn new(
        subjects: Vec<SubjectMatcher>,
        resources: Vec<ResourcePattern>,
        effect: PolicyEffect,
    ) -> Self {
        Self {
            subjects,
            resources,
            effect,
            conditions: Vec::new(),
        }
    }

    /// 添加条件。
    ///
    /// # 用法
    /// - 例如 `("region", "cn-shanghai")`、`("time", "08:00-18:00")`。
    pub fn add_condition(mut self, key: String, value: String) -> Self {
        self.conditions.push((key, value));
        self
    }

    /// 获取主体匹配器列表。
    pub fn subjects(&self) -> &Vec<SubjectMatcher> {
        &self.subjects
    }

    /// 获取资源模式列表。
    pub fn resources(&self) -> &Vec<ResourcePattern> {
        &self.resources
    }

    /// 获取效果。
    pub fn effect(&self) -> PolicyEffect {
        self.effect
    }

    /// 获取条件。
    pub fn conditions(&self) -> &Vec<(String, String)> {
        &self.conditions
    }
}

/// 策略效果枚举。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum PolicyEffect {
    /// 允许访问。
    Allow,
    /// 拒绝访问。
    Deny,
    /// 发起额外校验或多因子认证。
    Challenge,
}

/// 主体匹配器，支持精确匹配、前缀匹配与标签匹配。
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum SubjectMatcher {
    /// 匹配任意主体。
    Any,
    /// 精确匹配指定身份。
    Identity(IdentityDescriptor),
    /// 依据签发方与名称前缀匹配。
    IdentityPrefix {
        authority: String,
        name_prefix: String,
    },
    /// 按标签匹配，如 `role=admin`。
    Labels(Vec<(String, String)>),
}

/// 资源模式，描述可访问的目标。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResourcePattern {
    namespace: String,
    name: Option<String>,
    actions: Vec<String>,
    labels: Vec<(String, String)>,
}

impl ResourcePattern {
    /// 构建资源模式。
    ///
    /// # 契约
    /// - `namespace`：资源大类，如 `service`, `topic`, `bucket`。
    /// - `name`：可选具体名称，`None` 表示通配。
    /// - `actions`：允许的动作列表，如 `read`, `write`。
    pub fn new(namespace: String) -> Self {
        Self {
            namespace,
            name: None,
            actions: Vec::new(),
            labels: Vec::new(),
        }
    }

    /// 指定资源名称。
    pub fn with_name(mut self, name: String) -> Self {
        self.name = Some(name);
        self
    }

    /// 附加动作。
    pub fn add_action(mut self, action: String) -> Self {
        self.actions.push(action);
        self
    }

    /// 附加标签。
    pub fn add_label(mut self, key: String, value: String) -> Self {
        self.labels.push((key, value));
        self
    }

    /// 获取命名空间。
    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    /// 获取名称。
    pub fn name(&self) -> Option<&String> {
        self.name.as_ref()
    }

    /// 获取动作列表。
    pub fn actions(&self) -> &Vec<String> {
        &self.actions
    }

    /// 获取标签。
    pub fn labels(&self) -> &Vec<(String, String)> {
        &self.labels
    }
}

/// 策略挂载声明，将策略与具体作用域绑定。
///
/// # 用途
/// - 参考 Istio AuthorizationPolicy 与 AWS IAM Policy Attachment，将策略分发至路由、服务实例或集群级别。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PolicyAttachment {
    policy: SecurityPolicy,
    scope: String,
    priority: i32,
}

impl PolicyAttachment {
    /// 构建挂载。
    ///
    /// # 契约
    /// - `scope`：自定义作用域标识，如 `route:order-service`, `cluster:edge-a`。
    /// - `priority`：数值越大优先级越高，与规则内顺序共同决定最终评估顺序。
    pub fn new(policy: SecurityPolicy, scope: String, priority: i32) -> Self {
        Self {
            policy,
            scope,
            priority,
        }
    }

    /// 获取策略。
    pub fn policy(&self) -> &SecurityPolicy {
        &self.policy
    }

    /// 获取作用域。
    pub fn scope(&self) -> &str {
        &self.scope
    }

    /// 获取优先级。
    pub fn priority(&self) -> i32 {
        self.priority
    }
}
