use alloc::borrow::Cow;
use alloc::vec::Vec;

/// 配置档案（Profile）的层级策略。
///
/// ### 设计目的（Why）
/// - 参考 AWS AppConfig 与 Istio `DestinationRule` 的环境分层经验，允许同一套配置在不同环境中演进。
/// - 通过显式的拓扑顺序（从基础到覆盖）指导配置合并算法，实现可预测的覆盖关系。
///
/// ### 逻辑说明（How）
/// - `BaseFirst`：先应用基础 Profile，再叠加后续 Profile，典型场景是 `prod` 覆盖 `base`。
/// - `OverrideFirst`：优先使用最具体的 Profile，再回退到上层，常用于“局部覆盖 + 默认回退”的读取策略。
///
/// ### 契约定义（What）
/// - 该策略由 [`ProfileDescriptor`] 保存，供 [`ConfigurationBuilder`](crate::configuration::ConfigurationBuilder) 的合并逻辑使用。
/// - 调用方需根据业务需求选择合适策略，确保覆盖顺序符合预期。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum ProfileLayering {
    BaseFirst,
    OverrideFirst,
}

/// Profile 的稳定标识符。
///
/// ### 设计目的（Why）
/// - 对齐 Kubernetes Namespace、AWS 环境标签等概念，为跨平台部署提供统一命名契约。
/// - 借助 `Cow<'static, str>` 适配常量与动态创建场景。
///
/// ### 契约说明（What）
/// - **前置条件**：`name` 需满足 UTF-8，推荐使用短横线或下划线作为分隔符。
/// - **后置条件**：实现 `Eq`、`Hash`，可安全作为映射键值。
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ProfileId(Cow<'static, str>);

impl ProfileId {
    /// 创建新的 Profile 标识。
    #[inline]
    pub fn new<N>(name: N) -> Self
    where
        N: Into<Cow<'static, str>>,
    {
        Self(name.into())
    }

    /// 返回内部字符串表示。
    #[inline]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// 描述一个可组合的配置档案。
///
/// ### 设计目的（Why）
/// - 将 Profile 的依赖、分层策略、说明文案集中管理，方便自动化工具查询。
/// - 对齐 Istio、Envoy 对 Profile/Context 的抽象，方便进行差异化部署。
///
/// ### 逻辑说明（How）
/// - `identifier`：Profile 的唯一标识。
/// - `extends`：按优先顺序列出所依赖的 Profile。
/// - `layering`：配置合并策略，默认 `BaseFirst`。
/// - `summary`：供文档与 CLI 展示的描述。
///
/// ### 契约定义（What）
/// - **前置条件**：`extends` 列表不应产生循环依赖；构造函数不会自动检测，需上层治理。
/// - **后置条件**：对象可直接传入 `ConfigurationBuilder` 作为构建输入。
///
/// ### 设计权衡（Trade-offs）
/// - 未引入图拓扑排序逻辑，保持契约层的轻量；实际加载时可在 builder 内部执行验证。
#[derive(Clone, Debug)]
pub struct ProfileDescriptor {
    pub identifier: ProfileId,
    pub extends: Vec<ProfileId>,
    pub layering: ProfileLayering,
    pub summary: Cow<'static, str>,
}

impl ProfileDescriptor {
    /// 构造 Profile 描述信息。
    pub fn new<S>(
        identifier: ProfileId,
        extends: Vec<ProfileId>,
        layering: ProfileLayering,
        summary: S,
    ) -> Self
    where
        S: Into<Cow<'static, str>>,
    {
        Self {
            identifier,
            extends,
            layering,
            summary: summary.into(),
        }
    }
}
