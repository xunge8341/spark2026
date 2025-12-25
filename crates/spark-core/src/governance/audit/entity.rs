use alloc::borrow::Cow;
use alloc::string::String;
use alloc::vec::Vec;

/// 描述审计实体的稳定标识。
///
/// ## 设计动机（Why）
/// - 与 Kubernetes Audit、AWS CloudTrail 等系统保持一致，使用 `kind + id` 组合定位资源。
/// - 允许在跨模块复用时补充额外维度（例如租户、区域），通过 `labels` 字段承载。
///
/// ## 契约说明（What）
/// - `kind`：实体类型，例如 `configuration.profile`、`router.route`。
/// - `id`：实体在其命名空间内的唯一标识。
/// - `labels`：用于补充上下文的键值对，调用方需避免写入敏感信息。
#[derive(Clone, Debug, PartialEq)]
pub struct AuditEntityRef {
    pub kind: Cow<'static, str>,
    pub id: String,
    pub labels: Vec<(Cow<'static, str>, Cow<'static, str>)>,
}

impl AuditEntityRef {
    /// ## 计动机（Why）
    /// - 通过内部表示将公共 API 与具体序列化框架解耦，避免对 `serde` 的硬依赖。
    /// - 便于未来根据不同媒介（JSON、MessagePack 等）灵活生成载荷。
    ///
    /// ## 契约定义（What）
    /// - 入参：`&self`，要求调用者已构造出合法的实体引用。
    /// - 返回：[`AuditEntityRefRepr`]，仅供当前模块序列化使用。
    ///
    /// ## 逻辑解析（How）
    /// - 对全部字段执行浅克隆，使内部表示拥有独立所有权，便于跨线程与生命周期传递。
    /// - `labels` 直接克隆向量，确保键值在序列化过程中保持稳定顺序。
    ///
    /// ## 前置/后置条件（Contract）
    /// - 前置：上层需保证字段内容满足业务命名规范。
    /// - 后置：返回结构不会修改原实例，可安全重复调用。
    ///
    /// ## 设计考量（Trade-offs）
    /// - 牺牲一次拷贝换取类型隔离；审计实体字段规模有限，该成本可忽略。
    pub(crate) fn to_repr(&self) -> AuditEntityRefRepr {
        AuditEntityRefRepr {
            kind: self.kind.clone(),
            id: self.id.clone(),
            labels: self.labels.clone(),
        }
    }

    /// ## 设计动机（Why）
    /// - 从序列化层的中间表示恢复领域模型，满足反序列化和回放需求。
    ///
    /// ## 契约定义（What）
    /// - 入参：`repr`，经 `serde` 语法校验后的内部表示。
    /// - 返回：[`AuditEntityRef`]，供业务层继续使用。
    ///
    /// ## 逻辑解析（How）
    /// - 逐字段移动内部表示的所有权，避免多余分配。
    ///
    /// ## 前置/后置条件（Contract）
    /// - 前置：尚未进行业务层的合法性校验，需要调用方在之后补充。
    /// - 后置：返回结构不会修改原实例，可安全重复调用。
    ///
    /// ## 设计考量（Trade-offs）
    /// - 保持 `pub(crate)` 可见性，避免外部绕过类型抽象直接依赖内部表示。
    pub(crate) fn from_repr(repr: AuditEntityRefRepr) -> Self {
        Self {
            kind: repr.kind,
            id: repr.id,
            labels: repr.labels,
        }
    }
}

impl serde::Serialize for AuditEntityRef {
    fn serialize<S>(&self, serializer: S) -> crate::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.to_repr().serialize(serializer)
    }
}

impl<'de> serde::Deserialize<'de> for AuditEntityRef {
    fn deserialize<D>(deserializer: D) -> crate::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let repr = AuditEntityRefRepr::deserialize(deserializer)?;
        Ok(Self::from_repr(repr))
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
pub(crate) struct AuditEntityRefRepr {
    pub(crate) kind: Cow<'static, str>,
    pub(crate) id: String,
    #[serde(default)]
    pub(crate) labels: Vec<(Cow<'static, str>, Cow<'static, str>)>,
}
