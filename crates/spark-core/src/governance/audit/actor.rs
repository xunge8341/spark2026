use alloc::borrow::Cow;

/// 描述触发审计事件的操作者。
///
/// ## 设计动机（Why）
/// - 审计链需要明确“谁做的变更”，以满足合规与追责要求。
/// - 结构化字段便于后续扩展（例如来源 IP、设备指纹）。
///
/// ## 契约说明（What）
/// - `id`：操作者稳定标识，通常为 IAM 用户或服务账号。
/// - `display_name`：面向展示的人类可读名称，可选。
/// - `tenant`：多租户系统中的租户标识，可选。
#[derive(Clone, Debug, PartialEq)]
pub struct AuditActor {
    pub id: Cow<'static, str>,
    pub display_name: Option<Cow<'static, str>>,
    pub tenant: Option<Cow<'static, str>>,
}

impl AuditActor {
    /// ## 设计动机（Why）
    /// - 与其他审计结构保持一致，通过内部表示隐藏对 `serde` 的直接依赖。
    /// - 支持未来在不破坏类型定义的情况下扩展额外字段。
    ///
    /// ## 契约定义（What）
    /// - 入参：`&self`。
    /// - 返回：[`AuditActorRepr`]，仅供序列化通道使用。
    ///
    /// ## 逻辑解析（How）
    /// - 克隆 `Cow` 字段，确保内部表示拥有独立生命周期。
    /// - 保持 `Option` 语义不变，避免出现空字符串歧义值。
    ///
    /// ## 前置/后置条件（Contract）
    /// - 前置：调用者需保证 `id` 为非空、稳定标识。
    /// - 后置：返回值与原结构字段一一对应，可安全传递给 `serde`。
    ///
    /// ## 设计考量（Trade-offs）
    /// - 额外拷贝换取接口稳定性；相比审计事件写入频率，该成本可接受。
    pub(crate) fn to_repr(&self) -> AuditActorRepr {
        AuditActorRepr {
            id: self.id.clone(),
            display_name: self.display_name.clone(),
            tenant: self.tenant.clone(),
        }
    }

    /// ## 设计动机（Why）
    /// - 从中间表示恢复操作者信息，支撑 CLI 与后端共享数据格式。
    ///
    /// ## 契约定义（What）
    /// - 入参：`repr`，内部序列化表示。
    /// - 返回：[`AuditActor`]。
    ///
    /// ## 逻辑解析（How）
    /// - 将内部表示的所有权直接转移至领域模型，避免重复分配。
    ///
    /// ## 前置/后置条件（Contract）
    /// - 前置：字段已通过语法校验，但业务校验仍由上层负责。
    /// - 后置：返回结构中的可选字段若缺省则保持 `None`。
    ///
    /// ## 设计考量（Trade-offs）
    /// - 函数仅在 crate 内可见，防止外部绕过模型接口。
    pub(crate) fn from_repr(repr: AuditActorRepr) -> Self {
        Self {
            id: repr.id,
            display_name: repr.display_name,
            tenant: repr.tenant,
        }
    }
}

impl serde::Serialize for AuditActor {
    fn serialize<S>(&self, serializer: S) -> crate::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.to_repr().serialize(serializer)
    }
}

impl<'de> serde::Deserialize<'de> for AuditActor {
    fn deserialize<D>(deserializer: D) -> crate::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let repr = AuditActorRepr::deserialize(deserializer)?;
        Ok(Self::from_repr(repr))
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
pub(crate) struct AuditActorRepr {
    pub(crate) id: Cow<'static, str>,
    #[serde(default)]
    pub(crate) display_name: Option<Cow<'static, str>>,
    #[serde(default)]
    pub(crate) tenant: Option<Cow<'static, str>>,
}
