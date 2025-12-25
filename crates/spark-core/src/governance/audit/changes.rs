use alloc::format;
use alloc::vec::Vec;

use crate::configuration::{ChangeSet, ConfigKey, ConfigKeyRepr, ConfigValue, ConfigValueRepr};

/// 审计事件中用于回放的差异集合。
///
/// ## 设计动机（Why）
/// - 复用配置模块已有的 `ChangeSet` 语义，同时将键和值转换为可序列化结构。
/// - 在保持最小字段的前提下，确保 CLI 工具能够完全复现非脱敏的样例数据。
#[derive(Clone, Debug, PartialEq)]
pub struct AuditChangeSet {
    pub created: Vec<AuditChangeEntry>,
    pub updated: Vec<AuditChangeEntry>,
    pub deleted: Vec<AuditDeletedEntry>,
}

impl AuditChangeSet {
    /// 根据核心配置模块的 [`ChangeSet`] 构建审计差异集合。
    ///
    /// ### 逻辑解析（How）
    /// - 对 `created` 与 `updated`，将键值对映射为 [`AuditChangeEntry`] 并深拷贝，以保证后续序列化不依赖生命周期。
    /// - 对 `deleted`，仅保留键信息，避免意外泄露历史值。
    pub fn from_change_set(changes: &ChangeSet) -> Self {
        let created = changes
            .created
            .iter()
            .map(|(key, value)| AuditChangeEntry {
                key: key.clone(),
                value: value.clone(),
            })
            .collect();
        let updated = changes
            .updated
            .iter()
            .map(|(key, value)| AuditChangeEntry {
                key: key.clone(),
                value: value.clone(),
            })
            .collect();
        let deleted = changes
            .deleted
            .iter()
            .map(|key| AuditDeletedEntry { key: key.clone() })
            .collect();
        Self {
            created,
            updated,
            deleted,
        }
    }

    /// ## 设计动机（Why）
    /// - 将差异集合转换为内部表示，从而隐藏 `serde` 依赖并统一序列化入口。
    ///
    /// ## 契约定义（What）
    /// - 入参：`&self`。
    /// - 返回：[`AuditChangeSetRepr`]，供序列化使用。
    ///
    /// ## 逻辑解析（How）
    /// - 对三类变更分别迭代调用条目级 `to_repr`，确保结构一致。
    ///
    /// ## 前置/后置条件（Contract）
    /// - 前置：调用方应确保 `created`、`updated`、`deleted` 的键互斥。
    /// - 后置：返回表示拥有独立所有权，可在序列化过程中长期存活。
    ///
    /// ## 设计考量（Trade-offs）
    /// - 进行深拷贝以换取生命周期简化；考虑到变更数量通常有限，该开销可接受。
    pub(crate) fn to_repr(&self) -> AuditChangeSetRepr {
        AuditChangeSetRepr {
            created: self.created.iter().map(AuditChangeEntry::to_repr).collect(),
            updated: self.updated.iter().map(AuditChangeEntry::to_repr).collect(),
            deleted: self
                .deleted
                .iter()
                .map(AuditDeletedEntry::to_repr)
                .collect(),
        }
    }

    /// ## 设计动机（Why）
    /// - 支持从中间表示恢复差异集合，保证回放流程的对等性。
    ///
    /// ## 契约定义（What）
    /// - 入参：`repr`。
    /// - 返回：`crate::Result<AuditChangeSet, String>`，失败时包含详细错误信息。
    ///
    /// ## 逻辑解析（How）
    /// - 对内部表示的向量执行 `into_iter` 并调用条目级 `from_repr` 还原。
    ///
    /// ## 前置/后置条件（Contract）
    /// - 前置：`repr` 已通过 `serde` 成功解析。
    /// - 后置：返回结构满足原始语义，可直接传递给回放与校验逻辑。
    ///
    /// ## 设计考量（Trade-offs）
    /// - 保持函数 `pub(crate)`，防止外部组件绕过类型抽象直接依赖内部表示。
    pub(crate) fn from_repr(repr: AuditChangeSetRepr) -> crate::Result<Self, String> {
        let created = repr
            .created
            .into_iter()
            .enumerate()
            .map(|(idx, entry)| {
                AuditChangeEntry::from_repr(entry).map_err(|err| {
                    format!("invalid created change entry at index {}: {}", idx, err)
                })
            })
            .collect::<crate::Result<Vec<_>, _>>()?;
        let updated = repr
            .updated
            .into_iter()
            .enumerate()
            .map(|(idx, entry)| {
                AuditChangeEntry::from_repr(entry).map_err(|err| {
                    format!("invalid updated change entry at index {}: {}", idx, err)
                })
            })
            .collect::<crate::Result<Vec<_>, _>>()?;
        let deleted = repr
            .deleted
            .into_iter()
            .enumerate()
            .map(|(idx, entry)| {
                AuditDeletedEntry::from_repr(entry).map_err(|err| {
                    format!("invalid deleted change entry at index {}: {}", idx, err)
                })
            })
            .collect::<crate::Result<Vec<_>, _>>()?;
        Ok(Self {
            created,
            updated,
            deleted,
        })
    }
}

impl serde::Serialize for AuditChangeSet {
    fn serialize<S>(&self, serializer: S) -> crate::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.to_repr().serialize(serializer)
    }
}

impl<'de> serde::Deserialize<'de> for AuditChangeSet {
    fn deserialize<D>(deserializer: D) -> crate::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let repr = AuditChangeSetRepr::deserialize(deserializer)?;
        Self::from_repr(repr).map_err(serde::de::Error::custom)
    }
}

/// 表示单个新增或更新的配置条目。
///
/// ## 设计动机（补充）
/// - 序列化时会调用 `ConfigKey::to_repr` 与 `ConfigValue::to_repr`，在不暴露 `serde` 依赖的情况下完成 JSON 编码。
/// - 反序列化阶段若检测到非法作用域或不合法的时间间隔，会转换为 `serde` 的 `custom` 错误，便于上层诊断输入问题。
#[derive(Clone, Debug, PartialEq)]
pub struct AuditChangeEntry {
    pub key: ConfigKey,
    pub value: ConfigValue,
}

impl serde::Serialize for AuditChangeEntry {
    fn serialize<S>(&self, serializer: S) -> crate::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.to_repr().serialize(serializer)
    }
}

impl<'de> serde::Deserialize<'de> for AuditChangeEntry {
    fn deserialize<D>(deserializer: D) -> crate::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let repr = AuditChangeEntryRepr::deserialize(deserializer)?;
        AuditChangeEntry::from_repr(repr).map_err(serde::de::Error::custom)
    }
}

impl AuditChangeEntry {
    /// ## 设计动机（Why）
    /// - 提供统一的内部表示转换入口，隐藏 `serde` 依赖的同时维持审计条目的可序列化能力。
    ///
    /// ## 契约定义（What）
    /// - 入参：`&self`。
    /// - 返回：[`AuditChangeEntryRepr`]，包含可序列化的键和值。
    ///
    /// ## 逻辑解析（How）
    /// - 调用配置模块提供的 `to_repr` 方法，生成拥有所有权的键和值表示。
    ///
    /// ## 前置/后置条件（Contract）
    /// - 前置：当前结构需已通过业务校验。
    /// - 后置：返回的内部表示不会影响原实例，可安全地交由 `serde` 序列化。
    ///
    /// ## 设计考量（Trade-offs）
    /// - 深拷贝不可避免，但换来类型隔离与生命周期简化，适用于审计变更规模有限的场景。
    pub(crate) fn to_repr(&self) -> AuditChangeEntryRepr {
        AuditChangeEntryRepr {
            key: self.key.to_repr(),
            value: self.value.to_repr(),
        }
    }

    /// ## 设计动机（Why）
    /// - 从内部表示恢复领域模型，支撑 CLI、回放器及服务端之间的数据互通。
    ///
    /// ## 契约定义（What）
    /// - 入参：[`AuditChangeEntryRepr`]，通常由 `serde` 反序列化获得。
    /// - 返回：成功时为 [`AuditChangeEntry`]，失败时返回描述性错误字符串。
    ///
    /// ## 逻辑解析（How）
    /// - 分别调用 `ConfigKey::from_repr` 与 `ConfigValue::from_repr`，并为错误附加上下文信息。
    ///
    /// ## 前置/后置条件（Contract）
    /// - 前置：输入在语法上合法，但仍可能违反业务约束。
    /// - 后置：成功则构造出完整条目；失败则不产生任何副作用。
    ///
    /// ## 设计考量（Trade-offs）
    /// - 选择 `String` 作为错误类型，方便直接映射到 `serde` 的 `custom` 错误提示。
    pub(crate) fn from_repr(repr: AuditChangeEntryRepr) -> crate::Result<Self, String> {
        let key = ConfigKey::from_repr(repr.key)
            .map_err(|err| format!("invalid config key in change entry: {}", err))?;
        let value = ConfigValue::from_repr(repr.value)
            .map_err(|err| format!("invalid config value in change entry: {}", err))?;
        Ok(Self { key, value })
    }
}

/// 表示单个删除的配置条目。
///
/// ## 设计提醒（Trade-offs）
/// - 与新增/更新逻辑一致，通过内部表示完成序列化；非法作用域同样会映射为反序列化错误。
#[derive(Clone, Debug, PartialEq)]
pub struct AuditDeletedEntry {
    pub key: ConfigKey,
}

impl serde::Serialize for AuditDeletedEntry {
    fn serialize<S>(&self, serializer: S) -> crate::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.to_repr().serialize(serializer)
    }
}

impl<'de> serde::Deserialize<'de> for AuditDeletedEntry {
    fn deserialize<D>(deserializer: D) -> crate::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let repr = AuditDeletedEntryRepr::deserialize(deserializer)?;
        AuditDeletedEntry::from_repr(repr).map_err(serde::de::Error::custom)
    }
}

impl AuditDeletedEntry {
    /// ## 设计动机（Why）
    /// - 与新增/更新条目保持一致的内部表示转换，统一序列化策略。
    ///
    /// ## 契约定义（What）
    /// - 入参：`&self`。
    /// - 返回：[`AuditDeletedEntryRepr`]。
    ///
    /// ## 逻辑解析（How）
    /// - 调用配置模块的 `to_repr`，生成拥有所有权的键信息。
    ///
    /// ## 前置/后置条件（Contract）
    /// - 前置：键已通过业务层校验。
    /// - 后置：返回结构不共享可变状态，可直接交由 `serde` 序列化。
    ///
    /// ## 设计考量（Trade-offs）
    /// - 通过深拷贝换取生命周期简单性；考虑到删除条目数量有限，此开销可控。
    pub(crate) fn to_repr(&self) -> AuditDeletedEntryRepr {
        AuditDeletedEntryRepr {
            key: self.key.to_repr(),
        }
    }

    /// ## 设计动机（Why）
    /// - 支持从序列化表示恢复删除条目，确保审计回放能完整还原差异。
    ///
    /// ## 契约定义（What）
    /// - 入参：[`AuditDeletedEntryRepr`]。
    /// - 返回：成功时为 [`AuditDeletedEntry`]，失败时为描述性错误字符串。
    ///
    /// ## 逻辑解析（How）
    /// - 调用 `ConfigKey::from_repr`，并在错误消息中附加上下文便于诊断非法作用域等问题。
    ///
    /// ## 前置/后置条件（Contract）
    /// - 前置：输入在语法上合法。
    /// - 后置：成功时即可执行删除逻辑；失败时不会影响系统状态。
    ///
    /// ## 设计考量（Trade-offs）
    /// - 返回 `String` 以便与 `serde` 的 `custom` 错误无缝衔接。
    pub(crate) fn from_repr(repr: AuditDeletedEntryRepr) -> crate::Result<Self, String> {
        let key = ConfigKey::from_repr(repr.key)
            .map_err(|err| format!("invalid config key in deleted entry: {}", err))?;
        Ok(Self { key })
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
pub(crate) struct AuditChangeEntryRepr {
    pub(crate) key: ConfigKeyRepr,
    pub(crate) value: ConfigValueRepr,
}

#[derive(serde::Serialize, serde::Deserialize)]
pub(crate) struct AuditDeletedEntryRepr {
    pub(crate) key: ConfigKeyRepr,
}

#[derive(serde::Serialize, serde::Deserialize)]
pub(crate) struct AuditChangeSetRepr {
    pub(crate) created: Vec<AuditChangeEntryRepr>,
    pub(crate) updated: Vec<AuditChangeEntryRepr>,
    pub(crate) deleted: Vec<AuditDeletedEntryRepr>,
}
