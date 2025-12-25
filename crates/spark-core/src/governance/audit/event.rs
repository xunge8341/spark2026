use alloc::borrow::Cow;
use alloc::format;
use alloc::string::String;

use super::actor::{AuditActor, AuditActorRepr};
use super::changes::{AuditChangeSet, AuditChangeSetRepr};
use super::entity::{AuditEntityRef, AuditEntityRefRepr};
use super::tsa::{TsaEvidence, TsaEvidenceRepr};

/// 审计事件 v1.0 的标准载荷。
///
/// ## 设计动机（Why）
/// - 将事件最小字段固化，保障跨模块、跨语言的兼容性。
/// - 哈希链语义 (`state_prev_hash` -> `state_curr_hash`) 让事件缺失可以被快速检测。
///
/// ## 契约说明（What）
/// - `event_id`：唯一事件标识，调用方应保证全局唯一性。
/// - `sequence`：事件发生顺序，可与数据源自增序列对齐。
/// - `entity`：发生变更的对象引用。
/// - `action`：动作名称（如 `apply_changeset`、`rollback`）。
/// - `state_prev_hash` / `state_curr_hash`：前/后状态的 SHA-256 十六进制摘要。
/// - `actor`：操作者信息。
/// - `occurred_at`：事件发生时间（Unix 毫秒）。
/// - `tsa_evidence`：可选的可信时间锚点。
/// - `changes`：脱敏后的配置变更集合，可用于回放。
#[derive(Clone, Debug, PartialEq)]
pub struct AuditEventV1 {
    pub event_id: String,
    pub sequence: u64,
    pub entity: AuditEntityRef,
    pub action: Cow<'static, str>,
    pub state_prev_hash: String,
    pub state_curr_hash: String,
    pub actor: AuditActor,
    pub occurred_at: u64,
    pub tsa_evidence: Option<TsaEvidence>,
    pub changes: AuditChangeSet,
}

impl AuditEventV1 {
    /// ## 设计动机（Why）
    /// - 通过内部表示统一控制序列化行为，确保公共 API 不暴露 `serde` 类型。
    /// - 为未来多格式输出（JSON、YAML、二进制协议）预留扩展空间。
    ///
    /// ## 契约定义（What）
    /// - 入参：`&self`。
    /// - 返回：[`AuditEventV1Repr`]，供序列化栈使用。
    ///
    /// ## 逻辑解析（How）
    /// - 逐字段克隆标量值与 `Cow` 字符串，调用子结构的 `to_repr` 获取嵌套表示。
    /// - `tsa_evidence` 通过 `Option::map` 转换，保持可选语义。
    ///
    /// ## 前置/后置条件（Contract）
    /// - 前置：事件已经过业务层校验（哈希、序列、权限等）。
    /// - 后置：返回表示拥有完整所有权，可安全传递到任意序列化上下文。
    ///
    /// ## 设计考量（Trade-offs）
    /// - 复制字段带来的额外成本换来 API 稳定性与后续扩展能力。
    pub(crate) fn to_repr(&self) -> AuditEventV1Repr {
        AuditEventV1Repr {
            event_id: self.event_id.clone(),
            sequence: self.sequence,
            entity: self.entity.to_repr(),
            action: self.action.clone(),
            state_prev_hash: self.state_prev_hash.clone(),
            state_curr_hash: self.state_curr_hash.clone(),
            actor: self.actor.to_repr(),
            occurred_at: self.occurred_at,
            tsa_evidence: self.tsa_evidence.as_ref().map(TsaEvidence::to_repr),
            changes: self.changes.to_repr(),
        }
    }

    /// ## 设计动机（Why）
    /// - 集中管理反序列化逻辑，便于未来 Schema 升级时处理兼容策略。
    ///
    /// ## 契约定义（What）
    /// - 入参：`repr`，内部序列化表示。
    /// - 返回：`crate::Result<AuditEventV1, String>`，失败时携带可读错误信息。
    ///
    /// ## 逻辑解析（How）
    /// - 逐字段调用子结构的 `from_repr`，恢复原始领域模型。
    /// - 对 `Option` 字段进行 `map` 转换，保留缺省语义。
    ///
    /// ## 前置/后置条件（Contract）
    /// - 前置：`repr` 已通过 `serde` 的结构校验。
    /// - 后置：返回实例与原输入语义等价，可直接用于回放或校验。
    ///
    /// ## 设计考量（Trade-offs）
    /// - 通过集中转换点可以在未来 Schema 演进时加入默认值或迁移逻辑，降低破坏性改动。
    pub(crate) fn from_repr(repr: AuditEventV1Repr) -> crate::Result<Self, String> {
        let changes = AuditChangeSet::from_repr(repr.changes)
            .map_err(|err| format!("invalid change set: {}", err))?;
        Ok(Self {
            event_id: repr.event_id,
            sequence: repr.sequence,
            entity: AuditEntityRef::from_repr(repr.entity),
            action: repr.action,
            state_prev_hash: repr.state_prev_hash,
            state_curr_hash: repr.state_curr_hash,
            actor: AuditActor::from_repr(repr.actor),
            occurred_at: repr.occurred_at,
            tsa_evidence: repr.tsa_evidence.map(TsaEvidence::from_repr),
            changes,
        })
    }
}

impl serde::Serialize for AuditEventV1 {
    fn serialize<S>(&self, serializer: S) -> crate::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.to_repr().serialize(serializer)
    }
}

impl<'de> serde::Deserialize<'de> for AuditEventV1 {
    fn deserialize<D>(deserializer: D) -> crate::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let repr = AuditEventV1Repr::deserialize(deserializer)?;
        Self::from_repr(repr).map_err(serde::de::Error::custom)
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
pub(crate) struct AuditEventV1Repr {
    pub(crate) event_id: String,
    pub(crate) sequence: u64,
    pub(crate) entity: AuditEntityRefRepr,
    pub(crate) action: Cow<'static, str>,
    pub(crate) state_prev_hash: String,
    pub(crate) state_curr_hash: String,
    pub(crate) actor: AuditActorRepr,
    pub(crate) occurred_at: u64,
    #[serde(default)]
    pub(crate) tsa_evidence: Option<TsaEvidenceRepr>,
    pub(crate) changes: AuditChangeSetRepr,
}
