use alloc::borrow::Cow;
use alloc::sync::Arc;
use alloc::vec::Vec;

use super::actor::AuditActor;
use super::recorder::AuditRecorder;
use super::tsa::TsaEvidence;

/// 审计上下文：封装 Recorder 与实体/操作者静态信息。
///
/// ## 使用方式（How）
/// - 构建 [`AuditContext`] 后，通过 `ConfigurationBuilder::with_audit_pipeline` 注入。
/// - 框架在生成事件时会克隆上下文，并补全动态字段（例如实体 ID、哈希、变更集）。
#[derive(Clone)]
pub struct AuditPipeline {
    pub recorder: Arc<dyn AuditRecorder>,
    pub context: AuditContext,
}

/// 静态审计上下文信息。
///
/// ## 字段说明（What）
/// - `entity_kind`：审计实体类型，例 `configuration.profile`。
/// - `actor`：操作者元信息。
/// - `action`：默认动作描述，可在事件生成时直接使用。
/// - `tsa_evidence`：预先绑定的可信时间证据，可选。
/// - `entity_labels`：补充实体标签（租户、区域等），随事件透传。
#[derive(Clone, Debug)]
pub struct AuditContext {
    pub entity_kind: Cow<'static, str>,
    pub actor: AuditActor,
    pub action: Cow<'static, str>,
    pub tsa_evidence: Option<TsaEvidence>,
    pub entity_labels: Vec<(Cow<'static, str>, Cow<'static, str>)>,
}

impl AuditContext {
    /// 创建新的上下文。
    pub fn new(
        entity_kind: Cow<'static, str>,
        actor: AuditActor,
        action: Cow<'static, str>,
    ) -> Self {
        Self {
            entity_kind,
            actor,
            action,
            tsa_evidence: None,
            entity_labels: Vec::new(),
        }
    }

    /// 为事件附加 TSA 证据。
    pub fn with_tsa(mut self, evidence: TsaEvidence) -> Self {
        self.tsa_evidence = Some(evidence);
        self
    }

    /// 设置实体标签集合，便于在多租户或多区域场景中补充上下文。
    pub fn with_entity_labels(
        mut self,
        labels: Vec<(Cow<'static, str>, Cow<'static, str>)>,
    ) -> Self {
        self.entity_labels = labels;
        self
    }
}
