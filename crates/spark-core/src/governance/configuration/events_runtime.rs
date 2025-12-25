//! 配置事件运行期辅助模块：基于生成的事件描述符构建聚合结果与审计事件。
//!
//! # 教案式说明（Why）
//! - 控制面在聚合多节点漂移上报时需要统一的结构化输出与审计链路，本模块提供复用的聚合逻辑；
//! - 通过复用生成的事件描述符（SOT），避免手写常量造成字段或审计动作漂移。
//!
//! # 契约定义（What）
//! - 输入：业务侧提供的漂移节点报告列表及上下文（期望哈希、窗口、审计序列等）；
//! - 输出：`ConfigurationDriftDetected` 事件负载与对应的 [`AuditEventV1`]；
//! - 错误：当报告列表为空时返回 [`DriftAggregationError::EmptyReports`]，提示调用方补充数据。
//!
//! # 实现方式（How）
//! - 按节点差异键数量降序排序，确保严重节点优先展示；
//! - 将差异键集合排序去重，提供稳定 diff；
//! - 构造审计实体标签，补充事件代码、家族、严重性与哈希信息，便于外部索引。

use alloc::{
    borrow::Cow,
    format,
    string::{String, ToString},
    vec,
    vec::Vec,
};

use crate::audit::{AuditActor, AuditChangeSet, AuditEntityRef, AuditEventV1};
use crate::configuration::ChangeSet;

use super::events::{
    CONFIGURATION_DRIFT_DETECTED, ConfigurationDriftDetected, ConfigurationEventDescriptor,
    DriftNodeSnapshot,
};

/// 漂移节点上报数据。
///
/// # 教案式说明
/// - **Why**：承载单个节点的漂移信息，聚合器以此构造事件负载；
/// - **What**：包含节点标识、本地哈希与差异键集合；
/// - **How**：聚合前可按需填充差异键，模块会负责排序与去重。
#[derive(Clone, Debug, PartialEq)]
pub struct DriftNodeReport {
    pub node_id: String,
    pub observed_hash: String,
    pub difference_keys: Vec<String>,
}

/// 聚合上下文：描述一次漂移事件所需的公共元数据。
///
/// # 契约定义
/// - `profile_id`：目标配置 Profile；
/// - `expected_hash`/`majority_hash`：控制面期望与窗口内多数派哈希；
/// - `drift_window_ms`：聚合窗口毫秒数；
/// - `state_prev_hash`/`state_curr_hash`：审计链路的前后状态哈希；
/// - `actor`、`sequence`、`occurred_at`：审计事件基础信息；
/// - `change_set`：对应的配置变更集合，用于生成 `AuditChangeSet`。
#[derive(Clone, Debug)]
pub struct DriftAggregationContext<'a> {
    pub profile_id: &'a str,
    pub expected_hash: &'a str,
    pub majority_hash: &'a str,
    pub drift_window_ms: u64,
    pub state_prev_hash: &'a str,
    pub state_curr_hash: &'a str,
    pub actor: AuditActor,
    pub sequence: u64,
    pub occurred_at: u64,
    pub change_set: &'a ChangeSet,
}

/// 聚合产出：包含事件描述符、负载与审计事件。
#[derive(Clone, Debug, PartialEq)]
pub struct DriftAggregationOutcome {
    pub descriptor: &'static ConfigurationEventDescriptor,
    pub payload: ConfigurationDriftDetected,
    pub audit: AuditEventV1,
}

/// 漂移聚合错误枚举。
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DriftAggregationError {
    /// 无上报数据，无法生成事件。
    EmptyReports,
}

/// 聚合漂移上报，生成统一的事件与审计记录。
///
/// # 教案式说明（Why）
/// - 控制面需要在 1 个观测窗口内对多个节点上报进行归并，本函数提供标准化聚合，避免各调用者重复实现排序、去重与审计映射；
/// - 将事件契约（`ConfigurationDriftDetected`）与审计契约（[`AuditEventV1`]）同时产出，确保运行面与治理面能够复现相同的证据链。
///
/// # 契约定义（What）
/// - 参数 `reports`：差异节点的上报数组；每条记录需至少包含节点 ID、观察到的哈希与差异键集合。
/// - 参数 `context`：一次聚合操作的共享上下文，包含 Profile 标识、期望/多数哈希、窗口长度、审计序列号、状态哈希与变更集合。
/// - 前置条件：
///   - `reports` 至少包含 1 条数据，否则函数返回 [`DriftAggregationError::EmptyReports`]；
///   - `context.change_set` 必须来源于同一份配置差异快照，以便生成稳定的 [`AuditChangeSet`]。
/// - 后置条件：
///   - 返回的 [`ConfigurationDriftDetected`] 负载中的 `divergent_nodes` 已去重并按差异得分降序排列；
///   - 审计事件的实体标签补充了事件代码/家族/严重性与期望哈希，便于后续检索与追溯。
/// - 返回值：成功时给出 [`DriftAggregationOutcome`]，其中包含事件描述符、事件负载与审计事件；失败时返回 [`DriftAggregationError`]。
///
/// # 实现步骤（How）
/// 1. 对 `reports` 中的差异键集合排序并去重，计算差异得分；
/// 2. 按差异得分降序（并以 `node_id` 稳定排序）生成 [`DriftNodeSnapshot`] 列表；
/// 3. 组装 `ConfigurationDriftDetected` 负载并拷贝上下文元数据；
/// 4. 根据事件描述符构造 [`AuditEntityRef`] 与 [`AuditEventV1`]，附带输入的 [`ChangeSet`]；
/// 5. 返回封装好的 [`DriftAggregationOutcome`]，供控制面直接发送或记录。
pub fn aggregate_configuration_drift(
    reports: Vec<DriftNodeReport>,
    context: DriftAggregationContext<'_>,
) -> crate::Result<DriftAggregationOutcome, DriftAggregationError> {
    if reports.is_empty() {
        return Err(DriftAggregationError::EmptyReports);
    }

    let mut nodes: Vec<DriftNodeSnapshot> = reports
        .into_iter()
        .map(|mut report| {
            report.difference_keys.sort();
            report.difference_keys.dedup();
            let delta_score = report.difference_keys.len() as u64;
            DriftNodeSnapshot {
                node_id: report.node_id,
                observed_hash: report.observed_hash,
                difference_keys: report.difference_keys,
                delta_score,
            }
        })
        .collect();

    nodes.sort_by(|a, b| {
        b.delta_score
            .cmp(&a.delta_score)
            .then_with(|| a.node_id.cmp(&b.node_id))
    });

    let payload = ConfigurationDriftDetected {
        profile_id: context.profile_id.to_string(),
        expected_hash: context.expected_hash.to_string(),
        majority_hash: context.majority_hash.to_string(),
        drift_window_ms: context.drift_window_ms,
        observed_node_count: nodes.len() as u64,
        divergent_nodes: nodes,
    };

    let change_set = AuditChangeSet::from_change_set(context.change_set);
    let descriptor = &CONFIGURATION_DRIFT_DETECTED;
    let event_id = format!(
        "{}::{}::{}",
        descriptor.code, context.profile_id, context.sequence
    );
    let entity = AuditEntityRef {
        kind: Cow::Borrowed(descriptor.audit.entity_kind),
        id: context.profile_id.to_string(),
        labels: vec![
            (Cow::Borrowed("event.code"), Cow::Borrowed(descriptor.code)),
            (
                Cow::Borrowed("event.family"),
                Cow::Borrowed(descriptor.family),
            ),
            (
                Cow::Borrowed("event.severity"),
                Cow::Borrowed(descriptor.severity.as_str()),
            ),
            (
                Cow::Borrowed("configuration.expected_hash"),
                Cow::Owned(context.expected_hash.to_string()),
            ),
            (
                Cow::Borrowed("configuration.majority_hash"),
                Cow::Owned(context.majority_hash.to_string()),
            ),
        ],
    };

    let audit = AuditEventV1 {
        event_id,
        sequence: context.sequence,
        entity,
        action: Cow::Borrowed(descriptor.audit.action),
        state_prev_hash: context.state_prev_hash.to_string(),
        state_curr_hash: context.state_curr_hash.to_string(),
        actor: context.actor.clone(),
        occurred_at: context.occurred_at,
        tsa_evidence: None,
        changes: change_set,
    };

    Ok(DriftAggregationOutcome {
        descriptor,
        payload,
        audit,
    })
}
