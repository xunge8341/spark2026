use std::borrow::Cow;

use spark_core::audit::AuditChangeSet;
use spark_core::configuration::{
    aggregate_configuration_drift, ChangeSet, ConfigKey, ConfigMetadata, ConfigScope, ConfigValue,
    DriftAggregationContext, DriftAggregationError, DriftNodeReport, CONFIGURATION_DRIFT_DETECTED,
};

/// 教案级集成测试：验证 10 节点漂移聚合可生成正确事件与审计记录。
#[test]
fn drift_aggregation_produces_event_and_audit() {
    let reports = sample_reports();
    let change_set = sample_change_set();
    let expected_changes = AuditChangeSet::from_change_set(&change_set);

    let context = DriftAggregationContext {
        profile_id: "profile.alpha",
        expected_hash: "hash.expected",
        majority_hash: "hash.majority",
        drift_window_ms: 5_000,
        state_prev_hash: "prev.001",
        state_curr_hash: "curr.002",
        actor: spark_core::AuditActor {
            id: Cow::Borrowed("control-plane"),
            display_name: Some(Cow::Borrowed("control-plane")),
            tenant: None,
        },
        sequence: 42,
        occurred_at: 1_720_000_000_000,
        change_set: &change_set,
    };

    let outcome = aggregate_configuration_drift(reports, context).expect("aggregate drift");

    assert_eq!(outcome.descriptor.code, CONFIGURATION_DRIFT_DETECTED.code);
    assert_eq!(outcome.payload.profile_id, "profile.alpha");
    assert_eq!(outcome.payload.expected_hash, "hash.expected");
    assert_eq!(outcome.payload.majority_hash, "hash.majority");
    assert_eq!(outcome.payload.observed_node_count, 10);
    assert_eq!(outcome.payload.divergent_nodes.len(), 10);

    // 节点按差异键数量降序排列，首个节点应拥有最多差异键。
    let first = &outcome.payload.divergent_nodes[0];
    assert_eq!(first.node_id, "node-09");
    assert_eq!(first.delta_score, first.difference_keys.len() as u64);
    assert!(first.delta_score > outcome.payload.divergent_nodes[1].delta_score);

    // 审计事件应携带匹配的动作、实体与变更集合。
    assert_eq!(outcome.audit.action, Cow::Borrowed(CONFIGURATION_DRIFT_DETECTED.audit.action));
    assert_eq!(outcome.audit.entity.kind, CONFIGURATION_DRIFT_DETECTED.audit.entity_kind);
    assert_eq!(outcome.audit.entity.id, "profile.alpha");
    assert_eq!(outcome.audit.changes, expected_changes);
    assert_eq!(outcome.audit.state_prev_hash, "prev.001");
    assert_eq!(outcome.audit.state_curr_hash, "curr.002");
    assert!(outcome
        .audit
        .entity
        .labels
        .iter()
        .any(|(k, v)| k == "event.family" && v == CONFIGURATION_DRIFT_DETECTED.family));
    assert!(outcome
        .audit
        .entity
        .labels
        .iter()
        .any(|(k, v)| k == "configuration.expected_hash" && v == "hash.expected"));
    assert!(outcome
        .audit
        .event_id
        .starts_with(CONFIGURATION_DRIFT_DETECTED.code));
}

/// 空报告输入应返回错误，避免生成不完整事件。
#[test]
fn drift_aggregation_rejects_empty_reports() {
    let context = DriftAggregationContext {
        profile_id: "profile.alpha",
        expected_hash: "hash.expected",
        majority_hash: "hash.majority",
        drift_window_ms: 5_000,
        state_prev_hash: "prev",
        state_curr_hash: "curr",
        actor: spark_core::AuditActor {
            id: Cow::Borrowed("control-plane"),
            display_name: None,
            tenant: None,
        },
        sequence: 1,
        occurred_at: 0,
        change_set: &sample_change_set(),
    };

    let err = aggregate_configuration_drift(Vec::new(), context).unwrap_err();
    assert_eq!(err, DriftAggregationError::EmptyReports);
}

fn sample_reports() -> Vec<DriftNodeReport> {
    let mut reports = Vec::new();
    for idx in 0..10 {
        let mut keys = vec![format!("key.{}", idx)];
        if idx % 3 == 0 {
            keys.push("feature.toggle".to_string());
        }
        if idx % 5 == 0 {
            keys.push("runtime.patch".to_string());
        }
        // 提升最后一个节点的差异数量，验证排序逻辑。
        if idx == 9 {
            keys.push("routing.weight".to_string());
            keys.push("security.mtls".to_string());
        }
        reports.push(DriftNodeReport {
            node_id: format!("node-{:02}", idx),
            observed_hash: format!("hash.node.{}", idx),
            difference_keys: keys,
        });
    }
    reports
}

fn sample_change_set() -> ChangeSet {
    ChangeSet {
        created: Vec::new(),
        updated: vec![config_entry("runtime", "throttle.rate", "200")],
        deleted: vec![ConfigKey::new(
            "routing",
            "legacy.endpoint",
            ConfigScope::Cluster,
            "retired endpoint",
        )],
    }
}

fn config_entry(domain: &str, name: &str, value: &str) -> (ConfigKey, ConfigValue) {
    let key = ConfigKey::new(domain, name, ConfigScope::Cluster, "sample entry");
    let value = ConfigValue::Text(Cow::Owned(value.to_string()), ConfigMetadata::default());
    (key, value)
}
