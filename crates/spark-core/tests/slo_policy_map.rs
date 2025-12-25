#![cfg(feature = "std_json")]

use spark_core::configuration::{ConfigMetadata, ConfigValue};
use spark_core::runtime::{
    SloPolicyAction, SloPolicyDirective, SloPolicyManager, SloPolicyReloadReport,
};
use spark_core::types::{BudgetKind, BudgetSnapshot};
use std::path::PathBuf;

fn hot_meta() -> ConfigMetadata {
    ConfigMetadata {
        hot_reloadable: true,
        ..ConfigMetadata::default()
    }
}

fn text(value: &str) -> ConfigValue {
    ConfigValue::Text(value.to_string().into(), hot_meta())
}

fn integer(value: i64) -> ConfigValue {
    ConfigValue::Integer(value, hot_meta())
}

fn duration_ms(ms: u64) -> ConfigValue {
    ConfigValue::Duration(core::time::Duration::from_millis(ms), hot_meta())
}

fn duration_secs(secs: u64) -> ConfigValue {
    ConfigValue::Duration(core::time::Duration::from_secs(secs), hot_meta())
}

fn list(values: Vec<ConfigValue>) -> ConfigValue {
    ConfigValue::List(values, hot_meta())
}

fn dict(entries: Vec<(std::borrow::Cow<'static, str>, ConfigValue)>) -> ConfigValue {
    ConfigValue::Dictionary(entries, hot_meta())
}

fn key(name: &str, value: ConfigValue) -> (std::borrow::Cow<'static, str>, ConfigValue) {
    (std::borrow::Cow::Owned(name.to_string()), value)
}

fn baseline_policy() -> ConfigValue {
    dict(vec![key(
        "rules",
        list(vec![rate_limit_rule(), circuit_break_rule(), retry_rule()]),
    )])
}

fn updated_policy() -> ConfigValue {
    dict(vec![key(
        "rules",
        list(vec![rate_limit_rule(), retry_rule()]),
    )])
}

fn rate_limit_rule() -> ConfigValue {
    dict(vec![
        key("id", text("flow.rate-limit")),
        key("budget", text("flow")),
        key("summary", text("流量预警限流")),
        key(
            "trigger",
            dict(vec![
                key("activate_below_percent", integer(40)),
                key("deactivate_above_percent", integer(60)),
            ]),
        ),
        key(
            "action",
            dict(vec![
                key("kind", text("rate_limit")),
                key("limit_percent", integer(60)),
            ]),
        ),
    ])
}

fn circuit_break_rule() -> ConfigValue {
    dict(vec![
        key("id", text("flow.circuit-breaker")),
        key("budget", text("flow")),
        key("summary", text("流量熔断窗口")),
        key(
            "trigger",
            dict(vec![
                key("activate_below_percent", integer(20)),
                key("deactivate_above_percent", integer(40)),
            ]),
        ),
        key(
            "action",
            dict(vec![
                key("kind", text("circuit_break")),
                key("open_for", duration_secs(30)),
            ]),
        ),
    ])
}

fn retry_rule() -> ConfigValue {
    dict(vec![
        key("id", text("decode.retry")),
        key("budget", text("decode")),
        key("summary", text("解码预算重试")),
        key(
            "trigger",
            dict(vec![
                key("activate_below_percent", integer(50)),
                key("deactivate_above_percent", integer(70)),
            ]),
        ),
        key(
            "action",
            dict(vec![
                key("kind", text("retry")),
                key("max_attempts", integer(2)),
                key("backoff", duration_ms(200)),
            ]),
        ),
    ])
}

fn directives_to_json(directives: &[SloPolicyDirective]) -> serde_json::Value {
    serde_json::Value::Array(
        directives
            .iter()
            .map(|directive| {
                serde_json::json!({
                    "rule_id": directive.rule_id().as_ref(),
                    "engaged": directive.is_engaged(),
                    "action": action_to_json(directive.action()),
                    "summary": directive.summary().map(|s| s.as_ref()),
                })
            })
            .collect(),
    )
}

fn action_to_json(action: &SloPolicyAction) -> serde_json::Value {
    match action {
        SloPolicyAction::RateLimit { limit_percent } => {
            serde_json::json!({"kind": "rate_limit", "limit_percent": limit_percent })
        }
        SloPolicyAction::Degrade { feature } => {
            serde_json::json!({"kind": "degrade", "feature": feature.as_ref() })
        }
        SloPolicyAction::CircuitBreak { open_for } => serde_json::json!({
            "kind": "circuit_break",
            "open_for_ms": open_for.as_millis(),
        }),
        SloPolicyAction::Retry {
            max_attempts,
            backoff,
        } => serde_json::json!({
            "kind": "retry",
            "max_attempts": max_attempts,
            "backoff_ms": backoff.as_millis(),
        }),
    }
}

fn report_to_json(report: &SloPolicyReloadReport) -> serde_json::Value {
    let mut added: Vec<String> = report
        .added
        .iter()
        .map(|v| v.as_ref().to_string())
        .collect();
    let mut removed: Vec<String> = report
        .removed
        .iter()
        .map(|v| v.as_ref().to_string())
        .collect();
    let mut retained: Vec<String> = report
        .retained
        .iter()
        .map(|v| v.as_ref().to_string())
        .collect();
    added.sort();
    removed.sort();
    retained.sort();
    serde_json::json!({
        "added": added,
        "removed": removed,
        "retained": retained,
    })
}

#[test]
fn slo_policy_mapping_baseline() {
    let manager = SloPolicyManager::new();
    let initial_policy = baseline_policy();
    let load_report = manager
        .apply_config(&initial_policy)
        .expect("initial policy should load");

    let flow_30 = manager.evaluate_snapshot(&BudgetSnapshot::new(BudgetKind::Flow, 30, 100));
    let flow_15 = manager.evaluate_snapshot(&BudgetSnapshot::new(BudgetKind::Flow, 15, 100));
    let flow_80 = manager.evaluate_snapshot(&BudgetSnapshot::new(BudgetKind::Flow, 80, 100));
    let decode_40 = manager.evaluate_snapshot(&BudgetSnapshot::new(BudgetKind::Decode, 40, 100));
    let decode_90 = manager.evaluate_snapshot(&BudgetSnapshot::new(BudgetKind::Decode, 90, 100));

    let reload_policy = updated_policy();
    let reload_report = manager
        .apply_config(&reload_policy)
        .expect("updated policy should load");
    let post_reload = manager.evaluate_snapshot(&BudgetSnapshot::new(BudgetKind::Flow, 10, 100));

    let snapshot = serde_json::json!({
        "load": report_to_json(&load_report),
        "activation": {
            "flow_30": directives_to_json(&flow_30),
            "flow_15": directives_to_json(&flow_15),
            "flow_80": directives_to_json(&flow_80),
            "decode_40": directives_to_json(&decode_40),
            "decode_90": directives_to_json(&decode_90),
        },
        "reload": report_to_json(&reload_report),
        "post_reload_flow_10": directives_to_json(&post_reload),
    });

    let actual = serde_json::to_string_pretty(&snapshot).expect("json encoding");

    let mut baseline_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    // 教案提示：目录迁移至 `crates/` 之后需回退两级才能抵达仓库根。`pop()` 的使用避免了
    // 字符串拼接错误，也能在后续目录调整时及时暴露 panic。
    baseline_path.pop(); // 移除 `spark-core`
    baseline_path.pop(); // 移除 `crates`
    baseline_path.push("snapshots");
    baseline_path.push("slo-policy-baseline.json");
    let expected = std::fs::read_to_string(&baseline_path)
        .unwrap_or_else(|err| panic!("failed to read baseline {:?}: {}", baseline_path, err));

    assert_eq!(actual, expected);
}
