use crate::case::{TckCase, TckSuite};
use spark_core::contract::{DEFAULT_OBSERVABILITY_CONTRACT, ObservabilityContract};

const CASES: &[TckCase] = &[
    TckCase {
        name: "observability_default_contract_fields_are_stable",
        test: observability_default_contract_fields_are_stable,
    },
    TckCase {
        name: "observability_custom_contract_reflects_inputs",
        test: observability_custom_contract_reflects_inputs,
    },
];

const SUITE: TckSuite = TckSuite {
    name: "observability",
    cases: CASES,
};

/// 返回“可观测性契约”主题的测试套件。
///
/// # 教案式说明
/// - **意图 (Why)**：确保默认观测契约字段稳定，并验证自定义契约的零拷贝语义。
/// - **逻辑 (How)**：包含对默认常量与自定义构造的两个用例。
/// - **契约 (What)**：返回 `'static` 引用供宏调用。
pub const fn suite() -> &'static TckSuite {
    &SUITE
}

/// 验证默认契约暴露的指标、日志、追踪、审计字段集合保持稳定。
///
/// # 教案式说明
/// - **意图 (Why)**：防止在未同步文档的情况下更改关键字段，导致观测系统失配。
/// - **逻辑 (How)**：检查集合非空，验证首个元素仍为请求总量指标，同时确认队列深度指标依旧存在，末尾指标对应最新的管道变更计数。
/// - **契约 (What)**：若断言失败说明契约被破坏，需要同步更新文档及依赖。
fn observability_default_contract_fields_are_stable() {
    let contract = &DEFAULT_OBSERVABILITY_CONTRACT;
    assert!(!contract.metric_names().is_empty());
    assert_eq!(contract.metric_names()[0], "spark.request.total");
    assert!(
        contract
            .metric_names()
            .contains(&"spark.limits.queue.depth"),
        "默认契约应包含队列深度指标"
    );
    assert_eq!(
        contract.metric_names()[contract.metric_names().len() - 1],
        "spark.pipeline.mutation.total"
    );

    assert_eq!(contract.log_fields()[0], "request.id");
    assert_eq!(contract.trace_keys()[0], "traceparent");
    assert_eq!(contract.audit_schema()[0], "event_id");
}

/// 验证自定义契约在构造时不会复制输入切片。
///
/// # 教案式说明
/// - **意图 (Why)**：调用方希望复用静态切片，若内部复制会增加内存与维护成本。
/// - **逻辑 (How)**：构造契约后通过 `ptr::eq` 检查引用地址。
/// - **契约 (What)**：所有访问器返回的切片引用均与输入相同。
fn observability_custom_contract_reflects_inputs() {
    static METRICS: [&str; 1] = ["custom.metric"];
    static LOGS: [&str; 1] = ["custom.log"];
    static TRACES: [&str; 1] = ["custom.trace"];
    static AUDIT: [&str; 1] = ["custom.audit"];
    let contract = ObservabilityContract::new(&METRICS, &LOGS, &TRACES, &AUDIT);

    assert!(core::ptr::eq(contract.metric_names(), &METRICS));
    assert!(core::ptr::eq(contract.log_fields(), &LOGS));
    assert!(core::ptr::eq(contract.trace_keys(), &TRACES));
    assert!(core::ptr::eq(contract.audit_schema(), &AUDIT));
}
