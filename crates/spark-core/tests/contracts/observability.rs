use spark_core::contract::{ObservabilityContract, DEFAULT_OBSERVABILITY_CONTRACT};

/// 验证默认可观测性契约对外暴露的指标、日志、追踪、审计键集合。
///
/// # 测试目标（Why）
/// - 确保常量 `DEFAULT_OBSERVABILITY_CONTRACT` 的字段顺序与内容保持稳定，为跨团队协作提供固定命名；
/// - 提供回归保障，避免后续修改时意外删除关键指标。
///
/// # 测试步骤（How）
/// 1. 直接引用常量并读取四类字段；
/// 2. 使用样本索引（首、末元素）进行断言，验证集合内容未发生意外变动；
/// 3. 校验各集合非空，以满足契约要求。
///
/// # 输入/输出契约（What）
/// - **前置条件**：无外部输入；
/// - **后置条件**：指标、日志、追踪、审计集合均保持非空；
/// - **风险提示**：若断言失败，应审查是否更新了契约且需同步更新文档。
#[test]
fn observability_default_contract_fields_are_stable() {
    let contract = &DEFAULT_OBSERVABILITY_CONTRACT;
    assert!(
        !contract.metric_names().is_empty(),
        "默认契约的指标集合不应为空"
    );
    assert_eq!(contract.metric_names()[0], "spark.request.total");
    assert_eq!(
        contract.metric_names()[contract.metric_names().len() - 1],
        "spark.transport.bytes.outbound"
    );

    assert_eq!(contract.log_fields()[0], "request.id");
    assert_eq!(contract.trace_keys()[0], "traceparent");
    assert_eq!(contract.audit_schema()[0], "event_id");
}

/// 验证通过 `ObservabilityContract::new` 自定义契约时，各访问器可正确返回引用。
///
/// # 测试目标（Why）
/// - 确保调用方定制契约时可以直接传入静态切片，并在测试中读取到相同引用；
/// - 避免由于拷贝或重新排序导致的字段变化。
///
/// # 测试步骤（How）
/// 1. 创建自定义契约并传入各类字段；
/// 2. 逐一断言访问器返回的切片与输入引用相同。
///
/// # 输入/输出契约（What）
/// - **前置条件**：所有输入为 `'static` 切片；
/// - **后置条件**：访问器返回与输入相同的引用；
/// - **风险提示**：若实现发生拷贝，可能导致生命周期不一致或内存占用增加。
#[test]
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
