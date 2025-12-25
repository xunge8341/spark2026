use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use spark_core::{
    contract::CallContext,
    observability::{
        AttributeSet, Counter, Gauge, Histogram, InstrumentDescriptor, LogRecord, LogSeverity,
        Logger, MetricAttributeValue, MetricsProvider, OpsEventBus,
        metrics::contract::pipeline as pipeline_contract,
    },
    pipeline::{
        Channel, Pipeline,
        controller::{Handler, HotSwapPipeline, PipelineHandleId, handler_from_inbound},
        handler::InboundHandler,
    },
    runtime::{AsyncRuntime, CoreServices},
    test_stubs::observability::{NoopCounter, NoopGauge, NoopHistogram, StaticObservabilityFacade},
};

use super::hot_swap::{NoopBufferPool, NoopOpsBus, NoopRuntime, TestChannel};

/// 验证 Pipeline 变更会同步记录纪元 Gauge、变更计数器与结构化日志。
///
/// # 教案式说明
/// - **意图（Why）**：确保 HotSwapPipeline 在执行 add/remove/replace 时，
///   能对外暴露 `spark.pipeline.epoch` 与 `spark.pipeline.mutation.total`，并输出统一的 INFO 日志，
///   以支撑 SRE 在告警与回退流程中进行比对。
/// - **逻辑（How）**：通过自定义 `RecordingMetrics` 与 `RecordingLogger` 捕获调用次数、标签和值，
///   依次触发新增、替换、移除操作，并断言指标与日志是否匹配预期顺序与内容。
/// - **契约（What）**：测试仅关注观测信号，不涉及 Handler 执行逻辑；当未来增加新的变更类型时，
///   需要扩展断言集合保持覆盖。
#[test]
fn pipeline_mutations_emit_epoch_metrics_and_logs() {
    let runtime = Arc::new(NoopRuntime::new());
    let metrics = Arc::new(RecordingMetrics::default());
    let logger = Arc::new(RecordingLogger::default());
    let ops_bus = Arc::new(NoopOpsBus::default());

    let services = CoreServices::with_observability_facade(
        runtime as Arc<dyn AsyncRuntime>,
        Arc::new(NoopBufferPool),
        StaticObservabilityFacade::new(
            logger.clone() as Arc<dyn Logger>,
            metrics.clone() as Arc<dyn MetricsProvider>,
            ops_bus as Arc<dyn OpsEventBus>,
            Arc::new(Vec::new()),
        ),
    );

    let call_context = CallContext::builder().build();
    let channel = Arc::new(TestChannel::new("epoch-metrics-channel"));
    let controller =
        HotSwapPipeline::new(channel.clone() as Arc<dyn Channel>, services, call_context);
    channel.bind_controller(controller.clone() as Arc<dyn Pipeline<HandleId = PipelineHandleId>>);

    controller.register_inbound_handler("alpha", Box::new(NopInbound));
    let alpha_handle = controller
        .registry()
        .snapshot()
        .into_iter()
        .find(|entry| entry.label() == "alpha")
        .expect("alpha handler should be registered")
        .handle_id();

    let beta_handler: Arc<dyn Handler> = handler_from_inbound(Arc::new(NopInbound));
    let beta_handle = controller.add_handler_after(alpha_handle, "beta", beta_handler);

    let replacement: Arc<dyn Handler> = handler_from_inbound(Arc::new(NopInbound));
    assert!(
        controller.replace_handler(beta_handle, replacement),
        "replace should succeed"
    );

    assert!(
        controller.remove_handler(beta_handle),
        "remove should succeed"
    );

    let gauge_events = metrics.take_gauge_events();
    let counter_events = metrics.take_counter_events();
    let log_entries = logger.take_entries();

    let expected_epochs = [1.0, 2.0, 3.0, 4.0];
    assert_eq!(
        gauge_events.len(),
        expected_epochs.len(),
        "每次提交应产生一次 Gauge 观测",
    );
    for (event, expected_epoch) in gauge_events.iter().zip(expected_epochs) {
        assert_eq!(
            event.descriptor.as_str(),
            pipeline_contract::EPOCH.name,
            "Gauge 描述符应为 pipeline epoch",
        );
        assert!(
            (event.value - expected_epoch).abs() < f64::EPSILON,
            "Gauge 记录的纪元值必须按顺序递增"
        );
        assert_eq!(
            event.attributes.get(pipeline_contract::ATTR_CONTROLLER),
            Some(&"hot_swap".to_string()),
            "Gauge 标签应包含控制器名称",
        );
        assert_eq!(
            event.attributes.get(pipeline_contract::ATTR_PIPELINE_ID),
            Some(&"epoch-metrics-channel".to_string()),
            "Gauge 标签应包含 Pipeline 标识",
        );
    }

    let expected_ops = [
        pipeline_contract::OP_ADD,
        pipeline_contract::OP_ADD,
        pipeline_contract::OP_REPLACE,
        pipeline_contract::OP_REMOVE,
    ];
    assert_eq!(
        counter_events.len(),
        expected_epochs.len(),
        "每次变更应累计一次计数器",
    );
    for (event, expected_op, expected_epoch) in counter_events
        .iter()
        .zip(expected_ops)
        .zip(expected_epochs)
        .map(|((event, op), epoch)| (event, op, epoch))
    {
        assert_eq!(
            event.descriptor.as_str(),
            pipeline_contract::MUTATION_TOTAL.name,
            "Counter 描述符应为 pipeline mutation",
        );
        assert_eq!(event.value, 1, "每次变更都应累计 1 次",);
        assert_eq!(
            event.attributes.get(pipeline_contract::ATTR_MUTATION_OP),
            Some(&expected_op.to_string()),
            "Counter 应按操作类型打标签",
        );
        assert_eq!(
            event.attributes.get(pipeline_contract::ATTR_CONTROLLER),
            Some(&"hot_swap".to_string()),
            "Counter 应携带控制器标签",
        );
        assert_eq!(
            event.attributes.get(pipeline_contract::ATTR_PIPELINE_ID),
            Some(&"epoch-metrics-channel".to_string()),
            "Counter 应携带 Pipeline 标识",
        );
        assert_eq!(
            event.attributes.get(pipeline_contract::ATTR_EPOCH),
            Some(&expected_epoch.to_string()),
            "Counter 标签应记录最新纪元值",
        );
    }

    assert_eq!(
        log_entries.len(),
        expected_epochs.len(),
        "每次变更都应生成一次日志",
    );
    for (entry, expected_op, expected_epoch) in log_entries
        .iter()
        .zip(expected_ops)
        .zip(expected_epochs)
        .map(|((entry, op), epoch)| (entry, op, epoch))
    {
        assert_eq!(entry.severity, LogSeverity::Info, "日志级别应为 INFO",);
        assert_eq!(
            entry.message,
            format!("pipeline.mutation applied epoch={expected_epoch}"),
            "日志消息应包含最新纪元",
        );
        assert_eq!(
            entry.attributes.get(pipeline_contract::ATTR_MUTATION_OP),
            Some(&expected_op.to_string()),
            "日志字段需标明操作类型",
        );
        assert_eq!(
            entry.attributes.get(pipeline_contract::ATTR_CONTROLLER),
            Some(&"hot_swap".to_string()),
            "日志字段应包含控制器名称",
        );
        assert_eq!(
            entry.attributes.get(pipeline_contract::ATTR_PIPELINE_ID),
            Some(&"epoch-metrics-channel".to_string()),
            "日志字段应包含 Pipeline 标识",
        );
        assert_eq!(
            entry.attributes.get(pipeline_contract::ATTR_EPOCH),
            Some(&expected_epoch.to_string()),
            "日志字段需记录最新纪元值",
        );
    }
}

#[derive(Default)]
struct RecordingMetrics {
    gauge_events: Mutex<Vec<GaugeEvent>>,
    counter_events: Mutex<Vec<CounterEvent>>,
}

impl RecordingMetrics {
    fn take_gauge_events(&self) -> Vec<GaugeEvent> {
        self.gauge_events.lock().expect("gauge events").clone()
    }

    fn take_counter_events(&self) -> Vec<CounterEvent> {
        self.counter_events.lock().expect("counter events").clone()
    }
}

impl MetricsProvider for RecordingMetrics {
    fn counter(&self, _descriptor: &InstrumentDescriptor<'_>) -> Arc<dyn Counter> {
        Arc::new(NoopCounter)
    }

    fn gauge(&self, _descriptor: &InstrumentDescriptor<'_>) -> Arc<dyn Gauge> {
        Arc::new(NoopGauge)
    }

    fn histogram(&self, _descriptor: &InstrumentDescriptor<'_>) -> Arc<dyn Histogram> {
        Arc::new(NoopHistogram)
    }

    fn record_counter_add(
        &self,
        descriptor: &InstrumentDescriptor<'_>,
        value: u64,
        attributes: AttributeSet<'_>,
    ) {
        let mut event = CounterEvent::new(descriptor.name.to_string(), value);
        event.extend(attributes);
        self.counter_events
            .lock()
            .expect("counter events push")
            .push(event);
    }

    fn record_gauge_set(
        &self,
        descriptor: &InstrumentDescriptor<'_>,
        value: f64,
        attributes: AttributeSet<'_>,
    ) {
        let mut event = GaugeEvent::new(descriptor.name.to_string(), value);
        event.extend(attributes);
        self.gauge_events
            .lock()
            .expect("gauge events push")
            .push(event);
    }
}

#[derive(Default)]
struct RecordingLogger {
    entries: Mutex<Vec<LogEntry>>,
}

impl RecordingLogger {
    fn take_entries(&self) -> Vec<LogEntry> {
        self.entries.lock().expect("log entries").clone()
    }
}

impl Logger for RecordingLogger {
    fn log(&self, record: &LogRecord<'_>) {
        let mut entry = LogEntry::new(record.severity, record.message.as_ref());
        entry.extend(record.attributes);
        self.entries.lock().expect("log entries push").push(entry);
    }
}

#[derive(Clone, Debug)]
struct GaugeEvent {
    descriptor: String,
    value: f64,
    attributes: HashMap<String, String>,
}

impl GaugeEvent {
    fn new(descriptor: String, value: f64) -> Self {
        Self {
            descriptor,
            value,
            attributes: HashMap::new(),
        }
    }

    fn extend(&mut self, attributes: AttributeSet<'_>) {
        merge_attributes(&mut self.attributes, attributes);
    }
}

#[derive(Clone, Debug)]
struct CounterEvent {
    descriptor: String,
    value: u64,
    attributes: HashMap<String, String>,
}

impl CounterEvent {
    fn new(descriptor: String, value: u64) -> Self {
        Self {
            descriptor,
            value,
            attributes: HashMap::new(),
        }
    }

    fn extend(&mut self, attributes: AttributeSet<'_>) {
        merge_attributes(&mut self.attributes, attributes);
    }
}

#[derive(Clone, Debug)]
struct LogEntry {
    severity: LogSeverity,
    message: String,
    attributes: HashMap<String, String>,
}

impl LogEntry {
    fn new(severity: LogSeverity, message: &str) -> Self {
        Self {
            severity,
            message: message.to_string(),
            attributes: HashMap::new(),
        }
    }

    fn extend(&mut self, attributes: AttributeSet<'_>) {
        merge_attributes(&mut self.attributes, attributes);
    }
}

fn merge_attributes(target: &mut HashMap<String, String>, attributes: AttributeSet<'_>) {
    for attribute in attributes {
        target.insert(
            attribute.key.to_string(),
            metric_value_to_string(&attribute.value),
        );
    }
}

fn metric_value_to_string(value: &MetricAttributeValue<'_>) -> String {
    match value {
        MetricAttributeValue::Text(text) => text.to_string(),
        MetricAttributeValue::Bool(flag) => flag.to_string(),
        MetricAttributeValue::F64(v) => v.to_string(),
        MetricAttributeValue::I64(v) => v.to_string(),
        _ => "_unsupported_".to_string(),
    }
}

#[derive(Default, Clone)]
struct NopInbound;

impl InboundHandler for NopInbound {
    fn on_channel_active(&self, _ctx: &dyn spark_core::pipeline::Context) {}

    fn on_read(
        &self,
        _ctx: &dyn spark_core::pipeline::Context,
        _msg: spark_core::buffer::PipelineMessage,
    ) {
    }

    fn on_read_complete(&self, _ctx: &dyn spark_core::pipeline::Context) {}

    fn on_writability_changed(&self, _ctx: &dyn spark_core::pipeline::Context, _is_writable: bool) {
    }

    fn on_user_event(
        &self,
        _ctx: &dyn spark_core::pipeline::Context,
        _event: spark_core::observability::CoreUserEvent,
    ) {
    }

    fn on_exception_caught(
        &self,
        _ctx: &dyn spark_core::pipeline::Context,
        _error: spark_core::error::CoreError,
    ) {
    }

    fn on_channel_inactive(&self, _ctx: &dyn spark_core::pipeline::Context) {}
}
