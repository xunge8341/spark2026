use std::{
    collections::VecDeque,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll, RawWaker, RawWakerVTable, Waker},
    thread,
    time::Duration as StdDuration,
};

use spark_core::{
    configuration::{
        ChangeEvent, ChangeNotification, ConfigDelta, ConfigKey, ConfigMetadata, ConfigScope,
        ConfigValue, ConfigurationBuilder, ConfigurationError, ConfigurationLayer,
        ConfigurationSource, ConfigurationUpdate, ConfigurationUpdateKind, ProfileDescriptor,
        ProfileId, ProfileLayering, SourceMetadata,
    },
    future::Stream,
    limits::{LimitRuntimeConfig, LimitSettings, ResourceKind},
    observability::{
        AttributeSet, InstrumentDescriptor, MetricsProvider,
        metrics::contract::hot_reload as contract,
    },
    runtime::{HotReloadApplyTimer, HotReloadFence, TimeoutRuntimeConfig, TimeoutSettings},
    test_stubs::observability::{NoopCounter, NoopGauge, NoopHistogram},
};

/// 自定义测试数据源：同 `limits_timeout` 用例，但复用在一致性验证中。
struct TestSource {
    layers: Vec<ConfigurationLayer>,
    events: Arc<Mutex<VecDeque<ConfigDelta>>>,
}

impl TestSource {
    fn new(layers: Vec<ConfigurationLayer>) -> (Self, Arc<Mutex<VecDeque<ConfigDelta>>>) {
        let events = Arc::new(Mutex::new(VecDeque::new()));
        (
            Self {
                layers,
                events: Arc::clone(&events),
            },
            events,
        )
    }
}

impl ConfigurationSource for TestSource {
    type Stream<'a>
        = TestStream
    where
        Self: 'a;

    fn load(
        &self,
        _profile: &ProfileId,
    ) -> spark_core::Result<Vec<ConfigurationLayer>, ConfigurationError> {
        Ok(self.layers.clone())
    }

    fn watch<'a>(
        &'a self,
        _profile: &ProfileId,
    ) -> spark_core::Result<Self::Stream<'a>, ConfigurationError> {
        Ok(TestStream {
            events: Arc::clone(&self.events),
        })
    }
}

struct TestStream {
    events: Arc<Mutex<VecDeque<ConfigDelta>>>,
}

impl Stream for TestStream {
    type Item = ConfigDelta;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut guard = self.events.lock().unwrap();
        if let Some(delta) = guard.pop_front() {
            Poll::Ready(Some(delta))
        } else {
            Poll::Pending
        }
    }
}

#[derive(Clone, Debug)]
struct MetricRecord {
    name: String,
    value: f64,
    attributes: Vec<(String, String)>,
}

impl MetricRecord {
    fn new(name: &InstrumentDescriptor<'_>, value: f64, attributes: AttributeSet<'_>) -> Self {
        Self {
            name: name.name.to_string(),
            value,
            attributes: attributes
                .iter()
                .map(|entry| {
                    (
                        entry.key.clone().into_owned(),
                        format_metric_value(&entry.value),
                    )
                })
                .collect(),
        }
    }
}

#[derive(Default)]
struct TestMetricsProvider {
    gauges: Mutex<Vec<MetricRecord>>,
    histograms: Mutex<Vec<MetricRecord>>,
}

impl TestMetricsProvider {
    fn gauge_records(&self) -> Vec<MetricRecord> {
        self.gauges.lock().unwrap().clone()
    }

    fn histogram_records(&self) -> Vec<MetricRecord> {
        self.histograms.lock().unwrap().clone()
    }
}

impl MetricsProvider for TestMetricsProvider {
    fn counter(
        &self,
        _descriptor: &InstrumentDescriptor<'_>,
    ) -> Arc<dyn spark_core::observability::metrics::Counter> {
        Arc::new(NoopCounter)
    }

    fn gauge(
        &self,
        _descriptor: &InstrumentDescriptor<'_>,
    ) -> Arc<dyn spark_core::observability::metrics::Gauge> {
        Arc::new(NoopGauge)
    }

    fn histogram(
        &self,
        _descriptor: &InstrumentDescriptor<'_>,
    ) -> Arc<dyn spark_core::observability::metrics::Histogram> {
        Arc::new(NoopHistogram)
    }

    fn record_gauge_set(
        &self,
        descriptor: &InstrumentDescriptor<'_>,
        value: f64,
        attributes: AttributeSet<'_>,
    ) {
        let record = MetricRecord::new(descriptor, value, attributes);
        self.gauges.lock().unwrap().push(record);
    }

    fn record_histogram(
        &self,
        descriptor: &InstrumentDescriptor<'_>,
        value: f64,
        attributes: AttributeSet<'_>,
    ) {
        let record = MetricRecord::new(descriptor, value, attributes);
        self.histograms.lock().unwrap().push(record);
    }
}

fn format_metric_value(
    value: &spark_core::observability::attributes::MetricAttributeValue<'_>,
) -> String {
    use spark_core::observability::attributes::MetricAttributeValue as Value;
    match value {
        Value::Text(text) => text.to_string(),
        Value::Bool(flag) => flag.to_string(),
        Value::F64(v) => format!("{v}"),
        Value::I64(v) => v.to_string(),
        _ => String::from("_"),
    }
}

pub fn hot_reload_config_consistency_case() {
    let profile = ProfileDescriptor::new(
        ProfileId::new("hotreload"),
        Vec::new(),
        ProfileLayering::BaseFirst,
        "hot reload test",
    );

    let initial_layer = ConfigurationLayer {
        metadata: SourceMetadata::new("inline", 0, None),
        entries: vec![
            (
                limit_key(ResourceKind::Connections),
                ConfigValue::Integer(4_096, ConfigMetadata::default()),
            ),
            (
                request_timeout_key(),
                ConfigValue::Integer(5_000, ConfigMetadata::default()),
            ),
            (
                idle_timeout_key(),
                ConfigValue::Integer(60_000, ConfigMetadata::default()),
            ),
        ],
    };

    let mut builder = ConfigurationBuilder::new().with_profile(profile);
    let (source, queue) = TestSource::new(vec![initial_layer]);
    builder
        .register_source(Box::new(source))
        .expect("register source");

    let outcome = builder.build().expect("build configuration");
    let mut handle = outcome.handle;
    let mut watch = Box::pin(handle.watch().expect("watch stream"));
    let initial = outcome.initial;

    let metrics = Arc::new(TestMetricsProvider::default());
    let fence = HotReloadFence::new();

    let limits_metrics: Arc<dyn MetricsProvider> = metrics.clone();
    let timeouts_metrics: Arc<dyn MetricsProvider> = metrics.clone();

    let limits = Arc::new(LimitRuntimeConfig::with_observability(
        LimitSettings::from_configuration(&initial).expect("parse limits"),
        fence.clone(),
        limits_metrics,
        "limits",
    ));
    let timeouts = Arc::new(TimeoutRuntimeConfig::with_observability(
        TimeoutSettings::from_configuration(&initial).expect("parse timeouts"),
        fence.clone(),
        timeouts_metrics,
        "timeouts",
    ));

    let running = Arc::new(AtomicBool::new(true));
    let mut readers = Vec::new();
    for _ in 0..8 {
        let fence = fence.clone();
        let limits = Arc::clone(&limits);
        let timeouts = Arc::clone(&timeouts);
        let running = Arc::clone(&running);
        readers.push(thread::spawn(move || {
            let mut last_limit_epoch = 0;
            let mut last_timeout_epoch = 0;
            while running.load(Ordering::SeqCst) {
                let guard = fence.read();
                let limit_snapshot = limits.snapshot_with_fence(&guard);
                let timeout_snapshot = timeouts.snapshot_with_fence(&guard);
                drop(guard);

                let plan = limit_snapshot.plan(ResourceKind::Connections);
                let limit_value = plan.limit();
                let request_ms = timeout_snapshot.request_timeout().as_millis() as u64;
                let idle_ms = timeout_snapshot.idle_timeout().as_millis() as u64;
                assert!(matches!(
                    (limit_value, request_ms, idle_ms),
                    (4_096, 5_000, 60_000) | (2_048, 8_000, 60_000) | (2_048, 8_000, 30_000)
                ));

                let limit_epoch = limits.config_epoch();
                let timeout_epoch = timeouts.config_epoch();
                assert!(limit_epoch >= last_limit_epoch);
                assert!(timeout_epoch >= last_timeout_epoch);
                last_limit_epoch = limit_epoch;
                last_timeout_epoch = timeout_epoch;

                thread::yield_now();
            }
        }));
    }

    // 推送第一次变更：调高连接限额、延长请求超时。
    {
        let mut events = queue.lock().unwrap();
        events.push_back(ConfigDelta::Change(ChangeNotification::new(
            1,
            1_700_000_000,
            vec![
                ChangeEvent::Updated {
                    key: limit_key(ResourceKind::Connections),
                    value: ConfigValue::Integer(2_048, ConfigMetadata::default()),
                },
                ChangeEvent::Updated {
                    key: request_timeout_key(),
                    value: ConfigValue::Integer(8_000, ConfigMetadata::default()),
                },
            ],
        )));
    }

    let update = wait_for_update(watch.as_mut());
    apply_update_with_fence(&update, &fence, &limits, &timeouts);

    thread::sleep(StdDuration::from_millis(20));

    // 推送第二次变更：缩短空闲超时。
    {
        let mut events = queue.lock().unwrap();
        events.push_back(ConfigDelta::Change(ChangeNotification::new(
            2,
            1_700_000_500,
            vec![ChangeEvent::Updated {
                key: idle_timeout_key(),
                value: ConfigValue::Integer(30_000, ConfigMetadata::default()),
            }],
        )));
    }

    let update = wait_for_update(watch.as_mut());
    apply_update_with_fence(&update, &fence, &limits, &timeouts);

    thread::sleep(StdDuration::from_millis(20));

    running.store(false, Ordering::SeqCst);
    for reader in readers {
        reader.join().expect("join reader");
    }

    // 验证指标：两个组件均应在最终纪元上报 >= 2，且延迟直方图有样本。
    let gauges = metrics.gauge_records();
    let histograms = metrics.histogram_records();

    assert!(
        gauges
            .iter()
            .any(|record| record.name == contract::CONFIG_EPOCH.name
                && has_component(record, "limits")
                && record.value >= 2.0)
    );
    assert!(
        gauges
            .iter()
            .any(|record| record.name == contract::CONFIG_EPOCH.name
                && has_component(record, "timeouts")
                && record.value >= 2.0)
    );

    assert!(
        histograms
            .iter()
            .filter(|record| record.name == contract::APPLY_LATENCY_MS.name
                && has_component(record, "limits"))
            .count()
            >= 2
    );
    assert!(
        histograms
            .iter()
            .filter(|record| record.name == contract::APPLY_LATENCY_MS.name
                && has_component(record, "timeouts"))
            .count()
            >= 2
    );
    assert!(histograms.iter().all(|record| record.value >= 0.0));
}

fn has_component(record: &MetricRecord, target: &str) -> bool {
    record
        .attributes
        .iter()
        .any(|(key, value)| key == contract::ATTR_COMPONENT && value == target)
}

fn wait_for_update(
    mut watch: Pin<&mut spark_core::configuration::ConfigurationWatch<'_>>,
) -> ConfigurationUpdate {
    loop {
        match poll_stream(watch.as_mut()) {
            Poll::Ready(Some(Ok(update))) => return update,
            Poll::Ready(Some(Err(err))) => panic!("configuration stream error: {err}"),
            Poll::Ready(None) => panic!("configuration stream ended unexpectedly"),
            Poll::Pending => thread::yield_now(),
        }
    }
}

fn apply_update_with_fence(
    update: &ConfigurationUpdate,
    fence: &HotReloadFence,
    limits: &LimitRuntimeConfig,
    timeouts: &TimeoutRuntimeConfig,
) {
    match &update.kind {
        ConfigurationUpdateKind::Incremental { .. } | ConfigurationUpdateKind::Refresh => {
            let guard = fence.write();
            limits
                .update_from_configuration_with_fence(
                    &guard,
                    &update.snapshot,
                    HotReloadApplyTimer::start(),
                )
                .expect("apply limit config");
            timeouts
                .update_from_configuration_with_fence(
                    &guard,
                    &update.snapshot,
                    HotReloadApplyTimer::start(),
                )
                .expect("apply timeout config");
        }
    }
}

fn poll_stream<S: Stream>(mut stream: Pin<&mut S>) -> Poll<Option<S::Item>> {
    let waker = noop_waker();
    let mut cx = Context::from_waker(&waker);
    stream.as_mut().poll_next(&mut cx)
}

fn noop_waker() -> Waker {
    unsafe fn clone(_: *const ()) -> RawWaker {
        RawWaker::new(std::ptr::null(), &VTABLE)
    }
    unsafe fn wake(_: *const ()) {}
    unsafe fn wake_by_ref(_: *const ()) {}
    unsafe fn drop(_: *const ()) {}

    static VTABLE: RawWakerVTable = RawWakerVTable::new(clone, wake, wake_by_ref, drop);
    unsafe { Waker::from_raw(RawWaker::new(std::ptr::null(), &VTABLE)) }
}

fn limit_key(resource: ResourceKind) -> ConfigKey {
    ConfigKey::new(
        "runtime",
        format!("limits.{}.limit", resource.as_str()),
        ConfigScope::Runtime,
        "runtime limit",
    )
}

fn request_timeout_key() -> ConfigKey {
    ConfigKey::new(
        "runtime",
        "timeouts.request_ms",
        ConfigScope::Runtime,
        "request timeout in milliseconds",
    )
}

fn idle_timeout_key() -> ConfigKey {
    ConfigKey::new(
        "runtime",
        "timeouts.idle_ms",
        ConfigScope::Runtime,
        "idle timeout in milliseconds",
    )
}
