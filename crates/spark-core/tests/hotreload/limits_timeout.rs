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
        ConfigurationSource, ConfigurationUpdateKind, ProfileDescriptor, ProfileId,
        ProfileLayering, SourceMetadata,
    },
    future::Stream,
    limits::{LimitRuntimeConfig, LimitSettings, ResourceKind},
    runtime::{TimeoutRuntimeConfig, TimeoutSettings},
};

/// 自定义测试数据源：提供固定初始 Layer，并通过内部队列驱动增量事件。
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

pub fn hot_reload_limits_and_timeouts_is_atomic_case() {
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
    let initial = outcome.initial;

    let limits = Arc::new(LimitRuntimeConfig::new(
        LimitSettings::from_configuration(&initial).expect("parse limits"),
    ));
    let timeouts = Arc::new(TimeoutRuntimeConfig::new(
        TimeoutSettings::from_configuration(&initial).expect("parse timeouts"),
    ));

    let mut watch = Box::pin(handle.watch().expect("watch stream"));

    let running = Arc::new(AtomicBool::new(true));
    let limit_reader = {
        let running = Arc::clone(&running);
        let limits = Arc::clone(&limits);
        thread::spawn(move || {
            while running.load(Ordering::SeqCst) {
                let snapshot = limits.snapshot();
                assert!(snapshot.plan(ResourceKind::Connections).limit() > 0);
                thread::yield_now();
            }
        })
    };
    let timeout_reader = {
        let running = Arc::clone(&running);
        let timeouts = Arc::clone(&timeouts);
        thread::spawn(move || {
            while running.load(Ordering::SeqCst) {
                let snapshot = timeouts.snapshot();
                assert!(snapshot.request_timeout() > StdDuration::from_millis(0));
                thread::yield_now();
            }
        })
    };

    let mut limit_epoch = limits.config_epoch();
    let mut timeout_epoch = timeouts.config_epoch();

    // 推送第一次增量变更：调高连接限额、延长请求超时。
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
    apply_update(&update, &limits, &timeouts);
    assert!(limits.config_epoch() > limit_epoch);
    assert!(timeouts.config_epoch() > timeout_epoch);
    limit_epoch = limits.config_epoch();
    timeout_epoch = timeouts.config_epoch();

    // 第二次变更：缩短空闲超时，验证纪元单调递增。
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
    apply_update(&update, &limits, &timeouts);
    assert!(limits.config_epoch() > limit_epoch);
    assert!(timeouts.config_epoch() > timeout_epoch);

    let final_limits = limits.snapshot();
    assert_eq!(final_limits.plan(ResourceKind::Connections).limit(), 2_048);

    let final_timeouts = timeouts.snapshot();
    assert_eq!(
        final_timeouts.request_timeout(),
        StdDuration::from_millis(8_000)
    );
    assert_eq!(
        final_timeouts.idle_timeout(),
        StdDuration::from_millis(30_000)
    );

    running.store(false, Ordering::SeqCst);
    limit_reader.join().expect("limit reader join");
    timeout_reader.join().expect("timeout reader join");
}

fn wait_for_update(
    mut watch: Pin<&mut spark_core::configuration::ConfigurationWatch<'_>>,
) -> spark_core::configuration::ConfigurationUpdate {
    loop {
        match poll_stream(watch.as_mut()) {
            Poll::Ready(Some(Ok(update))) => return update,
            Poll::Ready(Some(Err(err))) => panic!("configuration stream error: {err}"),
            Poll::Ready(None) => panic!("configuration stream ended unexpectedly"),
            Poll::Pending => thread::yield_now(),
        }
    }
}

fn apply_update(
    update: &spark_core::configuration::ConfigurationUpdate,
    limits: &LimitRuntimeConfig,
    timeouts: &TimeoutRuntimeConfig,
) {
    match update.kind {
        ConfigurationUpdateKind::Incremental { .. } | ConfigurationUpdateKind::Refresh => {
            limits
                .update_from_configuration(&update.snapshot)
                .expect("apply limit config");
            timeouts
                .update_from_configuration(&update.snapshot)
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
