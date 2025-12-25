use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Duration;

use spark_core::types::{Budget, BudgetDecision, BudgetKind};
use spark_core::observability::{
    keys::metrics::service as service_keys, AttributeSet, Counter, Gauge, Histogram, KeyValue,
    MetricAttributeValue, MetricsProvider,
};
use spark_core::test_stubs::observability::{NoopCounter, NoopGauge, NoopHistogram};
use spark_core::service::metrics::ServiceMetricsHook;
use spark_core::status::{ReadyState, RetryAdvice, RetryRhythm, SubscriptionBudget};

use super::support::DeterministicClock;

/// 场景 A：连续三次收到 `RetryAfter(200ms)`，应累计等待 600ms 且指标完整记录。
///
/// # 测试目标（Why）
/// - 保障 `RetryRhythm` 在连续软退避场景下按累计窗口推进，避免提前重试；
/// - 验证 `ServiceMetricsHook::record_ready_state` 在多次 RetryAfter 过程中正确记录
///   `spark.request.ready_state`、`retry_after_total` 与等待时间直方图。
///
/// # 测试步骤（How）
/// 1. 初始化确定性时钟与指标记录器；
/// 2. 三次调用 `RetryRhythm::observe` 并通过 `DeterministicClock` 按建议时间推进；
/// 3. 每次调用后触发 `record_ready_state`，累计计数与延迟；
/// 4. 最终断言累计等待 600ms，且指标中 RetryAfter 相关计数与直方图均为三条。
///
/// # 输入/输出契约（What）
/// - **前置条件**：基线标签固定（service/route/operation/protocol）。
/// - **后置条件**：
///   - `RetryRhythm::accumulated_wait` 为 600ms；
///   - `retry_after_total` = 3；
///   - `retry_after_delay_ms` 直方图包含三条 200ms 样本；
///   - `spark.request.ready_state{ready.state="retry_after"}` = 3。
#[test]
fn retry_after_sequence_accumulates_delay_and_metrics() {
    let base_attributes = build_base_attributes();
    let metrics = RecordingMetrics::default();
    let hook = ServiceMetricsHook::new(&metrics);
    let mut tracker = RetryRhythm::new();
    let mut clock = DeterministicClock::new();
    let advice = RetryAdvice::after(Duration::from_millis(200));

    for _ in 0..3 {
        let now = clock.now();
        let next_allowed = tracker.observe(now, &advice);
        assert_eq!(
            tracker.remaining_delay(now).as_millis(),
            200,
            "每次 RetryAfter 都应要求等待 200ms"
        );

        hook.record_ready_state(base_attributes.as_slice(), &ReadyState::RetryAfter(advice.clone()));

        assert!(
            tracker.next_ready_at().is_some(),
            "节律追踪器必须持有下一次可重试时间"
        );
        assert_eq!(
            tracker.next_ready_at().unwrap(),
            next_allowed,
            "observe 返回值应与内部时间点保持一致"
        );

        clock.advance(Duration::from_millis(200));
        assert!(
            tracker.is_ready(clock.now()),
            "等待建议时长后应允许下一次重试"
        );
    }

    assert_eq!(
        tracker.accumulated_wait(),
        Duration::from_millis(600),
        "三次 RetryAfter 应累计等待 600ms"
    );
    assert!(
        tracker.is_ready(clock.now()),
        "循环结束时应满足重试条件"
    );

    assert_eq!(
        metrics.counter_sum_by_attr(
            spark_core::observability::metrics::contract::service::REQUEST_READY_STATE.name,
            spark_core::observability::metrics::contract::service::ATTR_READY_STATE,
            spark_core::observability::metrics::contract::service::READY_STATE_RETRY_AFTER,
        ),
        3,
        "ready_state 指标应统计到 3 次 RetryAfter"
    );
    assert_eq!(
        metrics.counter_sum_by_attr(
            spark_core::observability::metrics::contract::service::RETRY_AFTER_TOTAL.name,
            spark_core::observability::metrics::contract::service::ATTR_READY_STATE,
            spark_core::observability::metrics::contract::service::READY_STATE_RETRY_AFTER,
        ),
        3,
        "retry_after_total 计数器应累计 3 次"
    );

    let samples = metrics
        .all_histogram_samples(
            spark_core::observability::metrics::contract::service::RETRY_AFTER_DELAY_MS.name,
        );
    assert_eq!(samples.len(), 3, "直方图应包含三条样本");
    for sample in samples {
        assert!(
            (sample - 200.0).abs() < f64::EPSILON,
            "直方图样本应记录 200ms 的等待时长"
        );
    }
}

/// 场景 B：RetryAfter 与 BudgetExhausted 交替时，预算不应因 RetryAfter 被额外扣减。
///
/// # 测试目标（Why）
/// - 保证 `RetryRhythm` 在记录 RetryAfter 节律时不会修改 `Budget` 剩余额度；
/// - 验证 `record_ready_state` 仅在真正 `BudgetExhausted` 时累计预算指标，避免 RetryAfter
///   被误记入预算耗尽统计。
///
/// # 测试步骤（How）
/// 1. 创建容量为 2 的预算并立即消耗完，使其处于耗尽状态；
/// 2. 构造 RetryAfter 与 BudgetExhausted 交替的状态序列；
/// 3. 对 RetryAfter 分支调用 `RetryRhythm::observe` 并确认预算剩余不变；
/// 4. 对 BudgetExhausted 分支调用 `Budget::try_consume`，确认持续返回耗尽；
/// 5. 最后检查指标：RetryAfter 与 BudgetExhausted 的计数分别为 2，直方图记录两条等待样本。
///
/// # 输入/输出契约（What）
/// - **前置条件**：预算上限为 2，初始全部消耗；
/// - **后置条件**：
///   - 预算剩余始终为 0；
///   - `ready.state=retry_after` 与 `ready.state=budget_exhausted` 各计 2 次；
///   - `retry_after_total` = 2，直方图样本为 120ms 与 80ms。
#[test]
fn retry_after_alternating_with_budget_exhausted_preserves_budget() {
    let base_attributes = build_base_attributes();
    let metrics = RecordingMetrics::default();
    let hook = ServiceMetricsHook::new(&metrics);
    let mut tracker = RetryRhythm::new();
    let mut clock = DeterministicClock::new();

    let budget = Budget::new(BudgetKind::Flow, 2);
    let grant = budget.try_consume(2);
    assert!(matches!(grant, BudgetDecision::Granted { .. }));
    assert_eq!(budget.remaining(), 0, "预算初始应被耗尽");

    let sequence = [
        ReadyState::RetryAfter(RetryAdvice::after(Duration::from_millis(120))),
        ReadyState::BudgetExhausted(SubscriptionBudget { limit: 2, remaining: 0 }),
        ReadyState::RetryAfter(RetryAdvice::after(Duration::from_millis(80))),
        ReadyState::BudgetExhausted(SubscriptionBudget { limit: 2, remaining: 0 }),
    ];

    for state in &sequence {
        match state {
            ReadyState::RetryAfter(advice) => {
                let before = budget.remaining();
                tracker.observe(clock.now(), advice);
                assert_eq!(
                    before,
                    budget.remaining(),
                    "RetryAfter 仅提供退避建议，不应修改预算"
                );
                clock.advance(advice.wait);
            }
            ReadyState::BudgetExhausted(_) => {
                let decision = budget.try_consume(1);
                assert!(
                    matches!(decision, BudgetDecision::Exhausted { .. }),
                    "预算耗尽分支应持续返回 Exhausted"
                );
            }
            ReadyState::Busy(_) | ReadyState::Ready => unreachable!(),
        }

        hook.record_ready_state(base_attributes.as_slice(), state);
    }

    assert_eq!(budget.remaining(), 0, "循环结束后预算仍应为 0");
    assert_eq!(
        tracker.accumulated_wait(),
        Duration::from_millis(200),
        "两次 RetryAfter 累计等待应为 120ms + 80ms"
    );

    let service_contract = spark_core::observability::metrics::contract::service;

    assert_eq!(
        metrics.counter_sum_by_attr(
            service_contract::REQUEST_READY_STATE.name,
            service_contract::ATTR_READY_STATE,
            service_contract::READY_STATE_BUDGET_EXHAUSTED,
        ),
        2,
        "预算耗尽分支应记录两次"
    );
    assert_eq!(
        metrics.counter_sum_by_attr(
            service_contract::REQUEST_READY_STATE.name,
            service_contract::ATTR_READY_STATE,
            service_contract::READY_STATE_RETRY_AFTER,
        ),
        2,
        "RetryAfter 分支也应记录两次"
    );
    assert_eq!(
        metrics.counter_sum_by_attr(
            service_contract::RETRY_AFTER_TOTAL.name,
            service_contract::ATTR_READY_STATE,
            service_contract::READY_STATE_RETRY_AFTER,
        ),
        2,
        "RetryAfter 计数器应统计两次"
    );

    let histogram_samples =
        metrics.all_histogram_samples(service_contract::RETRY_AFTER_DELAY_MS.name);
    assert_eq!(
        histogram_samples,
        vec![120.0, 80.0],
        "直方图应按顺序记录 120ms 与 80ms"
    );
}

fn build_base_attributes() -> Vec<KeyValue<'static>> {
    vec![
        KeyValue::new(service_keys::ATTR_SERVICE_NAME, "retry-contract"),
        KeyValue::new(service_keys::ATTR_ROUTE_ID, "catalog"),
        KeyValue::new(service_keys::ATTR_OPERATION, "poll_ready"),
        KeyValue::new(service_keys::ATTR_PROTOCOL, "grpc"),
    ]
}

#[derive(Default)]
struct RecordingMetrics {
    counters: Mutex<HashMap<String, Vec<RecordedCounter>>>,
    histograms: Mutex<HashMap<String, Vec<RecordedHistogram>>>,
}

#[derive(Clone)]
struct RecordedCounter {
    value: u64,
    attributes: Vec<(String, String)>,
}

#[derive(Clone)]
struct RecordedHistogram {
    value: f64,
    attributes: Vec<(String, String)>,
}

impl RecordingMetrics {
    fn counter_sum_by_attr(&self, name: &str, attr_key: &str, attr_value: &str) -> u64 {
        self.counters
            .lock()
            .unwrap()
            .get(name)
            .map(|entries| {
                entries
                    .iter()
                    .filter(|entry| {
                        entry
                            .attributes
                            .iter()
                            .any(|(key, value)| key == attr_key && value == attr_value)
                    })
                    .map(|entry| entry.value)
                    .sum()
            })
            .unwrap_or(0)
    }

    fn all_histogram_samples(&self, name: &str) -> Vec<f64> {
        self.histograms
            .lock()
            .unwrap()
            .get(name)
            .map(|entries| entries.iter().map(|entry| entry.value).collect())
            .unwrap_or_default()
    }
}

impl MetricsProvider for RecordingMetrics {
    fn counter(&self, _descriptor: &spark_core::observability::InstrumentDescriptor<'_>) -> std::sync::Arc<dyn Counter> {
        std::sync::Arc::new(NoopCounter)
    }

    fn gauge(&self, _descriptor: &spark_core::observability::InstrumentDescriptor<'_>) -> std::sync::Arc<dyn Gauge> {
        std::sync::Arc::new(NoopGauge)
    }

    fn histogram(
        &self,
        _descriptor: &spark_core::observability::InstrumentDescriptor<'_>,
    ) -> std::sync::Arc<dyn Histogram> {
        std::sync::Arc::new(NoopHistogram)
    }

    fn record_counter_add(
        &self,
        descriptor: &spark_core::observability::InstrumentDescriptor<'_>,
        value: u64,
        attributes: AttributeSet<'_>,
    ) {
        let mut map = self.counters.lock().unwrap();
        let entry = map.entry(descriptor.name.to_string()).or_default();
        entry.push(RecordedCounter {
            value,
            attributes: snapshot_attributes(attributes),
        });
    }

    fn record_histogram(
        &self,
        descriptor: &spark_core::observability::InstrumentDescriptor<'_>,
        value: f64,
        attributes: AttributeSet<'_>,
    ) {
        let mut map = self.histograms.lock().unwrap();
        let entry = map.entry(descriptor.name.to_string()).or_default();
        entry.push(RecordedHistogram {
            value,
            attributes: snapshot_attributes(attributes),
        });
    }
}

fn snapshot_attributes(attributes: AttributeSet<'_>) -> Vec<(String, String)> {
    attributes
        .iter()
        .map(|kv| {
            let key = kv.key.to_string();
            let value = match &kv.value {
                MetricAttributeValue::Text(text) => text.to_string(),
                MetricAttributeValue::Bool(flag) => flag.to_string(),
                MetricAttributeValue::F64(v) => format!("{:.3}", v),
                MetricAttributeValue::I64(v) => v.to_string(),
            };
            (key, value)
        })
        .collect()
}

