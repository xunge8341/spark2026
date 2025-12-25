use core::time::Duration;

use crate::observability::{
    AttributeSet, InstrumentDescriptor, MetricsProvider, OwnedAttributeSet,
    metrics::contract::service as contract,
};
use crate::status::{BusyReason, ReadyState, RetryAdvice};

/// 描述请求载荷方向。
///
/// # 设计动机（Why）
/// - 将请求/响应字节打点统一抽象为两个枚举值，避免在调用方手写字符串引起拼写错误；
/// - 结合 [`ServiceMetricsHook::record_payload_bytes`]，在 Service 层即可区分入站与出站数据量。
///
/// # 契约说明（What）
/// - `Inbound`：代表从客户端进入本实例的载荷；
/// - `Outbound`：代表由本实例发送给客户端的载荷；
/// - **前置条件**：调用方需确保传入的字节数为实际业务有效载荷（不含 TLS 头等传输开销）。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PayloadDirection {
    Inbound,
    Outbound,
}

impl PayloadDirection {
    /// 将方向映射到指标描述符，供内部复用。
    #[inline]
    fn descriptor(&self) -> &'static InstrumentDescriptor<'static> {
        match self {
            PayloadDirection::Inbound => &contract::BYTES_INBOUND,
            PayloadDirection::Outbound => &contract::BYTES_OUTBOUND,
        }
    }
}

/// 请求完成状态。
///
/// # 设计动机（Why）
/// - 指标契约要求以稳定标签 `outcome` 区分成功与失败；
/// - 在代码中使用枚举可避免直接拼接字符串，便于审计与补全。
///
/// # 契约说明（What）
/// - `Success`：业务处理完整结束且未返回错误；
/// - `Error`：业务处理返回错误或框架层检测到失败；
/// - **前置条件**：调用方应根据自身业务语义正确选择分支，例如 gRPC Status != OK 时视为失败；
/// - **后置条件**：调用方需保证在构建指标标签集合时同步写入 `contract::ATTR_OUTCOME` 的取值。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ServiceOutcome {
    Success,
    Error,
}

impl ServiceOutcome {
    /// outcome 标签中“成功”对应的枚举值。
    pub const SUCCESS_LABEL: &'static str = contract::OUTCOME_SUCCESS;
    /// outcome 标签中“失败”对应的枚举值。
    pub const ERROR_LABEL: &'static str = contract::OUTCOME_ERROR;

    /// 将枚举值转换为契约规定的字符串标签。
    #[inline]
    pub fn as_label(self) -> &'static str {
        match self {
            ServiceOutcome::Success => Self::SUCCESS_LABEL,
            ServiceOutcome::Error => Self::ERROR_LABEL,
        }
    }
}

/// Service 指标挂钩的轻量包装。
///
/// # 设计动机（Why）
/// - 封装常用的打点模式（开始、结束、载荷大小），帮助调用方在不持有 `Arc` 的情况下直接调用 `MetricsProvider`；
/// - 在框架内部提供统一的收敛点，便于未来扩展采样或批量刷写策略而不改动业务代码。
///
/// # 契约说明（What）
/// - 构造时仅接受对 [`MetricsProvider`] 的借用引用，确保挂钩对象本身零开销；
/// - 所有方法均假设调用方遵循 `on_call_start` → 业务逻辑 → `on_call_finish` 的顺序；
/// - **前置条件**：属性集合需使用 `contract::ATTR_*` 常量控制基数；
/// - **后置条件**：每次调用后，底层指标至少被记录一次（若后端实现为降级 Noop，则视为接受）。
pub struct ServiceMetricsHook<'a> {
    provider: &'a dyn MetricsProvider,
}

impl<'a> ServiceMetricsHook<'a> {
    /// 构造 Service 指标挂钩。
    ///
    /// # 参数说明
    /// - `provider`：可观测性层注入的指标提供者，通常来自 `PipelineContext::metrics()`。
    ///
    /// # 前置条件
    /// - `provider` 必须在整个调用生命周期内保持有效；若为延迟初始化的代理，应确保此时已经就绪。
    ///
    /// # 后置条件
    /// - 返回的挂钩对象仅持有借用引用，可按值传递或存放在栈上，不会增加引用计数。
    pub fn new(provider: &'a dyn MetricsProvider) -> Self {
        Self { provider }
    }

    /// 在请求进入业务逻辑前记录并发数。
    ///
    /// # 调用契约
    /// - **输入**：`base_attributes` 需至少包含 `service.name`、`route.id`、`operation`、`protocol`；
    /// - **行为**：内部调用 `spark.request.inflight` Gauge 的 `increment(1)`，用于统计当前并发；
    /// - **前置条件**：应在真正执行业务逻辑前调用，以免遗漏极短请求；
    /// - **后置条件**：Gauge 值增加 1，便于在失败前也能观察并发变化。
    pub fn on_call_start(&self, base_attributes: AttributeSet<'_>) {
        self.provider
            .gauge(&contract::REQUEST_INFLIGHT)
            .increment(1.0, base_attributes);
    }

    /// 在请求完成时记录延迟、总数以及错误计数，并回收并发 Gauge。
    ///
    /// # 调用契约
    /// - **输入参数**：
    ///   - `base_attributes`：与 `on_call_start` 使用的属性集合一致，用于匹配 Gauge 标签；
    ///   - `outcome`：请求结果，驱动是否累加错误计数；
    ///   - `duration`：端到端耗时，内部转换为毫秒写入直方图；
    ///   - `completion_attributes`：包含 `base_attributes` 以及 `status.code`、`outcome`、`error.kind`（当失败时）等扩展标签；
    /// - **执行流程（How）**：
    ///   1. 调用 Gauge `decrement(1)` 释放并发计数；
    ///   2. 将耗时写入 `spark.request.duration` 直方图；
    ///   3. 将请求计入 `spark.request.total`；
    ///   4. 若 `outcome == Error`，额外累加 `spark.request.errors`；
    /// - **前置条件**：必须与 `on_call_start` 成对调用，即便业务报错也需执行，以免 Gauge 泄漏；
    /// - **后置条件**：所有计数器成功累加一次，Gauge 值回到先前水平。
    pub fn on_call_finish(
        &self,
        base_attributes: AttributeSet<'_>,
        outcome: ServiceOutcome,
        duration: Duration,
        completion_attributes: AttributeSet<'_>,
    ) {
        self.provider
            .gauge(&contract::REQUEST_INFLIGHT)
            .decrement(1.0, base_attributes);

        let duration_ms = duration.as_secs_f64() * 1_000.0;
        self.provider.record_histogram(
            &contract::REQUEST_DURATION,
            duration_ms,
            completion_attributes,
        );
        self.provider
            .record_counter_add(&contract::REQUEST_TOTAL, 1, completion_attributes);

        if matches!(outcome, ServiceOutcome::Error) {
            self.provider
                .record_counter_add(&contract::REQUEST_ERRORS, 1, completion_attributes);
        }
    }

    /// 记录请求或响应的有效载荷大小。
    ///
    /// # 调用契约
    /// - **输入参数**：
    ///   - `direction`：指明是入站还是出站；
    ///   - `bytes`：有效载荷大小，单位字节；
    ///   - `attributes`：与 `base_attributes` 一致的标签集合；
    /// - **行为**：根据方向选择 `spark.bytes.inbound` 或 `spark.bytes.outbound`，执行 `record_counter_add`；
    /// - **前置条件**：`bytes` 应为累积量，可多次调用累加；
    /// - **后置条件**：相应指标增加 `bytes`，用于统计吞吐量。
    pub fn record_payload_bytes(
        &self,
        direction: PayloadDirection,
        bytes: u64,
        attributes: AttributeSet<'_>,
    ) {
        self.provider
            .record_counter_add(direction.descriptor(), bytes, attributes);
    }

    /// 记录 `Service::poll_ready` 的返回结果，并在遇到 `RetryAfter` 时追加节律指标。
    ///
    /// # 教案式说明
    /// - **意图 (Why)**：统一将 Ready/Busy/BudgetExhausted/RetryAfter 四态映射到指标，
    ///   便于在 Grafana 上与 `spark.request.ready_state`、`retry_after_total` 等图表对齐；
    ///   特别是 RetryAfter 需要同步记录等待时长，才能验证退避策略是否符合预期节奏。
    /// - **契约 (What)**：
    ///   - **输入参数**：
    ///     - `base_attributes`：服务通用标签集合（`service.name` 等），会被复制后追加 `ready.*` 标签；
    ///     - `state`：`poll_ready` 返回的 [`ReadyState`]；
    ///   - **前置条件**：调用方需保证 `base_attributes` 使用稳定标签值；
    ///   - **后置条件**：
    ///     1. `spark.request.ready_state` 计数器累加一次；
    ///     2. 若 `state` 为 `RetryAfter`，额外累加 `retry_after_total` 并将等待毫秒数写入直方图；
    ///     3. 标签集始终包含 `ready.state` 与 `ready.detail`，避免出现“缺失维度”的观测陷阱。
    /// - **实现 (How)**：
    ///   1. 根据 `state` 解析出稳定标签值（例如 `busy` + `queue_full`）；
    ///   2. 使用 [`OwnedAttributeSet::extend_from`] 克隆基础标签并追加 `ready.*` 标签；
    ///   3. 写入 `REQUEST_READY_STATE` 计数器；
    ///   4. 当分支为 `RetryAfter` 时，调用内部辅助函数写入次数与延迟直方图。
    /// - **风险与权衡 (Trade-offs)**：
    ///   - 复制标签会产生少量堆分配，建议上层复用 `OwnedAttributeSet` 或按需抽样；
    ///   - `RetryAdvice` 未来若扩展绝对时间 (`at`) 语义，需要在标签映射中新增对 `ready.detail` 的映射。
    pub fn record_ready_state(&self, base_attributes: AttributeSet<'_>, state: &ReadyState) {
        let (state_label, detail_label) = ready_state_labels(state);
        let mut owned = OwnedAttributeSet::new();
        owned.extend_from(base_attributes);
        owned.push_owned(contract::ATTR_READY_STATE, state_label);
        owned.push_owned(contract::ATTR_READY_DETAIL, detail_label);

        let attributes = owned.as_slice();
        self.provider
            .record_counter_add(&contract::REQUEST_READY_STATE, 1, attributes);

        if let ReadyState::RetryAfter(advice) = state {
            self.record_retry_after_metrics(advice, attributes);
        }
    }

    /// 将 RetryAfter 信号映射为次数与延迟直方图。
    fn record_retry_after_metrics(&self, advice: &RetryAdvice, attributes: AttributeSet<'_>) {
        self.provider
            .record_counter_add(&contract::RETRY_AFTER_TOTAL, 1, attributes);

        let wait_ms = advice.wait.as_secs_f64() * 1_000.0;
        self.provider
            .record_histogram(&contract::RETRY_AFTER_DELAY_MS, wait_ms, attributes);
    }
}

/// 根据 `ReadyState` 返回稳定的标签取值。
fn ready_state_labels(state: &ReadyState) -> (&'static str, &'static str) {
    match state {
        ReadyState::Ready => (
            contract::READY_STATE_READY,
            contract::READY_DETAIL_PLACEHOLDER,
        ),
        ReadyState::Busy(reason) => (
            contract::READY_STATE_BUSY,
            match reason {
                BusyReason::Upstream => contract::READY_DETAIL_UPSTREAM,
                BusyReason::Downstream => contract::READY_DETAIL_DOWNSTREAM,
                BusyReason::QueueFull(_) => contract::READY_DETAIL_QUEUE_FULL,
                BusyReason::Custom(_) => contract::READY_DETAIL_CUSTOM,
            },
        ),
        ReadyState::BudgetExhausted(_) => (
            contract::READY_STATE_BUDGET_EXHAUSTED,
            contract::READY_DETAIL_PLACEHOLDER,
        ),
        ReadyState::RetryAfter(advice) => {
            let detail = if advice.reason.is_some() {
                contract::READY_DETAIL_CUSTOM
            } else {
                contract::READY_DETAIL_RETRY_AFTER
            };
            (contract::READY_STATE_RETRY_AFTER, detail)
        }
    }
}
