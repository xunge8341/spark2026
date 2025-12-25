#![allow(clippy::module_name_repetitions)]

use alloc::borrow::ToOwned;
use alloc::{borrow::Cow, boxed::Box, format, sync::Arc, vec::Vec};
use core::{fmt, time::Duration};

use spark_core::{
    Result as CoreResult, SparkError,
    contract::{CloseReason, Deadline},
    future::BoxFuture,
    host::{GracefulShutdownReport, GracefulShutdownStatus, GracefulShutdownTarget},
    observability::{
        OpsEvent, OwnedAttributeSet, TraceContext, keys::logging::shutdown as shutdown_fields,
    },
    pipeline::Channel,
    runtime::{AsyncRuntime, CoreServices, MonotonicTimePoint},
};

/// `GracefulShutdownCoordinator` 负责串联宿主的优雅关闭流程。
///
/// # 教案式注解
/// - **架构定位 (Where)**：该结构位于 `spark-hosting` crate，用于在实际宿主实现中复用 `spark-core`
///   暴露的关闭契约类型；
/// - **问题动机 (Why)**：T19 任务要求“所有长寿对象可通知关闭并等待完成”，协调器集中编排日志、运维
///   事件与计时策略，使停机流程具备可观测性与可回归性；
/// - **协作方式 (How)**：调用方首先通过 [`Self::register_target`] 或 [`Self::register_channel`] 注册需要托管的对象，随后
///   调用 [`Self::shutdown`] 广播关闭信号并串联等待逻辑。
///
/// # 契约说明 (What)
/// - **前置条件**：`CoreServices` 内的 `runtime`、`logger`、`ops_bus` 必须实现完整；
/// - **输入**：`reason` 为业务关闭原因，`deadline` 为整体截止时间（可选）；
/// - **后置条件**：所有目标至少接收一次优雅关闭通知，超时路径必定触发硬关闭回调并输出 WARN 日志。
///
/// # 风险提示 (Trade-offs)
/// - 当前按照注册顺序逐个等待各目标完成；若需要更激进的并发等待，可在后续引入 `join` 类扩展；
/// - 当 `deadline` 已过期时会立即执行硬关闭，保证流程不被卡死，但可能丢失尚未持久化的状态。
pub struct GracefulShutdownCoordinator {
    services: CoreServices,
    targets: Vec<GracefulShutdownTarget>,
    audit_traces: Vec<TraceContext>,
}

impl GracefulShutdownCoordinator {
    /// 使用宿主运行时服务构造协调器。
    ///
    /// - **输入参数**：调用方需传入 [`CoreServices`]，其中包含运行时、日志与运维事件总线；
    /// - **前置条件**：`CoreServices` 各字段不可为空指针，且运行时需支持 `sleep`/`now`；
    /// - **后置条件**：返回的协调器内部尚未注册任何目标。
    pub fn new(services: CoreServices) -> Self {
        Self {
            services,
            targets: Vec::new(),
            audit_traces: Vec::new(),
        }
    }

    /// 注册关闭目标。
    ///
    /// - **输入参数**：[`GracefulShutdownTarget`]，由调用方根据业务对象自行封装；
    /// - **效果**：目标将被追加到协调器内部列表，保持注册顺序；
    /// - **风险提示**：请确保 `target` 内部回调保持幂等，避免多次注册导致副作用叠加。
    pub fn register_target(&mut self, target: GracefulShutdownTarget) {
        self.targets.push(target);
    }

    /// 以 Channel 注册关闭目标的便捷方法。
    ///
    /// - **设计意图**：为 Pipeline/Transport 等实现复用默认行为，避免重复编写关闭桥接逻辑；
    /// - **输入**：`label` 用于日志 & 审计标识；`channel` 为需要托管的 [`Channel`]；
    /// - **实现细节**：内部调用 [`GracefulShutdownTarget::for_channel`] 生成目标并注册。
    pub fn register_channel(
        &mut self,
        label: impl Into<Cow<'static, str>>,
        channel: Arc<dyn Channel>,
    ) {
        self.register_target(GracefulShutdownTarget::for_channel(label, channel));
    }

    /// 附加审计链路追踪上下文，便于停机日志与分布式追踪关联。
    ///
    /// - **输入**：[`TraceContext`]（通常来源于上游控制指令）；
    /// - **行为**：顺序存储在内部向量中，INFO 日志使用首个条目，其余条目以 TRACE 记录；
    /// - **风险**：请避免一次性附加大量上下文，以免日志放大。
    pub fn add_audit_trace(&mut self, trace: TraceContext) {
        self.audit_traces.push(trace);
    }

    /// 发起优雅关闭并等待结果，返回结构化报告。
    ///
    /// - **输入**：关闭原因 `reason` 与可选截止时间 `deadline`；
    /// - **流程概览**：
    ///   1. 广播 `ShutdownTriggered` 运维事件并输出结构化日志；
    ///   2. 并行触发所有目标的优雅关闭回调；
    ///   3. 逐个等待目标完成，基于运行时计时器执行超时竞赛；
    ///   4. 收集 [`spark_core::host::GracefulShutdownRecord`] 列表，生成最终的 [`GracefulShutdownReport`]。
    /// - **返回值**：成功时给出报告；若任何目标 Future 返回 [`SparkError`]，会记录在报告中。
    pub fn shutdown(
        mut self,
        reason: CloseReason,
        deadline: Option<Deadline>,
    ) -> BoxFuture<'static, GracefulShutdownReport> {
        let services = self.services.clone();
        let targets = core::mem::take(&mut self.targets);
        let traces = core::mem::take(&mut self.audit_traces);

        Box::pin(async move {
            let runtime = Arc::clone(&services.runtime);
            let logger = Arc::clone(&services.logger);
            let ops_bus = Arc::clone(&services.ops_bus);

            let now = runtime.now();
            let absolute_deadline = deadline.and_then(|d| d.instant());
            let deadline_duration = absolute_deadline
                .map(|point| point.saturating_duration_since(now))
                .unwrap_or(Duration::MAX);

            ops_bus.broadcast(OpsEvent::ShutdownTriggered {
                deadline: deadline_duration,
            });

            let mut fields = OwnedAttributeSet::new();
            fields.push_owned(shutdown_fields::FIELD_REASON_CODE, reason.code().to_owned());
            fields.push_owned(
                shutdown_fields::FIELD_REASON_MESSAGE,
                reason.message().to_owned(),
            );
            fields.push_owned(shutdown_fields::FIELD_TARGET_COUNT, targets.len() as u64);
            fields.push_owned(
                shutdown_fields::FIELD_DEADLINE_MS,
                deadline_duration.as_millis() as i64,
            );
            let message = format!(
                "graceful shutdown initiated (code={}, targets={})",
                reason.code(),
                targets.len()
            );
            logger.info_with_fields(message.as_str(), fields.as_slice(), traces.first());

            for trace in traces.iter().skip(1) {
                logger.trace("shutdown trace-context attached", Some(trace));
            }

            for target in &targets {
                target.trigger(&reason, deadline);
            }

            let mut results = Vec::with_capacity(targets.len());
            for target in targets {
                let start = runtime.now();
                let wait_future = TimeoutFuture::new(
                    target.await_closed(),
                    absolute_deadline,
                    Arc::clone(&runtime),
                );

                match wait_future.await {
                    TimeoutOutcome::Completed(Ok(())) => {
                        let elapsed = runtime.now().saturating_duration_since(start);
                        results
                            .push(target.into_record(GracefulShutdownStatus::Completed, elapsed));
                    }
                    TimeoutOutcome::Completed(Err(err)) => {
                        let mut warn_fields = OwnedAttributeSet::new();
                        warn_fields.push_owned(
                            shutdown_fields::FIELD_TARGET_LABEL,
                            target.label().to_owned(),
                        );
                        warn_fields.push_owned(shutdown_fields::FIELD_ERROR_CODE, err.code());
                        logger.error_with_fields(
                            "graceful shutdown failed",
                            Some(&err),
                            warn_fields.as_slice(),
                            err.trace_context(),
                        );
                        let elapsed = runtime.now().saturating_duration_since(start);
                        results
                            .push(target.into_record(GracefulShutdownStatus::Failed(err), elapsed));
                    }
                    TimeoutOutcome::TimedOut => {
                        target.force_close();
                        let elapsed = runtime.now().saturating_duration_since(start);
                        let mut warn_fields = OwnedAttributeSet::new();
                        warn_fields.push_owned(
                            shutdown_fields::FIELD_TARGET_LABEL,
                            target.label().to_owned(),
                        );
                        warn_fields.push_owned(
                            shutdown_fields::FIELD_ELAPSED_MS,
                            elapsed.as_millis() as i64,
                        );
                        logger.warn_with_fields(
                            "graceful shutdown timed out and forced",
                            warn_fields.as_slice(),
                            None,
                        );
                        results.push(
                            target.into_record(GracefulShutdownStatus::ForcedTimeout, elapsed),
                        );
                    }
                }
            }

            GracefulShutdownReport::new(reason, deadline, results)
        })
    }
}

impl fmt::Debug for GracefulShutdownCoordinator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GracefulShutdownCoordinator")
            .field("target_count", &self.targets.len())
            .finish()
    }
}

/// 内部 Future，负责在目标 Future 与超时计时之间做竞赛。
///
/// # 教案式说明
/// - **Why**：避免额外依赖，在 `no_std + alloc` 环境下手工实现 `select` 语义；
/// - **How**：并行维护目标 Future 与基于运行时的计时器，先完成者获胜；
/// - **Contract**：若计时器率先完成则返回 [`TimeoutOutcome::TimedOut`]，否则返回目标 Future 结果。
struct TimeoutFuture {
    future: BoxFuture<'static, CoreResult<(), SparkError>>,
    timer: Option<BoxFuture<'static, ()>>,
}

impl TimeoutFuture {
    fn new(
        future: BoxFuture<'static, CoreResult<(), SparkError>>,
        deadline: Option<MonotonicTimePoint>,
        runtime: Arc<dyn AsyncRuntime>,
    ) -> Self {
        let timer = deadline.map(|abs_deadline| {
            let now = runtime.now();
            let wait = abs_deadline.saturating_duration_since(now);
            if wait.is_zero() {
                let fut: BoxFuture<'static, ()> = Box::pin(async {});
                fut
            } else {
                let driver = Arc::clone(&runtime);
                let fut: BoxFuture<'static, ()> = Box::pin(async move {
                    driver.sleep(wait).await;
                });
                fut
            }
        });

        Self { future, timer }
    }
}

enum TimeoutOutcome<T> {
    Completed(T),
    TimedOut,
}

impl core::future::Future for TimeoutFuture {
    type Output = TimeoutOutcome<CoreResult<(), SparkError>>;

    fn poll(
        mut self: core::pin::Pin<&mut Self>,
        cx: &mut core::task::Context<'_>,
    ) -> core::task::Poll<Self::Output> {
        if let core::task::Poll::Ready(result) = self.future.as_mut().poll(cx) {
            return core::task::Poll::Ready(TimeoutOutcome::Completed(result));
        }

        if let Some(timer) = self.timer.as_mut()
            && let core::task::Poll::Ready(()) = timer.as_mut().poll(cx)
        {
            return core::task::Poll::Ready(TimeoutOutcome::TimedOut);
        }

        core::task::Poll::Pending
    }
}
