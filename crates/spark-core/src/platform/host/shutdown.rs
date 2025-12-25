//! 优雅关闭契约类型：在 `spark-core` 内保存跨宿主实现的共享语义。
//!
//! # 教案式导航
//! - **定位（Where）**：本模块仅包含轻量类型，不再绑定任何运行时或日志依赖；实际协调流程迁移至
//!   `spark_hosting::shutdown` 模块。
//! - **动机（Why）**：确保所有实现均复用统一的枚举、结构体与回调签名，避免“契约 vs. 实现”分裂；
//! - **扩展（How）**：宿主方可在 `spark-hosting` 或自定义扩展中组合这些类型构建关闭协调器。

use alloc::{borrow::Cow, boxed::Box, sync::Arc, vec::Vec};
use core::fmt;
use core::time::Duration;

use crate::SparkError;
use crate::contract::{CloseReason, Deadline};
use crate::future::BoxFuture;
use crate::pipeline::Channel;

type TriggerFn = dyn Fn(&CloseReason, Option<Deadline>) + Send + Sync + 'static;
type AwaitFn =
    dyn Fn() -> BoxFuture<'static, crate::Result<(), SparkError>> + Send + Sync + 'static;
type ForceFn = dyn Fn() + Send + Sync + 'static;

/// `GracefulShutdownStatus` 表示单个关闭目标的执行结果。
///
/// # 设计初衷（Why）
/// - 宿主在停机时需要判定每个长寿命对象（Channel、Transport、Router 等）的关闭结果，以便决定是否升级为硬关闭；
/// - 在 A/B 演练或双活环境中，需要对比“全部成功”“部分失败”“被强制终止”三类情况，本枚举提供稳定语义。
///
/// # 契约说明（What）
/// - `Completed`：目标在截止时间内完成优雅关闭；
/// - `Failed`：目标显式返回 [`SparkError`]，代表调用方需进行审计或补偿；
/// - `ForcedTimeout`：在截止时间到期后仍未完成，被协调器触发硬关闭，需重点关注写缓冲是否丢失。
///
/// # 风险提示（Trade-offs）
/// - `Failed` 不会自动触发硬关闭，由调用方根据错误码决定后续动作；
/// - `ForcedTimeout` 仅表示协调器调用了硬关闭钩子，并不保证资源立即释放，宿主需结合日志/指标确认状态。
#[derive(Debug)]
#[non_exhaustive]
pub enum GracefulShutdownStatus {
    Completed,
    Failed(SparkError),
    ForcedTimeout,
}

/// `GracefulShutdownRecord` 记录单个目标的关闭摘要。
///
/// # 设计初衷（Why）
/// - 为宿主审计与单元测试提供结构化结果，避免解析日志才能获取结论；
/// - 便于在多目标停机流程中保留顺序信息，与日志、运维事件对齐。
///
/// # 契约说明（What）
/// - `label`：调用方注册目标时提供的稳定标识，用于日志与审计；
/// - `status`：对应的 [`GracefulShutdownStatus`]；
/// - `elapsed`：从发起等待到返回结果的持续时间，便于识别耗时热点。
///
/// # 风险提示（Trade-offs）
/// - `elapsed` 基于协调器读取的单调时钟，若宿主实现 `TimeDriver::sleep` 为近似值，结果也会同步偏差；
/// - 结构体设计为只读视图，不暴露可变引用，防止测试在断言时意外修改状态。
#[derive(Debug)]
pub struct GracefulShutdownRecord {
    label: Cow<'static, str>,
    status: GracefulShutdownStatus,
    elapsed: Duration,
}

impl GracefulShutdownRecord {
    /// 获取目标标识。
    pub fn label(&self) -> &str {
        &self.label
    }

    /// 获取关闭状态。
    pub fn status(&self) -> &GracefulShutdownStatus {
        &self.status
    }

    /// 获取目标关闭耗时。
    pub fn elapsed(&self) -> Duration {
        self.elapsed
    }
}

/// `GracefulShutdownReport` 汇总整次停机的执行情况。
///
/// # 设计初衷（Why）
/// - 宿主需要在停机结束后得到结构化摘要，以驱动指标打点或自动化审计；
/// - 支持测试验证“全部完成 / 部分失败 / 强制关闭数量”等指标，确保契约在 CI 中可回归。
///
/// # 契约说明（What）
/// - `reason`：触发关闭的业务原因；
/// - `deadline`：整体截止时间快照；
/// - `results`：按注册顺序排列的各目标关闭记录；
/// - `forced_count()`：统计硬关闭目标数量，便于快速判断风险等级。
///
/// # 风险提示（Trade-offs）
/// - `deadline` 仅记录触发时的计划值，若宿主在流程中动态调整截止时间，需要结合日志分析；
/// - `results` 只记录协调器内部视角，不代表底层实现已经真正释放资源，必要时需结合 `OpsEvent` 与指标确认。
#[derive(Debug)]
pub struct GracefulShutdownReport {
    reason: CloseReason,
    deadline: Option<Deadline>,
    results: Vec<GracefulShutdownRecord>,
}

impl GracefulShutdownReport {
    /// 获取关闭原因。
    pub fn reason(&self) -> &CloseReason {
        &self.reason
    }

    /// 获取关闭截止计划。
    pub fn deadline(&self) -> Option<Deadline> {
        self.deadline
    }

    /// 访问各目标关闭结果。
    pub fn results(&self) -> &[GracefulShutdownRecord] {
        &self.results
    }

    /// 统计被强制关闭的目标数量。
    pub fn forced_count(&self) -> usize {
        self.results
            .iter()
            .filter(|record| matches!(record.status, GracefulShutdownStatus::ForcedTimeout))
            .count()
    }

    /// 统计显式失败的目标数量。
    pub fn failure_count(&self) -> usize {
        self.results
            .iter()
            .filter(|record| matches!(record.status, GracefulShutdownStatus::Failed(_)))
            .count()
    }

    /// 构造新的关闭报告。
    ///
    /// - **输入参数**：业务关闭原因、可选截止时间、目标关闭记录列表；
    /// - **使用场景**：宿主实现（例如 `spark-hosting`）在完成协调流程后生成最终报告；
    /// - **注意事项**：调用方应确保 `results` 顺序与注册顺序一致，便于交叉对齐日志与追踪。
    pub fn new(
        reason: CloseReason,
        deadline: Option<Deadline>,
        results: Vec<GracefulShutdownRecord>,
    ) -> Self {
        Self {
            reason,
            deadline,
            results,
        }
    }
}

/// `GracefulShutdownTarget` 描述协调器可管理的单个关闭对象。
///
/// # 设计初衷（Why）
/// - 将“如何触发优雅关闭”“如何等待完成”“如何执行硬关闭”封装为统一结构，避免宿主在不同对象上重复编写样板代码；
/// - 支持通过自定义回调扩展到 Channel 之外的任意长寿命对象（如 Router、Transport、外部任务）。
///
/// # 契约说明（What）
/// - `label`：稳定标识，用于日志、审计、报告；
/// - `trigger_graceful`：在协调器发起关闭时执行，需尽快返回；
/// - `await_closed`：返回等待关闭完成的 Future；
/// - `force_close`：在超时或业务选择硬关闭时调用，应执行幂等操作。
///
/// # 风险提示（Trade-offs）
/// - `trigger_graceful` 在调用期间禁止阻塞，否则会拖慢整个关闭流程；
/// - 若对象不支持硬关闭，可在 `force_close` 中记录日志或保持空实现，但应在文档说明风险。
pub struct GracefulShutdownTarget {
    label: Cow<'static, str>,
    trigger_graceful: Box<TriggerFn>,
    await_closed: Box<AwaitFn>,
    force_close: Box<ForceFn>,
}

impl GracefulShutdownTarget {
    /// 基于 [`Channel`] 构造默认的关闭目标，自动调用 `close_graceful`/`closed`/`close`。
    ///
    /// # 契约说明（What）
    /// - **前置条件**：`label` 应能唯一标识通道；
    /// - **后置条件**：协调器会按照 Channel 契约触发优雅关闭并等待完成，超时时调用 `close()` 触发硬关闭。
    pub fn for_channel(label: impl Into<Cow<'static, str>>, channel: Arc<dyn Channel>) -> Self {
        let graceful_channel = Arc::clone(&channel);
        let closed_channel = Arc::clone(&channel);
        let force_channel = Arc::clone(&channel);
        Self {
            label: label.into(),
            trigger_graceful: Box::new(move |reason, deadline| {
                graceful_channel.close_graceful(reason.clone(), deadline);
            }),
            await_closed: Box::new(move || closed_channel.closed()),
            force_close: Box::new(move || {
                force_channel.close();
            }),
        }
    }

    /// 基于自定义回调构造关闭目标，适用于 Router、Service 等非 Channel 对象。
    ///
    /// # 契约说明（What）
    /// - `trigger_graceful`：接收关闭原因与截止时间，要求非阻塞；
    /// - `await_closed`：返回等待完成的 Future；
    /// - `force_close`：执行硬关闭钩子，可为 no-op。
    pub fn for_callbacks(
        label: impl Into<Cow<'static, str>>,
        trigger_graceful: impl Fn(&CloseReason, Option<Deadline>) + Send + Sync + 'static,
        await_closed: impl Fn() -> BoxFuture<'static, crate::Result<(), SparkError>>
        + Send
        + Sync
        + 'static,
        force_close: impl Fn() + Send + Sync + 'static,
    ) -> Self {
        Self {
            label: label.into(),
            trigger_graceful: Box::new(trigger_graceful),
            await_closed: Box::new(await_closed),
            force_close: Box::new(force_close),
        }
    }

    /// 返回注册时记录的目标标识。
    ///
    /// - **用途**：日志与指标在触发关闭事件时需要引用该标识；
    /// - **注意**：返回值为借用，调用方若需延长生命周期请显式克隆。
    pub fn label(&self) -> &str {
        &self.label
    }

    /// 触发优雅关闭回调。
    ///
    /// - **契约**：调用必须无阻塞，且允许重复触发（幂等）；
    /// - **行为**：直接转发给注册时提供的闭包，实现层负责派发 FIN 等操作。
    pub fn trigger(&self, reason: &CloseReason, deadline: Option<Deadline>) {
        (self.trigger_graceful)(reason, deadline);
    }

    /// 构造等待关闭完成的 Future。
    ///
    /// - **返回值**：与 `for_callbacks` 注册时一致的 `BoxFuture`；
    /// - **风险提示**：调用者负责在 Future 被丢弃时触发硬关闭或记录日志。
    pub fn await_closed(&self) -> BoxFuture<'static, crate::Result<(), SparkError>> {
        (self.await_closed)()
    }

    /// 执行硬关闭回调。
    ///
    /// - **用途**：用于截止时间到期或上游指令要求强制关闭的场景；
    /// - **要求**：实现需保证幂等性，可选择记录额外日志。
    pub fn force_close(&self) {
        (self.force_close)();
    }

    /// 消耗目标并生成关闭记录。
    ///
    /// - **输入**：目标最终状态与耗时；
    /// - **输出**：对应的 [`GracefulShutdownRecord`]；
    /// - **后置条件**：内部回调被丢弃，调用方不能再触发关闭行为。
    pub fn into_record(
        self,
        status: GracefulShutdownStatus,
        elapsed: Duration,
    ) -> GracefulShutdownRecord {
        GracefulShutdownRecord {
            label: self.label,
            status,
            elapsed,
        }
    }
}

impl fmt::Debug for GracefulShutdownTarget {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GracefulShutdownTarget")
            .field("label", &self.label)
            .finish_non_exhaustive()
    }
}
