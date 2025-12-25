//! # Contract-only Runtime Surface
//!
//! ## 契约声明
//! * **Contract-only：** 本模块仅定义运行时可供合约调用的抽象 API，约束业务侧只能依赖这些接口而非具体执行器实现，以确保在无状态执行环境、回放环境中保持一致行为。
//! * **禁止实现：** 本文件及其子模块不允许落地具体执行逻辑，实现必须由宿主运行时或测试替身在独立 crate 中提供，从而杜绝合约代码在此处混入状态机或 I/O 细节。
//! * **解耦外设：** 所有接口均以 `Send + Sync + 'static` 能力描述，对具体执行器、定时器、异步 runtime 完全解耦，便于在 wasm/no-std 等受限环境中替换宿主。
//!
//! ## 并发与错误语义
//! * **并发模型：** 默认遵循单请求上下文内的协作式并发——接口返回的 future/stream 必须可被外部执行器安全地 `await`；禁止在实现中假设特定调度器或线程池。
//! * **错误传播：** 约定使用 `SparkError` 系列枚举表达业务/系统异常；调用方必须准备处理超时、取消、幂等失败等场景，实现方不得吞掉错误或 panic。
//!
//! ## 使用前后条件
//! * **前置条件：** 合约端在调用前必须确保上下文由外部运行时注入（如 tracing span、租户信息），且所有句柄均来自这些契约。
//! * **后置条件：** 成功调用保证返回值可用于继续构造合约逻辑，但不会持有宿主资源的独占所有权，避免破坏运行时调度；任何需要长生命周期资源的对象都必须通过显式托管接口申请。
//!
//! ## 设计取舍提示
//! * **架构选择：** 通过模块化接口换取统一性，牺牲了直接操作底层 executor 的灵活性，却换来可测试性与跨平台部署能力。
//! * **边界情况：** 合约作者需关注超时、重复调度、以及宿主拒绝服务等边界；本模块接口文档会明确每个 API 的退化行为，便于上层实现补偿逻辑。
//!
use crate::{async_trait, sealed::Sealed};
use alloc::boxed::Box;
use core::time::Duration;

/// `MonotonicTimePoint` 以相对时间刻度表达单调时钟读数。
///
/// # 设计背景（Why）
/// - `std::time::Instant` 在 `no_std` 场景不可用。该结构提供与其等价的基本能力，
///   以满足跨平台运行时的延时与调度需求。
///
/// # 逻辑解析（How）
/// - 内部以自启动以来的偏移量（`Duration`）表示，避免依赖壁钟时间。
/// - 提供加减操作与饱和差值，确保在不同硬件计时分辨率下行为一致。
///
/// # 契约说明（What）
/// - **前置条件**：调用方需确保所有时间点都来自同一计时来源，避免跨源比较导致语义错误。
/// - **后置条件**：由 [`TimeDriver::now`] 返回的时间点可直接与该结构协作运算。
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct MonotonicTimePoint(Duration);

impl MonotonicTimePoint {
    /// 根据启动以来的偏移量构造时间点。
    pub fn from_offset(offset: Duration) -> Self {
        MonotonicTimePoint(offset)
    }

    /// 返回自启动以来的时间偏移。
    pub fn as_duration(&self) -> Duration {
        self.0
    }

    /// 计算两个时间点的饱和差值。
    pub fn saturating_duration_since(&self, earlier: MonotonicTimePoint) -> Duration {
        self.0
            .checked_sub(earlier.0)
            .unwrap_or_else(|| Duration::from_secs(0))
    }

    /// 基于当前时间点创建新的偏移量。
    pub fn saturating_add(&self, delta: Duration) -> MonotonicTimePoint {
        MonotonicTimePoint(self.0.saturating_add(delta))
    }
}

/// `TimeDriver` 定义统一的计时与延时接口。
///
/// # 设计背景（Why）
/// - 吸收 Tokio 定时驱动（time driver）、Quiche `Timer` 等成熟实现，结合实时调度研究中的精确计时需求，
///   提供最小但可组合的时间原语。
///
/// # 逻辑解析（How）
/// - `now`：返回单调时钟读数。
/// - `sleep`：基于持续时间延时，常用于节流、超时与心跳。
/// - `sleep_until`：基于目标时间点延时，便于对齐周期性任务。
/// - 默认实现 `sleep_until` 通过比较当前时间与目标时间决定是否立即完成。
///
/// # 契约说明（What）
/// - **前置条件**：实现者必须保证 `now` 单调递增；否则延时语义将被破坏。
/// - **后置条件**：延时 Future 完成时，运行时应确保至少等待了指定时间间隔。
///
/// # 性能契约（Performance Contract）
/// - `sleep` 与 `sleep_until` 通过 `async fn` 暴露异步能力；宏 [`crate::async_trait`] 自动完成 Future 装箱，每次调用包含一次堆分配与虚表调度。
/// - `async_contract_overhead` Future 场景基于 20 万次轮询测得泛型实现 6.23ns/次、装箱路径 6.09ns/次（约 -0.9%）。
///   结果表明调度面引入的额外 CPU 消耗可控。【e8841c†L4-L13】
/// - 针对高频短延时（如 1ms 心跳），可复用内部 `Box` 缓冲或额外暴露泛型接口（例如 `fn sleep_typed(...) -> impl Future`）以避
///   免动态分发；调用方也可在掌握实现类型时直接使用具体 API。
///
/// # 风险提示（Trade-offs）
/// - `sleep_until` 默认实现使用 `saturating_duration_since`，在系统时钟回拨情况下会立即完成；
///   如需不同策略，可在实现中覆写该方法。
#[async_trait]
pub trait TimeDriver: Send + Sync + 'static + Sealed {
    fn now(&self) -> MonotonicTimePoint;

    async fn sleep(&self, duration: Duration);

    async fn sleep_until(&self, deadline: MonotonicTimePoint) {
        let now = self.now();
        let wait = deadline.saturating_duration_since(now);
        self.sleep(wait).await
    }
}
