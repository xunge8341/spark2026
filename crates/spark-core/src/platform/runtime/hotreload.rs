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
use alloc::{borrow::Cow, sync::Arc};
use core::fmt;
use core::time::Duration;

use crate::observability::{
    MetricsProvider, OwnedAttributeSet, metrics::contract::hot_reload as contract,
};

use spin::{RwLock, RwLockReadGuard, RwLockWriteGuard};

/// 热更新切换过程的全局栅栏，确保在同一个写锁持有期间完成原子级配置替换。
///
/// # 教案级注释（Why）
/// - 限流、超时等运行时配置在热更新时需要“边界点切换”，否则读取方可能观察到撕裂状态；
/// - 通过共享的读写锁，写线程在持有写锁期间可一次性刷新同一批组件；读线程在持有读锁期间
///   则能读取到一致的快照；
/// - 该结构封装锁实现，便于未来替换为 RCU/epoch based reclamation 等机制，而不影响调用方。
///
/// # 契约说明（What）
/// - `read`：返回读锁守卫，允许多个线程并发读取快照；
/// - `write`：返回写锁守卫，在守卫释放前禁止新的读写进入，适合批量提交配置；
/// - Clone 后的实例共享同一把锁，可在不同运行时配置间传递以构造统一栅栏。
#[derive(Clone, Debug, Default)]
pub struct HotReloadFence {
    lock: Arc<RwLock<()>>,
}

impl HotReloadFence {
    /// 构造新的热更新栅栏。
    pub fn new() -> Self {
        Self {
            lock: Arc::new(RwLock::new(())),
        }
    }

    /// 进入读侧临界区，保证在守卫释放前不会被写侧打断。
    pub fn read(&self) -> HotReloadReadGuard<'_> {
        HotReloadReadGuard {
            _guard: self.lock.read(),
        }
    }

    /// 进入写侧临界区，确保在守卫释放前完成一次性配置切换。
    pub fn write(&self) -> HotReloadWriteGuard<'_> {
        HotReloadWriteGuard {
            _guard: self.lock.write(),
        }
    }
}

/// 读锁守卫的轻量包装，主要用于标记生命周期，避免直接暴露底层类型。
pub struct HotReloadReadGuard<'a> {
    _guard: RwLockReadGuard<'a, ()>,
}

impl fmt::Debug for HotReloadReadGuard<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("HotReloadReadGuard")
    }
}

/// 写锁守卫的轻量包装，配合 [`HotReloadFence`] 实现跨组件的同步提交。
pub struct HotReloadWriteGuard<'a> {
    _guard: RwLockWriteGuard<'a, ()>,
}

impl fmt::Debug for HotReloadWriteGuard<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("HotReloadWriteGuard")
    }
}

/// 热更新应用耗时的计时器封装，兼容 `std` 与 `no_std` (alloc) 构建模式。
///
/// - 在 `std` 环境下内部记录 `std::time::Instant`，提供真实的耗时；
/// - 在 `no_std + alloc` 下为空结构，`elapsed` 恒返回 `None`，调用方可选择性忽略直方图打点。
#[derive(Clone, Copy, Debug)]
pub struct HotReloadApplyTimer {
    #[cfg(feature = "std")]
    start: std::time::Instant,
}

impl HotReloadApplyTimer {
    /// 启动计时。
    pub fn start() -> Self {
        Self {
            #[cfg(feature = "std")]
            start: std::time::Instant::now(),
        }
    }

    /// 返回自启动以来的耗时；`no_std` 环境下返回 `None`。
    pub fn elapsed(&self) -> Option<Duration> {
        #[cfg(feature = "std")]
        {
            Some(self.start.elapsed())
        }

        #[cfg(not(feature = "std"))]
        {
            None
        }
    }
}

/// 热更新观测性的统一封装，用于在每次配置切换后上报 `config_epoch` 与应用延迟。
pub(crate) struct HotReloadObservability {
    metrics: Option<HotReloadMetrics>,
}

impl HotReloadObservability {
    /// 构造空的观测性对象，默认不打点。
    pub(crate) fn new() -> Self {
        Self { metrics: None }
    }

    /// 构造绑定指标提供者与组件标签的观测性对象。
    pub(crate) fn with_component(
        provider: Arc<dyn MetricsProvider>,
        component: Cow<'static, str>,
    ) -> Self {
        let mut attributes = OwnedAttributeSet::new();
        attributes.push_owned(contract::ATTR_COMPONENT, component.into_owned());
        Self {
            metrics: Some(HotReloadMetrics {
                provider,
                attributes,
            }),
        }
    }

    /// 记录最新纪元与可选的应用耗时。
    pub(crate) fn record(&self, epoch: u64, latency: Option<Duration>) {
        if let Some(metrics) = &self.metrics {
            metrics.record(epoch, latency);
        }
    }
}

struct HotReloadMetrics {
    provider: Arc<dyn MetricsProvider>,
    attributes: OwnedAttributeSet,
}

impl HotReloadMetrics {
    fn record(&self, epoch: u64, latency: Option<Duration>) {
        let attrs = self.attributes.as_slice();
        self.provider
            .record_gauge_set(&contract::CONFIG_EPOCH, epoch as f64, attrs);

        if let Some(latency) = latency {
            let millis = latency.as_secs_f64() * 1_000.0;
            self.provider
                .record_histogram(&contract::APPLY_LATENCY_MS, millis, attrs);
        }
    }
}
