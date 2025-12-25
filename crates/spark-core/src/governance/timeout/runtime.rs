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
use core::{
    fmt,
    sync::atomic::{AtomicU64, Ordering},
};

type Duration = core::time::Duration;

use crate::arc_swap::ArcSwap;
use crate::governance::configuration::{
    ConfigKey, ConfigScope, ConfigValue, ResolvedConfiguration,
};
use crate::governance::observability::MetricsProvider;
use crate::platform::runtime::{
    HotReloadApplyTimer, HotReloadFence, HotReloadObservability, HotReloadReadGuard,
    HotReloadWriteGuard,
};

/// 描述运行时关键超时阈值。
///
/// ### 设计目的（Why）
/// - 将请求超时与空闲连接超时集中管理，避免各组件散落维护导致语义不一致。
/// - 作为动态配置的解析结果，便于通过 [`TimeoutRuntimeConfig`] 在热更新时原子替换。
///
/// ### 契约说明（What）
/// - `request_timeout`：单次请求允许的最大持续时间，超过后运行时应主动取消；
/// - `idle_timeout`：连接或通道在无活动时允许保持的最长时间，用于清理闲置资源；
/// - 默认值分别为 5 秒与 60 秒，可满足大多数 RPC 场景的安全基准。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TimeoutSettings {
    request_timeout: Duration,
    idle_timeout: Duration,
}

impl TimeoutSettings {
    /// 构造带自定义阈值的设置。
    pub const fn new(request_timeout: Duration, idle_timeout: Duration) -> Self {
        Self {
            request_timeout,
            idle_timeout,
        }
    }

    /// 请求超时阈值。
    pub const fn request_timeout(&self) -> Duration {
        self.request_timeout
    }

    /// 空闲超时阈值。
    pub const fn idle_timeout(&self) -> Duration {
        self.idle_timeout
    }

    /// 从合并后的配置解析超时设置。
    pub fn from_configuration(
        config: &ResolvedConfiguration,
    ) -> crate::Result<Self, TimeoutConfigError> {
        let mut settings = Self::default();

        if let Some(value) = config.values.get(&request_timeout_key()) {
            let duration = parse_timeout_value(TimeoutField::Request, value)?;
            settings.request_timeout = duration;
        }

        if let Some(value) = config.values.get(&idle_timeout_key()) {
            let duration = parse_timeout_value(TimeoutField::Idle, value)?;
            settings.idle_timeout = duration;
        }

        Ok(settings)
    }
}

impl Default for TimeoutSettings {
    fn default() -> Self {
        Self::new(Duration::from_secs(5), Duration::from_secs(60))
    }
}

/// 热更新友好的超时配置容器。
///
/// ### 设计目的（Why）
/// - 历史实现依赖 ArcSwap 与纪元计数，实现“读无锁、写常数时间”的动态超时分发机制；纯契约阶段保留该交互设想以指导未来实现。
/// - 通过 `config_epoch()` 提供与观测面对齐的指标，便于确认配置是否生效。
///
/// ### 契约说明（What）
/// - `new`：接受初始设置并将纪元置零；
/// - `snapshot`：返回当前快照的 `Arc`，调用方可持久保存并在并发路径读取；
/// - `replace`：直接替换为给定设置；
/// - `update_from_configuration`：解析 [`ResolvedConfiguration`] 并在成功后递增纪元；
/// - 所有更新均原子生效，读者不需要额外同步原语。
pub struct TimeoutRuntimeConfig {
    epoch: AtomicU64,
    settings: ArcSwap<TimeoutSettings>,
    fence: HotReloadFence,
    observability: HotReloadObservability,
}

impl TimeoutRuntimeConfig {
    /// 创建新的运行时配置容器。
    pub fn new(initial: TimeoutSettings) -> Self {
        Self::with_shared_fence(initial, HotReloadFence::new())
    }

    /// 使用共享栅栏构造运行时配置，支持跨组件同步切换。
    pub fn with_shared_fence(initial: TimeoutSettings, fence: HotReloadFence) -> Self {
        Self {
            epoch: AtomicU64::new(0),
            settings: ArcSwap::new(Arc::new(initial)),
            fence,
            observability: HotReloadObservability::new(),
        }
    }

    /// 构造具备指标上报能力的运行时配置容器。
    pub fn with_observability(
        initial: TimeoutSettings,
        fence: HotReloadFence,
        metrics: Arc<dyn MetricsProvider>,
        component: impl Into<Cow<'static, str>>,
    ) -> Self {
        let observability = HotReloadObservability::with_component(metrics, component.into());
        observability.record(0, None);
        Self {
            epoch: AtomicU64::new(0),
            settings: ArcSwap::new(Arc::new(initial)),
            fence,
            observability,
        }
    }

    /// 返回当前使用的热更新栅栏。
    pub fn fence(&self) -> HotReloadFence {
        self.fence.clone()
    }

    /// 返回当前配置快照，并在内部获取读锁以保证一致性。
    pub fn snapshot(&self) -> Arc<TimeoutSettings> {
        let guard = self.fence.read();
        self.snapshot_with_fence(&guard)
    }

    /// 在调用方已持有读锁的情况下返回快照，避免重复加锁。
    pub fn snapshot_with_fence(&self, guard: &HotReloadReadGuard<'_>) -> Arc<TimeoutSettings> {
        let _ = guard;
        self.settings.load_full()
    }

    /// 查询配置纪元（从 0 开始），可用于导出指标或调试日志。
    pub fn config_epoch(&self) -> u64 {
        self.epoch.load(Ordering::SeqCst)
    }

    /// 直接替换为新的设置。
    pub fn replace(&self, settings: TimeoutSettings) {
        let timer = HotReloadApplyTimer::start();
        let guard = self.fence.write();
        let epoch = self.commit_with_guard(&guard, settings);
        self.observability.record(epoch, timer.elapsed());
    }

    /// 在共享写锁的上下文中替换配置。
    pub fn replace_with_fence(
        &self,
        guard: &HotReloadWriteGuard<'_>,
        settings: TimeoutSettings,
        timer: HotReloadApplyTimer,
    ) {
        let epoch = self.commit_with_guard(guard, settings);
        self.observability.record(epoch, timer.elapsed());
    }

    /// 解析并更新配置。
    pub fn update_from_configuration(
        &self,
        config: &ResolvedConfiguration,
    ) -> crate::Result<(), TimeoutConfigError> {
        let timer = HotReloadApplyTimer::start();
        let parsed = TimeoutSettings::from_configuration(config)?;
        let guard = self.fence.write();
        let epoch = self.commit_with_guard(&guard, parsed);
        self.observability.record(epoch, timer.elapsed());
        Ok(())
    }

    /// 在共享写锁的上下文中解析并更新配置。
    pub fn update_from_configuration_with_fence(
        &self,
        guard: &HotReloadWriteGuard<'_>,
        config: &ResolvedConfiguration,
        timer: HotReloadApplyTimer,
    ) -> crate::Result<(), TimeoutConfigError> {
        let parsed = TimeoutSettings::from_configuration(config)?;
        let epoch = self.commit_with_guard(guard, parsed);
        self.observability.record(epoch, timer.elapsed());
        Ok(())
    }

    fn commit_with_guard(&self, _guard: &HotReloadWriteGuard<'_>, next: TimeoutSettings) -> u64 {
        self.settings.store(Arc::new(next));
        self.epoch.fetch_add(1, Ordering::SeqCst) + 1
    }
}

impl Default for TimeoutRuntimeConfig {
    fn default() -> Self {
        Self::new(TimeoutSettings::default())
    }
}

/// 超时配置解析过程中可能出现的错误。
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TimeoutConfigError {
    InvalidValueType {
        field: TimeoutField,
        expected: &'static str,
    },
    NonPositiveDuration {
        field: TimeoutField,
        provided: i64,
    },
}

impl fmt::Display for TimeoutConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TimeoutConfigError::InvalidValueType { field, expected } => {
                write!(f, "invalid value type for {}: expected {}", field, expected)
            }
            TimeoutConfigError::NonPositiveDuration { field, provided } => {
                write!(
                    f,
                    "{} must be greater than zero milliseconds, got {}",
                    field, provided
                )
            }
        }
    }
}

impl crate::Error for TimeoutConfigError {
    fn source(&self) -> Option<&(dyn crate::Error + 'static)> {
        let _ = self;
        None
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TimeoutField {
    Request,
    Idle,
}

impl fmt::Display for TimeoutField {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TimeoutField::Request => f.write_str("timeouts.request"),
            TimeoutField::Idle => f.write_str("timeouts.idle"),
        }
    }
}

/// 返回请求超时的配置键。
///
/// ### 教案级注释
/// - **意图（Why）**：统一超时配置命名，便于运行时与配置中心约定键名，防止不同组件各自拼接字符串导致热更新撕裂。
/// - **契约（What）**：返回值指向 `runtime::timeouts.request_ms`，作用域固定为 [`ConfigScope::Runtime`]，调用方可据此从 [`ResolvedConfiguration`]
///   中检索毫秒数值。
/// - **逻辑（How）**：通过 [`ConfigKey::new`] 构造键对象，保持与 `spark-contract-tests`、运营平台使用的名称一致。
#[must_use]
pub fn request_timeout_key() -> ConfigKey {
    ConfigKey::new(
        "runtime",
        "timeouts.request_ms",
        ConfigScope::Runtime,
        "request timeout in milliseconds",
    )
}

/// 返回空闲超时的配置键。
///
/// ### 教案级注释
/// - **意图（Why）**：在连接管理、长轮询等场景需要统一读取空闲超时阈值，公开该键便于运行时热更新与观测系统共用。
/// - **契约（What）**：键名称固定为 `runtime::timeouts.idle_ms`，约定值类型为正整数毫秒或 [`ConfigValue::Duration`]。
/// - **逻辑（How）**：与请求超时相同，通过 [`ConfigKey::new`] 构造结构化键，确保调用方无需自行拼接字符串。
#[must_use]
pub fn idle_timeout_key() -> ConfigKey {
    ConfigKey::new(
        "runtime",
        "timeouts.idle_ms",
        ConfigScope::Runtime,
        "idle timeout in milliseconds",
    )
}

/// 将配置值解析为正的毫秒超时时间。
///
/// ### 教案级注释
/// - **意图（Why）**：超时键可以以整数毫秒或 `Duration` 表达，此函数统一解析逻辑并返回核心运行时所需的 [`Duration`]。
/// - **契约（What）**：接受字段枚举与配置值，若类型不匹配则返回 [`TimeoutConfigError::InvalidValueType`]，若数值小于等于零则返回
///   [`TimeoutConfigError::NonPositiveDuration`]；成功时返回严格大于零的 [`Duration`]。
/// - **逻辑（How）**：对整数值执行毫秒转换，对 [`ConfigValue::Duration`] 直接复用其内部表示，其余类型一律报错，并在整数路径检查正数约束。
fn parse_timeout_value(
    field: TimeoutField,
    value: &ConfigValue,
) -> crate::Result<Duration, TimeoutConfigError> {
    match value {
        ConfigValue::Integer(raw, _) => {
            if *raw <= 0 {
                return Err(TimeoutConfigError::NonPositiveDuration {
                    field,
                    provided: *raw,
                });
            }

            Ok(Duration::from_millis(*raw as u64))
        }
        ConfigValue::Duration(duration, _) => {
            if duration.is_zero() {
                return Err(TimeoutConfigError::NonPositiveDuration { field, provided: 0 });
            }

            Ok(*duration)
        }
        _ => Err(TimeoutConfigError::InvalidValueType {
            field,
            expected: "integer milliseconds or duration",
        }),
    }
}
