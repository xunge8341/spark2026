use crate::arc_swap::ArcSwap;
use alloc::{
    borrow::Cow,
    format,
    string::{String, ToString},
    sync::Arc,
};
use core::{
    fmt,
    sync::atomic::{AtomicU64, Ordering},
};

use crate::configuration::{ConfigKey, ConfigScope, ConfigValue, ResolvedConfiguration};
use crate::error::SparkError;
use crate::observability::{
    AttributeSet, MetricsProvider, OwnedAttributeSet, metrics::contract::limits as metrics_contract,
};
use crate::runtime::{
    HotReloadApplyTimer, HotReloadFence, HotReloadObservability, HotReloadReadGuard,
    HotReloadWriteGuard,
};
use crate::status::ready::QueueDepth;

/// 资源种类的统一枚举，用于在限额/配额治理中定位目标资源池。
///
/// # 设计背景（Why）
/// - T20 目标要求对连接、内存、句柄等关键资源施加统一的限额模型，避免各组件各自维护一套配额逻辑。
/// - 通过集中枚举可在配置、观测、测试等场景中使用稳定的标识符，方便跨模块协作与文档生成。
///
/// # 契约说明（What）
/// - `Connections`：网络连接（含出入站）的并发上限，单位为个数。
/// - `MemoryBytes`：运行时可用内存配额，单位为字节。
/// - `FileHandles`：文件或系统句柄数量上限，单位为个数。
/// - `ALL`：常量数组，便于遍历全部资源类型。
///
/// # 设计取舍（Trade-offs）
/// - 当前仅覆盖最核心的三类资源；若未来需要扩展（如线程、GPU 租约），可在保持向后兼容的前提下扩充该枚举。
/// - 为避免引入额外依赖，枚举实现 `Copy + Eq + Hash`，适合做 BTreeMap/BTreeSet 键值。
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum ResourceKind {
    Connections,
    MemoryBytes,
    FileHandles,
}

impl ResourceKind {
    /// 资源枚举的全集，便于在配置解析与报告生成时迭代。
    pub const ALL: [ResourceKind; 3] = [
        ResourceKind::Connections,
        ResourceKind::MemoryBytes,
        ResourceKind::FileHandles,
    ];

    /// 返回资源对应的稳定字符串，用于指标标签与配置键生成。
    pub const fn as_str(self) -> &'static str {
        match self {
            ResourceKind::Connections => "connections",
            ResourceKind::MemoryBytes => "memory_bytes",
            ResourceKind::FileHandles => "file_handles",
        }
    }

    /// 返回资源的默认限额值，用于在未配置时提供保守的安全阈值。
    ///
    /// # 逻辑说明（How）
    /// - 连接：默认 4096，并结合排队策略承载短暂突发。
    /// - 内存：默认 512 MiB，兼顾容器场景与嵌入式部署。
    /// - 句柄：默认 4096，与常见 Linux `nofile` 软限制保持一致。
    pub const fn default_limit(self) -> u64 {
        match self {
            ResourceKind::Connections => 4_096,
            ResourceKind::MemoryBytes => 512 * 1024 * 1024,
            ResourceKind::FileHandles => 4_096,
        }
    }

    /// 返回资源允许配置的硬上限，用于校验配置输入。
    ///
    /// # 背景（Why）
    /// - 避免运营事故中误将限额设为远大于宿主能力的值（如 10^12），导致指标失真或资源透支。
    pub const fn max_limit(self) -> u64 {
        match self {
            ResourceKind::Connections => 65_535,
            ResourceKind::MemoryBytes => 32 * 1024 * 1024 * 1024,
            ResourceKind::FileHandles => 65_535,
        }
    }

    /// 若默认策略为排队，返回建议的队列容量；其他资源返回 `None`。
    pub const fn default_queue_capacity(self) -> Option<u32> {
        match self {
            ResourceKind::Connections => Some(512),
            ResourceKind::MemoryBytes | ResourceKind::FileHandles => None,
        }
    }

    /// 返回资源单位描述，便于在日志与指标中拼接人类可读信息。
    pub const fn unit(self) -> &'static str {
        match self {
            ResourceKind::Connections => "connections",
            ResourceKind::MemoryBytes => "bytes",
            ResourceKind::FileHandles => "handles",
        }
    }
}

impl fmt::Display for ResourceKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// 限额触发时采取的策略。
///
/// # 设计背景（Why）
/// - T20 提出“拒绝 / 排队 / 降级”三类动作，分别应对硬上限、可等待的突发和功能降级场景。
/// - 统一策略枚举，方便将来扩展更多维度（如动态扩缩容、借用配额等）。
///
/// # 契约说明（What）
/// - `Reject`：立即拒绝新请求，调用方应收到错误或背压信号。
/// - `Queue { max_queue_depth }`：进入等待队列，容量超过 `max_queue_depth` 时再拒绝。
/// - `Degrade`：允许请求继续，但要求上层切换到降级路径（如返回缓存、精简响应）。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum LimitAction {
    Reject,
    Queue { max_queue_depth: u32 },
    Degrade,
}

impl LimitAction {
    /// 返回策略的稳定字符串，供指标标签与配置序列化使用。
    pub const fn as_str(self) -> &'static str {
        match self {
            LimitAction::Reject => "reject",
            LimitAction::Queue { .. } => "queue",
            LimitAction::Degrade => "degrade",
        }
    }

    /// 若为队列策略，返回队列容量。
    pub const fn queue_capacity(self) -> Option<u32> {
        match self {
            LimitAction::Queue { max_queue_depth } => Some(max_queue_depth),
            LimitAction::Reject | LimitAction::Degrade => None,
        }
    }
}

impl fmt::Display for LimitAction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LimitAction::Reject => f.write_str("reject"),
            LimitAction::Queue { max_queue_depth } => {
                write!(f, "queue(max_depth={})", max_queue_depth)
            }
            LimitAction::Degrade => f.write_str("degrade"),
        }
    }
}

/// 限额决策结果。
///
/// # 契约说明（What）
/// - `Permit`：当前使用量低于阈值，可直接处理请求。
/// - `Queued { depth, capacity }`：请求进入等待队列，`depth` 表示入队后的深度，`capacity` 为队列容量。
/// - `Rejected`：被硬性拒绝，调用方应返回错误或触发背压。
/// - `Degraded`：允许处理但需执行降级路径。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum LimitDecision {
    Permit,
    Queued { depth: u32, capacity: u32 },
    Rejected,
    Degraded,
}

impl LimitDecision {
    /// 将决策转换为稳定字符串，便于指标标签与日志输出。
    pub const fn as_str(self) -> &'static str {
        match self {
            LimitDecision::Permit => "permit",
            LimitDecision::Queued { .. } => "queued",
            LimitDecision::Rejected => "rejected",
            LimitDecision::Degraded => "degraded",
        }
    }
}

impl fmt::Display for LimitDecision {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LimitDecision::Permit => f.write_str("permit"),
            LimitDecision::Queued { depth, capacity } => {
                write!(f, "queued(depth={}, capacity={})", depth, capacity)
            }
            LimitDecision::Rejected => f.write_str("rejected"),
            LimitDecision::Degraded => f.write_str("degraded"),
        }
    }
}

/// 单个资源的限额计划，包含当前阈值与超限策略。
///
/// # 设计背景（Why）
/// - 将资源、限额、策略组合成不可分割的对象，方便在运行时缓存与复用。
/// - 提供校验逻辑，确保配置变更后仍满足默认上界约束。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LimitPlan {
    resource: ResourceKind,
    limit: u64,
    action: LimitAction,
}

impl LimitPlan {
    /// 构造默认计划，根据资源类型附带推荐策略。
    pub fn default_for(resource: ResourceKind) -> Self {
        let limit = resource.default_limit();
        let action = match resource {
            ResourceKind::Connections => LimitAction::Queue {
                max_queue_depth: resource.default_queue_capacity().unwrap_or(0).max(1),
            },
            ResourceKind::MemoryBytes => LimitAction::Degrade,
            ResourceKind::FileHandles => LimitAction::Reject,
        };
        Self {
            resource,
            limit,
            action,
        }
    }

    /// 返回资源类型。
    pub const fn resource(&self) -> ResourceKind {
        self.resource
    }

    /// 查询当前限额。
    pub const fn limit(&self) -> u64 {
        self.limit
    }

    /// 查询超限策略。
    pub const fn action(&self) -> LimitAction {
        self.action
    }

    /// 若策略为排队，返回队列容量。
    pub const fn queue_capacity(&self) -> Option<u32> {
        self.action.queue_capacity()
    }

    /// 调整限额，校验输入是否在合法范围内。
    pub fn set_limit(&mut self, new_limit: u64) -> crate::Result<(), LimitConfigError> {
        if new_limit == 0 {
            return Err(LimitConfigError::LimitBelowMinimum {
                resource: self.resource,
                provided: new_limit as i64,
            });
        }
        if new_limit > self.resource.max_limit() {
            return Err(LimitConfigError::LimitExceedsMaximum {
                resource: self.resource,
                provided: new_limit,
                maximum: self.resource.max_limit(),
            });
        }
        self.limit = new_limit;
        Ok(())
    }

    /// 覆盖策略，若传入排队策略需保证容量有效。
    pub fn set_action(&mut self, action: LimitAction) -> crate::Result<(), LimitConfigError> {
        if matches!(action.queue_capacity(), Some(0)) {
            return Err(LimitConfigError::QueueCapacityInvalid {
                resource: self.resource,
                provided: 0,
            });
        }
        self.action = action;
        Ok(())
    }

    /// 更新排队容量，仅当当前策略为 `Queue` 时生效。
    pub fn update_queue_capacity(&mut self, capacity: u32) -> crate::Result<(), LimitConfigError> {
        if capacity == 0 {
            return Err(LimitConfigError::QueueCapacityInvalid {
                resource: self.resource,
                provided: capacity,
            });
        }
        match self.action {
            LimitAction::Queue { .. } => {
                self.action = LimitAction::Queue {
                    max_queue_depth: capacity,
                };
                Ok(())
            }
            LimitAction::Reject | LimitAction::Degrade => {
                Err(LimitConfigError::QueueCapacityWithoutQueueStrategy {
                    resource: self.resource,
                })
            }
        }
    }

    /// 根据当前使用量与队列深度评估决策。
    ///
    /// # 参数说明（What）
    /// - `current_usage`：当前占用量。
    /// - `queue_depth`：可选的现有排队深度，若未知传入 `None`。
    pub fn evaluate(&self, current_usage: u64, queue_depth: Option<u32>) -> LimitDecision {
        if current_usage < self.limit {
            return LimitDecision::Permit;
        }

        match self.action {
            LimitAction::Reject => LimitDecision::Rejected,
            LimitAction::Degrade => LimitDecision::Degraded,
            LimitAction::Queue { max_queue_depth } => {
                let depth = queue_depth.unwrap_or(0);
                if depth < max_queue_depth {
                    let next_depth = depth.saturating_add(1).min(max_queue_depth);
                    LimitDecision::Queued {
                        depth: next_depth,
                        capacity: max_queue_depth,
                    }
                } else {
                    LimitDecision::Rejected
                }
            }
        }
    }
}

/// 限额配置集合，覆盖全部受控资源。
///
/// # 设计背景（Why）
/// - 在运行时组件中一次性注入所有资源的限额计划，避免散落的全局变量。
/// - 提供从配置解析的入口，支撑热更新与快照生成。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LimitSettings {
    connections: LimitPlan,
    memory: LimitPlan,
    file_handles: LimitPlan,
}

impl LimitSettings {
    /// 构造默认配置。
    pub fn new() -> Self {
        Self {
            connections: LimitPlan::default_for(ResourceKind::Connections),
            memory: LimitPlan::default_for(ResourceKind::MemoryBytes),
            file_handles: LimitPlan::default_for(ResourceKind::FileHandles),
        }
    }

    /// 返回对应资源的限额计划。
    ///
    /// # 设计意图（Why）
    /// - 上层组件（如连接管理、内存配额器）需要只读视图以决定行为。
    ///
    /// # 契约说明（What）
    /// - **输入**：目标资源种类。
    /// - **输出**：`LimitPlan` 的不可变引用，可安全跨线程读取。
    ///
    /// # 调用提示（How）
    /// - 若需要修改限额，应使用 [`LimitSettings::from_configuration`] 或内部变更流程，避免直接修改返回引用。
    pub fn plan(&self, resource: ResourceKind) -> &LimitPlan {
        match resource {
            ResourceKind::Connections => &self.connections,
            ResourceKind::MemoryBytes => &self.memory,
            ResourceKind::FileHandles => &self.file_handles,
        }
    }

    /// 返回可变引用，用于应用配置覆盖。
    ///
    /// # 使用警示（Why）
    /// - 仅限配置解析与内部单元测试调用，防止外部绕过校验直接修改计划。
    ///
    /// # 契约（What）
    /// - 返回对应资源的可变引用，调用方必须确保后续修改经过 [`LimitPlan`] 自身的校验方法。
    fn plan_mut(&mut self, resource: ResourceKind) -> &mut LimitPlan {
        match resource {
            ResourceKind::Connections => &mut self.connections,
            ResourceKind::MemoryBytes => &mut self.memory,
            ResourceKind::FileHandles => &mut self.file_handles,
        }
    }

    /// 从分层配置解析限额覆盖，未提供的键使用默认值。
    ///
    /// # 解析逻辑（How）
    /// 1. 读取 `limit` 键（整数，单位与资源一致），校验范围后覆盖默认值。
    /// 2. 读取 `action` 键（文本：`reject`/`queue`/`degrade`），若为 `queue` 则结合 `queue_capacity` 键。
    /// 3. 单独出现 `queue_capacity` 时要求当前策略为队列，否则报错提醒配置矛盾。
    pub fn from_configuration(
        config: &ResolvedConfiguration,
    ) -> crate::Result<Self, LimitConfigError> {
        let mut settings = Self::new();
        for resource in ResourceKind::ALL {
            let mut overrides = LimitOverride::default();

            if let Some(value) = config.values.get(&limit_key(resource)) {
                overrides.limit = Some(parse_limit_value(resource, value)?);
            }

            if let Some(value) = config.values.get(&action_key(resource)) {
                overrides.action = Some(parse_action_value(resource, value)?);
            }

            if let Some(value) = config.values.get(&queue_capacity_key(resource)) {
                overrides.queue_capacity = Some(parse_queue_capacity(resource, value)?);
            }

            apply_overrides(settings.plan_mut(resource), overrides)?;
        }

        Ok(settings)
    }
}

impl Default for LimitSettings {
    fn default() -> Self {
        Self::new()
    }
}

/// 运行时持有的限额配置快照，支持原子级热更新。
///
/// ### 设计目的（Why）
/// - 将配置解析结果放入 [`ArcSwap`]，在高并发场景下实现读写无锁化，避免因 `RwLock` 造成的抖动。
/// - 暴露 `config_epoch()` 指标，便于可观测性体系追踪配置更新次数，与指标/日志联动排查。
///
/// ### 契约说明（What）
/// - `new` 接受初始 [`LimitSettings`]，作为热更新前的基线；内部纪元计数从 `0` 开始。
/// - `snapshot` 返回 `Arc<LimitSettings>`，调用方可在无锁前提下长期持有并读取。
/// - `update_from_configuration` 解析新的 [`ResolvedConfiguration`] 并原子替换，若输入非法则返回 [`LimitConfigError`]。
/// - **前置条件**：调用方需确保传入配置已完成语义校验（例如 Profile、Layer 等合规性检查）。
/// - **后置条件**：成功更新后 `config_epoch()` 单调递增，且新快照立即对并发读者可见。
pub struct LimitRuntimeConfig {
    epoch: AtomicU64,
    settings: ArcSwap<LimitSettings>,
    fence: HotReloadFence,
    observability: HotReloadObservability,
}

impl LimitRuntimeConfig {
    /// 构造运行时配置容器，使用独立的热更新栅栏。
    pub fn new(initial: LimitSettings) -> Self {
        Self::with_shared_fence(initial, HotReloadFence::new())
    }

    /// 使用外部提供的栅栏构造运行时配置，便于多组件统一切换。
    pub fn with_shared_fence(initial: LimitSettings, fence: HotReloadFence) -> Self {
        Self {
            epoch: AtomicU64::new(0),
            settings: ArcSwap::new(Arc::new(initial)),
            fence,
            observability: HotReloadObservability::new(),
        }
    }

    /// 构造带有指标上报能力的运行时配置容器。
    pub fn with_observability(
        initial: LimitSettings,
        fence: HotReloadFence,
        metrics: Arc<dyn MetricsProvider>,
        component: impl Into<Cow<'static, str>>,
    ) -> Self {
        let observability = HotReloadObservability::with_component(metrics, component.into());
        // 记录初始纪元（0），帮助监控面确认指标正常上报。
        observability.record(0, None);
        Self {
            epoch: AtomicU64::new(0),
            settings: ArcSwap::new(Arc::new(initial)),
            fence,
            observability,
        }
    }

    /// 返回当前配置所使用的热更新栅栏，供外部批量加锁。
    pub fn fence(&self) -> HotReloadFence {
        self.fence.clone()
    }

    /// 返回当前配置快照，内部自动加读锁以保证读取与写入互斥。
    pub fn snapshot(&self) -> Arc<LimitSettings> {
        let guard = self.fence.read();
        self.snapshot_with_fence(&guard)
    }

    /// 在调用方已持有读锁的情况下返回快照，避免重复加锁。
    pub fn snapshot_with_fence(&self, _guard: &HotReloadReadGuard<'_>) -> Arc<LimitSettings> {
        self.settings.load_full()
    }

    /// 查询配置纪元，便于导出监控或调试。
    pub fn config_epoch(&self) -> u64 {
        self.epoch.load(Ordering::SeqCst)
    }

    /// 将新的配置直接写入，适用于调用方已经解析好的场景。
    pub fn replace(&self, next: LimitSettings) {
        let timer = HotReloadApplyTimer::start();
        let guard = self.fence.write();
        let epoch = self.commit_with_guard(&guard, next);
        self.observability.record(epoch, timer.elapsed());
    }

    /// 在共享写锁的上下文中直接替换配置。
    pub fn replace_with_fence(
        &self,
        guard: &HotReloadWriteGuard<'_>,
        next: LimitSettings,
        timer: HotReloadApplyTimer,
    ) {
        let epoch = self.commit_with_guard(guard, next);
        self.observability.record(epoch, timer.elapsed());
    }

    /// 从 [`ResolvedConfiguration`] 解析并更新限额。
    pub fn update_from_configuration(
        &self,
        config: &ResolvedConfiguration,
    ) -> crate::Result<(), LimitConfigError> {
        let timer = HotReloadApplyTimer::start();
        let parsed = LimitSettings::from_configuration(config)?;
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
    ) -> crate::Result<(), LimitConfigError> {
        let parsed = LimitSettings::from_configuration(config)?;
        let epoch = self.commit_with_guard(guard, parsed);
        self.observability.record(epoch, timer.elapsed());
        Ok(())
    }

    fn commit_with_guard(&self, _guard: &HotReloadWriteGuard<'_>, next: LimitSettings) -> u64 {
        self.settings.store(Arc::new(next));
        self.epoch.fetch_add(1, Ordering::SeqCst) + 1
    }
}

impl Default for LimitRuntimeConfig {
    fn default() -> Self {
        Self::new(LimitSettings::default())
    }
}

/// 限额配置解析可能出现的错误。
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LimitConfigError {
    InvalidValueType {
        resource: ResourceKind,
        field: LimitField,
        expected: &'static str,
    },
    LimitBelowMinimum {
        resource: ResourceKind,
        provided: i64,
    },
    LimitExceedsMaximum {
        resource: ResourceKind,
        provided: u64,
        maximum: u64,
    },
    InvalidAction {
        resource: ResourceKind,
        provided: String,
    },
    QueueCapacityInvalid {
        resource: ResourceKind,
        provided: u32,
    },
    QueueCapacityWithoutQueueStrategy {
        resource: ResourceKind,
    },
}

impl fmt::Display for LimitConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LimitConfigError::InvalidValueType {
                resource,
                field,
                expected,
            } => write!(
                f,
                "invalid {} value for {}: expected {}",
                field, resource, expected
            ),
            LimitConfigError::LimitBelowMinimum { resource, provided } => {
                write!(f, "{} limit must be > 0, got {}", resource, provided)
            }
            LimitConfigError::LimitExceedsMaximum {
                resource,
                provided,
                maximum,
            } => write!(
                f,
                "{} limit {} exceeds maximum {}",
                resource, provided, maximum
            ),
            LimitConfigError::InvalidAction { resource, provided } => {
                write!(f, "invalid action '{}' for {}", provided, resource)
            }
            LimitConfigError::QueueCapacityInvalid { resource, provided } => {
                write!(
                    f,
                    "queue capacity for {} must be > 0, got {}",
                    resource, provided
                )
            }
            LimitConfigError::QueueCapacityWithoutQueueStrategy { resource } => {
                write!(
                    f,
                    "queue capacity configured for {} but strategy is not queue",
                    resource
                )
            }
        }
    }
}

impl crate::Error for LimitConfigError {
    #[allow(unused_parens)]
    fn source(&self) -> Option<&(dyn crate::Error + 'static)> {
        None
    }
}

/// 配置字段枚举，用于在错误信息中指出具体键。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LimitField {
    Limit,
    Action,
    QueueCapacity,
}

impl fmt::Display for LimitField {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LimitField::Limit => f.write_str("limit"),
            LimitField::Action => f.write_str("action"),
            LimitField::QueueCapacity => f.write_str("queue_capacity"),
        }
    }
}

#[derive(Default)]
struct LimitOverride {
    limit: Option<u64>,
    action: Option<ActionKind>,
    queue_capacity: Option<u32>,
}

#[derive(Clone, Copy)]
enum ActionKind {
    Reject,
    Queue,
    Degrade,
}

fn apply_overrides(
    plan: &mut LimitPlan,
    overrides: LimitOverride,
) -> crate::Result<(), LimitConfigError> {
    if let Some(limit) = overrides.limit {
        plan.set_limit(limit)?;
    }

    if let Some(action) = overrides.action {
        let final_action = match action {
            ActionKind::Reject => LimitAction::Reject,
            ActionKind::Degrade => LimitAction::Degrade,
            ActionKind::Queue => {
                let capacity = overrides
                    .queue_capacity
                    .or_else(|| plan.queue_capacity())
                    .or_else(|| plan.resource().default_queue_capacity())
                    .unwrap_or(1);
                if capacity == 0 {
                    return Err(LimitConfigError::QueueCapacityInvalid {
                        resource: plan.resource(),
                        provided: capacity,
                    });
                }
                LimitAction::Queue {
                    max_queue_depth: capacity,
                }
            }
        };
        plan.set_action(final_action)?;
    } else if let Some(capacity) = overrides.queue_capacity {
        if plan.queue_capacity().is_some() {
            plan.update_queue_capacity(capacity)?;
        } else {
            return Err(LimitConfigError::QueueCapacityWithoutQueueStrategy {
                resource: plan.resource(),
            });
        }
    }

    Ok(())
}

/// 内联辅助函数：将配置值解析为正整数。
///
/// # 契约说明
/// - 仅接受 `ConfigValue::Integer` 变体；其它类型视为配置错误。
/// - 返回值必须大于零，否则触发 `LimitBelowMinimum`。
fn parse_limit_value(
    resource: ResourceKind,
    value: &ConfigValue,
) -> crate::Result<u64, LimitConfigError> {
    match value {
        ConfigValue::Integer(v, _) => {
            if *v <= 0 {
                return Err(LimitConfigError::LimitBelowMinimum {
                    resource,
                    provided: *v,
                });
            }
            Ok(*v as u64)
        }
        _ => Err(LimitConfigError::InvalidValueType {
            resource,
            field: LimitField::Limit,
            expected: "integer",
        }),
    }
}

/// 将配置值解析为策略枚举。
///
/// # 契约说明
/// - 接受的文本值：`reject`、`queue`、`degrade`（大小写敏感）。
/// - 其它取值触发 `InvalidAction` 错误。
fn parse_action_value(
    resource: ResourceKind,
    value: &ConfigValue,
) -> crate::Result<ActionKind, LimitConfigError> {
    match value {
        ConfigValue::Text(text, _) => match text.as_ref() {
            "reject" => Ok(ActionKind::Reject),
            "queue" => Ok(ActionKind::Queue),
            "degrade" => Ok(ActionKind::Degrade),
            other => Err(LimitConfigError::InvalidAction {
                resource,
                provided: other.to_string(),
            }),
        },
        _ => Err(LimitConfigError::InvalidValueType {
            resource,
            field: LimitField::Action,
            expected: "string",
        }),
    }
}

/// 解析队列容量配置。
///
/// # 契约说明
/// - 仅允许正整数；0 或负数均视为非法。
fn parse_queue_capacity(
    resource: ResourceKind,
    value: &ConfigValue,
) -> crate::Result<u32, LimitConfigError> {
    match value {
        ConfigValue::Integer(v, _) => {
            if *v <= 0 {
                return Err(LimitConfigError::QueueCapacityInvalid {
                    resource,
                    provided: *v as u32,
                });
            }
            Ok(*v as u32)
        }
        _ => Err(LimitConfigError::InvalidValueType {
            resource,
            field: LimitField::QueueCapacity,
            expected: "integer",
        }),
    }
}

/// 构造资源限额对应的配置键。
fn limit_key(resource: ResourceKind) -> ConfigKey {
    ConfigKey::new(
        "runtime",
        format!("limits.{}.limit", resource.as_str()),
        ConfigScope::Runtime,
        format!("{} resource limit", resource.as_str()),
    )
}

/// 构造资源策略对应的配置键。
fn action_key(resource: ResourceKind) -> ConfigKey {
    ConfigKey::new(
        "runtime",
        format!("limits.{}.action", resource.as_str()),
        ConfigScope::Runtime,
        format!("{} limit action", resource.as_str()),
    )
}

/// 构造队列容量配置键。
fn queue_capacity_key(resource: ResourceKind) -> ConfigKey {
    ConfigKey::new(
        "runtime",
        format!("limits.{}.queue_capacity", resource.as_str()),
        ConfigScope::Runtime,
        format!("{} queue capacity", resource.as_str()),
    )
}

/// 指标挂钩：在资源限额决策时记录使用量、触发次数与降级/丢弃统计。
///
/// # 设计背景（Why）
/// - 验收标准要求“指标能准确反映丢弃/降级比”，因此需要在每次限额评估后自动打点。
/// - 复用 `MetricsProvider` 抽象，保持与 Service/Codec/Transport 指标一致的使用体验。
///
/// # 使用契约（What）
/// - 构造时仅借用 `MetricsProvider`，不产生额外 `Arc`；
/// - `observe` 需传入当前使用量、决策结果及属性集合；
/// - 调用方应至少提供 `limit.resource` 与 `limit.action` 标签，以便聚合统计。
pub struct LimitMetricsHook<'a> {
    provider: &'a dyn MetricsProvider,
}

impl<'a> LimitMetricsHook<'a> {
    /// 构造指标挂钩。
    pub fn new(provider: &'a dyn MetricsProvider) -> Self {
        Self { provider }
    }

    /// 构建包含标准标签的属性集合，便于快速打点。
    pub fn base_attributes(resource: ResourceKind, action: LimitAction) -> OwnedAttributeSet {
        let mut attributes = OwnedAttributeSet::new();
        attributes.push_owned(metrics_contract::ATTR_RESOURCE, resource.as_str());
        attributes.push_owned(metrics_contract::ATTR_ACTION, action.as_str());
        attributes
    }

    /// 记录限额评估的指标数据。
    ///
    /// # 打点明细（How）
    /// - 始终更新 `spark.limits.usage` 与 `spark.limits.limit` Gauge。
    /// - 当 `current_usage >= plan.limit` 时：
    ///   - `spark.limits.hit` +1。
    ///   - 若决策为 `Rejected`，额外记录 `spark.limits.drop`。
    ///   - 若决策为 `Degraded`，额外记录 `spark.limits.degrade`。
    ///   - 若决策为 `Queued`，更新 `spark.limits.queue.depth` Gauge。
    pub fn observe(
        &self,
        plan: &LimitPlan,
        current_usage: u64,
        queue_depth: Option<QueueDepth>,
        decision: LimitDecision,
        attributes: AttributeSet<'_>,
    ) {
        self.provider.record_gauge_set(
            &metrics_contract::RESOURCE_USAGE,
            current_usage as f64,
            attributes,
        );
        self.provider.record_gauge_set(
            &metrics_contract::RESOURCE_LIMIT,
            plan.limit() as f64,
            attributes,
        );

        if current_usage >= plan.limit() {
            self.provider
                .record_counter_add(&metrics_contract::HIT_TOTAL, 1, attributes);

            match decision {
                LimitDecision::Rejected => {
                    self.provider
                        .record_counter_add(&metrics_contract::DROP_TOTAL, 1, attributes);
                }
                LimitDecision::Degraded => {
                    self.provider.record_counter_add(
                        &metrics_contract::DEGRADE_TOTAL,
                        1,
                        attributes,
                    );
                }
                LimitDecision::Queued { .. } => {
                    let depth = queue_depth.map(|d| d.depth).unwrap_or(0) as f64;
                    self.provider.record_gauge_set(
                        &metrics_contract::QUEUE_DEPTH,
                        depth,
                        attributes,
                    );
                }
                LimitDecision::Permit => {}
            }
        }
    }
}

/// 将限额决策转换为 `QueueDepth` 结构，方便复用就绪状态的辅助方法。
///
/// # 契约说明（What）
/// - **输入**：`LimitDecision` 引用。
/// - **输出**：若决策为 `Queued` 则返回带有深度/容量的快照，否则为 `None`。
///
/// # 应用场景（Why/How）
/// - Pipeline 与就绪检查模块使用 `QueueDepth` 统一表达排队上下文，该辅助函数提供零拷贝转换。
pub fn decision_queue_snapshot(decision: &LimitDecision) -> Option<QueueDepth> {
    match decision {
        LimitDecision::Queued { depth, capacity } => Some(QueueDepth {
            depth: *depth as usize,
            capacity: *capacity as usize,
        }),
        _ => None,
    }
}

/// 将限额错误包装为 `SparkError`，便于统一错误处理路径。
///
/// # 设计动机（Why）
/// - 外部调用者通常依赖 `SparkError` 进行错误分级与日志打点，需要在不暴露内部枚举的情况下携带详细说明。
///
/// # 契约说明（What）
/// - **输入**：`LimitConfigError` 实例。
/// - **输出**：带有稳定错误码 `runtime.limit.invalid` 的 [`SparkError`]。
///
/// # 调用方式（How）
/// - 适用于配置加载或动态变更过程中将校验错误上抛给宿主；
/// - 返回的错误消息包含 `LimitConfigError` 的详细描述，可直接用于日志。
pub fn config_error_to_spark(err: LimitConfigError) -> SparkError {
    SparkError::new("runtime.limit.invalid", format!("{}", err))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::observability::metrics::{
        Counter, Gauge, Histogram, InstrumentDescriptor, MetricsProvider,
    };
    use alloc::{borrow::Cow, sync::Arc};
    use core::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

    #[derive(Default)]
    struct TestCounter {
        call_count: AtomicUsize,
        total: AtomicU64,
    }

    impl Counter for TestCounter {
        fn add(&self, value: u64, _attributes: AttributeSet<'_>) {
            self.call_count.fetch_add(1, Ordering::SeqCst);
            self.total.fetch_add(value, Ordering::SeqCst);
        }
    }

    #[derive(Default)]
    struct TestGauge {
        call_count: AtomicUsize,
        last_value_bits: AtomicU64,
    }

    impl Gauge for TestGauge {
        fn set(&self, value: f64, _attributes: AttributeSet<'_>) {
            self.call_count.fetch_add(1, Ordering::SeqCst);
            self.last_value_bits
                .store(value.to_bits(), Ordering::SeqCst);
        }

        fn increment(&self, value: f64, _attributes: AttributeSet<'_>) {
            self.call_count.fetch_add(1, Ordering::SeqCst);
            self.last_value_bits
                .store(value.to_bits(), Ordering::SeqCst);
        }

        fn decrement(&self, value: f64, _attributes: AttributeSet<'_>) {
            self.call_count.fetch_add(1, Ordering::SeqCst);
            self.last_value_bits
                .store((-value).to_bits(), Ordering::SeqCst);
        }
    }

    #[derive(Default)]
    struct TestHistogram;

    impl Histogram for TestHistogram {
        fn record(&self, _value: f64, _attributes: AttributeSet<'_>) {}
    }

    #[derive(Default)]
    struct TestMetricsProvider {
        counter: Arc<TestCounter>,
        gauge: Arc<TestGauge>,
    }

    impl MetricsProvider for TestMetricsProvider {
        fn counter(&self, _descriptor: &InstrumentDescriptor<'_>) -> Arc<dyn Counter> {
            self.counter.clone()
        }

        fn gauge(&self, _descriptor: &InstrumentDescriptor<'_>) -> Arc<dyn Gauge> {
            self.gauge.clone()
        }

        fn histogram(&self, _descriptor: &InstrumentDescriptor<'_>) -> Arc<dyn Histogram> {
            Arc::new(TestHistogram)
        }
    }

    #[test]
    fn default_plan_matches_resource() {
        let plan = LimitPlan::default_for(ResourceKind::Connections);
        assert_eq!(plan.limit(), 4_096);
        assert!(matches!(plan.action(), LimitAction::Queue { .. }));
    }

    #[test]
    fn evaluate_queue_allows_until_capacity() {
        let mut plan = LimitPlan::default_for(ResourceKind::Connections);
        plan.set_limit(10).unwrap();
        plan.set_action(LimitAction::Queue { max_queue_depth: 2 })
            .unwrap();

        assert_eq!(plan.evaluate(9, Some(0)), LimitDecision::Permit);
        assert_eq!(
            plan.evaluate(10, Some(0)),
            LimitDecision::Queued {
                depth: 1,
                capacity: 2,
            }
        );
        assert_eq!(plan.evaluate(10, Some(2)), LimitDecision::Rejected);
    }

    #[test]
    fn metrics_hook_records_drop_and_usage() {
        let provider = TestMetricsProvider::default();
        let hook = LimitMetricsHook::new(&provider);
        let plan = LimitPlan::default_for(ResourceKind::FileHandles);
        let attrs = LimitMetricsHook::base_attributes(plan.resource(), plan.action());
        let decision = LimitDecision::Rejected;
        hook.observe(&plan, plan.limit(), None, decision, attrs.as_slice());

        assert_eq!(provider.counter.call_count.load(Ordering::SeqCst), 2);
        assert_eq!(provider.gauge.call_count.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn settings_parse_overrides() {
        let mut config = ResolvedConfiguration {
            values: Default::default(),
            version: 1,
        };
        config.values.insert(
            limit_key(ResourceKind::Connections),
            ConfigValue::Integer(8_192, Default::default()),
        );
        config.values.insert(
            action_key(ResourceKind::Connections),
            ConfigValue::Text(Cow::Borrowed("reject"), Default::default()),
        );

        let parsed = LimitSettings::from_configuration(&config).unwrap();
        assert_eq!(parsed.plan(ResourceKind::Connections).limit(), 8_192);
        assert!(matches!(
            parsed.plan(ResourceKind::Connections).action(),
            LimitAction::Reject
        ));
    }
}
