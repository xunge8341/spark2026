//! 集群订阅流控与队列观测的统一抽象层。
//!
//! # 模块定位（Why）
//! - 将成员订阅与服务发现共用的流控语义集中管理，避免两个子模块各自维护一套选项导致概念漂移。
//! - 通过结构化的配置体与探针接口，引导调用方在高吞吐事件流中显式规划缓冲区与监控逻辑。
//!
//! # 协议互操作（How）
//! - [`SubscriptionFlowControl`] 以不可变配置的形式传入，实现可在建立订阅时决定缓冲模型与溢出策略。
//! - 若调用方启用队列观测，返回值 [`SubscriptionStream`] 会绑定一个实现 [`SubscriptionQueueProbe`] 的探针，
//!   使用者可定期查询以驱动指标或自适应节流算法。
//!
//! # 适用场景（What）
//! - 事件中心、控制面广播、集群健康监测等需要自定义队列容量或感知丢弃率的异步订阅接口。
//! - 与 [`crate::cluster::membership::ClusterMembership::subscribe`]、
//!   [`crate::cluster::discovery::ServiceDiscovery::watch`] 同步演进，保证 API 契约一致。
//!
//! # 风险与扩展（Trade-offs）
//! - 有界缓冲策略需要实现层配合，否则 `Bounded` 模式可能退化为无界缓存；若无法满足需求，建议通过队列探针在运行期及时捕获异常信号。
//! - 队列探针通常以 `Arc` 包裹的轻量状态结构实现，若观测频率极高，应在实现中加入快照缓存以避免锁竞争。
use crate::{BoxStream, sealed::Sealed};
use alloc::sync::Arc;
use core::{fmt, num::NonZeroUsize};

/// 分布式订阅使用的溢出处理策略。
///
/// # 设计背景（Why）
/// - 结合 Kafka、etcd、NATS 等系统的经验，总结最常用的三类溢出策略：丢弃最旧、丢弃最新、直接失败。
/// - 统一语义可帮助调用方跨实现编写稳定的恢复逻辑（例如感知丢弃并回滚修订号）。
///
/// # 契约说明（What）
/// - `DropOldest`：优先保证最新事件，适合状态收敛速度要求高的场景。
/// - `DropNewest`：优先保证完整的历史，常用于日志/审计场景。
/// - `FailStream`：一旦触发溢出即终止 Stream，并返回 `cluster.queue_overflow` 错误。
///
/// # 风险提示（Trade-offs）
/// - 不同实现可能对溢出检测粒度不同（按事件、按批次），调用方应结合 [`SubscriptionQueueProbe`] 做运行期观测。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum OverflowPolicy {
    DropOldest,
    DropNewest,
    FailStream,
}

/// 流控模式，描述订阅内部使用的缓冲模型。
///
/// # 设计背景（Why）
/// - 归纳分布式事件订阅常见的两类策略：完全依赖调用方控制速率（无界）与在实现侧进行容量限制（有界）。
/// - 通过统一枚举承载容量与溢出策略，可避免实现层自行扩展字段导致的语义割裂。
///
/// # 契约说明（What）
/// - `Unbounded`：实现无需显式限制缓冲深度，但仍可在内部进行流控（如背压下游或阻塞生产者）。
/// - `Bounded { capacity, overflow }`：调用方要求订阅在指定容量内运行，并以 [`OverflowPolicy`] 指定溢出时的处理方式。
/// - **前置条件**：当选择 `Bounded` 时，`capacity` 必须为正的非零整数。
/// - **后置条件**：实现需尽力遵守配置；若无法保证，应尽早通过错误或探针告知调用方。
///
/// # 风险提示（Trade-offs）
/// - `Unbounded` 模式下若消费者持续滞后，可能触发内存压力；建议配合队列探针监控深度。
/// - `Bounded` 模式适合约束内存占用，但当溢出策略为 `DropNewest` 时，调用方需具备重放或快照恢复能力以补齐缺失事件。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum FlowControlMode {
    /// 无界缓冲，由实现自行决定溢出策略（可能退化为阻塞/等待）。
    Unbounded,
    /// 有界缓冲 + 指定溢出策略。
    Bounded {
        capacity: NonZeroUsize,
        overflow: OverflowPolicy,
    },
}

/// 订阅流控配置，调用方可通过构造函数表达期望行为。
///
/// # 设计背景（Why）
/// - 提供统一的配置入口，让上游根据业务 SLA 调整缓冲容量、溢出策略与观测需求，避免在不同实现之间编写特定控制消息。
///
/// # 逻辑解析（How）
/// - 默认使用 [`FlowControlMode::Unbounded`]，可通过 [`Self::bounded`] 指定容量与溢出策略。
/// - 调用 [`Self::with_queue_observation`] 时，内部仅切换布尔标志；真实的队列探针由实现层按需创建，并通过
///   [`SubscriptionStream::with_probe`] 附加在返回值中。
///
/// # 契约说明（What）
/// - `mode`：缓冲容量与溢出策略，默认 `Unbounded`。
/// - `observe_queue`：是否请求实现暴露队列探针；若为 `true`，实现应在返回的 [`SubscriptionStream`] 中填充 `queue_probe`。
/// - **前置条件**：当 `mode` 为 `Bounded` 时，`capacity` 必须大于 0；建议根据事件大小设置合理数值。
/// - **后置条件**：实现接收到该配置后应尽力遵守；若无法满足，应在订阅流的第一条事件或错误中说明。
///
/// # 风险提示（Trade-offs）
/// - 队列探针通常以原子计数或轻量互斥实现，若观测频率极高，应在实现侧增加节流或缓存快照以避免性能退化。
/// - 当选择 [`OverflowPolicy::FailStream`] 时，调用方需要准备重连/重放流程，以免在溢出后失去事件连续性。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SubscriptionFlowControl {
    pub mode: FlowControlMode,
    pub observe_queue: bool,
}

impl SubscriptionFlowControl {
    /// 构造一个默认的无界配置。
    pub const fn unbounded() -> Self {
        Self {
            mode: FlowControlMode::Unbounded,
            observe_queue: false,
        }
    }

    /// 构造一个带容量限制的配置。
    pub const fn bounded(capacity: NonZeroUsize, overflow: OverflowPolicy) -> Self {
        Self {
            mode: FlowControlMode::Bounded { capacity, overflow },
            observe_queue: false,
        }
    }

    /// 请求实现提供队列观测能力。
    pub const fn with_queue_observation(mut self, enable: bool) -> Self {
        self.observe_queue = enable;
        self
    }
}

impl Default for SubscriptionFlowControl {
    fn default() -> Self {
        Self::unbounded()
    }
}

/// 订阅流的返回体，承载事件流与可选的队列探针。
///
/// # 设计背景（Why）
/// - 将事件流与队列探针打包返回，避免额外的“探针查询”接口，简化调用方的生命周期管理。
/// - 便于未来为订阅附加更多元数据（如重放令牌、延迟统计），而不破坏既有的 Stream 契约。
///
/// # 逻辑解析（How）
/// - 所有实现至少需要提供 `stream` 字段，采用 [`crate::BoxStream`] 维持对象安全。
/// - 若实现支持队列观测，应通过 [`Self::with_probe`] 将 `Arc<dyn SubscriptionQueueProbe>` 装入结构体。
/// - 调用方在消费事件时，可并行查询 `queue_probe` 获取 [`SubscriptionQueueSnapshot`] 并据此调整处理速率。
///
/// # 契约说明（What）
/// - `stream`：核心事件流，语义与旧版 `BoxStream` 返回值一致。
/// - `queue_probe`：若调用方通过配置请求观测能力，且实现支持，则提供该探针；调用方可周期性调用 `snapshot` 读取深度和丢弃次数。
/// - **前置条件**：实现需保证 `queue_probe` 与 `stream` 生命周期一致；当流结束时，应视为探针数据不再更新。
/// - **后置条件**：调用方可在消费流的同时使用探针对接指标系统，实现实时背压调优。
///
/// # 使用示例（Example）
/// ```ignore
/// let flow = SubscriptionFlowControl::bounded(NonZeroUsize::new(512).unwrap(), OverflowPolicy::DropOldest)
///     .with_queue_observation(true);
/// let SubscriptionStream { stream, queue_probe } = cluster.subscribe(scope, None, flow);
/// // `stream` 用于消费增量事件；`queue_probe`（若存在）可用于暴露 Prometheus Gauge。
/// ```
pub struct SubscriptionStream<T> {
    pub stream: BoxStream<'static, T>,
    pub queue_probe: Option<Arc<dyn SubscriptionQueueProbe>>,
}

impl<T> SubscriptionStream<T> {
    /// 仅使用事件流构建订阅返回体。
    ///
    /// # 设计背景（Why）
    /// - 为实现提供零成本的默认构造器，方便在不支持队列探针时快速返回结构体。
    ///
    /// # 契约说明（What）
    /// - **输入**：`stream` 为对象安全的事件流，必须满足订阅接口定义的顺序与修订号约束。
    /// - **后置条件**：返回值中的 `queue_probe` 默认为 `None`，表示未启用队列观测。
    pub fn new(stream: BoxStream<'static, T>) -> Self {
        Self {
            stream,
            queue_probe: None,
        }
    }

    /// 为订阅附加队列探针。
    ///
    /// # 逻辑解析（How）
    /// - 将传入的 `Arc<dyn SubscriptionQueueProbe>` 存储到结构体中，调用方即可在后续生命周期查询快照。
    ///
    /// # 契约说明（What）
    /// - **输入**：`probe` 需实现 [`SubscriptionQueueProbe`]，并保证线程安全。
    /// - **后置条件**：返回值中的 `queue_probe` 为 `Some(probe)`，方便链式调用。
    pub fn with_probe(mut self, probe: Arc<dyn SubscriptionQueueProbe>) -> Self {
        self.queue_probe = Some(probe);
        self
    }
}

impl<T> fmt::Debug for SubscriptionStream<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SubscriptionStream")
            .field("has_probe", &self.queue_probe.is_some())
            .finish()
    }
}

/// 订阅队列的观测快照。
///
/// # 契约说明（What）
/// - `capacity`：若存在容量上限则返回 `Some`；无界时为 `None`。
/// - `depth`：当前队列中待消费事件数量。
/// - `dropped_events`：自订阅建立以来的累计丢弃事件数。
/// - **后置条件**：实现应保证同一订阅的快照 `depth` 是单调变化的真实值或近似上界（允许存在读取延迟）。
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SubscriptionQueueSnapshot {
    pub capacity: Option<NonZeroUsize>,
    pub depth: usize,
    pub dropped_events: u64,
}

/// 订阅队列探针，用于查询当前背压状态。
///
/// # 契约说明（What）
/// - `snapshot`：应返回最新已知状态，调用成本应足够低以支持高频查询。
/// - **前置条件**：实现需保证该方法线程安全；若查询代价高昂，可在实现内部做缓存。
/// - **后置条件**：若订阅已终止，允许继续返回最后一次快照，也可选择在内部标记并返回零值。
pub trait SubscriptionQueueProbe: Send + Sync + 'static + Sealed {
    fn snapshot(&self) -> SubscriptionQueueSnapshot;
}
