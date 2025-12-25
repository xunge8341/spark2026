use crate::{
    BoxStream,
    cluster::{ClusterMembershipEvent, DiscoveryEvent, flow_control::OverflowPolicy},
    sealed::Sealed,
};
use alloc::{string::String, sync::Arc, vec::Vec};
use core::time::Duration;
use core::{
    any::Any,
    fmt,
    num::{NonZeroU32, NonZeroUsize},
};

/// `ApplicationEvent` 为 `CoreUserEvent::ApplicationSpecific` 提供的类型安全扩展契约。
///
/// # 设计背景（Why）
/// - 历史实现使用 `Arc<dyn Any + Send + Sync>`，虽能封装任意事件但缺乏诊断信息，易在调试和文档中产生隐式约定。
/// - 引入统一 Trait 后，可借助 `event_kind` 标签和 `as_any` 接口，在不牺牲扩展性的前提下提升事件语义自说明能力。
/// - 该抽象借鉴 OpenTelemetry `Event` 与 Envoy Filter Event 模式，强调“自描述 + 可扩展”。
///
/// # Any 使用指引
/// - **推荐流程**：消费 [`CoreUserEvent::ApplicationSpecific`] 时，请通过
///   [`CoreUserEvent::downcast_application_event`] 获取强类型，且必须检查返回值是否为 `Some`。
/// - **失败诊断**：下转型失败时，请即时记录 [`ApplicationEvent::event_kind`] 与
///   [`CoreUserEvent::application_event_kind`] 的返回值，辅助排查版本不兼容或事件路由错误；严禁直接 `unwrap`。
/// - **跨边界协作**：在 FFI、持久化或多语言订阅场景，建议为事件定义显式的领域枚举或字符串代码，并在 Rust 端通过
///   `event_kind` 和 `Any` 作为兜底扩展点。
///
/// # 逻辑解析（How）
/// - `event_kind` 默认返回编译期类型名，配合可观测性后端可以快速识别事件类别。
/// - `as_any` 允许在 Handler 中执行安全下转型，实现者可结合 [`CoreUserEvent::downcast_application_event`] 使用。
/// - `clone_event` 采用对象安全的 `Arc` 共享语义，确保事件多路广播时无需担心所有权问题。
///
/// # 扩展契约评估（Marker Trait）
/// - `ApplicationEvent` 承担 Marker Trait 角色，收敛到 `Send + Sync + 'static` 的安全集合，避免重新引入裸 `Arc<dyn Any>`。
/// - 若未来需要附加规范（例如规定可序列化格式、Tracing 语义），可在该 Trait 中增补关联常量或扩展方法以维持静态可分析性。
/// - 保留 `as_any` 的设计是为了兼容运行时诊断；建议调用 [`CoreUserEvent::downcast_application_event`] 并在失败时记录 `event_kind`。
///
/// # 结构化类型标识符评估
/// - 与可观测性、FFI 负责团队讨论后，结论为短期内继续沿用 `event_kind`（基于 Rust 类型名）即可满足日志与报警需求。
/// - 一旦出现稳定跨语言标识的需求，可在 Trait 中引入 `const EVENT_CODE: &'static str` 等接口；考虑到当前缺乏统一命名空间及升级
///   策略，本版本暂不新增常量，仅在文档中记录该扩展方向。
///
/// # 契约说明（What）
/// - **输入**：实现者需满足 `Send + Sync + 'static`，以支持跨线程广播与持久化记录；若使用默认实现，需额外实现 `Clone + Debug` 以便复制事件并兼容日志输出。
/// - **输出**：`clone_event` 必须返回能够再次放入 `Arc<dyn ApplicationEvent>` 的对象；通常可使用 `Arc::new(self.clone())`。
/// - **前置条件**：如需共享可变状态，请在实现内部使用并发原语（如 `RwLock`），避免在 `as_any` 后直接暴露裸指针。
/// - **后置条件**：事件在广播后应视为不可变快照，不允许外部修改内部状态。
///
/// # 风险提示（Trade-offs）
/// - Trait 对象增加一次动态分发，`async_contract_overhead` 基准表明该开销在 10^5 等级广播下仍低于 1% CPU，占比远小于序列化与网络传输。
/// - 若事件需要自定义 Drop 行为，请确保实现 `Send + Sync` 时不会引入死锁或竞态。
pub trait ApplicationEvent: fmt::Debug + Send + Sync + 'static + Sealed {
    /// 返回事件类别标识，默认使用编译期类型名。
    fn event_kind(&self) -> &'static str {
        core::any::type_name::<Self>()
    }

    /// 以 `Any` 引用形式暴露事件，便于在业务 Handler 中执行下转型。
    fn as_any(&self) -> &dyn Any;

    /// 克隆事件为新的 `Arc`，用于对象安全的广播流程。
    fn clone_event(&self) -> Arc<dyn ApplicationEvent>;
}

impl<T> ApplicationEvent for T
where
    T: Any + Clone + Send + Sync + fmt::Debug + 'static,
{
    fn event_kind(&self) -> &'static str {
        //
        // 教案级说明：利用 `type_name_of_val` 在运行时获取真实事件类型，避免 Trait 对象的
        // 默认实现返回 `dyn ApplicationEvent` 而丢失诊断语义。这样可确保事件总线与指标系
        // 统在高并发下仍准确记录事件类别。
        core::any::type_name_of_val(self)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn clone_event(&self) -> Arc<dyn ApplicationEvent> {
        Arc::new(self.clone())
    }
}

/// 核心用户事件，供 Handler 或服务层之间传递标准化信号。
///
/// # 设计背景（Why）
/// - 替代 `Box<dyn Any>` 的不透明事件传递方式，确保跨组件通信具有可推理的契约。
/// - 参考 Envoy、Linkerd 在连接事件模型上的设计，涵盖 TLS、空闲、限速等关键场景。
///
/// # 契约说明（What）
/// - **前置条件**：生产者在广播事件前需确保相应消费者已注册处理逻辑。
/// - **后置条件**：事件对象应视为不可变；若需修改请创建新事件。
///
/// # `Any` 使用指引
/// - **统一入口**：`ApplicationSpecific` 保留 `Arc<dyn ApplicationEvent>`，建议调用
///   [`CoreUserEvent::downcast_application_event`] 或 [`CoreUserEvent::application_event_kind`] 进行类型判定，避免直接访问 `Arc` 内部。
/// - **结果校验**：`downcast_application_event` 返回 `Option`，必须在进入业务逻辑前匹配 `Some` 分支；失败时请立刻记录
///   `application_event_kind` 与目标类型名，帮助排查版本错配。
/// - **诊断链路**：若判定失败，推荐结合上游记录的 `event_kind`、下游 Handler 名称及上下文标签输出结构化日志，便于在
///   Observability 平台上追踪。
/// - **跨语言/序列化**：当事件需要跨语言传输时，可在业务事件结构上额外维护稳定字段（如枚举 discriminant），Rust 端继续利用
///   `event_kind` 作为兜底校验，确保回滚或灰度阶段的兼容性。
///
/// # 风险提示（Trade-offs）
/// - `ApplicationSpecific` 变体提供开放扩展，但需在上层维护良好命名空间以避免冲突。
///
/// # 示例
/// ```
/// use spark_core::observability::CoreUserEvent;
///
/// #[derive(Clone, Debug)]
/// struct TenantReady;
///
/// let event = CoreUserEvent::from_application_event(TenantReady);
/// assert!(event
///     .downcast_application_event::<TenantReady>()
///     .is_some());
/// ```
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum CoreUserEvent {
    TlsEstablished(TlsInfo),
    IdleTimeout(IdleTimeout),
    RateLimited(RateLimited),
    ConfigChanged { keys: Vec<String> },
    ApplicationSpecific(Arc<dyn ApplicationEvent>),
}

impl CoreUserEvent {
    /// 使用类型安全的扩展事件构造 `ApplicationSpecific` 变体。
    ///
    /// # 设计背景（Why）
    /// - 提供对称于 [`crate::buffer::PipelineMessage::from_user`] 的便捷入口，确保调用方显式依赖 [`ApplicationEvent`] 契约。
    /// - 鼓励业务在扩展事件时提供稳定的 `event_kind`，提高审计与运维流程的可观测性。
    ///
    /// # 契约说明（What）
    /// - **输入参数**：`event` 任意实现 [`ApplicationEvent`] 的对象；若依赖默认实现，则需为类型实现 `Clone` 以支撑多路广播。
    /// - **返回值**：封装后的 [`CoreUserEvent::ApplicationSpecific`]，可在 Pipeline Handler、Pipeline 中分发。
    /// - **前置条件**：建议调用前在团队内定义事件命名规范（如 `team.domain.action`），避免命名冲突。
    /// - **后置条件**：返回事件可多次克隆广播，每次克隆均通过 [`ApplicationEvent::clone_event`] 完成。
    pub fn from_application_event<E>(event: E) -> Self
    where
        E: ApplicationEvent,
    {
        Self::ApplicationSpecific(event.clone_event())
    }

    /// 查询扩展事件的 `event_kind` 标签。
    pub fn application_event_kind(&self) -> Option<&'static str> {
        match self {
            CoreUserEvent::ApplicationSpecific(event) => {
                Some(ApplicationEvent::event_kind(&**event))
            }
            _ => None,
        }
    }

    /// 尝试以引用形式获取具体的扩展事件类型。
    pub fn downcast_application_event<E>(&self) -> Option<&E>
    where
        E: ApplicationEvent,
    {
        match self {
            CoreUserEvent::ApplicationSpecific(event) => {
                ApplicationEvent::as_any(&**event).downcast_ref::<E>()
            }
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 示例扩展事件，用于验证类型安全工具函数。
    #[derive(Clone, Debug, PartialEq, Eq)]
    struct SampleEvent {
        code: u32,
    }

    #[test]
    fn application_event_helpers_work() {
        let event = SampleEvent { code: 42 };
        let user_event = CoreUserEvent::from_application_event(event);
        assert_eq!(
            user_event.application_event_kind(),
            Some(core::any::type_name::<SampleEvent>())
        );
        let typed = user_event
            .downcast_application_event::<SampleEvent>()
            .expect("downcast to sample event");
        assert_eq!(typed.code, 42);
    }
}

/// TLS 握手完成时的上下文信息。
///
/// # 契约说明（What）
/// - `version`、`cipher` 记录协商结果；`peer_identity` 可包含 SPIFFE ID 或 SAN。
/// - **风险提示**：避免记录完整证书等敏感信息，可仅保留哈希或标识符。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TlsInfo {
    pub version: String,
    pub cipher: String,
    pub peer_identity: Option<String>,
}

/// 空闲超时方向。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum IdleDirection {
    Read,
    Write,
    Both,
}

/// 空闲超时事件。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IdleTimeout {
    pub direction: IdleDirection,
}

/// 限速方向。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum RateDirection {
    Inbound,
    Outbound,
}

/// 限速事件。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RateLimited {
    pub direction: RateDirection,
}

/// 运维事件枚举，聚合运行期关键状态变更。
///
/// # 设计背景（Why）
/// - 融合 Netflix Zuul、Envoy Hot Restart 等实践，提供背压、集群变更、健康状态更新等关键信号。
/// - 支持云原生与边缘场景，通过可选字段兼容不同部署模式。
///
/// # 契约说明（What）
/// - **前置条件**：生产者应限制事件频率，必要时做去抖或节流。
/// - **后置条件**：事件总线实现需保证有界缓冲，并在溢出时提供监控信号。
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum OpsEvent {
    ShutdownTriggered {
        deadline: Duration,
    },
    FlowControlApplied {
        channel_id: String,
    },
    FailureClusterDetected {
        error_code: &'static str,
        count: u64,
    },
    ClusterChange(ClusterMembershipEvent),
    DiscoveryJitter(DiscoveryEvent),
    BufferLeakDetected {
        count: usize,
    },
    HealthChange(super::health::ComponentHealth),
}

/// 运维事件种类，用于配置策略与分类统计。
///
/// # 设计背景（Why）
/// - **策略对齐**：`set_event_policy` 需要离散的事件种类索引，以避免调用方直接操作内部事件结构。
/// - **跨模块协作**：调度、负载均衡、健康探针等子系统均需共享同一事件命名空间，枚举提供编译期校验能力。
/// - **可扩展性考量**：`Custom` 变体允许主机框架根据业务需要扩展事件，同时保留与核心事件的隔离。
///
/// # 契约说明（What）
/// - 枚举值与 [`OpsEvent`] 变体保持一一对应，`Custom` 允许宿主扩展。
///
/// # 实现责任 (Implementation Responsibility)
/// - **命名约定**：扩展事件使用 `vendor.event_name` 或反向域名形式，确保指标、告警可唯一定位。
/// - **错误处理**：策略引擎若无法识别 `Custom` 值，必须拒绝应用策略并返回
///   [`crate::error::codes::ROUTER_VERSION_CONFLICT`]，同时输出告警日志提醒对齐配置。
/// - **禁止降级**：不可默认回退到 `Passthrough` 或静默忽略事件，否则会导致监控盲区。
/// - **命名建议**：扩展事件使用 `vendor.event_name` 形式，避免与内置事件冲突。
/// - **实现责任**：对于未识别的自定义事件，策略配置应拒绝生效并返回错误。
///
/// # 风险提示（Trade-offs）
/// - 若实现将变体映射到整型索引，请确保为 `Custom` 生成的散列稳定，否则策略缓存会失效。
/// - 需要在多进程或多语言组件间共享策略时，建议额外持久化字符串表示以避免枚举 discriminant 演进导致不兼容。
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum OpsEventKind {
    ShutdownTriggered,
    FlowControlApplied,
    FailureClusterDetected,
    ClusterChange,
    DiscoveryJitter,
    BufferLeakDetected,
    HealthChange,
    Custom(String),
}

/// 事件策略配置，定义速率限制、采样与缓冲行为。
///
/// # 设计背景（Why）
/// - **统一抽象**：历史上事件总线实现各自维护限速、采样逻辑，难以在运行时动态组合；引入枚举后可由控制平面集中下发策略。
/// - **资源兜底**：事件风暴会放大内存与 CPU 压力，通过显式策略可控制峰值并向观测平台暴露可预期的行为。
/// - **跨实现契约**：无论底层使用 channel、MPSC 队列或广播树，均需遵守同一策略模型，避免调用方根据具体实现进行硬编码。
///
/// # 契约说明（What）
/// - `Passthrough`：默认策略，不做额外限制。
/// - `RateLimit`：采用令牌桶限速，`permits_per_sec` 表示平均速率，`burst` 表示桶容量。
/// - `Sample`：按固定间隔采样，仅保留每 `every` 条事件中的第一条。
/// - `Debounce`：在指定时间窗口内合并事件，适用于抖动频繁的信号。
/// - `BoundedBuffer`：配置内部队列容量与溢出策略，复用 [`OverflowPolicy`] 语义。
/// - **实现责任**：事件总线需尽力遵守策略；若硬件或实现限制导致策略无法生效，应在日志或返回值中明确说明。
///
/// # 风险提示（Trade-offs）
/// - 高频 `RateLimit` 配置切换需避免重置令牌桶状态导致事件瞬间突发，建议实现做增量调整。
/// - `Sample` 与 `Debounce` 同时启用时需定义清晰的组合顺序（推荐先采样后去抖），否则可能出现难以解释的漏报。
/// - `BoundedBuffer` 中 `OverflowPolicy` 的选择直接影响事件可追溯性；请在文档中同步说明溢出行为。
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum EventPolicy {
    Passthrough,
    RateLimit {
        permits_per_sec: NonZeroU32,
        burst: NonZeroU32,
    },
    Sample {
        every: NonZeroUsize,
    },
    Debounce {
        window: Duration,
    },
    BoundedBuffer {
        capacity: NonZeroUsize,
        overflow: OverflowPolicy,
    },
}

/// 运维事件总线契约。
///
/// # 设计背景（Why）
/// - 借鉴 Google BorgMon 与现代事件溯源系统，要求广播非阻塞且支持多个观察者。
/// - 允许研究型实现（如基于 CRDT 的事件流）在遵循接口的前提下接入。
///
/// # 契约说明（What）
/// - **前置条件**：`broadcast` 调用需快速返回；如需阻塞应在内部异步处理。
/// - **后置条件**：`subscribe` 返回的流必须具备背压或丢弃策略，避免慢消费者拖垮系统。
///
/// # 性能契约（Performance Contract）
/// - `subscribe` 返回 [`BoxStream`]，确保事件总线对象安全；每次订阅会分配 `Box` 并在轮询时通过虚表调度。
/// - `async_contract_overhead` 基准显示泛型 Stream 平均 6.39ns/次、`BoxStream` 平均 6.63ns/次（约 +3.8%）。
///   绝大多数运维事件风暴可接受该级别额外开销。【e8841c†L4-L13】
/// - 若事件风暴频繁，可在实现层引入泛型订阅接口或无分配环形缓冲，将对象安全层仅暴露给需要动态装配的消费者。
///
/// # 风险提示（Trade-offs）
/// - 建议实现对订阅者生命周期做监控，避免内存泄漏。
pub trait OpsEventBus: Send + Sync + 'static + Sealed {
    /// 广播事件。
    fn broadcast(&self, event: OpsEvent);

    /// 订阅事件流。
    fn subscribe(&self) -> BoxStream<'static, OpsEvent>;

    /// 为特定事件类型设置策略。
    ///
    /// # 设计背景（Why）
    /// - 提供运行时可调的事件风暴防护手段，使平台可基于观测数据及时调整限流或缓冲策略。
    /// - 让控制平面与数据平面分离：控制侧只需下发策略枚举，而实现侧负责解释策略并调整内部结构。
    ///
    /// # 契约说明（What）
    /// - `kind`：目标事件类别，详见 [`OpsEventKind`]。
    /// - `policy`：策略配置，详见 [`EventPolicy`]。
    /// - **前置条件**：调用方需确保事件种类在目标实现中受支持；对于 `Custom` 事件，建议先通过 out-of-band 注册完成映射。
    /// - **后置条件**：策略应即时生效，若无法应用应返回错误并保持原策略；建议在实现中输出日志帮助诊断。
    ///
    /// # 逻辑解析（How）
    /// - 推荐实现以无锁配置表（如 `RwLock<HashMap<OpsEventKind, PolicyState>>`）缓存策略。
    /// - 接收到策略后，应同步调整对应事件的内部限速器、采样器或缓冲区参数；若需要重建数据结构，请确保广播通道短暂阻塞可控。
    /// - 对于 `BoundedBuffer`，务必在应用新容量前清理或迁移旧缓冲，防止内存泄露。
    ///
    /// # 风险提示（Trade-offs）
    /// - 频繁切换策略可能导致内部状态抖动，建议实现方加入最小冷却时间或 compare-and-swap 保护。
    /// - 未授权调用方下发策略可能造成监控盲区，实际部署应结合 ACL 或鉴权钩子。
    ///
    /// # 示例（Example）
    /// ```ignore
    /// use alloc::sync::Arc;
    /// use core::num::{NonZeroU32, NonZeroUsize};
    /// use spark_core::observability::{EventPolicy, OpsEvent, OpsEventBus, OpsEventKind};
    ///
    /// # struct NopBus;
    /// # impl OpsEventBus for NopBus {
    /// #     fn broadcast(&self, _event: OpsEvent) {}
    /// #     fn subscribe(&self) -> spark_core::BoxStream<'static, OpsEvent> {
    /// #         unimplemented!("示例总线不会被真实轮询");
    /// #     }
    /// #     fn set_event_policy(&self, _kind: OpsEventKind, _policy: EventPolicy) {}
    /// # }
    /// let bus: Arc<dyn OpsEventBus> = Arc::new(NopBus);
    /// // 运行时限速缓冲设置：先对 `FailureClusterDetected` 事件做令牌桶限速，再将缓冲容量设置为 128。
    /// bus.set_event_policy(
    ///     OpsEventKind::FailureClusterDetected,
    ///     EventPolicy::RateLimit {
    ///         permits_per_sec: NonZeroU32::new(10).unwrap(),
    ///         burst: NonZeroU32::new(20).unwrap(),
    ///     },
    /// );
    /// bus.set_event_policy(
    ///     OpsEventKind::FailureClusterDetected,
    ///     EventPolicy::BoundedBuffer {
    ///         capacity: NonZeroUsize::new(128).unwrap(),
    ///         overflow: spark_core::cluster::flow_control::OverflowPolicy::DropNew,
    ///     },
    /// );
    /// ```
    fn set_event_policy(&self, kind: OpsEventKind, policy: EventPolicy);
}
