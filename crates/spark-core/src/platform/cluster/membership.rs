use crate::{
    async_trait,
    cluster::{
        flow_control::{SubscriptionFlowControl, SubscriptionStream},
        topology::{ClusterConsistencyLevel, ClusterEpoch, ClusterRevision, RoleDescriptor},
    },
    error::CoreError,
    sealed::Sealed,
    transport::Endpoint,
};
use alloc::{boxed::Box, collections::BTreeMap, string::String, vec::Vec};

/// 集群领域统一使用的错误类型别名。
///
/// # 设计背景（Why）
/// - 对外暴露的 Trait 需要稳定错误域，方便上层做指标与重试策略决策。
/// - 直接复用框架级的 [`CoreError`]，避免重复定义错误枚举，并兼容链路追踪元数据。
///
/// # 契约说明（What）
/// - 所有实现必须返回该错误类型，且错误码需遵循 `cluster.*` 命名空间；若需额外的实现层上下文，可先转换为 `DomainError` 再调用
///   [`IntoCoreError`](crate::IntoCoreError::into_core_error)。
/// - 错误实例可以附带节点 ID、对端地址等上下文，帮助运行时定位异常。
///
/// # 风险提示（Trade-offs）
/// - 若实现者希望使用更轻量的错误类型，可在内部转换后再暴露为 `ClusterError`，但需承担额外的分配成本。
pub type ClusterError = CoreError;

/// 节点唯一标识。
///
/// # 设计背景（Why）
/// - 参考 Kubernetes、Consul 等平台的通用实践，节点 ID 通常以字符串形式表示（UUID、主机名、Provider ID）。
/// - 以 `String` 表示，兼容多平台字符集，并支持运行时动态生成。
///
/// # 契约说明（What）
/// - ID 必须全局唯一且稳定；建议遵循 `provider://region/cluster/node` 等具备层级语义的格式。
/// - **前置条件**：调用方需保证输入字符串可用于日志与可观测性系统，无需额外编码。
/// - **后置条件**：框架不会修改 ID，直接用于事件与快照。
///
/// # 风险提示（Trade-offs）
/// - 未对 ID 格式做硬性约束；若需校验可在实现层引入正则或 Schema 校验器。
pub type NodeId = String;

/// 节点健康状态枚举。
///
/// # 设计背景（Why）
/// - 结合五大主流系统（Kubernetes、Consul、Zookeeper、Eureka、Nomad）的状态语义，归纳为四个最常用的运行态。
/// - 增加 `Degraded` 以承载学术界提出的部分退化（partial failure）判定，可与基于心跳的概率健康模型对接。
///
/// # 契约说明（What）
/// - `Active`：节点可完全提供服务能力。
/// - `Degraded`：节点仍在线，但性能或功能受限，建议路由层降权。
/// - `Unreachable`：节点不可达，应立即停止流量。
/// - `Retiring`：节点正在退出，拒绝新流量但允许完成存量请求。
/// - **前置条件**：实现者需确保状态转换满足单调性，即 `Retiring`、`Unreachable` 不应直接回退到 `Active`，除非配套补偿逻辑。
/// - **后置条件**：任何 `ClusterMembershipEvent` 中出现的状态值必须同步更新快照。
///
/// # 风险提示（Trade-offs）
/// - 状态枚举不含 `Unknown` 分支，以鼓励实现尽快给出明确判定；若无法判断，可在事件中附带原因并暂存为 `Degraded`。
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum ClusterNodeState {
    Active,
    Degraded,
    Unreachable,
    Retiring,
}

/// 节点全量画像。
///
/// # 设计背景（Why）
/// - 生产系统常需同时存储通信地址、角色信息、权重元数据，以支撑路由与调度。
/// - 引入 `ClusterRevision`，确保每次更新都能追踪版本，借鉴 CRDT 与向量时钟理念降低脑裂风险。
///
/// # 契约说明（What）
/// - `node_id`：节点唯一标识。
/// - `endpoint`：与节点通信的主地址，通常为 RPC/QUIC Endpoint。
/// - `roles`：节点承担的角色列表，使用 [`RoleDescriptor`] 描述行业共识角色名。
/// - `metadata`：扩展键值对，如区域、权重、机型等，建议遵循 `snake_case` 键名。
/// - `state`：当前健康状态。
/// - `revision`：最近一次更新的修订信息。
/// - **前置条件**：构造实例时需保证 `roles` 中无重复项，可由实现者自行去重。
/// - **后置条件**：成功广播的事件与快照必须带有最新修订号，以便调用方实现幂等更新。
///
/// # 风险提示（Trade-offs）
/// - `metadata` 采用 [`BTreeMap`]，在读多写少的场景下提供稳定排序，用于 diff 与审计；插入复杂度为 `O(log n)`，若更关注写性能，可在
///   实现中改用 `HashMap` 缓存并在导出前转换，或对热点字段引入并行缓存以降低重排成本。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClusterNodeProfile {
    pub node_id: NodeId,
    pub endpoint: Endpoint,
    pub roles: Vec<RoleDescriptor>,
    pub metadata: BTreeMap<String, String>,
    pub state: ClusterNodeState,
    pub revision: ClusterRevision,
}

/// 快照级别的成员集合。
///
/// # 设计背景（Why）
/// - 业界成熟产品均提供“全量快照 + 增量事件”组合，快照用于恢复一致性，事件负责快速感知变化。
/// - 学术界的分布式一致性研究建议附带 `epoch` 与版本号，以在多活与跨区域场景下辨别主从关系。
///
/// # 契约说明（What）
/// - `epoch`：当前集群世代，与控制面的选举周期绑定。
/// - `members`：节点画像列表，应按 `node_id` 排序以方便差异化对比。
/// - `generated_at_revision`：生成快照时的全局修订号。
/// - **前置条件**：实现方需确保快照内所有成员共享同一 `epoch`。
/// - **后置条件**：当事件流丢失时，调用方可使用快照重新同步并根据修订号进行去重。
///
/// # 风险提示（Trade-offs）
/// - 快照可能较大，建议实现方提供压缩或分页能力，或与上层协商快照刷新频率。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClusterMembershipSnapshot {
    pub epoch: ClusterEpoch,
    pub members: Vec<ClusterNodeProfile>,
    pub generated_at_revision: ClusterRevision,
}

/// 订阅范围选择器。
///
/// # 设计背景（Why）
/// - 借鉴 Kubernetes `LabelSelector` 与 Consul `Service` 过滤能力，允许调用方按角色或拓扑分片订阅事件。
/// - 额外提供 `Custom` 分支支持学术实验中常见的标签表达式或 DSL。
///
/// # 契约说明（What）
/// - `EntireCluster`：订阅全量节点。
/// - `ByRole`：限定角色（如 `RoleDescriptor::new("gateway")`）。
/// - `ByShard`：限定逻辑分片或机架，使用字面字符串标识。
/// - `Custom`：实现自定义过滤语法，字符串内容由双方协商（可采用 CEL、Rego 等表达式）。
///
/// # 实现责任 (Implementation Responsibility)
/// - **命名约定**：推荐以 `vendor://feature` 标识语法来源，或遵循反向域名，避免与社区扩展冲突。
/// - **错误处理**：若宿主或控制面无法解析该语法，必须返回 `cluster.unsupported_scope` 或
///   [`crate::error::codes::ROUTER_VERSION_CONFLICT`]，并附带详细错误描述。
/// - **禁止降级**：不得静默忽略 `Custom` 选择器或退化为 `EntireCluster`，以免造成越权订阅。
/// - **前置条件**：实现者需明确哪些分支受支持；若不支持应在运行时返回错误。
/// - **后置条件**：选定范围内的事件必须保持顺序一致性。
///
/// # 风险提示（Trade-offs）
/// - `Custom` 分支解析成本可能较高，建议缓存解析结果或限制表达式复杂度。
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum ClusterScopeSelector {
    EntireCluster,
    ByRole(RoleDescriptor),
    ByShard(String),
    Custom(String),
}

/// 成员订阅的范围描述。
///
/// # 设计背景（Why）
/// - 将选择器与订阅方期望的一致性等级解耦，便于调用方在不同场景下复用同一选择器。
/// - 提供默认实现 `ClusterMembershipScope::entire_cluster()`，降低常见场景的样板代码。
///
/// # 契约说明（What）
/// - `selector`：核心过滤条件。
/// - `consistency`：事件与快照的期望一致性，参见 [`ClusterConsistencyLevel`].
/// - **前置条件**：`selector` 与 `consistency` 均需在实现者支持的能力范围内，否则应返回 `cluster.unsupported_scope` 错误码。
/// - **后置条件**：成功订阅后，事件必须满足指定的一致性契约（例如 `Linearizable` 不得乱序）。
///
/// # 风险提示（Trade-offs）
/// - 更强的一致性等级通常意味着更高延迟，应由调用方结合业务 SLA 选择合适配置。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClusterMembershipScope {
    pub selector: ClusterScopeSelector,
    pub consistency: ClusterConsistencyLevel,
}

impl ClusterMembershipScope {
    /// 创建订阅全量节点、最终一致性的默认范围。
    pub fn entire_cluster() -> Self {
        Self {
            selector: ClusterScopeSelector::EntireCluster,
            consistency: ClusterConsistencyLevel::Eventual,
        }
    }
}

/// 成员事件流。
///
/// # 设计背景（Why）
/// - 结合生产案例的事件语义（加入、离开、更新、状态改变）与前沿研究的修订号机制，以支持可重放与幂等处理。
/// - `SnapshotApplied` 事件允许实现方在连接恢复时推送一份基础快照，无需重新发起单独的快照请求。
///
/// # 契约说明（What）
/// - `revision` 字段表示事件对应的全局修订号，必须严格递增。
/// - `MemberUpdated` 仅表示元数据或角色变化；若状态变更应发送 `MemberStateChanged`。
/// - `MemberRetired` 用于节点正常退出；如因故障被移除，应使用 `MemberUnreachable`。
/// - **前置条件**：事件必须遵循单调递增的修订号；如遇到乱序，调用方可触发快照重建。
/// - **后置条件**：调用方应用事件后，快照与本地缓存应保持一致。
///
/// # 风险提示（Trade-offs）
/// - 事件流需要有界缓冲；当消费者处理过慢时，建议实现回退到推送快照并重置订阅，以避免无限阻塞。
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum ClusterMembershipEvent {
    SnapshotApplied(ClusterMembershipSnapshot),
    MemberJoined {
        revision: ClusterRevision,
        profile: ClusterNodeProfile,
    },
    MemberUpdated {
        revision: ClusterRevision,
        profile: ClusterNodeProfile,
    },
    MemberStateChanged {
        revision: ClusterRevision,
        node_id: NodeId,
        new_state: ClusterNodeState,
    },
    MemberRetired {
        revision: ClusterRevision,
        node_id: NodeId,
    },
    MemberUnreachable {
        revision: ClusterRevision,
        node_id: NodeId,
    },
}

/// 集群成员管理契约。
///
/// # 设计背景（Why）
/// - 对齐业界 Top5 产品的公共能力：快照、事件流、自身信息查询、范围过滤、一致性选项。
/// - 吸收前沿研究（如 AP 集群中的 CRDT 与因果广播），通过 `ClusterRevision` 确保事件可幂等重放。
///
/// # 逻辑解析（How）
/// - `snapshot`：获取指定范围的全量视图，应尊重 `consistency` 的语义，例如 `Linearizable` 需要借助 Raft/etcd 的读屏障。
/// - `subscribe`：返回一个流式事件源，可选起始修订号用于断点续传，并允许通过
///   [`SubscriptionFlowControl`] 协商缓冲模式与队列探针；当启用观测时，应在
///   [`SubscriptionStream::queue_probe`] 中填充实现提供的观测对象。
/// - `self_profile`：提供运行时自身节点的画像，便于 Handler 决策。
///
/// # 契约说明（What）
/// - **输入参数**：
///   - `scope`：事件过滤器与一致性配置，详见 [`ClusterMembershipScope`].
///   - `resume_from`：可选修订号，表示消费端已经处理到的进度。
///   - `flow_control`：订阅流控配置，详见 [`SubscriptionFlowControl`]。
/// - **返回值**：
///   - `snapshot` 返回 `ClusterMembershipSnapshot`，若范围为空需返回空集合而非错误。
///   - `subscribe` 返回 [`SubscriptionStream<ClusterMembershipEvent>`]，要求事件按修订号递增；若 `queue_probe`
///     为 `Some`，表示实现已启用队列观测能力。
///   - `self_profile` 返回当前节点画像，若节点未注册应返回错误 `cluster.self_not_registered`。
/// - **前置条件**：实现者需确保底层存储已初始化，且事件流在订阅前开始记录。
/// - **后置条件**：调用成功后，消费者可将返回值作为权威真相来源并在本地缓存。
///
/// # 性能契约（Performance Contract）
/// - `snapshot` 与 `self_profile` 通过 `async fn` 返回业务结果，`subscribe` 返回 [`SubscriptionStream`]（内部事件流仍为
///   [`crate::BoxStream`]），这些对象安全包装会触发一次堆分配与通过虚表的 `poll` 跳转。
/// - `async_contract_overhead` 基准量化了额外成本：Future 模拟场景中泛型实现 6.23ns/次、装箱后的路径 6.09ns/次（约 -0.9%）。
///   Stream 模拟场景中泛型 6.39ns/次、`BoxStream` 6.63ns/次（约 +3.8%），覆盖大多数控制面负载需求。【e8841c†L4-L13】
/// - 若实现面向高频事件（>10^6 qps），建议提供附加泛型订阅接口、在内部复用 `Box` 缓冲池。
///   也可允许性能敏感的调用方直接依赖具名实现类型，以将分配与虚表开销降至最低。
///
/// # 设计取舍与风险（Trade-offs）
/// - 接口未强制使用某种一致性算法，允许实现者选择 Raft、Viewstamped Replication、Gossip+Delta CRDT 等不同方案；但更强一致性会增
///   加延迟。
/// - `subscribe` 必须在背压严重时提供自适应策略（如快照重置或事件压缩），否则可能导致内存膨胀；当启用
///   `SubscriptionFlowControl::bounded` 且溢出时，应返回 `cluster.queue_overflow` 或在探针中增加丢弃计数。
///
/// # 错误契约（Error Contract）
/// - `snapshot`：
///   - 网络分区或无法联通共识面时，必须返回 [`crate::error::codes::CLUSTER_NETWORK_PARTITION`]，引导调用方退避并观察集群恢复状态。
///   - 若当前主控节点丢失领导权，应返回 [`crate::error::codes::CLUSTER_LEADER_LOST`]，提示上游等待新领导者确认后重试。
/// - `subscribe`：
///   - 当订阅背压容量耗尽且策略为 `FailStream` 时，应终止流并返回
///     [`crate::error::codes::CLUSTER_QUEUE_OVERFLOW`]，提醒调用方提升处理速率或调整订阅策略。
///   - 底层控制面发生网络分区或领导者失效时，需返回
///     [`crate::error::codes::CLUSTER_NETWORK_PARTITION`] / [`crate::error::codes::CLUSTER_LEADER_LOST`]，
///     并在事件流中给出恢复建议（如切换到降级快照）。
/// - `self_profile`：
///   - 若读取到陈旧元数据或缓存尚未刷新，应返回 [`crate::error::codes::DISCOVERY_STALE_READ`]，驱动调用方刷新快照或执行线性一致读。
#[async_trait]
pub trait ClusterMembership: Send + Sync + 'static + Sealed {
    /// 获取指定范围的全量快照。
    async fn snapshot(
        &self,
        scope: ClusterMembershipScope,
    ) -> crate::Result<ClusterMembershipSnapshot, ClusterError>;

    /// 订阅指定范围的增量事件。
    fn subscribe(
        &self,
        scope: ClusterMembershipScope,
        resume_from: Option<ClusterRevision>,
        flow_control: SubscriptionFlowControl,
    ) -> SubscriptionStream<ClusterMembershipEvent>;

    /// 获取当前节点的画像。
    async fn self_profile(&self) -> crate::Result<ClusterNodeProfile, ClusterError>;
}
