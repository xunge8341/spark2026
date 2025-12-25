use alloc::{string::String, vec::Vec};
use core::time::Duration;

/// 集群世代标识。
///
/// # 设计背景（Why）
/// - 大型平台常在控制面选举或重大拓扑变更时切换世代（epoch），以避免旧节点持有的元数据污染当前视图。
/// - 参考 Raft、Zookeeper 以及 Kubernetes 等系统的做法，使用单调递增的无符号整数表示。
///
/// # 契约说明（What）
/// - `ClusterEpoch` 必须由控制面单调递增更新。
/// - **前置条件**：在开始广播成员或服务事件前，需先发布当前 `epoch`。
/// - **后置条件**：所有关联的 [`ClusterRevision`] 均应携带同一 `epoch`，确保消费者能够判定事件来自同一世代。
///
/// # 风险提示（Trade-offs）
/// - 若出现脑裂导致同时存在多个控制面，可能出现重复或回退的 `epoch`；此时消费者应结合修订号进行保护性检查。
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ClusterEpoch(pub u64);

impl ClusterEpoch {
    /// 使用原始数值创建新的世代标识。
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// 返回内部的整数值，便于持久化或日志记录。
    pub const fn value(self) -> u64 {
        self.0
    }
}

/// 全局修订号。
///
/// # 设计背景（Why）
/// - 结合 CRDT 与版本向量研究成果，将 `epoch` 与单调递增的计数器组合，既兼容强一致控制面也支持多区域容错。
/// - 该结构与 etcd 的 `revision`、Consul 的 `ModifyIndex` 概念对应，便于工程团队迁移。
///
/// # 契约说明（What）
/// - `epoch`：所属集群世代。
/// - `counter`：在当前世代内递增的版本号，必须单调不减。
/// - **前置条件**：生成新修订号时需读取最新计数器值并加一，避免重复。
/// - **后置条件**：消费者可用 `(epoch, counter)` 作为幂等键。
///
/// # 风险提示（Trade-offs）
/// - 若实现采用最终一致存储（如 Dynamo 风格），需通过租约或逻辑时钟避免跨节点的计数器竞争。
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ClusterRevision {
    pub epoch: ClusterEpoch,
    pub counter: u64,
}

impl ClusterRevision {
    /// 构造新的修订号。
    pub const fn new(epoch: ClusterEpoch, counter: u64) -> Self {
        Self { epoch, counter }
    }

    /// 创建下一个修订号，供控制面自增使用。
    pub const fn next(self) -> Self {
        Self {
            epoch: self.epoch,
            counter: self.counter.saturating_add(1),
        }
    }
}

/// 集群事件与服务解析需要遵循的一致性等级。
///
/// # 设计背景（Why）
/// - 归纳出云原生平台常见的三类一致性（最终一致、顺序一致、线性一致），并补充“有界陈旧”以适配边缘节点的能耗优化场景。
/// - 允许调用方在性能与一致性之间自主权衡，匹配生产与科研的不同需求。
///
/// # 契约说明（What）
/// - `Eventual`：最终一致，允许短暂陈旧，适合高吞吐环境。
/// - `Sequential`：顺序一致，保证单客户端的更新顺序可见。
/// - `Linearizable`：线性一致，提供最强一致性语义。
/// - `BoundedStaleness`：陈旧度受限，`max_staleness` 表示允许的最长时间差。
/// - **前置条件**：调用方需确认实现是否支持目标等级；不支持时应返回 `cluster.unsupported_consistency`。
/// - **后置条件**：实现需在文档中说明各等级的延迟与可靠性特征，供调用方制定 SLA。
///
/// # 风险提示（Trade-offs）
/// - 较强的一致性通常伴随更高延迟和更低可用性；`BoundedStaleness` 要求实现方维护可靠时钟或租约机制。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum ClusterConsistencyLevel {
    Eventual,
    Sequential,
    Linearizable,
    BoundedStaleness { max_staleness: Duration },
}

impl ClusterConsistencyLevel {
    /// 获取可选的陈旧度上限，用于指标或缓存策略。
    pub fn max_staleness(&self) -> Option<Duration> {
        match self {
            Self::BoundedStaleness { max_staleness } => Some(*max_staleness),
            _ => None,
        }
    }
}

/// 节点角色的标准化描述。
///
/// # 设计背景（Why）
/// - 业界存在大量角色命名（control-plane、ingress、worker、storage 等），因此采用字符串表示角色类型并允许附带上下文。
/// - 为支持研究领域的弹性角色绑定，引入 `attributes` 字段以存储补充语义（如优先级、算力指标）。
///
/// # 契约说明（What）
/// - `name`：角色名称，建议使用业界常见名称（`control-plane`、`data-plane`、`gateway`、`scheduler` 等）。
/// - `attributes`：额外属性集合，可用于表达角色等级或绑定的资源池。
/// - **前置条件**：名称必须非空；属性键值需满足 UTF-8。
/// - **后置条件**：角色描述可直接参与排序或比较，用于差分更新。
///
/// # 风险提示（Trade-offs）
/// - 未提供强制的枚举列表，若团队存在命名分歧需额外制定规范；否则可能导致过滤器失效。
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct RoleDescriptor {
    pub name: String,
    pub attributes: Vec<String>,
}

impl RoleDescriptor {
    /// 使用角色名称构建描述，属性默认为空。
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            attributes: Vec::new(),
        }
    }

    /// 使用名称与属性构建描述。
    pub fn with_attributes(name: impl Into<String>, attributes: Vec<String>) -> Self {
        Self {
            name: name.into(),
            attributes,
        }
    }

    /// 追加单个属性标签。
    pub fn push_attribute(&mut self, value: impl Into<String>) {
        self.attributes.push(value.into());
    }
}
