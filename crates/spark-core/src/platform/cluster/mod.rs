//! 集群契约模块。
//!
//! # 模块定位（Why）
//! - 将原先集中在 `distributed` 单文件中的集群契约拆分为"成员管理"、"服务发现"、"拓扑抽象"三部分，
//!   对应云原生主流平台（Kubernetes、Consul、Eureka、etcd）在生产中的职责划分。
//! - 通过模块化设计，为未来引入更多亚领域（如多活部署、边缘协同）保留空间，避免单文件逐渐不可维护。
//!
//! # 架构关系（How）
//! - `flow_control`：定义订阅缓冲模式、溢出策略与队列观测探针，为成员/服务订阅提供统一配置体。
//! - `membership`：节点身份、状态与增量事件流的统一契约。
//! - `discovery`：逻辑服务解析与订阅接口，面向 API Gateway、微服务框架的稳定依赖。
//! - `topology`：跨模块共享的拓扑语义（角色、修订号、一致性等级），抽象出多家厂商的共性模型。
//!
//! # 合规契约（What）
//! - 三个子模块均以 `no_std + alloc` 兼容为前提，确保在嵌入式、云上与裸机环境均可复用。
//! - 所有对外 Trait 均要求 `Send + Sync + 'static`，满足多线程运行时调度器的安全约束。
//!
//! # 设计取舍与风险（Trade-offs）
//! - 拓扑语义参考了业界前五（Kubernetes、Consul、Zookeeper、Eureka、HashiCorp Nomad）的稳定能力，同时引入
//!   研究领域常见的 CRDT 修订号模型。实现者若仅需轻量能力，可选择性忽略部分枚举分支。
//! - 模块拆分后，使用者需显式引入子模块，初期迁移成本略有提升，但换来长期的可维护性与可测试性。

pub mod discovery;
pub mod flow_control;
pub mod membership;
pub mod topology;

pub use discovery::{
    DiscoveryEvent, DiscoverySnapshot, ServiceDiscovery, ServiceInstance, ServiceName,
};
pub use flow_control::{
    FlowControlMode, OverflowPolicy, SubscriptionFlowControl, SubscriptionQueueProbe,
    SubscriptionQueueSnapshot, SubscriptionStream,
};
pub use membership::{
    ClusterMembership, ClusterMembershipEvent, ClusterMembershipScope, ClusterMembershipSnapshot,
    ClusterNodeProfile, ClusterNodeState, ClusterScopeSelector, NodeId,
};
pub use topology::{ClusterConsistencyLevel, ClusterEpoch, ClusterRevision, RoleDescriptor};
