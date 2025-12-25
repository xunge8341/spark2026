use crate::{async_trait, sealed::Sealed};
use alloc::{boxed::Box, string::String, sync::Arc, vec::Vec};

/// 组件健康状态的快照。
///
/// # 设计背景（Why）
/// - 对齐 Kubernetes Probes、AWS Health Model 等实践，提供统一健康度量用于探针与 AIOps。
/// - 结合站点可靠性工程（SRE）的健康度矩阵，引入 `Degraded` 表示可服务但性能下降。
///
/// # 逻辑解析（How）
/// - `name` 为组件稳定标识，建议采用 `domain.component` 命名。
/// - `state` 使用 [`HealthState`] 表达健康级别。
/// - `details` 提供人类可读的诊断信息，便于快速排障。
///
/// # 契约说明（What）
/// - **前置条件**：`name` 必须全局唯一；`details` 应避免泄露敏感信息。
/// - **后置条件**：实例可安全克隆；推荐结合指标、运维事件共同使用。
///
/// # 风险提示（Trade-offs）
/// - 过长的 `details` 会增加日志与指标压力；建议控制在 1KB 以内。
#[derive(Clone, Debug)]
pub struct ComponentHealth {
    pub name: &'static str,
    pub state: HealthState,
    pub details: String,
}

impl ComponentHealth {
    /// 构造健康状态。
    pub fn new(name: &'static str, state: HealthState, details: impl Into<String>) -> Self {
        Self {
            name,
            state,
            details: details.into(),
        }
    }
}

/// 健康状态枚举。
///
/// # 契约说明（What）
/// - `Up`：完全正常；`Degraded`：部分功能异常但仍可用；`Down`：服务不可用需告警。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum HealthState {
    Up,
    Degraded,
    Down,
}

/// 健康检查提供者抽象。
///
/// # 设计背景（Why）
/// - 抽象数据库、缓存、外部依赖等探针，实现统一的健康读取接口。
/// - 可结合学术研究中的自适应探针（Adaptive Probing）实现动态频率调整。
///
/// # 契约说明（What）
/// - **前置条件**：实现需在合理超时内完成检查，避免阻塞健康聚合线程。
/// - **后置条件**：返回的 [`ComponentHealth`] 应包含最新状态快照。
///
/// # 性能契约（Performance Contract）
/// - `check_health` 通过 `async fn` 返回健康快照；宏 [`crate::async_trait`] 自动完成 Future 装箱，每次探针执行会产生一次堆分配与虚函数调度。
/// - `async_contract_overhead` Future 场景基于 20 万次轮询测得泛型实现 6.23ns/次、装箱路径 6.09ns/次（约 -0.9%）。
///   对常规健康聚合线程的 CPU 影响可忽略。【e8841c†L4-L13】
/// - 在 CPU 或内存受限环境，可通过实现自定义探针管理器（如对象池缓存 Future）或暴露静态类型接口，允许调用方按需绕过分配。
///
/// # 风险提示（Trade-offs）
/// - 建议在实现中复用共享连接或缓存，避免探针本身影响依赖性能。
#[async_trait]
pub trait HealthCheckProvider: Send + Sync + 'static + Sealed {
    /// 异步执行健康检查。
    async fn check_health(&self) -> ComponentHealth;
}

/// 健康检查集合的引用，便于在运行时统一管理。
pub type HealthChecks = Arc<Vec<Arc<dyn HealthCheckProvider>>>;
