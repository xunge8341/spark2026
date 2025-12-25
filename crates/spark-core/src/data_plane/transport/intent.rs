use alloc::string::String;
use core::time::Duration;

use super::{Endpoint, TransportParams};

/// 通信质量等级，兼容主流云厂商与网络协议的 QoS 语义。
///
/// # 设计依据（Why）
/// - **生产经验**：综合 gRPC 优先级、MQTT QoS、QUIC Stream Priority，沉淀成最常见的三类质量等级。
/// - **科研探索**：通过有限枚举便于在仿真中映射为延迟/丢包约束，避免无限扩展带来的组合爆炸。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum QualityOfService {
    /// 交互式，请求-响应或低延迟通信。
    Interactive,
    /// 长连接流式传输，对吞吐与稳定性有要求。
    Streaming,
    /// 批量任务，允许更高延迟但追求带宽效率。
    BulkTransfer,
}

/// 可用性约束，定义连接在建立时的容错策略。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum AvailabilityRequirement {
    /// 尽力而为，失败时由上游策略决定是否重试。
    BestEffort,
    /// 要求至少成功建立一条可用连接，否则返回错误。
    RequireReady,
    /// 要求达到多副本或多通道冗余，通常用于关键业务链路。
    RequireRedundant,
}

/// 会话生命周期分类。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum SessionLifecycle {
    /// 短会话（一次性请求/响应）。
    Ephemeral,
    /// 持久双向流，典型于 gRPC streaming/QUIC bidirectional。
    BidirectionalStream,
    /// 订阅式长连接，例如消息总线、观察者模式。
    Subscription,
}

/// 传输安全模式。
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum SecurityMode {
    /// 继承环境或注册中心设定。
    Inherit,
    /// 强制启用 TLS/DTLS/QUIC-TLS。
    EnforcedTls,
    /// 明文传输，多用于内网或测试。
    Plaintext,
    /// 自定义安全插件，`identifier` 标识协议/算法。
    ///
    /// # 实现责任 (Implementation Responsibility)
    /// - **命名约定**：使用实现方命名空间或反向域名（如 `acme.post_quantum_tls`），避免冲突。
    /// - **错误处理**：传输工厂若无法识别 `identifier`，必须返回
    ///   [`crate::error::codes::APP_UNAUTHORIZED`] 或
    ///   [`crate::error::codes::ROUTER_VERSION_CONFLICT`]，并记录告警提醒升级或回滚。
    /// - **禁止降级**：不得自动回退为 `Inherit`/`Plaintext`，防止在未验证条件下暴露明文。
    Custom { identifier: String },
}

/// `ConnectionIntent` 将端点、质量、可用性、安全等信息整合，驱动传输工厂决策。
///
/// # 设计背景（Why）
/// - **行业对标**：结合 Kubernetes Gateway API、AWS App Mesh、Azure Service Fabric 中的连接配置项，将散落的参数统一为“意图”模型。
/// - **学术借鉴**：吸收 Intent-Based Networking、Software-Defined Networking 的策略表达，把策略声明与实际连接解耦。
///
/// # 契约说明（What）
/// - `endpoint`：目标 [`Endpoint`]。
/// - `qos`：质量等级，用于驱动调度/拥塞控制策略。
/// - `availability`：容错需求，协助传输工厂决定是否并行建连或回退。
/// - `lifecycle`：会话形态，指导是否需要保持心跳、自动重连。
/// - `security`：安全模式声明。
/// - `timeout`：整体建连超时；`None` 表示遵循默认。
/// - `retry_budget`：允许的重试次数上限。
/// - `params`：额外参数，覆盖或扩展端点默认值。
/// - **前置条件**：`endpoint` 已按需要（逻辑/物理）构建；若为逻辑地址应提供服务发现。
/// - **后置条件**：工厂按照意图返回通道或错误，调用方可复用该结构用于指标或审计。
///
/// # 风险提示（Trade-offs）
/// - 该结构不负责自动重试，调用方需结合重试策略组件；`retry_budget` 仅提供配置传递。
/// - 扩展字段需保持向后兼容，建议通过 `params` 注入实验性能力。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConnectionIntent {
    endpoint: Endpoint,
    qos: QualityOfService,
    availability: AvailabilityRequirement,
    lifecycle: SessionLifecycle,
    security: SecurityMode,
    timeout: Option<Duration>,
    retry_budget: Option<u32>,
    params: TransportParams,
}

impl ConnectionIntent {
    /// 构造意图，默认 QoS 为交互式，可用性尽力而为。
    pub fn new(endpoint: Endpoint) -> Self {
        Self {
            endpoint,
            qos: QualityOfService::Interactive,
            availability: AvailabilityRequirement::BestEffort,
            lifecycle: SessionLifecycle::Ephemeral,
            security: SecurityMode::Inherit,
            timeout: None,
            retry_budget: None,
            params: TransportParams::new(),
        }
    }

    /// Builder 风格修改 QoS。
    pub fn with_qos(mut self, qos: QualityOfService) -> Self {
        self.qos = qos;
        self
    }

    /// 修改可用性要求。
    pub fn with_availability(mut self, availability: AvailabilityRequirement) -> Self {
        self.availability = availability;
        self
    }

    /// 修改会话生命周期。
    pub fn with_lifecycle(mut self, lifecycle: SessionLifecycle) -> Self {
        self.lifecycle = lifecycle;
        self
    }

    /// 指定安全模式。
    pub fn with_security(mut self, security: SecurityMode) -> Self {
        self.security = security;
        self
    }

    /// 设置建连超时。
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// 设置重试预算。
    pub fn with_retry_budget(mut self, retries: u32) -> Self {
        self.retry_budget = Some(retries);
        self
    }

    /// 合并额外参数。
    pub fn with_params(mut self, params: TransportParams) -> Self {
        self.params = params;
        self
    }

    /// 访问目标端点。
    pub fn endpoint(&self) -> &Endpoint {
        &self.endpoint
    }

    /// 访问 QoS。
    pub fn qos(&self) -> QualityOfService {
        self.qos
    }

    /// 访问可用性要求。
    pub fn availability(&self) -> AvailabilityRequirement {
        self.availability
    }

    /// 访问生命周期。
    pub fn lifecycle(&self) -> SessionLifecycle {
        self.lifecycle
    }

    /// 访问安全模式。
    pub fn security(&self) -> &SecurityMode {
        &self.security
    }

    /// 获取建连超时。
    pub fn timeout(&self) -> Option<Duration> {
        self.timeout
    }

    /// 获取重试预算。
    pub fn retry_budget(&self) -> Option<u32> {
        self.retry_budget
    }

    /// 获取额外参数。
    pub fn params(&self) -> &TransportParams {
        &self.params
    }
}
