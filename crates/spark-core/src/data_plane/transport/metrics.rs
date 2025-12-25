use core::time::Duration;

use crate::observability::{
    AttributeSet, InstrumentDescriptor, MetricsProvider, metrics::contract::transport as contract,
};

/// 传输层字节方向。
///
/// # 设计动机（Why）
/// - 底层传输层需要同时统计入站/出站字节量，枚举封装减少调用方重复判断。
///
/// # 契约说明（What）
/// - `Inbound`：由远端发送到本实例；
/// - `Outbound`：由本实例发送到远端；
/// - **前置条件**：字节数应来源于物理链路的真实读写，避免与 Service 层重复统计；
/// - **后置条件**：可多次调用累加，指标用于衡量吞吐与带宽。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LinkDirection {
    Inbound,
    Outbound,
}

impl LinkDirection {
    #[inline]
    fn descriptor(&self) -> &'static InstrumentDescriptor<'static> {
        match self {
            LinkDirection::Inbound => &contract::BYTES_INBOUND,
            LinkDirection::Outbound => &contract::BYTES_OUTBOUND,
        }
    }
}

/// 传输层指标挂钩。
///
/// # 设计动机（Why）
/// - 统一记录连接生命周期（尝试、建立、关闭）及链路性能指标；
/// - 为未来在 Transport 工厂中自动注入打点提供基线实现。
///
/// # 契约说明（What）
/// - 构造时仅借用 [`MetricsProvider`]；
/// - 所有方法假设调用者遵循“尝试 → 建立 → 关闭”的顺序；
/// - **前置条件**：标签集合需包含 `transport.protocol`、`listener.id`、`peer.role` 等稳定字段；
/// - **后置条件**：指标会被立即写入或缓存至后端，实现不得 panic。
pub struct TransportMetricsHook<'a> {
    provider: &'a dyn MetricsProvider,
}

impl<'a> TransportMetricsHook<'a> {
    /// 构造传输层指标挂钩。
    pub fn new(provider: &'a dyn MetricsProvider) -> Self {
        Self { provider }
    }

    /// 记录一次建连尝试。
    ///
    /// # 调用契约
    /// - **输入**：`attempt_attributes` 应包含 `result` 标签（`success` 或 `failure`）；
    /// - **行为**：无论成功与否都会累加 `connection.attempts`；当 `success == false` 时同时累加 `connection.failures`；
    /// - **前置条件**：在握手开始前调用；
    /// - **后置条件**：计数器增加 1，失败分支额外写入失败计数。
    pub fn on_connection_attempt(&self, attempt_attributes: AttributeSet<'_>, success: bool) {
        self.provider
            .record_counter_add(&contract::CONNECTION_ATTEMPTS, 1, attempt_attributes);
        if !success {
            self.provider
                .record_counter_add(&contract::CONNECTION_FAILURES, 1, attempt_attributes);
        }
    }

    /// 在连接成功建立后增加活跃连接 Gauge。
    ///
    /// # 调用契约
    /// - **输入**：`active_attributes` 不含 `result`，仅包含协议、监听器、角色等稳定标签；
    /// - **前置条件**：应在握手完成后调用，确保 Gauge 与真实状态一致；
    /// - **后置条件**：`spark.transport.connections` 增加 1。
    pub fn on_connection_established(&self, active_attributes: AttributeSet<'_>) {
        self.provider
            .gauge(&contract::CONNECTIONS_ACTIVE)
            .increment(1.0, active_attributes);
    }

    /// 在连接关闭时回收活跃连接 Gauge。
    ///
    /// # 调用契约
    /// - **输入**：`active_attributes` 必须与 `on_connection_established` 使用的标签集合一致；
    /// - **前置条件**：无论连接是正常结束还是异常中断，都需调用该方法；
    /// - **后置条件**：`spark.transport.connections` 减少 1。
    pub fn on_connection_closed(&self, active_attributes: AttributeSet<'_>) {
        self.provider
            .gauge(&contract::CONNECTIONS_ACTIVE)
            .decrement(1.0, active_attributes);
    }

    /// 记录握手耗时。
    ///
    /// # 调用契约
    /// - **输入**：`duration` 为握手完成所耗费的时间；
    /// - **前置条件**：若握手失败仍可记录失败时长，需确保属性中包含 `result = failure`；
    /// - **后置条件**：`spark.transport.handshake.duration` 累加一个样本。
    pub fn record_handshake_duration(&self, duration: Duration, attributes: AttributeSet<'_>) {
        let duration_ms = duration.as_secs_f64() * 1_000.0;
        self.provider
            .record_histogram(&contract::HANDSHAKE_DURATION, duration_ms, attributes);
    }

    /// 累加链路字节量。
    ///
    /// # 调用契约
    /// - **输入**：`direction` 指示收包或发包；`bytes` 为本次增量；
    /// - **前置条件**：允许在一个连接生命周期内多次调用，确保增量累加；
    /// - **后置条件**：`spark.transport.bytes.*` 对应指标增加 `bytes`。
    pub fn record_link_bytes(
        &self,
        direction: LinkDirection,
        bytes: u64,
        attributes: AttributeSet<'_>,
    ) {
        self.provider
            .record_counter_add(direction.descriptor(), bytes, attributes);
    }
}
