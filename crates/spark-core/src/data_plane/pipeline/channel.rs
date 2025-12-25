use crate::{
    buffer::PipelineMessage,
    contract::{CloseReason, Deadline},
    error::{CoreError, SparkError},
    future::BoxFuture,
    sealed::Sealed,
    transport::TransportSocketAddr,
};

use super::pipeline::{Pipeline, PipelineHandleId};

/// 通道生命周期状态机，统一约束不同传输协议的状态转换。
///
/// # 设计背景（Why）
/// - **业界经验汇总**：综合 Netty `ChannelState`、Envoy `StreamState`、QUIC 连接状态，将复杂的协议生命周期抽象为四个核心状态，简化 Handler 的推理成本。
/// - **科研扩展**：状态机为运行时监控和形式化验证提供锚点，便于注入状态过渡的断言或自动化推理。
///
/// # 契约说明（What）
/// - `Initialized`：资源分配完成但尚未投入 I/O。
/// - `Active`：可进行全双工读写。
/// - `Draining`：执行优雅关闭，允许冲刷剩余写缓冲。
/// - `Closed`：终态，任何事件均应被忽略。
///
/// # 设计取舍（Trade-offs）
/// - 为兼顾 TCP、QUIC、内存传输等实现，本枚举保持最小状态集合；如需更细粒度状态，应通过扩展属性暴露，避免破坏共识。
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
#[non_exhaustive]
pub enum ChannelState {
    /// 初始态：资源已分配但尚未进入活跃读写。
    Initialized,
    /// 活跃态：底层连接建立完毕，可进行全双工 I/O。
    Active,
    /// 析构态：执行优雅关闭，拒绝新入站读但允许冲刷剩余写。
    Draining,
    /// 终止态：所有资源释放，后续事件必须被忽略。
    Closed,
}

/// 写入反馈信号，贯穿 Netty 背压、NATS Flow Control、Tower Concurrency Limit 等策略。
///
/// # 设计背景（Why）
/// - 将常见的背压语义压缩为三种信号，既兼容硬件级（RDMA、DPDK）实现，也适配软件协议栈。
///
/// # 契约说明（What）
/// - `Accepted`：消息进入缓冲但尚未刷出。
/// - `AcceptedAndFlushed`：消息已成功写出或持久化。
/// - `FlowControlApplied`：下游无法立即接收，调用方需减速或重试。
///
/// # 风险提示（Trade-offs）
/// - 某些协议缺乏显式背压，需在实现内部以阈值模拟；调用方应尊重此信号以避免雪崩放大。
/// - 每个变体均可映射到 [`BackpressureSignal`](crate::contract::BackpressureSignal)：`Accepted*`
///   → `Ready`，`FlowControlApplied` → `Busy` 或 `RetryAfter`，便于跨层统一治理。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum WriteSignal {
    /// 消息进入缓冲，尚待刷出。
    Accepted,
    /// 消息已经落盘或发送。
    AcceptedAndFlushed,
    /// 触发背压，调用方应重试或降速。
    FlowControlApplied,
}

/// `Channel` 抽象单个 I/O 连接的控制面能力。
///
/// # 设计背景（Why）
/// - **统一协议栈接口**：结合 Netty Channel、Hyper Connection、Tokio Transport、QUIC Streams、WebTransport Session 的公共能力，为上层 Handler 提供一致 API。
/// - **跨平台契约**：在 `no_std` 环境中仍可运行，便于嵌入式或内核态（eBPF、DPDK）实验。
///
/// # 契约说明（What）
/// - `id`：返回可用于日志/追踪的唯一标识。
/// - `state`：暴露当前 [`ChannelState`]，便于 Handler 做状态判断。
/// - `is_writable`：指示是否可写，通常结合背压使用。
/// - `controller`：获取当前控制器引用，用于事件广播或链路信息查询。
/// - `extensions`：访问跨 Handler 共享的扩展存储。
/// - `peer_addr` / `local_addr`：分别返回对端、本地地址（若实现可提供）。
/// - `close_graceful` / `close`：优雅或立即关闭连接。
/// - `write` / `flush`：写入消息与冲刷缓冲。
///
/// # 线程安全与生命周期说明
/// - `Channel: Send + Sync + 'static`：
///   - **原因**：通道可能被封装在 `Arc<dyn Channel>` 中跨线程调度，且生命周期覆盖整个连接周期；
///   - **对比**：传入的 [`PipelineMessage`] 不要求 `'static`，允许实现按需转移所有权或在写入前完成序列化。
/// - `extensions()` 返回的 Map 仅要求 `Send + Sync`（由实现负责），其值必须 `'static` 以支持跨 Handler 共享。
///
/// # 前置/后置条件（Contract）
/// - **前置**：调用者需确保对象来源于有效的传输工厂；在 `Closed` 状态下调用写相关方法应立即失败或被忽略。
/// - **后置**：成功写入后，至少有一次 `flush` 才能保证消息实际发送；`close_graceful` 应推进状态至 `Draining` 或 `Closed`。
///
/// # 风险提示（Trade-offs）
/// - 部分实现可能将 `write` 视为异步操作，返回 `Accepted` 不代表落盘；若需要持久化保证，应结合 ACK 或上层协议确认。
/// - 在多线程环境中实现者需注意 `controller()` 与 `extensions()` 可能返回内部引用，需保持线程安全。
pub trait Channel: Send + Sync + 'static + Sealed {
    /// 返回便于日志关联的 Channel 唯一 ID。
    fn id(&self) -> &str;

    /// 获取当前状态机状态。
    fn state(&self) -> ChannelState;

    /// 指示当前是否可写。
    fn is_writable(&self) -> bool;

    /// 返回关联的控制器引用。
    fn controller(&self) -> &dyn Pipeline<HandleId = PipelineHandleId>;

    /// 返回扩展属性映射。
    fn extensions(&self) -> &dyn crate::pipeline::ExtensionsMap;

    /// 获取对端地址。
    fn peer_addr(&self) -> Option<TransportSocketAddr>;

    /// 获取本地地址。
    fn local_addr(&self) -> Option<TransportSocketAddr>;

    /// 触发优雅关闭，允许在截止前冲刷缓冲。
    ///
    /// - **语义映射**：调用后应向上游广播
    ///   [`BackpressureSignal::ShutdownPending`](crate::contract::BackpressureSignal::ShutdownPending)，
    ///   提醒状态机进入 `Draining`，并据
    ///   [`ShutdownGraceful`](crate::contract::ShutdownGraceful) 的截止信息安排排空；
    /// - **前置条件**：`reason` 必须来源于业务或调度策略，`deadline` 表示排空截止；
    /// - **后置条件**：实现至少应推进内部状态至 [`ChannelState::Draining`]。
    fn close_graceful(&self, reason: CloseReason, deadline: Option<Deadline>);

    /// 立即终止连接。
    ///
    /// - **语义映射**：调用方应在外部产生
    ///   [`BackpressureSignal::ShutdownEnforced`](crate::contract::BackpressureSignal::ShutdownEnforced)，
    ///   并且实现必须立刻停止接收/发送；
    /// - **风险提示**：应优先用于不可恢复错误，防止遗漏资源清理。
    fn close(&self);

    /// 等待连接完全关闭，用于满足“优雅关闭契约”。
    ///
    /// - **契约说明**：返回的 Future 完成表示底层状态机报告 [`ChannelState::Closed`]，
    ///   若提前触发 [`ShutdownImmediate`](crate::contract::ShutdownImmediate) 也必须完成；
    /// - **调用建议**：结合
    ///   [`ContractStateMachine`](crate::contract::ContractStateMachine) 的状态观察，避免重复等待。
    fn closed(&self) -> BoxFuture<'static, crate::Result<(), SparkError>>;

    /// 向通道写入消息，返回背压信号。
    ///
    /// - **返回语义**：`WriteSignal` 应映射为
    ///   [`BackpressureSignal`](crate::contract::BackpressureSignal)，使得上层可以统一处理背压；
    /// - **预算协作**：若写入与 [`Budget`](crate::contract::Budget) 绑定，触发 `FlowControlApplied`
    ///   时应同步返回
    ///   `BudgetExhausted` 相关事件。
    fn write(&self, msg: PipelineMessage) -> crate::Result<WriteSignal, CoreError>;

    /// 刷新底层缓冲。
    fn flush(&self);
}
