use alloc::borrow::Cow;
use core::{fmt, future::Future, time::Duration};

use bytes::{Buf, BufMut};

use super::{ShutdownDirection, TransportSocketAddr, metrics::TransportMetricsHook};
use crate::{Result, observability::AttributeSet};

/// 连接级背压决策枚举。
///
/// # 教案级说明
///
/// ## 意图（Why）
/// - 将底层传输实现探测到的可写性、预算消耗情况统一封装，
///   使上层调度器能够以稳定 API 获取“是否可以继续写入”这一关键信号；
/// - 作为 `spark-core::status::ReadyState` 与传输实现之间的过渡层，
///   降低不同协议在背压语义上的差异。
///
/// ## 契约（What）
/// - `Ready`：传输实现已准备就绪，可以继续写入；
/// - `Busy`：暂时不可写，但无需额外等待；
/// - `RetryAfter { delay }`：建议在 `delay` 之后重试；
/// - `BudgetExhausted`：上层预算已耗尽，应等待新预算或放弃；
/// - `Rejected`：底层拒绝请求（例如连接已关闭）。
///
/// ## 解析逻辑（How）
/// - 具体实现通常根据 `ReadyState` 或内部指标映射到上述枚举；
/// - 该类型不携带额外上下文，所有诊断信息应通过日志或指标补充。
///
/// ## 设计考量（Trade-offs）
/// - 刻意保持枚举精简，避免直接暴露传输协议的细节；
/// - 若未来需要扩展原因字段，可在上层结合日志/指标记录。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BackpressureDecision {
    Ready,
    Busy,
    RetryAfter { delay: Duration },
    BudgetExhausted,
    Rejected,
}

/// 背压指标上下文：承载可选的指标挂钩与标签集合。
///
/// # 教案级说明
///
/// ## 意图（Why）
/// - 允许传输实现将 `classify_backpressure` 的结果与观测系统对接，
///   在返回决策的同时记录写路径的节奏变更；
/// - 避免在接口层直接依赖具体指标类型，保持可选性。
///
/// ## 契约（What）
/// - `hook`：可选的 [`TransportMetricsHook`] 引用，用于写入指标；
/// - `attributes`：可选的 `AttributeSet`，描述协议、监听器等标签；
/// - `new()`：构造空上下文；
/// - `with_metrics(...)`：封装具备指标信息的上下文；
/// - **前置条件**：调用方需确保引用的生命周期覆盖 `classify_backpressure` 执行时段；
/// - **后置条件**：本结构本身不记录状态，仅提供访问器。
///
/// ## 解析逻辑（How）
/// - 以 `Option` 表示指标组件是否存在；
/// - 访问器返回引用，避免克隆或复制标签集合。
///
/// ## 风险提示（Trade-offs）
/// - 若调用方传入的 `AttributeSet` 为临时变量，应保证其在决策函数执行期间有效；
/// - 当前未对指标进行写入封装，具体调用逻辑由实现者控制。
#[derive(Clone, Copy, Default)]
pub struct BackpressureMetrics<'a> {
    hook: Option<&'a TransportMetricsHook<'a>>,
    attributes: Option<AttributeSet<'a>>,
}

impl<'a> BackpressureMetrics<'a> {
    /// 构造空的指标上下文。
    pub const fn new() -> Self {
        Self {
            hook: None,
            attributes: None,
        }
    }

    /// 构造携带指标挂钩与标签的上下文。
    pub const fn with_metrics(
        hook: &'a TransportMetricsHook<'a>,
        attributes: AttributeSet<'a>,
    ) -> Self {
        Self {
            hook: Some(hook),
            attributes: Some(attributes),
        }
    }

    /// 返回可选的指标挂钩引用。
    pub const fn hook(&self) -> Option<&'a TransportMetricsHook<'a>> {
        self.hook
    }

    /// 返回可选的标签集合。
    pub const fn attributes(&self) -> Option<&AttributeSet<'a>> {
        self.attributes.as_ref()
    }
}

impl<'a> fmt::Debug for BackpressureMetrics<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BackpressureMetrics")
            .field("has_hook", &self.hook.is_some())
            .field("has_attributes", &self.attributes.is_some())
            .finish()
    }
}

/// 统一的传输通道契约。
///
/// # 教案级注释
///
/// ## 意图（Why）
/// - 为 TCP、TLS、QUIC 等具体实现提供共同接口，
///   让上层只需依赖一套 trait 即可完成读写、刷新、半关闭与背压探测；
/// - 携带 `CallContext`/`Context` 泛型，使取消、截止时间与预算语义贯穿到底层实现。
///
/// ## 契约（What）
/// - 关联类型：
///   - `Error`：统一的错误类型，通常映射为 [`crate::CoreError`]；
///   - `CallCtx<'ctx>`：执行单次 IO 操作时所需的调用上下文；
///   - `ReadyCtx<'ctx>`：执行背压探测时的上下文视图；
///   - `ReadFuture`/`WriteFuture`/`ShutdownFuture`/`FlushFuture`：对应异步操作的 Future；
/// - 核心方法：
///   - `id()`：返回连接标识，用于日志与指标；
///   - `peer_addr()` / `local_addr()`：提供对端与本端地址；
///   - `read()` / `write()` / `flush()` / `shutdown()`：封装一次完整的异步操作；
///   - `classify_backpressure()`：根据内部状态与可选指标上下文给出背压决策；
/// - **前置条件**：所有异步方法都需要生命周期覆盖调用周期的上下文引用；
/// - **后置条件**：成功返回表示操作完成或决策可供上层调度使用；
/// - **错误处理**：失败应返回结构化错误，保持错误码稳定。
///
/// ## 解析逻辑（How）
/// - 通过 GAT（泛型关联类型）定义每个操作返回的 Future，
///   使实现者可以直接返回 `async` 块而无需额外装箱；
/// - 缓冲区参数统一采用 `bytes::Buf` / `BufMut` trait 对象，确保与 Buffer 模块接口一致；
/// - `classify_backpressure` 接收 `BackpressureMetrics` 以便在决策阶段写入观测数据。
///
/// ## 风险与考量（Trade-offs）
/// - Trait 要求 `Send + Sync + 'static`，在极端裸机环境可能需要额外封装；
/// - `Buf`/`BufMut` trait 对象带来一次 vtable 跳转，但换取了跨模块的统一性；
/// - 若实现需要更细粒度的控制（例如零拷贝），可在具体 crate 中提供扩展方法，但需保持此 trait 的稳定语义。
pub trait Channel: Send + Sync + 'static {
    type Error: fmt::Debug + Send + Sync + 'static;
    type CallCtx<'ctx>;
    type ReadyCtx<'ctx>;

    type ReadFuture<'ctx>: Future<Output = Result<usize, Self::Error>> + Send + 'ctx
    where
        Self: 'ctx,
        Self::CallCtx<'ctx>: 'ctx;

    type WriteFuture<'ctx>: Future<Output = Result<usize, Self::Error>> + Send + 'ctx
    where
        Self: 'ctx,
        Self::CallCtx<'ctx>: 'ctx;

    type ShutdownFuture<'ctx>: Future<Output = Result<(), Self::Error>> + Send + 'ctx
    where
        Self: 'ctx,
        Self::CallCtx<'ctx>: 'ctx;

    type FlushFuture<'ctx>: Future<Output = Result<(), Self::Error>> + Send + 'ctx
    where
        Self: 'ctx,
        Self::CallCtx<'ctx>: 'ctx;

    /// 返回连接标识，用于日志与指标。
    fn id(&self) -> Cow<'_, str>;

    /// 返回对端地址。
    fn peer_addr(&self) -> Option<TransportSocketAddr>;

    /// 返回本地地址。
    fn local_addr(&self) -> Option<TransportSocketAddr>;

    /// 读取数据到可变缓冲区，返回实际读取字节数。
    fn read<'ctx>(
        &'ctx self,
        ctx: &'ctx Self::CallCtx<'ctx>,
        buf: &'ctx mut (dyn BufMut + Send + Sync + 'static),
    ) -> Self::ReadFuture<'ctx>;

    /// 从只读缓冲区写入数据，返回写入字节数。
    fn write<'ctx>(
        &'ctx self,
        ctx: &'ctx Self::CallCtx<'ctx>,
        buf: &'ctx mut (dyn Buf + Send + Sync + 'static),
    ) -> Self::WriteFuture<'ctx>;

    /// 刷新底层发送缓冲。
    fn flush<'ctx>(&'ctx self, ctx: &'ctx Self::CallCtx<'ctx>) -> Self::FlushFuture<'ctx>;

    /// 按方向执行半关闭。
    fn shutdown<'ctx>(
        &'ctx self,
        ctx: &'ctx Self::CallCtx<'ctx>,
        direction: ShutdownDirection,
    ) -> Self::ShutdownFuture<'ctx>;

    /// 给出背压决策，供上层调度使用。
    fn classify_backpressure(
        &self,
        ctx: &Self::ReadyCtx<'_>,
        metrics: &BackpressureMetrics<'_>,
    ) -> BackpressureDecision;
}
