use alloc::boxed::Box;

use crate::{CoreError, sealed::Sealed};

use super::{context::Context, initializer::InitializerDescriptor};

use crate::buffer::PipelineMessage;
use crate::observability::CoreUserEvent;

/// 入站事件处理合约，面向从传输层到业务层的正向数据流。
///
/// # 设计背景（Why）
/// - 汇总 Netty `ChannelInboundHandler`、Envoy Stream Filter、gRPC Server Interceptor、Tower `Service` 调用链的经验，确保 Handler 能以细粒度响应事件。
/// - 结合科研需求，引入 `InitializerDescriptor` 以支持静态分析、链路可视化。
///
/// # 契约说明（What）
/// - 所有方法均在 Pipeline 线程或执行上下文中调用，必须无阻塞或将耗时操作移交到运行时。
/// - `on_user_event` 适配跨模块的业务事件广播。
/// - 异常需通过 `on_exception_caught` 处理，必要时触发降级或关闭连接。
///
/// # 前置/后置条件（Contract）
/// - **前置**：实现类型必须是 `'static + Send + Sync`，以便在多线程环境下安全复用。
/// - **后置**：若 `on_exception_caught` 未能恢复，应通知 Pipeline 触发关闭，以防资源泄漏。
///
/// # 风险提示（Trade-offs）
/// - 请避免在 Handler 内部持久化 `Context` 引用；若确有需要，需确保不会导致引用循环。
/// - `on_read` 可能在高频调用下成为性能瓶颈，可结合 `InitializerDescriptor::stage` 信息调度到合适线程池。
pub trait InboundHandler: Send + Sync + 'static + Sealed {
    /// 返回 Handler 元数据，默认提供匿名描述，便于链路观测。
    fn describe(&self) -> InitializerDescriptor {
        InitializerDescriptor::anonymous("inbound-handler")
    }

    /// 通道活跃时调用。
    fn on_channel_active(&self, ctx: &dyn Context);

    /// 处理读到的消息。
    fn on_read(&self, ctx: &dyn Context, msg: PipelineMessage);

    /// 一批读取完成。
    fn on_read_complete(&self, ctx: &dyn Context);

    /// 可写性变化。
    fn on_writability_changed(&self, ctx: &dyn Context, is_writable: bool);

    /// 用户事件。
    fn on_user_event(&self, ctx: &dyn Context, event: CoreUserEvent);

    /// 异常处理。
    fn on_exception_caught(&self, ctx: &dyn Context, error: CoreError);

    /// 通道不再活跃。
    fn on_channel_inactive(&self, ctx: &dyn Context);
}

/// 出站事件处理合约，负责从业务层到传输层的逆向数据流。
///
/// # 设计背景（Why）
/// - 兼容 Netty `ChannelOutboundHandler`、Tower Layer、gRPC Client Interceptor、Envoy Outbound Filter 的模式。
/// - 支持科研场景中对写路径进行形式化验证或性能剖析。
///
/// # 契约说明（What）
/// - `on_write` 必须遵循背压信号语义，将 [`WriteSignal`](super::channel::WriteSignal) 返回给上游。
/// - `on_flush` 用于冲刷缓冲或触发批处理。
/// - `on_close_graceful` 需协调下游 Handler 保证截止时间内完成资源回收。
///
/// # 风险提示（Trade-offs）
/// - 若实现内部持有缓冲池，应考虑在异常路径释放资源，以免造成泄漏。
/// - 对性能敏感场景，应避免在写路径执行复杂序列化，可结合编解码器模块预处理。
pub trait OutboundHandler: Send + Sync + 'static + Sealed {
    /// 返回 Handler 元数据，默认提供匿名描述。
    fn describe(&self) -> InitializerDescriptor {
        InitializerDescriptor::anonymous("outbound-handler")
    }

    /// 写入消息。
    fn on_write(
        &self,
        ctx: &dyn Context,
        msg: PipelineMessage,
    ) -> crate::Result<super::channel::WriteSignal, CoreError>;

    /// 刷新写缓冲。
    fn on_flush(&self, ctx: &dyn Context) -> crate::Result<(), CoreError>;

    /// 优雅关闭。
    fn on_close_graceful(
        &self,
        ctx: &dyn Context,
        deadline: Option<core::time::Duration>,
    ) -> crate::Result<(), CoreError>;
}

/// 同时处理入站与出站事件的全双工 Handler。
///
/// # 设计背景（Why）
/// - 对齐 gRPC Bidi Stream、QUIC Stream Handler 等双向协议需求，使实现者可在单类型中处理双向逻辑。
///
/// # 契约说明（What）
/// - 任何实现 `InboundHandler + OutboundHandler` 的类型自动实现 `DuplexHandler`。
/// - 适合用于需要共享状态的编解码器、加密器或应用层协议适配器。
pub trait DuplexHandler: InboundHandler + OutboundHandler + Sealed {}

impl<T> DuplexHandler for T where T: InboundHandler + OutboundHandler {}

/// 将 `'static` 生命周期的 Handler 引用适配为 `Box<dyn InboundHandler>`。
///
/// # 设计动机（Why）
/// - 中心运行时经常在常量或懒加载阶段构造 Handler，并以 `'static` 引用暴露给多条链路。
/// - 传统 `register_*` API 仅接受拥有所有权的 `Box`，迫使调用方复制或重新分配对象。
/// - 本适配器为“借用入口”提供零拷贝桥接：保留原始 `'static` 引用，仅在外层包裹一个轻量代理。
///
/// # 工作方式（How）
/// - [`BorrowedInboundHandlerAdapter`] 内部保存对源 Handler 的引用，所有回调直接转发至原实现；
/// - 适配器自身满足 `Send + Sync + 'static`，符合 `InboundHandler` 的线程安全约束；
/// - `Box` 仅承载代理对象，不重新分配底层资源。
///
/// # 契约说明（What）
/// - **输入**：`handler` 必须指向 `'static` 生命周期的 `InboundHandler` 实例，常见于 `lazy_static!` 或 `OnceLock` 场景；
/// - **返回值**：可直接传递给 `Pipeline::register_inbound_handler` 等拥有型入口；
/// - **前置条件**：调用方需保证底层 Handler 在系统生命周期内有效；
/// - **后置条件**：代理仅负责转发，不拥有底层资源，关闭时不会触发额外析构。
///
/// # 风险提示（Trade-offs）
/// - 若底层 Handler 非 `'static`，请使用 `Box` 拥有型入口避免悬垂引用；
/// - 代理不会拦截或扩展回调，如需在转发前后注入逻辑，仍应实现自定义 Handler。
pub(crate) fn box_inbound_from_static(
    handler: &'static dyn InboundHandler,
) -> Box<dyn InboundHandler> {
    Box::new(BorrowedInboundHandlerAdapter { inner: handler })
}

/// 将 `'static` 生命周期的出站 Handler 引用适配为 `Box<dyn OutboundHandler>`。
///
/// # 设计动机（Why）
/// - 配置驱动的链路经常以全局单例形式存放出站 Handler；
/// - 通过借用适配器即可在保持零拷贝的同时复用既有拥有型注册入口。
///
/// # 契约说明（What）
/// - **输入**：`handler` 为 `'static` 出站 Handler；
/// - **返回值**：可交由 `Pipeline::register_outbound_handler` 继续处理；
/// - **前置条件/后置条件**：与 [`box_inbound_from_static`] 一致。
pub(crate) fn box_outbound_from_static(
    handler: &'static dyn OutboundHandler,
) -> Box<dyn OutboundHandler> {
    Box::new(BorrowedOutboundHandlerAdapter { inner: handler })
}

/// 代理入站 Handler，将所有调用转发给底层 `'static` 引用。
struct BorrowedInboundHandlerAdapter {
    inner: &'static dyn InboundHandler,
}

impl InboundHandler for BorrowedInboundHandlerAdapter {
    fn describe(&self) -> InitializerDescriptor {
        self.inner.describe()
    }

    fn on_channel_active(&self, ctx: &dyn Context) {
        self.inner.on_channel_active(ctx)
    }

    fn on_read(&self, ctx: &dyn Context, msg: PipelineMessage) {
        self.inner.on_read(ctx, msg)
    }

    fn on_read_complete(&self, ctx: &dyn Context) {
        self.inner.on_read_complete(ctx)
    }

    fn on_writability_changed(&self, ctx: &dyn Context, is_writable: bool) {
        self.inner.on_writability_changed(ctx, is_writable)
    }

    fn on_user_event(&self, ctx: &dyn Context, event: CoreUserEvent) {
        self.inner.on_user_event(ctx, event)
    }

    fn on_exception_caught(&self, ctx: &dyn Context, error: CoreError) {
        self.inner.on_exception_caught(ctx, error)
    }

    fn on_channel_inactive(&self, ctx: &dyn Context) {
        self.inner.on_channel_inactive(ctx)
    }
}

/// 代理出站 Handler，将所有调用转发给底层 `'static` 引用。
struct BorrowedOutboundHandlerAdapter {
    inner: &'static dyn OutboundHandler,
}

impl OutboundHandler for BorrowedOutboundHandlerAdapter {
    fn describe(&self) -> InitializerDescriptor {
        self.inner.describe()
    }

    fn on_write(
        &self,
        ctx: &dyn Context,
        msg: PipelineMessage,
    ) -> crate::Result<super::channel::WriteSignal, CoreError> {
        self.inner.on_write(ctx, msg)
    }

    fn on_flush(&self, ctx: &dyn Context) -> crate::Result<(), CoreError> {
        self.inner.on_flush(ctx)
    }

    fn on_close_graceful(
        &self,
        ctx: &dyn Context,
        deadline: Option<core::time::Duration>,
    ) -> crate::Result<(), CoreError> {
        self.inner.on_close_graceful(ctx, deadline)
    }
}
