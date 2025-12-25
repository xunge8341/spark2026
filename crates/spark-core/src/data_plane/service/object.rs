use alloc::{boxed::Box, sync::Arc};
use core::fmt;

use crate::{
    SparkError, async_trait,
    buffer::PipelineMessage,
    context::Context,
    contract::{CallContext, CloseReason},
    sealed::Sealed,
    status::PollReady,
};

use super::generic::Service;

/// `DynService` 代表对象层的服务契约，用于插件、脚本等运行时动态扩展场景。
///
/// # 设计初衷（Why）
/// - 在运行时加载的 Handler/Router 组件中，需要统一的对象安全接口以存放在容器或注册表；
/// - 与泛型 [`Service`] 对应，实现“双层 API 等价”——所有语义（背压、预算、优雅关闭）完全保持一致；
/// - 避免额外依赖：仅引用 `PipelineMessage` 这一最小化的通用消息体，减少插件引入体积。
///
/// # 行为逻辑（How）
/// 1. `poll_ready_dyn` 调用底层实现的就绪检查，返回 [`PollReady<SparkError>`]；
/// 2. `call_dyn` 以 `async fn` 形式返回业务结果，`#[async_trait]` 自动装箱内部 Future；
/// 3. `graceful_close`/`closed` 维护优雅关闭契约，`closed` 通过异步完成信号通知运行时释放资源。
///
/// # 契约说明（What）
/// - **输入**：上下文与泛型层一致，均来源于 [`CallContext`] 的视图；
/// - **前置条件**：必须在 `call_dyn` 之前获得 `ReadyState::Ready`；
/// - **后置条件**：实现需保证在关闭流程中完成资源回收或返回明确错误。
///
/// # 线程安全与生命周期说明
/// - `DynService: Send + Sync + 'static`：
///   - **原因**：对象层服务通常存放在 `Arc<dyn DynService>` 中跨线程共享，且需在整个服务生命周期内有效；
///   - **借用策略**：请求/响应消息体通过 [`PipelineMessage`] 承载，不强制 `'static`，避免阻碍短生命周期缓冲在调用结束后释放。
///
/// # 风险提示（Trade-offs）
/// - 相较泛型层，多出一次虚表调用与一次堆分配；在热路径若可静态确定类型，仍建议使用 [`Service`]。
#[async_trait]
pub trait DynService: Send + Sync + Sealed {
    /// 检查对象层服务是否就绪。
    fn poll_ready_dyn(
        &mut self,
        ctx: &Context<'_>,
        cx: &mut core::task::Context<'_>,
    ) -> PollReady<SparkError>;

    /// 调用对象层服务。
    async fn call_dyn(
        &mut self,
        ctx: CallContext,
        req: PipelineMessage,
    ) -> crate::Result<PipelineMessage, SparkError>;

    /// 通知服务执行优雅关闭逻辑。
    fn graceful_close(&mut self, reason: CloseReason);

    /// 等待服务完全关闭。
    async fn closed(&mut self) -> crate::Result<(), SparkError>;
}

/// 用于自定义关闭逻辑的钩子类型别名。
pub type CloseCallback<S> = dyn Fn(&mut S, CloseReason) + Send + Sync;

/// `ServiceObject` 将泛型 [`Service`] 适配为 [`DynService`]。
///
/// # 设计初衷（Why）
/// - 为控制面/插件系统提供统一的桥接器，避免重复编写类型擦除代码；
/// - 通过注入 `decode`/`encode` 闭包，将领域特定的数据模型转换为 `PipelineMessage`。
///
/// # 行为逻辑（How）
/// - 构造时保存 `inner` 泛型服务与转换闭包；
/// - `call_dyn` 解码消息、调用泛型服务、再编码响应；借助 `async_trait` 自动完成 Future 装箱；
/// - `graceful_close` 触发可选关闭钩子，方便释放外部资源。
///
/// # 契约说明（What）
/// - **泛型参数**：`Request`/`Response` 为泛型层业务类型；
/// - **前置条件**：`decode` 必须能够接受来自 Pipeline 的消息格式；
/// - **后置条件**：`encode` 需保证输出的 `PipelineMessage` 满足下游 Handler/Router 的期待。
///
/// # 风险提示（Trade-offs）
/// - `decode` 失败时直接返回错误，调用方需结合观测系统记录异常流量；
/// - 适配器内部克隆 `Arc` 闭包，若热路径频繁调用，可考虑自定义更轻量的适配器。
pub struct ServiceObject<S, Request, Response, Decode, Encode>
where
    S: Service<Request, Response = Response, Error = SparkError>,
    Decode: Fn(PipelineMessage) -> crate::Result<Request, SparkError> + Send + Sync + 'static,
    Encode: Fn(Response) -> PipelineMessage + Send + Sync + 'static,
{
    inner: S,
    decode: Arc<Decode>,
    encode: Arc<Encode>,
    on_close: Arc<CloseCallback<S>>,
}

impl<S, Request, Response, Decode, Encode> ServiceObject<S, Request, Response, Decode, Encode>
where
    S: Service<Request, Response = Response, Error = SparkError>,
    Decode: Fn(PipelineMessage) -> crate::Result<Request, SparkError> + Send + Sync + 'static,
    Encode: Fn(Response) -> PipelineMessage + Send + Sync + 'static,
{
    /// 创建默认关闭钩子的适配器实例。
    pub fn new(inner: S, decode: Decode, encode: Encode) -> Self {
        Self {
            inner,
            decode: Arc::new(decode),
            encode: Arc::new(encode),
            on_close: Arc::new(|_: &mut S, _: CloseReason| {}),
        }
    }

    /// 指定额外的关闭钩子，用于释放外部资源或记录审计日志。
    pub fn with_close_hook(
        mut self,
        hook: impl Fn(&mut S, CloseReason) + Send + Sync + 'static,
    ) -> Self {
        self.on_close = Arc::new(hook);
        self
    }

    /// 访问内部泛型实现，便于测试或高级自定义。
    pub fn into_inner(self) -> S {
        self.inner
    }
}

#[async_trait]
impl<S, Request, Response, Decode, Encode> DynService
    for ServiceObject<S, Request, Response, Decode, Encode>
where
    S: Service<Request, Response = Response, Error = SparkError>,
    Decode: Fn(PipelineMessage) -> crate::Result<Request, SparkError> + Send + Sync + 'static,
    Encode: Fn(Response) -> PipelineMessage + Send + Sync + 'static,
{
    fn poll_ready_dyn(
        &mut self,
        ctx: &Context<'_>,
        cx: &mut core::task::Context<'_>,
    ) -> PollReady<SparkError> {
        self.inner.poll_ready(ctx, cx)
    }

    async fn call_dyn(
        &mut self,
        ctx: CallContext,
        req: PipelineMessage,
    ) -> crate::Result<PipelineMessage, SparkError> {
        let decode = Arc::clone(&self.decode);
        let encode = Arc::clone(&self.encode);
        let request = (decode.as_ref())(req)?;
        let response = self.inner.call(ctx, request).await?;
        Ok((encode.as_ref())(response))
    }

    fn graceful_close(&mut self, reason: CloseReason) {
        (self.on_close)(&mut self.inner, reason);
    }

    async fn closed(&mut self) -> crate::Result<(), SparkError> {
        Ok(())
    }
}

/// `BoxService` 为对象层提供可克隆的服务句柄。
///
/// # 设计初衷（Why）
/// - Router 与 Pipeline 需要在多处共享对象层服务，实现 `Clone` 便于在多线程环境下分发；
/// - 内部使用 `Arc<dyn DynService>`，保证引用计数语义明确。
///
/// # 契约说明（What）
/// - **构造**：`new` 接收任意 `Arc<dyn DynService>`；
/// - **前置条件**：传入的 `Arc` 必须来源于有效的对象层实现；
/// - **后置条件**：克隆后的句柄共享相同的底层服务实例。
#[derive(Clone)]
pub struct BoxService {
    inner: Arc<dyn DynService>,
}

impl BoxService {
    /// 基于对象层实例构造句柄。
    pub fn new(inner: Arc<dyn DynService>) -> Self {
        Self { inner }
    }

    /// 以 trait 对象形式访问内部服务。
    pub fn as_dyn(&self) -> &dyn DynService {
        &*self.inner
    }

    /// 消费句柄并返回内部的 [`Arc<dyn DynService>`]。
    ///
    /// # 教案式注释
    /// - **意图 (Why)**：在测试或需要直接驱动对象层服务的框架内部，常需要获得可变引用以调用
    ///   `poll_ready_dyn`/`call_dyn`；通过返回 `Arc`，调用方可在未共享前使用
    ///   [`Arc::get_mut`](alloc::sync::Arc::get_mut) 获取 `&mut dyn DynService`；
    /// - **位置 (Where)**：对象层 `BoxService` 的便捷入口，供运行时与测试代码重用；
    /// - **契约 (What)**：消耗当前句柄并返回内部 `Arc`；若后续仍需克隆句柄，请在调用本方法前完成；
    /// - **风险提示 (Trade-offs)**：一旦 `Arc` 被克隆，将无法再通过 `Arc::get_mut` 获得可变引用，调用方需
    ///   在桥接后的第一时间完成必要的对象层操作。
    pub fn into_arc(self) -> Arc<dyn DynService> {
        self.inner
    }
}

impl fmt::Debug for BoxService {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BoxService").finish()
    }
}
