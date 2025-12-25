use alloc::{boxed::Box, sync::Arc};
use core::{fmt, future::Future};

use super::{
    TransportSocketAddr, channel::Channel as TransportChannel, handshake::HandshakeOutcome,
    server::ListenerShutdown,
};
use crate::{
    CoreError, Result, async_trait,
    context::Context,
    contract::CallContext,
    pipeline::{Channel as PipelineChannel, PipelineInitializer},
    sealed::Sealed,
};

/// `PipelineInitializer` 选择器的统一类型别名。
///
/// # 教案级注释
///
/// ## 意图（Why）
/// - 将“协议协商结果 → PipelineInitializer” 的决策过程抽象为稳定的函数接口，
///   便于不同 ServerChannel 在握手完成后复用一致的策略；
/// - 对标 Netty `ChannelInitializer` 动态选择链路的做法，使多协议监听器能够
///   根据 ALPN/SNI 等信息注入定制的中间件组合。
///
/// ## 契约（What）
/// - 输入：[`HandshakeOutcome`]，封装版本、能力与握手元数据；
/// - 输出：`Result<Arc<dyn PipelineInitializer>, CoreError>`，允许选择器在发现策略缺失
///   或配置冲突时返回结构化错误；
/// - 必须满足 `Send + Sync`，以便在多线程监听场景下安全共享。
///
/// ## 风险与注意事项（Trade-offs）
/// - 选择器返回的 `PipelineInitializer` 应为幂等配置，避免在重复调用时产生
///   竞态或资源泄漏；
/// - 若选择器内部执行阻塞操作，应在实现中自行调度至后台线程。
pub type PipelineInitializerSelector =
    dyn Fn(&HandshakeOutcome) -> Result<Arc<dyn PipelineInitializer>, CoreError> + Send + Sync;

/// 传输层服务端通道（`ServerChannel`）接口。
///
/// # 教案级注释
///
/// ## 意图（Why）
/// - 为各类协议（TCP、TLS、QUIC 等）提供与 Netty `ServerSocketChannel`
///   一致的服务端通道抽象，使上层可以在不感知具体协议的情况下接受连接并执行优雅关闭；
/// - 补充 [`crate::transport::channel::Channel`]，共同覆盖服务端监听通道与客户端通道的双向契约。
///
/// ## 契约（What）
/// - `Error`：结构化错误类型；
/// - `AcceptCtx<'ctx>`：接受新连接时的上下文；
/// - `ShutdownCtx<'ctx>`：执行关闭时的上下文；
/// - `Connection`：接受后返回的连接类型，必须实现 [`crate::transport::channel::Channel`]；
/// - `AcceptFuture`：返回 `(Connection, TransportSocketAddr)` 的 Future；
/// - `ShutdownFuture`：执行优雅关闭；
/// - `scheme()`：返回协议标识；
/// - `local_addr()`：查询服务端通道的监听地址；
/// - `accept()`：接受新连接并附带对端地址；
/// - `shutdown()`：依据 [`ListenerShutdown`] 计划执行优雅关闭；
/// - **前置条件**：上下文生命周期需覆盖 Future 执行时间；
/// - **后置条件**：Future 成功完成即表示操作符合协议语义。
///
/// ## 解析逻辑（How）
/// - 采用 GAT 约束，允许实现直接返回 `async` 块；
/// - 返回的地址使用 [`TransportSocketAddr`] 保持跨协议一致。
///
/// ## 风险提示（Trade-offs）
/// - 某些协议的服务端通道可能无法支持精细的半关闭，此时实现者应记录日志并返回适当错误；
/// - Trait 要求 `Send + Sync + 'static`，若运行在受限环境需增加适配层。
pub trait ServerChannel: Send + Sync + 'static {
    type Error: fmt::Debug + Send + Sync + 'static;
    type AcceptCtx<'ctx>;
    type ShutdownCtx<'ctx>;
    type Connection: TransportChannel;

    type AcceptFuture<'ctx>: Future<Output = Result<(Self::Connection, TransportSocketAddr), Self::Error>>
        + Send
        + 'ctx
    where
        Self: 'ctx,
        Self::AcceptCtx<'ctx>: 'ctx;

    type ShutdownFuture<'ctx>: Future<Output = Result<(), Self::Error>> + Send + 'ctx
    where
        Self: 'ctx,
        Self::ShutdownCtx<'ctx>: 'ctx;

    /// 返回协议标识。
    fn scheme(&self) -> &'static str;

    /// 查询服务端通道监听地址。
    fn local_addr(&self) -> Result<TransportSocketAddr, Self::Error>;

    /// 接受新连接。
    fn accept<'ctx>(&'ctx self, ctx: &'ctx Self::AcceptCtx<'ctx>) -> Self::AcceptFuture<'ctx>;

    /// 注入协议协商后如何选择 [`PipelineInitializer`] 的决策函数。
    ///
    /// # 教案级注释
    ///
    /// ## 意图（Why）
    /// - 将“协议握手结果 → PipelineInitializer” 的 L1 策略从路由层回收至监听器，
    ///   使 `ServerChannel` 在完成 ALPN/SNI 等协商后即可决定如何装配 Pipeline；
    /// - 对齐 Netty `ChannelInitializer` 的扩展点，便于多协议监听器在一个入口
    ///   根据协议族动态构建 Handler 链。
    ///
    /// ## 契约（What）
    /// - `selector`：`Arc` 包裹的决策闭包，输入为 [`HandshakeOutcome`]，输出为
    ///   `Result<Arc<dyn PipelineInitializer>, CoreError>`；实现必须确保闭包本身为无
    ///   状态或正确同步；
    /// - **前置条件**：调用方应在启动接受循环前设置该闭包；重复设置时以最后
    ///   一次调用为准；
    /// - **后置条件**：监听器在握手完成后应调用该闭包以取得匹配的初始化器，
    ///   并据此装配连接对应的 Pipeline。
    ///
    /// ## 解析逻辑（How）
    /// - 默认实现交由具体传输实现保存闭包引用；
    /// - `ServerChannel` 在 `accept` 成功后应读取握手元数据构造
    ///   [`HandshakeOutcome`]，再调用选择器获得初始化器。
    ///
    /// ## 风险提示（Trade-offs）
    /// - 若选择器未设置，监听器无法完成链路装配，应返回结构化错误或记录日志；
    /// - 闭包内部禁止阻塞，以免拖慢握手路径；必要时请委托后台执行器。
    fn set_initializer_selector(&self, selector: Arc<PipelineInitializerSelector>);

    /// 执行优雅关闭。
    fn shutdown<'ctx>(
        &'ctx self,
        ctx: &'ctx Self::ShutdownCtx<'ctx>,
        plan: ListenerShutdown,
    ) -> Self::ShutdownFuture<'ctx>;
}

/// 对象层监听器接口，供插件系统与脚本运行时存放在 `dyn` 容器中。
///
/// # 教案级注释
///
/// ## 意图（Why）
/// - 为运行时动态注册的传输实现提供统一的对象安全封装，便于插件、脚本等场景
///   在不知道具体泛型类型的情况下调度监听器；
/// - 与泛型层 [`ServerChannel`] 保持语义一致，从而保证双层接口语义等价。
///
/// ## 逻辑（How）
/// - `scheme_dyn` 返回协议名，帮助上层在多协议场景中进行路由；
/// - `local_addr_dyn` 查询监听地址，当前保留 [`Context`] 形参以便后续扩展审计/观测；
/// - `accept_dyn` 异步接受新连接，并将泛型通道装箱为 `Box<dyn PipelineChannel>`；
/// - `set_initializer_selector_dyn`、`shutdown_dyn` 分别桥接 Pipeline 注入与优雅关闭流程。
///
/// ## 契约（What）
/// - **前置条件**：监听器必须处于运行状态；
/// - **后置条件**：Future 成功完成时保证监听器按照计划返回对象层通道或完成关闭；
/// - **错误处理**：失败时返回 [`CoreError`]，调用方应记录并采取补救动作。
///
/// ## 风险提示（Trade-offs）
/// - 对象层调用会产生一次堆分配和虚表跳转，性能敏感路径应优先选择泛型接口；
/// - 若监听器未设置初始化选择器，应在实现中返回结构化错误提示配置缺失。
#[async_trait]
pub trait DynServerChannel: Send + Sync + Sealed {
    /// 返回协议标识。
    fn scheme_dyn(&self) -> &'static str;

    /// 返回监听绑定地址。
    ///
    /// # 教案级注释
    ///
    /// ## 意图（Why）
    /// - 保留 [`Context`] 参数用于未来扩展，例如根据调用方上下文追加审计标签；
    ///
    /// ## 逻辑（How）
    /// - 当前实现通常直接转调泛型层 [`ServerChannel::local_addr`]；
    /// - `ctx` 暂未被消费，仅用于保持接口对称性。
    ///
    /// ## 契约（What）
    /// - `ctx`：执行上下文；
    /// - 返回：监听器绑定的 [`TransportSocketAddr`]，失败时给出 [`CoreError`]。
    ///
    /// ## 考量（Trade-offs）
    /// - 统一签名避免对象层实现和泛型层出现语义偏差，后续扩展不会破坏接口稳定性。
    fn local_addr_dyn(&self, ctx: &Context<'_>) -> crate::Result<TransportSocketAddr, CoreError>;

    /// 接受新的入站连接。
    ///
    /// # 教案级注释
    ///
    /// ## 意图（Why）
    /// - 在对象安全语境下保留 [`CallContext`] 所携带的取消/截止约束；
    /// - 让宿主可以通过统一接口获取装箱后的 [`crate::pipeline::Channel`] 实例。
    ///
    /// ## 逻辑（How）
    /// - 调用泛型层 [`ServerChannel::accept`] 并将结果装箱；
    /// - 将对端地址一并返回，保证观测和路由信息完备。
    ///
    /// ## 契约（What）
    /// - `ctx`：[`CallContext`]，描述接受流程的取消与预算；
    /// - 返回：通道与对端地址。
    ///
    /// ## 考量（Trade-offs）
    /// - 若 `CallContext` 已取消，实现应快速返回错误，避免监听器阻塞。
    async fn accept_dyn(
        &self,
        ctx: &CallContext,
    ) -> crate::Result<(Box<dyn PipelineChannel>, TransportSocketAddr), CoreError>;

    /// 设置协议协商后的 PipelineInitializer 选择策略。
    ///
    /// # 教案级注释
    ///
    /// ## 意图（Why）
    /// - 补齐对象层对 Pipeline 装配策略的注入能力，保持与泛型层一致的扩展点；
    ///
    /// ## 契约（What）
    /// - `selector`：输入 [`HandshakeOutcome`]，输出 `Arc<dyn PipelineInitializer>` 的闭包；
    /// - **前置条件**：应在接受循环前完成设置；
    /// - **后置条件**：握手完成后会调用该闭包决定链路初始化器。
    fn set_initializer_selector_dyn(&self, selector: Arc<PipelineInitializerSelector>);

    /// 根据计划执行优雅关闭。
    ///
    /// # 教案级注释
    ///
    /// ## 意图（Why）
    /// - 继承调用方设置的取消/截止语义，保证对象层关闭行为与泛型层完全一致；
    ///
    /// ## 逻辑（How）
    /// - 透传 [`Context`] 与 [`ListenerShutdown`] 至泛型实现；
    ///
    /// ## 契约（What）
    /// - `ctx`：执行上下文；
    /// - `plan`：关闭计划；
    /// - 返回：遵循上下文约束的异步结果。
    ///
    /// ## 考量（Trade-offs）
    /// - 对象层无需重复解析上下文，减少重复逻辑。
    async fn shutdown_dyn(
        &self,
        ctx: &Context<'_>,
        plan: ListenerShutdown,
    ) -> crate::Result<(), CoreError>;
}

/// 将泛型监听器适配为对象层实现。
pub struct ServerChannelObject<T>
where
    T: for<'ctx> ServerChannel<
            Error = CoreError,
            AcceptCtx<'ctx> = CallContext,
            ShutdownCtx<'ctx> = Context<'ctx>,
        >,
    T::Connection: PipelineChannel + 'static,
{
    inner: Arc<T>,
}

impl<T> ServerChannelObject<T>
where
    T: for<'ctx> ServerChannel<
            Error = CoreError,
            AcceptCtx<'ctx> = CallContext,
            ShutdownCtx<'ctx> = Context<'ctx>,
        >,
    T::Connection: PipelineChannel + 'static,
{
    /// 构造新的对象层监听器包装器。
    pub fn new(inner: T) -> Self {
        Self {
            inner: Arc::new(inner),
        }
    }

    /// 取回内部泛型实现。
    pub fn into_inner(self) -> Arc<T> {
        self.inner
    }
}

#[async_trait]
impl<T> DynServerChannel for ServerChannelObject<T>
where
    T: for<'ctx> ServerChannel<
            Error = CoreError,
            AcceptCtx<'ctx> = CallContext,
            ShutdownCtx<'ctx> = Context<'ctx>,
        >,
    T::Connection: PipelineChannel + 'static,
{
    fn scheme_dyn(&self) -> &'static str {
        self.inner.scheme()
    }

    fn local_addr_dyn(&self, _ctx: &Context<'_>) -> crate::Result<TransportSocketAddr, CoreError> {
        self.inner.local_addr()
    }

    async fn accept_dyn(
        &self,
        ctx: &CallContext,
    ) -> crate::Result<(Box<dyn PipelineChannel>, TransportSocketAddr), CoreError> {
        let (connection, addr) = self.inner.accept(ctx).await?;
        Ok((Box::new(connection) as Box<dyn PipelineChannel>, addr))
    }

    fn set_initializer_selector_dyn(&self, selector: Arc<PipelineInitializerSelector>) {
        self.inner.set_initializer_selector(selector);
    }

    async fn shutdown_dyn(
        &self,
        ctx: &Context<'_>,
        plan: ListenerShutdown,
    ) -> crate::Result<(), CoreError> {
        self.inner.shutdown(ctx, plan).await
    }
}
