use crate::{
    TcpChannel, TcpSocketConfig,
    error::{self, map_io_error},
    util::{deadline_expired, monotonic_now, run_with_context, to_socket_addr},
};
use core::fmt;
use spark_core::error::codes;
use spark_core::prelude::{CallContext, Context, CoreError, TransportSocketAddr};
use spark_core::transport::TransportBuilder;
use spark_core::transport::{
    CapabilityBitmap, HandshakeOffer, HandshakeOutcome, ListenerShutdown,
    PipelineInitializerSelector, ServerChannel, Version, negotiate,
};
use std::boxed::Box;
use std::sync::{Arc, RwLock};
use std::{future::Future, pin::Pin, time::Duration};
use tokio::net::TcpListener as TokioTcpListener;

/// 对 Tokio `TcpListener` 的语义封装。
///
/// # 教案式注释
///
/// ## 意图 (Why)
/// - 在不暴露 Tokio 具体类型的前提下，提供“监听 → 接受连接”的最小能力，
///   让上层能够以 `spark-core` 的契约管理生命周期与错误分类。
/// - 对齐 `spark-core::transport::ServerChannel` 命名体系，将原 `TcpListener`
///   语义提升为通用 ServerChannel，让传输层与 Netty 等框架的 `ServerChannel`
///   概念保持一致。
/// - `accept` 会继承 [`CallContext`] 的取消与截止语义，避免监听线程阻塞。
///
/// ## 逻辑 (How)
/// - `bind`：将 [`TransportSocketAddr`] 转换为 `std::net::SocketAddr` 并调用
///   Tokio 绑定；
/// - `accept`：调用内部监听器的异步 `accept`，并通过
///   内部工具函数 `run_with_context` 注入取消/超时；
///   成功后将底层 `TcpStream` 包装为 [`TcpChannel`]；
/// - `local_addr`：返回监听器绑定地址的结构化表示。
///
/// ## 契约 (What)
/// - **前置条件**：调用方必须在 Tokio 运行时中使用该监听器；
/// - **后置条件**：`accept` 成功返回的 [`TcpChannel`] 已携带本地/对端地址，
///   并准备好进行读写；
/// - **错误语义**：绑定/接受失败时返回 [`CoreError`]，并附带稳定错误码与
///   [`ErrorCategory`](spark_core::error::ErrorCategory)。
///
/// ## 注意事项 (Trade-offs)
/// - 当前实现未支持 `SO_REUSEPORT` 等高级套接字选项，后续可在绑定前扩展；
/// - `accept` 在循环取消时依赖定时轮询，取消响应存在毫秒级延迟。
pub struct TcpServerChannel {
    inner: TokioTcpListener,
    local_addr: TransportSocketAddr,
    default_config: TcpSocketConfig,
    initializer_selector: RwLock<Option<Arc<PipelineInitializerSelector>>>,
}

impl fmt::Debug for TcpServerChannel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let has_selector = self
            .initializer_selector
            .read()
            .map(|opt| opt.is_some())
            .unwrap_or(false);
        f.debug_struct("TcpServerChannel")
            .field("local_addr", &self.local_addr)
            .field("has_initializer_selector", &has_selector)
            .finish()
    }
}

impl TcpServerChannel {
    /// 绑定到指定地址并返回监听器。
    pub async fn bind(addr: TransportSocketAddr) -> spark_core::Result<Self, CoreError> {
        Self::bind_with_config(addr, TcpSocketConfig::default()).await
    }

    /// 绑定到指定地址并设置默认的套接字配置。
    ///
    /// # 教案级注释
    ///
    /// ## 意图（Why）
    /// - 支持在监听阶段就指定新连接的默认 `TcpSocketConfig`（例如 `SO_LINGER`），保证服务端在大量连接场景下仍能保持一致的关闭策略；
    /// - 作为 Builder 模式的实际落地入口，让 `TcpServerChannelBuilder` 可以在构造时一次性注入介质特有配置。
    ///
    /// ## 契约（What）
    /// - `addr`：监听的传输地址；
    /// - `default_config`：后续 `accept` 默认应用的 [`TcpSocketConfig`]；
    /// - 返回：成功初始化的 [`TcpServerChannel`]；失败时返回结构化 [`CoreError`]，错误码与原 `bind` 保持一致；
    /// - **前置条件**：调用方需在 Tokio 运行时中执行；
    /// - **后置条件**：`TcpServerChannel::default_socket_config` 返回的配置即为传入值。
    ///
    /// ## 逻辑（How）
    /// - 将 `TransportSocketAddr` 转换为标准库地址，调用 Tokio `bind`；
    /// - 读取实际绑定地址并封装为 `TransportSocketAddr`；
    /// - 缓存 `default_config`，供后续 `accept` 克隆使用。
    pub async fn bind_with_config(
        addr: TransportSocketAddr,
        default_config: TcpSocketConfig,
    ) -> spark_core::Result<Self, CoreError> {
        let socket_addr = to_socket_addr(addr);
        let listener = TokioTcpListener::bind(socket_addr)
            .await
            .map_err(|err| map_io_error(error::BIND, err))?;
        let local = listener
            .local_addr()
            .map_err(|err| map_io_error(error::BIND, err))?;
        Ok(Self {
            inner: listener,
            local_addr: TransportSocketAddr::from(local),
            default_config,
            initializer_selector: RwLock::new(None),
        })
    }

    /// 返回监听器实际绑定的地址。
    pub fn local_addr(&self) -> TransportSocketAddr {
        self.local_addr
    }

    /// 读取监听器为后续 `accept` 预设的默认套接字配置。
    ///
    /// # 教案级注释
    ///
    /// ## 意图（Why）
    /// - 让运维或测试在运行时快速确认新连接将继承的 `TcpSocketConfig`，避免逐个检查内核选项；
    /// - 方便 Builder 与监听器之间共享状态：`TcpServerChannelBuilder` 设置的配置即可通过本方法验证。
    ///
    /// ## 契约（What）
    /// - 返回值为 [`TcpSocketConfig`] 的只读引用，反映 `bind` 阶段指定的默认值；
    /// - **前置条件**：监听器已通过 [`TcpServerChannel::bind`] 或 [`TcpServerChannel::bind_with_config`] 成功初始化；
    /// - **后置条件**：函数不会触发 I/O，也不会修改监听器内部状态。
    pub fn default_socket_config(&self) -> &TcpSocketConfig {
        &self.default_config
    }

    /// 接受一个入站连接，并根据上下文处理取消/超时。
    pub async fn accept(
        &self,
        ctx: &CallContext,
    ) -> spark_core::Result<(TcpChannel, TransportSocketAddr), CoreError> {
        self.accept_with_config(ctx, self.default_config.clone())
            .await
    }

    /// 接受一个入站连接，并根据上下文处理取消/超时，可指定套接字配置。
    ///
    /// # 教案级注释
    ///
    /// ## 意图（Why）
    /// - 允许服务端在接受阶段就决定连接的 `SO_LINGER` 策略，确保与客户端一致的
    ///   优雅关闭体验；
    /// - 避免宿主层重复操作 `TcpStream` 套接字选项。
    ///
    /// ## 契约（What）
    /// - `ctx`：控制取消/超时的 [`CallContext`]；
    /// - `config`：应用到新建通道的 [`TcpSocketConfig`]；
    /// - 返回 `(TcpChannel, TransportSocketAddr)`：通道及对端地址；
    /// - **前置条件**：监听器处于活跃状态且 `ctx` 未被取消；
    /// - **后置条件**：成功返回的通道已经写入 `config`，失败时保持监听器继续工作。
    ///
    /// ## 实现逻辑（How）
    /// - 复用 `run_with_context` 执行 `accept`，从而继承取消与截止语义；
    /// - 读取本地/对端地址后调用 `TcpChannel::from_parts` 并传入 `config`；
    /// - 套接字选项配置失败会被映射为 [`CoreError`] 返回给调用方。
    ///
    /// ## 注意事项（Trade-offs）
    /// - 若 `config` 设置的 linger 过短，仍可能导致未发送完的数据被 RST；
    /// - 当前实现逐个接受连接，若需批量或并发接受，可在上层引入任务池。
    pub async fn accept_with_config(
        &self,
        ctx: &CallContext,
        config: TcpSocketConfig,
    ) -> spark_core::Result<(TcpChannel, TransportSocketAddr), CoreError> {
        if deadline_expired(ctx.deadline()) {
            return Err(error::timeout_error(error::ACCEPT));
        }
        if ctx.cancellation().is_cancelled() {
            return Err(error::cancelled_error(error::ACCEPT));
        }

        let (stream, remote) = run_with_context(ctx, error::ACCEPT, self.inner.accept()).await?;
        let local_addr = stream
            .local_addr()
            .map_err(|err| map_io_error(error::ACCEPT, err))?;
        let peer_addr = TransportSocketAddr::from(remote);
        let channel = TcpChannel::from_parts(
            stream,
            TransportSocketAddr::from(local_addr),
            peer_addr,
            config,
        )?;

        let outcome = self.perform_handshake()?;

        let selector = self
            .selector_snapshot()
            .ok_or_else(error::pipeline_initializer_missing)?;
        let initializer = selector(&outcome)?;
        channel.bind_pipeline_initializer(outcome, initializer)?;

        Ok((channel, peer_addr))
    }
}

impl TcpServerChannel {
    /// 生成本地默认的握手宣告。
    ///
    /// # 教案级注释
    ///
    /// ## 意图（Why）
    /// - 在缺省 TCP 监听场景中尚未引入 ALPN/SNI 协商，因此使用稳定版本
    ///   与空能力集合代表“最小可用”协议族；
    /// - 作为后续 TLS/QUIC 扩展的兜底声明，避免协商阶段出现空指针。
    ///
    /// ## 契约（What）
    /// - 返回 [`HandshakeOffer`]，版本固定为 `1.0.0`，能力位图均为空；
    /// - **前置条件**：调用方无需准备额外上下文；
    /// - **后置条件**：返回值可直接传入 [`negotiate`]。
    fn default_handshake_offer(&self) -> HandshakeOffer {
        HandshakeOffer::new(
            Version::new(1, 0, 0),
            CapabilityBitmap::empty(),
            CapabilityBitmap::empty(),
        )
    }

    /// 执行一次占位握手并返回协商结果。
    ///
    /// # 教案级注释
    ///
    /// ## 意图（Why）
    /// - 将“监听器接受连接后立即完成协议协商”的职责收敛在传输层，
    ///   使上层 Router 不再关心 ALPN/SNI 细节；
    /// - 对齐任务 T-2.2 的目标：在 `accept` 流程中决定 L1 Pipeline 初始化策略。
    ///
    /// ## 契约（What）
    /// - 当前实现采用本地与远端相同的保守宣告，代表尚未暴露多协议特性；
    /// - 发生错误时返回 [`CoreError`]，错误码为 `protocol.negotiation`；
    /// - **前置条件**：监听器已处于运行状态；
    /// - **后置条件**：成功返回的结果可作为 [`PipelineInitializerSelector`] 的输入。
    fn perform_handshake(&self) -> spark_core::Result<HandshakeOutcome, CoreError> {
        let local_offer = self.default_handshake_offer();
        let remote_offer = self.default_handshake_offer();
        let occurred_at = monotonic_now().as_duration().as_micros() as u64;

        negotiate(&local_offer, &remote_offer, occurred_at, None).map_err(|error| {
            CoreError::new(
                codes::PROTOCOL_NEGOTIATION,
                format!("tcp handshake negotiation failed: {error}"),
            )
            .with_cause(error)
        })
    }

    /// 读取当前配置的初始化器选择器。
    ///
    /// # 教案级注释
    ///
    /// ## 意图（Why）
    /// - 将 `RwLock` 访问封装为独立函数，确保并发读取时保持最小临界区；
    /// - 允许监听器在协商失败或未配置策略时优雅退化。
    ///
    /// ## 契约（What）
    /// - 返回 `Option<Arc<PipelineInitializerSelector>>`；
    /// - 若锁被毒化，则视为未配置并返回 `None`；
    /// - **前置条件**：监听器已构造完成；
    /// - **后置条件**：返回的 `Arc` 可安全在调用方持有并在后续调用中复用。
    fn selector_snapshot(&self) -> Option<Arc<PipelineInitializerSelector>> {
        self.initializer_selector
            .read()
            .ok()
            .and_then(|guard| guard.clone())
    }
}

/// `TcpServerChannel` 的建造器，实现介质选项的下沉配置。
///
/// # 教案级注释
///
/// ## 意图（Why）
/// - 提供 Builder 模式以传递 `TcpSocketConfig` 等介质特有选项，确保宿主只依赖 `TransportBuilder` 抽象即可装配 TCP 监听器；
/// - 支持在启动前一次性设定默认的 `SO_LINGER` 或其他套接字行为，避免在业务层重复操作 `socket2`。
///
/// ## 契约（What）
/// - `new`：以监听地址创建 Builder，默认配置等同于 [`TcpSocketConfig::default`]；
/// - `with_default_socket_config`：显式替换默认配置；
/// - `with_linger`：提供常见的快捷设置；
/// - `build`：遵循 [`TransportBuilder`] 契约，若 [`Context`](spark_core::context::Context) 已取消则立即返回取消错误，否则调用 [`TcpServerChannel::bind_with_config`]。
///
/// ## 风险提示（Trade-offs）
/// - Builder 被消费后无法复用，若需不同配置请分别构建；
/// - 当前仅封装 `linger`，后续扩展需保持方法命名一致性，以便阅读者快速识别。
#[derive(Clone, Debug)]
pub struct TcpServerChannelBuilder {
    addr: TransportSocketAddr,
    default_config: TcpSocketConfig,
}

impl TcpServerChannelBuilder {
    /// 基于监听地址创建 Builder。
    pub fn new(addr: TransportSocketAddr) -> Self {
        Self {
            addr,
            default_config: TcpSocketConfig::default(),
        }
    }

    /// 覆盖默认的套接字配置。
    pub fn with_default_socket_config(mut self, config: TcpSocketConfig) -> Self {
        self.default_config = config;
        self
    }

    /// 便捷设置 `SO_LINGER`。
    pub fn with_linger(mut self, linger: Option<Duration>) -> Self {
        self.default_config = self.default_config.clone().with_linger(linger);
        self
    }
}

impl TransportBuilder for TcpServerChannelBuilder {
    type Output = TcpServerChannel;

    type BuildFuture<'ctx>
        = Pin<Box<dyn Future<Output = spark_core::Result<Self::Output, CoreError>> + Send + 'ctx>>
    where
        Self: 'ctx;

    fn scheme(&self) -> &'static str {
        "tcp"
    }

    fn build<'ctx>(self, ctx: &'ctx Context<'ctx>) -> Self::BuildFuture<'ctx> {
        let cancelled = ctx.cancellation().is_cancelled();
        let addr = self.addr;
        let config = self.default_config;
        Box::pin(async move {
            if cancelled {
                return Err(error::cancelled_error(error::BIND));
            }
            TcpServerChannel::bind_with_config(addr, config).await
        })
    }
}

impl ServerChannel for TcpServerChannel {
    type Error = CoreError;
    type AcceptCtx<'ctx> = CallContext;
    type ShutdownCtx<'ctx> = Context<'ctx>;
    type Connection = TcpChannel;

    type AcceptFuture<'ctx>
        = Pin<
        Box<
            dyn core::future::Future<
                    Output = spark_core::Result<(Self::Connection, TransportSocketAddr), CoreError>,
                > + Send
                + 'ctx,
        >,
    >
    where
        Self: 'ctx,
        Self::AcceptCtx<'ctx>: 'ctx;

    type ShutdownFuture<'ctx>
        =
        Pin<Box<dyn core::future::Future<Output = spark_core::Result<(), CoreError>> + Send + 'ctx>>
    where
        Self: 'ctx,
        Self::ShutdownCtx<'ctx>: 'ctx;

    fn scheme(&self) -> &'static str {
        "tcp"
    }

    fn local_addr(&self) -> spark_core::Result<TransportSocketAddr, CoreError> {
        Ok(TcpServerChannel::local_addr(self))
    }

    fn accept<'ctx>(&'ctx self, ctx: &'ctx Self::AcceptCtx<'ctx>) -> Self::AcceptFuture<'ctx> {
        Box::pin(async move { self.accept(ctx).await })
    }

    fn set_initializer_selector(&self, selector: Arc<PipelineInitializerSelector>) {
        if let Ok(mut guard) = self.initializer_selector.write() {
            *guard = Some(selector);
        }
    }

    fn shutdown<'ctx>(
        &'ctx self,
        _ctx: &'ctx Self::ShutdownCtx<'ctx>,
        _plan: ListenerShutdown,
    ) -> Self::ShutdownFuture<'ctx> {
        Box::pin(async move { Ok(()) })
    }
}

#[allow(dead_code)]
fn _assert_tcp_server_channel()
where
    TcpServerChannel: ServerChannel<Error = CoreError, Connection = TcpChannel>,
{
}

#[cfg(test)]
mod tests {
    use super::*;
    use spark_core::transport::Version;
    use spark_core::{contract::CallContext, transport::TransportSocketAddr};
    use std::net::SocketAddr;
    use std::sync::Arc;
    use tokio::join;

    use spark_core::pipeline::{ChainBuilder, Channel, InitializerDescriptor, PipelineInitializer};
    use spark_core::platform::runtime::CoreServices;

    /// 验证 `TcpServerChannelBuilder` 能够将自定义的 `linger` 配置写入监听器默认配置。
    #[tokio::test(flavor = "multi_thread")]
    async fn builder_applies_default_config() {
        let addr: SocketAddr = "127.0.0.1:0".parse().expect("parse addr");
        let linger = Some(Duration::from_secs(2));
        let listener = TcpServerChannelBuilder::new(TransportSocketAddr::from(addr))
            .with_linger(linger)
            .build(&CallContext::builder().build().execution())
            .await
            .expect("build listener");

        assert_eq!(listener.default_socket_config().linger(), linger);
    }

    #[derive(Clone, Default)]
    struct TestInitializer {
        label: &'static str,
    }

    impl TestInitializer {
        const fn new(label: &'static str) -> Self {
            Self { label }
        }
    }

    impl PipelineInitializer for TestInitializer {
        fn descriptor(&self) -> InitializerDescriptor {
            InitializerDescriptor::new(self.label, "tests", "tcp listener pipeline binding")
        }

        fn configure(
            &self,
            _chain: &mut dyn ChainBuilder,
            _channel: &dyn Channel,
            _services: &CoreServices,
        ) -> spark_core::Result<(), CoreError> {
            Ok(())
        }
    }

    /// 验证未配置初始化器选择器时，监听器会返回结构化错误避免继续处理。
    #[tokio::test(flavor = "multi_thread")]
    async fn accept_without_selector_returns_error() {
        let addr: SocketAddr = "127.0.0.1:0".parse().expect("parse addr");
        let listener = TcpServerChannel::bind(TransportSocketAddr::from(addr))
            .await
            .expect("bind listener");
        let call_ctx = CallContext::builder().build();
        let socket_addr: SocketAddr = listener.local_addr().into();

        let (client, outcome) = join!(
            tokio::net::TcpStream::connect(socket_addr),
            listener.accept(&call_ctx)
        );
        let _client_stream = client.expect("connect client");
        let error = outcome.expect_err("missing selector should error");

        assert_eq!(error.code(), error::PIPELINE_MISSING.code);
    }

    /// 验证监听器在握手后会将初始化器与握手结果绑定到通道上。
    #[tokio::test(flavor = "multi_thread")]
    async fn accept_binds_pipeline_metadata() {
        let addr: SocketAddr = "127.0.0.1:0".parse().expect("parse addr");
        let listener = TcpServerChannel::bind(TransportSocketAddr::from(addr))
            .await
            .expect("bind listener");
        let call_ctx = CallContext::builder().build();
        let initializer: Arc<dyn PipelineInitializer> =
            Arc::new(TestInitializer::new("tcp.tests.listener"));
        let expected = Arc::clone(&initializer);
        listener.set_initializer_selector(Arc::new(move |_outcome| Ok(Arc::clone(&initializer))));

        let socket_addr: SocketAddr = listener.local_addr().into();
        let (client, accepted) = join!(
            tokio::net::TcpStream::connect(socket_addr),
            listener.accept(&call_ctx)
        );
        let _client_stream = client.expect("connect client");
        let (channel, _peer_addr) = accepted.expect("selector configured");

        let handshake = channel
            .handshake_outcome()
            .expect("handshake outcome stored");
        assert_eq!(handshake.version(), Version::new(1, 0, 0));
        assert!(handshake.downgrade().is_lossless());

        let stored_initializer = channel
            .pipeline_initializer()
            .expect("initializer stored on channel");
        assert!(Arc::ptr_eq(&stored_initializer, &expected));
    }

    /// 验证当选择器返回错误时，监听器会透传该错误并停止装配流程。
    #[tokio::test(flavor = "multi_thread")]
    async fn accept_propagates_selector_error() {
        let addr: SocketAddr = "127.0.0.1:0".parse().expect("parse addr");
        let listener = TcpServerChannel::bind(TransportSocketAddr::from(addr))
            .await
            .expect("bind listener");
        let call_ctx = CallContext::builder().build();

        listener.set_initializer_selector(Arc::new(move |_outcome| {
            Err(error::pipeline_attach_conflict())
        }));

        let socket_addr: SocketAddr = listener.local_addr().into();
        let (client, accepted) = join!(
            tokio::net::TcpStream::connect(socket_addr),
            listener.accept(&call_ctx)
        );
        let _client_stream = client.expect("connect client");
        let error = accepted.expect_err("selector returning error should fail");

        assert_eq!(error.code(), error::PIPELINE_ATTACH.code);
    }
}
