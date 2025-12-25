use std::sync::Arc;

use arc_swap::ArcSwap;
use rustls::ServerConfig;
use spark_core::{CoreError, context::Context, prelude::CallContext, transport::TransportBuilder};
use spark_transport_tcp::TcpChannel;
use std::{future::Future, pin::Pin};
use tokio_rustls::TlsAcceptor as TokioTlsAcceptor;

use crate::{channel::TlsChannel, error, util::run_with_context};

/// TLS 服务端握手入口。
///
/// # 教案级注释
///
/// ## 意图（Why）
/// - 在 TCP 层接受连接后，执行 TLS1.3 握手并生成 [`TlsChannel`]，为上层提供加密读写能力；
/// - 通过 [`ArcSwap`] 支持热更新 [`ServerConfig`]，满足证书轮换与策略切换需求；
/// - 把握错误分类：握手失败时根据具体原因映射为 `Security`/`ResourceExhausted`/`Retryable`。
///
/// ## 逻辑（How）
/// 1. 调用 [`TcpChannel::try_into_parts`] 获取原始 `TcpStream` 与地址信息；
/// 2. 读取当前配置并构造 `tokio_rustls::TlsAcceptor`；
/// 3. 借助 `run_with_context` 注入取消/截止语义，执行异步握手；
/// 4. 将握手结果包装为 [`TlsChannel`]，同时缓存协商得到的 SNI 与 ALPN。
///
/// ## 契约（What）
/// - `accept`：在成功时返回可用的 [`TlsChannel`]；若上下文取消或握手失败，返回结构化 `CoreError`；
/// - `replace_config`：原子替换 TLS 配置；
/// - `config_snapshot`：获取当前配置的 `Arc` 副本，方便上层调试或指标采集。
///
/// ## 风险与权衡（Trade-offs）
/// - 当调用方持有 `TcpChannel` 的多个克隆时，无法拆解原始套接字，会返回资源耗尽错误；
/// - 握手流程依赖 `run_with_context` 的轮询取消，可能存在毫秒级响应延迟，但换取了实现简单性；
/// - `ArcSwap` 替换配置是无锁操作，但调用方需保证新配置中的证书链/密钥有效，否则握手将以
///   `Security` 类错误失败。
#[derive(Clone, Debug)]
pub struct TlsAcceptor {
    config: Arc<ArcSwap<ServerConfig>>,
}

impl TlsAcceptor {
    /// 使用初始配置创建握手器。
    pub fn new(config: Arc<ServerConfig>) -> Self {
        Self {
            config: Arc::new(ArcSwap::new(config)),
        }
    }

    /// 替换当前 TLS 配置，通常用于证书热更新。
    pub fn replace_config(&self, config: Arc<ServerConfig>) {
        self.config.store(config);
    }

    /// 获取当前配置的快照。
    pub fn config_snapshot(&self) -> Arc<ServerConfig> {
        self.config.load_full()
    }

    /// 对单个 TCP 连接执行 TLS 握手。
    pub async fn accept(
        &self,
        ctx: &CallContext,
        channel: TcpChannel,
    ) -> spark_core::Result<TlsChannel, CoreError> {
        let parts = channel
            .try_into_parts()
            .map_err(|_| error::exclusive_channel_error())?;
        let acceptor = TokioTlsAcceptor::from(self.config.load_full());
        let stream = run_with_context(
            ctx,
            error::HANDSHAKE,
            acceptor.accept(parts.stream),
            error::map_handshake_error,
        )
        .await?;
        Ok(TlsChannel::new(stream, parts.local_addr, parts.peer_addr))
    }
}

/// `TlsAcceptor` 的建造器，支持在 Builder 模式下注入初始配置。
pub struct TlsAcceptorBuilder {
    config: Arc<ServerConfig>,
}

impl TlsAcceptorBuilder {
    /// 使用 `Arc<ServerConfig>` 构建 Builder。
    pub fn new(config: Arc<ServerConfig>) -> Self {
        Self { config }
    }
}

impl TransportBuilder for TlsAcceptorBuilder {
    type Output = TlsAcceptor;

    type BuildFuture<'ctx>
        = Pin<Box<dyn Future<Output = spark_core::Result<Self::Output, CoreError>> + Send + 'ctx>>
    where
        Self: 'ctx;

    fn scheme(&self) -> &'static str {
        "tls"
    }

    fn build<'ctx>(self, _ctx: &'ctx Context<'ctx>) -> Self::BuildFuture<'ctx> {
        let config = self.config;
        Box::pin(async move { Ok(TlsAcceptor::new(config)) })
    }
}
