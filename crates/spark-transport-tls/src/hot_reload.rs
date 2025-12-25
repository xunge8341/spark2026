use std::fmt;
use std::sync::Arc;

use arc_swap::ArcSwap;
use rustls::ServerConfig;
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_rustls::{TlsAcceptor, server::TlsStream};

/// TLS 服务端配置的热更新容器。
///
/// # 设计动机（Why）
/// - **零中断目标**：针对“证书热更”需求，封装 `ArcSwap<ServerConfig>`，确保监听线程在更新配置时无需暂停或重建监听器；
/// - **架构角色**：位于传输实现层与证书管理子系统之间，向服务器握手流程提供“随取随用”的 TLS 配置快照；
/// - **模式选择**：利用 `ArcSwap` 的“读无锁、写常数时间”特性，实现经典的 RCU（Read-Copy-Update）式配置广播。
///
/// # 核心契约（What）
/// - **持有状态**：内部保存 `Arc<ServerConfig>`，保证调用方获取到的配置可直接交给 `tokio-rustls::TlsAcceptor` 使用；
/// - **前置条件**：写入的 `ServerConfig` 必须已完成证书链、私钥以及会话存储等初始化；调用方负责保障其线程安全；
/// - **后置条件**：`accept` 每次握手都会读取一次最新配置，新旧连接互不影响；`replace` 立即对后续 `accept` 调用可见。
///
/// # 实现逻辑（How）
/// - 构造时创建 `ArcSwap<ServerConfig>`，写入初始配置；
/// - `accept` 在握手前获取 `Arc<ServerConfig>` 快照，生成临时 `TlsAcceptor` 完成异步握手；
/// - 旧连接因持有旧 `Arc<ServerConfig>` 引用而继续有效，新连接读取的则是最新快照，实现热更新语义。
///
/// # 风险提示（Trade-offs & Gotchas）
/// - **写入成本**：虽然写入为常数时间，但仍需完整构造 `ServerConfig`；若仅替换证书链，可复用已有配置并在外部提前拼装；
/// - **内存峰值**：高频更新时会暂存多个 `Arc<ServerConfig>`，需结合握手并发度评估内存峰值；
/// - **错误传播**：握手失败会以 [`TlsHandshakeError`] 返回，调用方应结合遥测记录原因。
#[derive(Clone)]
pub struct HotReloadingServerConfig {
    /// 共享的配置存储。
    ///
    /// - **设计考量**：使用 `Arc<ArcSwap<ServerConfig>>` 允许监听线程与工作线程共享同一热更容器；
    /// - **同步语义**：读路径全程无锁，写路径通过 `swap` 提供原子替换；
    /// - **生命周期**：调用方可任意克隆本结构体，无需额外的生命周期管理代码。
    inner: Arc<ArcSwap<ServerConfig>>,
}

impl HotReloadingServerConfig {
    /// 基于已有的 `Arc<ServerConfig>` 构造热更容器。
    ///
    /// # 意图（Why）
    /// - 在证书初始化阶段通常已经产出 `Arc<ServerConfig>`，此方法避免额外的 `Arc` 克隆或重新包装。
    ///
    /// # 契约（What）
    /// - **参数**：`initial` 为初始的 TLS 服务端配置，必须保证其内部状态可在多线程下安全共享；
    /// - **后置条件**：内部 `ArcSwap` 将保存 `initial` 的所有权拷贝，后续读取都能获取同一快照。
    ///
    /// # 实现（How）
    /// - 直接调用 `ArcSwap::new(initial)` 创建交换容器，再封装至 `Arc` 便于克隆分发。
    pub fn new(initial: Arc<ServerConfig>) -> Self {
        Self {
            inner: Arc::new(ArcSwap::new(initial)),
        }
    }

    /// 以值语义构造热更容器，适用于尚未封装为 `Arc` 的配置。
    ///
    /// # 契约（What）
    /// - **参数**：`initial` 为完整所有权；
    /// - **后置条件**：内部会自动封装成 `Arc`，语义与 [`Self::new`] 等价。
    #[inline]
    pub fn from_config(initial: ServerConfig) -> Self {
        Self::new(Arc::new(initial))
    }

    /// 获取当前配置的共享快照。
    ///
    /// # 契约（What）
    /// - **返回值**：返回一个 `Arc<ServerConfig>`，可安全跨线程使用且与内部状态一致；
    /// - **后置条件**：调用方持有的快照不会随后续热更而失效，但也不会感知新的配置。
    ///
    /// # 实现（How）
    /// - 调用 `ArcSwap::load_full` 获取 `Arc` 克隆，属于零拷贝操作，仅增加引用计数。
    #[inline]
    pub fn snapshot(&self) -> Arc<ServerConfig> {
        self.inner.load_full()
    }

    /// 用新的配置替换当前快照，并返回旧值。
    ///
    /// # 契约（What）
    /// - **参数**：`next` 为准备好的新配置；
    /// - **返回值**：返回被替换下来的旧配置，便于调用方回收或用于审计；
    /// - **前置条件**：调用方需确保 `next` 已完成证书链与私钥的装载。
    ///
    /// # 实现（How）
    /// - 调用 `ArcSwap::swap` 原子地替换内部指针，保证并发读者立即可见新值；
    /// - 原指针随返回值一起交给调用方，避免无意的立即销毁。
    #[inline]
    pub fn replace(&self, next: Arc<ServerConfig>) -> Arc<ServerConfig> {
        self.inner.swap(next)
    }

    /// 使用当前配置受理 TLS 握手，并返回加密后的流。
    ///
    /// # 契约（What）
    /// - **参数**：`stream` 为底层传输流，需实现 `AsyncRead + AsyncWrite + Unpin`；
    /// - **返回值**：握手成功则返回 `TlsStream`，失败时以 [`TlsHandshakeError`] 描述原因；
    /// - **前置条件**：调用方需确保底层流代表刚接受的入站连接，且尚未被其它任务读取。
    /// - **后置条件**：握手成功后，新连接独占 `TlsStream`，可在任意任务内继续读写。
    ///
    /// # 实现（How）
    /// - 从 `ArcSwap` 读取当前配置快照；
    /// - 基于快照构造 `TlsAcceptor` 并执行异步握手；
    /// - 返回 `tokio-rustls` 提供的 `TlsStream`，保持与现有 TCP/QUIC 适配器一致的抽象。
    ///
    /// # 风险提示（Trade-offs & Gotchas）
    /// - 握手期间若底层连接中断，`tokio-rustls` 会返回 `io::Error`，上层需结合遥测判断是否触发熔断或重试；
    /// - 若配置包含需要外部资源的会话存储（如缓存引用），应确保其生命周期覆盖所有旧连接。
    pub async fn accept<IO>(
        &self,
        stream: IO,
    ) -> spark_core::Result<TlsStream<IO>, TlsHandshakeError>
    where
        IO: AsyncRead + AsyncWrite + Unpin,
    {
        // 读取当前配置快照：新连接始终获取最新配置，而旧连接继续持有各自的 `Arc`。
        let config = self.inner.load_full();
        let acceptor = TlsAcceptor::from(config);
        // 执行异步握手，错误由 `TlsHandshakeError` 统一描述，便于上层聚合日志。
        acceptor
            .accept(stream)
            .await
            .map_err(TlsHandshakeError::from)
    }
}

impl fmt::Debug for HotReloadingServerConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HotReloadingServerConfig")
            .finish_non_exhaustive()
    }
}

/// TLS 握手阶段的错误类型。
///
/// # 意图（Why）
/// - 将 `tokio-rustls` 返回的 `io::Error` 封装为领域内错误，便于上层统一处理与打点。
///
/// # 契约（What）
/// - 目前仅包含 `Handshake` 变体，未来可扩展证书加载或配置验证阶段的错误；
/// - `source` 保存底层 `io::Error`，供调用方进一步判定错误码。
#[derive(Debug, Error)]
pub enum TlsHandshakeError {
    /// 握手过程中发生 IO 或 TLS 协商错误。
    #[error("TLS 握手失败: {source}")]
    Handshake {
        /// 底层 IO 错误，通常由网络中断或证书校验失败触发。
        source: std::io::Error,
    },
}

impl From<std::io::Error> for TlsHandshakeError {
    fn from(source: std::io::Error) -> Self {
        TlsHandshakeError::Handshake { source }
    }
}
