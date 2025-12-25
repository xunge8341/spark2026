#![doc = r#"
# spark-transport-udp

## 模块使命（Why）
- **统一 UDP 通路**：为 Spark 运行时提供一套围绕 Tokio `UdpSocket` 的轻量封装，使上层能够通过 `spark-core` 的抽象以一致方式访问无连接传输能力。
- **SIP 互操作诉求**：面向 SIP 协议的 `rport` 语义进行解析与回写，保证经由 NAT 的终端也能可靠接收响应。
- **NAT Keepalive 基石**：暴露回源路由（`UdpReturnRoute`）描述，使业务侧能够基于最近一次报文持续发送心跳维持 NAT 映射。

## 核心契约（What）
- `UdpEndpoint` 负责套接字生命周期管理，并提供 `recv_from`/`send_to` 的异步接口。
- `UdpReturnRoute` 用于描述回应路径及是否需要在 SIP `Via` 头中填充 `rport`。
- 约束：调用方必须运行在 Tokio 多线程运行时，且所有报文按 UTF-8 解析 SIP 头（遇到非法 UTF-8 将优雅退化为原样转发）。

## 实现策略（How）
- 绑定及收发直接委托给 Tokio `UdpSocket`，并通过 `TransportSocketAddr` 与 `std::net::SocketAddr` 互转保持与 `spark-core` 协调。
- `recv_from` 在读取后解析首个 `Via` 头提取 `rport`，生成回源路由；`send_to` 在必要时重写 `rport` 并把报文发送至 NAT 可达端口。
- 解析与改写均采用纯字节扫描，以避免正则表达式开销，并明确记录大小写不敏感匹配逻辑，兼顾性能与可读性。
"#]
#![cfg_attr(
    not(feature = "runtime-tokio"),
    doc = r#"## 功能开关：`runtime-tokio`
- **默认状态**：当禁用该特性时，本 crate 仅导出类型定义与文档，便于在 `no_std` 或纯配置场景编译。
- **启用条件**：依赖方需在 `Cargo.toml` 中打开 `features = [\"runtime-tokio\"]`，并在运行期初始化 Tokio 多线程运行时。
- **风险提示**：若在未启用特性的前提下调用网络 API，将触发编译错误而非运行期 panic。
"#
)]

#[cfg(feature = "runtime-tokio")]
pub mod batch;

#[cfg(feature = "runtime-tokio")]
mod runtime_impl {
    use std::{
        borrow::Cow,
        io,
        net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
        pin::Pin,
        time::Duration,
    };

    use spark_core::transport::{DatagramEndpoint, TransportSocketAddr};
    use spark_core::{
        CoreError, context::Context, error::ErrorCategory, status::RetryAdvice,
        transport::TransportBuilder,
    };
    use thiserror::Error;
    use tokio::net::UdpSocket;

    /// 表示 UDP 传输层在处理 SIP 报文时的 `rport` 状态。
    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    pub enum SipViaRportDisposition {
        Absent,
        Advertised(u16),
        Requested,
    }

    impl SipViaRportDisposition {
        /// 判断该 `rport` 状态是否要求服务端在响应中主动改写端口。
        pub(crate) fn requires_rewrite(self) -> bool {
            matches!(self, SipViaRportDisposition::Requested)
        }
    }

    /// 描述一次 UDP 收包后可用的回源路径信息。
    ///
    /// # Why
    /// - **角色定位**：承载 NAT Keepalive 与响应发送所需的最小信息，避免业务层重复解析 `rport`。
    ///
    /// # What
    /// - `target`：下一次发送响应应使用的真实 `SocketAddr`。
    /// - `sip_rport`：记录请求中的 `rport` 状态，供发送端决策是否改写。
    ///
    /// # How
    /// - 由 [`UdpEndpoint::recv_from`] 根据源地址与 `Via` 头解析填充。
    #[derive(Clone, Debug)]
    pub struct UdpReturnRoute {
        target: SocketAddr,
        sip_rport: SipViaRportDisposition,
    }

    #[allow(dead_code)]
    impl UdpReturnRoute {
        /// 构造回源路径。
        fn new(target: SocketAddr, sip_rport: SipViaRportDisposition) -> Self {
            Self { target, sip_rport }
        }

        /// 提供回源目标地址。
        pub fn target(&self) -> SocketAddr {
            self.target
        }

        /// 暴露 `rport` 解析结果，便于业务逻辑进行策略扩展。
        pub fn sip_rport(&self) -> SipViaRportDisposition {
            self.sip_rport
        }

        /// 判断响应是否需要对 `Via` 头进行 `rport` 改写。
        fn requires_rewrite(&self) -> bool {
            self.sip_rport.requires_rewrite()
        }
    }

    /// UDP 套接字的可选参数集合。
    ///
    /// # 教案级注释
    ///
    /// ## 意图（Why）
    /// - 将 `SO_BROADCAST`、`SO_REUSEADDR` 等介质特有配置显式建模，避免宿主层散布字符串参数；
    /// - 为 Builder 模式提供可组合的数据结构，便于未来继续扩展更多 UDP 选项。
    ///
    /// ## 契约（What）
    /// - `broadcast`：是否允许广播；
    /// - `multicast_loop_v4`：是否开启 IPv4 组播回环；
    /// - `apply`：在绑定后将配置应用到 `UdpSocket`。
    #[derive(Clone, Debug, Default)]
    pub struct UdpSocketOptions {
        broadcast: bool,
        multicast_loop_v4: bool,
    }

    impl UdpSocketOptions {
        /// 启用或关闭广播。
        pub fn with_broadcast(mut self, enabled: bool) -> Self {
            self.broadcast = enabled;
            self
        }

        /// 控制 IPv4 组播回环。
        pub fn with_multicast_loop_v4(mut self, enabled: bool) -> Self {
            self.multicast_loop_v4 = enabled;
            self
        }

        /// 当前是否启用广播。
        pub fn broadcast(&self) -> bool {
            self.broadcast
        }

        /// 当前是否开启 IPv4 组播回环。
        pub fn multicast_loop_v4(&self) -> bool {
            self.multicast_loop_v4
        }

        /// 将配置应用到实际套接字。
        fn apply(&self, sock: &UdpSocket) -> io::Result<()> {
            sock.set_broadcast(self.broadcast)?;
            sock.set_multicast_loop_v4(self.multicast_loop_v4)?;
            Ok(())
        }
    }

    /// `UdpEndpoint` 封装了 Tokio `UdpSocket`，提供教案级注释的收发逻辑。
    ///
    /// # Why
    /// - **架构位置**：位于传输实现层，向上满足 `spark-core` 对无连接传输的期待。
    /// - **功能目标**：屏蔽地址转换细节，补齐 SIP NAT 场景的 `rport` 能力。
    ///
    /// # What
    /// - `sock`：底层 Tokio 套接字，负责实际 IO。
    ///
    /// # How
    /// - `bind`：创建并绑定套接字。
    /// - `recv_from`：读取报文、解析 `rport`，返回 [`UdpIncoming`]。
    /// - `send_to`：按需改写 `rport` 后发送报文。
    pub struct UdpEndpoint {
        sock: UdpSocket,
        options: UdpSocketOptions,
    }

    impl UdpEndpoint {
        /// 绑定到指定传输地址。
        ///
        /// # 参数契约（What）
        /// - `addr`：`TransportSocketAddr`，允许传入 IPv4/IPv6；端口可为 `0` 表示自动分配。
        ///
        /// # 前置条件（Preconditions）
        /// - Tokio 多线程运行时已启动；否则 `bind` 会 panic。
        ///
        /// # 后置条件（Postconditions）
        /// - 返回的 [`UdpEndpoint`] 拥有独占的 `UdpSocket`；若端口为 0，则绑定到系统分配端口。
        ///
        /// # 错误处理
        /// - 将底层 `std::io::Error` 封装为 [`UdpError::Bind`]，并包含原始地址字符串，便于排障。
        pub async fn bind(addr: TransportSocketAddr) -> spark_core::Result<Self, UdpError> {
            Self::bind_with_options(addr, UdpSocketOptions::default()).await
        }

        /// 带可选参数的绑定入口。
        pub async fn bind_with_options(
            addr: TransportSocketAddr,
            options: UdpSocketOptions,
        ) -> spark_core::Result<Self, UdpError> {
            let display = addr.to_string();
            let std_addr = transport_to_std(addr);
            let sock = UdpSocket::bind(std_addr)
                .await
                .map_err(|source| UdpError::Bind {
                    addr: display.clone(),
                    source,
                })?;
            options.apply(&sock).map_err(|source| UdpError::Bind {
                addr: display,
                source,
            })?;
            Ok(Self { sock, options })
        }

        /// 查询监听地址。
        ///
        /// # 设计说明
        /// - `spark-core` 使用 `TransportSocketAddr` 作为统一表示，因此此处转换回去。
        ///
        /// # 返回
        /// - 成功时返回当前套接字绑定地址。
        /// - 失败时将底层错误包装为 [`UdpError::LocalAddr`]。
        pub fn local_addr(&self) -> spark_core::Result<TransportSocketAddr, UdpError> {
            let addr = self.sock.local_addr().map_err(UdpError::LocalAddr)?;
            Ok(addr.into())
        }

        /// 返回绑定时使用的套接字选项，便于测试与运维核对。
        pub fn options(&self) -> &UdpSocketOptions {
            &self.options
        }

        /// 接收 UDP 报文并返回解析结果。
        ///
        /// # 参数
        /// - `buffer`：外部提供的可写字节切片，用于承载报文数据。
        ///
        /// # 前置条件
        /// - 调用方需确保缓冲区容量足够，否则报文将被截断（Tokio 行为）。
        ///
        /// # 返回值
        /// - `UdpIncoming`：包含实际读取长度、对端地址、回源路由信息。
        ///
        /// # 核心逻辑（How）
        /// 1. 使用 `recv_from` 读取报文，得到源地址及长度。
        /// 2. 调用 `parse_sip_rport` 解析 `Via` 头的 `rport` 状态。
        /// 3. 根据解析结果决定回源端口（若未提供则使用源端口）。
        /// 4. 返回封装后的 [`UdpIncoming`]。
        pub async fn recv_from(
            &self,
            buffer: &mut [u8],
        ) -> spark_core::Result<UdpIncoming, UdpError> {
            let (len, peer) = self
                .sock
                .recv_from(buffer)
                .await
                .map_err(UdpError::Receive)?;
            let sip_rport = parse_sip_rport(&buffer[..len]);
            let response_port = match sip_rport {
                SipViaRportDisposition::Advertised(port) => port,
                _ => peer.port(),
            };
            let target = SocketAddr::new(peer.ip(), response_port);
            Ok(UdpIncoming {
                len,
                peer,
                return_route: UdpReturnRoute::new(target, sip_rport),
            })
        }

        /// 发送 UDP 报文。
        ///
        /// # 参数
        /// - `payload`：待发送的原始字节片；函数内部不会修改调用方缓冲。
        /// - `route`：响应回源路径；若 `rport` 需要回写，将基于其中记录的端口改写 `Via` 头。
        ///
        /// # 核心逻辑
        /// - 当 `route.requires_rewrite()` 为真时，调用内部的 `rewrite_sip_rport` 生成新的字节序列。
        /// - 若解析失败或不是 SIP 报文，则退化为原样发送，保证健壮性。
        ///
        /// # 返回值
        /// - 成功返回写入字节数；失败则封装为 [`UdpError::Send`]。
        pub async fn send_to(
            &self,
            payload: &[u8],
            route: &UdpReturnRoute,
        ) -> spark_core::Result<usize, UdpError> {
            let to_send: Cow<'_, [u8]> = if route.requires_rewrite() {
                match rewrite_sip_rport(payload, route.target().port()) {
                    Some(updated) => Cow::Owned(updated),
                    None => Cow::Borrowed(payload),
                }
            } else {
                Cow::Borrowed(payload)
            };

            self.sock
                .send_to(&to_send, route.target())
                .await
                .map_err(UdpError::Send)
        }
    }

    /// `UdpEndpoint` 的建造器，负责下沉介质选项。
    #[derive(Clone, Debug)]
    pub struct UdpEndpointBuilder {
        addr: TransportSocketAddr,
        options: UdpSocketOptions,
    }

    impl UdpEndpointBuilder {
        /// 基于绑定地址创建 Builder。
        pub fn new(addr: TransportSocketAddr) -> Self {
            Self {
                addr,
                options: UdpSocketOptions::default(),
            }
        }

        /// 覆盖完整的套接字配置。
        pub fn with_options(mut self, options: UdpSocketOptions) -> Self {
            self.options = options;
            self
        }

        /// 启用或关闭广播。
        pub fn with_broadcast(mut self, enabled: bool) -> Self {
            self.options = self.options.clone().with_broadcast(enabled);
            self
        }

        /// 配置 IPv4 组播回环。
        pub fn with_multicast_loop_v4(mut self, enabled: bool) -> Self {
            self.options = self.options.clone().with_multicast_loop_v4(enabled);
            self
        }
    }

    impl TransportBuilder for UdpEndpointBuilder {
        type Output = UdpEndpoint;

        type BuildFuture<'ctx>
            =
            Pin<Box<dyn Future<Output = spark_core::Result<Self::Output, CoreError>> + Send + 'ctx>>
        where
            Self: 'ctx;

        fn scheme(&self) -> &'static str {
            "udp"
        }

        fn build<'ctx>(self, ctx: &'ctx Context<'ctx>) -> Self::BuildFuture<'ctx> {
            let cancelled = ctx.cancellation().is_cancelled();
            let addr = self.addr;
            let options = self.options;
            Box::pin(async move {
                if cancelled {
                    return Err(udp_cancelled_error());
                }
                match UdpEndpoint::bind_with_options(addr, options).await {
                    Ok(endpoint) => Ok(endpoint),
                    Err(err) => Err(map_udp_bind_error(err)),
                }
            })
        }
    }

    fn udp_cancelled_error() -> CoreError {
        CoreError::new(
            "spark.transport.udp.cancelled",
            "udp bind cancelled by caller",
        )
        .with_category(ErrorCategory::Cancelled)
    }

    fn map_udp_bind_error(error: UdpError) -> CoreError {
        match error {
            UdpError::Bind { addr, source } => CoreError::new(
                "spark.transport.udp.bind_failed",
                format!("udp bind {addr}: {source}"),
            )
            .with_category(ErrorCategory::Retryable(RetryAdvice::after(
                Duration::from_millis(50),
            ))),
            other => CoreError::new(
                "spark.transport.udp.bind_failed",
                format!("udp bind error: {other}"),
            )
            .with_category(ErrorCategory::NonRetryable),
        }
    }

    impl DatagramEndpoint for UdpEndpoint {
        type Error = UdpError;
        type CallCtx<'ctx> = ();
        type InboundMeta = UdpIncoming;
        type OutboundMeta = UdpReturnRoute;

        type RecvFuture<'ctx>
            = Pin<
            Box<
                dyn core::future::Future<
                        Output = spark_core::Result<(usize, UdpIncoming), UdpError>,
                    > + Send
                    + 'ctx,
            >,
        >
        where
            Self: 'ctx,
            Self::CallCtx<'ctx>: 'ctx;

        type SendFuture<'ctx>
            = Pin<
            Box<
                dyn core::future::Future<Output = spark_core::Result<usize, UdpError>>
                    + Send
                    + 'ctx,
            >,
        >
        where
            Self: 'ctx,
            Self::CallCtx<'ctx>: 'ctx;

        fn local_addr(&self) -> spark_core::Result<TransportSocketAddr, UdpError> {
            UdpEndpoint::local_addr(self)
        }

        fn recv<'ctx>(
            &'ctx self,
            _ctx: &'ctx Self::CallCtx<'ctx>,
            buf: &'ctx mut [u8],
        ) -> Self::RecvFuture<'ctx> {
            Box::pin(async move {
                let incoming = self.recv_from(buf).await?;
                let len = incoming.len;
                Ok((len, incoming))
            })
        }

        fn send<'ctx>(
            &'ctx self,
            _ctx: &'ctx Self::CallCtx<'ctx>,
            payload: &'ctx [u8],
            meta: &'ctx Self::OutboundMeta,
        ) -> Self::SendFuture<'ctx> {
            Box::pin(async move { self.send_to(payload, meta).await })
        }
    }

    #[allow(dead_code)]
    fn _assert_udp_datagram_endpoint()
    where
        UdpEndpoint: DatagramEndpoint<Error = UdpError>,
    {
    }

    /// UDP 接收结果描述。
    ///
    /// # 字段说明
    /// - `len`：实际读取的字节数。
    /// - `peer`：原始报文的来源地址，用于诊断与日志。
    /// - `return_route`：响应时推荐使用的路径。
    #[derive(Debug)]
    pub struct UdpIncoming {
        len: usize,
        peer: SocketAddr,
        return_route: UdpReturnRoute,
    }

    impl UdpIncoming {
        /// 报文有效负载长度。
        pub fn len(&self) -> usize {
            self.len
        }

        /// 判断报文是否为空。
        pub fn is_empty(&self) -> bool {
            self.len == 0
        }

        /// 对端地址（未考虑 `rport` 的原始来源）。
        pub fn peer(&self) -> SocketAddr {
            self.peer
        }

        /// 获取回源路由信息。
        pub fn return_route(&self) -> &UdpReturnRoute {
            &self.return_route
        }
    }

    /// UDP 模块统一错误类型。
    #[derive(Debug, Error)]
    pub enum UdpError {
        /// 绑定失败。
        #[error("无法绑定 UDP 套接字到 {addr}: {source}")]
        Bind {
            addr: String,
            source: std::io::Error,
        },
        /// 查询本地地址失败。
        #[error("无法获取 UDP 套接字本地地址: {0}")]
        LocalAddr(#[source] std::io::Error),
        /// 接收失败。
        #[error("接收 UDP 报文失败: {0}")]
        Receive(#[source] std::io::Error),
        /// 发送失败。
        #[error("发送 UDP 报文失败: {0}")]
        Send(#[source] std::io::Error),
    }

    /// 将 `TransportSocketAddr` 转换为标准库 `SocketAddr`。
    ///
    /// # 设计考量
    /// - 仅支持 IPv4/IPv6，保持与 `TransportSocketAddr` 定义一致。
    fn transport_to_std(addr: TransportSocketAddr) -> SocketAddr {
        match addr {
            TransportSocketAddr::V4 { addr, port } => {
                SocketAddr::new(IpAddr::V4(Ipv4Addr::from(addr)), port)
            }
            TransportSocketAddr::V6 { addr, port } => {
                SocketAddr::new(IpAddr::V6(Ipv6Addr::from(addr)), port)
            }
        }
    }

    /// 解析 SIP `Via` 头的 `rport` 状态。
    ///
    /// # 解析策略（How）
    /// - 遍历报文的每一行，寻找首个 `Via`/`v` 头。
    /// - 在该行内进行大小写不敏感的 `;rport` 匹配：
    ///   - 形如 `;rport=5060` 记为 `Advertised(5060)`。
    ///   - 形如 `;rport` 或 `;rport=` 记为 `Requested`。
    /// - 未找到则返回 `Absent`。
    fn parse_sip_rport(payload: &[u8]) -> SipViaRportDisposition {
        let mut cursor = 0;
        while cursor <= payload.len() {
            let line_end = match payload[cursor..].iter().position(|&b| b == b'\n') {
                Some(offset) => cursor + offset,
                None => payload.len(),
            };
            let line = &payload[cursor..line_end];
            let trimmed = trim_start_ascii_whitespace(line);
            if is_via_header(trimmed) {
                let disposition = parse_rport_from_via(trimmed);
                return disposition.unwrap_or(SipViaRportDisposition::Absent);
            }
            if line_end == payload.len() {
                break;
            }
            cursor = line_end + 1;
        }
        SipViaRportDisposition::Absent
    }

    /// 若需要，将 `Via` 头中的 `rport` 改写为真实端口。
    ///
    /// # 实现要点
    /// - 仅当找到 `;rport` 且无显式端口时才改写。
    /// - 保留原始大小写及剩余参数顺序。
    fn rewrite_sip_rport(payload: &[u8], port: u16) -> Option<Vec<u8>> {
        let mut cursor = 0;
        let port_bytes = port.to_string().into_bytes();
        while cursor <= payload.len() {
            let line_end = match payload[cursor..].iter().position(|&b| b == b'\n') {
                Some(offset) => cursor + offset,
                None => payload.len(),
            };
            let line = &payload[cursor..line_end];
            let trimmed = trim_start_ascii_whitespace(line);
            if is_via_header(trimmed) {
                if let Some(rewritten) =
                    rewrite_rport_in_line(payload, cursor, line_end, port_bytes)
                {
                    return Some(rewritten);
                }
                return None;
            }
            if line_end == payload.len() {
                break;
            }
            cursor = line_end + 1;
        }
        None
    }

    /// 判断一行是否为 `Via` 头。
    fn is_via_header(line: &[u8]) -> bool {
        line.len() >= 4 && line[..4].eq_ignore_ascii_case(b"Via:")
            || line.len() >= 2 && line[..2].eq_ignore_ascii_case(b"V:")
    }

    /// 解析 `Via` 头中的 `rport`。
    fn parse_rport_from_via(line: &[u8]) -> Option<SipViaRportDisposition> {
        let lower = to_ascii_lowercase(line);
        let rport = lower.windows(6).position(|window| window == b";rport")?;
        let offset = rport + 6;
        let suffix = &line[offset..];
        if suffix.starts_with(b"=") {
            let mut idx = 1;
            while idx < suffix.len() && suffix[idx].is_ascii_digit() {
                idx += 1;
            }
            if idx == 1 {
                Some(SipViaRportDisposition::Requested)
            } else {
                let port = std::str::from_utf8(&suffix[1..idx]).ok()?;
                let port = port.parse().ok()?;
                Some(SipViaRportDisposition::Advertised(port))
            }
        } else {
            Some(SipViaRportDisposition::Requested)
        }
    }

    /// 在整段报文中改写 `rport`，保持除端口外的内容不变。
    fn rewrite_rport_in_line(
        payload: &[u8],
        line_start: usize,
        line_end: usize,
        port_bytes: Vec<u8>,
    ) -> Option<Vec<u8>> {
        let line = &payload[line_start..line_end];
        let lower = to_ascii_lowercase(line);
        let pos = lower.windows(6).position(|window| window == b";rport")?;
        let absolute = line_start + pos + 6;
        if absolute > payload.len() {
            return None;
        }
        let suffix = &payload[absolute..line_end];
        if suffix.starts_with(b"=") {
            let mut idx = 1;
            while idx < suffix.len() && suffix[idx].is_ascii_digit() {
                idx += 1;
            }
            if idx == 1 {
                let mut rewritten = Vec::with_capacity(payload.len() + port_bytes.len());
                rewritten.extend_from_slice(&payload[..absolute + 1]);
                rewritten.extend_from_slice(&port_bytes);
                rewritten.extend_from_slice(&payload[absolute + 1..]);
                return Some(rewritten);
            }
            None
        } else {
            let mut rewritten = Vec::with_capacity(payload.len() + port_bytes.len() + 1);
            rewritten.extend_from_slice(&payload[..absolute]);
            rewritten.push(b'=');
            rewritten.extend_from_slice(&port_bytes);
            rewritten.extend_from_slice(&payload[absolute..]);
            Some(rewritten)
        }
    }

    /// 去除行首的 ASCII 空白。
    fn trim_start_ascii_whitespace(slice: &[u8]) -> &[u8] {
        let mut idx = 0;
        while idx < slice.len() && matches!(slice[idx], b' ' | b'\t') {
            idx += 1;
        }
        &slice[idx..]
    }

    /// 将字节切片转换为全小写形式（ASCII）。
    fn to_ascii_lowercase(slice: &[u8]) -> Vec<u8> {
        slice.iter().map(|b| b.to_ascii_lowercase()).collect()
    }
}

#[cfg(feature = "runtime-tokio")]
pub use runtime_impl::*;

#[cfg(not(feature = "runtime-tokio"))]
mod runtime_stub {
    #![allow(dead_code)]

    use core::future::Future;
    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

    use spark_core::transport::{DatagramEndpoint, TransportSocketAddr};
    use spark_core::{
        CoreError, context::Context, error::ErrorCategory, prelude::Result,
        transport::TransportBuilder,
    };
    use thiserror::Error;

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    pub enum SipViaRportDisposition {
        Absent,
        Advertised(u16),
        Requested,
    }

    #[derive(Clone, Debug)]
    pub struct UdpReturnRoute {
        target: SocketAddr,
        sip_rport: SipViaRportDisposition,
    }

    impl UdpReturnRoute {
        pub(crate) fn new(target: SocketAddr, sip_rport: SipViaRportDisposition) -> Self {
            Self { target, sip_rport }
        }

        pub fn target(&self) -> SocketAddr {
            self.target
        }

        pub fn sip_rport(&self) -> SipViaRportDisposition {
            self.sip_rport
        }

        pub(crate) fn requires_rewrite(&self) -> bool {
            matches!(self.sip_rport, SipViaRportDisposition::Requested)
        }
    }

    /// 占位实现错误，用于提示调用方启用 `runtime-tokio` 特性。
    #[derive(Debug, Error)]
    pub enum UdpError {
        #[error("spark-transport-udp 需要启用 `runtime-tokio` 特性方可使用网络实现")]
        RuntimeDisabled,
    }

    #[derive(Clone, Debug)]
    pub struct UdpIncoming;

    impl UdpIncoming {
        pub fn len(&self) -> usize {
            0
        }

        pub fn is_empty(&self) -> bool {
            true
        }

        pub fn peer(&self) -> SocketAddr {
            SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0)
        }

        pub fn return_route(&self) -> &UdpReturnRoute {
            panic!("runtime-tokio feature is disabled")
        }
    }

    #[derive(Clone, Debug, Default)]
    pub struct UdpSocketOptions;

    impl UdpSocketOptions {
        pub fn with_broadcast(self, _enabled: bool) -> Self {
            self
        }

        pub fn with_multicast_loop_v4(self, _enabled: bool) -> Self {
            self
        }

        pub fn broadcast(&self) -> bool {
            false
        }

        pub fn multicast_loop_v4(&self) -> bool {
            false
        }
    }

    pub struct UdpEndpoint {
        _options: UdpSocketOptions,
    }

    impl UdpEndpoint {
        pub async fn bind(_addr: TransportSocketAddr) -> Result<Self, UdpError> {
            Err(UdpError::RuntimeDisabled)
        }

        pub async fn bind_with_options(
            _addr: TransportSocketAddr,
            _options: UdpSocketOptions,
        ) -> Result<Self, UdpError> {
            Err(UdpError::RuntimeDisabled)
        }

        pub fn local_addr(&self) -> Result<TransportSocketAddr, UdpError> {
            Err(UdpError::RuntimeDisabled)
        }

        pub fn options(&self) -> &UdpSocketOptions {
            &self._options
        }

        pub async fn recv_from(&self, _buffer: &mut [u8]) -> Result<UdpIncoming, UdpError> {
            Err(UdpError::RuntimeDisabled)
        }

        pub async fn send_to(
            &self,
            _payload: &[u8],
            _route: &UdpReturnRoute,
        ) -> Result<usize, UdpError> {
            Err(UdpError::RuntimeDisabled)
        }
    }

    #[derive(Clone, Debug)]
    pub struct UdpEndpointBuilder {
        addr: TransportSocketAddr,
        options: UdpSocketOptions,
    }

    impl UdpEndpointBuilder {
        pub fn new(addr: TransportSocketAddr) -> Self {
            Self {
                addr,
                options: UdpSocketOptions::default(),
            }
        }

        pub fn with_options(mut self, options: UdpSocketOptions) -> Self {
            self.options = options;
            self
        }

        pub fn with_broadcast(mut self, enabled: bool) -> Self {
            self.options = self.options.with_broadcast(enabled);
            self
        }

        pub fn with_multicast_loop_v4(mut self, enabled: bool) -> Self {
            self.options = self.options.with_multicast_loop_v4(enabled);
            self
        }
    }

    impl TransportBuilder for UdpEndpointBuilder {
        type Output = UdpEndpoint;

        type BuildFuture<'ctx>
            =
            core::pin::Pin<Box<dyn Future<Output = Result<Self::Output, CoreError>> + Send + 'ctx>>
        where
            Self: 'ctx;

        fn scheme(&self) -> &'static str {
            "udp"
        }

        fn build<'ctx>(self, _ctx: &'ctx Context<'ctx>) -> Self::BuildFuture<'ctx> {
            Box::pin(async { Err(runtime_disabled_error()) })
        }
    }

    fn runtime_disabled_error() -> CoreError {
        CoreError::new(
            "spark.transport.udp.runtime_disabled",
            "udp runtime feature `runtime-tokio` is disabled",
        )
        .with_category(ErrorCategory::NonRetryable)
    }

    impl DatagramEndpoint for UdpEndpoint {
        type Error = UdpError;
        type CallCtx<'ctx> = ();
        type InboundMeta = UdpIncoming;
        type OutboundMeta = UdpReturnRoute;

        type RecvFuture<'ctx> = core::pin::Pin<
            Box<dyn Future<Output = Result<(usize, UdpIncoming), UdpError>> + Send + 'ctx>,
        >;
        type SendFuture<'ctx> =
            core::pin::Pin<Box<dyn Future<Output = Result<usize, UdpError>> + Send + 'ctx>>;

        fn recv<'ctx>(
            &'ctx self,
            _ctx: &'ctx (),
            _buffer: &'ctx mut [u8],
        ) -> Self::RecvFuture<'ctx> {
            Box::pin(async { Err(UdpError::RuntimeDisabled) })
        }

        fn send<'ctx>(
            &'ctx self,
            _ctx: &'ctx (),
            _payload: &'ctx [u8],
            _meta: &'ctx UdpReturnRoute,
        ) -> Self::SendFuture<'ctx> {
            Box::pin(async { Err(UdpError::RuntimeDisabled) })
        }

        fn local_addr(&self) -> Result<TransportSocketAddr, UdpError> {
            Err(UdpError::RuntimeDisabled)
        }
    }

    pub fn transport_to_std(addr: TransportSocketAddr) -> SocketAddr {
        match addr {
            TransportSocketAddr::V4 { addr, port } => {
                SocketAddr::new(IpAddr::V4(Ipv4Addr::from(addr)), port)
            }
            TransportSocketAddr::V6 { addr, port } => {
                SocketAddr::new(IpAddr::V6(Ipv6Addr::from(addr)), port)
            }
        }
    }
}

#[cfg(not(feature = "runtime-tokio"))]
pub use runtime_stub::*;

#[cfg(all(test, feature = "runtime-tokio"))]
mod tests {
    use super::*;
    use spark_core::{
        contract::CallContext,
        transport::{TransportBuilder, TransportSocketAddr},
    };
    use std::net::SocketAddr;

    /// 验证 `UdpEndpointBuilder` 可以将选项写入监听器。
    #[tokio::test(flavor = "multi_thread")]
    async fn builder_applies_socket_options() {
        let addr: SocketAddr = "127.0.0.1:0".parse().expect("parse addr");
        let endpoint = UdpEndpointBuilder::new(TransportSocketAddr::from(addr))
            .with_broadcast(true)
            .with_multicast_loop_v4(true)
            .build(&CallContext::builder().build().execution())
            .await
            .expect("bind endpoint");

        assert!(endpoint.options().broadcast());
        assert!(endpoint.options().multicast_loop_v4());
    }
}
