//! Tokio 运行时后端实现。
//!
//! # 设计说明（Why）
//! - 该模块提供 Tokio 平台上的 UDP 传输实现细节；由于主库目前以内联实现为准，模块暂保留以支撑未来的拆分计划。
//! - 在默认构建中尚未启用此后端，因此大量符号未被引用，需显式放宽 `dead_code` 检查。
//!
//! # 使用策略（How）
//! - 当未来启用分离运行时时，可通过 `pub use tokio_runtime::*` 替换内联实现；当前仅作为设计占位。
#![allow(dead_code)]

use std::{
    borrow::Cow,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
};

use spark_core::prelude::TransportSocketAddr;
use thiserror::Error;
use tokio::net::UdpSocket;

/// 表示 UDP 传输层在处理 SIP 报文时的 `rport` 状态。
///
/// # 设计动机（Why）
/// - SIP 要求服务端在响应中回写客户端透出的 `rport`，以便 NAT 后端口可达。
/// - 若请求中未声明 `rport`，则无需额外动作；若声明但无值，表示需要服务端填写。
///
/// # 契约说明（What）
/// - `Absent`：请求未携带 `rport` 参数，发送响应时不做改写。
/// - `Advertised(u16)`：请求显式指定回源端口，响应应沿用原值。
/// - `Requested`：请求声明了 `;rport` 或 `;rport=`，但未提供端口，需要服务端填入实际源端口。
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
        let display = addr.to_string();
        let std_addr = transport_to_std(addr);
        let sock = UdpSocket::bind(std_addr)
            .await
            .map_err(|source| UdpError::Bind {
                addr: display,
                source,
            })?;
        Ok(Self { sock })
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
    pub async fn recv_from(&self, buffer: &mut [u8]) -> spark_core::Result<UdpIncoming, UdpError> {
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
        // 采用 panic 明确提示：若 `TransportSocketAddr` 扩展新变体，应在此补充转换逻辑。
        _ => panic!("未支持的 TransportSocketAddr 变体，请补齐 UDP 转换实现"),
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
            if let Some(rewritten) = rewrite_rport_in_line(payload, cursor, line_end, port_bytes) {
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
