use core::fmt;

/// 传输层统一地址枚举。
///
/// # 教案级说明
///
/// ## 意图（Why）
/// - 为 TCP、QUIC、UDP 等传输实现提供统一的地址表示，
///   避免在跨协议协作时出现 `std::net::SocketAddr` 与自定义结构混用；
/// - 通过值语义封装 IPv4/IPv6，使该类型在 `no_std + alloc` 环境同样可用。
///
/// ## 契约（What）
/// - `V4 { addr, port }`：IPv4 地址，`addr` 使用网络序字节数组；
/// - `V6 { addr, port }`：IPv6 地址，采用 16 字节网络序表示；
/// - `v4` / `v6`：构造对应变体；
/// - `port()`：读取端口；
/// - `into_parts()`：按协议族拆解为字节数组与端口；
/// - **前置条件**：调用方需保证字节数组来源合法；
/// - **后置条件**：类型本身不执行 DNS 解析，仅存储静态数据。
///
/// ## 解析逻辑（How）
/// - 采用 `#[derive(Clone, Copy, Eq, PartialEq, Hash)]`，便于作为 HashMap 键；
/// - `Display` 实现复用标准库格式，保持日志输出一致；
/// - 在启用 `std` 特性时，实现 `From<SocketAddr>` 与 `From<TransportSocketAddr> for SocketAddr`
///   以方便在运行时与标准网络 API 转换。
///
/// ## 风险提示（Trade-offs）
/// - 当前未包含 Unix Domain Socket 等扩展地址族；若未来需要，可增量添加新变体；
/// - `into_parts` 返回裸字节数组，上层应谨慎处理字节序问题。
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum TransportSocketAddr {
    V4 { addr: [u8; 4], port: u16 },
    V6 { addr: [u8; 16], port: u16 },
}

impl TransportSocketAddr {
    /// 构造 IPv4 地址。
    pub const fn v4(addr: [u8; 4], port: u16) -> Self {
        Self::V4 { addr, port }
    }

    /// 构造 IPv6 地址。
    pub const fn v6(addr: [u8; 16], port: u16) -> Self {
        Self::V6 { addr, port }
    }

    /// 返回端口号。
    pub const fn port(&self) -> u16 {
        match self {
            Self::V4 { port, .. } | Self::V6 { port, .. } => *port,
        }
    }

    /// 拆解为字节数组与端口，便于自定义序列化。
    pub const fn into_parts(self) -> (AddressFamily, u16) {
        match self {
            Self::V4 { addr, port } => (AddressFamily::V4(addr), port),
            Self::V6 { addr, port } => (AddressFamily::V6(addr), port),
        }
    }
}

/// 地址族辅助枚举，配合 [`TransportSocketAddr::into_parts`] 使用。
///
/// # 教案级注释
/// - **意图（Why）**：在序列化或跨语言传递地址时，明确区分 IPv4/IPv6 字节布局；
/// - **契约（What）**：仅包含原始字节，不携带端口；
/// - **风险（Trade-offs）**：调用方需自行处理大小端与压缩表示。
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum AddressFamily {
    V4([u8; 4]),
    V6([u8; 16]),
}

impl fmt::Display for TransportSocketAddr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::V4 { addr, port } => {
                write!(
                    f,
                    "{}.{}.{}.{}:{}",
                    addr[0], addr[1], addr[2], addr[3], port
                )
            }
            Self::V6 { addr, port } => {
                let segments = [
                    (u16::from(addr[0]) << 8) | u16::from(addr[1]),
                    (u16::from(addr[2]) << 8) | u16::from(addr[3]),
                    (u16::from(addr[4]) << 8) | u16::from(addr[5]),
                    (u16::from(addr[6]) << 8) | u16::from(addr[7]),
                    (u16::from(addr[8]) << 8) | u16::from(addr[9]),
                    (u16::from(addr[10]) << 8) | u16::from(addr[11]),
                    (u16::from(addr[12]) << 8) | u16::from(addr[13]),
                    (u16::from(addr[14]) << 8) | u16::from(addr[15]),
                ];
                write!(
                    f,
                    "[{:x}:{:x}:{:x}:{:x}:{:x}:{:x}:{:x}:{:x}]:{}",
                    segments[0],
                    segments[1],
                    segments[2],
                    segments[3],
                    segments[4],
                    segments[5],
                    segments[6],
                    segments[7],
                    port
                )
            }
        }
    }
}

#[cfg(feature = "std")]
mod std_impls {
    use super::TransportSocketAddr;
    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

    impl From<SocketAddr> for TransportSocketAddr {
        fn from(addr: SocketAddr) -> Self {
            match addr {
                SocketAddr::V4(v4) => Self::V4 {
                    addr: v4.ip().octets(),
                    port: v4.port(),
                },
                SocketAddr::V6(v6) => Self::V6 {
                    addr: v6.ip().octets(),
                    port: v6.port(),
                },
            }
        }
    }

    impl From<TransportSocketAddr> for SocketAddr {
        fn from(addr: TransportSocketAddr) -> Self {
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
}
