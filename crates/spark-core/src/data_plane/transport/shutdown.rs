/// 半关闭方向描述。
///
/// # 教案级说明
///
/// ## 意图（Why）
/// - 统一 TCP、TLS、QUIC 等协议在优雅收尾阶段的方向控制语义；
/// - 为 `Channel::shutdown` 与上层调用者提供一致的枚举类型，
///   避免业务侧直接依赖运行时特定的枚举（如 `std::net::Shutdown`）。
///
/// ## 契约（What）
/// - `Read`：关闭读方向，继续允许写；
/// - `Write`：关闭写方向，仍可读取对端数据；
/// - `Both`：同时关闭读写，等价于连接终止；
/// - **前置条件**：调用前需确认底层协议支持相应方向；
/// - **后置条件**：具体副作用由传输实现负责，枚举本身不产生行为。
///
/// ## 解析逻辑（How）
/// - 该类型仅作为意图描述，不存储额外状态；
/// - 在启用 `std` 时，可转换为 `std::net::Shutdown` 以复用标准库 API。
///
/// ## 风险提示（Trade-offs）
/// - 某些协议可能不支持精确的半关闭语义，应在实现层返回错误或退化为 `Both`；
/// - 在 `no_std` 环境下不提供任何额外方法，保持最小依赖。
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum ShutdownDirection {
    Read,
    Write,
    Both,
}

#[cfg(feature = "std")]
impl From<ShutdownDirection> for std::net::Shutdown {
    fn from(direction: ShutdownDirection) -> Self {
        match direction {
            ShutdownDirection::Read => std::net::Shutdown::Read,
            ShutdownDirection::Write => std::net::Shutdown::Write,
            ShutdownDirection::Both => std::net::Shutdown::Both,
        }
    }
}
