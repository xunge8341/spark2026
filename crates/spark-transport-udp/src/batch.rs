//! UDP 批量 IO 内部优化模块。
//!
//! # 模块定位（Why）
//! - **减少系统调用成本**：在 Linux 下借助 `recvmmsg`/`sendmmsg` 一次完成多条报文的收发，
//!   用于承载上层 T2-1 并行流水线所需的批处理基础能力。
//! - **可选增强路径**：通过 `batch-udp-unix` 特性开关启用，未开启或非 Linux 平台自动退化为
//!   顺序循环，避免破坏现有 API 与兼容性。
//!
//! # 暴露接口（What）
//! - [`recv_from`]：读取多个报文填充到调用方预置的缓冲槽。
//! - [`send_to`]：批量发送多条报文。
//! - [`RecvBatchSlot`] / [`SendBatchSlot`]：描述单条批次任务的输入输出契约。
//!
//! # 设计要点（How）
//! - **Unix 优化**：在 `cfg(all(target_os = "linux", feature = "batch-udp-unix"))` 下，结合
//!   `socket2` 与 `nix` 的零拷贝封装构建 `mmsghdr` 数组，配合 Tokio `try_io` 驱动非阻塞
//!   系统调用。
//! - **跨平台兼容**：其它平台使用 Tokio 自带的 `recv_from`/`send_to` 循环，确保行为等价。
//! - **契约显式**：槽位对象在调用前后分别表达输入（缓冲区、目标地址）与输出
//!   （长度、对端、是否截断），便于调用者在上层调度任务。

use std::{io, net::SocketAddr};

use thiserror::Error;
use tokio::net::UdpSocket;

/// UDP 批量操作的统一错误类型。
///
/// # Why
/// - 将 Linux 原生错误（来自 `recvmmsg`/`sendmmsg`）与跨平台循环路径的 IO 错误统一封装，
///   方便调用方做一致的错误恢复或降级策略。
///
/// # What
/// - `Receive`：批量接收阶段出现的底层错误。
/// - `Send`：批量发送阶段出现的底层错误。
///
/// # 契约说明
/// - `source` 字段保留原始 `std::io::Error`，确保错误码、错误信息完整透传。
#[derive(Debug, Error)]
pub enum BatchIoError {
    /// 接收阶段失败。
    #[error("批量接收 UDP 报文失败: {source}")]
    Receive { source: io::Error },
    /// 发送阶段失败。
    #[error("批量发送 UDP 报文失败: {source}")]
    Send { source: io::Error },
}

/// 承载单条接收槽位的输入缓冲与输出结果。
///
/// # Why
/// - 调用方通常预先管理缓冲区池，本结构让缓冲与结果绑定，既能避免重复分配，也便于
///   上层根据 `len` 与 `addr` 元信息进行路由。
///
/// # What
/// - `buffer`：调用方提供的可写字节切片，为本次接收的承载空间。
/// - `len`：实际写入的字节数。
/// - `addr`：报文来源地址。
/// - `truncated`：指示底层是否标记了 `MSG_TRUNC`，提示缓冲可能被截断。
///
/// # How
/// - 使用 [`RecvBatchSlot::new`] 创建并绑定缓冲。
/// - 批量接收函数在成功后通过内部方法填充长度与地址。
///
/// # 契约（Contract）
/// - **前置条件**：`buffer` 必须在整个批处理生命周期内保持有效；调用方需在下一次复用前
///   通过 [`RecvBatchSlot::reset`] 清空旧数据。
/// - **后置条件**：当 `len > 0` 时，`buffer[..len]` 为有效负载；`addr` 为 `Some`。
#[derive(Debug)]
pub struct RecvBatchSlot<'a> {
    buffer: &'a mut [u8],
    len: usize,
    addr: Option<SocketAddr>,
    truncated: bool,
}

impl<'a> RecvBatchSlot<'a> {
    /// 构造新的接收槽位。
    pub fn new(buffer: &'a mut [u8]) -> Self {
        Self {
            buffer,
            len: 0,
            addr: None,
            truncated: false,
        }
    }

    /// 当前填充的有效负载长度。
    pub fn len(&self) -> usize {
        self.len
    }

    /// 快速判断槽位是否尚未填充。
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// 获取报文来源地址。
    pub fn addr(&self) -> Option<SocketAddr> {
        self.addr
    }

    /// 是否标记为被截断。
    pub fn truncated(&self) -> bool {
        self.truncated
    }

    /// 读取当前缓冲内容的只读切片。
    pub fn payload(&self) -> &[u8] {
        &self.buffer[..self.len]
    }

    /// 暴露底层缓冲区用于在批量操作前重新写入。
    pub fn buffer_mut(&mut self) -> &mut [u8] {
        self.buffer
    }

    /// 重置槽位状态，供上层在复用缓冲前调用。
    pub fn reset(&mut self) {
        self.len = 0;
        self.addr = None;
        self.truncated = false;
    }

    /// 结束一次接收填充。
    pub(super) fn finish(&mut self, len: usize, addr: SocketAddr, truncated: bool) {
        self.len = len;
        self.addr = Some(addr);
        self.truncated = truncated;
    }
}

/// 承载批量发送任务的描述与结果。
///
/// # Why
/// - 在批量发送场景中，需要将目标地址与待发 payload 配对，同时回写成功发送的字节数，
///   方便上层感知是否发生部分写入或错误重试。
///
/// # What
/// - `payload`：原始待发送数据。
/// - `addr`：目标地址。
/// - `sent`：系统调用返回的已发送字节数。
///
/// # 契约
/// - **前置条件**：payload 生命周期需覆盖批处理完成；调用前应当通过 [`SendBatchSlot::mark_unsent`] 将
///   `sent` 清零以避免读取到旧值。
/// - **后置条件**：成功后 `sent` 记录内核报告的写入字节数。
#[derive(Debug)]
pub struct SendBatchSlot<'a> {
    payload: &'a [u8],
    addr: SocketAddr,
    sent: usize,
}

impl<'a> SendBatchSlot<'a> {
    /// 构造新的发送槽位。
    pub fn new(payload: &'a [u8], addr: SocketAddr) -> Self {
        Self {
            payload,
            addr,
            sent: 0,
        }
    }

    /// 访问待发送的原始数据。
    pub fn payload(&self) -> &[u8] {
        self.payload
    }

    /// 返回目标地址。
    pub fn target(&self) -> SocketAddr {
        self.addr
    }

    /// 返回内核报告的写入字节数。
    pub fn sent(&self) -> usize {
        self.sent
    }

    /// 发送前重置状态，避免复用旧记录。
    pub fn mark_unsent(&mut self) {
        self.sent = 0;
    }

    /// 内部辅助：写入成功后的回填逻辑。
    pub(super) fn mark_sent(&mut self, sent: usize) {
        self.sent = sent;
    }
}

/// 批量接收多个 UDP 报文。
///
/// # Why
/// - 将多次 IO 聚合为一次调度，在 Linux 下显著降低内核/用户态切换次数；在其他平台则保持
///   语义一致的循环收包路径。
///
/// # What
/// - `socket`：Tokio UDP 套接字，需已绑定且处于非阻塞模式。
/// - `slots`：待填充的槽位数组。
///
/// # How
/// - 在函数内部先重置槽位状态，然后委托平台专属实现：
///   - Linux + 特性启用：`recvmmsg`。
///   - 其他情况：`recv_from` 循环。
///
/// # 契约
/// - **前置条件**：`slots` 不可包含重叠缓冲区，避免数据竞争；Tokio 运行时需处于多线程或
///   当前任务允许执行阻塞 `try_io`。
/// - **后置条件**：返回值表示成功填充的槽位数量，且这些槽位的 `len > 0`、`addr = Some`。
pub async fn recv_from(
    socket: &UdpSocket,
    slots: &mut [RecvBatchSlot<'_>],
) -> spark_core::Result<usize, BatchIoError> {
    for slot in slots.iter_mut() {
        slot.reset();
    }
    platform::recv_from(socket, slots)
        .await
        .map_err(|source| BatchIoError::Receive { source })
}

/// 批量发送多个 UDP 报文。
///
/// # Why
/// - 与接收对称，将多条待发消息在 Linux 下通过一次 `sendmmsg` 发出，减少锁竞争与调度抖动。
///
/// # What
/// - `socket`：Tokio UDP 套接字。
/// - `slots`：包含 payload 与目的地址的槽位集合。
///
/// # 契约
/// - **前置条件**：调用方应保证套接字可写；若部分槽位依赖前序响应，需自行控制入队顺序。
/// - **后置条件**：成功的槽位会填充 `sent`，返回值为成功发送的个数。
pub async fn send_to(
    socket: &UdpSocket,
    slots: &mut [SendBatchSlot<'_>],
) -> spark_core::Result<usize, BatchIoError> {
    for slot in slots.iter_mut() {
        slot.mark_unsent();
    }
    platform::send_to(socket, slots)
        .await
        .map_err(|source| BatchIoError::Send { source })
}

#[cfg(all(feature = "batch-udp-unix", target_os = "linux"))]
mod platform {
    use super::{RecvBatchSlot, SendBatchSlot};
    use std::{
        io::{self, ErrorKind},
        net::SocketAddr,
    };

    use nix::errno::Errno;
    use nix::libc;
    use nix::sys::socket::{AddressFamily, SockaddrLike, SockaddrStorage};
    use socket2::SockAddr;
    use tokio::io::Interest;
    use tokio::net::UdpSocket;

    /// 将 `Errno` 转换为 `std::io::Error`。
    fn nix_err_to_io(errno: Errno) -> io::Error {
        io::Error::from_raw_os_error(errno as i32)
    }

    /// 将 `SockaddrStorage` 转换为标准库 `SocketAddr`。
    fn storage_to_std(storage: &SockaddrStorage) -> io::Result<SocketAddr> {
        match storage.family() {
            Some(AddressFamily::Inet) => storage
                .as_sockaddr_in()
                .map(|addr| SocketAddr::V4((*addr).into()))
                .ok_or_else(|| io::Error::new(ErrorKind::InvalidData, "缺失 IPv4 地址信息")),
            Some(AddressFamily::Inet6) => storage
                .as_sockaddr_in6()
                .map(|addr| SocketAddr::V6((*addr).into()))
                .ok_or_else(|| io::Error::new(ErrorKind::InvalidData, "缺失 IPv6 地址信息")),
            _ => Err(io::Error::new(
                ErrorKind::InvalidData,
                "批量接收仅支持 IPv4/IPv6 地址族",
            )),
        }
    }

    pub(super) async fn recv_from(
        socket: &UdpSocket,
        slots: &mut [RecvBatchSlot<'_>],
    ) -> io::Result<usize> {
        if slots.is_empty() {
            return Ok(0);
        }

        loop {
            match socket.try_io(Interest::READABLE, || recv_once(socket, slots)) {
                Ok(result) => return Ok(result),
                Err(err) if err.kind() == ErrorKind::WouldBlock => continue,
                Err(err) => return Err(err),
            }
        }
    }

    fn recv_once(socket: &UdpSocket, slots: &mut [RecvBatchSlot<'_>]) -> io::Result<usize> {
        let fd = socket.as_raw_fd();
        let count = slots.len();
        let mut storages: Vec<SockaddrStorage> =
            (0..count).map(|_| unsafe { std::mem::zeroed() }).collect();
        let mut iovecs: Vec<libc::iovec> = slots
            .iter_mut()
            .map(|slot| {
                let buf = slot.buffer_mut();
                libc::iovec {
                    iov_base: buf.as_mut_ptr() as *mut libc::c_void,
                    iov_len: buf.len(),
                }
            })
            .collect();
        let mut headers: Vec<libc::mmsghdr> = (0..count)
            .map(|idx| libc::mmsghdr {
                msg_hdr: libc::msghdr {
                    msg_name: (&mut storages[idx]) as *mut _ as *mut libc::c_void,
                    msg_namelen: std::mem::size_of::<SockaddrStorage>() as libc::socklen_t,
                    msg_iov: &mut iovecs[idx],
                    msg_iovlen: 1,
                    msg_control: std::ptr::null_mut(),
                    msg_controllen: 0,
                    msg_flags: 0,
                },
                msg_len: 0,
            })
            .collect();

        let received = unsafe {
            libc::recvmmsg(
                fd,
                headers.as_mut_ptr(),
                headers.len() as libc::c_uint,
                libc::MSG_DONTWAIT,
                std::ptr::null_mut(),
            )
        };

        if received < 0 {
            return Err(nix_err_to_io(Errno::last()));
        }

        let received = received as usize;
        for idx in 0..received {
            let hdr = &headers[idx].msg_hdr;
            let addr = storage_to_std(&storages[idx])?;
            let truncated = (hdr.msg_flags & libc::MSG_TRUNC) != 0;
            slots[idx].finish(headers[idx].msg_len as usize, addr, truncated);
        }
        Ok(received)
    }

    pub(super) async fn send_to(
        socket: &UdpSocket,
        slots: &mut [SendBatchSlot<'_>],
    ) -> io::Result<usize> {
        if slots.is_empty() {
            return Ok(0);
        }

        loop {
            match socket.try_io(Interest::WRITABLE, || send_once(socket, slots)) {
                Ok(result) => return Ok(result),
                Err(err) if err.kind() == ErrorKind::WouldBlock => continue,
                Err(err) => return Err(err),
            }
        }
    }

    fn send_once(socket: &UdpSocket, slots: &mut [SendBatchSlot<'_>]) -> io::Result<usize> {
        let fd = socket.as_raw_fd();
        let count = slots.len();
        let sockaddrs: Vec<SockAddr> = slots
            .iter()
            .map(|slot| SockAddr::from(slot.target()))
            .collect();
        let mut iovecs: Vec<libc::iovec> = slots
            .iter()
            .map(|slot| libc::iovec {
                iov_base: slot.payload().as_ptr() as *mut libc::c_void,
                iov_len: slot.payload().len(),
            })
            .collect();
        let mut headers: Vec<libc::mmsghdr> = (0..count)
            .map(|idx| libc::mmsghdr {
                msg_hdr: libc::msghdr {
                    msg_name: sockaddrs[idx].as_ptr() as *mut libc::c_void,
                    msg_namelen: sockaddrs[idx].len() as libc::socklen_t,
                    msg_iov: &mut iovecs[idx],
                    msg_iovlen: 1,
                    msg_control: std::ptr::null_mut(),
                    msg_controllen: 0,
                    msg_flags: 0,
                },
                msg_len: 0,
            })
            .collect();

        let sent = unsafe {
            libc::sendmmsg(
                fd,
                headers.as_mut_ptr(),
                headers.len() as libc::c_uint,
                libc::MSG_DONTWAIT,
            )
        };

        if sent < 0 {
            return Err(nix_err_to_io(Errno::last()));
        }

        let sent = sent as usize;
        for idx in 0..sent {
            slots[idx].mark_sent(headers[idx].msg_len as usize);
        }
        Ok(sent)
    }

    use std::os::unix::io::AsRawFd;
}

#[cfg(not(all(feature = "batch-udp-unix", target_os = "linux")))]
mod platform {
    use super::{RecvBatchSlot, SendBatchSlot};
    use std::io::{self, ErrorKind};
    use tokio::net::UdpSocket;

    pub(super) async fn recv_from(
        socket: &UdpSocket,
        slots: &mut [RecvBatchSlot<'_>],
    ) -> io::Result<usize> {
        if slots.is_empty() {
            return Ok(0);
        }

        let (len, addr) = socket.recv_from(slots[0].buffer_mut()).await?;
        slots[0].finish(len, addr, false);
        let mut filled = 1;

        for slot in &mut slots[1..] {
            match socket.try_recv_from(slot.buffer_mut()) {
                Ok((len, addr)) => {
                    slot.finish(len, addr, false);
                    filled += 1;
                }
                Err(err) if err.kind() == ErrorKind::WouldBlock => break,
                Err(err) => return Err(err),
            }
        }

        Ok(filled)
    }

    pub(super) async fn send_to(
        socket: &UdpSocket,
        slots: &mut [SendBatchSlot<'_>],
    ) -> io::Result<usize> {
        if slots.is_empty() {
            return Ok(0);
        }

        let mut sent = 0;
        let first_written = socket
            .send_to(slots[0].payload(), slots[0].target())
            .await?;
        slots[0].mark_sent(first_written);
        sent += 1;

        for slot in &mut slots[1..] {
            match socket.try_send_to(slot.payload(), slot.target()) {
                Ok(written) => {
                    slot.mark_sent(written);
                    sent += 1;
                }
                Err(err) if err.kind() == ErrorKind::WouldBlock => {
                    let written = socket.send_to(slot.payload(), slot.target()).await?;
                    slot.mark_sent(written);
                    sent += 1;
                }
                Err(err) => return Err(err),
            }
        }

        Ok(sent)
    }
}
