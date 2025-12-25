use crate::error::{self, OperationKind};
use spark_core::prelude::{
    CallContext, Cancellation, CoreError, Deadline, MonotonicTimePoint, TransportSocketAddr,
};
use std::future::Future;
use std::net::{IpAddr, Ipv6Addr, SocketAddr};
use std::sync::OnceLock;
use std::time::{Duration, Instant};
use tokio::time::Instant as TokioInstant;

/// QUIC 工具函数集合。
///
/// # 教案式注释
///
/// ## 意图（Why）
/// - **地址转换**：在 `spark-core` 的抽象地址与标准库 `SocketAddr` 之间互转，确保传输层
///   可以与 Tokio/Quinn 正常交互。
/// - **上下文执行**：为所有异步 IO 操作注入取消与截止时间语义，使 QUIC 通道与框架
///   的 `CallContext` 契约保持一致。
/// - **时间基准**：提供统一的单调时间点，避免在 `poll_ready` 等热路径反复读取系统时钟。
///
/// ## 逻辑（How）
/// - `to_socket_addr`：根据枚举变体组装 IPv4/IPv6 `SocketAddr`；
/// - `run_with_context`：在执行 Future 前检查截止/取消，并使用 `tokio::select!` 同步等待；
/// - `deadline_expired`/`deadline_remaining`：辅助判断上下文中截止时间剩余量；
/// - 单调基准通过 `OnceLock` 缓存第一次读取的时间点，后续调用以差值形式获得 `MonotonicTimePoint`。
///
/// ## 契约（What）
/// - `run_with_context` 要求传入的 Future 返回 `spark_core::Result<T, CoreError>`，若 Future 成功则原样返回；
/// - **前置条件**：调用方须确保传入的 `CallContext` 来自框架构造器，且生命周期覆盖 Future 执行；
/// - **后置条件**：一旦触发取消/超时，将返回带有相应错误码的 `CoreError`，并不会继续执行 Future。
///
/// ## 风险与注意（Trade-offs）
/// - `tokio::select!` 采用 `biased` 策略，保证取消/超时优先；若 Future 内部也使用 `select!`，
///   需注意避免自旋；
/// - `deadline_remaining` 若遇到非常大的截止时间，转换为 Tokio `Instant` 可能存在精度损失，
///   但对传输层影响可忽略；
/// - `to_socket_addr` 当前仅实现 IPv4/IPv6，未来若新增变体需同步更新。
pub(crate) fn to_socket_addr(addr: TransportSocketAddr) -> SocketAddr {
    match addr {
        TransportSocketAddr::V4 { addr, port } => SocketAddr::new(IpAddr::from(addr), port),
        TransportSocketAddr::V6 { addr, port } => {
            SocketAddr::new(IpAddr::from(Ipv6Addr::from(addr)), port)
        }
    }
}

pub(crate) fn monotonic_now() -> MonotonicTimePoint {
    let base = monotonic_base();
    let now = Instant::now();
    MonotonicTimePoint::from_offset(now.duration_since(base))
}

pub(crate) fn deadline_expired(deadline: Deadline) -> bool {
    match deadline.instant() {
        Some(target) => target <= monotonic_now(),
        None => false,
    }
}

pub(crate) fn deadline_remaining(deadline: Deadline) -> Option<Duration> {
    deadline
        .instant()
        .map(|instant| instant.saturating_duration_since(monotonic_now()))
}

pub(crate) async fn run_with_context<'a, F, T>(
    ctx: &CallContext,
    kind: OperationKind,
    future: F,
) -> spark_core::Result<T, CoreError>
where
    F: Future<Output = spark_core::Result<T, CoreError>> + Send + 'a,
    T: Send + 'a,
{
    if deadline_expired(ctx.deadline()) {
        return Err(error::timeout_error(kind));
    }
    if ctx.cancellation().is_cancelled() {
        return Err(error::cancelled_error(kind));
    }

    let cancel = wait_for_cancellation(ctx.cancellation());
    tokio::pin!(cancel);
    tokio::pin!(future);

    if let Some(deadline) = to_tokio_deadline(ctx.deadline()) {
        let sleep = tokio::time::sleep_until(deadline);
        tokio::pin!(sleep);
        tokio::select! {
            biased;
            _ = &mut cancel => Err(error::cancelled_error(kind)),
            _ = &mut sleep => Err(error::timeout_error(kind)),
            result = &mut future => result,
        }
    } else {
        tokio::select! {
            biased;
            _ = &mut cancel => Err(error::cancelled_error(kind)),
            result = &mut future => result,
        }
    }
}

async fn wait_for_cancellation(cancellation: &Cancellation) {
    if cancellation.is_cancelled() {
        return;
    }
    while !cancellation.is_cancelled() {
        tokio::time::sleep(CANCELLATION_POLL_INTERVAL).await;
    }
}

fn to_tokio_deadline(deadline: Deadline) -> Option<TokioInstant> {
    deadline.instant().map(|instant| {
        let base = monotonic_base();
        let target = base + instant.as_duration();
        TokioInstant::from_std(target)
    })
}

fn monotonic_base() -> Instant {
    static BASE: OnceLock<Instant> = OnceLock::new();
    *BASE.get_or_init(Instant::now)
}

const CANCELLATION_POLL_INTERVAL: Duration = Duration::from_millis(5);
