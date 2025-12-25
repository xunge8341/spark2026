use crate::error::{OperationKind, cancelled_error, map_io_error, timeout_error};
use spark_core::prelude::{
    CallContext, Cancellation, CoreError, Deadline, MonotonicTimePoint, TransportSocketAddr,
};
use std::future::Future;
use std::io;
use std::net::{IpAddr, Ipv6Addr, SocketAddr};
use std::sync::OnceLock;
use std::time::{Duration, Instant};
use tokio::time::Instant as TokioInstant;

const CANCELLATION_POLL_INTERVAL: Duration = Duration::from_millis(5);

/// 将框架内部的地址表示转换为标准库地址。
pub(crate) fn to_socket_addr(addr: TransportSocketAddr) -> SocketAddr {
    match addr {
        TransportSocketAddr::V4 { addr, port } => SocketAddr::new(IpAddr::from(addr), port),
        TransportSocketAddr::V6 { addr, port } => {
            SocketAddr::new(IpAddr::from(Ipv6Addr::from(addr)), port)
        }
    }
}

/// 获取当前的单调时间点，供 Deadline 比较使用。
pub(crate) fn monotonic_now() -> MonotonicTimePoint {
    let base = monotonic_base();
    let now = Instant::now();
    MonotonicTimePoint::from_offset(now.duration_since(base))
}

/// 判断截止时间是否已经过期。
pub(crate) fn deadline_expired(deadline: Deadline) -> bool {
    match deadline.instant() {
        Some(target) => target <= monotonic_now(),
        None => false,
    }
}

/// 计算距离截止时间的剩余时长。
pub(crate) fn deadline_remaining(deadline: Deadline) -> Option<Duration> {
    deadline
        .instant()
        .map(|instant| instant.saturating_duration_since(monotonic_now()))
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

async fn wait_for_cancellation(cancellation: &Cancellation) {
    if cancellation.is_cancelled() {
        return;
    }
    while !cancellation.is_cancelled() {
        tokio::time::sleep(CANCELLATION_POLL_INTERVAL).await;
    }
}

/// 在保留取消/超时语义的前提下执行 IO Future。
pub(crate) async fn run_with_context<'a, F, T>(
    ctx: &CallContext,
    kind: OperationKind,
    future: F,
) -> spark_core::Result<T, CoreError>
where
    F: Future<Output = io::Result<T>> + Send + 'a,
    T: Send + 'a,
{
    if deadline_expired(ctx.deadline()) {
        return Err(timeout_error(kind));
    }
    if ctx.cancellation().is_cancelled() {
        return Err(cancelled_error(kind));
    }

    let cancel = wait_for_cancellation(ctx.cancellation());
    tokio::pin!(cancel);
    tokio::pin!(future);

    if let Some(deadline) = to_tokio_deadline(ctx.deadline()) {
        let sleep = tokio::time::sleep_until(deadline);
        tokio::pin!(sleep);
        tokio::select! {
            biased;
            _ = &mut cancel => Err(cancelled_error(kind)),
            _ = &mut sleep => Err(timeout_error(kind)),
            result = &mut future => result.map_err(|err| map_io_error(kind, err)),
        }
    } else {
        tokio::select! {
            biased;
            _ = &mut cancel => Err(cancelled_error(kind)),
            result = &mut future => result.map_err(|err| map_io_error(kind, err)),
        }
    }
}
