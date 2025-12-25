use std::{
    future::Future,
    io,
    time::{Duration, Instant},
};

use spark_core::prelude::{CallContext, Cancellation, CoreError, Deadline, MonotonicTimePoint};
use tokio::time::Instant as TokioInstant;

use crate::error::{self, OperationKind};

/// TLS 辅助工具集合。
///
/// # 教案级注释
///
/// ## 意图（Why）
/// - 复用 TCP 层已经验证的“取消/截止”治理逻辑，避免 TLS 实现再次编写冗长的 `tokio::select!` 代码；
/// - 将 `CallContext` 中的时间、取消语义落实到 `rustls` 握手与后续 I/O，保证上下文契约在加密层也生效。
///
/// ## 逻辑（How）
/// - `run_with_context` 接收操作描述与 `Future`，统一处理截止时间、取消轮询与错误映射；
/// - 辅助函数 `deadline_expired`、`deadline_remaining`、`to_tokio_deadline` 提供与单调时钟兼容的转换；
/// - 由于 TLS 握手同样需要阻塞等待网络事件，内部实现沿用了“定时轮询取消”策略以避免长时间阻塞。
///
/// ## 契约（What）
/// - 若截止时间已过或取消标记已触发，立即返回对应的 `CoreError`；
/// - Future 成功完成时返回其产物，否则根据 `map_error` 映射为结构化错误；
/// - 调用方需保证传入的 Future 遵循 `Send + 'a` 约束，以便 Tokio 在多线程运行时调度。
///
/// ## 风险与权衡（Trade-offs）
/// - 轮询取消存在毫秒级延迟，但能避免在 TLS 握手阶段频繁创建额外任务；
/// - 若未来引入本地事件驱动的取消原语，可在此处集中替换实现，保持上层 API 稳定。
const CANCELLATION_POLL_INTERVAL: Duration = Duration::from_millis(5);

pub(crate) fn deadline_expired(deadline: Deadline) -> bool {
    match deadline.instant() {
        Some(target) => target <= monotonic_now(),
        None => false,
    }
}

pub(crate) async fn run_with_context<'a, F, T, Map>(
    ctx: &CallContext,
    kind: OperationKind,
    future: F,
    map_error: Map,
) -> spark_core::Result<T, CoreError>
where
    F: Future<Output = io::Result<T>> + Send + 'a,
    T: Send + 'a,
    Map: Fn(OperationKind, io::Error) -> CoreError,
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
            result = &mut future => result.map_err(|err| map_error(kind, err)),
        }
    } else {
        tokio::select! {
            biased;
            _ = &mut cancel => Err(error::cancelled_error(kind)),
            result = &mut future => result.map_err(|err| map_error(kind, err)),
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

fn monotonic_now() -> MonotonicTimePoint {
    let base = monotonic_base();
    let now = Instant::now();
    MonotonicTimePoint::from_offset(now.duration_since(base))
}

fn monotonic_base() -> Instant {
    use std::sync::OnceLock;
    static BASE: OnceLock<Instant> = OnceLock::new();
    *BASE.get_or_init(Instant::now)
}
