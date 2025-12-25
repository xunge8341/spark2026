use crate::{
    backpressure::QuicBackpressure,
    error::{self, FLUSH},
    util::{deadline_expired, deadline_remaining, run_with_context},
};
use bytes::{Buf, BufMut};
use futures::task::noop_waker_ref;
use quinn::{Connection, RecvStream, SendStream, VarInt};
use spark_core::prelude::{
    CallContext, Context as ExecutionView, CoreError, PollReady, ReadyCheck, ReadyState,
    ShutdownDirection, TransportSocketAddr,
};
use spark_core::transport::{BackpressureDecision, BackpressureMetrics, Channel};
use std::borrow::Cow;
use std::{
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context as TaskContext, Poll},
};
use tokio::sync::Mutex as AsyncMutex;

/// QUIC 双向流通道封装。
///
/// # 教案式注释
///
/// ## 意图（Why）
/// - **统一语义**：为上层提供与 TCP 通道一致的 `read`/`write`/`shutdown`/`poll_ready`
///   接口，使 QUIC 多路复用流也能遵循 `CallContext` 与 `ReadyState` 契约。
/// - **背压治理**：结合 `QuicBackpressure` 将流控与拥塞窗口转换为标准化信号，避免业务
///   直接解析 `quinn` 指标。
/// - **并发安全**：通过 Tokio/STD 互斥锁序列化访问，确保在多任务并发读写时不破坏 QUIC
///   内部状态机。
///
/// ## 逻辑（How）
/// - 内部保存 `quinn::SendStream`/`RecvStream` 与 `Connection` 引用，并用 `Arc` 包装实现克隆；
/// - `read`/`write` 使用 `run_with_context` 注入取消与截止时间语义，错误统一映射为
///   [`CoreError`]；
/// - `poll_ready` 通过无阻塞尝试写入空缓冲区探测背压，结合 `QuicBackpressure` 决定返回
///   `Busy` 或 `RetryAfter`；
/// - `shutdown` 根据方向调用 `finish` / `stop`，忽略幂等错误以提升健壮性。
///
/// ## 契约（What）
/// - `read`：读取至外部缓冲区，返回实际字节数（EOF 返回 `0`）；
/// - `write`：写入完整缓冲区，成功返回写入字节数，失败抛出结构化错误；
/// - `shutdown`：支持关闭读半部、写半部或同时关闭；
/// - `poll_ready`：返回 `PollReady<CoreError>`，保证符合 `ReadyState` 五态；
/// - **前置条件**：所有方法需在 Tokio 多线程运行时中调用，且 `CallContext` 生命周期需
///   覆盖异步操作；
/// - **后置条件**：成功写入后会重置背压统计，确保下一次 `poll_ready` 能正确反映当前状态。
///
/// ## 风险与注意（Trade-offs）
/// - 使用互斥锁序列化读写会牺牲部分并发度；若未来需要极致性能，可拆分读写半部；
/// - `poll_ready` 利用空写探测背压，可能引入极轻微的系统调用开销，但换取了统一语义；
/// - `shutdown` 忽略已关闭错误，确保幂等，但若业务需严格检测，可在外层补充状态机。
#[derive(Clone, Debug)]
pub struct QuicChannel {
    inner: Arc<QuicChannelInner>,
}

#[derive(Debug)]
struct QuicChannelInner {
    connection: Connection,
    send: AsyncMutex<SendStream>,
    recv: AsyncMutex<RecvStream>,
    backpressure: Mutex<QuicBackpressure>,
    peer_addr: TransportSocketAddr,
    local_addr: TransportSocketAddr,
}

impl QuicChannel {
    pub(crate) fn from_streams(
        connection: Connection,
        send: SendStream,
        recv: RecvStream,
        local_addr: TransportSocketAddr,
    ) -> Self {
        let peer_addr = connection.remote_address().into();
        Self {
            inner: Arc::new(QuicChannelInner {
                connection,
                send: AsyncMutex::new(send),
                recv: AsyncMutex::new(recv),
                backpressure: Mutex::new(QuicBackpressure::new()),
                peer_addr,
                local_addr,
            }),
        }
    }

    pub async fn read(
        &self,
        ctx: &CallContext,
        buf: &mut (dyn BufMut + Send + Sync + 'static),
    ) -> spark_core::Result<usize, CoreError> {
        run_with_context(ctx, error::READ, async {
            let mut guard = self.inner.recv.lock().await;
            let (size, capacity) = {
                let chunk = buf.chunk_mut();
                let capacity = chunk.len();
                if capacity == 0 {
                    (0, capacity)
                } else {
                    let raw = unsafe {
                        core::slice::from_raw_parts_mut(chunk.as_mut_ptr().cast::<u8>(), capacity)
                    };
                    let size = match guard.read(raw).await {
                        Ok(Some(size)) => size,
                        Ok(None) => 0,
                        Err(err) => return Err(error::map_read_error(error::READ, err)),
                    };
                    (size, capacity)
                }
            };
            if size == 0 {
                return Ok(0);
            }
            debug_assert!(
                size <= capacity,
                "quinn read reported bytes beyond reserved chunk"
            );
            unsafe {
                buf.advance_mut(size);
            }
            Ok(size)
        })
        .await
    }

    pub async fn write(
        &self,
        ctx: &CallContext,
        buf: &mut (dyn Buf + Send + Sync + 'static),
    ) -> spark_core::Result<usize, CoreError> {
        if !buf.has_remaining() {
            return Ok(0);
        }
        let result = run_with_context(ctx, error::WRITE, async {
            let mut guard = self.inner.send.lock().await;
            let mut total = 0usize;
            while buf.has_remaining() {
                let chunk = buf.chunk();
                if chunk.is_empty() {
                    break;
                }
                match guard.write(chunk).await {
                    Ok(0) => break,
                    Ok(size) => {
                        buf.advance(size);
                        total += size;
                    }
                    Err(err) => return Err(error::map_write_error(error::WRITE, err)),
                }
            }
            Ok(total)
        })
        .await?;

        if let Ok(mut state) = self.inner.backpressure.lock() {
            state.on_ready();
        }
        Ok(result)
    }

    pub async fn flush(&self, ctx: &CallContext) -> spark_core::Result<(), CoreError> {
        if deadline_expired(ctx.deadline()) {
            return Err(error::timeout_error(FLUSH));
        }
        if ctx.cancellation().is_cancelled() {
            return Err(error::cancelled_error(FLUSH));
        }
        Ok(())
    }

    pub async fn shutdown(
        &self,
        ctx: &CallContext,
        direction: ShutdownDirection,
    ) -> spark_core::Result<(), CoreError> {
        run_with_context(ctx, error::SHUTDOWN, async {
            match direction {
                ShutdownDirection::Write => self.finish_send().await,
                ShutdownDirection::Read => self.stop_receive().await,
                ShutdownDirection::Both => {
                    self.finish_send().await?;
                    self.stop_receive().await
                }
            }
        })
        .await?;
        if let Ok(mut state) = self.inner.backpressure.lock() {
            state.on_ready();
        }
        Ok(())
    }

    pub fn poll_ready(&self, ctx: &ExecutionView<'_>) -> PollReady<CoreError> {
        if deadline_expired(ctx.deadline()) {
            return Poll::Ready(ReadyCheck::Err(error::timeout_error(error::POLL_READY)));
        }
        if ctx.cancellation().is_cancelled() {
            return Poll::Ready(ReadyCheck::Err(error::cancelled_error(error::POLL_READY)));
        }
        if let Some(remaining) = deadline_remaining(ctx.deadline())
            && remaining.is_zero()
        {
            return Poll::Ready(ReadyCheck::Err(error::timeout_error(error::POLL_READY)));
        }

        let mut state = match self.inner.backpressure.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };

        match self.inner.send.try_lock() {
            Ok(mut guard) => {
                let waker = noop_waker_ref();
                let mut cx = TaskContext::from_waker(waker);
                match Pin::new(&mut *guard).poll_write(&mut cx, &[]) {
                    Poll::Ready(Ok(_)) => {
                        let ready = state.on_ready();
                        Poll::Ready(ReadyCheck::Ready(ready))
                    }
                    Poll::Ready(Err(err)) => {
                        drop(guard);
                        let core_error = error::map_write_error(error::POLL_READY, err);
                        Poll::Ready(ReadyCheck::Err(core_error))
                    }
                    Poll::Pending => {
                        drop(guard);
                        let stats = self.inner.connection.stats();
                        let ready_state = state.on_blocked(&stats);
                        Poll::Ready(ReadyCheck::Ready(ready_state))
                    }
                }
            }
            Err(_) => {
                let ready_state = state.on_manual_busy();
                Poll::Ready(ReadyCheck::Ready(ready_state))
            }
        }
    }

    pub fn peer_addr(&self) -> TransportSocketAddr {
        self.inner.peer_addr
    }

    pub fn local_addr(&self) -> TransportSocketAddr {
        self.inner.local_addr
    }

    async fn finish_send(&self) -> spark_core::Result<(), CoreError> {
        let mut guard = self.inner.send.lock().await;
        match guard.finish() {
            Ok(_) => Ok(()),
            Err(_) => Ok(()),
        }
    }

    async fn stop_receive(&self) -> spark_core::Result<(), CoreError> {
        let mut guard = self.inner.recv.lock().await;
        if guard.stop(VarInt::from_u32(0)).is_err() {
            // 若流已关闭或重复调用，忽略错误以保持幂等。
        }
        Ok(())
    }
}

impl Channel for QuicChannel {
    type Error = CoreError;
    type CallCtx<'ctx> = CallContext;
    type ReadyCtx<'ctx> = ExecutionView<'ctx>;

    type ReadFuture<'ctx>
        = Pin<
        Box<dyn core::future::Future<Output = spark_core::Result<usize, CoreError>> + Send + 'ctx>,
    >
    where
        Self: 'ctx,
        Self::CallCtx<'ctx>: 'ctx;

    type WriteFuture<'ctx>
        = Pin<
        Box<dyn core::future::Future<Output = spark_core::Result<usize, CoreError>> + Send + 'ctx>,
    >
    where
        Self: 'ctx,
        Self::CallCtx<'ctx>: 'ctx;

    type ShutdownFuture<'ctx>
        =
        Pin<Box<dyn core::future::Future<Output = spark_core::Result<(), CoreError>> + Send + 'ctx>>
    where
        Self: 'ctx,
        Self::CallCtx<'ctx>: 'ctx;

    type FlushFuture<'ctx>
        =
        Pin<Box<dyn core::future::Future<Output = spark_core::Result<(), CoreError>> + Send + 'ctx>>
    where
        Self: 'ctx,
        Self::CallCtx<'ctx>: 'ctx;

    fn id(&self) -> Cow<'_, str> {
        Cow::Owned(format!(
            "quic:{}->{}",
            self.inner.local_addr, self.inner.peer_addr
        ))
    }

    fn peer_addr(&self) -> Option<TransportSocketAddr> {
        Some(self.inner.peer_addr)
    }

    fn local_addr(&self) -> Option<TransportSocketAddr> {
        Some(self.inner.local_addr)
    }

    fn read<'ctx>(
        &'ctx self,
        ctx: &'ctx Self::CallCtx<'ctx>,
        buf: &'ctx mut (dyn BufMut + Send + Sync + 'static),
    ) -> Self::ReadFuture<'ctx> {
        Box::pin(async move { QuicChannel::read(self, ctx, buf).await })
    }

    fn write<'ctx>(
        &'ctx self,
        ctx: &'ctx Self::CallCtx<'ctx>,
        buf: &'ctx mut (dyn Buf + Send + Sync + 'static),
    ) -> Self::WriteFuture<'ctx> {
        Box::pin(async move { QuicChannel::write(self, ctx, buf).await })
    }

    fn flush<'ctx>(&'ctx self, ctx: &'ctx Self::CallCtx<'ctx>) -> Self::FlushFuture<'ctx> {
        Box::pin(async move { QuicChannel::flush(self, ctx).await })
    }

    fn shutdown<'ctx>(
        &'ctx self,
        ctx: &'ctx Self::CallCtx<'ctx>,
        direction: ShutdownDirection,
    ) -> Self::ShutdownFuture<'ctx> {
        Box::pin(async move { QuicChannel::shutdown(self, ctx, direction).await })
    }

    fn classify_backpressure(
        &self,
        ctx: &Self::ReadyCtx<'_>,
        _metrics: &BackpressureMetrics,
    ) -> BackpressureDecision {
        match self.poll_ready(ctx) {
            Poll::Pending => BackpressureDecision::Busy,
            Poll::Ready(ReadyCheck::Ready(ReadyState::Ready)) => BackpressureDecision::Ready,
            Poll::Ready(ReadyCheck::Ready(ReadyState::Busy(_))) => BackpressureDecision::Busy,
            Poll::Ready(ReadyCheck::Ready(ReadyState::RetryAfter(advice))) => {
                BackpressureDecision::RetryAfter { delay: advice.wait }
            }
            Poll::Ready(ReadyCheck::Ready(ReadyState::BudgetExhausted(_))) => {
                BackpressureDecision::BudgetExhausted
            }
            Poll::Ready(ReadyCheck::Err(_)) => BackpressureDecision::Rejected,
            Poll::Ready(_) => BackpressureDecision::Busy,
        }
    }
}

#[allow(dead_code)]
fn _assert_quic_channel_contract()
where
    QuicChannel: Channel<Error = CoreError>,
{
}
