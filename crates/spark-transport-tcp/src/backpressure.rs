use spark_core::prelude::{BusyReason, ReadyState, RetryAdvice};
use std::time::{Duration, Instant};

/// 管理 TCP 写路径的背压统计。
///
/// # 教案式注释
///
/// ## 意图 (Why)
/// - 聚合最近一次 `WouldBlock` 或锁竞争的事件时间戳与计数，
///   将其转化为统一的 [`ReadyState`]，供上层调度判断是否需要退避。
/// - 通过统一的节奏控制，避免在套接字暂时拥塞时造成忙等或
///   级联重试风暴。
///
/// ## 逻辑 (How)
/// - 使用 `consecutive_would_block` 记录连续 `WouldBlock` 次数；
/// - 使用 `manual_busy` 记录由于写锁被占用而无法立即写入的次数；
/// - `refresh` 会在事件间隔超过 `WOULD_BLOCK_DECAY` 时重置计数，
///   保持历史数据不过度影响当前状态；
/// - `on_would_block`、`on_manual_busy` 分别将计数映射为 `Busy`
///   或 `RetryAfter`；`on_ready` 在写入成功后清零状态。
///
/// ## 契约 (What)
/// - `on_would_block`：在探测到 `WouldBlock` 时调用，返回相应的
///   [`ReadyState`]；
/// - `on_manual_busy`：在写锁被其他任务持有时调用；
/// - `on_ready`：在写入成功或通道空闲后调用，重置统计；
/// - **前置条件**：调用方需确保这些方法在同一线程序列化调用，
///   以避免竞态；
/// - **后置条件**：内部状态保持自洽，不会溢出或倒退。
///
/// ## 注意事项 (Trade-offs)
/// - 当前实现采用固定阈值与线性退避；未来若需指数退避或自适应，
///   可在 `to_ready_state` 内扩展策略；
/// - 由于使用 `Instant` 做时间窗口，若系统时钟发生跳变，可能导致
///   统计被提前或延后重置。
#[derive(Debug)]
pub(crate) struct BackpressureState {
    consecutive_would_block: u32,
    manual_busy: u32,
    last_event: Option<Instant>,
}

impl BackpressureState {
    /// 创建默认状态。
    pub(crate) fn new() -> Self {
        Self {
            consecutive_would_block: 0,
            manual_busy: 0,
            last_event: None,
        }
    }

    /// 根据时间窗口刷新状态，避免旧事件长期影响当前判断。
    pub(crate) fn refresh(&mut self) {
        if let Some(last) = self.last_event {
            if last.elapsed() > WOULD_BLOCK_DECAY {
                self.consecutive_would_block = 0;
                self.manual_busy = 0;
                self.last_event = None;
            }
        } else {
            self.manual_busy = 0;
        }
    }

    /// 写操作成功后重置统计。
    pub(crate) fn on_ready(&mut self) {
        self.consecutive_would_block = 0;
        self.manual_busy = 0;
        self.last_event = None;
    }

    /// 记录一次锁竞争导致的忙碌信号。
    pub(crate) fn on_manual_busy(&mut self) -> ReadyState {
        self.manual_busy = self.manual_busy.saturating_add(1);
        self.last_event = Some(Instant::now());
        self.to_ready_state(self.manual_busy, LOCKED_REASON)
    }

    /// 记录一次 `WouldBlock`，并返回对应的背压信号。
    pub(crate) fn on_would_block(&mut self) -> ReadyState {
        let now = Instant::now();
        if let Some(last) = self.last_event
            && now.duration_since(last) > WOULD_BLOCK_DECAY
        {
            self.consecutive_would_block = 0;
        }
        self.consecutive_would_block = self.consecutive_would_block.saturating_add(1);
        self.manual_busy = 0;
        self.last_event = Some(now);
        self.to_ready_state(self.consecutive_would_block, WOULD_BLOCK_REASON)
    }

    fn to_ready_state(&self, counter: u32, reason: &'static str) -> ReadyState {
        if counter >= RETRY_AFTER_THRESHOLD {
            let factor = (counter - RETRY_AFTER_THRESHOLD + 1) as u64;
            let wait =
                Duration::from_millis((factor * RETRY_AFTER_UNIT_MS).min(RETRY_AFTER_MAX_MS));
            ReadyState::RetryAfter(RetryAdvice::after(wait).with_reason(reason))
        } else {
            ReadyState::Busy(BusyReason::downstream())
        }
    }
}

const WOULD_BLOCK_DECAY: Duration = Duration::from_millis(200);
const RETRY_AFTER_THRESHOLD: u32 = 3;
const RETRY_AFTER_UNIT_MS: u64 = 5;
const RETRY_AFTER_MAX_MS: u64 = 100;
const LOCKED_REASON: &str = "tcp writer is held by another task";
const WOULD_BLOCK_REASON: &str = "tcp socket send buffer reported WouldBlock";
