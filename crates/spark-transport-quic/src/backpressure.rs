use quinn::ConnectionStats;
use spark_core::prelude::{BusyReason, ReadyState, RetryAdvice};
use std::time::{Duration, Instant};

/// QUIC 写路径背压探测器。
///
/// # 教案式注释
///
/// ## 意图（Why）
/// - **目标**：将 QUIC 协议栈中的拥塞窗口、流控帧等原始指标转化为框架统一的
///   [`ReadyState`] 信号，帮助上层调度在多路复用场景下做出退避决策。
/// - **架构位置**：位于 `spark-transport-quic` 内部，由 [`crate::channel::QuicChannel`]
///   持有并驱动，是 TCP 版本 `BackpressureState` 的 QUIC 对应实现。
/// - **设计理念**：通过采样 `ConnectionStats`，以纯函数方式判断“拥塞（Busy）”与
///   “需要冷却（RetryAfter）”两种状态，避免直接暴露底层细节。
///
/// ## 逻辑（How）
/// 1. 每次探测调用 `refresh` 以时间窗口衰减历史计数，防止旧事件长期影响判断；
/// 2. `on_blocked` 比对本次与上次快照的 `DATA_BLOCKED`/`STREAM_DATA_BLOCKED` 计数，
///    若出现增量则判定为“流控限制”，返回 `ReadyState::RetryAfter`；
/// 3. 若未触发流控但拥塞窗口（`cwnd`）逼近阈值，返回携带自定义原因的
///    `ReadyState::Busy`，提示上层存在拥塞趋势；
/// 4. 连续多次触发 `Blocked` 时，以线性退避计算冷却时间，避免热流反复抢占窗口；
/// 5. `on_manual_busy` 用于 `poll_ready` 在无法获取写锁时的兜底，统一映射为 Busy。
///
/// ## 契约（What）
/// - `on_ready`：写入成功后调用，重置内部计数，返回 `ReadyState::Ready`；
/// - `on_blocked`：写入因流控/拥塞受阻时调用，根据 `ConnectionStats` 推导状态；
/// - `on_manual_busy`：锁竞争导致写入推迟时调用，返回 Busy 状态；
/// - **前置条件**：调用方需保证三个方法在同一任务上下文中串行执行，避免竞态；
/// - **后置条件**：内部状态自洽，不会因时间漂移或计数溢出导致错误判断。
///
/// ## 风险与注意（Trade-offs）
/// - 线性退避参数基于实验值，若部署环境 RTT 显著不同，可视情况调优常量；
/// - `ConnectionStats` 为瞬时快照，存在轻微滞后；在极端高并发场景下需结合额外指标；
/// - 当前实现未区分 0-RTT 与 1-RTT 流，若未来需要差异化策略，可在快照中记录额外标记。
#[derive(Debug)]
pub(crate) struct QuicBackpressure {
    snapshot: Option<Snapshot>,
    consecutive_blocked: u32,
    last_update: Option<Instant>,
}

#[derive(Clone, Copy, Debug, Default)]
struct Snapshot {
    data_blocked: u64,
    stream_blocked: u64,
    cwnd: u64,
}

impl Snapshot {
    fn from_stats(stats: &ConnectionStats) -> Self {
        Self {
            data_blocked: stats.frame_tx.data_blocked,
            stream_blocked: stats.frame_tx.stream_data_blocked,
            cwnd: stats.path.cwnd,
        }
    }

    fn flow_control_increment(&self, previous: Option<Snapshot>) -> bool {
        previous
            .map(|last| {
                self.data_blocked > last.data_blocked || self.stream_blocked > last.stream_blocked
            })
            .unwrap_or_else(|| self.data_blocked > 0 || self.stream_blocked > 0)
    }
}

impl QuicBackpressure {
    pub(crate) fn new() -> Self {
        Self {
            snapshot: None,
            consecutive_blocked: 0,
            last_update: None,
        }
    }

    pub(crate) fn on_ready(&mut self) -> ReadyState {
        self.consecutive_blocked = 0;
        self.last_update = Some(Instant::now());
        ReadyState::Ready
    }

    pub(crate) fn on_manual_busy(&mut self) -> ReadyState {
        self.refresh();
        self.consecutive_blocked = self.consecutive_blocked.saturating_add(1);
        ReadyState::Busy(BusyReason::custom(MANUAL_BUSY_REASON))
    }

    pub(crate) fn on_blocked(&mut self, stats: &ConnectionStats) -> ReadyState {
        self.refresh();
        self.consecutive_blocked = self.consecutive_blocked.saturating_add(1);

        let snapshot = Snapshot::from_stats(stats);
        let flow_control = snapshot.flow_control_increment(self.snapshot);
        self.snapshot = Some(snapshot);
        self.last_update = Some(Instant::now());

        if flow_control {
            let wait = retry_delay(self.consecutive_blocked);
            return ReadyState::RetryAfter(
                RetryAdvice::after(wait).with_reason(FLOW_CONTROL_REASON),
            );
        }

        if snapshot.cwnd < CWND_LOW_WATERMARK {
            return ReadyState::Busy(BusyReason::custom(CONGESTION_TREND_REASON));
        }

        if self.consecutive_blocked > RETRY_THRESHOLD {
            let level = self.consecutive_blocked - RETRY_THRESHOLD;
            let wait = retry_delay(level);
            return ReadyState::RetryAfter(
                RetryAdvice::after(wait).with_reason(CONGESTION_COOLDOWN_REASON),
            );
        }

        ReadyState::Busy(BusyReason::downstream())
    }

    fn refresh(&mut self) {
        if let Some(last) = self.last_update
            && last.elapsed() > BLOCKED_DECAY
        {
            self.consecutive_blocked = 0;
            self.snapshot = None;
            self.last_update = None;
        }
    }
}

fn retry_delay(level: u32) -> Duration {
    let clamped = level.clamp(1, RETRY_LEVEL_CAP);
    Duration::from_millis((clamped as u64) * RETRY_UNIT_MS)
}

const MANUAL_BUSY_REASON: &str = "quic send stream mutex is held by another task";
const FLOW_CONTROL_REASON: &str = "quic stream blocked by peer flow control";
const CONGESTION_TREND_REASON: &str = "quic congestion window is nearing low watermark";
const CONGESTION_COOLDOWN_REASON: &str = "quic congestion window saturated";
const BLOCKED_DECAY: Duration = Duration::from_millis(200);
const RETRY_THRESHOLD: u32 = 3;
const RETRY_UNIT_MS: u64 = 10;
const RETRY_LEVEL_CAP: u32 = 20;
const CWND_LOW_WATERMARK: u64 = 12 * 1200;
