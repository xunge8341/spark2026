use core::time::Duration;

use libm::pow;

/// 自适应 `RetryAfter` 计算核心：依据拥塞度（backlog）、观测 RTT 与错误建议的基础等待时间，
/// 生成带抖动、饱和上限与冷却下限的最终等待窗口。
///
/// # 教案式说明
/// - **意图（Why）**：`ErrorCategory::Retryable` 仅提供静态等待时间，若忽视实时拥塞指标，
///   大规模调用方会在同一时间重试，进一步拉高队列长度与 RTT。本函数将 `RetryAdvice` 的
///   基线时长与运行时反馈组合，形成自适应节律，降低集体“惊群”风险。
/// - **定位（Where）**：供 `spark_core::error` 默认策略与上层节律追踪器调用，属于
///   `spark_core::retry::adaptive` 策略的入口。
/// - **契约（What）**：
///   - `backlog`：浮点表示的拥塞程度，0 表示空闲，数值越大代表队列越长；允许 >=0 的任意实数，
///     负值将被夹到 0，正无穷视为饱和；
///   - `rtt`：最近观测到的平均往返延迟；
///   - `base`：来自错误建议的基础等待时间；
///   - **返回**：综合调整后的等待时长，保证 >= `base` 的冷却下限，且不会超过策略约定的最大退避。
/// - **前置条件**：调用方需提供由同一时间源采集的 RTT 与队列统计，以避免跨时钟误差；
///   `base` 可为零，函数内部会回退至默认冷却窗口。
/// - **后置条件**：输出时长在 `[cooldown_floor, max_backoff]` 范围内，并引入轻微抖动防止同步；
///   抖动为确定性伪随机数，保证在相同输入下重复调用的可测试性。
/// - **实现策略（How）**：
///   1. 设定冷却下限 `MIN_COOLDOWN` 与上限 `MAX_WAIT`，将 `base` 夹紧在该区间内；
///   2. 将 `backlog` 归一化到 `[0, BACKLOG_CEILING]`，按指数函数放大高拥塞区间的权重；
///   3. 将 `rtt` 与基准 RTT (`BASELINE_RTT`) 比较，生成额外放大系数；
///   4. 叠乘拥塞权重、RTT 权重与基础时间，得到未抖动的等待窗口；
///   5. 通过 SplitMix64 生成确定性抖动，幅度控制在 ±5%，并最终夹紧到合法区间；
///   6. 输出 `Duration`，供上层节律追踪器复用。
/// - **权衡与注意事项（Trade-offs & Gotchas）**：
///   - 采用浮点运算换取平滑调节，已通过 `clamp` 避免溢出；在 `no_std + alloc` 环境下仍可使用；
///   - 抖动为确定性伪随机，以确保测试可重复；若未来引入真实随机源，可在外层注入种子；
///   - 饱和上限设置为 3 秒，兼顾快速恢复与防止极端拥塞时无限拉长；
///   - 若要引入指数退避，只需调整权重函数或在外层包裹迭代器，无需修改本核心逻辑。
pub fn compute(backlog: f32, rtt: Duration, base: Duration) -> Duration {
    let cooled_base = if base < MIN_COOLDOWN {
        MIN_COOLDOWN
    } else {
        base
    };

    let backlog_value = if backlog.is_finite() {
        backlog
    } else {
        BACKLOG_CEILING as f32
    };
    let backlog_units = if backlog_value < 0.0 {
        0.0
    } else {
        backlog_value as f64
    };
    let capped_backlog = if backlog_units > BACKLOG_CEILING {
        BACKLOG_CEILING
    } else {
        backlog_units
    };
    let backlog_pressure = 1.0 + BACKLOG_WEIGHT * pow(capped_backlog / BACKLOG_CEILING, 1.35);

    let baseline_rtt_secs = BASELINE_RTT.as_secs_f64();
    let observed_rtt_secs = rtt.as_secs_f64();
    let rtt_ratio = if baseline_rtt_secs > 0.0 {
        clamp_f64(observed_rtt_secs / baseline_rtt_secs, 0.0, MAX_RTT_RATIO)
    } else {
        1.0
    };
    let rtt_pressure = 1.0 + RTT_WEIGHT * rtt_ratio;

    let mut wait_secs = cooled_base.as_secs_f64() * backlog_pressure * rtt_pressure;

    let jitter_seed =
        mix64((capped_backlog.to_bits()) ^ fold_duration(rtt) ^ fold_duration(cooled_base));
    let jitter = jitter_factor(jitter_seed);
    wait_secs *= jitter;

    wait_secs = clamp_f64(wait_secs, cooled_base.as_secs_f64(), MAX_WAIT.as_secs_f64());

    Duration::from_secs_f64(wait_secs)
}

const MIN_COOLDOWN: Duration = Duration::from_millis(40);
const MAX_WAIT: Duration = Duration::from_secs(3);
const BACKLOG_CEILING: f64 = 4.0;
const BACKLOG_WEIGHT: f64 = 0.65;
const RTT_WEIGHT: f64 = 0.35;
const BASELINE_RTT: Duration = Duration::from_millis(55);
const MAX_RTT_RATIO: f64 = 6.0;
const JITTER_RANGE: f64 = 0.05;

#[inline]
fn clamp_f64(value: f64, min: f64, max: f64) -> f64 {
    if value < min {
        min
    } else if value > max {
        max
    } else {
        value
    }
}

#[inline]
fn fold_duration(duration: Duration) -> u64 {
    let nanos = duration.as_nanos();
    let upper = (nanos >> 64) as u64;
    let lower = nanos as u64;
    upper ^ lower
}

#[inline]
fn jitter_factor(seed: u64) -> f64 {
    let mixed = mix64(seed);
    let mantissa = (mixed >> 11) as f64;
    let unit = mantissa / ((1u64 << 53) as f64);
    1.0 + (unit * 2.0 - 1.0) * JITTER_RANGE
}

#[inline]
fn mix64(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9E3779B97F4A7C15);
    x = (x ^ (x >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94D049BB133111EB);
    x ^ (x >> 31)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn adaptive_wait_respects_floor_and_cap() {
        let zero_base = Duration::from_millis(0);
        let wait = compute(0.0, Duration::from_millis(10), zero_base);
        assert!(wait >= MIN_COOLDOWN);
        let wait = compute(10.0, Duration::from_secs(10), Duration::from_secs(5));
        assert!(wait <= MAX_WAIT);
    }
}
