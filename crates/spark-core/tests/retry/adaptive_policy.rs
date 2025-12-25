pub mod adaptive_policy {
    use core::time::Duration;

    use spark_core::retry::adaptive::compute;

    /// # 教案式说明：拥塞回放测试
    ///
    /// - **意图（Why）**：模拟调用队列由空闲逐步饱和、再缓慢恢复的过程，检验自适应退避在动态场景
    ///   下是否维持单调趋势、避免过度波动，从而降低“误触发”即：拥塞上升时等待反而缩短、拥塞下降
    ///   时等待反而拉长。
    /// - **契约（What）**：
    ///   - 误触发率需低于 10%，且波动幅度在 5% 容忍区间内被忽略；
    ///   - 退避时长必须落在 `compute` 所定义的上下限之间；
    ///   - RTT 采用线性插值模拟跨机房延迟逐渐抖动的场景。
    /// - **实现（How）**：
    ///   1. 生成拥塞爬升（0.0 → 3.5）与回落（3.5 → 0.0）的两个序列；
    ///   2. 对每个时间片调用 `compute`，记录返回的 `Duration`；
    ///   3. 对相邻窗口比较，统计违反单调性的次数并计算误触发率；
    ///   4. 使用 5% 的相对阈值过滤掉由 ±5% 抖动造成的可接受波动；
    ///   5. 通过断言约束确保策略调节满足治理需求。
    /// - **权衡（Trade-offs & Gotchas）**：
    ///   - 抖动是确定性的伪随机，测试使用相同输入可复现结果；
    ///   - `Tolerance` 设置为 5%，略高于抖动 5% 的幅度上限，可过滤可接受噪声并捕捉显著逆势波动；
    ///   - 若未来调整常量，需同步更新本文档顶部的阈值说明和测试断言。
    const BASE_WAIT: Duration = Duration::from_millis(180);
    const FALSE_RATE_THRESHOLD: f64 = 0.10;
    const RELATIVE_TOLERANCE: f64 = 0.05;

    #[test]
    fn congestion_ramp_up_false_positive_rate_below_threshold() {
        let backlogs = linspace(0.0, 3.5, 60);
        let rtts = linspace_duration(Duration::from_millis(40), Duration::from_millis(260), 60);
        let waits = playback(&backlogs, &rtts);

        assert!(waits.iter().all(|w| *w >= Duration::from_millis(40)));
        assert!(waits.iter().all(|w| *w <= Duration::from_secs(3)));

        let rate = false_positive_rate_increasing(&waits, RELATIVE_TOLERANCE);
        assert!(rate <= FALSE_RATE_THRESHOLD, "false positive rate {} > {}", rate, FALSE_RATE_THRESHOLD);
    }

    #[test]
    fn congestion_ramp_down_false_positive_rate_below_threshold() {
        let backlogs = linspace(3.5, 0.0, 60);
        let rtts = linspace_duration(Duration::from_millis(260), Duration::from_millis(40), 60);
        let waits = playback(&backlogs, &rtts);

        assert!(waits.iter().all(|w| *w >= Duration::from_millis(40)));
        assert!(waits.iter().all(|w| *w <= Duration::from_secs(3)));

        let rate = false_positive_rate_decreasing(&waits, RELATIVE_TOLERANCE);
        assert!(rate <= FALSE_RATE_THRESHOLD, "false positive rate {} > {}", rate, FALSE_RATE_THRESHOLD);
    }

    fn playback(backlogs: &[f32], rtts: &[Duration]) -> Vec<Duration> {
        backlogs
            .iter()
            .zip(rtts.iter())
            .map(|(&backlog, &rtt)| compute(backlog, rtt, BASE_WAIT))
            .collect()
    }

    fn false_positive_rate_increasing(series: &[Duration], tolerance: f64) -> f64 {
        let mut violations = 0u32;
        let mut total = 0u32;
        for window in series.windows(2) {
            let prev = window[0].as_secs_f64();
            let next = window[1].as_secs_f64();
            total += 1;
            if next + prev * tolerance < prev {
                violations += 1;
            }
        }
        if total == 0 {
            0.0
        } else {
            violations as f64 / total as f64
        }
    }

    fn false_positive_rate_decreasing(series: &[Duration], tolerance: f64) -> f64 {
        let mut violations = 0u32;
        let mut total = 0u32;
        for window in series.windows(2) {
            let prev = window[0].as_secs_f64();
            let next = window[1].as_secs_f64();
            total += 1;
            if next > prev * (1.0 + tolerance) {
                violations += 1;
            }
        }
        if total == 0 {
            0.0
        } else {
            violations as f64 / total as f64
        }
    }

    fn linspace(start: f32, end: f32, steps: usize) -> Vec<f32> {
        let mut values = Vec::with_capacity(steps);
        if steps == 0 {
            return values;
        }
        let step = if steps == 1 { 0.0 } else { (end - start) / (steps as f32 - 1.0) };
        for i in 0..steps {
            values.push(start + step * i as f32);
        }
        values
    }

    fn linspace_duration(start: Duration, end: Duration, steps: usize) -> Vec<Duration> {
        let mut values = Vec::with_capacity(steps);
        if steps == 0 {
            return values;
        }
        if steps == 1 {
            values.push(start);
            return values;
        }
        let start_ns = start.as_nanos() as f64;
        let end_ns = end.as_nanos() as f64;
        let delta = (end_ns - start_ns) / (steps as f64 - 1.0);
        for i in 0..steps {
            let value = start_ns + delta * i as f64;
            let clamped = value.max(0.0).min(u64::MAX as f64);
            values.push(Duration::from_nanos(clamped as u64));
        }
        values
    }
}
