pub mod deterministic_retry_after {
    //! 时间契约测试：验证 `RetryAfterThrottle` 在虚拟时钟下能够保持确定性的唤醒顺序。
    //!
    //! # 测试目标（Why）
    //! - 确保 `RetryAfter` 节律在 `MockClock` 控制下不会受真实时间抖动影响，满足 CI 对“完全可复现”要求；
    //! - 验证 `Clock::sleep` 返回的 Future 能正确登记并唤醒 waker，保障 `poll_ready` 的协作取消语义。
    //!
    //! # 测试结构（What）
    //! - [`tests::time::deterministic_retry_after::retry_after_sequence_wakes_deterministically`]：
    //!   组合虚拟时钟与 `RetryAfterThrottle` 验证两次建议的唤醒顺序与次数完全一致。
    //!
    //! # 执行步骤（How）
    //! 1. 使用 `MockClock` 构造虚拟时间源，并注入到 [`RetryAfterThrottle`]；
    //! 2. 连续两次发送 `RetryAfter` 建议，分别推进时间到达/未到达截止点；
    //! 3. 记录 waker 唤醒顺序，断言仅在跨过截止点时被触发，且触发次数精确等于建议次数。

    use std::sync::{Arc, Mutex};
    use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};
    use std::time::Duration;

    use spark_core::status::{RetryAdvice, RetryAfterThrottle};
    use spark_core::time::clock::{Clock, MockClock};

    struct Recorder {
        label: &'static str,
        events: Arc<Mutex<Vec<&'static str>>>,
    }

    /// 构造记录唤醒顺序的自定义 waker。
    ///
    /// # 教案式说明
    /// - **意图 (Why)**：捕获 `RetryAfterThrottle` 在虚拟时间推进时触发 waker 的精确序列；
    /// - **契约 (What)**：
    ///   - `events`：共享的事件缓冲区，存储唤醒标签；
    ///   - `label`：标识当前 waker，便于断言多次调用的顺序；
    /// - **实现 (How)**：通过 `RawWaker` 手工实现 `clone`/`wake`/`wake_by_ref`/`drop`，并配合 `Arc` 维持生命周期；
    /// - **风险 (Trade-offs)**：
    ///   - 必须确保 `Arc::from_raw`/`Arc::into_raw` 成对出现以避免提前释放；
    ///   - 若未来需要记录更多上下文，请考虑扩展 `Recorder` 结构而非额外全局状态。
    fn recording_waker(events: Arc<Mutex<Vec<&'static str>>>, label: &'static str) -> Waker {
        unsafe fn clone(data: *const ()) -> RawWaker {
            // SAFETY: `data` 源自 `Arc::into_raw`，此处立即转回 `Arc` 并克隆，随后通过 `into_raw` 归还所有权。
            let arc = unsafe { Arc::<Recorder>::from_raw(data as *const Recorder) };
            let cloned = Arc::clone(&arc);
            let _ = Arc::into_raw(arc);
            RawWaker::new(Arc::into_raw(cloned) as *const (), &VTABLE)
        }

        unsafe fn wake(data: *const ()) {
            // SAFETY: 唤醒路径消耗 waker，对应唯一一次 `from_raw`，释放后无需重新注册。
            let arc = unsafe { Arc::<Recorder>::from_raw(data as *const Recorder) };
            arc.events
                .lock()
                .expect("recorder lock")
                .push(arc.label);
        }

        unsafe fn wake_by_ref(data: *const ()) {
            // SAFETY: `wake_by_ref` 需保留拥有权，因此在末尾重新 `into_raw`，保持引用计数平衡。
            let arc = unsafe { Arc::<Recorder>::from_raw(data as *const Recorder) };
            arc.events
                .lock()
                .expect("recorder lock")
                .push(arc.label);
            let _ = Arc::into_raw(arc);
        }

        unsafe fn drop(data: *const ()) {
            // SAFETY: Drop 最终释放 waker 对 `Arc` 的一次引用，配平 `Arc::into_raw`。
            let _ = unsafe { Arc::<Recorder>::from_raw(data as *const Recorder) };
        }

        static VTABLE: RawWakerVTable = RawWakerVTable::new(clone, wake, wake_by_ref, drop);

        let recorder = Arc::new(Recorder { label, events });
        let ptr = Arc::into_raw(recorder) as *const ();
        unsafe { Waker::from_raw(RawWaker::new(ptr, &VTABLE)) }
    }

    /// 验证两次 `RetryAfter` 建议在虚拟时钟下产生确定性的唤醒序列。
    ///
    /// # 步骤拆解
    /// 1. 构建 `MockClock` 与 `RetryAfterThrottle`，注入自定义 waker；
    /// 2. 推进时间至第一次建议截止点，断言恰好唤醒一次；
    /// 3. 第二次建议后在截止点前推进，确认不会提前唤醒；
    /// 4. 跨过第二次截止点后断言再次唤醒并恢复就绪。
    #[test]
    pub fn retry_after_sequence_wakes_deterministically() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let clock = Arc::new(MockClock::new());
        let dyn_clock: Arc<dyn Clock> = clock.clone();
        let mut throttle = RetryAfterThrottle::new(dyn_clock);

        let waker = recording_waker(Arc::clone(&events), "retry");
        let mut cx = Context::from_waker(&waker);

        let advice_first = RetryAdvice::after(Duration::from_millis(120));
        let advice_second = RetryAdvice::after(Duration::from_millis(90));

        throttle.observe(&advice_first);
        assert!(matches!(throttle.poll(&mut cx), Poll::Pending));

        clock.advance(Duration::from_millis(60));
        assert!(matches!(throttle.poll(&mut cx), Poll::Pending));
        assert!(events.lock().expect("events lock").is_empty());

        clock.advance(Duration::from_millis(60));
        {
            let recorded = events.lock().expect("events lock");
            assert_eq!(recorded.as_slice(), ["retry"], "第一次 RetryAfter 应唤醒一次 waker");
        }
        assert!(matches!(throttle.poll(&mut cx), Poll::Ready(())));

        throttle.observe(&advice_second);
        assert!(matches!(throttle.poll(&mut cx), Poll::Pending));

        clock.advance(Duration::from_millis(80));
        assert!(matches!(throttle.poll(&mut cx), Poll::Pending));
        {
            let recorded = events.lock().expect("events lock");
            assert_eq!(recorded.as_slice(), ["retry"], "提前推进不应触发额外唤醒");
        }

        clock.advance(Duration::from_millis(15));
        {
            let recorded = events.lock().expect("events lock");
            assert_eq!(recorded.as_slice(), ["retry", "retry"], "两次建议应各触发一次唤醒");
        }
        assert!(matches!(throttle.poll(&mut cx), Poll::Ready(())));
        assert!(!throttle.is_waiting(), "等待窗口结束后应恢复就绪");
        assert!(throttle.remaining_delay().is_zero());
    }
}
