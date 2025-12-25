use spark_core::contract::Deadline;

/// 验证截止时间在不同时间点的超时判定，确保时间窗口稳定。
///
/// # 测试目标（Why）
/// - 确保截止时间在“未到达”“恰好到达”“超过”三个阶段分别返回正确的超时结果；
/// - 防止运行时回拨或计时器精度差导致的误判情况。
///
/// # 测试步骤（How）
/// 1. 构造当前时间 `now` 与未来时间 `deadline_at`；
/// 2. 调用 [`Deadline::at`] 创建截止实例；
/// 3. 使用 [`assert_deadline_window!`] 验证在不同时间点调用 `is_expired` 的结果。
///
/// # 输入/输出契约（What）
/// - **前置条件**：时间点均来源于同一计时源（本测试使用辅助函数 `monotonic`）；
/// - **后置条件**：截止前返回 `false`，截止点及之后返回 `true`；
/// - **风险提示**：若截止判定不稳定，将导致预算与取消逻辑失去统一依据。
#[test]
fn deadline_expiration_window_contract() {
    let now = super::support::monotonic(5, 0);
    let deadline_at = super::support::monotonic(10, 0);
    let deadline = Deadline::at(deadline_at);

    assert_deadline_window! {
        deadline,
        before: now => false,
        at: deadline_at => true,
        after: super::support::monotonic(12, 0) => true,
    };
}

/// 验证未设定截止时间的调用永远不会被判定超时。
///
/// # 测试目标（Why）
/// - `Deadline::none` 是调用方显式声明“无硬超时”的手段，测试需确保任意时间点都不会误报。
///
/// # 测试步骤（How）
/// 1. 创建 `Deadline::none()`；
/// 2. 在多个时间点调用 `is_expired`，确认始终返回 `false`；
/// 3. 追加一个极大时间点，模拟长时间运行仍保持未过期。
///
/// # 输入/输出契约（What）
/// - **前置条件**：无额外依赖；
/// - **后置条件**：任意时间点均返回 `false`；
/// - **风险提示**：若实现错误返回 `true`，会导致上层错误地取消长任务。
#[test]
fn deadline_none_never_expires() {
    let deadline = Deadline::none();
    let checkpoints = [
        super::support::monotonic(0, 0),
        super::support::monotonic(50, 0),
        super::support::monotonic(5_000, 0),
    ];

    for point in checkpoints {
        assert!(
            !deadline.is_expired(point),
            "无截止时间的上下文不应超时"
        );
    }
}

/// 验证 `with_timeout` 语义：在当前时间基础上饱和相加持续时间。
///
/// # 测试目标（Why）
/// - 确保 `with_timeout` 能在时间接近上限时正确饱和，避免溢出导致的回绕。
///
/// # 测试步骤（How）
/// 1. 设置当前时间点 `now`；
/// 2. 调用 `with_timeout` 构造截止时间；
/// 3. 检查 `instant()` 返回值与手动计算结果一致，且在超长持续时间下表现稳定。
///
/// # 输入/输出契约（What）
/// - **前置条件**：持续时间不应超过 `Duration::MAX`；
/// - **后置条件**：返回的 `Deadline` 与 `saturating_add` 行为一致；
/// - **风险提示**：若饱和逻辑缺失会导致超时时间回绕，影响安全策略。
#[test]
fn deadline_with_timeout_respects_saturation() {
    let now = super::support::monotonic(1, 0);
    let timeout = std::time::Duration::from_secs(4);
    let deadline = Deadline::with_timeout(now, timeout);
    assert_eq!(
        deadline.instant(),
        Some(now.saturating_add(timeout)),
        "with_timeout 结果应与 saturating_add 一致"
    );

    let long_timeout = std::time::Duration::from_secs(u64::MAX);
    let saturated = Deadline::with_timeout(now, long_timeout);
    assert_eq!(
        saturated.instant(),
        Some(now.saturating_add(long_timeout)),
        "超长持续时间也必须遵循饱和语义"
    );
}
