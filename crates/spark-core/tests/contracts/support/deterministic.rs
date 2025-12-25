use std::time::Duration;

use spark_core::runtime::MonotonicTimePoint;

/// 确定性线性同余发生器，供契约测试按固定种子重放调度序列。
///
/// # 设计背景（Why）
/// - T15 任务要求“相同种子重放一致”，因此需要一个无全局状态的伪随机数生成器；
/// - 选择 LCG（Linear Congruential Generator）算法是因为其实现轻量，且在测试中已足够表达“不同但可复现”的事件序列。
///
/// # 实现逻辑（How）
/// - `next()` 基于 `state = state * A + C`（mod 2^64）生成下一状态；
/// - `next_in_range(upper)` 通过模运算裁剪到 `[0, upper)`，用于派生事件类型或时间抖动；
/// - 算法常量来自 `Numerical Recipes`，经验证拥有良好的周期性，确保 100 次重放不会出现循环过短的问题。
///
/// # 输入/输出契约（What）
/// - **参数**：`seed` 作为初始状态，只要输入一致就能复现同一序列；
/// - **返回值**：`next()` 返回未经裁剪的 `u64`，`next_in_range` 返回指定范围内的 `u64`；
/// - **前置条件**：`upper` 必须大于 0，否则会 panic（测试场景视为编程错误）；
/// - **后置条件**：多次调用不会修改外部状态，仅更新内部 `state`。
///
/// # 风险与取舍（Trade-offs）
/// - LCG 并非密码学安全，但用于测试足够；
/// - 若未来需要跨语言重放，可复用同一常量以保持一致；
/// - 如需更高质量分布，可替换为 PCG/Xoshiro，同时保留 `seed` 接口以兼容旧测试。
#[derive(Clone, Debug)]
pub(crate) struct Lcg64 {
    state: u64,
}

impl Lcg64 {
    /// 创建发生器。
    pub(crate) fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    /// 产生下一组原始随机数。
    pub(crate) fn next(&mut self) -> u64 {
        // Numerical Recipes 推荐的 64 位常量。
        const A: u64 = 636_413_622_384_679_3005;
        const C: u64 = 1;
        self.state = self.state.wrapping_mul(A).wrapping_add(C);
        self.state
    }

    /// 产生 `[0, upper)` 范围内的确定性值。
    pub(crate) fn next_in_range(&mut self, upper: u64) -> u64 {
        assert!(upper > 0, "LCG 上界必须大于 0");
        self.next() % upper
    }
}

/// 确定性单调时钟，配合 LCG 构造可复现的时间线。
///
/// # 设计背景（Why）
/// - 合约测试需要精确控制“当前时间”，以验证截止时间与取消之间的竞态；
/// - 使用真实系统时钟会引入抖动，导致 100 次重放无法保证完全一致。
///
/// # 实现逻辑（How）
/// - 内部仅维护当前 `MonotonicTimePoint`，默认从 0 偏移启动；
/// - `advance(delta)` 饱和相加并返回更新后的时间点，供测试记录；
/// - `now()` 提供只读视图，用于与 `Deadline`/`Cancellation` 组合断言。
///
/// # 输入/输出契约（What）
/// - **参数**：`delta` 为推进时间的持续时长，必须符合 `Duration` 的约束；
/// - **返回值**：`advance` 返回推进后的 `MonotonicTimePoint`，`now` 返回当前时间；
/// - **前置条件**：调用方需保证多次调用使用同一实例，以维护单调性；
/// - **后置条件**：时间点只会前进，不会回拨。
///
/// # 风险与取舍（Trade-offs）
/// - 依赖 `saturating_add` 防止溢出；若测试传入极端大值仍可能触达 `Duration::MAX`，届时再调用 `advance` 将停留在最大值；
/// - 未实现倒退接口，符合单调时钟的契约；若未来需要模拟回拨，可新增独立方法避免破坏已有测试。
#[derive(Clone, Debug)]
pub(crate) struct DeterministicClock {
    now: MonotonicTimePoint,
}

impl DeterministicClock {
    /// 构造起始时间为 0 的时钟。
    pub(crate) fn new() -> Self {
        Self {
            now: MonotonicTimePoint::from_offset(Duration::from_secs(0)),
        }
    }

    /// 返回当前时间点。
    pub(crate) fn now(&self) -> MonotonicTimePoint {
        self.now
    }

    /// 按指定持续时间推进时钟。
    pub(crate) fn advance(&mut self, delta: Duration) -> MonotonicTimePoint {
        self.now = self.now.saturating_add(delta);
        self.now
    }
}
