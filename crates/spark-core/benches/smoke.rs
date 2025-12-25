use std::env;
use std::hint::black_box;
use std::time::Instant;

/// `cargo bench -- --quick` 对应的基线性能冒烟测试入口。
///
/// # 背景阐释（Why）
/// - 仿照 Criterion Quick Benchmark 约定，提供一个快速完成的 CPU 累加任务，验证管线可执行。
/// - CI 只需验证宿主契约引入后仍可运行基准测试，因此选择常数复杂度的小任务避免测试不稳定。
///
/// # 执行逻辑（How）
/// - 解析命令行参数，识别是否传入 `--quick`，以决定迭代次数。
/// - 调用 [`run_accumulation`] 执行确定性运算并输出耗时，避免 `#[bench]` 依赖 `libtest` 的 `--quick` 解析能力不足。
///
/// # 契约说明（What）
/// - **输入**：命令行参数可包含 `--quick`，未提供时默认进行 100_000 次迭代。
/// - **输出**：标准输出打印两行信息：迭代次数与耗时纳秒。
/// - **前置条件**：运行环境需提供标准库（基准测试默认启用 `std`）。
/// - **后置条件**：函数结束前调用 `black_box` 避免编译器优化掉计算结果。
///
/// # 风险与注意事项（Trade-offs）
/// - 使用 `Instant` 进行高精度计时，但在部分虚拟化平台可能受到时钟粒度限制；可通过扩展为统计型 benchmark 改进。
fn main() {
    let is_quick = env::args().skip(1).any(|arg| arg == "--quick");
    let iterations = if is_quick { 10_000_u64 } else { 100_000_u64 };

    let started = Instant::now();
    let checksum = run_accumulation(iterations);
    let elapsed = started.elapsed();

    println!("smoke_iterations={iterations}");
    println!("smoke_elapsed_ns={}", elapsed.as_nanos());

    // 前置后置条件补充说明：确保累加结果被视为 side effect，防止优化导致计时失真。
    black_box(checksum);
}

/// 执行确定性累加任务，模拟最小的算力消耗。
///
/// # 背景（Why）
/// - 参考 Google Benchmark 中的算术基准，通过大量整数累加验证 CPU 友好性，同时避免 I/O 带来的噪音。
///
/// # 逻辑解析（How）
/// - 从 `0..iterations` 顺序累加，使用 `wrapping_add` 避免溢出 panic，确保在 Release 模式下行为稳定。
///
/// # 契约（What）
/// - **输入**：`iterations` 为非负次数，代表循环次数；超出 `u64::MAX` 的情况不可能发生。
/// - **输出**：返回最终累加结果，供调用方传入 `black_box` 以固定副作用。
/// - **前置条件**：调用前无需其他准备，但需保证在 Release 模式下运行以贴近真实性能。
/// - **后置条件**：函数不会触发 I/O 或分配，确保可在 `no_std` 受限环境模拟。
///
/// # 风险提示（Trade-offs）
/// - 若未来需要更复杂的基准，可将该函数替换为多阶段流水线，但需保持 deterministic 输出以便对比。
fn run_accumulation(iterations: u64) -> u64 {
    let mut acc = 0_u64;
    for idx in 0..iterations {
        acc = acc.wrapping_add(idx);
    }
    acc
}

#[cfg(test)]
mod tests {
    /// 冒烟测试：验证累加输出在小规模迭代下的确定性。
    #[test]
    fn accumulation_is_deterministic() {
        assert_eq!(super::run_accumulation(5), 10);
    }
}
