use criterion::{Criterion, criterion_group, criterion_main};

/// `bench_noop` 作为烟雾测试验证 `criterion` 基础设施配置正确。
///
/// # 设计目的（Why）
/// - 与仓库 `Makefile` 中的 `cargo bench -- --quick` 约定对齐，确保命令行参数被 `criterion` 正确识别。
/// - 提供最小可运行样例，避免在持续集成中出现“无基准函数”导致的误判。
///
/// # 执行逻辑（How）
/// - 使用 `Criterion::bench_function` 注册一个空操作基准，框架会自动处理 warmup/measurement。
///
/// # 契约说明（What）
/// - 函数不依赖任何外部状态，可在所有平台稳定运行。
///
/// # 风险提示（Trade-offs）
/// - 该基准不会反映真实性能，仅用于验证基础设施；实际性能测试请另行添加专业基准。
fn bench_noop(c: &mut Criterion) {
    c.bench_function("noop", |b| b.iter(|| ()));
}

criterion_group!(core_benches, bench_noop);
criterion_main!(core_benches);
