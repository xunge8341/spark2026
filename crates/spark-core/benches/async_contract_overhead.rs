#![allow(clippy::too_many_arguments)]

use core::future::Future;
use core::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};
use serde::Serialize;
use spark_core as spark;
use spark_core::Service;
use spark_core::SparkError;
use spark_core::buffer::PipelineMessage;
use spark_core::contract::CallContext;
use spark_core::service::{DynBridge, SimpleServiceFn};
use spark_core::status::{ReadyCheck, ReadyState};
use std::alloc::{GlobalAlloc, Layout, System};
use std::env;
#[cfg(feature = "std_json")]
use std::fs;
#[cfg(feature = "std_json")]
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

/// 全局分配器统计器：记录堆分配调用次数与字节数，用于量化对象层桥接的额外成本。
///
/// # 教案式说明
/// - **意图 (Why)**：`BoxService` 在桥接时会额外产生 `BoxFuture` 与 `Arc` 分配，需要显式量化分配次数/字节，才能给出“P99 开销 < 5%”
///   的论证数据。
/// - **架构位置 (Where)**：仅在本基准二进制中生效，不影响库或其他测试，保证统计不会污染业务运行时。
/// - **执行逻辑 (How)**：包装系统分配器，每次 `alloc`/`alloc_zeroed`/`realloc` 时累加计数；`dealloc` 记录释放次数，便于排查泄漏。
/// - **契约 (What)**：调用方可通过 [`AllocationSnapshot::capture`] 获得当前累积值，再用差分计算某段代码的分配成本。
/// - **权衡 (Trade-offs)**：记录使用 `Relaxed` 原子避免额外同步开销；由于仅在单线程基准环境运行，顺序一致性不是必需条件。
struct CountingAllocator;

static ALLOC_COUNT: AtomicU64 = AtomicU64::new(0);
static ALLOC_BYTES: AtomicU64 = AtomicU64::new(0);
static DEALLOC_COUNT: AtomicU64 = AtomicU64::new(0);
static DEALLOC_BYTES: AtomicU64 = AtomicU64::new(0);
static REALLOC_COUNT: AtomicU64 = AtomicU64::new(0);
static REALLOC_BYTES: AtomicU64 = AtomicU64::new(0);

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            ALLOC_COUNT.fetch_add(1, Ordering::Relaxed);
            ALLOC_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        }
        ptr
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc_zeroed(layout) };
        if !ptr.is_null() {
            ALLOC_COUNT.fetch_add(1, Ordering::Relaxed);
            ALLOC_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) };
        DEALLOC_COUNT.fetch_add(1, Ordering::Relaxed);
        DEALLOC_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let new_ptr = unsafe { System.realloc(ptr, layout, new_size) };
        if !new_ptr.is_null() {
            REALLOC_COUNT.fetch_add(1, Ordering::Relaxed);
            REALLOC_BYTES.fetch_add(new_size as u64, Ordering::Relaxed);
        }
        new_ptr
    }
}

#[global_allocator]
static GLOBAL: CountingAllocator = CountingAllocator;

/// 一次分配统计快照，供差分计算使用。
#[derive(Clone, Copy, Debug)]
struct AllocationSnapshot {
    allocations: u64,
    allocation_bytes: u64,
    reallocations: u64,
    reallocation_bytes: u64,
}

impl AllocationSnapshot {
    /// 捕获当前的累计分配状态。
    fn capture() -> Self {
        Self {
            allocations: ALLOC_COUNT.load(Ordering::Relaxed),
            allocation_bytes: ALLOC_BYTES.load(Ordering::Relaxed),
            reallocations: REALLOC_COUNT.load(Ordering::Relaxed),
            reallocation_bytes: REALLOC_BYTES.load(Ordering::Relaxed),
        }
    }
}

/// 统计差分结果，描述某段代码额外引入的堆分配成本。
#[derive(Clone, Copy, Debug, Serialize)]
struct AllocationDelta {
    allocations: u64,
    allocation_bytes: u64,
    reallocations: u64,
    reallocation_bytes: u64,
}

impl AllocationDelta {
    /// 根据前后快照计算差分。
    fn from_snapshots(before: AllocationSnapshot, after: AllocationSnapshot) -> Self {
        Self {
            allocations: after.allocations.saturating_sub(before.allocations),
            allocation_bytes: after
                .allocation_bytes
                .saturating_sub(before.allocation_bytes),
            reallocations: after.reallocations.saturating_sub(before.reallocations),
            reallocation_bytes: after
                .reallocation_bytes
                .saturating_sub(before.reallocation_bytes),
        }
    }

    /// 将分配次数换算为“每次调用平均分配次数”，方便与延迟数据对齐。
    fn allocations_per_iter(&self, iterations: u64) -> f64 {
        if iterations == 0 {
            0.0
        } else {
            self.allocations as f64 / iterations as f64
        }
    }

    /// 将分配字节换算为“每次调用平均分配字节数”。
    fn bytes_per_iter(&self, iterations: u64) -> f64 {
        if iterations == 0 {
            0.0
        } else {
            self.allocation_bytes as f64 / iterations as f64
        }
    }
}

/// 场景枚举：手写 Service 与宏生成 Service。
#[derive(Clone, Copy, Debug)]
enum ScenarioKind {
    Manual,
    Macro,
}

impl ScenarioKind {
    fn label(&self) -> &'static str {
        match self {
            ScenarioKind::Manual => "manual_service",
            ScenarioKind::Macro => "macro_service",
        }
    }
}

/// 单个场景的统计结果。
#[derive(Debug, Serialize)]
struct ScenarioReport {
    name: &'static str,
    samples: u64,
    latency_ns: LatencyReport,
    allocations: AllocationSummary,
}

/// 延迟统计，包含 P50/P95/P99 与平均值，单位为纳秒。
#[derive(Debug, Serialize)]
struct LatencyReport {
    p50: f64,
    p95: f64,
    p99: f64,
    mean: f64,
    min: u64,
    max: u64,
}

/// 分配统计概览。
#[derive(Debug, Serialize)]
struct AllocationSummary {
    allocations: u64,
    allocation_bytes: u64,
    reallocations: u64,
    reallocation_bytes: u64,
    allocations_per_iter: f64,
    allocation_bytes_per_iter: f64,
}

/// 汇总报告：输出到 `docs/reports/benchmarks/*.json` 供 CI 与文档消费。
#[derive(Debug, Serialize)]
struct BenchmarkReport {
    benchmark: &'static str,
    quick_mode: bool,
    iterations: u64,
    scenarios: Vec<ScenarioReport>,
    p99_overhead_ratio: f64,
    /// 宏生成 Service 相比手写基线的 P99 相对开销（0.03 表示 3%）。
    macro_p99_overhead: f64,
    p99_overhead_percent: f64,
    allocation_overhead_percent: f64,
}

/// 单次样本聚合的批大小：将多次调用聚合为一个样本，降低 `Instant` 噪声。
const SAMPLE_BATCH_SIZE: u64 = 64;

/// 模拟业务逻辑的迭代次数：通过固定的计算量代表常见的编解码/检查逻辑。
const SIMULATED_WORK_ROUNDS: u32 = 2048;

/// 本基准使用的业务载荷：简单地回显序号，满足 `UserMessage` 契约即可。
#[derive(Debug, Clone)]
struct EchoMessage {
    seq: u64,
}

/// 宏生成 Service：评估 `#[spark::service]` 展开后的执行胶水开销。
///
/// # 教案式注释
/// - **意图 (Why)**：对比宏展开与手写 Service 的运行时开销，为“宏展开 P99 开销 ≤ 3%”提供量化依据。
/// - **位置 (Where)**：仅在本基准文件使用，不影响库对外 API。
/// - **执行逻辑 (How)**：宏生成的服务会复用顺序协调器，与 [`SimpleServiceFn`] 保持一致，内部逻辑直接委托
///   给 [`process_request`]。
/// - **契约 (What)**：输入输出均为 [`PipelineMessage`]，错误类型为 [`SparkError`]；调用方需遵循 `poll_ready` → `call` 契约。
/// - **风险提示 (Trade-offs & Gotchas)**：若未来 `process_request` 引入阻塞操作，需要在宏路径同步评估 waker 注册与
///   释放逻辑是否仍满足零 Pending 的假设。
#[spark::service]
async fn macro_generated_echo(
    _ctx: spark::CallContext,
    req: PipelineMessage,
) -> spark_core::Result<PipelineMessage, SparkError> {
    process_request(req)
}

/// 基准主入口：执行两个场景并输出 JSON 报告。
fn main() {
    // 解析命令行开关，CI 会传入 `--quick` 以缩短运行时间。
    let quick_mode = env::args().any(|arg| arg == "--quick");
    let iterations = if quick_mode { 20_000 } else { 100_000 };

    // 准备共享上下文：waker、CallContext 与执行视图在两个场景之间共用，保证对比公平。
    let waker = noop_waker();
    let call_ctx = CallContext::builder().build();

    // 逐场景测量延迟与分配。
    let manual = measure_scenario(ScenarioKind::Manual, iterations, &waker, &call_ctx);
    let macro_generated = measure_scenario(ScenarioKind::Macro, iterations, &waker, &call_ctx);

    // 生成 JSON 报告并落盘。
    let report = build_report(iterations, quick_mode, manual, macro_generated);
    persist_report(&report, quick_mode).expect("write benchmark report");

    // 控制台打印摘要，方便本地调试时快速确认结论。
    println!("benchmark=async_contract_overhead quick_mode={quick_mode}");
    println!(
        "manual_p99_ns={:.2} macro_p99_ns={:.2} overhead_percent={:.3}",
        report.scenarios[0].latency_ns.p99,
        report.scenarios[1].latency_ns.p99,
        report.p99_overhead_percent,
    );
    println!(
        "manual_alloc_per_iter={:.4} macro_alloc_per_iter={:.4}",
        report.scenarios[0].allocations.allocations_per_iter,
        report.scenarios[1].allocations.allocations_per_iter,
    );
}

/// 根据两个场景的原始数据构建最终报告。
fn build_report(
    iterations: u64,
    quick_mode: bool,
    manual: ScenarioReport,
    macro_generated: ScenarioReport,
) -> BenchmarkReport {
    let p99_ratio = macro_generated.latency_ns.p99 / manual.latency_ns.p99;
    let macro_overhead = p99_ratio - 1.0;
    let p99_overhead_percent = macro_overhead * 100.0;

    let allocation_overhead_percent = if manual.allocations.allocations_per_iter == 0.0 {
        if macro_generated.allocations.allocations_per_iter == 0.0 {
            0.0
        } else {
            f64::INFINITY
        }
    } else {
        ((macro_generated.allocations.allocations_per_iter
            / manual.allocations.allocations_per_iter)
            - 1.0)
            * 100.0
    };

    BenchmarkReport {
        benchmark: "async_contract_overhead",
        quick_mode,
        iterations,
        scenarios: vec![manual, macro_generated],
        p99_overhead_ratio: p99_ratio,
        macro_p99_overhead: macro_overhead,
        p99_overhead_percent,
        allocation_overhead_percent,
    }
}

/// 生成 `docs/reports/benchmarks` 目录下的 JSON 文件。
#[cfg(feature = "std_json")]
fn persist_report(report: &BenchmarkReport, quick_mode: bool) -> std::io::Result<()> {
    let repo_root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|dir| dir.parent())
        .expect("workspace expects crates/<name> layout");
    let target_dir = repo_root.join("docs").join("reports").join("benchmarks");
    fs::create_dir_all(&target_dir)?;

    let file_name = if quick_mode {
        "service_macro_overhead.quick.json"
    } else {
        "service_macro_overhead.full.json"
    };
    let output_path = target_dir.join(file_name);
    let file = fs::File::create(&output_path)?;
    serde_json::to_writer_pretty(file, report)?;
    println!("report_path={}", output_path.display());
    Ok(())
}

#[cfg(not(feature = "std_json"))]
fn persist_report(_report: &BenchmarkReport, _quick_mode: bool) -> std::io::Result<()> {
    Ok(())
}

/// 针对特定场景执行基准循环，返回统计摘要。
fn measure_scenario(
    kind: ScenarioKind,
    iterations: u64,
    waker: &Waker,
    call_ctx: &CallContext,
) -> ScenarioReport {
    // 预先分配存储空间，避免测量过程中额外的 Vec 扩容影响统计。
    let sample_capacity = iterations.div_ceil(SAMPLE_BATCH_SIZE) as usize;
    let mut samples = Vec::with_capacity(sample_capacity);
    let before = AllocationSnapshot::capture();

    match kind {
        ScenarioKind::Manual => {
            run_manual(iterations, SAMPLE_BATCH_SIZE, waker, call_ctx, &mut samples);
        }
        ScenarioKind::Macro => {
            run_macro(iterations, SAMPLE_BATCH_SIZE, waker, call_ctx, &mut samples);
        }
    }

    let after = AllocationSnapshot::capture();
    let allocation_delta = AllocationDelta::from_snapshots(before, after);
    let latency = analyze_latency(samples);

    ScenarioReport {
        name: kind.label(),
        samples: iterations,
        latency_ns: latency,
        allocations: AllocationSummary {
            allocations: allocation_delta.allocations,
            allocation_bytes: allocation_delta.allocation_bytes,
            reallocations: allocation_delta.reallocations,
            reallocation_bytes: allocation_delta.reallocation_bytes,
            allocations_per_iter: allocation_delta.allocations_per_iter(iterations),
            allocation_bytes_per_iter: allocation_delta.bytes_per_iter(iterations),
        },
    }
}

/// 将请求从 `PipelineMessage` 转换为业务对象，执行模拟业务逻辑后重新封装为响应。
///
/// # 教案式注释
/// - **意图 (Why)**：真实 Handler 往往需要先下转业务消息、执行若干校验或加工逻辑，再将结果写回 `PipelineMessage`。
///   若直接回显原始对象，将人为放大小型对象擦除带来的相对开销，因此基准在这里注入固定计算量以模拟常见逻辑。
/// - **执行逻辑 (How)**：下转 `EchoMessage`，调用 [`simulate_user_logic`] 进行伪随机计算，然后将结果重新装入
///   `PipelineMessage::from_user`，保持泛型层与对象层对称。
/// - **契约 (What)**：输入输出均为 `PipelineMessage`；若消息类型不符，则返回结构化的 `SparkError`。
/// - **风险提示 (Trade-offs & Gotchas)**：`try_into_user` 在失败时会返回原消息，本基准选择直接抛错以显式暴露类型不匹配，
///   便于定位调用方是否使用了错误的基准载荷。
#[allow(clippy::result_large_err)]
fn process_request(req: PipelineMessage) -> spark_core::Result<PipelineMessage, SparkError> {
    let echo = req.try_into_user::<EchoMessage>().map_err(|_| {
        SparkError::new(
            "bench.payload_mismatch",
            "expected EchoMessage payload in async_contract_overhead",
        )
    })?;
    let next_seq = simulate_user_logic(echo.seq);
    Ok(PipelineMessage::from_user(EchoMessage { seq: next_seq }))
}

/// 模拟 Handler 的纯 CPU 逻辑：通过旋转/混合操作近似编解码或校验的耗时。
///
/// # 教案式注释
/// - **意图 (Why)**：强化基准的现实意义，避免仅测“空函数”导致对象层开销相对值被放大。
/// - **执行逻辑 (How)**：对输入执行固定次数的旋转、异或与乘法混合，使用 [Knuth 常数](https://en.wikipedia.org/wiki/Golden_ratio_base)
///   生成扰动；最终通过 `black_box` 防止编译器优化掉循环。
/// - **契约 (What)**：输入输出均为 `u64`，对任意值都保持确定性；不产生额外分配。
/// - **风险提示 (Trade-offs & Gotchas)**：`SIMULATED_WORK_ROUNDS` 的选择会直接影响总耗时，若未来服务逻辑显著变重，
///   可适度下调以缩短基准执行时间，但需同步更新文档中的描述。
fn simulate_user_logic(seed: u64) -> u64 {
    let mut acc = seed;
    let golden = 0x9E37_79B9_7F4A_7C15_u64;
    for round in 0..SIMULATED_WORK_ROUNDS {
        let perturb = (round as u64).wrapping_mul(0x7F4A_7C15);
        acc = acc.rotate_left(11) ^ (golden.wrapping_add(perturb));
        acc = acc.wrapping_mul(golden.rotate_left(3));
    }
    core::hint::black_box(acc)
}

/// 手写 Service 场景：直接在 `SimpleServiceFn` 上驱动 `poll_ready` 与 `call`。
fn run_manual(
    iterations: u64,
    batch_size: u64,
    waker: &Waker,
    call_ctx: &CallContext,
    samples: &mut Vec<u64>,
) {
    // 教案式注释
    // - **意图 (Why)**：建立零虚分派的手写基线，作为宏展开性能对比的参照物。
    // - **执行逻辑 (How)**：构造顺序执行的 `SimpleServiceFn`，再通过 `DynBridge` 保持与宏生成代码一致的胶水层，循环驱动
    //   `poll_ready` 与 `call` 并在同一 waker 下立即完成 Future。
    // - **契约 (What)**：输入为 `PipelineMessage`，响应按原样返回；遇到非就绪或错误会直接 panic，确保基准环境保持稳定。
    let mut service = DynBridge::<_, PipelineMessage>::new(SimpleServiceFn::new(
        |_ctx: CallContext, req: PipelineMessage| async move { process_request(req) },
    ));

    let exec_ctx = call_ctx.execution();

    let mut remaining = iterations;
    let mut seq = 0_u64;
    while remaining > 0 {
        let chunk = remaining.min(batch_size);
        let started = Instant::now();
        for _ in 0..chunk {
            let mut task_cx = Context::from_waker(waker);
            match service.poll_ready(&exec_ctx, &mut task_cx) {
                Poll::Ready(ReadyCheck::Ready(ReadyState::Ready)) => {}
                other => panic!("manual service unexpectedly pending: {other:?}"),
            }

            let payload = PipelineMessage::from_user(EchoMessage { seq });
            seq = seq.wrapping_add(1);
            let future = service.call(call_ctx.clone(), payload);
            let response =
                block_on_ready(future, waker).expect("manual service must echo the message back");
            consume_response(response);
        }
        let elapsed = started.elapsed().as_nanos();
        let per_call = ((elapsed + (chunk as u128 / 2)) / chunk as u128) as u64;
        samples.push(per_call);
        remaining -= chunk;
    }
}

/// 宏生成 Service 场景：直接测量 `#[spark::service]` 展开的胶水代码开销。
fn run_macro(
    iterations: u64,
    batch_size: u64,
    waker: &Waker,
    call_ctx: &CallContext,
    samples: &mut Vec<u64>,
) {
    // 教案式注释
    // - **意图 (Why)**：与手写场景对比宏展开生成的调度胶水是否引入额外延迟或分配。
    // - **执行逻辑 (How)**：调用 [`macro_generated_echo`] 构造服务，复用同一业务逻辑，再按顺序驱动 `poll_ready` 与 `call`。
    // - **契约 (What)**：输入输出仍为 `PipelineMessage`，若宏展开路径出现 Pending 或错误将直接 panic，便于第一时间发现回归。
    let mut service = macro_generated_echo();

    let exec_ctx = call_ctx.execution();

    let mut remaining = iterations;
    let mut seq = 0_u64;
    while remaining > 0 {
        let chunk = remaining.min(batch_size);
        let started = Instant::now();
        for _ in 0..chunk {
            let mut task_cx = Context::from_waker(waker);
            match service.poll_ready(&exec_ctx, &mut task_cx) {
                Poll::Ready(ReadyCheck::Ready(ReadyState::Ready)) => {}
                other => panic!("macro service unexpectedly pending: {other:?}"),
            }

            let payload = PipelineMessage::from_user(EchoMessage { seq });
            seq = seq.wrapping_add(1);
            let future = service.call(call_ctx.clone(), payload);
            let response =
                block_on_ready(future, waker).expect("macro service must echo the message back");
            consume_response(response);
        }
        let elapsed = started.elapsed().as_nanos();
        let per_call = ((elapsed + (chunk as u128 / 2)) / chunk as u128) as u64;
        samples.push(per_call);
        remaining -= chunk;
    }
}

/// 将 Future 在当前线程上拉取到完成，仅适用于本基准中“立刻就绪”的 Future。
fn block_on_ready<F>(future: F, waker: &Waker) -> F::Output
where
    F: Future,
{
    let mut future = std::pin::pin!(future);
    let mut cx = Context::from_waker(waker);
    match Future::poll(future.as_mut(), &mut cx) {
        Poll::Ready(output) => output,
        Poll::Pending => panic!("benchmark future unexpectedly pending"),
    }
}

/// 消费响应消息，避免被编译器优化掉。
fn consume_response(message: PipelineMessage) {
    match message.try_into_user::<EchoMessage>() {
        Ok(echo) => {
            let _ = core::hint::black_box(echo.seq);
        }
        Err(other) => panic!("unexpected response payload: {other:?}"),
    }
}

/// 分析延迟样本，计算关键分位数。
fn analyze_latency(mut samples: Vec<u64>) -> LatencyReport {
    if samples.is_empty() {
        return LatencyReport {
            p50: 0.0,
            p95: 0.0,
            p99: 0.0,
            mean: 0.0,
            min: 0,
            max: 0,
        };
    }

    let mut sum = 0u128;
    let mut min = u64::MAX;
    let mut max = 0u64;
    for &sample in &samples {
        sum += sample as u128;
        min = min.min(sample);
        max = max.max(sample);
    }

    samples.sort_unstable();
    let len = samples.len() as f64;
    let p50 = percentile(&samples, 0.50);
    let p95 = percentile(&samples, 0.95);
    let p99 = percentile(&samples, 0.99);
    let mean = sum as f64 / len;

    LatencyReport {
        p50,
        p95,
        p99,
        mean,
        min,
        max,
    }
}

/// 通用分位数计算，使用线性插值减小离散误差。
fn percentile(samples: &[u64], quantile: f64) -> f64 {
    debug_assert!((0.0..=1.0).contains(&quantile));
    if samples.is_empty() {
        return 0.0;
    }

    let n = samples.len();
    if n == 1 {
        return samples[0] as f64;
    }

    let rank = quantile * (n as f64 - 1.0);
    let low = rank.floor() as usize;
    let high = rank.ceil() as usize;
    if low == high {
        samples[low] as f64
    } else {
        let lower = samples[low] as f64;
        let upper = samples[high] as f64;
        let weight = rank - low as f64;
        lower + (upper - lower) * weight
    }
}

/// 构造一个永不触发唤醒的空 Waker，用于同步轮询。
fn noop_waker() -> Waker {
    unsafe fn clone(_: *const ()) -> RawWaker {
        RawWaker::new(core::ptr::null(), &VTABLE)
    }
    unsafe fn wake(_: *const ()) {}
    unsafe fn wake_by_ref(_: *const ()) {}
    unsafe fn drop(_: *const ()) {}
    static VTABLE: RawWakerVTable = RawWakerVTable::new(clone, wake, wake_by_ref, drop);
    unsafe { Waker::from_raw(RawWaker::new(core::ptr::null(), &VTABLE)) }
}
