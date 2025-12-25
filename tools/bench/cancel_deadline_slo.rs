//! 取消/截止传播 SLO 基准程序。
//!
//! # 设计目标（Why）
//! - 建立“取消标记 → 语法糖上下文”的端到端延迟基准，持续验证 DX 语法糖不会拉长传播长尾；
//! - 通过同一工具生成 `quick` 与 `full` 两类报告，供 CI 与文档展示复用；
//! - 在运行时即可断言 P99 阈值，第一时间暴露超过 5ms 的回归。
//!
//! # 工作机制（How）
//! - 构建两种场景：
//!   1. `manual_call_context`：直接克隆 [`CallContext`] 并轮询取消标记；
//!   2. `runtime_sugar_context`：通过 [`spark_core::runtime::CallContext`] 语法糖访问同一取消标记，覆盖 DX 包装层；
//! - 每轮基准都会：
//!   - 创建带 50ms 截止时间的 `CallContext`；
//!   - 生成 `concurrency` 份视图并同步触发 `cancel()`；
//!   - 逐个测量“取消起点 → 观察到取消”的延迟，同时校验截止时间是否同步超时；
//! - 采样结果写入环形水位（1,000,000 条），既保持 P99 精度，又避免 30s 基准导致内存爆炸。
//!
//! # 契约说明（What）
//! - **输入参数**（通过 CLI 提供）：
//!   - `--concurrency <usize>`：每轮并发视图数量，要求大于 0；
//!   - `--duration <secs>`：单个场景运行时长（秒）。若搭配 `--quick`，默认 5 秒；否则默认 30 秒；
//!   - `--output <path>`：结果写入路径，默认根据 `quick/full` 模式选用 `docs/reports/benchmarks/cancel_deadline_slo.{quick,full}.json`；
//!   - `--assert-p99-ms <float>`：可选 SLO 阈值，触发时若任一场景 P99 超过阈值将立即报错退出；
//!   - `--quick`：启用快速模式（缩短默认时长、输出 quick 报告标记）。
//! - **输出**：结构化 JSON（含 P50/P95/P99、均值、采样规模、截止校验结果等），同时打印到标准输出；
//! - **前置条件**：运行环境需使用 `--release` 以减少测量噪声，且在高并发下建议绑定 CPU 频率；
//! - **后置条件**：若提供 `--output`，工具会覆盖目标文件（附带换行），便于直接纳入版本库。
//!
//! # 风险与权衡（Trade-offs & Gotchas）
//! - 采样采用“最新 100 万条”环形缓存：在 30 秒大样本下可压制内存开销，但若长尾仅在早期出现会被覆盖——建议在 CI 失败后结合原始日志复现；
//! - 延迟测量为单线程轮询（对每个视图依次记录），模拟“下游监听取消位”的最坏情况；若需真实线程调度，可在后续演进为多线程方案；
//! - 若未来新增第三种传播路径，可在 [`ScenarioKind`] 中扩展枚举，保持输出结构稳定。

use core::time::Duration;
use core::{future::Future, hint::spin_loop};
use serde::Serialize;
use spark_core::{
    contract::{CallContext, Deadline},
    runtime::sugar::BorrowedRuntimeCaps,
    runtime::{self, MonotonicTimePoint},
};
use std::{env, error::Error, fmt, fs::File, io::Write, path::PathBuf, time::Instant};

/// CLI 解析结果，统一描述一次基准运行的配置。
///
/// # 结构说明（How）
/// - `concurrency`：每轮克隆/视图数量，直接决定延迟样本规模；
/// - `duration`：单个场景运行时长（秒级），用于控制采样窗口；
/// - `output`：JSON 写入位置，`None` 时仅打印到标准输出；
/// - `quick_mode`：是否处于快速模式（影响默认持续时间与报告元数据）；
/// - `assert_p99_ms`：可选阈值，满足“内建 SLO 校验”需求。
///
/// # 契约（What）
/// - `concurrency` 必须大于 0，否则程序拒绝运行；
/// - 若未显式指定 `duration`，根据 `quick_mode` 自动取 5s 或 30s；
/// - `output` 未设置时不会写文件，避免覆盖历史数据。
#[derive(Debug, Clone)]
struct Config {
    concurrency: usize,
    duration: Duration,
    output: Option<PathBuf>,
    quick_mode: bool,
    assert_p99_ms: Option<f64>,
}

impl Config {
    /// 默认的快速/完整报告路径，保持与其它基准命名一致。
    const QUICK_OUTPUT: &'static str = "docs/reports/benchmarks/cancel_deadline_slo.quick.json";
    const FULL_OUTPUT: &'static str = "docs/reports/benchmarks/cancel_deadline_slo.full.json";

    /// 解析 CLI 参数。
    ///
    /// # 解析逻辑（How）
    /// - 逐项消费 `env::args()`，识别长参数并处理取值；
    /// - 支持 `--quick` 快捷开关，在未显式指定 `--duration` 时缩短为 5 秒；
    /// - 非法或缺失的参数立即返回 [`BenchError::InvalidArgument`]。
    ///
    /// # 返回值（What）
    /// - 成功时返回 [`Config`]；失败时携带人类可读的错误描述。
    fn parse() -> Result<Self, BenchError> {
        let mut args = env::args().skip(1);
        let mut concurrency = 512_usize;
        let mut duration = Duration::from_secs(30);
        let mut duration_set = false;
        let mut output: Option<PathBuf> = None;
        let mut quick_mode = false;
        let mut assert_p99_ms: Option<f64> = None;

        while let Some(flag) = args.next() {
            match flag.as_str() {
                "--concurrency" => {
                    let value = args.next().ok_or_else(|| {
                        BenchError::InvalidArgument("--concurrency 之后需要提供数值".into())
                    })?;
                    concurrency = value.parse::<usize>().map_err(|error| {
                        BenchError::InvalidArgument(format!("无法解析并发度 `{value}`：{error}"))
                    })?;
                }
                "--duration" => {
                    let value = args.next().ok_or_else(|| {
                        BenchError::InvalidArgument("--duration 之后需要提供秒数".into())
                    })?;
                    let secs = value.parse::<u64>().map_err(|error| {
                        BenchError::InvalidArgument(format!("无法解析持续时间 `{value}`：{error}"))
                    })?;
                    duration = Duration::from_secs(secs.max(1));
                    duration_set = true;
                }
                "--output" => {
                    let value = args.next().ok_or_else(|| {
                        BenchError::InvalidArgument("--output 之后需要提供路径".into())
                    })?;
                    output = Some(PathBuf::from(value));
                }
                "--assert-p99-ms" => {
                    let value = args.next().ok_or_else(|| {
                        BenchError::InvalidArgument("--assert-p99-ms 之后需要提供阈值".into())
                    })?;
                    let threshold = value.parse::<f64>().map_err(|error| {
                        BenchError::InvalidArgument(format!("无法解析 P99 阈值 `{value}`：{error}"))
                    })?;
                    if threshold <= 0.0 {
                        return Err(BenchError::InvalidArgument("P99 阈值必须为正数".into()));
                    }
                    assert_p99_ms = Some(threshold);
                }
                "--quick" => {
                    quick_mode = true;
                }
                other => {
                    return Err(BenchError::InvalidArgument(format!("未知参数 `{other}`")));
                }
            }
        }

        if concurrency == 0 {
            return Err(BenchError::InvalidArgument(
                "--concurrency 必须大于 0".into(),
            ));
        }

        if quick_mode && !duration_set {
            duration = Duration::from_secs(5);
        }

        if output.is_none() {
            output = Some(PathBuf::from(if quick_mode {
                Self::QUICK_OUTPUT
            } else {
                Self::FULL_OUTPUT
            }));
        }

        Ok(Self {
            concurrency,
            duration,
            output,
            quick_mode,
            assert_p99_ms,
        })
    }
}

/// 内建错误类型，统一封装 CLI/IO/断言等失败场景。
#[derive(Debug)]
pub enum BenchError {
    InvalidArgument(String),
    Io(std::io::Error),
    Serialize(serde_json::Error),
    Assertion {
        scenario: &'static str,
        observed_us: f64,
        threshold_ms: f64,
    },
}

impl fmt::Display for BenchError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BenchError::InvalidArgument(msg) => write!(f, "参数错误: {msg}"),
            BenchError::Io(err) => write!(f, "IO 错误: {err}"),
            BenchError::Serialize(err) => write!(f, "序列化错误: {err}"),
            BenchError::Assertion {
                scenario,
                observed_us,
                threshold_ms,
            } => write!(
                f,
                "场景 `{scenario}` 的 P99 = {:.3} µs 超过阈值 {:.3} ms",
                observed_us, threshold_ms
            ),
        }
    }
}

impl Error for BenchError {}

impl From<std::io::Error> for BenchError {
    fn from(value: std::io::Error) -> Self {
        BenchError::Io(value)
    }
}

impl From<serde_json::Error> for BenchError {
    fn from(value: serde_json::Error) -> Self {
        BenchError::Serialize(value)
    }
}

/// 延迟采样环形缓冲区容量：存储最新 100 万条数据。
const RESERVOIR_CAPACITY: usize = 1_000_000;

/// 描述不同的传播场景，方便扩展更多对比对象。
#[derive(Clone, Copy, Debug)]
enum ScenarioKind {
    Manual,
    RuntimeSugar,
}

impl ScenarioKind {
    fn label(&self) -> &'static str {
        match self {
            ScenarioKind::Manual => "manual_call_context",
            ScenarioKind::RuntimeSugar => "runtime_sugar_context",
        }
    }
}

/// 单个场景的运行统计。
#[derive(Debug, Serialize)]
struct ScenarioReport {
    name: &'static str,
    concurrency: usize,
    batches: u64,
    observations: u64,
    runtime_secs: f64,
    deadline_checks: u64,
    deadline_violations: u64,
    latency_us: LatencyReport,
}

/// 延迟统计摘要，单位均为微秒。
#[derive(Debug, Serialize)]
struct LatencyReport {
    p50: f64,
    p95: f64,
    p99: f64,
    mean: f64,
    min: f64,
    max: f64,
    samples_recorded: usize,
    total_observations: u64,
}

/// 顶层报告结构，描述整体基准输出。
#[derive(Debug, Serialize)]
struct BenchmarkReport {
    benchmark: &'static str,
    quick_mode: bool,
    requested_concurrency: usize,
    scenario_duration_secs: f64,
    total_observations: u64,
    scenarios: Vec<ScenarioReport>,
}

/// 环形采样缓冲区，负责记录延迟数据并保留最近的样本。
struct LatencyAccumulator {
    capacity: usize,
    storage: Vec<f64>,
    total_count: u64,
    sum: f64,
    min: f64,
    max: f64,
}

impl LatencyAccumulator {
    fn new(capacity: usize) -> Self {
        Self {
            capacity,
            storage: Vec::with_capacity(capacity.min(1024)),
            total_count: 0,
            sum: 0.0,
            min: f64::INFINITY,
            max: f64::NEG_INFINITY,
        }
    }

    /// 记录一次延迟样本，必要时覆盖最旧数据。
    fn record(&mut self, value: f64) {
        if self.capacity == 0 {
            return;
        }
        if self.storage.len() < self.capacity {
            self.storage.push(value);
        } else {
            let position = (self.total_count % self.capacity as u64) as usize;
            self.storage[position] = value;
        }
        self.total_count += 1;
        self.sum += value;
        if value < self.min {
            self.min = value;
        }
        if value > self.max {
            self.max = value;
        }
    }

    /// 汇总统计结果。
    fn finalize(&self) -> Option<LatencyReport> {
        if self.total_count == 0 {
            return None;
        }
        let samples_len = self.storage.len().min(self.total_count as usize);
        if samples_len == 0 {
            return None;
        }
        let mut samples = self.storage.clone();
        samples.truncate(samples_len);
        samples.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let p50 = percentile(&samples, 0.50);
        let p95 = percentile(&samples, 0.95);
        let p99 = percentile(&samples, 0.99);
        let mean = self.sum / self.total_count as f64;
        Some(LatencyReport {
            p50,
            p95,
            p99,
            mean,
            min: self.min,
            max: self.max,
            samples_recorded: samples_len,
            total_observations: self.total_count,
        })
    }
}

/// 百分位计算，使用向下/向上插值。
fn percentile(sorted: &[f64], quantile: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let clamped = quantile.clamp(0.0, 1.0);
    if sorted.len() == 1 {
        return sorted[0];
    }
    let rank = clamped * (sorted.len() - 1) as f64;
    let lower = rank.floor() as usize;
    let upper = rank.ceil() as usize;
    if lower == upper {
        sorted[lower]
    } else {
        let weight = rank - lower as f64;
        sorted[lower] + (sorted[upper] - sorted[lower]) * weight
    }
}

/// 运行单个场景：生成上下文、触发取消、记录延迟与截止一致性。
fn run_scenario(kind: ScenarioKind, config: &Config) -> ScenarioReport {
    let mut accumulator = LatencyAccumulator::new(RESERVOIR_CAPACITY);
    let mut deadline_checks = 0_u64;
    let mut deadline_violations = 0_u64;
    let mut batches = 0_u64;
    let concurrency = config.concurrency;
    let scenario_start = Instant::now();
    let monotonic_origin = Instant::now();
    let mut manual_pool: Vec<CallContext> = Vec::with_capacity(concurrency);

    while scenario_start.elapsed() < config.duration {
        batches += 1;
        manual_pool.clear();
        let now_point = MonotonicTimePoint::from_offset(monotonic_origin.elapsed());
        let deadline = Deadline::with_timeout(now_point, Duration::from_millis(50));
        let call = CallContext::builder().with_deadline(deadline).build();
        let check_point = now_point.saturating_add(Duration::from_millis(100));

        match kind {
            ScenarioKind::Manual => {
                manual_pool.resize_with(concurrency, || call.clone());
                let start = Instant::now();
                let cancelled = call.cancellation().cancel();
                debug_assert!(cancelled, "新建上下文的 cancel() 应首次返回 true");
                for ctx in &manual_pool {
                    while !ctx.cancellation().is_cancelled() {
                        spin_loop();
                    }
                    deadline_checks += 1;
                    if !ctx.deadline().is_expired(check_point) {
                        deadline_violations += 1;
                    }
                    accumulator.record(start.elapsed().as_secs_f64() * 1_000_000.0);
                }
            }
            ScenarioKind::RuntimeSugar => {
                let runtime_caps = NoopRuntimeCaps;
                let start = Instant::now();
                let cancelled = call.cancellation().cancel();
                debug_assert!(cancelled, "新建上下文的 cancel() 应首次返回 true");
                for _ in 0..concurrency {
                    let view =
                        runtime::CallContext::new(&call, BorrowedRuntimeCaps::new(&runtime_caps));
                    let call_ref = view.call();
                    while !call_ref.cancellation().is_cancelled() {
                        spin_loop();
                    }
                    deadline_checks += 1;
                    if !call_ref.deadline().is_expired(check_point) {
                        deadline_violations += 1;
                    }
                    accumulator.record(start.elapsed().as_secs_f64() * 1_000_000.0);
                }
            }
        }
    }

    let observations = accumulator.total_count;
    let latency = accumulator.finalize().expect("至少应记录一条延迟样本");

    ScenarioReport {
        name: kind.label(),
        concurrency,
        batches,
        observations,
        runtime_secs: scenario_start.elapsed().as_secs_f64(),
        deadline_checks,
        deadline_violations,
        latency_us: latency,
    }
}

/// 不执行任何实际调度的运行时能力桩，满足语法糖引用要求。
struct NoopRuntimeCaps;

impl runtime::RuntimeCaps for NoopRuntimeCaps {
    type Join<T: Send + 'static> = ();

    fn spawn_with<F>(&self, _ctx: &CallContext, _fut: F) -> Self::Join<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        panic!("取消 SLO 基准不应触发 spawn_with：若触发说明有新需求需扩展实现");
    }
}

/// 程序主入口：解析配置、运行场景、输出报告并执行 SLO 断言。
pub fn run() -> Result<(), BenchError> {
    let config = Config::parse()?;
    let manual = run_scenario(ScenarioKind::Manual, &config);
    let sugar = run_scenario(ScenarioKind::RuntimeSugar, &config);
    let scenarios = vec![manual, sugar];

    if let Some(threshold_ms) = config.assert_p99_ms {
        let threshold_us = threshold_ms * 1_000.0;
        for scenario in &scenarios {
            if scenario.latency_us.p99 > threshold_us {
                return Err(BenchError::Assertion {
                    scenario: scenario.name,
                    observed_us: scenario.latency_us.p99,
                    threshold_ms,
                });
            }
        }
    }

    let total_observations = scenarios.iter().map(|scenario| scenario.observations).sum();

    let report = BenchmarkReport {
        benchmark: "cancel_deadline_slo",
        quick_mode: config.quick_mode,
        requested_concurrency: config.concurrency,
        scenario_duration_secs: config.duration.as_secs_f64(),
        total_observations,
        scenarios,
    };

    let payload = serde_json::to_string_pretty(&report)?;

    if let Some(path) = &config.output {
        let mut file = File::create(path)?;
        file.write_all(payload.as_bytes())?;
        file.write_all(b"\n")?;
    }

    println!("{}", payload);
    Ok(())
}
