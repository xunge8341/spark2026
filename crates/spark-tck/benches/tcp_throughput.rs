use bytes::Bytes;
use serde::Serialize;
use spark_core::{
    contract::CallContext,
    error::{CoreError, codes},
    transport::TransportSocketAddr,
};
use spark_transport_tcp::{TcpChannel, TcpServerChannel};
use std::{
    env, fmt,
    net::SocketAddr,
    time::{Duration, Instant},
};
use tokio::{runtime::Builder as RuntimeBuilder, sync::oneshot, task::JoinError};

/// TCP 吞吐基准：围绕 `spark-transport-tcp` 的 Tokio 实现建立性能基线。
///
/// # 教案式综述（Why / How / What / Trade-offs）
/// - **目标 (Why)**：
///   - 为“Tokio 路径”建立可回归的性能基线，确保未来迭代（如背压、批量写）不会意外回退。
///   - 通过结构化 JSON 输出，为 CI 编排（`make ci-bench-smoke` 等）提供机器可读的 p99 抖动数据。
/// - **执行策略 (How)**：
///   1. 以 Tokio 多线程运行时驱动单连接 TCP 循环；
///   2. 依次测量 1 KiB、64 KiB、1 MiB 三档负载的“写入 → 回显”往返耗时；
///   3. 统计 p50/p99 延迟与吞吐，换算 p99 抖动百分比；
///   4. 将结果包装为 JSON 并输出到标准输出，由上游 CI 校验阈值。
/// - **契约 (What)**：
///   - **输入**：命令行参数可包含 `--quick`（缩短样本数）与 `--iterations=<N>`（自定义样本数）；
///   - **输出**：单个 JSON 对象，字段包含运行时标签、样本配置、各负载档位的统计；
///   - **前置条件**：宿主必须具备 TCP loopback 能力，并允许 Tokio 创建多线程运行时；
///   - **后置条件**：所有 TCP 通道在基准结束前关闭，避免悬挂任务影响后续基准。
/// - **设计取舍 (Trade-offs)**：
///   - 选择“固定单连接 + 回显”而非多连接并发，以降低结果噪声并强调 `TcpChannel` 基线；若未来要覆盖多连接场景，可在保持 JSON 格式的前提下扩展字段。
fn main() -> spark_core::Result<(), BenchError> {
    let cli = CliOptions::parse_from_env()?;
    let runtime = RuntimeBuilder::new_multi_thread()
        .worker_threads(cli.worker_threads)
        .enable_all()
        .build()?;

    let mut payload_reports = Vec::with_capacity(PAYLOAD_SIZES.len());
    for &payload in PAYLOAD_SIZES.iter() {
        let report = runtime.block_on(run_payload_benchmark(payload, &cli))?;
        payload_reports.push(report);
    }

    let summary = BenchmarkSummary {
        benchmark: BENCHMARK_NAME,
        runtime: RUNTIME_LABEL,
        version: env!("CARGO_PKG_VERSION"),
        iterations: cli.iterations,
        warmup_iterations: cli.warmup_iterations,
        quick_mode: cli.quick_mode,
        worker_threads: cli.worker_threads,
        payloads: payload_reports,
    };

    let json = serde_json::to_string_pretty(&summary)?;
    println!("{json}");
    Ok(())
}

/// 基准内部统一使用的错误类型，显式区分核心错误与环境错误来源。
#[derive(Debug)]
enum BenchError {
    Core(CoreError),
    Io(std::io::Error),
    Json(serde_json::Error),
    ParseInt(std::num::ParseIntError),
    InvalidConfig(&'static str),
}

impl fmt::Display for BenchError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BenchError::Core(err) => write!(f, "core error: {err}"),
            BenchError::Io(err) => write!(f, "io error: {err}"),
            BenchError::Json(err) => write!(f, "json error: {err}"),
            BenchError::ParseInt(err) => write!(f, "invalid numeric argument: {err}"),
            BenchError::InvalidConfig(msg) => write!(f, "invalid benchmark configuration: {msg}"),
        }
    }
}

impl std::error::Error for BenchError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            BenchError::Core(_) => None,
            BenchError::Io(err) => Some(err),
            BenchError::Json(err) => Some(err),
            BenchError::ParseInt(err) => Some(err),
            BenchError::InvalidConfig(_) => None,
        }
    }
}

impl From<CoreError> for BenchError {
    fn from(err: CoreError) -> Self {
        BenchError::Core(err)
    }
}

impl From<std::io::Error> for BenchError {
    fn from(err: std::io::Error) -> Self {
        BenchError::Io(err)
    }
}

impl From<serde_json::Error> for BenchError {
    fn from(err: serde_json::Error) -> Self {
        BenchError::Json(err)
    }
}

impl From<std::num::ParseIntError> for BenchError {
    fn from(err: std::num::ParseIntError) -> Self {
        BenchError::ParseInt(err)
    }
}

/// 支持 `tcp_throughput` 基准的常量配置。
const BENCHMARK_NAME: &str = "tcp_throughput";
const RUNTIME_LABEL: &str = "tokio-multi-thread";
const PAYLOAD_SIZES: [usize; 3] = [1024, 64 * 1024, 1024 * 1024];
const DEFAULT_ITERATIONS: usize = 128;
const QUICK_ITERATIONS: usize = 32;
const DEFAULT_WORKER_THREADS: usize = 2;

/// 运行前的热身轮次：用于填充 TCP 缓冲并激活内核调度器，避免统计阶段被冷启动放大。
const DEFAULT_WARMUP_ITERATIONS: usize = 4;

/// 命令行配置解析结果。
///
/// # 教案式说明
/// - **意图 (Why)**：集中解析与校验命令行参数，便于未来扩展更多调优选项，同时保证主流程专注于基准逻辑。
/// - **逻辑 (How)**：
///   1. 扫描 `std::env::args()`，识别 `--quick` 与 `--iterations=` 前缀；
///   2. 根据参数决定样本数量、是否启用 Quick 模式以及 Tokio worker 数；
///   3. 将解析结果封装成结构体，供后续流程引用。
/// - **契约 (What)**：
///   - `iterations`：实际采样次数，至少为 1；
///   - `warmup_iterations`：预热轮次，本实现固定为 4，可在未来通过 CLI 替换；
///   - `quick_mode`：指示是否因 `--quick` 缩短样本数，便于 CI 选择不同阈值；
///   - `worker_threads`：Tokio 多线程运行时的工作线程数量，默认为 2；
///   - **前置条件**：命令行中若同时出现 `--quick` 与 `--iterations`，显式指定的次数优先；
///   - **后置条件**：结构体保证所有字段满足最小取值约束，避免运行期再做重复校验。
#[derive(Debug, Clone)]
struct CliOptions {
    iterations: usize,
    warmup_iterations: usize,
    quick_mode: bool,
    worker_threads: usize,
}

impl CliOptions {
    /// 从环境变量解析 CLI 选项。
    ///
    /// # 教案式注释
    /// - **意图 (Why)**：提供单入口解析逻辑，确保所有命令行参数都经过统一校验。
    /// - **逻辑 (How)**：
    ///   - 遍历参数，检测 `--quick` 与 `--iterations=` 前缀；
    ///   - 若提供了显式次数，则覆盖默认值；否则根据 `--quick` 选择预设的 32 或 128 样本；
    ///   - 最终确保次数不为零，避免基准流程构造空向量。
    /// - **契约 (What)**：返回的 [`CliOptions`] 至少保证一次采样，且包含固定 4 次预热；
    ///   若输入非法（如 `--iterations=0`），立即返回错误。
    /// - **风险提示 (Trade-offs)**：目前未暴露 worker 数量的 CLI 控制，但保留字段以便后续扩展；当未来需要调整线程数时，可直接新增参数解析分支。
    fn parse_from_env() -> spark_core::Result<Self, BenchError> {
        let mut iterations = DEFAULT_ITERATIONS;
        let mut quick_mode = false;
        for arg in env::args().skip(1) {
            if arg == "--quick" {
                iterations = QUICK_ITERATIONS;
                quick_mode = true;
            } else if let Some(value) = arg.strip_prefix("--iterations=") {
                iterations = value.parse::<usize>()?;
                quick_mode = false;
            }
        }
        if iterations == 0 {
            return Err(BenchError::InvalidConfig("iterations must be >= 1"));
        }
        Ok(Self {
            iterations,
            warmup_iterations: DEFAULT_WARMUP_ITERATIONS,
            quick_mode,
            worker_threads: DEFAULT_WORKER_THREADS,
        })
    }
}

/// 基准运行的整体摘要，作为 JSON 输出的根节点。
///
/// # 教案式说明
/// - **意图 (Why)**：将执行配置与各负载档位的统计结果整合为单一文档，方便 CI 与人类读者快速理解上下文。
/// - **契约 (What)**：
///   - `benchmark`：固定字符串 `tcp_throughput`，表示测量场景；
///   - `runtime`：运行时标签，当前固定为 `tokio-multi-thread`，后续若扩展其他运行时可沿用字段；
///   - `version`：二进制的 `CARGO_PKG_VERSION`，便于跨版本对比；
///   - `iterations`/`warmup_iterations`/`quick_mode`/`worker_threads`：记录执行参数；
///   - `payloads`：按负载大小列出详尽统计。
/// - **风险提示 (Trade-offs)**：结构设计刻意保持扁平化字段，避免嵌套过深导致 CI 脚本解析复杂度过高。
#[derive(Debug, Serialize)]
struct BenchmarkSummary {
    benchmark: &'static str,
    runtime: &'static str,
    version: &'static str,
    iterations: usize,
    warmup_iterations: usize,
    quick_mode: bool,
    worker_threads: usize,
    payloads: Vec<PayloadReport>,
}

/// 单个负载档位的统计结果。
///
/// # 教案式说明
/// - **意图 (Why)**：细分不同数据量级下的吞吐表现，便于观察小包/大包场景的差异与回归趋势。
/// - **契约 (What)**：
///   - `payload_bytes`：单次往返的有效负载大小；
///   - `iterations`/`warmup_iterations`：对应配置；
///   - `duration_ns`：p50/p99 延迟（纳秒）；
///   - `throughput_mib_s`：对应吞吐（MiB/s）；
///   - `p99_jitter_pct`：`(p99_latency - p50_latency) / p50_latency * 100`，用于 CI 判定是否超过 5% 阈值。
/// - **风险提示**：延迟与吞吐都使用 `f64`，在极小样本下可能出现浮点舍入误差，但对 CI 阈值（百分位差）影响可忽略。
#[derive(Debug, Serialize)]
struct PayloadReport {
    payload_bytes: usize,
    iterations: usize,
    warmup_iterations: usize,
    duration_ns: PercentileStats,
    throughput_mib_s: PercentileStats,
    p99_jitter_pct: f64,
}

/// p50/p99 统计结构，复用在延迟与吞吐字段中。
#[derive(Debug, Serialize)]
struct PercentileStats {
    p50: f64,
    p99: f64,
}

/// 针对指定负载运行基准并生成统计数据。
///
/// # 教案式说明
/// - **意图 (Why)**：封装“建连 → 预热 → 采样 → 汇总”全流程，保持 `main` 逻辑精简，并便于单元/集成测试复用该函数。
/// - **逻辑 (How)**：
///   1. 绑定本地回环地址，启动 `TcpServerChannel` 并在后台协程中执行回显循环；
///   2. 客户端建立 `TcpChannel`，执行若干次预热以稳定 TCP 缓冲；
///   3. 对每次往返测量耗时，累积样本；
///   4. 汇总为结构化统计并返回。
/// - **契约 (What)**：
///   - **输入**：`payload_bytes` 指定单次往返负载大小，`cli` 携带样本配置；
///   - **输出**：`PayloadReport`，包含延迟、吞吐与抖动百分比；
///   - **前置条件**：`payload_bytes > 0`，Tokio 运行时上下文已由调用方负责；
///   - **后置条件**：服务器任务在函数返回前完成并回收，避免资源泄漏。
/// - **风险提示 (Trade-offs)**：当前实现仅建立单连接；若未来需覆盖并发连接，可在此处引入参数并调整统计逻辑。
async fn run_payload_benchmark(
    payload_bytes: usize,
    cli: &CliOptions,
) -> spark_core::Result<PayloadReport, BenchError> {
    let total_iterations = cli.warmup_iterations + cli.iterations;
    let listener_addr = TransportSocketAddr::from(SocketAddr::from(([127, 0, 0, 1], 0)));
    let listener = TcpServerChannel::bind(listener_addr).await?;
    let bound_addr = listener.local_addr();

    let accept_ctx = CallContext::builder().build();
    let server_ctx = CallContext::builder().build();

    let (ready_tx, ready_rx) = oneshot::channel();
    let server_task = tokio::spawn(run_server_loop(
        listener,
        accept_ctx,
        server_ctx,
        payload_bytes,
        total_iterations,
        ready_tx,
    ));

    let client_ctx = CallContext::builder().build();
    let client_channel = TcpChannel::connect(&client_ctx, bound_addr).await?;
    ready_rx
        .await
        .map_err(|_| oneshot_cancelled_error("accept"))?;

    let payload = vec![0_u8; payload_bytes];
    let mut scratch = vec![0_u8; payload_bytes];

    for warmup_round in 0..cli.warmup_iterations {
        perform_roundtrip(
            &client_channel,
            &client_ctx,
            &payload,
            &mut scratch,
            warmup_round == 0,
        )
        .await?;
    }

    let mut samples = Vec::with_capacity(cli.iterations);
    for _ in 0..cli.iterations {
        let started = Instant::now();
        perform_roundtrip(&client_channel, &client_ctx, &payload, &mut scratch, false).await?;
        samples.push(started.elapsed());
    }

    drop(client_channel);

    match server_task.await {
        Ok(result) => result?,
        Err(err) => return Err(BenchError::from(join_error_to_core_error(err))),
    }

    Ok(compute_payload_report(payload_bytes, cli, &samples))
}

/// 运行服务器回显循环：接受连接并按预定轮次收发数据。
///
/// # 教案式说明
/// - **意图 (Why)**：在后台协程中模拟真实服务端，将收到的数据原样回写，形成“直通”基线。
/// - **逻辑 (How)**：
///   1. 调用 `accept` 获取 `TcpChannel`；
///   2. 向主流程发送一次性就绪信号，避免客户端在监听器尚未准备好时提前测量；
///   3. 对每个轮次执行“读满 payload → 写回 payload”，直至完成所有预定轮次。
/// - **契约 (What)**：
///   - **输入**：监听器、上下文、负载大小、总轮次以及 `oneshot::Sender`；
///   - **输出**：`spark_core::Result<(), CoreError>`，表示回显循环是否顺利完成；
///   - **前置条件**：调用方负责保证 `payload_bytes > 0` 与 `total_iterations > 0`；
///   - **后置条件**：一旦返回，TCP 连接上的所有期望数据都已被处理。
async fn run_server_loop(
    listener: TcpServerChannel,
    accept_ctx: CallContext,
    server_ctx: CallContext,
    payload_bytes: usize,
    total_iterations: usize,
    ready_tx: oneshot::Sender<()>,
) -> spark_core::Result<(), CoreError> {
    let (channel, _) = listener.accept(&accept_ctx).await?;
    let _ = ready_tx.send(());
    let mut buffer = Vec::with_capacity(payload_bytes);
    for _ in 0..total_iterations {
        read_exact(&channel, &server_ctx, &mut buffer, payload_bytes).await?;
        let mut payload = Bytes::copy_from_slice(&buffer);
        channel.write(&server_ctx, &mut payload).await?;
    }
    Ok(())
}

/// 在客户端执行一次完整的“写入 → 读取”往返。
///
/// # 教案式说明
/// - **意图 (Why)**：抽象出单次往返逻辑，便于在预热与正式采样阶段复用，同时可在首轮启用校验确保正确性。
/// - **逻辑 (How)**：
///   1. 调用 `TcpChannel::write` 写入完整 payload；
///   2. 通过 [`read_exact`] 读取同等大小数据；
///   3. 如启用 `verify_echo`，比较回显内容与原始 payload，确保服务器未篡改数据。
/// - **契约 (What)**：
///   - **输入**：客户端通道、上下文、payload、可重复利用的 `scratch` 缓冲（以 `Vec<u8>` 表示）以及布尔标志；
///   - **输出**：若成功则返回 `()`，失败时传播底层 [`CoreError`]；
///   - **前置条件**：`scratch` 必须具备至少 `payload.len()` 的容量（函数会在内部清空并复用底层内存，避免重复分配）；
///   - **后置条件**：当 `verify_echo` 为真时，保证回显内容与原始 payload 一致。
async fn perform_roundtrip(
    channel: &TcpChannel,
    ctx: &CallContext,
    payload: &[u8],
    scratch: &mut Vec<u8>,
    verify_echo: bool,
) -> spark_core::Result<(), CoreError> {
    let mut payload_buf = Bytes::copy_from_slice(payload);
    channel.write(ctx, &mut payload_buf).await?;
    read_exact(channel, ctx, scratch, payload.len()).await?;
    if verify_echo && scratch.as_slice() != payload {
        return Err(CoreError::new(
            codes::TRANSPORT_IO,
            "echo payload mismatch during warmup",
        ));
    }
    Ok(())
}

/// 从 `TcpChannel` 读取指定字节数，直至缓冲区被填满。
///
/// # 教案式说明
/// - **意图 (Why)**：TCP 读操作可能出现短读，需显式循环以确保读取完整 payload，避免在吞吐统计中混入重试逻辑的额外成本。
/// - **逻辑 (How)**：循环调用 `read`，每次直接写入 `Vec<u8>` 的尾部空间；若返回 0 视为对端提前关闭，映射为 `CoreError`。
/// - **契约 (What)**：
///   - **输入**：通道、上下文、可增长的 `Vec<u8>` 以及期望读取的字节数；
///   - **输出**：若成功读取完整数据返回 `()`，否则返回封装的 `CoreError`；
///   - **前置条件**：`buffer` 的容量至少为 `expected_len`；函数会调用 `clear()` 以重置长度。
///   - **后置条件**：成功时 `buffer.len() == expected_len`，前 `expected_len` 字节为最新读取的数据。
async fn read_exact(
    channel: &TcpChannel,
    ctx: &CallContext,
    buffer: &mut Vec<u8>,
    expected_len: usize,
) -> spark_core::Result<(), CoreError> {
    if buffer.capacity() < expected_len {
        buffer.reserve(expected_len - buffer.capacity());
    }
    buffer.clear();
    while buffer.len() < expected_len {
        let bytes = channel.read(ctx, buffer).await?;
        if bytes == 0 {
            return Err(CoreError::new(
                codes::TRANSPORT_IO,
                "unexpected EOF while reading payload",
            ));
        }
    }
    if buffer.len() > expected_len {
        buffer.truncate(expected_len);
    }
    Ok(())
}

/// 根据采样数据计算百分位与抖动统计。
///
/// # 教案式说明
/// - **意图 (Why)**：将延迟样本转化为 CI 可消费的结构化指标，避免在 CI 脚本中重复实现统计逻辑。
/// - **逻辑 (How)**：
///   1. 将 `Duration` 样本分别转换为纳秒与吞吐（MiB/s）数组；
///   2. 对两组数据排序并计算 50/99 百分位；
///   3. 基于延迟的 p50/p99 计算抖动百分比。
/// - **契约 (What)**：
///   - **输入**：负载大小、CLI 配置与延迟样本；
///   - **输出**：`PayloadReport`，包含延迟、吞吐与抖动；
///   - **前置条件**：`samples` 非空；
///   - **后置条件**：返回值中的百分比已以 100 进制表示（例如 4.2 表示 4.2%）。
fn compute_payload_report(
    payload_bytes: usize,
    cli: &CliOptions,
    samples: &[Duration],
) -> PayloadReport {
    let mut latency_ns: Vec<f64> = samples
        .iter()
        .map(|duration| duration.as_secs_f64() * 1_000_000_000.0)
        .collect();
    latency_ns.sort_by(|a, b| a.partial_cmp(b).unwrap());

    let mut throughput_mib_s: Vec<f64> = samples
        .iter()
        .map(|duration| payload_bytes as f64 / duration.as_secs_f64() / (1024.0 * 1024.0))
        .collect();
    throughput_mib_s.sort_by(|a, b| a.partial_cmp(b).unwrap());

    let p50_latency = percentile_from_sorted(&latency_ns, 0.50);
    let p99_latency = percentile_from_sorted(&latency_ns, 0.99);
    let jitter = if p50_latency <= f64::EPSILON {
        0.0
    } else {
        ((p99_latency - p50_latency) / p50_latency) * 100.0
    };

    PayloadReport {
        payload_bytes,
        iterations: cli.iterations,
        warmup_iterations: cli.warmup_iterations,
        duration_ns: PercentileStats {
            p50: p50_latency,
            p99: p99_latency,
        },
        throughput_mib_s: PercentileStats {
            p50: percentile_from_sorted(&throughput_mib_s, 0.50),
            p99: percentile_from_sorted(&throughput_mib_s, 0.99),
        },
        p99_jitter_pct: jitter,
    }
}

/// 基于排序数组计算指定百分位的值，采用线性插值。
///
/// # 教案式说明
/// - **意图 (Why)**：提供稳定、可复用的百分位计算，避免在统计阶段重复编写索引与插值逻辑。
/// - **逻辑 (How)**：
///   1. 根据样本数量计算位置 `pos = percentile * (len - 1)`；
///   2. 拆分出下界与上界索引以及小数部分；
///   3. 使用线性插值获得最终百分位值。
/// - **契约 (What)**：
///   - **输入**：已升序排序的浮点数组与 `0.0..=1.0` 的百分位系数；
///   - **输出**：对应百分位的估计值；
///   - **前置条件**：数组非空、`percentile` 落在闭区间 [0, 1]；
///   - **后置条件**：返回值位于数组最小值与最大值之间。
fn percentile_from_sorted(sorted: &[f64], percentile: f64) -> f64 {
    assert!(
        (0.0..=1.0).contains(&percentile),
        "percentile must be within [0, 1]"
    );
    let len = sorted.len();
    assert!(len > 0, "percentile requires non-empty input");
    if len == 1 {
        return sorted[0];
    }
    let position = percentile * (len - 1) as f64;
    let lower_index = position.floor() as usize;
    let upper_index = position.ceil() as usize;
    if lower_index == upper_index {
        return sorted[lower_index];
    }
    let fraction = position - lower_index as f64;
    let lower = sorted[lower_index];
    let upper = sorted[upper_index];
    lower + (upper - lower) * fraction
}

/// 将 `JoinError` 转换为 `CoreError` 并装入 boxed error，便于主流程统一处理。
fn join_error_to_core_error(err: JoinError) -> CoreError {
    CoreError::new(
        codes::TRANSPORT_IO,
        format!("server task join error: {err}"),
    )
}

/// 构造 oneshot 取消场景对应的 `CoreError`。
fn oneshot_cancelled_error(stage: &'static str) -> CoreError {
    CoreError::new(
        codes::TRANSPORT_IO,
        format!("server oneshot channel dropped before {stage} completion"),
    )
}
