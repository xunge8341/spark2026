use serde::{Deserialize, Serialize};
use spark_core::buffer::BufView;
use std::env;
use std::fs;
use std::hint::black_box;
use std::path::{Path, PathBuf};
use std::time::Instant;

/// `zerocopy_extremes` 基准：在“极端分片 × 极端载荷”的组合下量化 `BufView`
/// 在编码、解码、转发三条路径上的额外开销，并对 P99 延迟设置守门红线。
///
/// # 教案式摘要
/// - **意图 (Why)**：BufView 契约承诺“任意分片布局下额外开销 ≤ 阈值”。为了验证这一点，
///   本基准穷举“分片数 = 1/4/8/32”与“负载 = 1KiB/64KiB/1MiB”的 12 种组合，并在编码/
///   解码/转发三条逻辑路径上测量 P99 延迟与对单片基线的额外百分比。
/// - **定位 (Where)**：基准作为 `cargo bench -- --quick` 的组成部分运行在 `spark-core`
///   crate 内的独立二进制，生成 `docs/reports/benchmarks/zerocopy_extremes.{quick,full}.json`。
/// - **执行逻辑 (How)**：针对每个场景反复执行编码/解码/转发模拟函数，记录“每次调用耗时”
///   与“每 KiB 耗时”，并对样本进行分位数统计，同时计算相对单片基线的 P99 额外开销。
/// - **契约 (What)**：
///   - **输入参数**：支持 `--quick`、`--output <path>`、`--threshold <path>` 命令行参数；
///     其余参数视为非法。
///   - **输出**：标准输出打印关键指标，同时将统计结果写入 JSON 文件；若任一路径的 P99
///     或额外开销超过阈值，立即返回错误码。
///   - **前置条件**：运行环境需提供 `std`，并允许在 `docs/reports/benchmarks` 下创建文件。
///   - **后置条件**：若阈值校验通过，则保证所有场景的 `p99_ns_per_kb` 与 `p99_overhead_pct`
///     满足 SLO。
/// - **设计权衡 (Trade-offs)**：编码/转发路径会收集分片指针元数据；这会引入轻微的
///   `Vec` 指针写入，但与真实业务通过 `Chunks::from_vec` 构造 scatter/gather 列表的成本
///   保持一致，确保基准具有代表性。
fn main() {
    if let Err(error) = run() {
        eprintln!("zerocopy_extremes_error={error}");
        std::process::exit(1);
    }
}

/// 统一的错误类型，覆盖参数解析、IO 与阈值比较等失败场景。
///
/// # Why
/// - 避免在主流程中频繁使用 `expect`/`unwrap`，让错误信息对 CI 更友好。
///
/// # How
/// - 枚举化不同错误来源，`Display` 实现提供人类可读描述。
///
/// # What
/// - `Cli(String)`：命令行格式错误或缺少参数。
/// - `Io(std::io::Error)`：读写文件失败。
/// - `Serde(serde_json::Error)`：阈值 JSON 解析失败。
/// - `Threshold(String)`：P99 校验失败时的诊断信息。
#[derive(Debug)]
enum BenchError {
    Cli(String),
    Io(std::io::Error),
    Serde(serde_json::Error),
    Threshold(String),
}

impl std::fmt::Display for BenchError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BenchError::Cli(msg) => write!(f, "CLI 参数错误: {msg}"),
            BenchError::Io(err) => write!(f, "IO 失败: {err}"),
            BenchError::Serde(err) => write!(f, "JSON 解析失败: {err}"),
            BenchError::Threshold(msg) => write!(f, "阈值校验失败: {msg}"),
        }
    }
}

impl std::error::Error for BenchError {}

impl From<std::io::Error> for BenchError {
    fn from(value: std::io::Error) -> Self {
        BenchError::Io(value)
    }
}

impl From<serde_json::Error> for BenchError {
    fn from(value: serde_json::Error) -> Self {
        BenchError::Serde(value)
    }
}

/// 解析命令行参数，生成基准运行配置。
///
/// # Why
/// - 允许开发者通过 `--quick` 控制迭代次数，通过 `--output`/`--threshold` 指定自定义路径。
///
/// # How
/// - 逐个遍历 `env::args`，根据关键字匹配并写入配置结构体；遇到未知参数立即报错。
///
/// # What
/// - 返回 `CliOptions`，包含是否快速模式以及可选的输出/阈值覆盖路径。
/// - 前置条件：参数成对出现（例如 `--output` 后必须跟路径）。
/// - 后置条件：若返回成功，`args` 中的所有标志均被识别并消费。
fn parse_cli() -> spark_core::Result<CliOptions, BenchError> {
    let mut quick_mode = false;
    let mut output = None;
    let mut threshold = None;

    let mut iter = env::args().skip(1);
    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "--quick" => {
                quick_mode = true;
            }
            "--bench" => {
                // Cargo 在执行 `cargo bench -p crate --bench name` 时会追加 `--bench` 参数，
                // 不携带任何值。基准无需处理该标志，直接忽略即可。
            }
            "--output-format" => {
                let value = iter
                    .next()
                    .ok_or_else(|| BenchError::Cli("--output-format 之后缺少格式字符串".into()))?;
                // `cargo bench -- --output-format bencher` 会强制传入该标志。当前基准使用
                // 自定义的 JSON/文本输出而非 bencher 标准格式，因此仅校验该值存在即可，
                // 随后忽略，避免在 CI 中误报参数错误。
                let _ = value;
            }
            "--output" => {
                let value = iter
                    .next()
                    .ok_or_else(|| BenchError::Cli("--output 之后缺少路径".into()))?;
                output = Some(PathBuf::from(value));
            }
            "--threshold" => {
                let value = iter
                    .next()
                    .ok_or_else(|| BenchError::Cli("--threshold 之后缺少路径".into()))?;
                threshold = Some(PathBuf::from(value));
            }
            other => {
                return Err(BenchError::Cli(format!("不支持的参数: {other}")));
            }
        }
    }

    Ok(CliOptions {
        quick_mode,
        output,
        threshold,
    })
}

/// CLI 解析结果。
#[derive(Default)]
struct CliOptions {
    quick_mode: bool,
    output: Option<PathBuf>,
    threshold: Option<PathBuf>,
}

/// 基准配置，描述迭代次数与批处理大小。
///
/// # Why
/// - 为 `quick/full` 模式分别提供合适的采样密度，兼顾执行时间与统计稳定性。
///
/// # How
/// - `iterations`：总调用次数；
/// - `batch_size`：将多次调用聚合为一个计时窗口，降低计时抖动。
///
/// # What
/// - `quick_mode`：标识是否为快速模式。
/// - 前置条件：`iterations` 必须大于 0；`batch_size` 至少为 1。
/// - 后置条件：调用方需保证 `iterations` 能被批量循环消费（余数由内部处理）。
#[derive(Clone, Copy)]
struct BenchConfig {
    iterations: u64,
    batch_size: u64,
    quick_mode: bool,
}

/// 单个样本的观测值。
#[derive(Clone, Copy)]
struct SamplePoint {
    ns_per_iter: u64,
    ns_per_kb: f64,
}

/// 基准中模拟的三类路径。
///
/// # Why
/// - 与业务实践对齐：编码阶段准备 scatter/gather 元数据，解码阶段快速校验报文，转发
///   阶段保留零拷贝语义继续传递。将这三类路径显式建模，便于观察不同访问模式的性能
///   差异。
///
/// # How
/// - 作为枚举提供有限集合，并实现 `Serialize`/`Deserialize` 以便直接写入/读取 JSON
///   报告与阈值文件。
///
/// # What
/// - `Encode`：模拟出站数据编码准备；
/// - `Decode`：模拟入站数据的轻量校验；
/// - `Forward`：模拟在 Handler 链中继续传递视图。
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum OperationKind {
    Encode,
    Decode,
    Forward,
}

impl OperationKind {
    /// 返回稳定的字符串标签，供日志与 JSON 输出复用。
    fn as_str(self) -> &'static str {
        match self {
            OperationKind::Encode => "encode",
            OperationKind::Decode => "decode",
            OperationKind::Forward => "forward",
        }
    }

    /// 枚举所有路径，保持遍历顺序固定（编码→解码→转发）。
    fn all() -> &'static [OperationKind] {
        const ALL: &[OperationKind] = &[
            OperationKind::Encode,
            OperationKind::Decode,
            OperationKind::Forward,
        ];
        ALL
    }

    /// 在给定场景上执行一次模拟操作（BufView 路径）。
    fn execute_bufview(self, scenario: &ScenarioSpec, scratch: &mut OperationScratch) {
        match self {
            OperationKind::Encode => {
                encode_like(&scenario.fixture, scratch, scenario.payload_bytes)
            }
            OperationKind::Decode => decode_like(&scenario.fixture, scratch),
            OperationKind::Forward => {
                forward_like(&scenario.fixture, scratch, scenario.expected_chunks)
            }
        }
    }

    /// 在给定场景上执行一次模拟操作（基线路径）。
    fn execute_baseline(self, scenario: &ScenarioSpec, scratch: &mut OperationScratch) {
        match self {
            OperationKind::Encode => {
                encode_like_baseline(&scenario.fixture, scratch, scenario.payload_bytes)
            }
            OperationKind::Decode => decode_like_baseline(&scenario.fixture, scratch),
            OperationKind::Forward => {
                forward_like_baseline(&scenario.fixture, scratch, scenario.expected_chunks)
            }
        }
    }
}

/// Scatter/Gather 元数据片段，仅存储指针与长度以模拟真实网络发送队列。
#[derive(Clone, Copy)]
struct FramePart {
    ptr: *const u8,
    len: usize,
}

/// 为操作重用的 scratch 区域，避免在热循环中重复分配。
struct OperationScratch {
    frame_parts: Vec<FramePart>,
    decode_checksum: u64,
}

impl OperationScratch {
    fn new(expected_chunks: usize) -> Self {
        Self {
            frame_parts: Vec::with_capacity(expected_chunks),
            decode_checksum: 0,
        }
    }
}

/// 编码阶段：收集分片指针并校验总字节数。
fn encode_like(view: &ViewFixture, scratch: &mut OperationScratch, expected_bytes: usize) {
    scratch.frame_parts.clear();
    let mut total = 0usize;
    for chunk in view.as_chunks() {
        scratch.frame_parts.push(FramePart {
            ptr: chunk.as_ptr(),
            len: chunk.len(),
        });
        total += chunk.len();
        black_box(chunk);
    }
    debug_assert_eq!(total, expected_bytes, "编码阶段观察到的字节数不匹配");
    let checksum = scratch.frame_parts.iter().fold(0usize, |acc, part| {
        acc ^ part.len ^ ((part.ptr as usize) & 0xFF)
    });
    black_box(checksum);
}

/// 解码阶段：轻量验证各分片首尾字节，模拟解析器对结构化帧的快速检查。
fn decode_like(view: &ViewFixture, scratch: &mut OperationScratch) {
    let mut checksum = 0u64;
    for chunk in view.as_chunks() {
        if let Some((&first, rest)) = chunk.split_first() {
            let last = rest.last().copied().unwrap_or(first);
            checksum = checksum.wrapping_add(first as u64);
            checksum = checksum.rotate_left(7) ^ last as u64;
            checksum = checksum.wrapping_add(chunk.len() as u64);
        } else {
            checksum = checksum.rotate_left(5) ^ 0xA5;
        }
    }
    scratch.decode_checksum ^= checksum;
    black_box(scratch.decode_checksum);
}

/// 转发阶段：复用 scatter/gather 元数据并校验分片数量。
fn forward_like(view: &ViewFixture, scratch: &mut OperationScratch, expected_chunks: usize) {
    scratch.frame_parts.clear();
    let mut observed = 0usize;
    for chunk in view.as_chunks() {
        scratch.frame_parts.push(FramePart {
            ptr: chunk.as_ptr(),
            len: chunk.len(),
        });
        observed += 1;
        black_box(chunk.len());
    }
    debug_assert_eq!(observed, expected_chunks, "转发阶段观察到的分片数不匹配");
    let checksum = scratch.frame_parts.iter().fold(0usize, |acc, part| {
        acc ^ part.len ^ ((part.ptr as usize) & 0xFF)
    });
    black_box(checksum);
}

/// 遍历底层分片的辅助函数，避免基线逻辑重复匹配枚举。
fn for_each_raw_chunk(view: &ViewFixture, mut visitor: impl FnMut(&[u8])) {
    match view {
        ViewFixture::Linear(data) => visitor(data.as_slice()),
        ViewFixture::Scatter(scatter) => {
            for shard in &scatter.shards {
                visitor(shard.as_ref());
            }
        }
    }
}

/// 编码路径基线：直接访问底层分片，不经过 `BufView` 抽象。
fn encode_like_baseline(view: &ViewFixture, scratch: &mut OperationScratch, expected_bytes: usize) {
    scratch.frame_parts.clear();
    let mut total = 0usize;
    for_each_raw_chunk(view, |chunk| {
        scratch.frame_parts.push(FramePart {
            ptr: chunk.as_ptr(),
            len: chunk.len(),
        });
        total += chunk.len();
        black_box(chunk);
    });
    debug_assert_eq!(total, expected_bytes, "基线编码观察到的字节数不匹配");
    let checksum = scratch.frame_parts.iter().fold(0usize, |acc, part| {
        acc ^ part.len ^ ((part.ptr as usize) & 0xFF)
    });
    black_box(checksum);
}

/// 解码路径基线：直接访问分片首尾字节。
fn decode_like_baseline(view: &ViewFixture, scratch: &mut OperationScratch) {
    let mut checksum = 0u64;
    for_each_raw_chunk(view, |chunk| {
        if let Some((&first, rest)) = chunk.split_first() {
            let last = rest.last().copied().unwrap_or(first);
            checksum = checksum.wrapping_add(first as u64);
            checksum = checksum.rotate_left(7) ^ last as u64;
            checksum = checksum.wrapping_add(chunk.len() as u64);
        } else {
            checksum = checksum.rotate_left(5) ^ 0xA5;
        }
    });
    scratch.decode_checksum ^= checksum;
    black_box(scratch.decode_checksum);
}

/// 转发路径基线：统计分片数量并生成相同的元数据校验。
fn forward_like_baseline(
    view: &ViewFixture,
    scratch: &mut OperationScratch,
    expected_chunks: usize,
) {
    scratch.frame_parts.clear();
    let mut observed = 0usize;
    for_each_raw_chunk(view, |chunk| {
        scratch.frame_parts.push(FramePart {
            ptr: chunk.as_ptr(),
            len: chunk.len(),
        });
        observed += 1;
        black_box(chunk.len());
    });
    debug_assert_eq!(observed, expected_chunks, "基线转发观察到的分片数不匹配");
    let checksum = scratch.frame_parts.iter().fold(0usize, |acc, part| {
        acc ^ part.len ^ ((part.ptr as usize) & 0xFF)
    });
    black_box(checksum);
}

/// 基准中使用的场景定义，包含名称与底层数据形态。
///
/// # Why
/// - 将“单片超大”“超多微片”抽象为统一结构，便于后续遍历与报告生成。
///
/// # How
/// - `fixture` 保存底层缓冲，`payload_bytes`/`expected_chunks` 提供契约校验所需的元信息。
///
/// # What
/// - `name`：场景名称，用于日志与阈值匹配。
/// - `payload_bytes`：每次遍历应观察到的总字节数。
/// - `expected_chunks`：理论分片数量，保证实现未擅自折叠。
struct ScenarioSpec {
    name: String,
    payload_label: &'static str,
    chunk_count: usize,
    fixture: ViewFixture,
    payload_bytes: usize,
    expected_chunks: usize,
}

impl ScenarioSpec {
    fn new(payload_label: &'static str, chunk_count: usize, fixture: ViewFixture) -> Self {
        let payload_bytes = fixture.total_len();
        let expected_chunks = fixture.expected_chunks();
        let name = format!("payload_{}_chunks_{}", payload_label, chunk_count);
        debug_assert_eq!(
            expected_chunks,
            if payload_bytes == 0 { 0 } else { chunk_count },
            "期望分片数与配置不符"
        );
        Self {
            name,
            payload_label,
            chunk_count,
            fixture,
            payload_bytes,
            expected_chunks,
        }
    }
}

/// 场景中用到的缓冲类型枚举。
///
/// # Why
/// - 需要同时覆盖 `Vec<u8>`（单片）与自定义 `ScatterView`（多片）两种布局。
///
/// # How
/// - 通过 `as_view` 返回统一的 `&dyn BufView` 接口。
///
/// # What
/// - `Linear(Vec<u8>)`：单片连续缓冲。
/// - `Scatter(ScatterView)`：按配置拆分的多片缓冲。
enum ViewFixture {
    Linear(Vec<u8>),
    Scatter(ScatterView),
}

impl ViewFixture {
    fn total_len(&self) -> usize {
        match self {
            ViewFixture::Linear(data) => data.len(),
            ViewFixture::Scatter(view) => view.total_len,
        }
    }

    fn expected_chunks(&self) -> usize {
        match self {
            ViewFixture::Linear(data) => {
                if data.is_empty() {
                    0
                } else {
                    1
                }
            }
            ViewFixture::Scatter(view) => view.chunk_count,
        }
    }
}

impl BufView for ViewFixture {
    fn as_chunks(&self) -> spark_core::buffer::Chunks<'_> {
        match self {
            ViewFixture::Linear(data) => spark_core::buffer::Chunks::from_single(data.as_slice()),
            ViewFixture::Scatter(view) => view.as_chunks(),
        }
    }

    fn len(&self) -> usize {
        self.total_len()
    }
}

/// 多分片缓冲视图实现，模拟极端 scatter/gather 场景。
///
/// # Why
/// - 真实业务中会出现“碎片化极多但总量有限”的情况，例如基于 `VecDeque` 或 ring buffer
///   的收发队列；该结构用于复现这类行为。
///
/// # How
/// - 构造时按给定每片大小生成 `Box<[u8]>`，`as_chunks` 通过指针向量零拷贝暴露。
///
/// # What
/// - `total_len`：缓存的总字节数，用于契约校验。
/// - `chunk_count`：分片数量，帮助阈值报告。
/// - 前置条件：构造参数必须保证 `chunk_len * chunk_count == total_len`。
/// - 后置条件：`as_chunks` 返回的切片顺序与构造时一致。
struct ScatterView {
    shards: Vec<Box<[u8]>>,
    total_len: usize,
    chunk_count: usize,
}

impl ScatterView {
    /// 构造多分片缓冲。
    ///
    /// # Why
    /// - 通过统一的工厂方法封装初始化细节，使得基准在不同分片规模下复用同一实现。
    ///
    /// # How
    /// - 为每个分片分配一段 `Box<[u8]>`，首字节写入索引用于调试；整体容量与输入参数成正比。
    ///
    /// # What
    /// - **输入**：`chunk_len` 为单片长度，`chunk_count` 为分片数量。
    /// - **前置条件**：二者均需大于 0；否则直接 panic，避免产生空视图干扰基准。
    /// - **后置条件**：`total_len` 与 `chunk_count` 与输入一致，且所有分片独立存储。
    ///
    /// # Trade-offs
    /// - 为了确保零拷贝语义，每次构造都会真实分配 `chunk_count` 个盒装切片，会占用额外内存；
    ///   基准默认使用 4K 级别的分片数量以避免 OOM。
    fn new(chunk_len: usize, chunk_count: usize) -> Self {
        assert!(chunk_len > 0, "chunk_len 必须大于 0");
        assert!(chunk_count > 0, "chunk_count 必须大于 0");
        let mut shards = Vec::with_capacity(chunk_count);
        for idx in 0..chunk_count {
            let mut data = vec![0u8; chunk_len];
            data[0] = (idx & 0xFF) as u8;
            shards.push(data.into_boxed_slice());
        }
        Self {
            shards,
            total_len: chunk_len * chunk_count,
            chunk_count,
        }
    }
}

impl BufView for ScatterView {
    fn as_chunks(&self) -> spark_core::buffer::Chunks<'_> {
        let mut slices = Vec::with_capacity(self.shards.len());
        for shard in &self.shards {
            slices.push(shard.as_ref());
        }
        spark_core::buffer::Chunks::from_vec(slices)
    }

    fn len(&self) -> usize {
        self.total_len
    }
}

/// 延迟统计摘要，用于 JSON 序列化。
///
/// # Why
/// - 在报告中记录 P50/P95/P99 同时追加 `std_dev`，方便后续脚本计算变异系数，
///   防止仅凭极值判断误判噪音。
///
/// # How
/// - 在 [`analyze_ns`] 与 [`analyze_ns_per_kb`] 中计算均值后，追加一次基于 Bessel 校正
///   的样本标准差，从而捕获离散程度。
///
/// # What
/// - 结构体字段新增 `std_dev`，单位与当前统计量一致（`ns` 或 `ns/KB`）。
/// - 前置条件：样本数量 ≥ 1；当样本不足 2 个时标准差回落为 0。
#[derive(Clone, Copy, Debug, Serialize)]
struct LatencySummaryF64 {
    p50: f64,
    p95: f64,
    p99: f64,
    mean: f64,
    min: f64,
    max: f64,
    std_dev: f64,
}

#[derive(Clone, Copy, Debug, Serialize)]
struct LatencySummaryU64 {
    p50: u64,
    p95: u64,
    p99: u64,
    mean: f64,
    min: u64,
    max: u64,
    std_dev: f64,
}

/// 单场景基准结果。
#[derive(Debug, Serialize)]
struct ScenarioReport {
    name: String,
    payload_label: &'static str,
    payload_bytes: usize,
    chunk_count: usize,
    expected_chunks: usize,
    operations: Vec<OperationReport>,
}

/// 单条路径的统计摘要，同时记录 BufView 与基线耗时。
///
/// # Why
/// - 将 BufView 与裸分片基线的分位数、均值与波动度一并存储，便于 CI 工具直接读取并判定
///   “额外开销是否可接受、样本是否稳定”。
///
/// # How
/// - 采样阶段会分别构建 BufView 与基线的 [`LatencySummaryF64`] / [`LatencySummaryU64`]；
/// - 新增字段 `bufview_ns_per_kb_cv` 与 `baseline_ns_per_kb_cv` 由样本标准差除以均值获得，
///   采用 Bessel 校正以减少低样本偏差；
/// - `p99_overhead_pct` 直接使用 `BufView P99` 与 `基线 P99` 的比值换算。
///
/// # What
/// - `samples`：本次测量的批次数，保证 JSON 中存在上下文；
/// - `bufview_ns_per_iter`/`baseline_ns_per_iter`：裸纳秒视角下的延迟统计；
/// - `bufview_ns_per_kb`/`baseline_ns_per_kb`：按 KB 归一化后的延迟统计；
/// - `bufview_ns_per_kb_cv`/`baseline_ns_per_kb_cv`：供脚本判定“CV < 0.2”约束；
/// - `p99_overhead_pct`：CI 的核心指标，目标 ≤ 3%。
#[derive(Debug, Serialize)]
struct OperationReport {
    kind: OperationKind,
    samples: u64,
    bufview_ns_per_iter: LatencySummaryU64,
    bufview_ns_per_kb: LatencySummaryF64,
    baseline_ns_per_iter: LatencySummaryU64,
    baseline_ns_per_kb: LatencySummaryF64,
    p99_overhead_pct: f64,
    bufview_ns_per_kb_cv: f64,
    baseline_ns_per_kb_cv: f64,
}

/// 总体基准报告。
#[derive(Debug, Serialize)]
struct BenchmarkReport {
    benchmark: &'static str,
    quick_mode: bool,
    iterations: u64,
    batch_size: u64,
    scenarios: Vec<ScenarioReport>,
}

/// 阈值文件结构。
#[derive(Debug, Deserialize)]
struct ThresholdFile {
    benchmark: String,
    modes: Vec<ModeThreshold>,
}

#[derive(Debug, Deserialize)]
struct ModeThreshold {
    quick_mode: bool,
    scenarios: Vec<ScenarioThreshold>,
}

#[derive(Debug, Deserialize)]
struct ScenarioThreshold {
    name: String,
    operations: Vec<OperationThreshold>,
}

#[derive(Debug, Deserialize)]
struct OperationThreshold {
    kind: OperationKind,
    max_p99_ns_per_kb: f64,
    max_mean_ns_per_kb: Option<f64>,
    max_p99_overhead_pct: Option<f64>,
}

/// 基准主流程：解析 CLI、运行场景、生成报告并执行阈值校验。
///
/// # Why
/// - 集中处理生命周期，确保执行顺序固定：先采样、再校验、最后持久化。
///
/// # How
/// 1. 调用 [`parse_cli`] 获取运行模式与路径覆盖；
/// 2. 根据模式构造 [`BenchConfig`]；
/// 3. 遍历 [`build_scenarios`] 返回的场景并采样；
/// 4. 输出测量结果（包含基线与 BufView 的对比统计）；
/// 5. 读取阈值文件、执行 [`enforce_thresholds`]（`debug` 配置下仅加载并打印提示，不强制校验）；
/// 6. 将结果写入 JSON。
///
/// # What
/// - 返回 `spark_core::Result<(), BenchError>`：成功时说明阈值满足，失败时携带诊断信息。
/// - 前置条件：工作目录位于仓库根目录附近，使默认路径可解析。
/// - 后置条件：若成功，`docs/reports/benchmarks` 中会出现最新报告。
fn run() -> spark_core::Result<(), BenchError> {
    let cli = parse_cli()?;
    let config = if cli.quick_mode {
        BenchConfig {
            iterations: 131_072,
            batch_size: 1_024,
            quick_mode: true,
        }
    } else {
        BenchConfig {
            iterations: 1_048_576,
            batch_size: 4_096,
            quick_mode: false,
        }
    };

    let scenarios = build_scenarios();
    let mut reports = Vec::with_capacity(scenarios.len());

    for scenario in &scenarios {
        reports.push(measure_scenario(scenario, config));
    }

    let report = BenchmarkReport {
        benchmark: "zerocopy_extremes",
        quick_mode: config.quick_mode,
        iterations: config.iterations,
        batch_size: config.batch_size,
        scenarios: reports,
    };

    for scenario in &report.scenarios {
        for operation in &scenario.operations {
            println!(
                "scenario={} operation={} bufview_p99_ns_per_kb={:.3} baseline_p99_ns_per_kb={:.3} p99_overhead_pct={:.3} bufview_cv_ns_per_kb={:.6} baseline_cv_ns_per_kb={:.6}",
                scenario.name,
                operation.kind.as_str(),
                operation.bufview_ns_per_kb.p99,
                operation.baseline_ns_per_kb.p99,
                operation.p99_overhead_pct,
                operation.bufview_ns_per_kb_cv,
                operation.baseline_ns_per_kb_cv,
            );
        }
    }

    let thresholds = load_thresholds(cli.threshold.as_deref(), config.quick_mode)?;
    if cfg!(debug_assertions) {
        println!(
            "debug_profile=1 skip_threshold_enforcement=1 reason=non-optimized build would violate latency SLOs"
        );
    } else {
        enforce_thresholds(&report, &thresholds)?;
    }
    persist_report(&report, cli.output.as_deref())?;

    Ok(())
}

/// 构造基准场景列表。
///
/// # Why
/// - 将极端分片模式固定在代码中，避免不同开发者在本地选择不同的组合导致结果不可对比。
///
/// # How
/// - 穷举 `{1, 4, 8, 32}` 四种分片数量；
/// - 结合 `{1KiB, 64KiB, 1MiB}` 三种负载大小，共生成 12 个场景。
///
/// # What
/// - 返回按顺序排列的 [`ScenarioSpec`] 向量。
/// - 前置条件：内存足以容纳最大场景（1MiB × 32 片）。
/// - 后置条件：每个场景的 `payload_bytes` 能被 `chunk_count` 整除。
fn build_scenarios() -> Vec<ScenarioSpec> {
    let payloads = [
        (1024usize, "1kb"),
        (64 * 1024usize, "64kb"),
        (1024 * 1024usize, "1mb"),
    ];
    let chunk_counts = [1usize, 4, 8, 32];

    let mut scenarios = Vec::new();
    for (payload_bytes, label) in payloads {
        for &chunk_count in &chunk_counts {
            assert_eq!(
                payload_bytes % chunk_count,
                0,
                "payload={} 不可整除 chunk_count={}",
                payload_bytes,
                chunk_count
            );
            let fixture = if chunk_count == 1 {
                ViewFixture::Linear(vec![0u8; payload_bytes])
            } else {
                let chunk_len = payload_bytes / chunk_count;
                ViewFixture::Scatter(ScatterView::new(chunk_len, chunk_count))
            };
            scenarios.push(ScenarioSpec::new(label, chunk_count, fixture));
        }
    }

    scenarios
}

/// 针对单个场景执行测量。
///
/// # Why
/// - 需要在同一数据布局下比较编码/解码/转发路径的差异。
///
/// # How
/// - 首先调用 [`validate_fixture`] 验证分片契约；
/// - 随后依次调用 [`measure_operation`]，对三条路径分别收集样本；
/// - 将结果聚合为结构化报告。
///
/// # What
/// - 输入：`scenario` 提供数据与期望，`config` 控制迭代总量。
/// - 输出：`ScenarioReport`，包含三条路径的统计摘要。
/// - 前置条件：场景在构建时确保 `payload_bytes` 能被 `chunk_count` 整除。
/// - 后置条件：若成功返回，`operations` 列表中每条路径至少包含一个样本。
fn measure_scenario(scenario: &ScenarioSpec, config: BenchConfig) -> ScenarioReport {
    validate_fixture(scenario);

    let mut operations = Vec::new();
    for &operation in OperationKind::all() {
        operations.push(measure_operation(scenario, config, operation));
    }

    ScenarioReport {
        name: scenario.name.clone(),
        payload_label: scenario.payload_label,
        payload_bytes: scenario.payload_bytes,
        chunk_count: scenario.chunk_count,
        expected_chunks: scenario.expected_chunks,
        operations,
    }
}

/// 针对指定路径执行基准采样。
///
/// # Why
/// - 不同路径的访问模式（指针收集、轻量校验、透明转发）对缓存友好度与分片数量敏感度
///   各不相同，将其拆分可以更精确地暴露瓶颈。
///
/// # How
/// - 复用批量计时逻辑：每个批次分别执行 BufView 路径与基线路径，记录平均纳秒；
/// - 将两组耗时换算为 `ns/KB`，并通过统计函数计算分位数与额外开销百分比。
/// - 根据 `mean`/`std_dev` 计算 BufView 与基线的 CV，方便 CI 判断采样稳定性。
///
/// # What
/// - 输入：场景定义、基准配置、待测路径枚举值；
/// - 输出：`OperationReport`，同时包含 BufView 与基线路径的统计结果；
/// - 前置条件：`validate_fixture` 已确认分片契约成立；
/// - 后置条件：`p99_overhead_pct` 以 `(BufView/BaseLine - 1) * 100` 计算完成。
fn measure_operation(
    scenario: &ScenarioSpec,
    config: BenchConfig,
    operation: OperationKind,
) -> OperationReport {
    let capacity = (config.iterations / config.batch_size) as usize + 1;
    let mut bufview_samples = Vec::with_capacity(capacity);
    let mut baseline_samples = Vec::with_capacity(capacity);
    let mut bufview_scratch = OperationScratch::new(scenario.expected_chunks.max(1));
    let mut baseline_scratch = OperationScratch::new(scenario.expected_chunks.max(1));

    let mut remaining = config.iterations;
    while remaining > 0 {
        let chunk = remaining.min(config.batch_size);

        let started_bufview = Instant::now();
        for _ in 0..chunk {
            operation.execute_bufview(scenario, &mut bufview_scratch);
        }
        let bufview_elapsed = started_bufview.elapsed().as_nanos();
        let bufview_per_iter = ((bufview_elapsed + (chunk as u128 / 2)) / chunk as u128) as u64;

        let started_baseline = Instant::now();
        for _ in 0..chunk {
            operation.execute_baseline(scenario, &mut baseline_scratch);
        }
        let baseline_elapsed = started_baseline.elapsed().as_nanos();
        let baseline_per_iter = ((baseline_elapsed + (chunk as u128 / 2)) / chunk as u128) as u64;

        let bytes = scenario.payload_bytes as f64;
        let bufview_ns_per_kb = if bytes == 0.0 {
            0.0
        } else {
            (bufview_per_iter as f64) / (bytes / 1024.0)
        };
        let baseline_ns_per_kb = if bytes == 0.0 {
            0.0
        } else {
            (baseline_per_iter as f64) / (bytes / 1024.0)
        };

        bufview_samples.push(SamplePoint {
            ns_per_iter: bufview_per_iter,
            ns_per_kb: bufview_ns_per_kb,
        });
        baseline_samples.push(SamplePoint {
            ns_per_iter: baseline_per_iter,
            ns_per_kb: baseline_ns_per_kb,
        });

        remaining -= chunk;
    }

    let bufview_ns_per_iter = analyze_ns(&bufview_samples);
    let bufview_ns_per_kb = analyze_ns_per_kb(&bufview_samples);
    let baseline_ns_per_iter = analyze_ns(&baseline_samples);
    let baseline_ns_per_kb = analyze_ns_per_kb(&baseline_samples);

    let p99_overhead_pct = if baseline_ns_per_kb.p99 > 0.0 {
        ((bufview_ns_per_kb.p99 / baseline_ns_per_kb.p99) - 1.0) * 100.0
    } else {
        0.0
    };
    let bufview_cv = if bufview_ns_per_kb.mean > 0.0 {
        bufview_ns_per_kb.std_dev / bufview_ns_per_kb.mean
    } else {
        0.0
    };
    let baseline_cv = if baseline_ns_per_kb.mean > 0.0 {
        baseline_ns_per_kb.std_dev / baseline_ns_per_kb.mean
    } else {
        0.0
    };

    OperationReport {
        kind: operation,
        samples: bufview_samples.len() as u64,
        bufview_ns_per_iter,
        bufview_ns_per_kb,
        baseline_ns_per_iter,
        baseline_ns_per_kb,
        p99_overhead_pct,
        bufview_ns_per_kb_cv: bufview_cv,
        baseline_ns_per_kb_cv: baseline_cv,
    }
}

/// 验证 BufView 契约，确保分片统计与场景描述一致。
///
/// # Why
/// - 在基准开始前进行一次快速检查，避免因实现偏差导致后续采样结果失真。
///
/// # How
/// - 迭代全部分片并统计字节数、分片数，与场景提供的期望值对比；
/// - 使用 `black_box` 阻止编译器将迭代优化掉。
///
/// # What
/// - 输入：[`ScenarioSpec`]，包含期望的总字节与分片数量。
/// - 前置条件：`fixture` 在调用期间保持只读。
/// - 后置条件：若契约不满足直接 panic，阻断基准执行。
fn validate_fixture(scenario: &ScenarioSpec) {
    let view = &scenario.fixture;
    let mut observed_bytes = 0usize;
    let mut observed_chunks = 0usize;
    for chunk in view.as_chunks() {
        observed_bytes += chunk.len();
        observed_chunks += 1;
        black_box(chunk.len());
    }
    assert_eq!(observed_bytes, scenario.payload_bytes, "分片长度总和不匹配");
    assert_eq!(observed_chunks, scenario.expected_chunks, "分片数量不匹配");
    black_box(observed_chunks);
}

/// 计算“每次调用耗时 (ns)”的统计量。
///
/// # Why
/// - 原始纳秒数据可帮助确认 Batch 均摊是否合理，同时为分析器提供绝对耗时基线。
///
/// # How
/// - 将样本排序后计算 P50/P95/P99；
/// - `mean` 使用 `u128` 累加避免溢出，同时结合 Bessel 校正计算样本标准差，方便后续
///   计算变异系数。
///
/// # What
/// - 输入：按批次收集的样本数组。
/// - 输出：[`LatencySummaryU64`]，记录分位数、均值以及 `std_dev`。
/// - 前置条件：样本可为空（空时返回全 0），调用方需自行确保批次>0。
/// - 后置条件：排序在本地 Vec 上进行，不会修改原输入。
fn analyze_ns(samples: &[SamplePoint]) -> LatencySummaryU64 {
    let mut values: Vec<u64> = samples.iter().map(|s| s.ns_per_iter).collect();
    values.sort_unstable();
    let min = *values.first().unwrap_or(&0);
    let max = *values.last().unwrap_or(&0);
    let (mean, std_dev) = if values.is_empty() {
        (0.0, 0.0)
    } else {
        let sum: u128 = values.iter().map(|&v| v as u128).sum();
        let mean = (sum as f64) / (values.len() as f64);
        let std_dev = if values.len() > 1 {
            let variance: f64 = values
                .iter()
                .map(|&v| {
                    let diff = (v as f64) - mean;
                    diff * diff
                })
                .sum::<f64>()
                / ((values.len() - 1) as f64);
            variance.sqrt()
        } else {
            0.0
        };
        (mean, std_dev)
    };
    LatencySummaryU64 {
        p50: percentile_u64(&values, 0.50),
        p95: percentile_u64(&values, 0.95),
        p99: percentile_u64(&values, 0.99),
        mean,
        min,
        max,
        std_dev,
    }
}

/// 计算“每 KB 耗时 (ns/KB)”的统计量。
///
/// # Why
/// - 阈值以 `ns/KB` 定义，需要单独的统计过程来规避舍入误差。
///
/// # How
/// - 复制样本后按浮点排序，使用线性插值计算分位数；
/// - 累加均值的同时计算样本标准差，为 CV 判定提供数据。
///
/// # What
/// - 输入：与 [`analyze_ns`] 相同的样本数组。
/// - 输出：[`LatencySummaryF64`]，包含 `std_dev` 供外部计算 CV。
/// - 前置条件：样本可能包含 `0.0`，排序需处理浮点比较。
/// - 后置条件：统计过程中不修改原样本。
fn analyze_ns_per_kb(samples: &[SamplePoint]) -> LatencySummaryF64 {
    let mut values: Vec<f64> = samples.iter().map(|s| s.ns_per_kb).collect();
    values.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let min = *values.first().unwrap_or(&0.0);
    let max = *values.last().unwrap_or(&0.0);
    let (mean, std_dev) = if values.is_empty() {
        (0.0, 0.0)
    } else {
        let mean = values.iter().sum::<f64>() / (values.len() as f64);
        let std_dev = if values.len() > 1 {
            let variance: f64 = values
                .iter()
                .map(|&v| {
                    let diff = v - mean;
                    diff * diff
                })
                .sum::<f64>()
                / ((values.len() - 1) as f64);
            variance.sqrt()
        } else {
            0.0
        };
        (mean, std_dev)
    };
    LatencySummaryF64 {
        p50: percentile_f64(&values, 0.50),
        p95: percentile_f64(&values, 0.95),
        p99: percentile_f64(&values, 0.99),
        mean,
        min,
        max,
        std_dev,
    }
}

/// 计算整型样本的分位数，使用线性插值避免阶梯效应。
///
/// # Why
/// - Benchmark 需要 P99 等非整数分位数，直接取下界会低估真实延迟。
///
/// # How
/// - 根据 `quantile` 计算浮点 rank，向下/向上取整后线性插值，最终四舍五入回 `u64`。
///
/// # What
/// - 输入：已排序的样本数组与目标分位数（0..=1）。
/// - 输出：估计值；当样本为空时返回 0。
/// - 风险：若数组未排序，将产生错误结果；调用方需事先保证排序。
fn percentile_u64(sorted: &[u64], quantile: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let rank = (quantile.clamp(0.0, 1.0)) * ((sorted.len() - 1) as f64);
    let lower = rank.floor() as usize;
    let upper = rank.ceil() as usize;
    if lower == upper {
        sorted[lower]
    } else {
        let weight = rank - lower as f64;
        let low = sorted[lower] as f64;
        let high = sorted[upper] as f64;
        (low + (high - low) * weight).round() as u64
    }
}

/// 计算浮点样本的分位数。
///
/// # Why
/// - `ns/KB` 指标为浮点，需要保持高精度插值，避免 P99 判定时出现过度保守的误差。
///
/// # How
/// - 与 [`percentile_u64`] 相同，采用 rank 插值。
///
/// # What
/// - 输入：已排序的 `f64` 数组与分位数。
/// - 输出：对应的估计值，空数组返回 0.0。
/// - 注意：浮点排序依赖 `partial_cmp`，若出现 NaN 将 panic；基准内部不生成 NaN。
fn percentile_f64(sorted: &[f64], quantile: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let rank = (quantile.clamp(0.0, 1.0)) * ((sorted.len() - 1) as f64);
    let lower = rank.floor() as usize;
    let upper = rank.ceil() as usize;
    if lower == upper {
        sorted[lower]
    } else {
        let weight = rank - lower as f64;
        sorted[lower] + (sorted[upper] - sorted[lower]) * weight
    }
}

/// 读取阈值配置，返回与当前模式匹配的规则。
///
/// # Why
/// - 让 CI 可以通过更新 JSON 文件调整 SLO，而无需改动 Rust 源码。
///
/// # How
/// - 确定阈值文件路径（命令行覆盖 > 默认）；
/// - 使用 `serde_json` 解析为 [`ThresholdFile`]；
/// - 按 `quick_mode` 选择对应配置。
///
/// # What
/// - 输入：可选路径与模式标识。
/// - 输出：[`ModeThreshold`]，供 [`enforce_thresholds`] 使用。
/// - 前置条件：JSON 顶层 `benchmark` 必须等于 `zerocopy_extremes`。
/// - 后置条件：若模式缺失或名称不匹配，返回 `BenchError::Threshold`。
fn load_thresholds(
    path: Option<&Path>,
    quick_mode: bool,
) -> spark_core::Result<ModeThreshold, BenchError> {
    let repo_root = resolve_repo_root();
    let default_path = repo_root
        .join("docs")
        .join("reports")
        .join("benchmarks")
        .join("zerocopy_extremes.thresholds.json");
    let target_path = path.unwrap_or(default_path.as_path());
    let content = fs::read_to_string(target_path)?;
    let thresholds: ThresholdFile = serde_json::from_str(&content)?;
    if thresholds.benchmark != "zerocopy_extremes" {
        return Err(BenchError::Threshold(format!(
            "阈值文件基准名称不匹配: {}",
            thresholds.benchmark
        )));
    }
    thresholds
        .modes
        .into_iter()
        .find(|mode| mode.quick_mode == quick_mode)
        .ok_or_else(|| {
            BenchError::Threshold(format!(
                "阈值文件缺少 {} 模式配置",
                if quick_mode { "quick" } else { "full" }
            ))
        })
}

/// 将测量结果与阈值进行对比，超出则报错。
///
/// # Why
/// - 在 CI 中自动守护“P99 ≤ 既定目标”的承诺，避免性能回退默默溜走。
///
/// # How
/// - 对每个场景查找阈值项，分别比较 `p99` 与可选的 `mean`；
/// - 收集所有违规项，统一输出，便于一次性修复多个问题。
///
/// # What
/// - 输入：测量报告与对应阈值。
/// - 输出：成功或 `BenchError::Threshold`，其中包含违规详情。
/// - 前置条件：阈值文件需覆盖所有场景。
/// - 后置条件：若函数返回 `Ok(())`，说明所有场景均满足 SLO。
fn enforce_thresholds(
    report: &BenchmarkReport,
    thresholds: &ModeThreshold,
) -> spark_core::Result<(), BenchError> {
    let mut violations = Vec::new();
    for scenario in &report.scenarios {
        let Some(limit) = thresholds
            .scenarios
            .iter()
            .find(|entry| entry.name == scenario.name)
        else {
            return Err(BenchError::Threshold(format!(
                "阈值文件未定义场景 {}",
                scenario.name
            )));
        };

        for operation in &scenario.operations {
            let Some(op_limit) = limit
                .operations
                .iter()
                .find(|entry| entry.kind == operation.kind)
            else {
                return Err(BenchError::Threshold(format!(
                    "阈值文件未定义场景 {} 的 {} 路径",
                    scenario.name,
                    operation.kind.as_str()
                )));
            };

            if operation.bufview_ns_per_kb.p99 > op_limit.max_p99_ns_per_kb {
                violations.push(format!(
                    "场景 {} 路径 {} 的 P99 {:.3}ns/KB 超过阈值 {:.3}ns/KB",
                    scenario.name,
                    operation.kind.as_str(),
                    operation.bufview_ns_per_kb.p99,
                    op_limit.max_p99_ns_per_kb
                ));
            }
            if let Some(max_mean) = op_limit.max_mean_ns_per_kb
                && operation.bufview_ns_per_kb.mean > max_mean
            {
                violations.push(format!(
                    "场景 {} 路径 {} 的均值 {:.3}ns/KB 超过阈值 {:.3}ns/KB",
                    scenario.name,
                    operation.kind.as_str(),
                    operation.bufview_ns_per_kb.mean,
                    max_mean
                ));
            }
            if let Some(max_overhead) = op_limit.max_p99_overhead_pct
                && operation.p99_overhead_pct > max_overhead
            {
                violations.push(format!(
                    "场景 {} 路径 {} 的 P99 额外开销 {:.3}% 超过阈值 {:.3}%",
                    scenario.name,
                    operation.kind.as_str(),
                    operation.p99_overhead_pct,
                    max_overhead
                ));
            }
        }
    }

    if violations.is_empty() {
        Ok(())
    } else {
        Err(BenchError::Threshold(violations.join("; ")))
    }
}

/// 按约定路径输出基准结果 JSON。
///
/// # Why
/// - 固化最新数据，便于文档引用与历史对比，同时供外部可视化工具消费。
///
/// # How
/// - 根据模式选择默认文件名，可被 `--output` 覆盖；
/// - 确保目录存在后写入 `serde_json` 序列化结果。
///
/// # What
/// - 输入：基准报告与可选覆盖路径。
/// - 输出：成功或 IO/序列化错误。
/// - 前置条件：文件系统允许创建/覆盖目标路径。
/// - 后置条件：若成功，会在标准输出打印 `report_path=` 便于 CI 抓取。
fn persist_report(
    report: &BenchmarkReport,
    override_path: Option<&Path>,
) -> spark_core::Result<(), BenchError> {
    let repo_root = resolve_repo_root();
    let default_file = if report.quick_mode {
        "zerocopy_extremes.quick.json"
    } else {
        "zerocopy_extremes.full.json"
    };
    let default_path = repo_root
        .join("docs")
        .join("reports")
        .join("benchmarks")
        .join(default_file);
    let output_path = override_path.map(PathBuf::from).unwrap_or(default_path);
    if let Some(parent) = output_path.parent() {
        fs::create_dir_all(parent)?;
    }
    let file = fs::File::create(&output_path)?;
    serde_json::to_writer_pretty(file, report)?;
    println!("report_path={}", output_path.display());
    Ok(())
}

/// 计算仓库根路径，兼容 `crates/` 子目录的新版布局。
///
/// ## 教案说明（Why）
/// - 重构后 `spark-core` 移入 `crates/` 层，`CARGO_MANIFEST_DIR` 不再等价于仓库根，直接拼接
///   文档路径会失败；
/// - 将路径爬升逻辑集中在一处，可供阈值加载与报告持久化复用，减少遗漏点。
///
/// ## 实现要点（How）
/// - 基于 `env!("CARGO_MANIFEST_DIR")` 获取当前 crate 的物理位置；
/// - 连续调用两次 `parent()`，从 `crates/<crate>` 回退到仓库根目录；
/// - 若目录层级不符合预期，`expect` 将立即报告，提醒维护者调整仓库布局。
fn resolve_repo_root() -> PathBuf {
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    manifest_dir
        .parent()
        .and_then(|dir| dir.parent())
        .expect("workspace expects crates/<name> layout")
        .to_path_buf()
}
