pub mod events;
pub mod facade;
pub mod health;
pub mod logging;
pub mod metrics;
pub mod resource;
pub mod trace;

pub use events::{CoreUserEvent, EventPolicy, OpsEvent, OpsEventBus, OpsEventKind};
#[allow(deprecated)]
pub use facade::{DefaultObservabilityFacade, LegacyObservabilityHandles};
pub use health::{ComponentHealth, HealthCheckProvider, HealthChecks, HealthState};
pub use logging::{LogField, LogRecord, LogSeverity, Logger};
pub use metrics::{Counter, Gauge, Histogram, InstrumentDescriptor, MetricsProvider};
pub use resource::resource_from_attrs;

use std::{
    borrow::Cow,
    sync::{Arc, OnceLock},
};

use opentelemetry::{global, trace::TracerProvider as _};
use opentelemetry_sdk::{
    Resource,
    trace::{self as sdktrace, TracerProvider},
};
use spark_core::pipeline::instrument::{
    HandlerSpanTracer, HandlerTracerError, install_handler_tracer,
};
use trace::make_handler_tracer;
use tracing::dispatcher;
use tracing_subscriber::{EnvFilter, layer::SubscriberExt};

#[cfg(feature = "test-util")]
use opentelemetry_sdk::export::trace::SpanData;
#[cfg(feature = "test-util")]
use test_support::InMemorySpanExporter;

/// 安装状态的全局缓存，确保 `install` 仅执行一次。
static INSTALL_STATE: OnceLock<InstallState> = OnceLock::new();

/// spark-otel 安装过程可能出现的错误类型。
///
/// # 教案式说明
/// - **意图（Why）**：框架化归纳安装阶段的全部失败路径，便于调用方在集成测试或启动流程中统一处理。
/// - **逻辑（How）**：覆盖“重复安装”“外部已设置 tracing Subscriber”“TraceState 转换失败”三类典型错误，并保留底层错误信息。
/// - **契约（What）**：所有错误都实现 [`std::error::Error`]，可直接交给 `anyhow`/`eyre` 等上层框架处理。
#[derive(Debug)]
pub enum Error {
    /// `spark_otel::install` 被重复调用。
    AlreadyInstalled,
    /// 外部提前设置了全局 `tracing` Subscriber，无法再次注册。
    SubscriberAlreadySet,
    /// `spark-core` 的 HandlerTracer 插槽已存在实现。
    HandlerTracerInstalled,
    /// W3C TraceState 在互转过程中校验失败。
    TraceStateConversion(String),
    /// 设置全局 Subscriber 失败的底层错误。
    SetGlobalSubscriber(tracing::dispatcher::SetGlobalDefaultError),
}

impl core::fmt::Display for Error {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Error::AlreadyInstalled => f.write_str("spark-otel 已完成安装，禁止重复调用 install"),
            Error::SubscriberAlreadySet => {
                f.write_str("全局 tracing Subscriber 已存在，spark-otel 无法覆盖")
            }
            Error::HandlerTracerInstalled => f.write_str("spark-core 已注册其他 HandlerSpanTracer"),
            Error::TraceStateConversion(reason) => write!(f, "TraceState 互转失败: {reason}"),
            Error::SetGlobalSubscriber(err) => {
                write!(f, "设置 tracing 全局 Subscriber 失败: {err}")
            }
        }
    }
}

impl std::error::Error for Error {}

/// 框架安装后的持久状态，用于保持 OpenTelemetry Provider 与 Handler Tracer 生命周期。
struct InstallState {
    // 教案级说明（Why）
    // - `TracerProvider` 需保持有效以支撑进程生命周期内的导出/刷新操作；
    //   尽管目前仅依赖全局注册，也保留本地引用以便未来实现显式 shutdown。
    //
    // 教案级说明（Trade-offs）
    // - 该字段当前仅为生命周期护栏，因此以 `#[allow(dead_code)]` 抑制“未读取”告警，保持命名语义。
    #[allow(dead_code)]
    provider: TracerProvider,
    // 教案级说明（Why）
    // - `Arc<dyn HandlerSpanTracer>` 需要与 Provider 同生命周期，以确保 Handler Span 创建时始终可用。
    // - 当前仅用于维持引用，不直接读取；未来若支持卸载可在此结构上扩展 Drop 逻辑。
    #[allow(dead_code)]
    handler_tracer: Arc<dyn HandlerSpanTracer>,
}

/// 零配置安装入口：构建 OpenTelemetry Provider、注册 tracing 层并接管 Handler Span 追踪。
///
/// # 教案式说明
/// - **意图（Why）**：提供“一键式”体验，开发者只需调用一次 `install`，即可获得 Trace/Span 自动注入、`tracing`
///   日志桥接与 Handler 自动建 Span 的完整链路。
/// - **逻辑（How）**：
///   1. 防御性检查避免重复安装或外部提前设置的 Subscriber；
///   2. 构建 `TracerProvider` 并注册到 `opentelemetry::global`；
///   3. 使用 `tracing-subscriber` 组装 `fmt + EnvFilter + OpenTelemetry` Layer，设置为全局 Subscriber；
///   4. 构造 `OtelHandlerTracer`，通过 `spark-core` 的安装入口注入到 Pipeline；
///   5. 将安装状态写入 `INSTALL_STATE`，确保 Provider 与 Tracer 在进程生命周期内保持有效。
/// - **契约（What）**：多次调用返回 [`Error::AlreadyInstalled`]；调用前若外部已配置 Subscriber，返回
///   [`Error::SubscriberAlreadySet`]；成功后全局可观测性能力立即生效。
pub fn install() -> spark_core::Result<(), Error> {
    if INSTALL_STATE.get().is_some() {
        return Err(Error::AlreadyInstalled);
    }
    if dispatcher::has_been_set() {
        return Err(Error::SubscriberAlreadySet);
    }

    let state = install_impl()?;
    INSTALL_STATE
        .set(state)
        .map_err(|_| Error::AlreadyInstalled)
        .map(|_| ())
}

fn install_impl() -> spark_core::Result<InstallState, Error> {
    let tracer_provider = build_tracer_provider();
    global::set_tracer_provider(tracer_provider.clone());

    let tracer = tracer_provider.versioned_tracer(
        "spark.pipeline",
        Some(env!("CARGO_PKG_VERSION")),
        Some(Cow::Borrowed(env!("CARGO_PKG_NAME"))),
        None,
    );

    let subscriber = tracing_subscriber::registry()
        .with(build_env_filter())
        .with(tracing_subscriber::fmt::layer())
        .with(tracing_opentelemetry::layer().with_tracer(tracer.clone()));
    tracing::subscriber::set_global_default(subscriber).map_err(Error::SetGlobalSubscriber)?;

    let handler_tracer = make_handler_tracer(tracer);
    install_handler_tracer(handler_tracer.clone()).map_err(|err| match err {
        HandlerTracerError::AlreadyInstalled => Error::HandlerTracerInstalled,
    })?;

    Ok(InstallState {
        provider: tracer_provider,
        handler_tracer,
    })
}

fn build_env_filter() -> EnvFilter {
    EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"))
}

fn build_tracer_provider() -> TracerProvider {
    #[allow(unused_mut)]
    let mut builder = TracerProvider::builder().with_config(
        sdktrace::config()
            .with_sampler(sdktrace::Sampler::AlwaysOn)
            .with_resource(Resource::default()),
    );

    #[cfg(feature = "test-util")]
    {
        let exporter = fetch_in_memory_exporter();
        builder = builder.with_simple_exporter(exporter.clone());
    }

    builder.build()
}

#[cfg(feature = "test-util")]
fn fetch_in_memory_exporter() -> InMemorySpanExporter {
    static EXPORTER: OnceLock<InMemorySpanExporter> = OnceLock::new();
    EXPORTER.get_or_init(InMemorySpanExporter::default).clone()
}

#[cfg(feature = "test-util")]
mod test_support {
    use std::sync::{Arc, Mutex};

    use futures_util::future::BoxFuture;
    use opentelemetry::trace::{TraceError, TraceResult};
    use opentelemetry_sdk::export::trace::{ExportResult, SpanData, SpanExporter};

    /// InMemorySpanExporter 的教学级封装，复刻官方 testing 实现以规避 `rt-async-std` 特性强制
    /// 引入过时运行时的依赖。
    ///
    /// # 教案式说明
    /// - **意图（Why）**：仅保留“收集已完成 Span 供断言”这一测试能力，避免 `opentelemetry-sdk/testing`
    ///   间接启用 `async-std`（已被 RustSec 判定为弃用）。
    /// - **架构定位（Where）**：模块私有于 `spark-otel`，仅在 `test-util` 特性开启时构建，供
    ///   `fetch_in_memory_exporter` 提供稳定出口。
    /// - **实现逻辑（How）**：
    ///   1. 通过 `Arc<Mutex<Vec<SpanData>>>` 保存导出结果，确保跨线程共享与可变访问；
    ///   2. `export` 将批量 Span 追加至内部缓冲，并以 `BoxFuture` 立即返回；
    ///   3. `get_finished_spans`/`reset` 分别用于断言与清理，锁失败时转换为 `TraceError`。
    /// - **契约（What）**：
    ///   - `get_finished_spans`：返回当前缓冲快照；锁被毒化时返回错误；
    ///   - `reset`：清空缓存；
    ///   - `SpanExporter::export`：接收一批已完成 Span，追加成功即返回 `Ok(())`。
    /// - **权衡（Trade-offs）**：
    ///   - 牺牲官方实现附带的 builder 与统计字段，以换取零额外依赖；
    ///   - 使用 `Mutex` 而非无锁结构，因测试场景规模有限、简单性优先。
    #[derive(Clone, Debug, Default)]
    pub struct InMemorySpanExporter {
        spans: Arc<Mutex<Vec<SpanData>>>,
    }

    impl InMemorySpanExporter {
        /// 返回当前已收集的 Span 数据副本，供断言使用。
        ///
        /// # 教案式说明
        /// - **前置条件**：调用方仅在测试线程中使用，避免与生产导出流程混用；
        /// - **后置条件**：若成功，返回时内部缓冲保持不变；失败时原样保留原始数据。
        pub fn get_finished_spans(&self) -> TraceResult<Vec<SpanData>> {
            self.spans
                .lock()
                .map(|guard| guard.iter().cloned().collect())
                .map_err(TraceError::from)
        }

        /// 清空内部缓冲，确保多轮测试之间互不干扰。
        ///
        /// # 教案式说明
        /// - **副作用**：仅在持有锁成功时清除，锁毒化时静默忽略以最大化测试延续性。
        pub fn reset(&self) {
            if let Ok(mut guard) = self.spans.lock() {
                guard.clear();
            }
        }
    }

    impl SpanExporter for InMemorySpanExporter {
        /// 将批量完成的 Span 追加到内存缓冲。
        ///
        /// # 教案式说明
        /// - **实现细节（How）**：尝试获取互斥锁并将批次逐条追加；若锁获取失败，返回 `TraceError`，
        ///   让调用方可见具体失败原因。
        /// - **并发契约（What）**：遵循 OpenTelemetry“单线程调用同一 exporter”的约定，仍以锁
        ///   保护数据以抵御测试误用。
        fn export(&mut self, mut batch: Vec<SpanData>) -> BoxFuture<'static, ExportResult> {
            let result = self
                .spans
                .lock()
                .map(|mut guard| guard.append(&mut batch))
                .map_err(TraceError::from);

            Box::pin(async move { result })
        }

        /// 关闭导出器时清理残留数据，保证下一次安装得到干净状态。
        fn shutdown(&mut self) {
            self.reset();
        }
    }
}

#[cfg(feature = "test-util")]
/// 测试辅助工具：提供访问导出 Span 的接口。
pub mod testing {
    use super::*;

    /// 强制刷新 Provider，确保导出的 Span 已进入导出器。
    pub fn force_flush() {
        if let Some(state) = INSTALL_STATE.get() {
            for result in state.provider.force_flush() {
                let _ = result;
            }
        }
    }

    /// 获取自安装以来导出的全部 Span。
    pub fn finished_spans() -> Vec<SpanData> {
        fetch_in_memory_exporter()
            .get_finished_spans()
            .unwrap_or_default()
    }

    /// 清空 In-Memory Exporter 中的 Span，便于隔离测试案例。
    pub fn reset() {
        fetch_in_memory_exporter().reset();
    }
}
