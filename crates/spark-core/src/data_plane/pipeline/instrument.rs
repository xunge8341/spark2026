use alloc::{boxed::Box, sync::Arc};
use core::fmt;

use crate::observability::{LogRecord, Logger, TraceContext};

use super::{controller::HandlerDirection, initializer::InitializerDescriptor};

use spin::Mutex;

/// Handler Span 注入参数，描述当前被调度的 Handler 元数据。
///
/// # 教案式说明
/// - **意图（Why）**：在桥接 OpenTelemetry 或其他追踪后端时，需要一份结构化的 Handler 描述，以便生成具备可读性的 Span 名称与属性。
/// - **逻辑（How）**：聚合 Handler 的方向（入站/出站）、注册时的标签以及 [`InitializerDescriptor`] 中声明的附加元信息。
/// - **契约（What）**：所有字段均为只读引用，调用方不得在 Span 生命周期内释放底层字符串；方向值来自 [`HandlerDirection`] 枚举。
#[derive(Clone, Copy, Debug)]
pub struct HandlerSpanParams<'a> {
    pub direction: HandlerDirection,
    pub label: &'a str,
    pub descriptor: &'a InitializerDescriptor,
}

impl<'a> HandlerSpanParams<'a> {
    /// 构造参数结构体，简化调用站点的样板代码。
    #[inline]
    pub fn new(
        direction: HandlerDirection,
        label: &'a str,
        descriptor: &'a InitializerDescriptor,
    ) -> Self {
        Self {
            direction,
            label,
            descriptor,
        }
    }
}

/// Handler Span 追踪实现需要返回的 Guard 对象，负责在 Span 生命周期结束时执行收尾逻辑。
///
/// # 教案式说明
/// - **意图（Why）**：不同追踪后端（如 OpenTelemetry、Zipkin、自研方案）在 Span 结束时可能需要执行额外步骤（提交事件、回收资源）。
/// - **逻辑（How）**：通过对象安全 Trait 暴露一个 `finish` 方法，使实现方可以在 [`Drop`] 钩子中调用并完成 Span 终止。
/// - **契约（What）**：实现必须是 `Send + Sync + 'static`，以便在多线程 Pipeline 中安全传递；`finish` 必须幂等或在内部做好防重入保护。
pub trait HandlerSpanGuard: Send + Sync + 'static {
    /// 结束 Span，并执行必要的清理或上报逻辑。
    fn finish(self: Box<Self>);
}

/// Handler Span 的统一封装，既持有派生后的 [`TraceContext`]，也负责在作用域结束时回收追踪资源。
///
/// # 教案式说明
/// - **意图（Why）**：Pipeline 调度器需要在调用 Handler 前后自动开启/关闭 Span，同时向业务侧暴露更新后的 `TraceContext` 以实现日志关联。
/// - **逻辑（How）**：在构造时记录子 Span 的上下文，并可选地持有一个 [`HandlerSpanGuard`]；当结构被 Drop 时调用 Guard 的 `finish` 完成收尾。
/// - **契约（What）**：`trace_context()` 提供给外部用于日志/指标注入；`Drop` 实现保证 Guard 至多执行一次。
pub struct HandlerSpan {
    trace_context: TraceContext,
    guard: Option<Box<dyn HandlerSpanGuard>>,
}

impl HandlerSpan {
    fn new(trace_context: TraceContext, guard: Option<Box<dyn HandlerSpanGuard>>) -> Self {
        Self {
            trace_context,
            guard,
        }
    }

    /// 获取子 Span 的 `TraceContext`，供上下文与日志注入使用。
    pub fn trace_context(&self) -> &TraceContext {
        &self.trace_context
    }

    /// 供外部追踪实现根据自定义 Guard 构造 [`HandlerSpan`]。
    pub fn from_parts(
        trace_context: TraceContext,
        guard: Option<Box<dyn HandlerSpanGuard>>,
    ) -> Self {
        Self::new(trace_context, guard)
    }
}

impl Drop for HandlerSpan {
    fn drop(&mut self) {
        if let Some(guard) = self.guard.take() {
            guard.finish();
        }
    }
}

impl fmt::Debug for HandlerSpan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HandlerSpan")
            .field("trace_context", &self.trace_context)
            .finish()
    }
}

/// 追踪实现注册错误。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HandlerTracerError {
    /// 已存在注册的 Span 追踪实现。
    AlreadyInstalled,
}

impl fmt::Display for HandlerTracerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            HandlerTracerError::AlreadyInstalled => f.write_str("pipeline handler tracer 已安装"),
        }
    }
}

/// Pipeline Handler Span 追踪器约束。
///
/// # 教案式说明
/// - **意图（Why）**：为 `spark-otel` 等插件提供统一注册入口，使其能够在 Handler 回调前后自动创建、关闭 Span。
/// - **逻辑（How）**：实现者根据 [`HandlerSpanParams`] 与父级 [`TraceContext`] 构造新的子 Span，并返回 [`HandlerSpan`] 以维持生命周期。
/// - **契约（What）**：实现必须线程安全；返回的 [`HandlerSpan`] 必须包含非空的 `TraceContext`，以保证日志关联可用。
pub trait HandlerSpanTracer: Send + Sync + 'static {
    /// 创建新的 Handler Span。
    fn start_span(&self, params: &HandlerSpanParams<'_>, parent: &TraceContext) -> HandlerSpan;
}

static HANDLER_TRACER: Mutex<Option<Arc<dyn HandlerSpanTracer>>> = Mutex::new(None);

/// 安装自定义 Handler Span 追踪实现。
///
/// # 教案式说明
/// - **意图（Why）**：允许在运行时为 Pipeline 安装一次性追踪后端，消除各 Handler 手工管理 Span 的负担。
/// - **逻辑（How）**：通过自旋锁确保注册操作原子化，当已存在实现时返回 [`HandlerTracerError::AlreadyInstalled`]。
/// - **契约（What）**：只允许安装一次；若需要替换实现，必须先调用 [`uninstall_handler_tracer`]。
pub fn install_handler_tracer(
    tracer: Arc<dyn HandlerSpanTracer>,
) -> crate::Result<(), HandlerTracerError> {
    let mut guard = HANDLER_TRACER.lock();
    if guard.is_some() {
        return Err(HandlerTracerError::AlreadyInstalled);
    }
    *guard = Some(tracer);
    Ok(())
}

/// 卸载当前的 Handler Span 追踪实现，主要用于测试或运行时热更新场景。
pub fn uninstall_handler_tracer() {
    let mut guard = HANDLER_TRACER.lock();
    *guard = None;
}

fn active_tracer() -> Option<Arc<dyn HandlerSpanTracer>> {
    HANDLER_TRACER.lock().as_ref().map(Arc::clone)
}

/// 为入站 Handler 创建 Span，若未安装追踪实现则回退为本地生成的子上下文。
pub(crate) fn start_inbound_span(
    label: &str,
    descriptor: &InitializerDescriptor,
    parent: &TraceContext,
) -> HandlerSpan {
    start_handler_span(HandlerDirection::Inbound, label, descriptor, parent)
}

/// 为出站 Handler 创建 Span，若未安装追踪实现则回退为本地生成的子上下文。
//
// 教案级说明（Why）
// - 当前版本的 Pipeline 仅在入站路径调用该函数，但出站链路即将接入（T32 需求）。
// - 保留实现可确保未来在控制器中接线时仅需解除禁用即可复用既有逻辑。
//
// 教案级说明（Trade-offs）
// - 以 `#[allow(dead_code)]` 暂时抑制编译告警，换取文档与代码的同步；
// - 若长期未启用出站路径，应在里程碑评审时重新评估是否删除该接口。
#[allow(dead_code)]
pub(crate) fn start_outbound_span(
    label: &str,
    descriptor: &InitializerDescriptor,
    parent: &TraceContext,
) -> HandlerSpan {
    start_handler_span(HandlerDirection::Outbound, label, descriptor, parent)
}

fn start_handler_span(
    direction: HandlerDirection,
    label: &str,
    descriptor: &InitializerDescriptor,
    parent: &TraceContext,
) -> HandlerSpan {
    if let Some(tracer) = active_tracer() {
        let params = HandlerSpanParams::new(direction, label, descriptor);
        tracer.start_span(&params, parent)
    } else {
        fallback_span(parent)
    }
}

fn fallback_span(parent: &TraceContext) -> HandlerSpan {
    let span_id = TraceContext::generate_span_id();
    let trace_context = parent.child_context(span_id);
    HandlerSpan::new(trace_context, None)
}

/// 注入 Trace/Span 信息的日志包装器，实现“零配置”日志追踪关联。
///
/// # 教案式说明
/// - **意图（Why）**：在 Handler 内部调用日志接口时自动补齐 `trace_id`/`span_id`，避免业务代码显式传参。
/// - **逻辑（How）**：对外暴露与原始 [`Logger`] 相同的接口，在 `log` 调用点将缺失的 `trace_context` 替换为当前 Span 上下文。
/// - **契约（What）**：内部持有原始 `Logger` 的 `Arc`，确保线程安全；派生的 `TraceContext` 必须对应当前 Handler 的 Span。
pub struct InstrumentedLogger {
    inner: Arc<dyn Logger>,
    trace_context: TraceContext,
}

impl InstrumentedLogger {
    /// 构造带自动注入能力的日志器。
    pub fn new(inner: Arc<dyn Logger>, trace_context: TraceContext) -> Self {
        Self {
            inner,
            trace_context,
        }
    }

    /// 返回当前注入使用的 `TraceContext`，便于诊断或测试断言。
    pub fn trace_context(&self) -> &TraceContext {
        &self.trace_context
    }
}

impl Logger for InstrumentedLogger {
    fn log(&self, record: &LogRecord<'_>) {
        let trace = record.trace_context.unwrap_or(&self.trace_context);
        let patched = LogRecord::new(
            record.message.as_ref(),
            record.severity,
            record.target.as_ref().map(|target| target.as_ref()),
            Some(trace),
            record.error,
            record.attributes,
        );
        self.inner.log(&patched);
    }
}

impl fmt::Debug for InstrumentedLogger {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("InstrumentedLogger")
            .field("trace_context", &self.trace_context)
            .finish()
    }
}
