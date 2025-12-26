use super::{attributes::AttributeSet, trace::TraceContext};
use crate::{Error, sealed::Sealed};
use alloc::borrow::Cow;

/// 日志级别枚举，参考 OpenTelemetry `SeverityNumber` 与 `tracing` crate 的交集。
///
/// # 设计背景（Why）
/// - 统一跨语言框架的日志语义，兼容 Info/Warn/Error，同时保留 Trace/Debug/Fatal 以满足研究实验。
///
/// # 契约说明（What）
/// - `Info` 表示业务常规事件，`Warn` 表示潜在风险，`Error` 表示故障，`Fatal` 代表不可恢复错误。
/// - **前置条件**：使用者需根据业务重要性正确选择级别，以便告警系统匹配阈值。
/// - **后置条件**：日志导出器可依据级别映射到目标系统（如 syslog、OpenTelemetry LogData）。
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum LogSeverity {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
    Fatal,
}

/// 结构化日志字段集合。
///
/// # 设计背景（Why）
/// - 采用与指标属性一致的切片抽象 [`AttributeSet`]，鼓励复用同一键值管理策略。
/// - 兼容 Datadog/Honeycomb 的日志字段约定，便于对接生态插件。
///
/// # 契约说明（What）
/// - **前置条件**：字段集合中的键值需符合低基数原则，避免日志存储爆炸。
/// - **后置条件**：实现方应保证字段在输出端以结构化形式呈现（JSON、Protobuf 等）。
///
/// # 风险提示（Trade-offs）
/// - 字段值引用同一生命周期，若需延迟输出需提前克隆或使用拥有所有权的集合。
pub type LogField<'a> = AttributeSet<'a>;

/// 单条结构化日志记录。
///
/// # 设计背景（Why）
/// - 借鉴 OpenTelemetry Log Data Model，将消息、级别、目标、上下文与结构化字段打包成单一结构。
/// - 支持可选的 `TraceContext` 关联，以便在日志-追踪联动时快速定位调用链。
///
/// # 逻辑解析（How）
/// - `message` 使用 `Cow<'a, str>`，既支持静态字符串也支持动态构建的消息。
/// - `target` 表示日志分类，可对齐 `tracing` 的 Target 或 Syslog Facility。
/// - `attributes` 采用 [`LogField`]，支持结构化字段。
/// - `error` 允许携带实现 [`Error`] 的对象，用于堆栈或根因追溯。
///
/// # 契约说明（What）
/// - **前置条件**：`attributes` 必须在调用后仍然有效；若使用临时 `Vec`，需确保生命周期覆盖调用过程。
/// - **后置条件**：记录提交给 [`Logger`] 后，应视为不可变对象，禁止修改引用数据。
///
/// # 风险提示（Trade-offs）
/// - `error` 字段以引用形式传递，避免克隆错误链；实现方需在导出前处理生命周期。
#[derive(Debug)]
pub struct LogRecord<'a> {
    pub message: Cow<'a, str>,
    pub severity: LogSeverity,
    pub target: Option<Cow<'a, str>>,
    pub trace_context: Option<&'a TraceContext>,
    pub error: Option<&'a dyn Error>,
    pub attributes: LogField<'a>,
}

impl<'a> LogRecord<'a> {
    /// 构建新的日志记录。
    ///
    /// # 契约说明
    /// - **输入参数**：详见结构体字段解释。
    /// - **前置条件**：`attributes` 切片需在 `Logger::log` 返回前保持有效。
    /// - **后置条件**：返回的记录仅包含引用，不进行任何复制。
    pub fn new(
        message: impl Into<Cow<'a, str>>,
        severity: LogSeverity,
        target: Option<impl Into<Cow<'a, str>>>,
        trace_context: Option<&'a TraceContext>,
        error: Option<&'a dyn Error>,
        attributes: LogField<'a>,
    ) -> Self {
        Self {
            message: message.into(),
            severity,
            target: target.map(Into::into),
            trace_context,
            error,
            attributes,
        }
    }
}

/// 日志接口的核心契约。
///
/// # 设计背景（Why）
/// - 统一框架对宿主系统日志实现的依赖，允许对接 `tracing`、OpenTelemetry Logger 或自研后端。
/// - 兼顾生产与研究需求，既支持结构化字段也支持轻量的消息日志。
///
/// # 逻辑解析（How）
/// - `log` 为唯一必需方法，实现结构化记录的提交。
/// - 额外提供 `info`/`warn`/`error` 等便捷方法，内部构造 [`LogRecord`] 再调用 `log`，确保所有路径共享相同逻辑。
///
/// # 契约说明（What）
/// - **前置条件**：调用方需在 `attributes` 中提供低基数键值；`error` 若存在必须满足 [`Error`]。
/// - **后置条件**：实现应尽量保证非阻塞，必要时可将日志异步写入后台线程。
///
/// # 风险提示（Trade-offs）
/// - 在高吞吐场景中，请谨慎使用拥有所有权的复制以免重复分配；建议通过批处理或环形缓冲优化。
pub trait Logger: Send + Sync + 'static + Sealed {
    /// 提交结构化日志。
    fn log(&self, record: &LogRecord<'_>);

    /// 输出 TRACE 日志（无额外字段）。
    fn trace(&self, message: &str, trace: Option<&TraceContext>) {
        self.trace_with_fields(message, &[], trace);
    }

    /// 输出带字段的 TRACE 日志。
    fn trace_with_fields(
        &self,
        message: &str,
        attributes: LogField<'_>,
        trace: Option<&TraceContext>,
    ) {
        let record = LogRecord::new(
            message,
            LogSeverity::Trace,
            None::<Cow<'_, str>>,
            trace,
            None,
            attributes,
        );
        self.log(&record);
    }

    /// 输出 DEBUG 日志（无额外字段）。
    fn debug(&self, message: &str, trace: Option<&TraceContext>) {
        self.debug_with_fields(message, &[], trace);
    }

    /// 输出带字段的 DEBUG 日志。
    fn debug_with_fields(
        &self,
        message: &str,
        attributes: LogField<'_>,
        trace: Option<&TraceContext>,
    ) {
        let record = LogRecord::new(
            message,
            LogSeverity::Debug,
            None::<Cow<'_, str>>,
            trace,
            None,
            attributes,
        );
        self.log(&record);
    }

    /// 输出 INFO 日志（无额外字段）。
    fn info(&self, message: &str, trace: Option<&TraceContext>) {
        self.info_with_fields(message, &[], trace);
    }

    /// 输出带字段的 INFO 日志。
    fn info_with_fields(
        &self,
        message: &str,
        attributes: LogField<'_>,
        trace: Option<&TraceContext>,
    ) {
        let record = LogRecord::new(
            message,
            LogSeverity::Info,
            None::<Cow<'_, str>>,
            trace,
            None,
            attributes,
        );
        self.log(&record);
    }

    /// 输出 WARN 日志（无额外字段）。
    fn warn(&self, message: &str, trace: Option<&TraceContext>) {
        self.warn_with_fields(message, &[], trace);
    }

    /// 输出带字段的 WARN 日志。
    fn warn_with_fields(
        &self,
        message: &str,
        attributes: LogField<'_>,
        trace: Option<&TraceContext>,
    ) {
        let record = LogRecord::new(
            message,
            LogSeverity::Warn,
            None::<Cow<'_, str>>,
            trace,
            None,
            attributes,
        );
        self.log(&record);
    }

    /// 输出 ERROR 日志（无额外字段）。
    fn error(&self, message: &str, error: Option<&dyn Error>, trace: Option<&TraceContext>) {
        self.error_with_fields(message, error, &[], trace);
    }

    /// 输出带字段的 ERROR 日志。
    fn error_with_fields(
        &self,
        message: &str,
        error: Option<&dyn Error>,
        attributes: LogField<'_>,
        trace: Option<&TraceContext>,
    ) {
        let record = LogRecord::new(
            message,
            LogSeverity::Error,
            None::<Cow<'_, str>>,
            trace,
            error,
            attributes,
        );
        self.log(&record);
    }

    /// 输出 FATAL 日志（无额外字段）。
    fn fatal(&self, message: &str, error: Option<&dyn Error>, trace: Option<&TraceContext>) {
        self.fatal_with_fields(message, error, &[], trace);
    }

    /// 输出带字段的 FATAL 日志。
    fn fatal_with_fields(
        &self,
        message: &str,
        error: Option<&dyn Error>,
        attributes: LogField<'_>,
        trace: Option<&TraceContext>,
    ) {
        let record = LogRecord::new(
            message,
            LogSeverity::Fatal,
            None::<Cow<'_, str>>,
            trace,
            error,
            attributes,
        );
        self.log(&record);
    }
}
