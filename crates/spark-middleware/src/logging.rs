use alloc::{borrow::Cow, boxed::Box, sync::Arc, vec::Vec};

use spark_core::{
    CoreError, Error,
    buffer::PipelineMessage,
    contract::{CloseReason, Deadline},
    observability::{
        attributes::KeyValue,
        logging::{LogRecord, LogSeverity, Logger},
    },
    pipeline::{
        ChainBuilder, Channel, InitializerDescriptor, PipelineInitializer,
        handler::{InboundHandler, OutboundHandler},
    },
    runtime::CoreServices,
};

/// 日志字段键常量，保持跨 Handler 的命名一致性。
const ATTR_DIRECTION: &str = "spark.middleware.logging.direction";
const ATTR_MESSAGE_KIND: &str = "spark.middleware.logging.message_kind";
const ATTR_MESSAGE_BYTES: &str = "spark.middleware.logging.message_bytes";
const ATTR_USER_KIND: &str = "spark.middleware.logging.user_kind";
const ATTR_EVENT: &str = "spark.middleware.logging.event";

/// 中间件配置，指导日志 Handler 如何输出记录。
///
/// # 教案式说明
/// - **意图（Why）**：在不同业务场景下，日志的目标、级别与注册标签各不相同。配置结构将这些
///   差异外部化，使中间件本身保持无状态并可复用。
/// - **结构（How）**：携带 `InitializerDescriptor`、注册标签 `label`、日志目标 `target` 与默认
///   日志级别 `severity`。`label` 用于在 `ChainBuilder` 中识别 Handler；`target` 对应
///   `LogRecord::target`，方便在观测平台上筛选；`severity` 控制普通读写事件的输出级别。
/// - **契约（What）**：
///   - `descriptor`：遵循 `vendor.component` 命名惯例，描述组件用途；
///   - `label`：低基数字符串，在同一 Pipeline 内唯一；
///   - `target`：面向日志后端的分类标签，建议与组件命名保持一致；
///   - `severity`：普通事件的默认日志级别，可按需调整为 `Debug` 或 `Info`。
/// - **风险提示（Trade-offs）**：高频链路若使用 `Info` 级别可能造成日志风暴，请结合
///   `severity` 控制输出或在上层设置采样。
#[derive(Clone, Debug)]
pub struct LoggingMiddlewareConfig {
    pub descriptor: InitializerDescriptor,
    pub label: Cow<'static, str>,
    pub target: Cow<'static, str>,
    pub severity: LogSeverity,
}

impl Default for LoggingMiddlewareConfig {
    fn default() -> Self {
        Self {
            descriptor: InitializerDescriptor::new(
                "spark.middleware.logging",
                "observability",
                "记录 Pipeline 入站/出站消息的结构化日志",
            ),
            label: Cow::Borrowed("logging"),
            target: Cow::Borrowed("spark.middleware.logging"),
            severity: LogSeverity::Info,
        }
    }
}

/// LoggingMiddleware 将日志输出逻辑装配到 Pipeline。
///
/// # 教案式说明
/// - **意图（Why）**：统一在 Handler 链中记录读写事件，避免各业务手写重复的日志代码。
/// - **结构（How）**：构造阶段持有 [`LoggingMiddlewareConfig`]，在 `configure` 中克隆运行时
///   `Logger`，生成同时实现 [`spark_core::pipeline::handler::InboundHandler`] 与
///   [`spark_core::pipeline::handler::OutboundHandler`] 的复合处理器 `LoggingHandler` 并注册到链路。
/// - **契约（What）**：实现 [`Middleware`]，`descriptor` 直接返回配置中的描述；`configure`
///   必须幂等，重复调用不会重复注册 Handler。
/// - **风险提示（Trade-offs）**：日志 Handler 自身不做采样，若接入高 QPS 通道，需在配置层调低
///   `severity` 或结合上游采样器使用。
#[derive(Clone, Debug)]
pub struct LoggingMiddleware {
    config: LoggingMiddlewareConfig,
}

impl LoggingMiddleware {
    /// 基于给定配置构造中间件。
    pub fn new(config: LoggingMiddlewareConfig) -> Self {
        Self { config }
    }
}

impl Default for LoggingMiddleware {
    fn default() -> Self {
        Self::new(LoggingMiddlewareConfig::default())
    }
}

impl PipelineInitializer for LoggingMiddleware {
    fn descriptor(&self) -> InitializerDescriptor {
        self.config.descriptor.clone()
    }

    fn configure(
        &self,
        chain: &mut dyn ChainBuilder,
        _channel: &dyn Channel,
        services: &CoreServices,
    ) -> spark_core::Result<(), CoreError> {
        let handler = LoggingHandler::new(
            self.config.descriptor.clone(),
            self.config.target.clone(),
            self.config.severity,
            Arc::clone(&services.logger),
        );
        chain.register_inbound(self.config.label.as_ref(), Box::new(handler.clone()));
        chain.register_outbound(self.config.label.as_ref(), Box::new(handler));
        Ok(())
    }
}

#[derive(Clone)]
struct LoggingHandler {
    inner: Arc<LoggingHandlerInner>,
}

struct LoggingHandlerInner {
    descriptor: InitializerDescriptor,
    target: Cow<'static, str>,
    severity: LogSeverity,
    logger: Arc<dyn Logger>,
}

impl LoggingHandler {
    fn new(
        descriptor: InitializerDescriptor,
        target: Cow<'static, str>,
        severity: LogSeverity,
        logger: Arc<dyn Logger>,
    ) -> Self {
        Self {
            inner: Arc::new(LoggingHandlerInner {
                descriptor,
                target,
                severity,
                logger,
            }),
        }
    }

    /// 组装结构化日志字段。
    fn build_fields(
        &self,
        direction: &'static str,
        msg: &PipelineMessage,
        event: &'static str,
    ) -> Vec<KeyValue<'static>> {
        let mut fields = Vec::with_capacity(4);
        fields.push(KeyValue::new(ATTR_DIRECTION, direction));
        fields.push(KeyValue::new(ATTR_EVENT, event));
        let user_kind = msg.user_kind();
        match msg {
            PipelineMessage::Buffer(buf) => {
                fields.push(KeyValue::new(ATTR_MESSAGE_KIND, "buffer"));
                fields.push(KeyValue::new(ATTR_MESSAGE_BYTES, buf.remaining() as i64));
            }
            PipelineMessage::User(_) => {
                fields.push(KeyValue::new(ATTR_MESSAGE_KIND, "user"));
                if let Some(kind) = user_kind {
                    fields.push(KeyValue::new(ATTR_USER_KIND, kind));
                }
                // UserMessage 无法直接获知大小，此处留空，避免误导观测数据。
            }
            &_ => {
                fields.push(KeyValue::new(ATTR_MESSAGE_KIND, "unknown"));
            }
        }
        fields
    }

    fn log_event(
        &self,
        ctx: &dyn spark_core::pipeline::context::Context,
        severity: LogSeverity,
        message: &str,
        attributes: &[KeyValue<'_>],
        error: Option<&CoreError>,
    ) {
        let error = error.map(|err| err as &dyn Error);
        let record = LogRecord::new(
            message,
            severity,
            Some(self.inner.target.as_ref()),
            Some(ctx.trace_context()),
            error,
            attributes,
        );
        self.inner.logger.log(&record);
    }
}

impl InboundHandler for LoggingHandler {
    fn describe(&self) -> InitializerDescriptor {
        self.inner.descriptor.clone()
    }

    fn on_channel_active(&self, ctx: &dyn spark_core::pipeline::context::Context) {
        let fields = [KeyValue::new(ATTR_EVENT, "channel_active")];
        self.log_event(
            ctx,
            LogSeverity::Info,
            "pipeline channel active",
            &fields,
            None,
        );
    }

    fn on_read(&self, ctx: &dyn spark_core::pipeline::context::Context, msg: PipelineMessage) {
        let fields = self.build_fields("inbound", &msg, "on_read");
        self.log_event(
            ctx,
            self.inner.severity,
            "pipeline inbound message received",
            &fields,
            None,
        );
        ctx.forward_read(msg);
    }

    fn on_read_complete(&self, ctx: &dyn spark_core::pipeline::context::Context) {
        let fields = [KeyValue::new(ATTR_EVENT, "read_complete")];
        self.log_event(
            ctx,
            self.inner.severity,
            "pipeline inbound read complete",
            &fields,
            None,
        );
    }

    fn on_writability_changed(
        &self,
        ctx: &dyn spark_core::pipeline::context::Context,
        is_writable: bool,
    ) {
        let fields = [
            KeyValue::new(ATTR_EVENT, "writability_changed"),
            KeyValue::new("spark.middleware.logging.is_writable", is_writable),
        ];
        self.log_event(
            ctx,
            LogSeverity::Debug,
            "pipeline channel writability changed",
            &fields,
            None,
        );
    }

    fn on_user_event(
        &self,
        ctx: &dyn spark_core::pipeline::context::Context,
        event: spark_core::observability::CoreUserEvent,
    ) {
        let kind = event.application_event_kind().unwrap_or(match &event {
            spark_core::observability::CoreUserEvent::TlsEstablished(_) => "tls_established",
            spark_core::observability::CoreUserEvent::IdleTimeout(_) => "idle_timeout",
            spark_core::observability::CoreUserEvent::RateLimited(_) => "rate_limited",
            spark_core::observability::CoreUserEvent::ConfigChanged { .. } => "config_changed",
            spark_core::observability::CoreUserEvent::ApplicationSpecific(_) => {
                "application_specific"
            }
            &_ => "unknown",
        });
        let fields = [
            KeyValue::new(ATTR_EVENT, "user_event"),
            KeyValue::new("spark.middleware.logging.user_event", kind),
        ];
        self.log_event(
            ctx,
            LogSeverity::Info,
            "pipeline user event propagated",
            &fields,
            None,
        );
    }

    fn on_exception_caught(
        &self,
        ctx: &dyn spark_core::pipeline::context::Context,
        error: CoreError,
    ) {
        let fields = [
            KeyValue::new(ATTR_EVENT, "exception_caught"),
            KeyValue::new("spark.middleware.logging.error_code", error.code()),
        ];
        self.log_event(
            ctx,
            LogSeverity::Error,
            "pipeline inbound exception",
            &fields,
            Some(&error),
        );
    }

    fn on_channel_inactive(&self, ctx: &dyn spark_core::pipeline::context::Context) {
        let fields = [KeyValue::new(ATTR_EVENT, "channel_inactive")];
        self.log_event(
            ctx,
            LogSeverity::Info,
            "pipeline channel inactive",
            &fields,
            None,
        );
    }
}

impl OutboundHandler for LoggingHandler {
    fn describe(&self) -> InitializerDescriptor {
        self.inner.descriptor.clone()
    }

    fn on_write(
        &self,
        ctx: &dyn spark_core::pipeline::context::Context,
        msg: PipelineMessage,
    ) -> spark_core::Result<spark_core::pipeline::channel::WriteSignal, CoreError> {
        let fields = self.build_fields("outbound", &msg, "on_write");
        self.log_event(
            ctx,
            self.inner.severity,
            "pipeline outbound message write",
            &fields,
            None,
        );
        ctx.write(msg)
    }

    fn on_flush(
        &self,
        ctx: &dyn spark_core::pipeline::context::Context,
    ) -> spark_core::Result<(), CoreError> {
        let fields = [KeyValue::new(ATTR_EVENT, "flush")];
        self.log_event(
            ctx,
            LogSeverity::Debug,
            "pipeline flush invoked",
            &fields,
            None,
        );
        ctx.flush();
        Ok(())
    }

    fn on_close_graceful(
        &self,
        ctx: &dyn spark_core::pipeline::context::Context,
        deadline: Option<core::time::Duration>,
    ) -> spark_core::Result<(), CoreError> {
        let mut fields = Vec::with_capacity(2);
        fields.push(KeyValue::new(ATTR_EVENT, "close_graceful"));
        if let Some(duration) = deadline {
            let millis = duration.as_millis();
            let millis = if millis > i64::MAX as u128 {
                i64::MAX
            } else {
                millis as i64
            };
            fields.push(KeyValue::new(
                "spark.middleware.logging.close_deadline_ms",
                millis,
            ));
        }
        self.log_event(
            ctx,
            LogSeverity::Info,
            "pipeline graceful close requested",
            &fields,
            None,
        );
        let deadline = deadline.map(|timeout| {
            let now = ctx.timer().now();
            Deadline::with_timeout(now, timeout)
        });
        ctx.close_graceful(
            CloseReason::new(
                "spark.middleware.logging.close",
                "logging middleware propagated graceful shutdown",
            ),
            deadline,
        );
        Ok(())
    }
}
