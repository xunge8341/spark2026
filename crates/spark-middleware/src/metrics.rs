use alloc::{borrow::Cow, boxed::Box, sync::Arc, vec::Vec};

use spark_core::{
    CoreError,
    buffer::PipelineMessage,
    contract::{CloseReason, Deadline},
    observability::{
        CoreUserEvent,
        attributes::KeyValue,
        metrics::{InstrumentDescriptor, MetricsProvider},
    },
    pipeline::{
        ChainBuilder, Channel, InitializerDescriptor, PipelineInitializer,
        channel::WriteSignal,
        handler::{InboundHandler, OutboundHandler},
    },
    runtime::CoreServices,
};

/// 指标描述符常量，确保指标命名与语义稳定。
const METRIC_READ_TOTAL: InstrumentDescriptor<'static> =
    InstrumentDescriptor::new("spark.middleware.pipeline.read.total")
        .with_description("Pipeline 入站消息总量")
        .with_unit("events");
const METRIC_READ_BYTES: InstrumentDescriptor<'static> =
    InstrumentDescriptor::new("spark.middleware.pipeline.read.bytes")
        .with_description("Pipeline 入站字节总量")
        .with_unit("bytes");
const METRIC_READ_COMPLETE_TOTAL: InstrumentDescriptor<'static> =
    InstrumentDescriptor::new("spark.middleware.pipeline.read_complete.total")
        .with_description("Pipeline 入站批次完成次数")
        .with_unit("events");
const METRIC_WRITE_TOTAL: InstrumentDescriptor<'static> =
    InstrumentDescriptor::new("spark.middleware.pipeline.write.total")
        .with_description("Pipeline 出站写入次数")
        .with_unit("events");
const METRIC_WRITE_BYTES: InstrumentDescriptor<'static> =
    InstrumentDescriptor::new("spark.middleware.pipeline.write.bytes")
        .with_description("Pipeline 出站字节总量")
        .with_unit("bytes");
const METRIC_WRITE_SIGNAL_TOTAL: InstrumentDescriptor<'static> =
    InstrumentDescriptor::new("spark.middleware.pipeline.write_signal.total")
        .with_description("Pipeline 出站写入反馈信号分布")
        .with_unit("events");
const METRIC_WRITE_ERROR_TOTAL: InstrumentDescriptor<'static> =
    InstrumentDescriptor::new("spark.middleware.pipeline.write.errors")
        .with_description("Pipeline 出站写入错误次数")
        .with_unit("events");
const METRIC_EXCEPTION_TOTAL: InstrumentDescriptor<'static> =
    InstrumentDescriptor::new("spark.middleware.pipeline.exceptions")
        .with_description("Pipeline 捕获的异常次数")
        .with_unit("events");
const METRIC_CHANNEL_ACTIVE_TOTAL: InstrumentDescriptor<'static> =
    InstrumentDescriptor::new("spark.middleware.pipeline.channel_active.total")
        .with_description("Pipeline 通道激活次数")
        .with_unit("events");
const METRIC_CHANNEL_INACTIVE_TOTAL: InstrumentDescriptor<'static> =
    InstrumentDescriptor::new("spark.middleware.pipeline.channel_inactive.total")
        .with_description("Pipeline 通道失活次数")
        .with_unit("events");
const METRIC_WRITABILITY_CHANGED_TOTAL: InstrumentDescriptor<'static> =
    InstrumentDescriptor::new("spark.middleware.pipeline.writability_changed.total")
        .with_description("Pipeline 可写性变更次数")
        .with_unit("events");
const METRIC_USER_EVENT_TOTAL: InstrumentDescriptor<'static> =
    InstrumentDescriptor::new("spark.middleware.pipeline.user_event.total")
        .with_description("Pipeline 用户事件广播次数")
        .with_unit("events");
const METRIC_FLUSH_TOTAL: InstrumentDescriptor<'static> =
    InstrumentDescriptor::new("spark.middleware.pipeline.flush.total")
        .with_description("Pipeline flush 调用次数")
        .with_unit("events");
const METRIC_CLOSE_TOTAL: InstrumentDescriptor<'static> =
    InstrumentDescriptor::new("spark.middleware.pipeline.close.total")
        .with_description("Pipeline 优雅关闭请求次数")
        .with_unit("events");

/// 结构化标签键常量。
const ATTR_DIRECTION: &str = "spark.middleware.metrics.direction";
const ATTR_EVENT: &str = "spark.middleware.metrics.event";
const ATTR_MESSAGE_KIND: &str = "spark.middleware.metrics.message_kind";
const ATTR_MESSAGE_BYTES: &str = "spark.middleware.metrics.message_bytes";
const ATTR_USER_KIND: &str = "spark.middleware.metrics.user_kind";
const ATTR_WRITE_SIGNAL: &str = "spark.middleware.metrics.write_signal";
const ATTR_ERROR_CODE: &str = "spark.middleware.metrics.error_code";
const ATTR_USER_EVENT: &str = "spark.middleware.metrics.user_event";
const ATTR_WRITABLE: &str = "spark.middleware.metrics.is_writable";
const ATTR_CLOSE_DEADLINE: &str = "spark.middleware.metrics.close_deadline";

/// 中间件配置：决定注册标签与描述符。
///
/// # 教案式说明
/// - **意图（Why）**：在不同 Pipeline 中复用同一套指标打点时，允许根据部署环境调整
///   描述符与标签，避免硬编码字符串导致命名漂移。
/// - **结构（How）**：记录 [`InitializerDescriptor`] 与链路注册标签，结构体实现 `Clone`
///   以便在多条链路之间共享。
/// - **契约（What）**：
///   - `descriptor`：用于控制面与可观测平台识别组件用途；
///   - `label`：在 `ChainBuilder` 中注册 Handler 时使用，需保持低基数且唯一；
///   - **前置条件**：配置应在装配阶段确定；
///   - **后置条件**：`MetricsMiddleware` 会直接复用这些字段注册 Handler。
/// - **风险提示（Trade-offs）**：若同一链路多次装配该中间件，请确保标签唯一以避免覆盖。
#[derive(Clone, Debug)]
pub struct MetricsMiddlewareConfig {
    pub descriptor: InitializerDescriptor,
    pub label: Cow<'static, str>,
}

impl Default for MetricsMiddlewareConfig {
    fn default() -> Self {
        Self {
            descriptor: InitializerDescriptor::new(
                "spark.middleware.metrics",
                "observability",
                "记录 Pipeline 关键事件的指标",
            ),
            label: Cow::Borrowed("metrics"),
        }
    }
}

/// MetricsMiddleware 将指标打点逻辑注入 Pipeline。
///
/// # 教案式说明
/// - **意图（Why）**：通过统一的 Handler 捕获读写、异常、flush、优雅关闭等事件，降低
///   指标埋点的心智负担，并保证命名一致性。
/// - **结构（How）**：`configure` 中克隆 [`MetricsProvider`]，构造复合处理器 `MetricsHandler` 并分别
///   注册到入站与出站方向；Handler 内部以私有方法封装常见打点模式。
/// - **契约（What）**：`descriptor` 与 `label` 直接来自配置，保证中间件描述可追踪；`configure`
///   必须幂等，以支持控制面重复装配。
/// - **风险提示（Trade-offs）**：指标键值集中使用布尔/枚举标签，若业务需要高基数维度应在
///   配置层新增采样或聚合策略。
#[derive(Clone, Debug)]
pub struct MetricsMiddleware {
    config: MetricsMiddlewareConfig,
}

impl MetricsMiddleware {
    pub fn new(config: MetricsMiddlewareConfig) -> Self {
        Self { config }
    }
}

impl Default for MetricsMiddleware {
    fn default() -> Self {
        Self::new(MetricsMiddlewareConfig::default())
    }
}

impl PipelineInitializer for MetricsMiddleware {
    fn descriptor(&self) -> InitializerDescriptor {
        self.config.descriptor.clone()
    }

    fn configure(
        &self,
        chain: &mut dyn ChainBuilder,
        _channel: &dyn Channel,
        services: &CoreServices,
    ) -> spark_core::Result<(), CoreError> {
        let handler = MetricsHandler::new(
            self.config.descriptor.clone(),
            Arc::clone(&services.metrics),
        );
        chain.register_inbound(self.config.label.as_ref(), Box::new(handler.clone()));
        chain.register_outbound(self.config.label.as_ref(), Box::new(handler));
        Ok(())
    }
}

/// 实际执行指标记录的 Duplex Handler。
///
/// # 教案式说明
/// - **意图（Why）**：在运行时复用同一套逻辑处理入站与出站事件，避免分散在多个 Handler 中。
/// - **结构（How）**：内部持有共享的 [`MetricsProvider`] 引用，通过一组辅助方法完成标签构建
///   与指标写入；
/// - **契约（What）**：实现 [`InboundHandler`] 与 [`OutboundHandler`]，遵守 Handler 契约确保
///   `configure` 的幂等性与线程安全。
#[derive(Clone)]
struct MetricsHandler {
    inner: Arc<MetricsHandlerInner>,
}

struct MetricsHandlerInner {
    descriptor: InitializerDescriptor,
    metrics: Arc<dyn MetricsProvider>,
}

impl MetricsHandler {
    fn new(descriptor: InitializerDescriptor, metrics: Arc<dyn MetricsProvider>) -> Self {
        Self {
            inner: Arc::new(MetricsHandlerInner {
                descriptor,
                metrics,
            }),
        }
    }

    /// 记录计数器事件，封装 [`MetricsProvider::record_counter_add`] 调用。
    ///
    /// # 教案式说明
    /// - **意图（Why）**：集中管理指标写入逻辑，后续若需要批处理或增加错误兜底，可在此统一演进。
    /// - **结构（How）**：直接透传参数给底层 `MetricsProvider`；函数本身不做缓冲或重试。
    /// - **契约（What）**：`descriptor` 必须遵循命名规范，`attrs` 为结构化标签集合，`value`
    ///   表示计数增量。
    fn record_counter(
        &self,
        descriptor: &InstrumentDescriptor<'static>,
        attrs: &[KeyValue<'static>],
        value: u64,
    ) {
        self.inner
            .metrics
            .record_counter_add(descriptor, value, attrs);
    }

    /// 构建读写消息的结构化标签，并返回可选字节数。
    ///
    /// # 教案式说明
    /// - **意图（Why）**：复用方向/事件/消息类型等标签，确保日志与指标语义一致；
    /// - **结构（How）**：根据 `PipelineMessage` 变体设置 `message_kind` 与可选 `user_kind`，
    ///   若为缓冲数据则额外记录字节长度；
    /// - **契约（What）**：返回 `(标签集合, 可选字节数)`，调用方据此写入 `total` 与 `bytes`
    ///   两类指标；
    /// - **风险提示（Trade-offs）**：当字节数超过 `i64::MAX` 时采用饱和写入，避免指标后端溢出。
    fn build_message_fields(
        &self,
        direction: &'static str,
        event: &'static str,
        msg: &PipelineMessage,
    ) -> (Vec<KeyValue<'static>>, Option<u64>) {
        let mut fields = Vec::with_capacity(4);
        fields.push(KeyValue::new(ATTR_DIRECTION, direction));
        fields.push(KeyValue::new(ATTR_EVENT, event));
        let user_kind = msg.user_kind();
        let byte_count = match msg {
            PipelineMessage::Buffer(buf) => {
                fields.push(KeyValue::new(ATTR_MESSAGE_KIND, "buffer"));
                let remaining = buf.remaining() as u64;
                let logged_bytes = if remaining > i64::MAX as u64 {
                    i64::MAX
                } else {
                    remaining as i64
                };
                fields.push(KeyValue::new(ATTR_MESSAGE_BYTES, logged_bytes));
                Some(remaining)
            }
            PipelineMessage::User(_) => {
                fields.push(KeyValue::new(ATTR_MESSAGE_KIND, "user"));
                if let Some(kind) = user_kind {
                    fields.push(KeyValue::new(ATTR_USER_KIND, kind));
                }
                None
            }
            &_ => {
                fields.push(KeyValue::new(ATTR_MESSAGE_KIND, "unknown"));
                None
            }
        };
        (fields, byte_count)
    }

    fn record_write_signal(&self, signal: WriteSignal, base: &[KeyValue<'static>]) {
        let mut attrs = base.to_vec();
        attrs.push(KeyValue::new(
            ATTR_WRITE_SIGNAL,
            match signal {
                WriteSignal::Accepted => "accepted",
                WriteSignal::AcceptedAndFlushed => "accepted_and_flushed",
                WriteSignal::FlowControlApplied => "flow_control_applied",
                _ => "unknown",
            },
        ));
        self.record_counter(&METRIC_WRITE_SIGNAL_TOTAL, &attrs, 1);
    }

    fn record_error(&self, code: &'static str, event: &'static str) {
        let attrs = [
            KeyValue::new(ATTR_EVENT, event),
            KeyValue::new(ATTR_ERROR_CODE, code),
        ];
        self.record_counter(&METRIC_EXCEPTION_TOTAL, &attrs, 1);
    }

    /// 根据用户事件写入统一标签，保持链路观测一致性。
    fn record_user_event(&self, event: &CoreUserEvent) {
        let kind = event.application_event_kind().unwrap_or(match event {
            CoreUserEvent::TlsEstablished(_) => "tls_established",
            CoreUserEvent::IdleTimeout(_) => "idle_timeout",
            CoreUserEvent::RateLimited(_) => "rate_limited",
            CoreUserEvent::ConfigChanged { .. } => "config_changed",
            CoreUserEvent::ApplicationSpecific(_) => "application_specific",
            _ => "unknown",
        });
        let attrs = [
            KeyValue::new(ATTR_EVENT, "user_event"),
            KeyValue::new(ATTR_USER_EVENT, kind),
        ];
        self.record_counter(&METRIC_USER_EVENT_TOTAL, &attrs, 1);
    }

    /// 将调用超时时间转换为 [`Deadline`]，保持 Handler 与底层通道语义一致。
    fn convert_deadline(
        &self,
        ctx: &dyn spark_core::pipeline::context::Context,
        timeout: Option<core::time::Duration>,
    ) -> Option<Deadline> {
        timeout.map(|duration| {
            let now = ctx.timer().now();
            Deadline::with_timeout(now, duration)
        })
    }
}

impl InboundHandler for MetricsHandler {
    fn describe(&self) -> InitializerDescriptor {
        self.inner.descriptor.clone()
    }

    fn on_channel_active(&self, _ctx: &dyn spark_core::pipeline::context::Context) {
        let attrs = [KeyValue::new(ATTR_EVENT, "channel_active")];
        self.record_counter(&METRIC_CHANNEL_ACTIVE_TOTAL, &attrs, 1);
    }

    fn on_read(&self, ctx: &dyn spark_core::pipeline::context::Context, msg: PipelineMessage) {
        let (fields, bytes) = self.build_message_fields("inbound", "on_read", &msg);
        self.record_counter(&METRIC_READ_TOTAL, &fields, 1);
        if let Some(len) = bytes {
            self.record_counter(&METRIC_READ_BYTES, &fields, len);
        }
        ctx.forward_read(msg);
    }

    fn on_read_complete(&self, _ctx: &dyn spark_core::pipeline::context::Context) {
        let attrs = [KeyValue::new(ATTR_EVENT, "read_complete")];
        self.record_counter(&METRIC_READ_COMPLETE_TOTAL, &attrs, 1);
    }

    fn on_writability_changed(
        &self,
        _ctx: &dyn spark_core::pipeline::context::Context,
        is_writable: bool,
    ) {
        let attrs = [
            KeyValue::new(ATTR_EVENT, "writability_changed"),
            KeyValue::new(ATTR_WRITABLE, is_writable),
        ];
        self.record_counter(&METRIC_WRITABILITY_CHANGED_TOTAL, &attrs, 1);
    }

    fn on_user_event(
        &self,
        _ctx: &dyn spark_core::pipeline::context::Context,
        event: CoreUserEvent,
    ) {
        self.record_user_event(&event);
    }

    fn on_exception_caught(
        &self,
        _ctx: &dyn spark_core::pipeline::context::Context,
        error: CoreError,
    ) {
        self.record_error(error.code(), "exception_caught");
    }

    fn on_channel_inactive(&self, _ctx: &dyn spark_core::pipeline::context::Context) {
        let attrs = [KeyValue::new(ATTR_EVENT, "channel_inactive")];
        self.record_counter(&METRIC_CHANNEL_INACTIVE_TOTAL, &attrs, 1);
    }
}

impl OutboundHandler for MetricsHandler {
    fn describe(&self) -> InitializerDescriptor {
        self.inner.descriptor.clone()
    }

    fn on_write(
        &self,
        ctx: &dyn spark_core::pipeline::context::Context,
        msg: PipelineMessage,
    ) -> spark_core::Result<WriteSignal, CoreError> {
        let (fields, bytes) = self.build_message_fields("outbound", "on_write", &msg);
        self.record_counter(&METRIC_WRITE_TOTAL, &fields, 1);
        if let Some(len) = bytes {
            self.record_counter(&METRIC_WRITE_BYTES, &fields, len);
        }
        match ctx.write(msg) {
            Ok(signal) => {
                self.record_write_signal(signal, &fields);
                Ok(signal)
            }
            Err(error) => {
                let mut attrs = fields.clone();
                attrs.push(KeyValue::new(ATTR_ERROR_CODE, error.code()));
                self.record_counter(&METRIC_WRITE_ERROR_TOTAL, &attrs, 1);
                Err(error)
            }
        }
    }

    fn on_flush(
        &self,
        ctx: &dyn spark_core::pipeline::context::Context,
    ) -> spark_core::Result<(), CoreError> {
        let attrs = [KeyValue::new(ATTR_EVENT, "flush")];
        self.record_counter(&METRIC_FLUSH_TOTAL, &attrs, 1);
        ctx.flush();
        Ok(())
    }

    fn on_close_graceful(
        &self,
        ctx: &dyn spark_core::pipeline::context::Context,
        deadline: Option<core::time::Duration>,
    ) -> spark_core::Result<(), CoreError> {
        let mut attrs = Vec::with_capacity(2);
        attrs.push(KeyValue::new(ATTR_EVENT, "close_graceful"));
        if deadline.is_some() {
            attrs.push(KeyValue::new(ATTR_CLOSE_DEADLINE, true));
        } else {
            attrs.push(KeyValue::new(ATTR_CLOSE_DEADLINE, false));
        }
        self.record_counter(&METRIC_CLOSE_TOTAL, &attrs, 1);
        let deadline = self.convert_deadline(ctx, deadline);
        ctx.close_graceful(
            CloseReason::new(
                "spark.middleware.metrics.close",
                "metrics middleware propagated graceful shutdown",
            ),
            deadline,
        );
        Ok(())
    }
}
