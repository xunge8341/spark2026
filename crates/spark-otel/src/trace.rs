use std::sync::Arc;

use opentelemetry::{
    Context, KeyValue,
    trace::{
        self as otel_trace, Span as _, SpanContext as OtelSpanContext, SpanKind,
        TraceContextExt as _, Tracer as _,
    },
};
use opentelemetry_sdk::trace::Tracer;
use spark_core::observability::keys::tracing::pipeline as pipeline_keys;
use spark_core::observability::{
    SpanId, TraceContext, TraceFlags, TraceId, TraceState, TraceStateEntry,
};
use spark_core::pipeline::controller::HandlerDirection;
use spark_core::pipeline::instrument::{
    HandlerSpan, HandlerSpanGuard, HandlerSpanParams, HandlerSpanTracer,
};

use crate::Error;

/// OpenTelemetry 版本的 Handler Span Tracer，实现 `spark-core` 的自动建 Span 需求。
///
/// # 教案式说明
/// - **意图（Why）**：在 Pipeline Handler 调用前后自动维护分布式追踪 Span，并向业务层提供 `TraceContext`。
/// - **逻辑（How）**：使用 OpenTelemetry `Tracer` 构建 Span、设置父上下文与语义属性，再通过 [`HandlerSpanGuard`] 在作用域末尾调用
///   `Span::end` 完成收尾。
/// - **契约（What）**：始终返回与父上下文同一 `trace_id` 的新子上下文，并保证在 Span 生命周期结束时正确退出。
#[derive(Clone)]
pub(crate) struct OtelHandlerTracer {
    tracer: Tracer,
}

impl OtelHandlerTracer {
    pub(crate) fn new(tracer: Tracer) -> Self {
        Self { tracer }
    }

    pub(crate) fn start_with_parent(
        &self,
        params: &HandlerSpanParams<'_>,
        parent: &TraceContext,
    ) -> spark_core::Result<HandlerSpan, Error> {
        let parent_context = trace_context_to_otel(parent)?;
        let span_name = format!(
            "spark.pipeline.{}.{}",
            direction_tag(params.direction),
            params.descriptor.name()
        );

        let mut builder = self.tracer.span_builder(span_name);
        builder.span_kind = Some(otel_span_kind(params.direction));
        builder.attributes = Some(vec![
            KeyValue::new(
                pipeline_keys::ATTR_DIRECTION,
                direction_tag(params.direction).to_string(),
            ),
            KeyValue::new(pipeline_keys::ATTR_LABEL, params.label.to_string()),
            KeyValue::new(
                pipeline_keys::ATTR_COMPONENT,
                params.descriptor.name().to_string(),
            ),
            KeyValue::new(
                pipeline_keys::ATTR_CATEGORY,
                params.descriptor.category().to_string(),
            ),
            KeyValue::new(
                pipeline_keys::ATTR_SUMMARY,
                params.descriptor.summary().to_string(),
            ),
        ]);

        let span = self.tracer.build_with_context(builder, &parent_context);
        let derived = trace_context_from_otel(span.span_context())?;
        let guard = Box::new(OtelHandlerSpanGuard::new(span));

        Ok(HandlerSpan::from_parts(derived, Some(guard)))
    }
}

impl HandlerSpanTracer for OtelHandlerTracer {
    fn start_span(&self, params: &HandlerSpanParams<'_>, parent: &TraceContext) -> HandlerSpan {
        self.start_with_parent(params, parent)
            .unwrap_or_else(|_| fallback_span(parent))
    }
}

/// 持有 `tracing` Span 的 Guard，负责在 Handler 回调结束后退出 Span。
struct OtelHandlerSpanGuard {
    span: opentelemetry_sdk::trace::Span,
}

impl OtelHandlerSpanGuard {
    fn new(span: opentelemetry_sdk::trace::Span) -> Self {
        Self { span }
    }
}

impl HandlerSpanGuard for OtelHandlerSpanGuard {
    fn finish(mut self: Box<Self>) {
        self.span.end();
    }
}

pub(crate) fn direction_tag(direction: HandlerDirection) -> &'static str {
    match direction {
        HandlerDirection::Inbound => pipeline_keys::DIRECTION_INBOUND,
        HandlerDirection::Outbound => pipeline_keys::DIRECTION_OUTBOUND,
        _ => pipeline_keys::DIRECTION_UNSPECIFIED,
    }
}

pub(crate) fn otel_span_kind(direction: HandlerDirection) -> SpanKind {
    match direction {
        HandlerDirection::Inbound => SpanKind::Server,
        HandlerDirection::Outbound => SpanKind::Client,
        _ => SpanKind::Internal,
    }
}

pub(crate) fn trace_context_to_otel(parent: &TraceContext) -> spark_core::Result<Context, Error> {
    let otel_state = otel_trace::TraceState::from_key_value(
        parent
            .trace_state
            .iter()
            .map(|entry| (entry.key.as_str(), entry.value.as_str())),
    )
    .map_err(|err| Error::TraceStateConversion(format!("构造 otel TraceState 失败: {err}")))?;

    let span_context = OtelSpanContext::new(
        otel_trace::TraceId::from_bytes(parent.trace_id.to_bytes()),
        otel_trace::SpanId::from_bytes(parent.span_id.to_bytes()),
        otel_trace::TraceFlags::new(parent.trace_flags.bits()),
        true,
        otel_state,
    );

    Ok(Context::new().with_remote_span_context(span_context))
}

pub(crate) fn trace_context_from_otel(
    ctx: &OtelSpanContext,
) -> spark_core::Result<TraceContext, Error> {
    let state = ctx.trace_state().header();
    let trace_state = if state.is_empty() {
        TraceState::default()
    } else {
        let entries = state
            .split(',')
            .filter_map(|pair| pair.split_once('='))
            .map(|(key, value)| TraceStateEntry::new(key.to_string(), value.to_string()))
            .collect();
        TraceState::from_entries(entries).map_err(|err| {
            Error::TraceStateConversion(format!("otel TraceState -> spark 失败: {err}"))
        })?
    };

    Ok(TraceContext {
        trace_id: TraceId::from_bytes(ctx.trace_id().to_bytes()),
        span_id: SpanId::from_bytes(ctx.span_id().to_bytes()),
        trace_flags: TraceFlags::new(ctx.trace_flags().to_u8()),
        trace_state,
    })
}

pub(crate) fn fallback_span(parent: &TraceContext) -> HandlerSpan {
    let span_id = TraceContext::generate().span_id;
    let trace_context = parent.child_context(span_id);
    HandlerSpan::from_parts(trace_context, None)
}

pub(crate) fn make_handler_tracer(tracer: Tracer) -> Arc<dyn HandlerSpanTracer> {
    Arc::new(OtelHandlerTracer::new(tracer))
}
