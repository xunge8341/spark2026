use alloc::{borrow::Cow, boxed::Box, sync::Arc};
use core::fmt;

use spark_core::{
    CoreError, Error,
    buffer::PipelineMessage,
    contract::{CloseReason, Deadline},
    observability::{attributes::KeyValue, logging::Logger},
    pipeline::{
        ChainBuilder, Channel, InitializerDescriptor, PipelineInitializer,
        channel::WriteSignal,
        handler::{InboundHandler, OutboundHandler},
    },
    runtime::CoreServices,
};

/// 函数类型别名：描述编解码闭包的签名。
type TransformFn = dyn Fn(
        &dyn spark_core::pipeline::context::Context,
        PipelineMessage,
    ) -> spark_core::Result<PipelineMessage, CoreError>
    + Send
    + Sync
    + 'static;

/// `CodecTransform` 封装入站解码与出站编码的具体策略。
///
/// # 教案式说明
/// - **意图（Why）**：在控制层以闭包方式注入编解码逻辑，既兼容泛型实现，也便于快速接入原型代码。
/// - **结构（How）**：内部持有 `Arc<TransformFn>`，分别对应 `decode` 与 `encode`，以确保 Handler
///   在多线程环境下安全克隆、共享状态。
/// - **契约（What）**：闭包必须满足 `Send + Sync + 'static`，并遵循 [`PipelineMessage`] 的所有权规则。
/// - **风险提示（Trade-offs）**：闭包若捕获大对象，请使用 `Arc` 以免拷贝成本过高；若解码失败需返回
///   `CoreError`，Handler 会负责日志记录与关闭策略。
#[derive(Clone)]
pub struct CodecTransform {
    decode: Arc<TransformFn>,
    encode: Arc<TransformFn>,
}

impl CodecTransform {
    /// 基于给定闭包构造编解码策略。
    pub fn new<D, E>(decode: D, encode: E) -> Self
    where
        D: Fn(
                &dyn spark_core::pipeline::context::Context,
                PipelineMessage,
            ) -> spark_core::Result<PipelineMessage, CoreError>
            + Send
            + Sync
            + 'static,
        E: Fn(
                &dyn spark_core::pipeline::context::Context,
                PipelineMessage,
            ) -> spark_core::Result<PipelineMessage, CoreError>
            + Send
            + Sync
            + 'static,
    {
        Self {
            decode: Arc::new(decode),
            encode: Arc::new(encode),
        }
    }

    /// 调用入站解码逻辑。
    pub fn decode(
        &self,
        ctx: &dyn spark_core::pipeline::context::Context,
        msg: PipelineMessage,
    ) -> spark_core::Result<PipelineMessage, CoreError> {
        (self.decode)(ctx, msg)
    }

    /// 调用出站编码逻辑。
    pub fn encode(
        &self,
        ctx: &dyn spark_core::pipeline::context::Context,
        msg: PipelineMessage,
    ) -> spark_core::Result<PipelineMessage, CoreError> {
        (self.encode)(ctx, msg)
    }
}

impl fmt::Debug for CodecTransform {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CodecTransform")
            .field("decode", &"opaque")
            .field("encode", &"opaque")
            .finish()
    }
}

/// 中间件配置：指定描述符与 Handler 注册标签。
///
/// # 教案式说明
/// - **意图（Why）**：在装配阶段显式描述编解码组件的来源与用途，便于观测与审计。
/// - **结构（How）**：包含 [`InitializerDescriptor`] 与注册标签 `label`，与其他中间件保持一致。
/// - **契约（What）**：`label` 需在同一 Pipeline 内唯一；`descriptor` 建议遵循 `vendor.component` 命名。
/// - **风险提示（Trade-offs）**：若同一链路安装多个编解码器，请确保标签唯一并在描述中区分用途。
#[derive(Clone, Debug)]
pub struct CodecMiddlewareConfig {
    pub descriptor: InitializerDescriptor,
    pub label: Cow<'static, str>,
}

impl Default for CodecMiddlewareConfig {
    fn default() -> Self {
        Self {
            descriptor: InitializerDescriptor::new(
                "spark.middleware.codec",
                "codec",
                "将 Pipeline 消息在字节与业务类型间转换",
            ),
            label: Cow::Borrowed("codec"),
        }
    }
}

/// CodecMiddleware 将编解码策略以 Handler 形式装配到 Pipeline。
///
/// # 教案式说明
/// - **意图（Why）**：提供声明式入口将任意编解码闭包注入 Pipeline，兼容科研原型与生产实现。
/// - **结构（How）**：持有 [`CodecTransform`] 与配置，`configure` 中构造复合处理器 `CodecHandler`
///   并注册为入站/出站 Handler。
/// - **契约（What）**：`descriptor` 来自配置；`configure` 幂等，可在热更新场景重复调用。
/// - **风险提示（Trade-offs）**：闭包执行在 Handler 线程上，需确保无阻塞，必要时可调用
///   `ctx.executor()` 将重计算任务移交给运行时。
#[derive(Clone, Debug)]
pub struct CodecMiddleware {
    config: CodecMiddlewareConfig,
    transform: CodecTransform,
}

impl CodecMiddleware {
    /// 使用默认配置与给定编解码策略构造中间件。
    pub fn new(transform: CodecTransform) -> Self {
        Self {
            config: CodecMiddlewareConfig::default(),
            transform,
        }
    }

    /// 使用自定义配置构造中间件。
    pub fn with_config(config: CodecMiddlewareConfig, transform: CodecTransform) -> Self {
        Self { config, transform }
    }
}

impl PipelineInitializer for CodecMiddleware {
    fn descriptor(&self) -> InitializerDescriptor {
        self.config.descriptor.clone()
    }

    fn configure(
        &self,
        chain: &mut dyn ChainBuilder,
        _channel: &dyn Channel,
        _services: &CoreServices,
    ) -> spark_core::Result<(), CoreError> {
        let handler = CodecHandler::new(self.config.descriptor.clone(), self.transform.clone());
        chain.register_inbound(self.config.label.as_ref(), Box::new(handler.clone()));
        chain.register_outbound(self.config.label.as_ref(), Box::new(handler));
        Ok(())
    }
}

/// 实际执行编解码的 Handler。
///
/// # 教案式说明
/// - **意图（Why）**：封装通用的错误处理与日志逻辑，让业务闭包专注于转换本身。
/// - **结构（How）**：持有 [`CodecTransform`]，在读路径调用 `decode`，写路径调用 `encode`；
///   失败时记录结构化日志并触发优雅关闭。
/// - **契约（What）**：实现 [`InboundHandler`] 与 [`OutboundHandler`]，保持线程安全与幂等性。
#[derive(Clone)]
struct CodecHandler {
    descriptor: InitializerDescriptor,
    transform: CodecTransform,
}

impl CodecHandler {
    fn new(descriptor: InitializerDescriptor, transform: CodecTransform) -> Self {
        Self {
            descriptor,
            transform,
        }
    }

    fn log_failure(
        &self,
        ctx: &dyn spark_core::pipeline::context::Context,
        stage: &'static str,
        error: &CoreError,
    ) {
        let logger: &dyn Logger = ctx.logger();
        let fields = [
            KeyValue::new("spark.middleware.codec.stage", stage),
            KeyValue::new("spark.middleware.codec.error_code", error.code()),
        ];
        logger.error_with_fields(
            "codec middleware encountered an error",
            Some(error as &dyn Error),
            &fields,
            Some(ctx.trace_context()),
        );
    }

    fn close_with_error(
        &self,
        ctx: &dyn spark_core::pipeline::context::Context,
        code: &'static str,
    ) {
        ctx.close_graceful(
            CloseReason::new(code, "codec middleware requested graceful shutdown"),
            None,
        );
    }
}

impl InboundHandler for CodecHandler {
    fn describe(&self) -> InitializerDescriptor {
        self.descriptor.clone()
    }

    fn on_read(&self, ctx: &dyn spark_core::pipeline::context::Context, msg: PipelineMessage) {
        match self.transform.decode(ctx, msg) {
            Ok(decoded) => ctx.forward_read(decoded),
            Err(error) => {
                self.log_failure(ctx, "decode", &error);
                self.close_with_error(ctx, "spark.middleware.codec.decode_failed");
            }
        }
    }

    fn on_exception_caught(
        &self,
        ctx: &dyn spark_core::pipeline::context::Context,
        error: CoreError,
    ) {
        self.log_failure(ctx, "decode_exception", &error);
        self.close_with_error(ctx, "spark.middleware.codec.decode_exception");
    }

    fn on_channel_active(&self, _ctx: &dyn spark_core::pipeline::context::Context) {}

    fn on_read_complete(&self, _ctx: &dyn spark_core::pipeline::context::Context) {}

    fn on_writability_changed(
        &self,
        _ctx: &dyn spark_core::pipeline::context::Context,
        _is_writable: bool,
    ) {
    }

    fn on_user_event(
        &self,
        _ctx: &dyn spark_core::pipeline::context::Context,
        _event: spark_core::observability::CoreUserEvent,
    ) {
    }

    fn on_channel_inactive(&self, _ctx: &dyn spark_core::pipeline::context::Context) {}
}

impl OutboundHandler for CodecHandler {
    fn describe(&self) -> InitializerDescriptor {
        self.descriptor.clone()
    }

    fn on_write(
        &self,
        ctx: &dyn spark_core::pipeline::context::Context,
        msg: PipelineMessage,
    ) -> spark_core::Result<WriteSignal, CoreError> {
        match self.transform.encode(ctx, msg) {
            Ok(encoded) => ctx.write(encoded),
            Err(error) => {
                self.log_failure(ctx, "encode", &error);
                Err(error)
            }
        }
    }

    fn on_flush(
        &self,
        ctx: &dyn spark_core::pipeline::context::Context,
    ) -> spark_core::Result<(), CoreError> {
        ctx.flush();
        Ok(())
    }

    fn on_close_graceful(
        &self,
        ctx: &dyn spark_core::pipeline::context::Context,
        deadline: Option<core::time::Duration>,
    ) -> spark_core::Result<(), CoreError> {
        let deadline = deadline.map(|timeout| {
            let now = ctx.timer().now();
            Deadline::with_timeout(now, timeout)
        });
        ctx.close_graceful(
            CloseReason::new(
                "spark.middleware.codec.close",
                "codec middleware propagated graceful shutdown",
            ),
            deadline,
        );
        Ok(())
    }
}
