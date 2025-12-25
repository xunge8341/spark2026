use alloc::{boxed::Box, sync::Arc};

use spark_core::{
    Result as CoreResult, async_trait,
    context::Context,
    pipeline::DynPipelineFactory,
    transport::{DynServerChannel, DynTransportFactory, ListenerConfig},
};

/// `TransportFactoryExt` 为对象层传输工厂补充统一的 `listen` 语义。
///
/// # 教案级说明
///
/// ## 意图 (Why)
/// - 将 `DynTransportFactory::bind_dyn` 封装为更语义化的 `listen` 调用，便于宿主侧以直观 API 启动监听流程；
/// - 隔离出未来可能扩展的生命周期管理（如监听器注册、健康检查预热），使上层不必直接操作底层 `bind`。
///
/// ## 解析逻辑 (How)
/// - 接收 `Context` 以继承取消/截止约束；
/// - 透传 `ListenerConfig` 与 Pipeline 工厂给传输实现，由其按协议完成监听器构建；
/// - 返回对象层 [`DynServerChannel`]，供宿主保存并在后续执行关闭或观测操作。
///
/// ## 契约定义 (What)
/// - `ctx`：调用时的执行上下文，要求生命周期覆盖监听器构建过程；
/// - `config`：监听配置，需确保与工厂协议匹配；
/// - `pipeline_factory`：Pipeline 控制器工厂，会被传输实现用于按连接装配控制面；
/// - 返回：成功时给出监听器句柄，失败时返回结构化 [`CoreError`]。
///
/// ## 考量与风险 (Trade-offs & Gotchas)
/// - 当前仅做简单透传，未来可在此处统一记录监听器指标或注入审计日志；
/// - 若底层实现长时间阻塞在 `bind_dyn`，调用方应通过 `ctx` 施加超时或取消策略；
/// - 该扩展 trait 仅为语义糖，保持零额外分配与虚分派开销。
#[async_trait]
pub trait TransportFactoryExt {
    /// 将传输工厂启动为监听器。
    async fn listen(
        &self,
        ctx: &Context<'_>,
        config: ListenerConfig,
        pipeline_factory: Arc<dyn DynPipelineFactory>,
    ) -> CoreResult<Box<dyn DynServerChannel>>;
}

#[async_trait]
impl<T> TransportFactoryExt for T
where
    T: DynTransportFactory + ?Sized,
{
    async fn listen(
        &self,
        ctx: &Context<'_>,
        config: ListenerConfig,
        pipeline_factory: Arc<dyn DynPipelineFactory>,
    ) -> CoreResult<Box<dyn DynServerChannel>> {
        self.bind_dyn(ctx, config, pipeline_factory).await
    }
}
