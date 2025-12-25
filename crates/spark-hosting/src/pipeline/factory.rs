use alloc::{sync::Arc, vec::Vec};

use spark_core::{
    CoreError, Result as CoreResult,
    contract::CallContext,
    pipeline::{
        Channel, Pipeline, PipelineFactory as CorePipelineFactory, PipelineInitializer,
        controller::HotSwapPipeline,
    },
    runtime::CoreServices,
};

/// `DefaultPipelineFactory` 负责将传输层创建的 `Channel`、宿主提供的
/// [`PipelineInitializer`] 序列与 `CoreServices` 聚合成可直接投入运行的
/// `PipelineController` 实例。
///
/// # 教案级说明
/// - **定位（Where）**：
///   - 位于 `spark-hosting` crate 内部，对外作为 `spark-examples`/`spark-hosting` 在运行时按连接
///     装配控制器的默认实现；
///   - 与 `spark-core` 的 `PipelineFactory` trait 对齐，确保既能服务泛型层也能被对象层
///     适配器复用。
/// - **意图（Why）**：
///   - 在连接建立后，框架需要一个统一入口将通道、调用上下文与 PipelineInitializer（旧称
///     Middleware）链路一次性注入
///     控制器，避免调用方散落地拼装；
///   - 通过集中式工厂可在后续扩展中统一附加审计、指标或调试钩子，而无需侵入每个
///     传输实现。
/// - **执行逻辑（How）**：
///   1. 构造阶段收集 `Channel`、`CallContext` 以及按顺序排列的
///      [`PipelineInitializer`] 引用；
///   2. [`Self::build`] 被调用时克隆 `CoreServices` 与 `CallContext`，创建
///      `PipelineController`；
///   3. 遍历缓存的 PipelineInitializer，依次调用 `install_middleware` 装配 Handler 链路；
///   4. 若任一步骤失败，立即返回结构化 [`CoreError`]，保证调用方能够中止管线初始化。
/// - **契约（What）**：
///   - 构造函数要求：
///     - `channel` 必须实现 [`Channel`]，生命周期覆盖整个连接；
///     - `call_context` 为单次连接派生的上下文副本；
///     - `middleware` 迭代顺序将直接映射为装配顺序。
///   - `build` 成功返回后，控制器已装配完所有 PipelineInitializer 并可立即响应事件。
/// - **风险与权衡（Trade-offs & Gotchas）**：
///   - 工厂内部持有 `Arc<dyn Channel>` 与 `Arc<dyn PipelineInitializer>`，构造与 `build` 均存在常数次
///     `Arc::clone` 开销；
///   - 若 PipelineInitializer 的 `configure` 实现非幂等，重复调用 `build` 可能导致链路状态不一致，
///     因此调用方应确保单个工厂仅用于单条连接；
///   - TODO(pipeline-factory-hooks)：后续可在此处插入装配可观测性或策略检查逻辑，以提
///     升诊断能力。
#[derive(Clone)]
pub struct DefaultPipelineFactory {
    /// 持有通道引用，供控制器在构造时绑定生命周期。
    channel: Arc<dyn Channel>,
    /// 连接级调用上下文，携带取消/截止/预算等契约。
    call_context: CallContext,
    /// 按顺序存放的 PipelineInitializer 列表，确保装配顺序确定。
    initializers: Vec<Arc<dyn PipelineInitializer>>,
}

type PipelineController = HotSwapPipeline;

impl DefaultPipelineFactory {
    /// 创建 `DefaultPipelineFactory` 实例。
    ///
    /// # 教案级说明
    /// - **参数与约束**：
    ///   - `channel`：满足 [`Channel`] 契约的 `Arc` 包装对象，要求在整个控制器生命周期内
    ///     保持有效；
    ///   - `call_context`：来自传输层或会话初始化阶段的上下文快照，通常按连接独立生成；
    ///   - `middleware`：实现 [`PipelineInitializer`] 的对象列表，迭代顺序将按原样用于链路装配。
    /// - **前置条件（Preconditions）**：
    ///   - 传入的 PipelineInitializer 实例需满足 `Send + Sync + 'static`，并在 `configure` 中保持幂等；
    ///   - 同一工厂实例不应跨多条连接复用，以免共享状态污染。
    /// - **后置条件（Postconditions）**：
    ///   - 返回的工厂缓存全部依赖，后续调用 [`Self::build`] 时无需再次收集参数；
    ///   - PipelineInitializer 会被按提供顺序存储，便于在诊断时追溯链路拓扑。
    /// - **设计考量（Trade-offs）**：
    ///   - 采用 `IntoIterator<Item = Arc<dyn PipelineInitializer>>` 作为输入，允许调用方在不同容器
    ///     间灵活转换，但会产生一次 `collect` 分配；
    ///   - 通过派生 `Clone`，便于测试或诊断时复制工厂并记录状态。
    pub fn new(
        channel: Arc<dyn Channel>,
        call_context: CallContext,
        middleware: impl IntoIterator<Item = Arc<dyn PipelineInitializer>>,
    ) -> Self {
        Self {
            channel,
            call_context,
            initializers: middleware.into_iter().collect(),
        }
    }
}

impl CorePipelineFactory for DefaultPipelineFactory {
    type Pipeline = Arc<PipelineController>;

    /// 装配带有 PipelineInitializer 链路的 `PipelineController` 并返回。
    ///
    /// # 教案级说明
    /// - **执行步骤（How）**：
    ///   1. 克隆 `CoreServices` 与 `CallContext`，构造控制器；
    ///   2. 遍历 PipelineInitializer 列表，调用 `install_middleware`，若任意步骤失败立即返回错误；
    ///   3. 全部装配完成后返回控制器供调用方使用。
    /// - **契约定义（What）**：
    ///   - **输入**：`core_services` 为运行时依赖快照，需要在调用前初始化完备；
    ///   - **输出**：成功时返回 `PipelineController`；失败时返回结构化 [`CoreError`]；
    ///   - **前置条件**：工厂实例必须由 [`Self::new`] 正确构造；
    ///   - **后置条件**：返回的控制器已完成所有 PipelineInitializer 装配，具备线程安全能力。
    /// - **风险提示（Trade-offs & Gotchas）**：
    ///   - 若某个 PipelineInitializer 装配失败，已安装的 PipelineInitializer 不会自动回滚；
    ///     调用方应丢弃当前控制器并
    ///     重新构建；
    ///   - 克隆 `CoreServices` 会产生若干 `Arc` 引用计数更新，属于常数成本；
    ///   - 为避免热路径重复构建，建议按连接复用工厂并仅调用一次 `build`。
    fn build(&self, core_services: &CoreServices) -> CoreResult<Self::Pipeline, CoreError> {
        // 克隆运行时依赖与上下文，确保控制器拥有独立的能力快照。
        let controller = PipelineController::new(
            Arc::clone(&self.channel),
            core_services.clone(),
            self.call_context.clone(),
        );

        // 依次装配 PipelineInitializer，遵循调用方提供的顺序，保障 Handler 拓扑可预测。
        for initializer in &self.initializers {
            controller.install_middleware(initializer.as_ref(), core_services)?;
        }

        Ok(controller)
    }
}
