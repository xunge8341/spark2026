use std::sync::Arc;

use spark_core::observability::{
    HealthChecks, Logger, MetricsProvider, ObservabilityFacade, OpsEventBus,
};

/// 以现有 `Arc` 组合实现的参考外观。
///
/// # 设计背景（Why）
/// - 服务于“原型验证”阶段：证明在不破坏既有字段的情况下，可通过外观整合可观测性能力；
/// - 提供默认实现以便测试与示例使用，降低迁移门槛。
///
/// # 构成说明（What）
/// - 持有日志、指标、运维事件的 `Arc` 克隆，以及共享的健康探针集合。
/// - **前置条件**：构造时传入的所有句柄必须满足各自 Trait 的线程安全约束。
/// - **后置条件**：外观本身可 `Clone`，克隆后仍指向同一底层资源，确保 Handler 间安全传递。
///
/// # 逻辑解析（How）
/// - `new` 构造函数直接存储来访句柄，不做额外校验；如需校验可在宿主层扩展。
/// - [`ObservabilityFacade`] 实现简单转发对应字段，保证零额外逻辑成本。
///
/// # 风险提示（Trade-offs）
/// - 该结构作为教学示例未实现能力探针或懒加载，如需复杂策略请自定义实现。
/// - `health_checks` 直接共享引用，如需写时复制需另行封装。
#[derive(Clone)]
pub struct DefaultObservabilityFacade {
    logger: Arc<dyn Logger>,
    metrics: Arc<dyn MetricsProvider>,
    ops_bus: Arc<dyn OpsEventBus>,
    health_checks: HealthChecks,
}

impl DefaultObservabilityFacade {
    /// 构造组合后的外观实例。
    ///
    /// # 参数（Inputs）
    /// - `logger`: 结构化日志句柄，需实现 [`Logger`].
    /// - `metrics`: 指标采集提供者，需实现 [`MetricsProvider`].
    /// - `ops_bus`: 运维事件总线，需实现 [`OpsEventBus`].
    /// - `health_checks`: 共享健康探针集合，一般来自运行时依赖注入。
    ///
    /// # 契约说明（What）
    /// - **前置条件**：所有参数均不可为 `None`；如无对应能力请在外层使用空实现（如 `NoopLogger`）。
    /// - **后置条件**：返回的外观可在多线程环境下安全克隆与共享，内部资源引用计数保持一致。
    ///
    /// # 设计考量（Trade-offs）
    /// - 采用 `Arc` 而非裸引用以确保对象安全；若宿主希望延迟初始化，可包装在 `OnceLock` 后再传入。
    pub fn new(
        logger: Arc<dyn Logger>,
        metrics: Arc<dyn MetricsProvider>,
        ops_bus: Arc<dyn OpsEventBus>,
        health_checks: HealthChecks,
    ) -> Self {
        Self {
            logger,
            metrics,
            ops_bus,
            health_checks,
        }
    }
}

impl ObservabilityFacade for DefaultObservabilityFacade {
    fn logger(&self) -> Arc<dyn Logger> {
        Arc::clone(&self.logger)
    }

    fn metrics(&self) -> Arc<dyn MetricsProvider> {
        Arc::clone(&self.metrics)
    }

    fn ops_bus(&self) -> Arc<dyn OpsEventBus> {
        Arc::clone(&self.ops_bus)
    }

    fn health_checks(&self) -> &HealthChecks {
        &self.health_checks
    }
}

/// 历史注入路径的兼容适配层。
///
/// # 设计动机（Why）
/// - 在引入 [`ObservabilityFacade`] 之前，调用方通常直接管理日志、指标、运维事件总线
///   以及健康探针四个句柄；为了保证迁移期平滑，本结构以 `#[deprecated]` 形式保留原
///   有“分散注入”接口，便于旧代码在不修改签名的情况下复用 Facade 能力。
/// - 结构位于 `spark-otel::facade` 模块，与 Facade 并列，扮演“适配层”角色：
///   既能快速转换为新的 Facade，也支持从 Facade 回退为旧句柄组合，为灰度迁移提供
///   双向通道。
///
/// # 逻辑解析（How）
/// - `new` 构造函数按原接口签名接收四个句柄；
/// - `into_facade` / `to_facade` 将内部字段封装为 [`DefaultObservabilityFacade`]，
///   供新 API 直接复用；
/// - `from_facade` 允许在仅持有 Facade 时重新获得旧句柄集合，方便尚未迁移的组件；
/// - 所有方法都克隆 `Arc`，确保不会改变原有所有权语义。
///
/// # 契约说明（What）
/// - **字段含义**：`logger`/`metrics`/`ops_bus` 分别对应结构化日志、指标、运维事件；
///   `health_checks` 为共享健康探针集合；
/// - **前置条件**：与旧接口一致，所有句柄必须满足 `Send + Sync + 'static`；
/// - **后置条件**：适配器仅复制引用计数，不修改底层实例状态。
///
/// # 风险与权衡（Trade-offs）
#[derive(Clone)]
#[deprecated(
    since = "0.2.0",
    note = "removal: planned after Trait 合并路线完成收敛阶段；migration: 直接依赖 ObservabilityFacade 并通过 CoreServices::with_observability_facade 注入能力，避免继续使用旧句柄组合。"
)]
pub struct LegacyObservabilityHandles {
    /// 结构化日志句柄（旧接口字段，保持向后兼容）。
    pub logger: Arc<dyn Logger>,
    /// 指标采集提供者（旧接口字段，保持向后兼容）。
    pub metrics: Arc<dyn MetricsProvider>,
    /// 运维事件总线（旧接口字段，保持向后兼容）。
    pub ops_bus: Arc<dyn OpsEventBus>,
    /// 健康探针集合（旧接口字段，保持向后兼容）。
    pub health_checks: HealthChecks,
}

#[allow(deprecated)]
impl LegacyObservabilityHandles {
    /// 以旧接口字段构造兼容适配层。
    pub fn new(
        logger: Arc<dyn Logger>,
        metrics: Arc<dyn MetricsProvider>,
        ops_bus: Arc<dyn OpsEventBus>,
        health_checks: HealthChecks,
    ) -> Self {
        Self {
            logger,
            metrics,
            ops_bus,
            health_checks,
        }
    }

    /// 将旧句柄组合升级为 [`DefaultObservabilityFacade`]。
    pub fn into_facade(self) -> DefaultObservabilityFacade {
        DefaultObservabilityFacade::new(self.logger, self.metrics, self.ops_bus, self.health_checks)
    }

    /// 从旧句柄组合生成新的 Facade，但保留原适配器供重复使用。
    pub fn to_facade(&self) -> DefaultObservabilityFacade {
        DefaultObservabilityFacade::new(
            Arc::clone(&self.logger),
            Arc::clone(&self.metrics),
            Arc::clone(&self.ops_bus),
            self.health_checks.clone(),
        )
    }

    /// 自 Facade 恢复旧句柄组合，支持旧代码路径访问具体句柄。
    #[allow(deprecated)]
    pub fn from_facade(facade: &impl ObservabilityFacade) -> Self {
        Self {
            logger: facade.logger(),
            metrics: facade.metrics(),
            ops_bus: facade.ops_bus(),
            health_checks: facade.health_checks().clone(),
        }
    }
}
