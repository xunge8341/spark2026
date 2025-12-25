//! 观测性与度量相关的测试桩集合，集中提供常见 `Noop` 实现。
//!
//! # 设计定位（Why）
//! - 合约测试与端到端验证经常需要满足 `MetricsProvider`、`Logger` 等契约，但在多数场景仅关注业务逻辑。
//! - 过去各测试文件内重复定义 `struct NoopCounter;` 等类型，不仅增加维护成本，也容易在接口调整时漏改。
//! - 通过统一出口，测试可以直接复用这些桩对象，并在接口发生变更时获得集中编译错误提示。
//!
//! # 使用方式（How）
//! - 通过 `use spark_core::test_stubs::observability::*;` 引入需要的桩类型，或使用提供的辅助函数快速获取共享引用。
//! - 所有类型均为零尺寸（ZST），可以按值或通过 `Arc` 复用，不会引入额外资源占用。
//! - 若未来需要扩展更多领域的桩对象，可在本模块追加子模块并保持“Why/How/What”风格的文档。
//!
//! # 契约说明（What）
//! - **前置条件**：调用方需保证这些桩类型仅用于测试或示例环境，生产代码若依赖应显式说明原因。
//! - **后置条件**：桩对象不会产生副作用，也不会与真实观测性基础设施交互。
//!
//! # 风险与权衡（Trade-offs）
//! - 桩对象完全忽略输入参数，可能掩盖依赖真实指标系统的行为；如需验证指标正确性，应替换为 Recording 实现。
//! - 模块公开为稳定测试 API，未来新增字段或方法时需同步更新此处实现，以避免破坏测试假设。

pub mod observability {
    //! 观测性契约使用的 `Noop` 实现合集。
    //!
    //! # 模块职责（Why）
    //! - 提供满足 [`MetricsProvider`]、[`Counter`]、[`Gauge`]、[`Histogram`] 与 [`Logger`] 契约的最小实现，
    //!   以便在测试中隔离真实监控系统。
    //! - 将“无副作用”实现集中管理，减少跨文件复制，并在接口演进时提供单点更新能力。
    //!
    //! # 使用指南（How）
    //! - 直接实例化零尺寸结构体，例如 `NoopCounter`、`NoopGauge`；这些类型实现 `Clone + Copy`，可以随意拷贝。
    //! - 若需要共享所有权，可调用 [`shared_metrics_provider`] 获取 `Arc<dyn MetricsProvider>`，避免在测试中重复包裹。
    //! - 所有方法都会立即返回，不会记录任何状态，可用于验证调用链是否被触达。
    //!
    //! # 契约细节（What）
    //! - **输入约束**：所有桩方法接受与真实接口一致的参数类型，但不会检查取值范围。
    //! - **输出保证**：`MetricsProvider` 的工厂方法始终返回实现对应契约的桩对象；`Logger::log` 为空操作。
    //! - **前置条件**：这些桩实现假定调用环境对观测数据不做断言，仅关心调用是否成功。
    //! - **后置条件**：不会触发网络、IO 或线程调度等副作用，适合在 `no_std + alloc` 环境复用。
    //!
    //! # 风险提示（Trade-offs & Gotchas）
    //! - 由于完全忽略输入，若测试希望断言指标名称或属性，需要改用记录型实现。
    //! - 这些类型随 `spark-core` 发布对外可见，若外部依赖于它们，请注意版本升级时的兼容性公告。

    use alloc::sync::Arc;

    use crate::observability::{
        AttributeSet, Counter, Gauge, HealthChecks, Histogram, InstrumentDescriptor, LogRecord,
        Logger, MetricsProvider, ObservabilityFacade, OpsEventBus,
    };

    /// `NoopMetricsProvider` 为度量系统提供统一的空实现，确保测试可以轻松满足依赖注入需求。
    ///
    /// # 设计动机（Why）
    /// - 在 Handler、Router 等组件的测试中，仅需要一个满足 [`MetricsProvider`] 契约的对象，
    ///   以便构建依赖图；无需真正输出指标。
    /// - 若未来 `MetricsProvider` 接口扩展，只需更新本结构体的实现即可完成全局适配。
    ///
    /// # 行为描述（How）
    /// - `counter/gauge/histogram` 方法始终返回对应的零尺寸桩对象。
    /// - 未覆盖 `record_*` 类方法，保持默认的空实现（即什么也不做）。
    ///
    /// # 契约定义（What）
    /// - **前置条件**：调用方无需提前配置任何状态。
    /// - **后置条件**：返回的桩对象满足 Send + Sync，可安全地跨线程共享。
    ///
    /// # 风险提示（Trade-offs）
    /// - 与真实实现不同，该桩不会校验 `InstrumentDescriptor` 的合法性，
    ///   因此无法捕获指标命名错误；如需验证请替换为 RecordingMetrics。
    #[derive(Debug, Default, Clone, Copy)]
    pub struct NoopMetricsProvider;

    impl MetricsProvider for NoopMetricsProvider {
        fn counter(&self, _descriptor: &InstrumentDescriptor<'_>) -> Arc<dyn Counter> {
            Arc::new(NoopCounter)
        }

        fn gauge(&self, _descriptor: &InstrumentDescriptor<'_>) -> Arc<dyn Gauge> {
            Arc::new(NoopGauge)
        }

        fn histogram(&self, _descriptor: &InstrumentDescriptor<'_>) -> Arc<dyn Histogram> {
            Arc::new(NoopHistogram)
        }
    }

    /// `NoopCounter` 作为计数型指标的桩实现，忽略所有入参。
    ///
    /// # 角色定位（Why）
    /// - 允许测试验证“计数器被请求创建”这一事实，而无需关心底层累加逻辑。
    ///
    /// # 执行逻辑（How）
    /// - `add` 方法直接返回，不会更新任何内部状态。
    ///
    /// # 契约约束（What）
    /// - **输入参数**：与真实接口保持一致，用于维持类型检查。
    /// - **前置条件**：调用方可在任意线程调用，无需同步原语。
    /// - **后置条件**：无状态变化，也不会 panic。
    #[derive(Debug, Default, Clone, Copy)]
    pub struct NoopCounter;

    impl Counter for NoopCounter {
        fn add(&self, _value: u64, _attributes: AttributeSet<'_>) {}
    }

    /// `NoopGauge` 提供空操作的仪表实现，适用于需要验证 API 调用链的测试。
    ///
    /// # 设计思路（Why）
    /// - 某些测试只关心 `gauge` 接口是否被触发，而不关心读写值；该桩避免显式依赖真实度量后端。
    ///
    /// # 执行细节（How）
    /// - `set`、`increment`、`decrement` 均直接返回，不保留任何数值。
    ///
    /// # 契约声明（What）
    /// - **前置条件**：无需额外配置，方法可在任意顺序调用。
    /// - **后置条件**：不会修改共享状态，也不会触发并发冲突。
    #[derive(Debug, Default, Clone, Copy)]
    pub struct NoopGauge;

    impl Gauge for NoopGauge {
        fn set(&self, _value: f64, _attributes: AttributeSet<'_>) {}

        fn increment(&self, _delta: f64, _attributes: AttributeSet<'_>) {}

        fn decrement(&self, _delta: f64, _attributes: AttributeSet<'_>) {}
    }

    /// `NoopHistogram` 提供分布型指标的空实现。
    ///
    /// # 设计目的（Why）
    /// - 覆盖框架中需要注入 `Histogram` 的代码路径，确保 API 调用合法而不触发实际记录。
    ///
    /// # 实现概述（How）
    /// - `record` 方法直接返回，忽略传入的样本值与属性集合。
    ///
    /// # 契约说明（What）
    /// - **输入**：接受任意浮点值与属性，确保与真实接口兼容。
    /// - **前置条件**：无需任何初始化。
    /// - **后置条件**：不会生成桶统计，也不会影响后续调用。
    #[derive(Debug, Default, Clone, Copy)]
    pub struct NoopHistogram;

    impl Histogram for NoopHistogram {
        fn record(&self, _value: f64, _attributes: AttributeSet<'_>) {}
    }

    /// `NoopLogger` 将日志调用吞掉，常用于需要注入 `Logger` 的测试场景。
    ///
    /// # 设计背景（Why）
    /// - 合约测试关注逻辑分支而非日志输出，使用空实现避免污染测试控制台。
    ///
    /// # 实现方式（How）
    /// - `log` 方法直接返回，不进行格式化或输出。
    ///
    /// # 契约约束（What）
    /// - **前置条件**：调用方可多次复用同一实例，无需初始化。
    /// - **后置条件**：不会分配额外内存，也不会阻塞线程。
    #[derive(Debug, Default, Clone, Copy)]
    pub struct NoopLogger;

    impl Logger for NoopLogger {
        fn log(&self, _record: &LogRecord<'_>) {}
    }

    /// 快速获取共享的度量桩对象，减少在测试中手动包裹 `Arc` 的样板代码。
    ///
    /// # 使用指南（How）
    /// - 在需要 `Arc<dyn MetricsProvider>` 的场景调用本函数，例如 `let metrics = shared_metrics_provider();`。
    ///
    /// # 契约描述（What）
    /// - **返回值**：`Arc<dyn MetricsProvider>`，内部持有 [`NoopMetricsProvider`]。
    /// - **后置条件**：返回的 `Arc` 可安全克隆并跨线程共享。
    pub fn shared_metrics_provider() -> Arc<dyn MetricsProvider> {
        Arc::new(NoopMetricsProvider)
    }

    /// 基于 `Arc` 组合的静态 Facade 实现，仅用于测试装配流程。
    ///
    /// # 设计说明（Why）
    /// - 随着 `DefaultObservabilityFacade` 迁移至 `spark-otel`，核心测试仍需一个
    ///   轻量的 Facade 来拼装日志、指标与运维事件句柄；
    /// - 在契约测试中频繁手写 Facade 会造成样板代码和易错点，统一封装可提升维护性。
    ///
    /// # 执行逻辑（How）
    /// - 结构体内部缓存传入的 `Arc` 与健康探针集合，所有 Trait 方法均简单克隆或引用；
    /// - `new` 构造函数保持与旧实现一致的字段语义，便于迁移。
    ///
    /// # 契约约束（What）
    /// - **前置条件**：传入的句柄必须满足 `Send + Sync + 'static`；
    /// - **后置条件**：实现 [`ObservabilityFacade`]，适合作为
    ///   [`crate::runtime::CoreServices::with_observability_facade`] 的便捷输入；
    /// - **风险提示**：仅用于测试环境，不会执行能力探针或懒加载策略。
    #[derive(Clone)]
    pub struct StaticObservabilityFacade {
        logger: Arc<dyn Logger>,
        metrics: Arc<dyn MetricsProvider>,
        ops_bus: Arc<dyn OpsEventBus>,
        health_checks: HealthChecks,
    }

    impl StaticObservabilityFacade {
        /// 以静态句柄集合构造 Facade。
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

    impl ObservabilityFacade for StaticObservabilityFacade {
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
}
