use alloc::{borrow::Cow, boxed::Box, format};

use crate::{CoreError, runtime::CoreServices, sealed::Sealed};

use super::channel::Channel;

use super::handler::{self, InboundHandler, OutboundHandler};

/// 描述 PipelineInitializer 或 Handler 的元数据，辅助链路编排与可观测性。
///
/// # 设计背景（Why）
/// - 借鉴 OpenTelemetry Attributes、Envoy Filter Metadata、Tower Layer Describe API，帮助平台编排系统识别组件用途。
/// - 允许在科研场景中收集构件级别的性能指标或生成链路图谱。
///
/// # 契约说明（What）
/// - `name`：组件的稳定标识，建议使用 `vendor.component` 命名。
/// - `category`：可选分类（如 `security`、`codec`、`routing`）。
/// - `summary`：人类可读描述，便于平台 UI 展示。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InitializerDescriptor {
    name: Cow<'static, str>,
    category: Cow<'static, str>,
    summary: Cow<'static, str>,
}

impl InitializerDescriptor {
    /// 构造新的描述对象。
    pub fn new(
        name: impl Into<Cow<'static, str>>,
        category: impl Into<Cow<'static, str>>,
        summary: impl Into<Cow<'static, str>>,
    ) -> Self {
        Self {
            name: name.into(),
            category: category.into(),
            summary: summary.into(),
        }
    }

    /// 构造匿名描述，常用于测试或快速原型。
    pub fn anonymous(stage: impl Into<Cow<'static, str>>) -> Self {
        let stage = stage.into();
        Self {
            name: Cow::Owned(format!("anonymous.{}", stage)),
            category: Cow::Borrowed("unspecified"),
            summary: Cow::Owned(format!("auto-generated descriptor for {}", stage)),
        }
    }

    /// 获取名称。
    pub fn name(&self) -> &str {
        &self.name
    }

    /// 获取类别。
    pub fn category(&self) -> &str {
        &self.category
    }

    /// 获取摘要。
    pub fn summary(&self) -> &str {
        &self.summary
    }
}

/// PipelineInitializer 装配链路时的注册接口。
///
/// # 设计背景（Why）
/// - 模仿 Express/Koa 中间件的“下一步”模式与 Tower Layer 的 `Layer::layer` 概念，将 Handler 注册过程显式化。
///
/// # 契约说明（What）
/// - `register_inbound`：以名称注册入站 Handler，顺序即执行顺序。
/// - `register_outbound`：以名称注册出站 Handler，顺序即逆向执行顺序。
/// - `register_*_static`：为 `'static` 借用提供无复制入口，常用于全局单例；
/// - 实现方可在内部维护有序列表或拓扑结构（如 DAG）以支持复杂编排。
///
/// # 风险提示（Trade-offs）
/// - 若允许并行或条件执行，需在实现中扩展额外语义；此 Trait 专注于顺序链路的最小契约。
pub trait ChainBuilder: Sealed {
    /// 注册入站 Handler。
    fn register_inbound(&mut self, label: &str, handler: Box<dyn InboundHandler>);

    /// 以 `'static` 借用方式注册入站 Handler，默认桥接到拥有型入口。
    fn register_inbound_static(&mut self, label: &str, handler: &'static dyn InboundHandler) {
        self.register_inbound(label, handler::box_inbound_from_static(handler));
    }

    /// 注册出站 Handler。
    fn register_outbound(&mut self, label: &str, handler: Box<dyn OutboundHandler>);

    /// 以 `'static` 借用方式注册出站 Handler。
    fn register_outbound_static(&mut self, label: &str, handler: &'static dyn OutboundHandler) {
        self.register_outbound(label, handler::box_outbound_from_static(handler));
    }
}

/// PipelineInitializer 合约：以声明式方式将 Handler 注入 Pipeline。
///
/// # 契约维度速览
/// - **语义**：`configure` 接口负责描述 Handler 链路拓扑，`descriptor` 提供自描述元数据，保证热更新与 introspection 的一致性。
/// - **错误**：`configure` 返回 [`CoreError`]；常见错误码沿用 `pipeline.middleware_conflict`、`pipeline.middleware_init_failed`，以兼容既有监控指标。
/// - **并发**：PipelineInitializer 实例需 `Send + Sync`；`configure` 可能在多个线程同时调用，内部状态必须以 `Arc`/锁保护或保持无状态。
/// - **背压**：装配阶段的控制逻辑需遵守 [`BackpressureSignal`](crate::contract::BackpressureSignal) 语义，不在装配阶段自行短路背压。
/// - **超时**：若装配需要访问外部配置，应使用 `CoreServices::timer` 与 [`CallContext::deadline()`](crate::contract::CallContext::deadline) 控制超时，超时即返回错误。
/// - **取消**：当 Pipeline 关闭或部署回滚触发取消时，装配过程需检查取消标记并提前退出，避免遗留半初始化 Handler。
/// - **观测标签**：`descriptor` 返回的 `name`/`category`/`summary` 将作为统一的观测标签，建议遵循 `vendor.component` 命名规范。
/// - **示例(伪码)**：
///   ```text
///   initializer.configure(chain, channel, services)?
///   chain.register_inbound("authz", Box::new(AuthzHandler::new(policy)))
///   ```
///
/// # 设计背景（Why）
/// - 综合 Netty ChannelInitializer、Express Middleware、Envoy Filter、gRPC Interceptor、Tower Layer 的经验，通过 `configure` 方法实现可重入、可组合的链路装配。
/// - 支持科研场景：可在 `configure` 中注入测量 Handler 或模型驱动的策略。
///
/// # 契约说明（What）
/// - `descriptor`：返回组件元数据。
/// - `configure`：在给定 [`ChainBuilder`]、[`Channel`] 以及 [`CoreServices`] 环境下注册 Handler；
///   `Channel` 提供连接级元数据（如握手结果、地址），帮助初始化器完成针对性装配。
/// - `configure` 必须是幂等操作，以支持热更新或多次装配。
///
/// # 风险提示（Trade-offs）
/// - PipelineInitializer 不应在 `configure` 中执行阻塞操作；如需异步初始化，可将任务委托给 `CoreServices` 中的执行器。
/// - 若在 `configure` 中捕获状态，需确保其线程安全并避免循环依赖。
pub trait PipelineInitializer: Send + Sync + 'static + Sealed {
    /// 返回组件元数据。
    fn descriptor(&self) -> InitializerDescriptor;

    /// 在链路中注册 Handler。
    fn configure(
        &self,
        chain: &mut dyn ChainBuilder,
        channel: &dyn Channel,
        services: &CoreServices,
    ) -> crate::Result<(), CoreError>;
}
