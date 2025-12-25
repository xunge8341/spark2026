//! Pipeline 工厂模块：统一泛型层与对象层的装配入口，并在单文件中提供双向适配器。
//!
//! # 模块职责（Why）
//! - **统一语义**：早期 `pipeline::traits` 将泛型与对象接口拆为子模块，随着体系稳定，应收敛为单一入口，降低路径分叉。
//! - **扩展兼容**：继续提供泛型零虚分派与对象层插件化能力，满足“同一实现双层语义等价”的 T05 目标。
//! - **契约聚合**：集中描述 Pipeline 构建的输入、输出与适配逻辑，方便架构评审从一个文件把握全部约束。
//!
//! # 使用方式（How）
//! 1. 业务若能在编译期决定控制器类型，直接实现 [`PipelineFactory`]，调用 [`PipelineFactory::build`].
//! 2. 需运行时注册或脚本注入时，实现 [`DynPipelineFactory`] 并返回 [`PipelineHandle`]。
//! 3. 使用 [`PipelineFactoryObject`] 或 [`DynPipelineFactoryAdapter`] 在两层接口之间互转，确保复用已实现逻辑。
//!
//! # 契约摘要（What）
//! - 所有工厂在 Pipeline 进入事件循环前完成构建，避免热路径初始化。
//! - 错误统一以 [`CoreError`] 返回，方便日志与诊断。
//! - 所有管线均需实现 [`Pipeline`]，并以 [`PipelineHandleId`] 作为 Handler 注册句柄。
//!
//! # 设计权衡（Trade-offs）
//! - 泛型层保留零虚分派优势，但要求调用方在编译期决策类型；对象层增加一次 `Arc` 包装换取运行时组合。
//! - 将双层接口放入同一文件，便于跨层重构，但文件体积更大；我们通过分节注释与统一导出结构保持可读性。

use alloc::{boxed::Box, sync::Arc};

use crate::{
    CoreError,
    pipeline::{Pipeline, PipelineHandleId},
    runtime::CoreServices,
    sealed::Sealed,
};

/// 泛型层的控制器工厂合约，提供零虚分派的装配路径。
///
/// # 设计初衷（Why）
/// - 将 Pipeline 控制面构造过程泛型化，允许在内建实现中完全依赖静态分派，避免热路径额外的虚表开销。
/// - 与对象层接口共享同一语义，方便在不牺牲性能的情况下与插件生态保持一致。
///
/// # 行为逻辑（How）
/// 1. 宿主在通道初始化阶段调用 [`build`](PipelineFactory::build)。
/// 2. 泛型实现根据 [`CoreServices`] 装配 Handler/PipelineInitializer，返回具体的 [`Pipeline`] 实例。
/// 3. 返回值由调用方直接持有，不经 `Box<dyn Pipeline>` 擦除，因此后续调用保持零虚分派。
///
/// # 契约说明（What）
/// - **关联类型**：[`Pipeline`](PipelineFactory::Pipeline) 必须实现 Pipeline 的控制器契约，且满足 `Send + Sync + 'static`。
/// - **输入参数**：[`CoreServices`] 聚合运行时依赖（调度、计时、监控等），实现者应在构造阶段借助该依赖注入资源。
/// - **前置条件**：调用发生在通道进入事件循环之前，避免在热路径执行昂贵初始化。
/// - **后置条件**：成功返回的控制器应立即可用，且具备线程安全性。
///
/// # 设计考量（Trade-offs）
/// - 泛型层避免 `Box` 分配，但意味着调用方需要在编译期获取具体类型；在需要运行时组合的场景，可改用对象层。
/// - 若控制器构造失败，必须返回结构化的 [`CoreError`]，以便宿主记录诊断信息。
pub trait PipelineFactory: Send + Sync + 'static + Sealed {
    /// 泛型层构造出的管线路径实现类型。
    type Pipeline: Pipeline<HandleId = PipelineHandleId>;

    /// 构建控制器并装配完整 Pipeline 链路。
    ///
    /// ## 输入（Parameters）
    /// - `core_services`: [`CoreServices`] 聚合运行时服务，包括调度、遥测、配置等能力，不能为空引用。
    ///
    /// ## 执行流程（Logic）
    /// - 实现者按需克隆或借用 `core_services` 中的资源；
    /// - 构造并返回满足 [`Pipeline`] 契约的类型；
    /// - 错误使用 [`CoreError`] 表达，以保持跨模块统一。
    ///
    /// ## 契约约束（Contract）
    /// - **前置条件**：调用方需确保 Pipeline 尚未进入事件循环；
    /// - **后置条件**：若返回 `Ok(controller)`，则控制器可立即用于注册 Handler/PipelineInitializer，且必须是线程安全的。
    fn build(&self, core_services: &CoreServices) -> crate::Result<Self::Pipeline, CoreError>;
}

/// 对象层控制器工厂接口，面向插件系统与脚本运行时。
///
/// # 设计动机（Why）
/// - 运行时注入的 Pipeline 扩展需要以 `dyn Trait` 形式注册，此接口提供统一入口。
/// - 与泛型层的 [`PipelineFactory`] 完全对齐，确保语义等价，可互相适配。
///
/// # 行为逻辑（How）
/// - [`DynPipelineFactory::build_dyn`] 接收 [`CoreServices`]，内部通常调用泛型实现再做类型擦除；
/// - 返回值为 [`PipelineHandle`]，封装 `Arc<dyn Pipeline<HandleId = PipelineHandleId>>`，方便在多线程环境复用；
/// - 适配器 [`PipelineFactoryObject`] 负责将泛型实现桥接到对象层。
///
/// # 契约说明（What）
/// - **输入**：必须在 Pipeline 初始化阶段调用，遵守与泛型层一致的前置条件；
/// - **输出**：[`PipelineHandle`] 持有引用计数的控制器实例，调用方可安全克隆共享；
/// - **错误处理**：出现构造错误时返回 [`CoreError`]，调用方应记录并中止建链流程。
///
/// # 风险提示（Trade-offs）
/// - 相较泛型层，多一次 `Arc` 克隆与虚表跳转；若可静态确定类型仍推荐使用泛型接口。
pub trait DynPipelineFactory: Send + Sync + Sealed {
    /// 构建对象层控制器句柄。
    ///
    /// ## 输入（Parameters）
    /// - `core_services`: [`CoreServices`] 的只读引用，提供 Handler/PipelineInitializer 所需的共享运行时。
    ///
    /// ## 执行流程（Logic）
    /// - 实现者装配底层控制器；
    /// - 以 [`PipelineHandle`] 包裹以确保对象安全；
    /// - 错误以 [`CoreError`] 形式返回。
    ///
    /// ## 契约（Contract）
    /// - **前置条件**：调用发生在 Pipeline 事件循环启动前；
    /// - **后置条件**：若成功返回句柄，则内部控制器应符合 [`Pipeline`] 线程安全要求。
    fn build_dyn(&self, core_services: &CoreServices) -> crate::Result<PipelineHandle, CoreError>;
}

/// `PipelineHandle` 持有对象层 Pipeline，实现 [`Pipeline`] 以便在泛型上下文复用。
///
/// # 设计目标（Why）
/// - 将 `Arc<dyn Pipeline>` 封装为可克隆的轻量句柄，避免外部模块直接依赖对象安全细节。
/// - 使得对象层返回值可以在泛型上下文中再次被视作控制器，实现“对象层到泛型层”的逆向适配。
///
/// # 运行机制（How）
/// - 内部存储 `Arc<dyn Pipeline<HandleId = PipelineHandleId>>`；
/// - 对外暴露 [`Pipeline`] Trait 的全部方法，简单转发给内部实现；
/// - 通过 [`PipelineHandle::from_pipeline`] 将泛型 Pipeline 转换为对象层句柄。
///
/// # 契约（What）
/// - **前置条件**：构造时需提供满足 [`Pipeline`] 的实例或对象层引用；
/// - **后置条件**：句柄本身实现 [`Pipeline`]，可直接参与 Pipeline 注册与事件触发。
///
/// # 注意事项（Trade-offs）
/// - 多次克隆带来 `Arc` 的原子引用计数开销；
/// - 透传实现意味着不会额外校验参数，调用方需确保底层控制器自身健壮。
#[derive(Clone)]
pub struct PipelineHandle {
    inner: Arc<dyn Pipeline<HandleId = PipelineHandleId>>,
}

impl PipelineHandle {
    /// 以 `Arc<dyn Pipeline<HandleId = PipelineHandleId>>` 包裹现有 Pipeline。
    ///
    /// # 契约说明
    /// - **输入**：`inner` 为已初始化的对象层 Pipeline，需满足线程安全；
    /// - **后置条件**：返回的句柄可被克隆并在多个线程共享。
    pub fn new(inner: Arc<dyn Pipeline<HandleId = PipelineHandleId>>) -> Self {
        Self { inner }
    }

    /// 从泛型 Pipeline 创建对象层句柄。
    ///
    /// # 契约说明
    /// - **输入**：`pipeline` 必须实现 [`Pipeline`], 并遵循 `HandleId = PipelineHandleId`。
    /// - **执行**：内部将其封装为 `Arc` 以实现共享。
    /// - **后置条件**：得到的句柄可在对象层与泛型层之间自由传递。
    pub fn from_pipeline<C>(pipeline: C) -> Self
    where
        C: Pipeline<HandleId = PipelineHandleId>,
    {
        Self {
            inner: Arc::new(pipeline),
        }
    }

    /// 以 trait 对象形式访问内部控制器。
    ///
    /// # 注意事项
    /// - **用途**：在需要对象安全引用的场景复用底层控制器；
    /// - **返回值**：生命周期与 `self` 一致的 `&dyn Pipeline` 引用。
    pub fn as_dyn(&self) -> &dyn Pipeline<HandleId = PipelineHandleId> {
        &*self.inner
    }
}

impl Pipeline for PipelineHandle {
    type HandleId = PipelineHandleId;

    fn register_inbound_handler(
        &self,
        label: &str,
        handler: Box<dyn crate::pipeline::InboundHandler>,
    ) {
        self.inner.register_inbound_handler(label, handler)
    }

    fn register_inbound_handler_static(
        &self,
        label: &str,
        handler: &'static dyn crate::pipeline::InboundHandler,
    ) {
        self.inner.register_inbound_handler_static(label, handler)
    }

    fn register_outbound_handler(
        &self,
        label: &str,
        handler: Box<dyn crate::pipeline::OutboundHandler>,
    ) {
        self.inner.register_outbound_handler(label, handler)
    }

    fn register_outbound_handler_static(
        &self,
        label: &str,
        handler: &'static dyn crate::pipeline::OutboundHandler,
    ) {
        self.inner.register_outbound_handler_static(label, handler)
    }

    fn install_middleware(
        &self,
        initializer: &dyn crate::pipeline::PipelineInitializer,
        services: &crate::runtime::CoreServices,
    ) -> crate::Result<(), CoreError> {
        self.inner.install_middleware(initializer, services)
    }

    fn emit_channel_activated(&self) {
        self.inner.emit_channel_activated()
    }

    fn emit_read(&self, msg: crate::buffer::PipelineMessage) {
        self.inner.emit_read(msg)
    }

    fn emit_read_completed(&self) {
        self.inner.emit_read_completed()
    }

    fn emit_writability_changed(&self, is_writable: bool) {
        self.inner.emit_writability_changed(is_writable)
    }

    fn emit_user_event(&self, event: crate::observability::CoreUserEvent) {
        self.inner.emit_user_event(event)
    }

    fn emit_exception(&self, error: CoreError) {
        self.inner.emit_exception(error)
    }

    fn emit_channel_deactivated(&self) {
        self.inner.emit_channel_deactivated()
    }

    fn registry(&self) -> &dyn crate::pipeline::HandlerRegistry {
        self.inner.registry()
    }

    fn add_handler_after(
        &self,
        anchor: Self::HandleId,
        label: &str,
        handler: Arc<dyn crate::pipeline::controller::Handler>,
    ) -> Self::HandleId {
        self.inner.add_handler_after(anchor, label, handler)
    }

    fn remove_handler(&self, handle: Self::HandleId) -> bool {
        self.inner.remove_handler(handle)
    }

    fn replace_handler(
        &self,
        handle: Self::HandleId,
        handler: Arc<dyn crate::pipeline::controller::Handler>,
    ) -> bool {
        self.inner.replace_handler(handle, handler)
    }

    fn epoch(&self) -> u64 {
        self.inner.epoch()
    }
}

/// 将泛型控制器工厂适配为对象层实现。
///
/// # 设计动机（Why）
/// - 提供“泛型 -> 对象”桥接，避免为插件系统重复实现相同逻辑。
///
/// # 执行流程（How）
/// - 保存泛型实现 `inner`；
/// - 在 [`DynPipelineFactory::build_dyn`] 中直接调用 [`PipelineFactory::build`]，并将结果封装为 [`PipelineHandle`]。
///
/// # 契约（What）
/// - **前置条件**：`inner` 必须实现 [`PipelineFactory`]；
/// - **后置条件**：构建成功即返回对象层句柄，可在任意需要 `dyn` 的上下文中使用。
pub struct PipelineFactoryObject<F>
where
    F: PipelineFactory,
{
    inner: F,
}

impl<F> PipelineFactoryObject<F>
where
    F: PipelineFactory,
{
    /// 创建新的适配器实例。
    ///
    /// - **输入**：泛型实现 `inner`；
    /// - **后置条件**：返回的对象可作为 [`DynPipelineFactory`] 使用。
    pub fn new(inner: F) -> Self {
        Self { inner }
    }

    /// 访问内部泛型实现，便于测试或高级自定义。
    ///
    /// - **注意**：该方法会取得所有权，调用后适配器失效；
    /// - **返回值**：原始泛型实现 `F`。
    pub fn into_inner(self) -> F {
        self.inner
    }
}

impl<F> DynPipelineFactory for PipelineFactoryObject<F>
where
    F: PipelineFactory,
{
    fn build_dyn(&self, core_services: &CoreServices) -> crate::Result<PipelineHandle, CoreError> {
        let pipeline = self.inner.build(core_services)?;
        Ok(PipelineHandle::from_pipeline(pipeline))
    }
}

/// 将对象层工厂重新包装为泛型接口，便于在需要静态分派的场景复用对象实现。
///
/// # 设计动机（Why）
/// - 在测试或特殊部署中，调用方可能仅持有 `Arc<dyn DynPipelineFactory>`；该适配器允许其在泛型上下文继续沿用。
///
/// # 行为逻辑（How）
/// - 保存对象层工厂引用计数；
/// - [`PipelineFactory::build`] 直接委托给 [`DynPipelineFactory::build_dyn`] 并返回 [`PipelineHandle`]。
///
/// # 契约（What）
/// - **前置条件**：传入的工厂必须对象安全且线程安全；
/// - **后置条件**：泛型接口返回 [`PipelineHandle`]，可按泛型语义继续使用。
pub struct DynPipelineFactoryAdapter {
    inner: Arc<dyn DynPipelineFactory>,
}

impl DynPipelineFactoryAdapter {
    /// 以对象层工厂构造适配器。
    ///
    /// - **输入**：`inner` 为满足 [`DynPipelineFactory`] 的引用计数对象；
    /// - **后置条件**：返回的适配器实现 [`PipelineFactory`]，可用于静态上下文。
    pub fn new(inner: Arc<dyn DynPipelineFactory>) -> Self {
        Self { inner }
    }
}

impl PipelineFactory for DynPipelineFactoryAdapter {
    type Pipeline = PipelineHandle;

    fn build(&self, core_services: &CoreServices) -> crate::Result<Self::Pipeline, CoreError> {
        self.inner.build_dyn(core_services)
    }
}
