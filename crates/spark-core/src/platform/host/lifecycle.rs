use crate::host::context::HostContext;
use crate::{Error, sealed::Sealed};
use alloc::string::String;

/// 宿主生命周期阶段。
///
/// # 背景（Why）
/// - 汇总 Kubernetes Pod Phase、Envoy Warmup 以及 Dapr Runtime 生命周期钩子的最佳实践。
/// - 统一阶段定义有助于组件实现延迟加载、灰度发布等策略。
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum StartupPhase {
    /// 宿主刚启动，尚未暴露网络端口。
    Bootstrapping,
    /// 宿主已加载配置但尚未对外服务。
    Warming,
    /// 宿主对外可用。
    Ready,
}

/// 宿主停止的原因枚举。
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum ShutdownReason {
    /// 正常滚动升级或运维。
    Graceful,
    /// 宿主发现不可恢复的错误。
    Failure(String),
    /// 资源回收或停机。
    Maintenance,
}

/// 宿主生命周期回调。
///
/// # 背景（Why）
/// - 借鉴 Envoy Lifecycle Callbacks、Dapr Host Hooks 与 Wasmtime Component Hooks，让组件能够在关键时刻获取上下文。
///
/// # 契约（What）
/// - `Error`：生命周期回调失败时的错误类型，需实现 `crate::Error`。
/// - `on_starting`：宿主启动时触发，传递启动阶段与上下文。
/// - `on_ready`：宿主完成所有初始化，准备对外服务。
/// - `on_shutdown`：宿主关闭时触发，附带关闭原因。
///
/// # 前后置条件（Contract）
/// - **前置条件**：宿主调用回调时必须保证 `HostContext` 已经稳定，不会在回调过程中变化。
/// - **后置条件**：若回调返回错误，宿主可选择终止启动或进入降级模式，具体策略由宿主决定。
///
/// # 风险提示（Trade-offs）
/// - 生命周期回调设计为同步方法，以保持调用顺序的可预测性；若回调逻辑耗时，应由实现方自行开启异步流程，避免阻塞宿主线程。
pub trait HostLifecycle: Sealed {
    /// 错误类型。
    type Error: Error;

    /// 通知组件宿主正在启动。
    fn on_starting(&self, ctx: &HostContext, phase: StartupPhase)
    -> crate::Result<(), Self::Error>;

    /// 通知组件宿主已经就绪。
    fn on_ready(&self, ctx: &HostContext) -> crate::Result<(), Self::Error>;

    /// 通知组件宿主准备关闭。
    fn on_shutdown(
        &self,
        ctx: &HostContext,
        reason: ShutdownReason,
    ) -> crate::Result<(), Self::Error>;
}
