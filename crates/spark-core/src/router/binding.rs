use alloc::borrow::Cow;
use alloc::vec::Vec;

use crate::data_plane::transport::intent::{QualityOfService, SecurityMode};
use crate::{Result, SparkError, buffer::PipelineMessage, service::BoxService};

use super::{RouteDescriptor, RouteId, RouteMetadata, RoutePattern};

/// `DynRouter` 定义对象层路由器的最小对象安全接口。
///
/// # 教案级说明
/// - **意图 (Why)**：以对象安全方式暴露路由能力，便于宿主在运行时注入动态实现或热替换。`
/// - **契约 (What)**：接收 [`RoutingContext`](super::context::RoutingContext) 并返回 [`RouteDecisionObject`]；
///   错误使用 [`RouteError`] 表达标准化的路由失败语义。
/// - **定位 (Where)**：处于数据平面 L2 层次，被 `spark-router` 的 `ApplicationRouter` 直接调用。
/// - **设计 (How)**：仅包含核心判定与快照接口，避免耦合具体存储/策略实现。
/// - **权衡 (Trade-offs)**：接口保持精简，复杂策略交由上层组合；以 `alloc` 为前提，不支持无堆环境。
pub trait DynRouter: Send + Sync + 'static {
    /// 基于上下文执行一次路由判定。
    #[allow(clippy::result_large_err)]
    fn route_dyn(
        &self,
        context: super::context::RoutingContext<'_, PipelineMessage>,
    ) -> Result<RouteDecisionObject, RouteError<SparkError>>;

    /// 返回只读路由目录与修订号快照。
    fn snapshot(&self) -> super::context::RoutingSnapshot<'_>;

    /// 在装载前对路由描述执行轻量校验，默认返回空警告集合。
    fn validate(&self, _descriptor: &RouteDescriptor) -> RouteValidation {
        RouteValidation::new()
    }
}

/// 路由绑定对象，描述命中后的服务、元数据及附加约束。
///
/// # 教案级说明
/// - **意图 (Why)**：统一封装路由决策产物，便于调用方在对象层直接消费；
/// - **契约 (What)**：包含稳定的 [`RouteId`]、新建的 [`BoxService`] 以及合并后的 [`RouteMetadata`]；
///   同时可选携带 QoS 与安全偏好，为后续传输/策略引擎提供输入。
/// - **执行逻辑 (How)**：通过 [`Self::new`] 聚合字段；调用方可使用只读访问器避免不必要的克隆。
/// - **风险提示 (Trade-offs)**：结构体克隆成本与字段线性相关，若在热路径大量复制建议复用引用。
#[derive(Clone, Debug)]
pub struct RouteBindingObject {
    id: RouteId,
    service: BoxService,
    metadata: RouteMetadata,
    expected_qos: Option<QualityOfService>,
    security_preference: Option<SecurityMode>,
}

impl RouteBindingObject {
    /// 构造新的路由绑定。
    pub fn new(
        id: RouteId,
        service: BoxService,
        metadata: RouteMetadata,
        expected_qos: Option<QualityOfService>,
        security_preference: Option<SecurityMode>,
    ) -> Self {
        Self {
            id,
            service,
            metadata,
            expected_qos,
            security_preference,
        }
    }

    /// 访问稳定路由 ID。
    pub fn id(&self) -> &RouteId {
        &self.id
    }

    /// 访问对象层服务实例。
    pub fn service(&self) -> &BoxService {
        &self.service
    }

    /// 获取合并后的元数据视图。
    pub fn metadata(&self) -> &RouteMetadata {
        &self.metadata
    }

    /// 获取预期的 QoS 偏好。
    pub fn expected_qos(&self) -> Option<QualityOfService> {
        self.expected_qos
    }

    /// 获取安全偏好声明。
    pub fn security_preference(&self) -> Option<&SecurityMode> {
        self.security_preference.as_ref()
    }
}

/// 路由决策对象，包含绑定与附加警告。
///
/// # 教案级说明
/// - **意图 (Why)**：在不破坏路由结果的前提下，向上游报告策略或匹配过程中的非致命问题；
/// - **契约 (What)**：必须持有一个 [`RouteBindingObject`]；可选的 `warnings` 用于携带诊断文本；
/// - **使用方式 (How)**：调用方通过访问器读取绑定与警告列表，随后负责驱动对象层服务执行。
#[derive(Clone, Debug)]
pub struct RouteDecisionObject {
    binding: RouteBindingObject,
    warnings: Vec<Cow<'static, str>>,
}

impl RouteDecisionObject {
    /// 聚合绑定结果与警告信息。
    pub fn new(binding: RouteBindingObject, warnings: Vec<Cow<'static, str>>) -> Self {
        Self { binding, warnings }
    }

    /// 读取命中的绑定对象。
    pub fn binding(&self) -> &RouteBindingObject {
        &self.binding
    }

    /// 访问警告集合，用于日志或指标输出。
    pub fn warnings(&self) -> &Vec<Cow<'static, str>> {
        &self.warnings
    }
}

/// 路由校验结果，当前仅承载警告列表。
#[derive(Clone, Debug, Default)]
pub struct RouteValidation {
    warnings: Vec<Cow<'static, str>>,
}

impl RouteValidation {
    /// 创建空的校验结果。
    pub fn new() -> Self {
        Self {
            warnings: Vec::new(),
        }
    }

    /// 访问校验警告集合。
    pub fn warnings(&self) -> &Vec<Cow<'static, str>> {
        &self.warnings
    }
}

/// 路由错误枚举，标准化路由失败语义。
#[derive(Debug)]
#[non_exhaustive]
pub enum RouteError<E> {
    /// 未找到匹配路由，附带原始意图与合并后的元数据。
    NotFound {
        pattern: RoutePattern,
        metadata: RouteMetadata,
    },
    /// 路由策略拒绝，返回拒绝原因。
    PolicyDenied { reason: Cow<'static, str> },
    /// 目标服务不可用，通常表示 `ServiceFactory` 构建失败或健康检查不通过。
    ServiceUnavailable { id: RouteId, source: SparkError },
    /// 内部错误，承载底层异常。
    Internal(E),
    /// 版本冲突或协议不兼容，留作扩展。
    VersionConflict {
        expected: Cow<'static, str>,
        actual: Cow<'static, str>,
    },
}

impl<E> core::fmt::Display for RouteError<E>
where
    E: core::fmt::Display,
{
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            RouteError::NotFound { pattern, .. } => {
                write!(f, "route pattern `{pattern:?}` not found")
            }
            RouteError::PolicyDenied { reason } => write!(f, "route denied: {reason}"),
            RouteError::ServiceUnavailable { id, .. } => {
                write!(f, "service bound to `{id:?}` is unavailable")
            }
            RouteError::Internal(inner) => write!(f, "internal routing error: {inner}"),
            RouteError::VersionConflict { expected, actual } => {
                write!(
                    f,
                    "router version conflict: expect {expected}, got {actual}"
                )
            }
        }
    }
}

impl<E> From<E> for RouteError<E> {
    fn from(source: E) -> Self {
        RouteError::Internal(source)
    }
}
