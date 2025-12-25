use crate::context::Context;
use crate::data_plane::transport::intent::{ConnectionIntent, QualityOfService, SecurityMode};

use super::{RouteCatalog, RouteMetadata, RoutePattern};

/// 表达调用方的路由意图，包含目标模式与偏好参数。
///
/// # 教案级说明
/// - **意图 (Why)**：将请求目标、QoS、安全偏好与静态标签集中存放，便于路由器统一读取；
/// - **契约 (What)**：必须提供合法的 [`RoutePattern`]；元数据、QoS、安全偏好均为可选增强信息；
/// - **执行逻辑 (How)**：通过 Builder 风格方法补充元数据或偏好，内部保持不可变以方便并发共享；
/// - **权衡 (Trade-offs)**：为避免热路径克隆，`expected_qos` 与 `security_preference` 采用 `Option` 引用/拷贝形式，
///   调用方在需要持久化时可自行克隆。
#[derive(Clone, Debug)]
pub struct RoutingIntent {
    target: RoutePattern,
    preferred_metadata: RouteMetadata,
    expected_qos: Option<QualityOfService>,
    security_preference: Option<SecurityMode>,
}

impl RoutingIntent {
    /// 基于目标模式构造意图，默认无额外偏好。
    pub fn new(target: RoutePattern) -> Self {
        Self {
            target,
            preferred_metadata: RouteMetadata::new(),
            expected_qos: None,
            security_preference: None,
        }
    }

    /// 附加首选元数据，后续可由路由器与动态标签合并。
    pub fn with_metadata(mut self, metadata: RouteMetadata) -> Self {
        self.preferred_metadata = metadata;
        self
    }

    /// 声明期望的 QoS 等级。
    pub fn with_expected_qos(mut self, qos: QualityOfService) -> Self {
        self.expected_qos = Some(qos);
        self
    }

    /// 声明安全偏好。
    pub fn with_security_preference(mut self, security: SecurityMode) -> Self {
        self.security_preference = Some(security);
        self
    }

    /// 读取目标路由模式。
    pub fn target(&self) -> &RoutePattern {
        &self.target
    }

    /// 读取首选元数据。
    pub fn preferred_metadata(&self) -> &RouteMetadata {
        &self.preferred_metadata
    }

    /// 读取期望 QoS。
    pub fn expected_qos(&self) -> Option<QualityOfService> {
        self.expected_qos
    }

    /// 读取安全偏好。
    pub fn security_preference(&self) -> Option<&SecurityMode> {
        self.security_preference.as_ref()
    }
}

/// 路由判定所需的上下文聚合，保持对调用上下文与请求体的借用。
///
/// # 教案级说明
/// - **意图 (Why)**：在路由判定过程中同时读取调用上下文、请求消息、意图与动态标签，避免重复查找；
/// - **契约 (What)**：持有对 `Context`、消息体、意图、连接意图（可选）、动态元数据与目录快照的借用；
/// - **设计 (How)**：采用生命周期参数确保借用合法，避免在无锁读路径上产生多余拷贝；
/// - **风险 (Trade-offs)**：结构体仅提供只读访问，若需要修改元数据应在构造前完成。
#[derive(Clone, Debug)]
pub struct RoutingContext<'a, M> {
    execution: Context<'a>,
    message: &'a M,
    intent: &'a RoutingIntent,
    connection: Option<&'a ConnectionIntent>,
    dynamic_metadata: &'a RouteMetadata,
    snapshot: RoutingSnapshot<'a>,
}

impl<'a, M> RoutingContext<'a, M> {
    /// 聚合路由判定所需的所有借用。
    pub fn new(
        execution: Context<'a>,
        message: &'a M,
        intent: &'a RoutingIntent,
        connection: Option<&'a ConnectionIntent>,
        dynamic_metadata: &'a RouteMetadata,
        snapshot: RoutingSnapshot<'a>,
    ) -> Self {
        Self {
            execution,
            message,
            intent,
            connection,
            dynamic_metadata,
            snapshot,
        }
    }

    /// 访问执行上下文视图。
    pub fn execution(&self) -> &Context<'a> {
        &self.execution
    }

    /// 访问原始消息。
    pub fn message(&self) -> &M {
        self.message
    }

    /// 访问路由意图。
    pub fn intent(&self) -> &RoutingIntent {
        self.intent
    }

    /// 访问可选连接意图。
    pub fn connection(&self) -> Option<&ConnectionIntent> {
        self.connection
    }

    /// 访问动态元数据借用。
    pub fn dynamic_metadata(&self) -> &RouteMetadata {
        self.dynamic_metadata
    }

    /// 访问目录快照。
    pub fn snapshot(&self) -> &RoutingSnapshot<'a> {
        &self.snapshot
    }
}

/// 路由目录快照，包含目录引用与修订号。
#[derive(Clone, Copy, Debug)]
pub struct RoutingSnapshot<'a> {
    catalog: &'a RouteCatalog,
    revision: u64,
}

impl<'a> RoutingSnapshot<'a> {
    /// 构造快照。
    pub fn new(catalog: &'a RouteCatalog, revision: u64) -> Self {
        Self { catalog, revision }
    }

    /// 获取目录引用。
    pub fn catalog(&self) -> &RouteCatalog {
        self.catalog
    }

    /// 返回修订号。
    pub fn revision(&self) -> u64 {
        self.revision
    }
}
