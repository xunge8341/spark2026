//! 安全契约模块，汇集身份、凭证、密钥检索、策略声明与握手协商的统一接口。
//!
//! # 模块边界（Why）
//! - **行业吸收**：融合 SPIFFE/SPIRE、Envoy SDS、Kubernetes RBAC、AWS IAM 以及 Istio 安全策略的设计经验，形成跨协议的安全抽象。
//! - **科研前沿**：引入零信任网络（Zero Trust Networking）、可证明安全通道协商与属性基访问控制（ABAC）的建模方式，便于未来扩展形式化验证。
//! - **架构定位**：该模块提供契约与数据结构，不承担具体加密实现，确保核心 crate 在 `no_std + alloc` 环境下也能使用。
//!
//! # 子模块（What）
//! - [`identity`]：身份描述与证明材料的标准化封装。
//! - [`credential`]：凭证生命周期与载荷的抽象。
//! - [`keystore`]：密钥与机密数据的拉取契约，兼容外部密钥管理系统。
//! - [`negotiation`]：安全协商流程的意图化建模，支持多种安全协议的优雅降级。
//! - [`policy`]：安全策略、主体/资源匹配及执行效果的表达。
//!
//! # 使用约束（Contract）
//! - 仅定义 API 契约，不内置任何默认实现，调用者需结合具体后端（如 SDS、Vault）实现。
//! - 所有公开结构均可序列化/传输，建议结合 `serde` 等库在上层进行编解码。

pub mod class;
pub mod credential;
pub mod identity;
pub mod keystore;
pub mod negotiation;
pub mod policy;

pub use class::SecurityClass;
pub use credential::{
    Credential, CredentialDescriptor, CredentialMaterial, CredentialScope, CredentialState,
};
pub use identity::{IdentityDescriptor, IdentityKind, IdentityProof};
pub use keystore::{
    KeyMaterial, KeyPurpose, KeyRequest, KeyResponse, KeyRetrievalError, KeySource,
};
pub use negotiation::{
    NegotiationContext, NegotiationError, NegotiationOutcome, NegotiationResult,
    SecurityNegotiationPlan, SecurityNegotiator, SecurityProtocol, SecurityProtocolOffer,
};
pub use policy::{
    PolicyAttachment, PolicyEffect, PolicyRule, ResourcePattern, SecurityPolicy, SubjectMatcher,
};
