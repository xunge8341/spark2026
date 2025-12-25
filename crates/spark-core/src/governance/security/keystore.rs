use core::fmt;
use core::time::Duration;

use alloc::string::String;
use alloc::vec::Vec;

use super::credential::{Credential, CredentialScope};
use super::identity::IdentityDescriptor;
use crate::sealed::Sealed;

/// 密钥用途枚举，统一 KMS/SDS/密钥代理的请求语义。
///
/// # 设计来源（Why）
/// - **行业输入**：综合 AWS KMS `KeyUsage`, HashiCorp Vault Transit API, Istio SDS 需求，将用途划分为握手、消息签名、静态数据加密等。
/// - **科研启发**：支持为后续的属性基加密、机密计算等扩展预留 `Custom` 变体。
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum KeyPurpose {
    /// 传输层握手密钥，如 TLS 证书、PSK。
    TransportHandshake,
    /// 消息或请求级别签名。
    MessageIntegrity,
    /// 数据静态加密（Data-At-Rest）。
    DataEncryption,
    /// 远程证明或机密计算场景需要的密钥。
    RemoteAttestation,
    /// 自定义用途。
    ///
    /// # 实现责任 (Implementation Responsibility)
    /// - **命名约定**：结合组织前缀（如 `acme.post_quantum_transport`）与用途语义，确保跨租户唯一。
    /// - **错误处理**：密钥管理服务若不支持该用途，应返回
    ///   [`crate::error::codes::APP_UNAUTHORIZED`] 并记录告警或审计日志说明原因。
    /// - **禁止降级**：不可默认映射为其他已知用途或静默返回空结果，避免密钥误用。
    Custom(String),
}

/// 密钥请求意图，描述调用方需要拉取何种机密材料。
///
/// # 字段契约（What）
/// - `identity`：请求密钥的主体身份，用于审计与访问控制。
/// - `purpose`：密钥用途，驱动后端选择正确的密钥池或证书模版。
/// - `scope`：目标凭证适用范围，辅助确定 TTL 与续期策略。
/// - `audience`：目标受众，如服务名称或集群 ID，避免密钥被跨环境滥用。
/// - `prefer_cached`：指示是否可返回缓存结果。
/// - `version_hint`：调用方已持有的版本指纹，有助于增量更新。
///
/// # 前置条件
/// - 调用方必须事先经过授权策略允许，才能请求指定 `purpose` 对应的密钥。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KeyRequest {
    identity: IdentityDescriptor,
    purpose: KeyPurpose,
    scope: CredentialScope,
    audience: Option<String>,
    prefer_cached: bool,
    version_hint: Option<String>,
}

impl KeyRequest {
    /// 构建密钥请求。
    ///
    /// # 契约
    /// - **参数**：`identity` 与 `purpose` 必填；`scope` 指示预期使用层级。
    /// - **返回值**：`audience` 与 `version_hint` 默认为 `None`，`prefer_cached = true`。
    pub fn new(identity: IdentityDescriptor, purpose: KeyPurpose, scope: CredentialScope) -> Self {
        Self {
            identity,
            purpose,
            scope,
            audience: None,
            prefer_cached: true,
            version_hint: None,
        }
    }

    /// 指定密钥受众或绑定目标。
    ///
    /// # 设计考虑
    /// - 参考 Google Cloud IAM `audience`、JWT `aud` 字段，避免跨系统滥用。
    pub fn with_audience(mut self, audience: String) -> Self {
        self.audience = Some(audience);
        self
    }

    /// 指示跳过缓存，强制从源头拉取最新密钥。
    ///
    /// # 风险提示
    /// - 频繁跳过缓存可能导致 KMS/SDS 压力过大，建议仅在轮换或故障恢复时使用。
    pub fn prefer_fresh(mut self) -> Self {
        self.prefer_cached = false;
        self
    }

    /// 附加已有版本指纹，便于增量下发。
    pub fn with_version_hint(mut self, version_hint: String) -> Self {
        self.version_hint = Some(version_hint);
        self
    }

    /// 获取身份。
    pub fn identity(&self) -> &IdentityDescriptor {
        &self.identity
    }

    /// 获取用途。
    pub fn purpose(&self) -> &KeyPurpose {
        &self.purpose
    }

    /// 获取范围。
    pub fn scope(&self) -> CredentialScope {
        self.scope
    }

    /// 获取受众。
    pub fn audience(&self) -> Option<&String> {
        self.audience.as_ref()
    }

    /// 是否允许缓存命中。
    pub fn prefer_cached(&self) -> bool {
        self.prefer_cached
    }

    /// 获取版本指纹。
    pub fn version_hint(&self) -> Option<&String> {
        self.version_hint.as_ref()
    }
}

/// 密钥材料容器，兼顾证书、对称密钥以及硬件引用。
///
/// # 字段说明
/// - `format`：材料类型或编码，如 `pem`, `der`, `opaque-handle`。
/// - `payload`：密钥字节或引用标识。对于硬件引用，可存储句柄。
/// - `expires_in`：建议的过期时间，以相对时长表示。
/// - `renewable`：是否允许续期。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KeyMaterial {
    format: String,
    payload: Vec<u8>,
    expires_in: Option<Duration>,
    renewable: bool,
}

impl KeyMaterial {
    /// 构建密钥材料。
    ///
    /// # 契约
    /// - `format`：描述编码或来源，不能为空。
    /// - `payload`：密钥内容或引用。
    /// - `expires_in`：默认 `None`，表示由上层策略决定失效时间。
    pub fn new(format: String, payload: Vec<u8>) -> Self {
        Self {
            format,
            payload,
            expires_in: None,
            renewable: true,
        }
    }

    /// 指定相对过期时间。
    pub fn with_expires_in(mut self, duration: Duration) -> Self {
        self.expires_in = Some(duration);
        self
    }

    /// 标记不可续期。
    pub fn non_renewable(mut self) -> Self {
        self.renewable = false;
        self
    }

    /// 获取格式。
    pub fn format(&self) -> &str {
        &self.format
    }

    /// 获取载荷。
    pub fn payload(&self) -> &Vec<u8> {
        &self.payload
    }

    /// 获取过期时间。
    pub fn expires_in(&self) -> Option<Duration> {
        self.expires_in
    }

    /// 是否可续期。
    pub fn renewable(&self) -> bool {
        self.renewable
    }
}

/// 密钥响应，允许同时返回凭证与原始密钥材料。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KeyResponse {
    credential: Option<Credential>,
    materials: Vec<KeyMaterial>,
}

impl KeyResponse {
    /// 构建响应。
    ///
    /// # 契约
    /// - 默认不附带凭证，调用方可后续通过 [`Self::with_credential`] 添加。
    pub fn new(materials: Vec<KeyMaterial>) -> Self {
        Self {
            credential: None,
            materials,
        }
    }

    /// 附加完整凭证。
    ///
    /// # 用途
    /// - 适用于 SDS 直接下发 TLS 证书链 + 私钥的场景。
    pub fn with_credential(mut self, credential: Credential) -> Self {
        self.credential = Some(credential);
        self
    }

    /// 获取凭证。
    pub fn credential(&self) -> Option<&Credential> {
        self.credential.as_ref()
    }

    /// 获取密钥材料列表。
    pub fn materials(&self) -> &Vec<KeyMaterial> {
        &self.materials
    }
}

/// 密钥拉取错误。
#[derive(Debug)]
#[non_exhaustive]
pub enum KeyRetrievalError {
    /// 未授权访问。
    Unauthorized,
    /// 后端未找到目标密钥或凭证。
    NotFound,
    /// 后端暂时不可用，可重试。
    Unavailable,
    /// 请求格式不符合策略要求。
    InvalidRequest(String),
    /// 其他错误。
    Other(String),
}

impl fmt::Display for KeyRetrievalError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            KeyRetrievalError::Unauthorized => write!(f, "unauthorized"),
            KeyRetrievalError::NotFound => write!(f, "not found"),
            KeyRetrievalError::Unavailable => write!(f, "unavailable"),
            KeyRetrievalError::InvalidRequest(reason) => write!(f, "invalid request: {reason}"),
            KeyRetrievalError::Other(reason) => write!(f, "error: {reason}"),
        }
    }
}

impl crate::Error for KeyRetrievalError {
    #[allow(unused_parens)]
    fn source(&self) -> Option<&(dyn crate::Error + 'static)> {
        None
    }
}

/// 密钥来源契约，抽象 Envoy SDS、SPIFFE Workload API、Vault Agent 等组件的行为。
pub trait KeySource: Sealed {
    /// 获取指定主体的密钥或凭证。
    ///
    /// # 契约
    /// - **输入**：[`KeyRequest`]，包含主体、用途、范围等信息。
    /// - **返回**：[`KeyResponse`]，可能包含凭证与多个密钥材料。
    /// - **错误**：[`KeyRetrievalError`]，调用方需根据错误类型决定是否重试或降级。
    ///
    /// # 设计取舍（Trade-offs）
    /// - Trait 不强制异步接口，便于在 `no_std` 环境实现；异步场景可在上层通过 [`crate::future::BoxFuture`] 包装。
    /// - 返回值允许为空列表，表示后端拒绝或暂无法提供密钥。
    fn fetch(&self, request: &KeyRequest) -> crate::Result<KeyResponse, KeyRetrievalError>;
}
