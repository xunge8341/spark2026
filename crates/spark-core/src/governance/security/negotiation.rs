use alloc::string::String;
use alloc::vec::Vec;

use crate::sealed::Sealed;

use super::credential::Credential;
use super::identity::IdentityDescriptor;

/// 安全协议枚举，抽象主流互联互通方案。
///
/// # 设计依据（Why）
/// - **行业对标**：涵盖 mTLS、基于令牌的安全通道、Noise Framework、自定义安全插件等场景。
/// - **科研吸收**：预留远程证明（Remote Attestation）与量子安全算法的扩展点，满足前沿研究需求。
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum SecurityProtocol {
    /// mTLS 或 TLS1.3 双向认证。
    MutualTls,
    /// 单向 TLS，结合 OAuth2/OIDC/JWT 等令牌绑定。
    TokenBinding,
    /// 基于 Noise Protocol Framework 的会话密钥协商。
    NoiseHandshake,
    /// 远程证明或可信执行环境验证。
    RemoteAttestation,
    /// 量子安全或后量子握手算法。
    PostQuantum,
    /// 自定义协议，通过名称与参数说明。
    ///
    /// # 实现责任 (Implementation Responsibility)
    /// - **命名约定**：`name` 使用稳定标识（如 `acme.tpm_remote_attest` 或反向域名），方便双方匹配能力。
    /// - **错误处理**：协商双方若不支持该协议，必须返回
    ///   [`crate::error::codes::APP_UNAUTHORIZED`] 并在参数或日志中指出兼容的替代方案。
    /// - **禁止降级**：不得在未确认的情况下自动回退到 `MutualTls` 等默认协议，避免绕过安全要求。
    Custom {
        name: String,
        parameters: Vec<(String, String)>,
    },
}

/// 协商选项，描述某一种协议及其所需材料。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SecurityProtocolOffer {
    protocol: SecurityProtocol,
    credential: Option<Credential>,
    mandatory: bool,
}

impl SecurityProtocolOffer {
    /// 创建协商选项。
    ///
    /// # 契约
    /// - **参数**：`protocol` 指定候选安全协议；`credential` 为可选预共享材料；`mandatory` 指示是否为必选项。
    pub fn new(protocol: SecurityProtocol) -> Self {
        Self {
            protocol,
            credential: None,
            mandatory: false,
        }
    }

    /// 附加预共享凭证。
    pub fn with_credential(mut self, credential: Credential) -> Self {
        self.credential = Some(credential);
        self
    }

    /// 标记为必选协议。
    pub fn require(mut self) -> Self {
        self.mandatory = true;
        self
    }

    /// 获取协议。
    pub fn protocol(&self) -> &SecurityProtocol {
        &self.protocol
    }

    /// 获取凭证。
    pub fn credential(&self) -> Option<&Credential> {
        self.credential.as_ref()
    }

    /// 是否必选。
    pub fn mandatory(&self) -> bool {
        self.mandatory
    }
}

/// 协商计划，定义客户端或服务端的安全偏好。
///
/// # 字段说明
/// - `offers`：按优先级排序的协议选项列表。
/// - `fall_back_to_plaintext`：是否允许降级为明文，仅在受控环境且经过审计时启用。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SecurityNegotiationPlan {
    offers: Vec<SecurityProtocolOffer>,
    fall_back_to_plaintext: bool,
}

impl SecurityNegotiationPlan {
    /// 创建协商计划。
    ///
    /// # 契约
    /// - 默认禁止回退至明文，遵循零信任原则。
    pub fn new(offers: Vec<SecurityProtocolOffer>) -> Self {
        Self {
            offers,
            fall_back_to_plaintext: false,
        }
    }

    /// 允许在特定场景下回退为明文。
    ///
    /// # 风险说明
    /// - 仅用于开发或封闭测试环境；生产环境启用需经过合规审批。
    pub fn allow_plaintext(mut self) -> Self {
        self.fall_back_to_plaintext = true;
        self
    }

    /// 获取选项列表。
    pub fn offers(&self) -> &Vec<SecurityProtocolOffer> {
        &self.offers
    }

    /// 是否允许降级为明文。
    pub fn fall_back_to_plaintext(&self) -> bool {
        self.fall_back_to_plaintext
    }
}

/// 协商上下文，提供环境信息以辅助决策。
///
/// # 字段契约
/// - `local_identity`：本地主体，用于选择合适的证书或密钥。
/// - `remote_hint`：对端身份提示，可能来自路由或策略引擎。
/// - `required_protocols`：策略要求必须支持的协议列表。
/// - `session_metadata`：额外的键值元数据（如租户、区域）。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NegotiationContext {
    local_identity: IdentityDescriptor,
    remote_hint: Option<IdentityDescriptor>,
    required_protocols: Vec<SecurityProtocol>,
    session_metadata: Vec<(String, String)>,
}

impl NegotiationContext {
    /// 构建上下文。
    pub fn new(local_identity: IdentityDescriptor) -> Self {
        Self {
            local_identity,
            remote_hint: None,
            required_protocols: Vec::new(),
            session_metadata: Vec::new(),
        }
    }

    /// 指定对端身份提示。
    pub fn with_remote_hint(mut self, remote: IdentityDescriptor) -> Self {
        self.remote_hint = Some(remote);
        self
    }

    /// 附加必选协议。
    pub fn require_protocol(mut self, protocol: SecurityProtocol) -> Self {
        self.required_protocols.push(protocol);
        self
    }

    /// 附加元数据键值。
    pub fn add_metadata(mut self, key: String, value: String) -> Self {
        self.session_metadata.push((key, value));
        self
    }

    /// 获取本地身份。
    pub fn local_identity(&self) -> &IdentityDescriptor {
        &self.local_identity
    }

    /// 获取对端提示。
    pub fn remote_hint(&self) -> Option<&IdentityDescriptor> {
        self.remote_hint.as_ref()
    }

    /// 获取必选协议。
    pub fn required_protocols(&self) -> &Vec<SecurityProtocol> {
        &self.required_protocols
    }

    /// 获取元数据。
    pub fn session_metadata(&self) -> &Vec<(String, String)> {
        &self.session_metadata
    }
}

/// 协商结果。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NegotiationOutcome {
    protocol: SecurityProtocol,
    negotiated_credential: Option<Credential>,
    peer_identity: Option<IdentityDescriptor>,
    session_metadata: Vec<(String, String)>,
}

impl NegotiationOutcome {
    /// 构造结果。
    pub fn new(protocol: SecurityProtocol) -> Self {
        Self {
            protocol,
            negotiated_credential: None,
            peer_identity: None,
            session_metadata: Vec::new(),
        }
    }

    /// 附加协商后的凭证。
    pub fn with_credential(mut self, credential: Credential) -> Self {
        self.negotiated_credential = Some(credential);
        self
    }

    /// 记录对端身份。
    pub fn with_peer_identity(mut self, identity: IdentityDescriptor) -> Self {
        self.peer_identity = Some(identity);
        self
    }

    /// 合并元数据。
    pub fn with_metadata(mut self, metadata: Vec<(String, String)>) -> Self {
        self.session_metadata.extend(metadata);
        self
    }

    /// 获取协议。
    pub fn protocol(&self) -> &SecurityProtocol {
        &self.protocol
    }

    /// 获取凭证。
    pub fn credential(&self) -> Option<&Credential> {
        self.negotiated_credential.as_ref()
    }

    /// 获取对端身份。
    pub fn peer_identity(&self) -> Option<&IdentityDescriptor> {
        self.peer_identity.as_ref()
    }

    /// 获取协商元数据。
    pub fn metadata(&self) -> &Vec<(String, String)> {
        &self.session_metadata
    }
}

/// 协商错误。
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum NegotiationError {
    /// 必选协议均不被对端接受。
    ProtocolRejected,
    /// 没有共同协议，且不允许明文回退。
    NoCommonProtocol,
    /// 凭证验证失败。
    CredentialInvalid(String),
    /// 协商过程超时或中断。
    Timeout,
    /// 其他错误。
    Other(String),
}

/// 协商结果类型别名。
pub type NegotiationResult = crate::Result<NegotiationOutcome, NegotiationError>;

/// 安全协商器契约。
pub trait SecurityNegotiator: Sealed {
    /// 执行安全协商。
    ///
    /// # 契约
    /// - **输入**：`plan` 定义本端优先级与回退策略；`context` 提供身份与策略约束。
    /// - **输出**：成功时返回 [`NegotiationOutcome`]，失败时返回 [`NegotiationError`]。
    ///
    /// # 设计考量
    /// - Trait 保持同步接口，便于在 `no_std` 场景结合状态机实现。异步框架可在外层封装。
    /// - 协商器应遵守 `context.required_protocols`，若不满足需返回 `ProtocolRejected`。
    fn negotiate(
        &self,
        plan: &SecurityNegotiationPlan,
        context: &NegotiationContext,
    ) -> NegotiationResult;
}
