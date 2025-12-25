use core::time::Duration;

use alloc::string::String;
use alloc::vec::Vec;

use super::identity::{IdentityDescriptor, IdentityProof};

/// 凭证的元信息与使用范围描述。
///
/// # 背景（Why）
/// - **行业经验**：对标 SPIFFE SVID TTL、Envoy SDS 证书分发、OAuth2 Access Token 生命周期，将凭证抽象为“身份 + 范围 + 有效期 + 可更新性”。
/// - **科研借鉴**：吸收零信任体系中“短期可轮换凭证”设计，支持以持续验证（Continuous Authentication）为目标的策略研究。
///
/// # 字段契约（What）
/// - `identity`：凭证绑定的主体 [`IdentityDescriptor`]。
/// - `proof`：可选的初始身份证明，便于预加载链路（如包含 mTLS 证书链）。
/// - `scope`：凭证适用的安全上下文，如连接、会话或单条消息。
/// - `valid_for`：建议的有效时长。`None` 表示交由外部策略决定（如永久有效或依赖撤销列表）。
/// - `renewable`：指示凭证是否允许自动续期或轮换。
///
/// # 前置条件
/// - 调用方需确保 `identity` 对应的签发策略允许创建此类凭证，否则后续验证会失败。
///
/// # 后置条件
/// - 结构体本身不进行签名或加密处理，仅作为元数据容器，需要配合 [`CredentialMaterial`] 使用。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CredentialDescriptor {
    identity: IdentityDescriptor,
    proof: Option<IdentityProof>,
    scope: CredentialScope,
    valid_for: Option<Duration>,
    renewable: bool,
}

impl CredentialDescriptor {
    /// 创建凭证描述。
    ///
    /// # 参数
    /// - `identity`：主体描述。
    /// - `scope`：凭证适用范围。
    ///
    /// # 返回值
    /// - 一个默认 `proof = None`、`valid_for = None`、`renewable = true` 的描述。
    pub fn new(identity: IdentityDescriptor, scope: CredentialScope) -> Self {
        Self {
            identity,
            proof: None,
            scope,
            valid_for: None,
            renewable: true,
        }
    }

    /// 关联预先准备好的身份证明材料。
    ///
    /// # 契约
    /// - **参数**：`proof` 通常为证书链或 JWT，可用于懒加载验证。
    /// - **前置条件**：证明材料需与 `identity` 一致，否则会在握手阶段被拒绝。
    /// - **后置条件**：返回包含 `proof` 的新实例。
    pub fn with_proof(mut self, proof: IdentityProof) -> Self {
        self.proof = Some(proof);
        self
    }

    /// 设置建议有效期。
    ///
    /// # 契约
    /// - **参数**：`duration` 表示从签发起建议的最大使用时长。
    /// - **设计考虑**：使用 `Duration` 而非绝对时间，便于在离线或嵌入式设备中使用单调计时器计算 TTL。
    /// - **风险提示**：若上层无法获取当前时间，则需结合撤销列表或一次性凭证策略确保安全。
    pub fn with_valid_for(mut self, duration: Duration) -> Self {
        self.valid_for = Some(duration);
        self
    }

    /// 标记为不可续期。
    ///
    /// # 用途
    /// - 适用于一次性令牌、短期访问密钥等需要强制轮换的场景。
    pub fn non_renewable(mut self) -> Self {
        self.renewable = false;
        self
    }

    /// 获取绑定身份。
    pub fn identity(&self) -> &IdentityDescriptor {
        &self.identity
    }

    /// 获取预置的身份证明。
    pub fn proof(&self) -> Option<&IdentityProof> {
        self.proof.as_ref()
    }

    /// 获取凭证适用范围。
    pub fn scope(&self) -> CredentialScope {
        self.scope
    }

    /// 获取建议有效期。
    pub fn valid_for(&self) -> Option<Duration> {
        self.valid_for
    }

    /// 是否允许自动续期。
    pub fn renewable(&self) -> bool {
        self.renewable
    }
}

/// 凭证在安全流程中的适用范围。
///
/// # 设计参考
/// - 结合 OAuth2 token scopes、Envoy SDS secret 类型、gRPC channel credentials 分类，将范围划分为连接/会话/消息层。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum CredentialScope {
    /// 面向传输连接（如 TLS 握手、QUIC 路径验证）。
    Connection,
    /// 面向逻辑会话（如 gRPC stream、WebSocket）。
    Session,
    /// 面向单次消息或请求。适用于一次性签名或 MAC。
    Message,
}

/// 凭证载荷。
///
/// # 背景
/// - 提供统一封装，便于在 `no_std` 环境通过 `Vec<u8>` 或字符串传输不同格式的密钥和令牌。
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum CredentialMaterial {
    /// DER 编码证书或私钥。
    CertificateChain(Vec<u8>),
    /// 对称密钥（如 AES、ChaCha20）。
    SymmetricKey(Vec<u8>),
    /// 签名令牌或断言（如 JWT、SAML）。
    SignedToken(Vec<u8>),
    /// 公钥材料（如 Ed25519、公钥指纹）。
    PublicKey(Vec<u8>),
    /// 其他类型，由 `format` 字段说明。
    ///
    /// # 实现责任 (Implementation Responsibility)
    /// - **命名约定**：`format` 推荐使用 IANA 注册名或组织前缀（如 `acme.sgx_quote`），确保跨系统唯一。
    /// - **错误处理**：凭证消费方若不支持该格式，必须返回
    ///   [`crate::error::codes::APP_UNAUTHORIZED`] 并在审计日志中记录原因与建议替代方案。
    /// - **禁止降级**：禁止将其默认为 `CertificateChain` 等已知类型或忽略载荷，避免安全验证缺失。
    Custom { format: String, payload: Vec<u8> },
}

/// 凭证实体，将描述与材料绑定。
///
/// # 合同说明
/// - `descriptor`：元信息；`material`：实际密钥或令牌；`version_hint`：用于软升级的可选指纹。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Credential {
    descriptor: CredentialDescriptor,
    material: CredentialMaterial,
    version_hint: Option<String>,
}

impl Credential {
    /// 构建凭证实体。
    ///
    /// # 契约
    /// - **参数**：
    ///   - `descriptor`：必须事先定义身份与范围。
    ///   - `material`：密钥或令牌字节，调用者需保证安全性。
    /// - **返回值**：新的 [`Credential`] 实例，默认 `version_hint = None`。
    ///
    /// # 风险提示
    /// - 凭证不会自动擦除内存，敏感场景请结合安全内存分配器或零化工具。
    pub fn new(descriptor: CredentialDescriptor, material: CredentialMaterial) -> Self {
        Self {
            descriptor,
            material,
            version_hint: None,
        }
    }

    /// 附加版本或指纹信息。
    ///
    /// # 用途
    /// - 便于客户端通过指纹比较决定是否轮换凭证。
    pub fn with_version_hint(mut self, version_hint: String) -> Self {
        self.version_hint = Some(version_hint);
        self
    }

    /// 获取描述信息。
    pub fn descriptor(&self) -> &CredentialDescriptor {
        &self.descriptor
    }

    /// 获取凭证材料。
    pub fn material(&self) -> &CredentialMaterial {
        &self.material
    }

    /// 获取版本指纹。
    pub fn version_hint(&self) -> Option<&String> {
        self.version_hint.as_ref()
    }

    /// 依据当前上下文估算凭证状态。
    ///
    /// # 契约
    /// - **参数**：
    ///   - `age`：从签发至今的时长估计。若无法获得，可传 `None`。
    ///   - `revoked`：外部撤销列表的判断结果。
    /// - **返回值**：[`CredentialState`]，指示凭证是否仍可使用。
    ///
    /// # 逻辑解析
    /// - 若 `revoked` 为真，直接返回 `Revoked`。
    /// - 若 `valid_for` 存在且 `age` 超过该值，返回 `Expired`。
    /// - 其他情况返回 `Active`。
    pub fn state(&self, age: Option<Duration>, revoked: bool) -> CredentialState {
        if revoked {
            return CredentialState::Revoked;
        }
        match (self.descriptor.valid_for, age) {
            (Some(valid_for), Some(current_age)) if current_age >= valid_for => {
                CredentialState::Expired
            }
            _ => CredentialState::Active,
        }
    }
}

/// 凭证状态枚举。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum CredentialState {
    /// 凭证仍可使用。
    Active,
    /// 凭证已过有效期，需要轮换。
    Expired,
    /// 凭证被撤销或标记为不可用。
    Revoked,
}
