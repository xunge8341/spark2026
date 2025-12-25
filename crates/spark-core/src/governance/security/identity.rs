use alloc::string::String;
use alloc::vec::Vec;

/// 标准化身份描述，用于跨平台传递调用主体信息。
///
/// # 背景（Why）
/// - **行业最佳实践**：参考 SPIFFE ID、AWS ARN、Kubernetes ServiceAccount 的命名约定，将身份拆解为“签发方 + 名称 + 类型 + 版本”。
/// - **科研借鉴**：吸收可验证凭证（Verifiable Credential）与去中心化身份（DID）的层次化命名思想，使结构既可用于集中式，也可用于分布式身份系统。
/// - **架构角色**：在本框架中作为所有安全协商、授权策略与审计日志的关键索引。
///
/// # 字段契约（What）
/// - `authority`：签发机构或命名空间，例如 `spiffe://cluster.local`、`arn:aws:iam::123456789012`。必须使用业界通用格式。
/// - `name`：主体名称或相对路径。需保证在 `authority` 下唯一。
/// - `kind`：身份类型，指明是工作负载、服务还是用户，以驱动策略细分。
/// - `version`：可选版本号/修订号，辅助密钥轮换或蓝绿发布。
///
/// # 前置 / 后置条件
/// - **前置条件**：调用者需确保 `authority` 与 `name` 已通过合法性校验（如 URI/ARN 规则）。
/// - **后置条件**：结构体创建后不会自动校验可达性，需由上层认证系统保证身份有效。
///
/// # 设计取舍与风险（Trade-offs）
/// - 为适配多身份系统，`authority` 与 `name` 均采用字符串，不强制 URI 语义；换来的是调用方需在接入层完成格式校验。
/// - `version` 使用 `Option`，避免在不需要版本控制的系统产生额外负担。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IdentityDescriptor {
    authority: String,
    name: String,
    kind: IdentityKind,
    version: Option<String>,
}

impl IdentityDescriptor {
    /// 构建身份描述。
    ///
    /// # 契约说明
    /// - **参数**：
    ///   - `authority`：签发方或命名空间，不能为空字符串。
    ///   - `name`：主体名称，不能为空字符串。
    ///   - `kind`：身份类型枚举。
    /// - **返回值**：包含基础字段、`version` 默认 `None` 的结构体。
    ///
    /// # 逻辑解析（How）
    /// - 直接分配字段并保持不可变引用语义；不做额外校验以保持 `no_std` 环境的轻量化。
    ///
    /// # 风险提示
    /// - 若调用者传入空字符串，将导致身份语义不明确；建议在外层配合断言或契约测试。
    pub fn new(authority: String, name: String, kind: IdentityKind) -> Self {
        Self {
            authority,
            name,
            kind,
            version: None,
        }
    }

    /// 为身份设置版本信息。
    ///
    /// # 契约说明
    /// - **参数**：`version` 为语义化版本、Git 提交或密钥指纹。允许空字符串但不推荐。
    /// - **返回值**：返回修改后的新实例，保持 Builder 风格链式调用。
    /// - **前置条件**：调用者需保证版本与上游证书/密钥仓库同步，否则可能导致策略匹配失败。
    /// - **后置条件**：结构体内部 `version` 字段被更新为 `Some(version)`。
    ///
    /// # 设计考量
    /// - 采用 `self` 按值接收，避免在 `no_std` 中引入可变引用生命周期复杂度。
    pub fn with_version(mut self, version: String) -> Self {
        self.version = Some(version);
        self
    }

    /// 获取签发方。
    ///
    /// # 合约
    /// - 返回对内部 `authority` 的借用，调用方不得修改内容。
    pub fn authority(&self) -> &str {
        &self.authority
    }

    /// 获取主体名称。
    pub fn name(&self) -> &str {
        &self.name
    }

    /// 获取身份类型。
    pub fn kind(&self) -> &IdentityKind {
        &self.kind
    }

    /// 获取版本信息。
    pub fn version(&self) -> Option<&String> {
        self.version.as_ref()
    }
}

/// 身份类别枚举，兼容主流零信任系统的主体分类。
///
/// # 设计来源
/// - **工作负载/服务/用户**：源于 SPIFFE 与 Kubernetes 对工作负载、命名空间服务、用户区分的需求。
/// - **机器**：吸收 AWS IAM 机器身份、SSH 主机密钥场景，强调宿主级别认证。
/// - **自定义**：允许对接私有身份系统，避免枚举膨胀。
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum IdentityKind {
    /// 运行在容器、虚拟机或函数上的工作负载。
    Workload,
    /// 面向网络暴露的服务或 API。
    Service,
    /// 人类用户或自动化工具的代表。
    User,
    /// 物理或虚拟机器身份。
    Machine,
    /// 自定义类型，通过字符串说明语义。
    ///
    /// # 实现责任 (Implementation Responsibility)
    /// - **命名约定**：遵循 `provider://domain/kind` 或反向域名前缀，确保跨系统唯一且可追踪。
    /// - **错误处理**：策略引擎或身份映射器若无法识别该类型，必须返回
    ///   [`crate::error::codes::APP_UNAUTHORIZED`] 并附带修复指引。
    /// - **禁止降级**：不得回退为通用 `User`/`Service` 类型或静默放行，避免授权绕过。
    Custom(String),
}

/// 身份证明材料，封装不同安全协议可识别的证据。
///
/// # 背景（Why）
/// - 结合 mTLS 证书链、JWT、SPIFFE SVID、WebAuthn 等行业案例，提供统一包装。
/// - 支持将证明材料传递给密钥协商、策略引擎，避免在调用路径上绑定某一协议。
///
/// # 契约（What）
/// - `X509Chain`：DER 编码的证书字节数组，按链路顺序排列。
/// - `JsonWebToken`：符合 RFC 7519 的紧凑序列化 JWT。
/// - `Spiffe`：SPIFFE ID 或 SVID 表示，字符串格式。
/// - `PublicKey`：裸公钥或公钥指纹，用于 SSH 或硬件模块。
/// - `Custom`：格式标识 + 原始载荷，留给未来的证明形式（如可验证凭证）。
///
/// # 风险提示
/// - 字节数组未做保密处理，敏感信息在加载后需尽快清理或移交安全内存管理组件。
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum IdentityProof {
    /// DER 编码的 X.509 证书链。
    X509Chain(Vec<u8>),
    /// 紧凑格式的 JWT。
    JsonWebToken(String),
    /// SPIFFE 身份字符串或 SVID。
    Spiffe(String),
    /// 裸公钥或指纹。
    PublicKey(Vec<u8>),
    /// 其他格式，由调用方自定义解释。
    ///
    /// # 实现责任 (Implementation Responsibility)
    /// - **命名约定**：`format` 使用稳定字符串（如 `did_jwt_vc` 或 `vendor.attestation`），方便跨团队协作。
    /// - **错误处理**：消费方若不支持该格式，应返回
    ///   [`crate::error::codes::APP_UNAUTHORIZED`] 并记录告警，同时提供降级或替代方案。
    /// - **禁止降级**：不可默认为 `JsonWebToken` 等已知格式，也不得忽略载荷，避免验证缺失。
    Custom {
        /// 证明格式标识，如 `webauthn`, `vc+ldp`。
        format: String,
        /// 原始载荷。
        payload: Vec<u8>,
    },
}
