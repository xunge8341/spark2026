//! 安全事件分类枚举，统一表达安全违规的主语义，便于在错误契约与关闭策略中复用。
//!
//! # 设计背景（Why）
//! - 框架需要在出现安全相关异常时输出结构化的分类，驱动自动化的告警、降级与关闭策略；
//! - 借鉴零信任架构常见的分层（认证、授权、保密、完整性、审计），以覆盖调用链中的主要安全风险点。
//!
//! # 契约说明（What）
//! - 枚举为 `#[non_exhaustive]`，允许未来扩展新的安全事件；
//! - 通过 [`SecurityClass::summary`] 与 [`SecurityClass::code`] 提供机器可读与人类可读的双视图；
//! - `Unknown` 分支用于承接临时或上层尚未细分的安全事件。
//!
//! # 风险提示（Trade-offs）
//! - 若调用方错误选择分类，会导致告警策略偏离实际风险；
//! - 未来扩展新分支时需同步更新默认处理器与测试，确保关闭策略保持一致。
/// 安全事件分类枚举。
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum SecurityClass {
    /// 认证失败，例如凭证过期、身份不存在。
    Authentication,
    /// 授权失败，例如策略拒绝或越权访问。
    Authorization,
    /// 保密性威胁，例如明文传输敏感数据。
    Confidentiality,
    /// 完整性校验失败，例如消息被篡改或校验和不符。
    Integrity,
    /// 审计或合规性异常，例如违规操作或缺失审计轨迹。
    Audit,
    /// 未归类的安全事件。
    Unknown,
}

impl SecurityClass {
    /// 返回分类对应的稳定代码，供日志与指标使用。
    pub const fn code(self) -> &'static str {
        match self {
            SecurityClass::Authentication => "security.authentication",
            SecurityClass::Authorization => "security.authorization",
            SecurityClass::Confidentiality => "security.confidentiality",
            SecurityClass::Integrity => "security.integrity",
            SecurityClass::Audit => "security.audit",
            SecurityClass::Unknown => "security.unknown",
        }
    }

    /// 返回人类可读摘要，用于关闭原因或告警文案。
    pub const fn summary(self) -> &'static str {
        match self {
            SecurityClass::Authentication => "身份验证失败，需重新认证",
            SecurityClass::Authorization => "权限不足或策略拒绝",
            SecurityClass::Confidentiality => "检测到保密性风险，需加密或隔离",
            SecurityClass::Integrity => "数据完整性校验失败",
            SecurityClass::Audit => "审计或合规策略触发告警",
            SecurityClass::Unknown => "未归类的安全事件，建议人工复核",
        }
    }
}
