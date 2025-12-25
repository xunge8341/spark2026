use alloc::borrow::Cow;
use alloc::string::String;

/// 时间戳权威（TSA）锚点，用于外部可信时间证明。
///
/// ## 设计动机（Why）
/// - 合规场景常要求事件绑定外部可信时间，防止本地时间被篡改。
///
/// ## 契约说明（What）
/// - `provider`：TSA 服务提供方。
/// - `evidence`：原始签名或凭证字符串，通常为 Base64 编码。
/// - `issued_at`：TSA 签发时间的 Unix 秒级时间戳。
#[derive(Clone, Debug, PartialEq)]
pub struct TsaEvidence {
    pub provider: Cow<'static, str>,
    pub evidence: String,
    pub issued_at: u64,
}

impl TsaEvidence {
    /// ## 设计动机（Why）
    /// - 通过内部表示隐藏序列化细节，保持公共 API 的纯净度。
    ///
    /// ## 契约定义（What）
    /// - 入参：`&self`。
    /// - 返回：[`TsaEvidenceRepr`]，可直接交由 `serde` 处理。
    ///
    /// ## 逻辑解析（How）
    /// - 克隆 `provider` 与 `evidence`，复制 `issued_at`，形成独立的中间表示。
    ///
    /// ## 前置/后置条件（Contract）
    /// - 前置：调用方保证字段值已满足业务要求（例如证据格式、时间范围）。
    /// - 后置：返回表示不会反向影响原结构，适合在多线程中复用。
    ///
    /// ## 设计考量（Trade-offs）
    /// - 为了保证类型隔离选择深拷贝，但对象尺寸较小，性能影响可忽略。
    pub(crate) fn to_repr(&self) -> TsaEvidenceRepr {
        TsaEvidenceRepr {
            provider: self.provider.clone(),
            evidence: self.evidence.clone(),
            issued_at: self.issued_at,
        }
    }

    /// ## 设计动机（Why）
    /// - 从序列化表示还原 TSA 锚点，支撑审计事件在不同系统间传递。
    ///
    /// ## 契约定义（What）
    /// - 入参：`repr`。
    /// - 返回：[`TsaEvidence`]。
    ///
    /// ## 逻辑解析（How）
    /// - 将内部表示的字段直接移动到新结构，避免重复分配。
    ///
    /// ## 前置/后置条件（Contract）
    /// - 前置：反序列化阶段已确保字段类型正确。
    /// - 后置：返回实例未对证据有效性做进一步校验。
    ///
    /// ## 设计考量（Trade-offs）
    /// - 将签名校验留给业务层，保持模块职责单一。
    pub(crate) fn from_repr(repr: TsaEvidenceRepr) -> Self {
        Self {
            provider: repr.provider,
            evidence: repr.evidence,
            issued_at: repr.issued_at,
        }
    }
}

impl serde::Serialize for TsaEvidence {
    fn serialize<S>(&self, serializer: S) -> crate::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.to_repr().serialize(serializer)
    }
}

impl<'de> serde::Deserialize<'de> for TsaEvidence {
    fn deserialize<D>(deserializer: D) -> crate::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let repr = TsaEvidenceRepr::deserialize(deserializer)?;
        Ok(Self::from_repr(repr))
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
pub(crate) struct TsaEvidenceRepr {
    pub(crate) provider: Cow<'static, str>,
    pub(crate) evidence: String,
    pub(crate) issued_at: u64,
}
