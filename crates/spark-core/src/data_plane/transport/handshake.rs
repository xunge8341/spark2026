//! 传输握手协商模块。
//!
//! # 模块定位（Why）
//! - **新增能力的安全演进**：围绕 T22 需求，引入显式版本与能力位图，使协议升级能够在不中断旧实现的前提下协商降级。
//! - **跨模块协作**：协商过程需向审计子系统写入事件，并为传输工厂、服务端监听器等调用方提供标准化结构体。
//! - **面向多环境**：所有类型与算法均兼容 `no_std + alloc` 场景，同时保留足够的扩展点支持云/边缘部署差异。
//!
//! # 核心组成（What）
//! - [`Version`]：封装语义化版本 `{major, minor, patch}`，用于判定向后兼容性。
//! - [`Capability`] 与 [`CapabilityBitmap`]：以位图形式声明能力集合，支持内建常量与自定义索引。
//! - [`HandshakeOffer`] / [`HandshakeOutcome`]：描述双方宣告与协商结果，并指出降级的位图差异。
//! - [`NegotiationAuditContext`]：将协商过程与 [`crate::audit`] 模块联动，确保事件进入不可篡改的哈希链。
//! - [`negotiate`]：执行实际的版本/能力协商，并在成功或失败时触发审计记录。
//!
//! # 协作方式（How）
//! 1. 调用方向两侧收集版本与能力，构造 [`HandshakeOffer`]。
//! 2. （可选）准备 [`NegotiationAuditContext`]，以便自动写入审计事件。
//! 3. 调用 [`negotiate`] 获得 [`HandshakeOutcome`] 或 [`HandshakeError`]。
//! 4. 依据 [`DowngradeReport`] 判断是否需要启用兼容策略或告警。
//!
//! # 风险提示（Trade-offs）
//! - 位图仅支持 128 个能力位；若需更大空间需在未来引入分片或变长编码。
//! - 审计记录失败会被视为握手失败，确保链式完整性但可能导致连接回退；调用方应为 Recorder 提供高可用保证。
//! - 所有 `custom` 能力索引约定 `< 128`，若越界会触发 panic；请在注册表中统一分配编号。

use crate::{
    SparkError,
    audit::{
        AuditActor, AuditChangeEntry, AuditChangeSet, AuditEntityRef, AuditError, AuditEventV1,
        AuditRecorder, TsaEvidence,
    },
    configuration::{ConfigKey, ConfigMetadata, ConfigScope, ConfigValue},
    error::codes,
};
use alloc::{
    borrow::Cow,
    format,
    string::{String, ToString},
    sync::Arc,
    vec,
    vec::Vec,
};
use core::{
    cmp::{self, Ordering},
    fmt,
};
use sha2::{Digest, Sha256};

const ZERO_HASH: &str = "0000000000000000000000000000000000000000000000000000000000000000";
const HEX_TABLE: &[u8; 16] = b"0123456789abcdef";

/// 协商使用的语义化版本号，采用 `{major, minor, patch}` 三段式表示。
///
/// # 背景阐释（Why）
/// - 框架长期运行于多版本互联环境，需要依赖主版本匹配来判断是否允许继续握手。
/// - 通过保留次版本/补丁位，可在不破坏旧实现的前提下协商出双方都支持的最小公共版本。
///
/// # 契约定义（What）
/// - `major`、`minor`、`patch` 均为无符号 16 位整数，足以覆盖主流版本策略。
/// - `major` 不相等时视为不兼容，`minor`、`patch` 仅用于选择最优落点，不影响兼容判断。
///
/// # 实现细节（How）
/// - `Version` 实现 `Ord`/`Eq`，因此可以直接用于排序、去重或 `BTreeMap` 键值。
/// - `Display` 以标准 `x.y.z` 形式输出，方便写入日志或审计事件。
///
/// # 风险提示（Trade-offs）
/// - 若未来需要 `pre-release` / `build metadata`，需在保持向后兼容的前提下扩展结构体；当前实现不含该信息。
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Version {
    major: u16,
    minor: u16,
    patch: u16,
}

impl Version {
    /// 构造版本实例。
    ///
    /// # 背景（Why）
    /// - 调用方通常从配置或常量中读取版本号，需要显式构造便于传递给协商函数。
    ///
    /// # 契约（What）
    /// - **输入**：`major`/`minor`/`patch` 必须符合语义化版本语义（非负整数）。
    /// - **后置条件**：返回的版本可参与排序、比较与显示。
    ///
    /// # 实现说明（How）
    /// - 直接存入结构体字段，不执行额外校验；调用方负责确保版本含义正确。
    pub const fn new(major: u16, minor: u16, patch: u16) -> Self {
        Self {
            major,
            minor,
            patch,
        }
    }

    /// 返回主版本号。
    ///
    /// # 契约说明
    /// - **作用**：用于判断兼容性或写入审计字段。
    /// - **前置条件**：无；调用方可在任意时间调用。
    pub const fn major(&self) -> u16 {
        self.major
    }

    /// 返回次版本号。
    ///
    /// # 使用场景
    /// - 在计算协商后的“最小公共版本”时，结合 `Ord` 比较使用。
    pub const fn minor(&self) -> u16 {
        self.minor
    }

    /// 返回补丁版本号。
    ///
    /// # 风险提示
    /// - 补丁位仅用于记录兼容优化；不得据此推断破坏性变更。
    pub const fn patch(&self) -> u16 {
        self.patch
    }

    /// 判断两个版本是否在主版本层面兼容。
    ///
    /// # 背景（Why）
    /// - T22 目标要求主版本不一致时必须优雅失败，避免协议误判。
    ///
    /// # 契约（What）
    /// - 返回 `true` 表示主版本一致，可继续比较次版本；否则必须终止握手。
    ///
    /// # 实现（How）
    /// - 直接比较 `major` 字段，不牵涉额外状态。
    pub const fn is_compatible_with(&self, other: &Self) -> bool {
        self.major == other.major
    }
}

impl Ord for Version {
    fn cmp(&self, other: &Self) -> Ordering {
        self.major
            .cmp(&other.major)
            .then(self.minor.cmp(&other.minor))
            .then(self.patch.cmp(&other.patch))
    }
}

impl PartialOrd for Version {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl fmt::Display for Version {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}.{}", self.major, self.minor, self.patch)
    }
}

/// 能力位图中的单个能力位标识。
///
/// # 背景（Why）
/// - 传统“字符串能力”在握手阶段需要多次比较与解析，效率较低；采用索引位图后可常数时间判定支持情况。
///
/// # 契约（What）
/// - 内建常量覆盖多路复用、压缩、零拷贝等常见能力。
/// - `custom` 允许实现方扩展，但索引必须 `< 128`，否则会触发 panic。
///
/// # 风险提示（Trade-offs）
/// - 索引冲突由上层注册中心治理；框架不会在运行时检测重复定义。
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Capability {
    index: u8,
}

impl Capability {
    /// 内建：多路复用能力（如多 Stream）。
    pub const MULTIPLEXING: Self = Self::from_raw(0);
    /// 内建：编解码压缩能力。
    pub const COMPRESSION: Self = Self::from_raw(1);
    /// 内建：零拷贝传输能力。
    pub const ZERO_COPY: Self = Self::from_raw(2);

    /// 构造自定义能力位。
    ///
    /// # 契约（What）
    /// - **输入**：`index < 128`；建议在组织内部维护映射表防止冲突。
    /// - **后置条件**：返回的能力可安全加入位图。
    ///
    /// # 风险提示
    /// - 超出范围将触发 panic；请在调试或构建阶段完成校验。
    pub const fn custom(index: u8) -> Self {
        Self::from_raw(index)
    }

    const fn from_raw(index: u8) -> Self {
        assert!(index < 128, "capability index must be < 128");
        Self { index }
    }

    const fn mask(self) -> u128 {
        1u128 << (self.index as u32)
    }
}

/// 能力位图，使用 `u128` 表示最多 128 个能力位。
///
/// # 背景（Why）
/// - 相比 `Vec<Capability>`，位图在协商与交集中具备更好的常数时间表现，并便于写入审计事件。
///
/// # 契约（What）
/// - 位 `1` 表示支持，位 `0` 表示未支持。
/// - 提供集合运算（并、交、差、子集判定）辅助协商流程。
///
/// # 风险提示（Trade-offs）
/// - 超过 128 位的需求需另行扩展；目前实现不支持动态扩容。
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct CapabilityBitmap {
    bits: u128,
}

impl CapabilityBitmap {
    /// 创建空位图。
    ///
    /// # 使用场景
    /// - 组装能力集合前的初始状态；常与 [`CapabilityBitmap::insert`] 配合使用。
    pub const fn empty() -> Self {
        Self { bits: 0 }
    }

    /// 根据原始位值构造位图。
    ///
    /// # 契约说明
    /// - **输入**：`bits` 的每一位代表一个能力；调用方需保证与能力索引一致。
    /// - **后置条件**：返回的位图可参与集合运算。
    pub const fn from_bits(bits: u128) -> Self {
        Self { bits }
    }

    /// 返回底层位值，便于写入日志或自定义序列化。
    pub const fn bits(&self) -> u128 {
        self.bits
    }

    /// 向位图写入一个能力位。
    ///
    /// # 契约
    /// - **输入**：`capability` 必须由 [`Capability`] 构造，索引 `< 128`。
    /// - **后置条件**：对应位被设为 `1`。
    pub fn insert(&mut self, capability: Capability) {
        self.bits |= capability.mask();
    }

    /// 计算位图并集。
    ///
    /// # 场景
    /// - 聚合“必选 + 可选”能力集合。
    pub const fn union(self, other: Self) -> Self {
        Self {
            bits: self.bits | other.bits,
        }
    }

    /// 计算位图交集。
    ///
    /// # 场景
    /// - 协商成功后确定双方同时支持的能力集合。
    pub const fn intersection(self, other: Self) -> Self {
        Self {
            bits: self.bits & other.bits,
        }
    }

    /// 计算位差集（`self \ other`）。
    ///
    /// # 场景
    /// - 定位“被降级的可选能力”。
    pub const fn difference(self, other: Self) -> Self {
        Self {
            bits: self.bits & !other.bits,
        }
    }

    /// 判断是否为另一个位图的子集。
    ///
    /// # 契约
    /// - 返回 `true` 表示 `self` 中所有位均出现在 `other` 中。
    pub const fn is_subset_of(self, other: Self) -> bool {
        (self.bits & !other.bits) == 0
    }

    /// 判断是否为空位图。
    pub const fn is_empty(&self) -> bool {
        self.bits == 0
    }
}

impl fmt::Display for CapabilityBitmap {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "0x{:032x}", self.bits)
    }
}

/// 双方握手时各自宣告的版本与能力集合。
///
/// # 背景（Why）
/// - 将必选与可选能力分离，有助于在协商失败时明确缺失来源，同时为降级策略提供依据。
///
/// # 契约（What）
/// - `mandatory` 必须为 `total = mandatory ∪ optional` 的子集。
/// - `version` 表示该端期望运行的最高版本。
///
/// # 风险提示
/// - 调用方需确保 `mandatory` 与 `optional` 不包含越界能力索引；框架仅在调试模式下 `debug_assert!`。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HandshakeOffer {
    version: Version,
    mandatory: CapabilityBitmap,
    optional: CapabilityBitmap,
}

impl HandshakeOffer {
    /// 构造握手宣告。
    ///
    /// # 契约
    /// - **输入**：`version`、`mandatory`、`optional`。
    /// - **后置条件**：内部保存原始值；调用方可通过访问器读取。
    ///
    /// # 风险提示
    /// - 若 `mandatory` 包含 `optional` 未声明的位，会在运行时通过差集检测导致握手失败。
    pub fn new(version: Version, mandatory: CapabilityBitmap, optional: CapabilityBitmap) -> Self {
        debug_assert!(mandatory.is_subset_of(mandatory.union(optional)));
        Self {
            version,
            mandatory,
            optional,
        }
    }

    /// 返回宣告的版本。
    pub fn version(&self) -> Version {
        self.version
    }

    /// 返回必选能力位图。
    pub fn mandatory(&self) -> CapabilityBitmap {
        self.mandatory
    }

    /// 返回可选能力位图。
    pub fn optional(&self) -> CapabilityBitmap {
        self.optional
    }

    /// 返回必选与可选能力的并集。
    pub fn total(&self) -> CapabilityBitmap {
        self.mandatory.union(self.optional)
    }
}

/// 协商过程中产生的能力降级报告。
///
/// # 契约（What）
/// - `local`：本地声明为可选但未启用的能力位。
/// - `remote`：对端声明为可选但未启用的能力位。
/// - `is_lossless`：若双方都没有被降级能力则返回 `true`。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DowngradeReport {
    local: CapabilityBitmap,
    remote: CapabilityBitmap,
}

impl DowngradeReport {
    /// 创建报告。
    pub fn new(local: CapabilityBitmap, remote: CapabilityBitmap) -> Self {
        Self { local, remote }
    }

    /// 本地被降级能力。
    pub fn local(&self) -> CapabilityBitmap {
        self.local
    }

    /// 对端被降级能力。
    pub fn remote(&self) -> CapabilityBitmap {
        self.remote
    }

    /// 是否无降级。
    pub fn is_lossless(&self) -> bool {
        self.local.is_empty() && self.remote.is_empty()
    }
}

/// 协商成功的最终结果。
///
/// # 契约（What）
/// - `version`：双方同意使用的兼容版本（取二者主版本相同情况下的较小值）。
/// - `capabilities`：启用的能力位图（双方总能力的交集）。
/// - `downgrade`：降级报告，帮助调用方决定是否启用兼容逻辑。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct HandshakeOutcome {
    version: Version,
    capabilities: CapabilityBitmap,
    downgrade: DowngradeReport,
}

impl HandshakeOutcome {
    /// 构造协商结果。
    pub fn new(
        version: Version,
        capabilities: CapabilityBitmap,
        downgrade: DowngradeReport,
    ) -> Self {
        Self {
            version,
            capabilities,
            downgrade,
        }
    }

    /// 协商后的版本。
    pub fn version(&self) -> Version {
        self.version
    }

    /// 最终启用的能力位图。
    pub fn capabilities(&self) -> CapabilityBitmap {
        self.capabilities
    }

    /// 降级详情。
    pub fn downgrade(&self) -> DowngradeReport {
        self.downgrade
    }
}

/// 握手失败原因的细分类别。
///
/// - `MajorVersionMismatch`：主版本不同导致无法协商。
/// - `LocalLacksRemoteRequirements`：本地缺少对端必选能力。
/// - `RemoteLacksLocalRequirements`：对端缺少本地必选能力。
/// - `AuditFailure`：审计事件写入失败。
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum HandshakeErrorKind {
    MajorVersionMismatch { local: Version, remote: Version },
    LocalLacksRemoteRequirements { missing: CapabilityBitmap },
    RemoteLacksLocalRequirements { missing: CapabilityBitmap },
    AuditFailure { error: AuditError },
}

/// 握手失败错误类型，兼容 [`crate::Error`]，可转换为 [`SparkError`]。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HandshakeError {
    kind: HandshakeErrorKind,
}

impl HandshakeError {
    /// 返回错误类别。
    pub fn kind(&self) -> &HandshakeErrorKind {
        &self.kind
    }

    fn audit(error: AuditError) -> Self {
        Self {
            kind: HandshakeErrorKind::AuditFailure { error },
        }
    }

    /// 转换为领域错误 [`SparkError`]，便于上层透传。
    pub fn into_spark_error(self) -> SparkError {
        match self.kind {
            HandshakeErrorKind::MajorVersionMismatch { local, remote } => SparkError::new(
                codes::PROTOCOL_NEGOTIATION,
                format!(
                    "传输握手失败：本地版本 {} 与对端版本 {} 主版本不兼容",
                    local, remote
                ),
            ),
            HandshakeErrorKind::LocalLacksRemoteRequirements { missing } => SparkError::new(
                codes::PROTOCOL_NEGOTIATION,
                format!("传输握手失败：本地缺失对端要求的能力位图 {}", missing),
            ),
            HandshakeErrorKind::RemoteLacksLocalRequirements { missing } => SparkError::new(
                codes::PROTOCOL_NEGOTIATION,
                format!("传输握手失败：对端缺失本地要求的能力位图 {}", missing),
            ),
            HandshakeErrorKind::AuditFailure { ref error } => SparkError::new(
                codes::PROTOCOL_NEGOTIATION,
                format!("传输握手失败：审计记录失败 ({})", error.message()),
            ),
        }
    }
}

impl fmt::Display for HandshakeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.kind {
            HandshakeErrorKind::MajorVersionMismatch { local, remote } => write!(
                f,
                "major version mismatch: local={} remote={}",
                local, remote
            ),
            HandshakeErrorKind::LocalLacksRemoteRequirements { missing } => {
                write!(f, "local lacks required capabilities {}", missing)
            }
            HandshakeErrorKind::RemoteLacksLocalRequirements { missing } => {
                write!(f, "remote lacks required capabilities {}", missing)
            }
            HandshakeErrorKind::AuditFailure { error } => {
                write!(f, "failed to record audit event: {}", error.message())
            }
        }
    }
}

impl crate::Error for HandshakeError {
    #[allow(unused_parens)]
    fn source(&self) -> Option<&(dyn crate::Error + 'static)> {
        None
    }
}

/// 审计上下文，封装 Recorder、实体信息以及事件序列号。
///
/// # 背景（Why）
/// - T12 要求所有关键事件进入审计链；握手协商亦需记录版本、能力与降级信息。
///
/// # 契约（What）
/// - `recorder`：实现 [`AuditRecorder`] 的对象，通常为高可用存储。
/// - `entity_kind` / `entity_id`：审计实体标识，建议采用 `transport.connection` + 连接 ID。
/// - `actor`：触发协商的操作者，可为系统账户或服务身份。
/// - `next_sequence`：下一个事件序号，调用 [`Self::with_start_sequence`] 可自定义起点。
///
/// # 风险提示
/// - Recorder 写入失败将转换为 [`HandshakeErrorKind::AuditFailure`] 并终止握手。
#[derive(Clone)]
pub struct NegotiationAuditContext {
    recorder: Arc<dyn AuditRecorder>,
    entity_kind: Cow<'static, str>,
    entity_id: String,
    entity_labels: Vec<(Cow<'static, str>, Cow<'static, str>)>,
    actor: AuditActor,
    success_action: Cow<'static, str>,
    failure_action: Cow<'static, str>,
    tsa_evidence: Option<TsaEvidence>,
    next_sequence: u64,
    chain_state: Option<String>,
}

impl NegotiationAuditContext {
    /// 创建上下文，默认动作分别为 `transport.handshake.succeeded/failed`。
    pub fn new(
        recorder: Arc<dyn AuditRecorder>,
        entity_kind: impl Into<Cow<'static, str>>,
        entity_id: impl Into<String>,
        actor: AuditActor,
    ) -> Self {
        Self {
            recorder,
            entity_kind: entity_kind.into(),
            entity_id: entity_id.into(),
            entity_labels: Vec::new(),
            actor,
            success_action: Cow::Borrowed("transport.handshake.succeeded"),
            failure_action: Cow::Borrowed("transport.handshake.failed"),
            tsa_evidence: None,
            next_sequence: 0,
            chain_state: None,
        }
    }

    /// 覆盖实体标签集合，便于多租户或多区域审计。
    pub fn with_labels(mut self, labels: Vec<(Cow<'static, str>, Cow<'static, str>)>) -> Self {
        self.entity_labels = labels;
        self
    }

    /// 绑定可信时间锚点。
    pub fn with_tsa_evidence(mut self, evidence: TsaEvidence) -> Self {
        self.tsa_evidence = Some(evidence);
        self
    }

    /// 调整成功/失败动作名称。
    pub fn with_actions(
        mut self,
        success: impl Into<Cow<'static, str>>,
        failure: impl Into<Cow<'static, str>>,
    ) -> Self {
        self.success_action = success.into();
        self.failure_action = failure.into();
        self
    }

    /// 设置初始事件序号。
    pub fn with_start_sequence(mut self, sequence: u64) -> Self {
        self.next_sequence = sequence;
        self
    }

    fn record_success(
        &mut self,
        outcome: &HandshakeOutcome,
        local: Version,
        remote: Version,
        occurred_at: u64,
    ) -> crate::Result<(), HandshakeError> {
        let prev_hash = self
            .chain_state
            .clone()
            .unwrap_or_else(|| ZERO_HASH.to_string());
        let new_hash = compute_state_hash(outcome);
        let sequence = self.next_sequence;
        let changes = AuditChangeSet {
            created: Vec::new(),
            updated: vec![
                AuditChangeEntry {
                    key: ConfigKey::new(
                        "transport",
                        "handshake.negotiated_version",
                        ConfigScope::Session,
                        "协商出的协议版本",
                    ),
                    value: ConfigValue::Text(
                        Cow::Owned(outcome.version().to_string()),
                        ConfigMetadata::default(),
                    ),
                },
                AuditChangeEntry {
                    key: ConfigKey::new(
                        "transport",
                        "handshake.capabilities.enabled",
                        ConfigScope::Session,
                        "最终启用的能力位图",
                    ),
                    value: ConfigValue::Text(
                        Cow::Owned(outcome.capabilities().to_string()),
                        ConfigMetadata::default(),
                    ),
                },
                AuditChangeEntry {
                    key: ConfigKey::new(
                        "transport",
                        "handshake.capabilities.local_downgraded",
                        ConfigScope::Session,
                        "本地未启用的可选能力",
                    ),
                    value: ConfigValue::Text(
                        Cow::Owned(outcome.downgrade().local().to_string()),
                        ConfigMetadata::default(),
                    ),
                },
                AuditChangeEntry {
                    key: ConfigKey::new(
                        "transport",
                        "handshake.capabilities.remote_downgraded",
                        ConfigScope::Session,
                        "对端未启用的可选能力",
                    ),
                    value: ConfigValue::Text(
                        Cow::Owned(outcome.downgrade().remote().to_string()),
                        ConfigMetadata::default(),
                    ),
                },
                AuditChangeEntry {
                    key: ConfigKey::new(
                        "transport",
                        "handshake.peer_version",
                        ConfigScope::Session,
                        "对端声明的版本",
                    ),
                    value: ConfigValue::Text(
                        Cow::Owned(remote.to_string()),
                        ConfigMetadata::default(),
                    ),
                },
                AuditChangeEntry {
                    key: ConfigKey::new(
                        "transport",
                        "handshake.local_version",
                        ConfigScope::Session,
                        "本地声明的版本",
                    ),
                    value: ConfigValue::Text(
                        Cow::Owned(local.to_string()),
                        ConfigMetadata::default(),
                    ),
                },
            ],
            deleted: Vec::new(),
        };
        let event = AuditEventV1 {
            event_id: format!("{}:{}:{}->{}", self.entity_id, sequence, local, remote),
            sequence,
            entity: AuditEntityRef {
                kind: self.entity_kind.clone(),
                id: self.entity_id.clone(),
                labels: self.entity_labels.clone(),
            },
            action: self.success_action.clone(),
            state_prev_hash: prev_hash,
            state_curr_hash: new_hash.clone(),
            actor: self.actor.clone(),
            occurred_at,
            tsa_evidence: self.tsa_evidence.clone(),
            changes,
        };
        self.recorder.record(event).map_err(HandshakeError::audit)?;
        self.next_sequence = sequence + 1;
        self.chain_state = Some(new_hash);
        Ok(())
    }

    fn record_failure(
        &mut self,
        error: &HandshakeError,
        local: Version,
        remote: Version,
        occurred_at: u64,
    ) -> crate::Result<(), HandshakeError> {
        let prev_hash = self
            .chain_state
            .clone()
            .unwrap_or_else(|| ZERO_HASH.to_string());
        let sequence = self.next_sequence;
        let changes = AuditChangeSet {
            created: Vec::new(),
            updated: vec![
                AuditChangeEntry {
                    key: ConfigKey::new(
                        "transport",
                        "handshake.failure.reason",
                        ConfigScope::Session,
                        "失败原因",
                    ),
                    value: ConfigValue::Text(
                        Cow::Owned(error.to_string()),
                        ConfigMetadata::default(),
                    ),
                },
                AuditChangeEntry {
                    key: ConfigKey::new(
                        "transport",
                        "handshake.peer_version",
                        ConfigScope::Session,
                        "对端声明的版本",
                    ),
                    value: ConfigValue::Text(
                        Cow::Owned(remote.to_string()),
                        ConfigMetadata::default(),
                    ),
                },
                AuditChangeEntry {
                    key: ConfigKey::new(
                        "transport",
                        "handshake.local_version",
                        ConfigScope::Session,
                        "本地声明的版本",
                    ),
                    value: ConfigValue::Text(
                        Cow::Owned(local.to_string()),
                        ConfigMetadata::default(),
                    ),
                },
            ],
            deleted: Vec::new(),
        };
        let event = AuditEventV1 {
            event_id: format!(
                "{}:{}:{}->{}:failure",
                self.entity_id, sequence, local, remote
            ),
            sequence,
            entity: AuditEntityRef {
                kind: self.entity_kind.clone(),
                id: self.entity_id.clone(),
                labels: self.entity_labels.clone(),
            },
            action: self.failure_action.clone(),
            state_prev_hash: prev_hash.clone(),
            state_curr_hash: prev_hash,
            actor: self.actor.clone(),
            occurred_at,
            tsa_evidence: self.tsa_evidence.clone(),
            changes,
        };
        self.recorder.record(event).map_err(HandshakeError::audit)?;
        self.next_sequence = sequence + 1;
        Ok(())
    }
}

/// 执行版本与能力协商。
///
/// # 流程概述（How）
/// 1. 检查主版本兼容性；若不兼容返回 [`HandshakeErrorKind::MajorVersionMismatch`].
/// 2. 校验双方是否满足对方必选能力；缺失时分别返回 `LocalLacksRemoteRequirements` 或 `RemoteLacksLocalRequirements`。
/// 3. 计算版本最小值、能力交集与降级报告，生成 [`HandshakeOutcome`]。
/// 4. 若提供审计上下文，记录成功/失败事件；Recorder 报错会转换为 `AuditFailure`。
///
/// # 契约说明（What）
/// - **输入**：本地/远端宣告、事件发生时间戳（Unix 毫秒）、可选审计上下文。
/// - **返回**：成功时 [`HandshakeOutcome`]；失败时 [`HandshakeError`]。
/// - **副作用**：
///   - 审计上下文存在时，会将事件写入 Recorder，并维护哈希链；
///   - 协商失败不会修改上下文的链表 tip。
///
/// # 风险提示（Trade-offs）
/// - `occurred_at` 由调用方提供，需使用可信时间源。
/// - 若在热路径频繁调用，应复用 `NegotiationAuditContext`，避免重复分配标签向量。
pub fn negotiate(
    local: &HandshakeOffer,
    remote: &HandshakeOffer,
    occurred_at: u64,
    mut audit: Option<&mut NegotiationAuditContext>,
) -> crate::Result<HandshakeOutcome, HandshakeError> {
    if !local.version().is_compatible_with(&remote.version()) {
        let error = HandshakeError {
            kind: HandshakeErrorKind::MajorVersionMismatch {
                local: local.version(),
                remote: remote.version(),
            },
        };
        if let Some(ctx) = audit.as_mut() {
            ctx.record_failure(&error, local.version(), remote.version(), occurred_at)?;
        }
        return Err(error);
    }

    let remote_requirements = remote.mandatory().difference(local.total());
    if !remote_requirements.is_empty() {
        let error = HandshakeError {
            kind: HandshakeErrorKind::LocalLacksRemoteRequirements {
                missing: remote_requirements,
            },
        };
        if let Some(ctx) = audit.as_mut() {
            ctx.record_failure(&error, local.version(), remote.version(), occurred_at)?;
        }
        return Err(error);
    }

    let local_requirements = local.mandatory().difference(remote.total());
    if !local_requirements.is_empty() {
        let error = HandshakeError {
            kind: HandshakeErrorKind::RemoteLacksLocalRequirements {
                missing: local_requirements,
            },
        };
        if let Some(ctx) = audit.as_mut() {
            ctx.record_failure(&error, local.version(), remote.version(), occurred_at)?;
        }
        return Err(error);
    }

    let negotiated_version = cmp::min(local.version(), remote.version());
    let enabled = local.total().intersection(remote.total());
    let downgrade = DowngradeReport::new(
        local.optional().difference(enabled),
        remote.optional().difference(enabled),
    );
    let outcome = HandshakeOutcome::new(negotiated_version, enabled, downgrade);

    if let Some(ctx) = audit.as_mut() {
        ctx.record_success(&outcome, local.version(), remote.version(), occurred_at)?;
    }

    Ok(outcome)
}

fn compute_state_hash(outcome: &HandshakeOutcome) -> String {
    let mut hasher = Sha256::new();
    hasher.update(outcome.version().major().to_le_bytes());
    hasher.update(outcome.version().minor().to_le_bytes());
    hasher.update(outcome.version().patch().to_le_bytes());
    hasher.update(outcome.capabilities().bits().to_le_bytes());
    hasher.update(outcome.downgrade().local().bits().to_le_bytes());
    hasher.update(outcome.downgrade().remote().bits().to_le_bytes());
    let digest = hasher.finalize();
    hex_from_bytes(&digest)
}

fn hex_from_bytes(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX_TABLE[(byte >> 4) as usize] as char);
        out.push(HEX_TABLE[(byte & 0x0F) as usize] as char);
    }
    out
}

impl From<HandshakeError> for SparkError {
    fn from(error: HandshakeError) -> Self {
        error.into_spark_error()
    }
}
