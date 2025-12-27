//! 传输握手协商模块。
//!
//! # 模块定位（Why）
//! - **新增能力的安全演进**：围绕 T22 需求，引入显式版本与能力位图，使协议升级能够在不中断旧实现的前提下协商降级。
//! - **跨模块协作**：协商结果直接服务于传输工厂、服务端监听器等调用方，提供标准化结构体。
//! - **面向多环境**：所有类型与算法均兼容 `no_std + alloc` 场景，同时保留足够的扩展点支持云/边缘部署差异。
//!
//! # 核心组成（What）
//! - [`Version`]：封装语义化版本 `{major, minor, patch}`，用于判定向后兼容性。
//! - [`Capability`] 与 [`CapabilityBitmap`]：以位图形式声明能力集合，支持内建常量与自定义索引。
//! - [`HandshakeOffer`] / [`HandshakeOutcome`]：描述双方宣告与协商结果，并指出降级的位图差异。
//! - [`negotiate`]：执行实际的版本/能力协商，并返回可感知的降级报告。
//!
//! # 协作方式（How）
//! 1. 调用方向两侧收集版本与能力，构造 [`HandshakeOffer`]。
//! 2. 调用 [`negotiate`] 获得 [`HandshakeOutcome`] 或 [`HandshakeError`]。
//! 3. 依据 [`DowngradeReport`] 判断是否需要启用兼容策略或告警。
//!
//! # 风险提示（Trade-offs）
//! - 位图仅支持 128 个能力位；若需更大空间需在未来引入分片或变长编码。
//! - 所有 `custom` 能力索引约定 `< 128`，若越界会触发 panic；请在注册表中统一分配编号。
//! - 模块不再内置审计写入；需要合规链路的调用方应在外层自行记录关键字段。

use crate::{SparkError, error::codes};
use alloc::format;
use core::{
    cmp::{self, Ordering},
    fmt,
};

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
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum HandshakeErrorKind {
    MajorVersionMismatch { local: Version, remote: Version },
    LocalLacksRemoteRequirements { missing: CapabilityBitmap },
    RemoteLacksLocalRequirements { missing: CapabilityBitmap },
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
        }
    }
}

impl crate::Error for HandshakeError {
    #[allow(unused_parens)]
    fn source(&self) -> Option<&(dyn crate::Error + 'static)> {
        None
    }
}

/// 执行版本与能力协商。
///
/// # 流程概述（How）
/// 1. 检查主版本兼容性；若不兼容返回 [`HandshakeErrorKind::MajorVersionMismatch`].
/// 2. 校验双方是否满足对方必选能力；缺失时分别返回 `LocalLacksRemoteRequirements` 或 `RemoteLacksLocalRequirements`。
/// 3. 计算版本最小值、能力交集与降级报告，生成 [`HandshakeOutcome`]。
///
/// # 契约说明（What）
/// - **输入**：本地/远端宣告。
/// - **返回**：成功时 [`HandshakeOutcome`]；失败时 [`HandshakeError`]。
///
/// # 风险提示（Trade-offs）
/// - 协商仅依赖双方声明的静态能力，若运行时还存在动态访问控制或租户隔离要求，调用方需在外层补充校验与记录。
pub fn negotiate(
    local: &HandshakeOffer,
    remote: &HandshakeOffer,
) -> crate::Result<HandshakeOutcome, HandshakeError> {
    if !local.version().is_compatible_with(&remote.version()) {
        let error = HandshakeError {
            kind: HandshakeErrorKind::MajorVersionMismatch {
                local: local.version(),
                remote: remote.version(),
            },
        };
        return Err(error);
    }

    let remote_requirements = remote.mandatory().difference(local.total());
    if !remote_requirements.is_empty() {
        let error = HandshakeError {
            kind: HandshakeErrorKind::LocalLacksRemoteRequirements {
                missing: remote_requirements,
            },
        };
        return Err(error);
    }

    let local_requirements = local.mandatory().difference(remote.total());
    if !local_requirements.is_empty() {
        let error = HandshakeError {
            kind: HandshakeErrorKind::RemoteLacksLocalRequirements {
                missing: local_requirements,
            },
        };
        return Err(error);
    }

    let negotiated_version = cmp::min(local.version(), remote.version());
    let enabled = local.total().intersection(remote.total());
    let downgrade = DowngradeReport::new(
        local.optional().difference(enabled),
        remote.optional().difference(enabled),
    );
    let outcome = HandshakeOutcome::new(negotiated_version, enabled, downgrade);
    Ok(outcome)
}

impl From<HandshakeError> for SparkError {
    fn from(error: HandshakeError) -> Self {
        error.into_spark_error()
    }
}
