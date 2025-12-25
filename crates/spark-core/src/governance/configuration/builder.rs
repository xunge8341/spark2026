use alloc::borrow::Cow;
use alloc::boxed::Box;
use alloc::collections::{BTreeMap, BTreeSet};
use alloc::format;
use alloc::string::{String, ToString};
use alloc::vec::Vec;
use core::{
    fmt,
    pin::Pin,
    sync::atomic::{AtomicU64, Ordering},
    task::{Context, Poll},
};

use crate::{
    audit::{AuditChangeSet, AuditEntityRef, AuditEventV1, AuditPipeline, AuditStateHasher},
    future::{BoxStream, Stream},
};

use super::{
    ChangeEvent, ChangeNotification, ChangeSet, ConfigDelta, ConfigKey, ConfigValue,
    ConfigurationError, ConfigurationErrorKind, ConfigurationLayer, ConfigurationSnapshot,
    DynConfigurationSource, ProfileDescriptor, ProfileId, ProfileLayering, SourceRegistrationError,
};

/// 已解析的配置结果。
///
/// ### 设计目的（Why）
/// - 将多个配置层按优先顺序合并为最终映射，供运行时快速读取。
/// - 采用 [`BTreeMap`] 保证遍历顺序稳定，便于日志与调试对齐，并为差异化比较提供确定性输出。
///
/// ### 契约定义（What）
/// - `values`：键到值的映射，后注册的高优先级层会覆盖低优先级层。
/// - `version`：递增版本号，用于与通知事件对齐。
///
/// ### BTreeMap 取舍（Trade-offs）
/// - 与 `HashMap` 相比，`BTreeMap` 的写入/合并为 `O(log n)`，在大规模配置层叠加时会稍慢；换来的是确定性序列化便于做快照哈希与审计。
/// - 若调用方在热路径需要更快的随机访问，可将 `values` 克隆为 `HashMap` 进行缓存，或在增量通知中仅同步差异集合以减轻重建成本。
#[derive(Clone, Debug, PartialEq)]
pub struct ResolvedConfiguration {
    pub values: BTreeMap<ConfigKey, ConfigValue>,
    pub version: u64,
}

/// 维护分层配置的句柄。
///
/// ### 设计目的（Why）
/// - 提供对最新配置的读取能力，并暴露订阅接口供运行时监听。
/// - 借鉴 Envoy 的 `ConfigSubscription`，将变更管道化。
///
/// ### 契约说明（What）
/// - `profile`：当前句柄绑定的 Profile。
/// - `layers`：存放原始 Layer，保持可追溯性。
/// - `audit`：可选的审计流水线，当存在时在变更后写入哈希链事件。
/// - `audit_chain_tip`：最近一次成功写入的状态哈希，用于检测事件缺失。
///
pub struct LayeredConfiguration {
    profile: ProfileId,
    layering: ProfileLayering,
    layers: Vec<ConfigurationLayer>,
    version: AtomicU64,
    audit: Option<AuditPipeline>,
    audit_chain_tip: Option<String>,
}

/// 配置更新的分类，区分增量通知与全量刷新。
///
/// ### 设计目的（Why）
/// - 运行时在处理更新时通常需要区分“局部差异”与“全量替换”，该枚举提供显式标签，避免魔法布尔值。
///
/// ### 契约说明（What）
/// - `Incremental`：包含原始 [`ChangeNotification`] 与解析好的 [`ChangeSet`]，便于调用方获取细粒度差异。
/// - `Refresh`：表示底层层次被整体替换，调用方应直接使用新快照覆盖缓存。
#[derive(Clone, Debug, PartialEq)]
pub enum ConfigurationUpdateKind {
    Incremental {
        notification: ChangeNotification,
        change_set: ChangeSet,
    },
    Refresh,
}

/// 热更新流的单次输出。
///
/// ### 设计目的（Why）
/// - 将最新解析快照与触发事件绑定在一起，方便调用方原子地读取配置与元信息。
///
/// ### 契约说明（What）
/// - `snapshot`：变更应用后的完整配置视图，版本号保证单调递增。
/// - `kind`：指示触发事件类型，见 [`ConfigurationUpdateKind`]。
#[derive(Clone, Debug, PartialEq)]
pub struct ConfigurationUpdate {
    pub snapshot: ResolvedConfiguration,
    pub kind: ConfigurationUpdateKind,
}

/// 聚合多源增量通知的流式驱动器。
///
/// ### 设计目的（Why）
/// - 将多个配置源的 `watch` 输出合并为单一 `Stream`，避免调用方手动管理多路选择与并发。
/// - 与 [`ConfigurationHandle`] 紧密协作：内部直接持有 [`LayeredConfiguration`] 的可变引用，以便原子更新快照。
///
/// ### 契约说明（What）
/// - 实现 [`Stream`]，每次轮询最多处理一个事件，保持顺序与源侧一致。
/// - 所有源结束后流即结束；若中途出现错误，会以 `Err` 事件向外传播。
pub struct ConfigurationWatch<'a> {
    layered: &'a mut LayeredConfiguration,
    streams: Vec<Option<BoxStream<'a, ConfigDelta>>>,
    cursor: usize,
}

impl<'a> ConfigurationWatch<'a> {
    fn new(
        layered: &'a mut LayeredConfiguration,
        streams: Vec<BoxStream<'a, ConfigDelta>>,
    ) -> Self {
        let wrapped = streams.into_iter().map(Some).collect();
        Self {
            layered,
            streams: wrapped,
            cursor: 0,
        }
    }
}

impl<'a> Stream for ConfigurationWatch<'a> {
    type Item = crate::Result<ConfigurationUpdate, ConfigurationError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        if this.streams.is_empty() {
            return Poll::Ready(None);
        }

        let len = this.streams.len();

        for offset in 0..len {
            let index = (this.cursor + offset) % len;
            let stream_slot = &mut this.streams[index];
            let Some(stream) = stream_slot.as_mut() else {
                continue;
            };

            match stream.as_mut().poll_next(cx) {
                Poll::Ready(Some(delta)) => {
                    this.cursor = (index + 1) % len;
                    match delta {
                        ConfigDelta::Change(notification) => {
                            match this.layered.apply_change(notification.clone()) {
                                Ok(change_set) => {
                                    let snapshot = this.layered.resolve();
                                    let update = ConfigurationUpdate {
                                        snapshot,
                                        kind: ConfigurationUpdateKind::Incremental {
                                            notification,
                                            change_set,
                                        },
                                    };
                                    return Poll::Ready(Some(Ok(update)));
                                }
                                Err(error) => return Poll::Ready(Some(Err(error))),
                            }
                        }
                        ConfigDelta::Refresh(layers) => match this.layered.apply_refresh(layers) {
                            Ok(snapshot) => {
                                let update = ConfigurationUpdate {
                                    snapshot,
                                    kind: ConfigurationUpdateKind::Refresh,
                                };
                                return Poll::Ready(Some(Ok(update)));
                            }
                            Err(error) => return Poll::Ready(Some(Err(error))),
                        },
                    }
                }
                Poll::Ready(None) => {
                    *stream_slot = None;
                }
                Poll::Pending => {}
            }
        }

        if this.streams.iter().all(|entry| entry.is_none()) {
            Poll::Ready(None)
        } else {
            Poll::Pending
        }
    }
}

impl LayeredConfiguration {
    /// 创建一个新的分层配置实例。
    ///
    /// ### 设计意图（Why）
    /// - Builder 在汇总完初始 Layer 后需要一个承载体保存运行时状态。
    /// - 独立构造函数可让测试或自定义装配流程绕过 `ConfigurationBuilder`。
    ///
    /// ### 契约说明（What）
    /// - **输入**：`profile` 指定当前配置面向的运行环境；`layering` 确定合并策略。
    /// - **前置条件**：调用方需确保 `profile` 的依赖已在更高层完成拓扑排序。
    /// - **后置条件**：返回的实例内部 Layer 列表为空，版本号初始化为 `0`。
    ///
    /// ### 逻辑概要（How）
    /// - 通过简单字段赋值构建结构体，避免隐藏副作用。
    pub fn new(profile: ProfileId, layering: ProfileLayering) -> Self {
        Self {
            profile,
            layering,
            layers: Vec::new(),
            version: AtomicU64::new(0),
            audit: None,
            audit_chain_tip: None,
        }
    }

    /// 注册一个配置层。
    ///
    /// ### 设计意图（Why）
    /// - 支持 Builder 或增量同步在运行时追加新的 Layer，例如引入更高优先级的租户级配置。
    ///
    /// ### 契约（What）
    /// - **前置条件**：调用者应按照 `SourceMetadata.priority` 从低到高调用，以保证覆盖顺序正确。
    /// - **后置条件**：Layer 被追加到内部列表末尾，不进行去重。
    ///
    /// ### 注意事项（Trade-offs）
    /// - 若未遵循优先级顺序，后续 `resolve` 仍能工作，但可能产生意料之外的覆盖结果。
    pub fn push_layer(&mut self, layer: ConfigurationLayer) {
        self.layers.push(layer);
    }

    /// 返回当前 Profile。
    #[inline]
    pub fn profile(&self) -> &ProfileId {
        &self.profile
    }

    /// 合并所有 Layer，并返回最新快照。
    ///
    /// ### 逻辑解析（How）
    /// - 根据 `layering` 决定遍历顺序。
    /// - 遍历过程中将键值写入 `BTreeMap`，后写入者覆盖先前值。
    /// - 版本号由增量变更驱动，`resolve` 仅返回当前版本号，避免纯读取导致的虚假变更。
    pub fn resolve(&self) -> ResolvedConfiguration {
        let mut values = BTreeMap::new();
        match self.layering {
            ProfileLayering::BaseFirst => {
                for layer in &self.layers {
                    for (key, value) in &layer.entries {
                        values.insert(key.clone(), value.clone());
                    }
                }
            }
            ProfileLayering::OverrideFirst => {
                for layer in self.layers.iter().rev() {
                    for (key, value) in &layer.entries {
                        values.insert(key.clone(), value.clone());
                    }
                }
            }
        }
        let version = self.version.load(Ordering::SeqCst);
        ResolvedConfiguration { values, version }
    }
}

/// Builder 构建过程的校验状态。
///
/// ### 设计动机（Why）
/// - 构建流程需要在多个阶段输出“是否通过”的判断，便于在审计或错误分析时定位失败环节。
/// - 使用显式枚举而非布尔值，可为未来扩展（例如 `Warn`、`Skipped`）预留空间。
///
/// ### 契约说明（What）
/// - `Passed`：该检查已通过；
/// - `Failed`：该检查未通过且构建流程被终止。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ValidationState {
    Passed,
    Failed,
}

/// 单条校验记录，描述构建过程中某个检查点的结果。
///
/// ### 设计意图（Why）
/// - 将检查名与简短说明绑定，方便调用方将报告渲染为表格或诊断日志。
/// - 采用 `Cow<'static, str>` 避免频繁分配，同时允许在运行时拼接索引信息。
///
/// ### 契约（What）
/// - `check`：机器可读的检查标识，推荐使用 `.` 分隔层级（如 `profile.present`）。
/// - `state`：检查结果，见 [`ValidationState`]。
/// - `detail`：面向人类的解释，禁止包含敏感内容；本模块仅拼接索引数字，确保符合脱敏要求。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ValidationFinding {
    check: Cow<'static, str>,
    state: ValidationState,
    detail: Cow<'static, str>,
}

impl ValidationFinding {
    /// 构造校验记录。
    pub fn new<C, D>(check: C, state: ValidationState, detail: D) -> Self
    where
        C: Into<Cow<'static, str>>,
        D: Into<Cow<'static, str>>,
    {
        Self {
            check: check.into(),
            state,
            detail: detail.into(),
        }
    }

    /// 返回检查标识。
    #[inline]
    pub fn check(&self) -> &str {
        &self.check
    }

    /// 返回检查状态。
    #[inline]
    pub fn state(&self) -> ValidationState {
        self.state
    }

    /// 返回详细说明。
    #[inline]
    pub fn detail(&self) -> &str {
        &self.detail
    }
}

/// Builder 构建阶段的校验报告集合。
///
/// ### 设计动机（Why）
/// - 提供“教案级”的运行轨迹，帮助调试者理解每一步做了什么、结果如何。
/// - 结合 [`BuildError`] 可输出部分成功的检查项，增强诊断透明度。
///
/// ### 契约说明（What）
/// - `findings` 保持按添加顺序存储，方便审计时对应构建步骤。
/// - `record_*` 方法在调用后立即返回 `&mut Self`，支持链式书写。
#[derive(Clone, Default, PartialEq, Eq)]
pub struct ValidationReport {
    findings: Vec<ValidationFinding>,
}

impl ValidationReport {
    /// 创建空的校验报告。
    pub fn new() -> Self {
        Self {
            findings: Vec::new(),
        }
    }

    /// 记录一次检查结果。
    pub fn push(&mut self, finding: ValidationFinding) {
        self.findings.push(finding);
    }

    /// 记录通过的检查。
    pub fn record_pass<C, D>(&mut self, check: C, detail: D)
    where
        C: Into<Cow<'static, str>>,
        D: Into<Cow<'static, str>>,
    {
        self.push(ValidationFinding::new(
            check,
            ValidationState::Passed,
            detail,
        ));
    }

    /// 记录失败的检查。
    pub fn record_fail<C, D>(&mut self, check: C, detail: D)
    where
        C: Into<Cow<'static, str>>,
        D: Into<Cow<'static, str>>,
    {
        self.push(ValidationFinding::new(
            check,
            ValidationState::Failed,
            detail,
        ));
    }

    /// 返回全部检查记录。
    #[inline]
    pub fn findings(&self) -> &[ValidationFinding] {
        &self.findings
    }

    /// 统计通过的检查数。
    pub fn passed_count(&self) -> usize {
        self.findings
            .iter()
            .filter(|finding| finding.state == ValidationState::Passed)
            .count()
    }

    /// 统计失败的检查数。
    pub fn failed_count(&self) -> usize {
        self.findings
            .iter()
            .filter(|finding| finding.state == ValidationState::Failed)
            .count()
    }
}

impl fmt::Debug for ValidationReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ValidationReport")
            .field("total", &self.findings.len())
            .field("passed", &self.passed_count())
            .field("failed", &self.failed_count())
            .finish()
    }
}

impl fmt::Display for ValidationReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "validation findings: total={}, passed={}, failed={}",
            self.findings.len(),
            self.passed_count(),
            self.failed_count()
        )
    }
}

/// Builder 成功后的辅助报告，包含校验结果与脱敏快照。
///
/// ### 设计意图（Why）
/// - 将成功构建的上下文信息统一打包，避免调用方再去收集校验日志或额外生成快照。
/// - 通过携带 [`ValidationReport`] 与 [`ConfigurationSnapshot`]，让实验室可以重放当时的输入条件。
///
/// ### 契约说明（What）
/// - `validation`：构建过程中执行的所有检查记录。
/// - `snapshot`：脱敏后的配置快照，仅包含安全可公开的信息。
///
/// ### 实现细节（How）
/// - 构造函数 [`BuildReport::new`] 接受所有权，保证报告在返回后只读。
/// - `Display` 实现仅输出概要（层数、条目数），防止默认日志记录泄露详细配置。
///
/// ### 风险提示（Trade-offs）
/// - 若调用方需要原始敏感值，应在可信环境手动抓取，`BuildReport` 不会存储任何明文凭据。
#[derive(Clone, Debug)]
pub struct BuildReport {
    validation: ValidationReport,
    snapshot: ConfigurationSnapshot,
}

impl BuildReport {
    /// 构造报告。
    pub fn new(validation: ValidationReport, snapshot: ConfigurationSnapshot) -> Self {
        Self {
            validation,
            snapshot,
        }
    }

    /// 访问校验报告。
    #[inline]
    pub fn validation(&self) -> &ValidationReport {
        &self.validation
    }

    /// 访问脱敏快照。
    #[inline]
    pub fn snapshot(&self) -> &ConfigurationSnapshot {
        &self.snapshot
    }
}

impl fmt::Display for BuildReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "build report ({} entries, {} layers)",
            self.snapshot.entries.len(),
            self.snapshot.layers.len()
        )
    }
}

/// 构建过程中可能出现的阶段划分，用于强化错误定位。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BuildErrorStage {
    Validation,
    Load,
    Finalize,
}

impl fmt::Display for BuildErrorStage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let label = match self {
            BuildErrorStage::Validation => "validation",
            BuildErrorStage::Load => "load",
            BuildErrorStage::Finalize => "finalize",
        };
        f.write_str(label)
    }
}

/// 构建失败的统一错误种类。
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BuildErrorKind {
    MissingProfile,
    MissingSources,
    ProfileExtendsDuplicate,
    ProfileExtendsSelfReference,
    SourceLoadFailure { index: usize },
}

impl fmt::Display for BuildErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BuildErrorKind::MissingProfile => f.write_str("profile descriptor is required"),
            BuildErrorKind::MissingSources => {
                f.write_str("at least one configuration source is required")
            }
            BuildErrorKind::ProfileExtendsDuplicate => {
                f.write_str("profile extends list contains duplicate entries")
            }
            BuildErrorKind::ProfileExtendsSelfReference => {
                f.write_str("profile cannot extend itself")
            }
            BuildErrorKind::SourceLoadFailure { index } => {
                write!(f, "source[{index}] load failed")
            }
        }
    }
}

/// 构建失败时返回的统一错误。
///
/// ### 设计意图（Why）
/// - 用结构化字段标识失败阶段与具体原因，支撑“随机错误注入”实验统计覆盖率。
/// - 携带 [`ValidationReport`]，即使失败也能还原已执行的检查步骤，帮助快速定位问题。
///
/// ### 契约说明（What）
/// - `kind`：错误类别，禁止包含敏感字段；
/// - `stage`：失败发生的阶段，用于聚合指标；
/// - `report`：截至失败时的检查结果；
/// - `cause`：可选的底层 [`ConfigurationError`]，保留具体实现原因。
///
/// ### 实现细节（How）
/// - 内部构造器 `new` 接受 `Option<ConfigurationError>`，保持与源错误的松耦合。
/// - `Debug` 输出仅包含结构化摘要，不会打印底层 `cause` 的明文上下文。
///
/// ### 风险提示（Trade-offs）
/// - 若底层 `ConfigurationError` 携带敏感上下文，调用方应避免直接格式化其 `Display` 输出。
pub struct BuildError {
    kind: BuildErrorKind,
    stage: BuildErrorStage,
    report: ValidationReport,
    cause: Option<ConfigurationError>,
}

impl BuildError {
    fn new(
        kind: BuildErrorKind,
        stage: BuildErrorStage,
        report: ValidationReport,
        cause: Option<ConfigurationError>,
    ) -> Self {
        Self {
            kind,
            stage,
            report,
            cause,
        }
    }

    /// 错误类型。
    #[inline]
    pub fn kind(&self) -> &BuildErrorKind {
        &self.kind
    }

    /// 失败阶段。
    #[inline]
    pub fn stage(&self) -> BuildErrorStage {
        self.stage
    }

    /// 已执行的校验报告。
    #[inline]
    pub fn report(&self) -> &ValidationReport {
        &self.report
    }

    /// 可选的底层原因。
    #[inline]
    pub fn cause(&self) -> Option<&ConfigurationError> {
        self.cause.as_ref()
    }
}

impl fmt::Debug for BuildError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BuildError")
            .field("kind", &self.kind)
            .field("stage", &self.stage)
            .field("report", &self.report)
            .finish()
    }
}

impl fmt::Display for BuildError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "builder failure during {}: {}", self.stage, self.kind)
    }
}

impl crate::Error for BuildError {
    #[allow(unused_parens)]
    fn source(&self) -> Option<&(dyn crate::Error + 'static)> {
        self.cause.as_ref().map(|error| error as &dyn crate::Error)
    }
}

/// 构建成功后的完整结果。
pub struct BuildOutcome {
    pub handle: ConfigurationHandle,
    pub initial: ResolvedConfiguration,
    pub report: BuildReport,
}

impl fmt::Debug for BuildOutcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BuildOutcome")
            .field("initial_version", &self.initial.version)
            .field("entry_count", &self.initial.values.len())
            .field("report", &self.report)
            .finish()
    }
}

/// Builder 负责组织 Profile 与数据源，生成最终的 [`ConfigurationHandle`]。
#[derive(Default)]
pub struct ConfigurationBuilder {
    profile: Option<ProfileDescriptor>,
    sources: Vec<Box<dyn DynConfigurationSource>>, // 采用 trait object 以兼容多种实现
    capacity: Option<usize>,
    audit: Option<AuditPipeline>,
}

impl ConfigurationBuilder {
    /// 创建新的 Builder。
    ///
    /// ### 设计意图（Why）
    /// - 提供零成本的构造方式，方便调用方按需配置数据源、Profile 等参数。
    /// - 对标业内 Builder 模式（如 Envoy bootstrap、Spring ConfigBuilder），提升可读性。
    ///
    /// ### 契约说明（What）
    /// - 返回的 Builder 内部尚未设置 Profile，也未注册任何配置源。
    /// - 可直接链式调用后续 `with_*` 方法。
    ///
    /// ### 实现细节（How）
    /// - 直接调用 `Default`，确保字段初始化为空集合。
    pub fn new() -> Self {
        Self::default()
    }

    /// 限制最多可注册的数据源数量。
    ///
    /// ### 设计意图（Why）
    /// - 借鉴 AWS AppConfig 对“配置源数量”限制，防止误配置导致内存膨胀。
    ///
    /// ### 契约说明（What）
    /// - **输入**：`capacity` 为允许注册的最大源数量。
    /// - **后置条件**：若后续注册超出该数量，将返回 [`SourceRegistrationError::Capacity`]。
    ///
    /// ### 设计取舍（Trade-offs）
    /// - 仅限制数量，不限制来源类型，保持扩展自由度。
    pub fn with_capacity(mut self, capacity: usize) -> Self {
        self.capacity = Some(capacity);
        self
    }

    /// 注入审计流水线。
    ///
    /// ### 设计意图（Why）
    /// - 将审计记录逻辑与核心 Builder 解耦，允许调用方按需插拔不同的 Recorder 实现。
    ///
    /// ### 契约说明（What）
    /// - **输入**：[`AuditPipeline`]，包含 Recorder 与静态上下文信息。
    /// - **后置条件**：后续调用 `build` 时，增量变更会生成 [`AuditEventV1`](crate::audit::AuditEventV1)。
    pub fn with_audit_pipeline(mut self, pipeline: AuditPipeline) -> Self {
        self.audit = Some(pipeline);
        self
    }

    /// 指定要构建的 Profile。
    ///
    /// ### 设计意图（Why）
    /// - Profile 决定合并策略与继承关系，是装配配置的核心前提。
    ///
    /// ### 契约说明（What）
    /// - **输入**：[`ProfileDescriptor`]，包含 identifier、extends、layering 等信息。
    /// - **后置条件**：Builder 内部保存该 Profile，用于 `build` 阶段解析。
    ///
    /// ### 风险提示（Trade-offs）
    /// - 当前不会校验 `extends` 的循环依赖，建议在生成 Profile 描述时完成验证。
    pub fn with_profile(mut self, descriptor: ProfileDescriptor) -> Self {
        self.profile = Some(descriptor);
        self
    }

    /// 注册新的配置源。
    ///
    /// ### 设计意图（Why）
    /// - 允许调用者按需组合本地文件、集中式配置中心等多种来源，实现分层治理。
    ///
    /// ### 契约说明（What）
    /// - **输入**：实现了 [`ConfigurationSource`](crate::configuration::ConfigurationSource) 的对象，并通过 [`DynConfigurationSource`] 装箱。
    /// - **前置条件**：若设置了 `capacity`，需保证未超过上限；Builder 会检测重复实例。
    /// - **后置条件**：成功注册后，数据源被保存供 `build` 阶段调用。
    ///
    /// ### 逻辑解析（How）
    /// - 首先检查容量限制，再通过指针比较避免同一实例重复注册。
    /// - 比较策略采用指针相等判断，确保不会误判两个内容相同但实例不同的源。
    ///
    /// ### 风险提示（Trade-offs）
    /// - 指针比较不会检测“逻辑重复”，若两个不同实例访问同一远程配置中心，上层需自行治理。
    pub fn register_source(
        &mut self,
        source: Box<dyn DynConfigurationSource>,
    ) -> crate::Result<(), SourceRegistrationError> {
        if self
            .capacity
            .is_some_and(|limit| self.sources.len() >= limit)
        {
            return Err(SourceRegistrationError::Capacity);
        }

        // 这里只进行简单的指针比较，调用方需自行保证不会注册重复实例。
        let new_ptr: *const dyn DynConfigurationSource = &*source;
        if self.sources.iter().any(|existing| {
            let existing_ptr: *const dyn DynConfigurationSource = &**existing;
            core::ptr::addr_eq(existing_ptr, new_ptr)
        }) {
            return Err(SourceRegistrationError::Duplicate);
        }

        self.sources.push(source);
        Ok(())
    }

    /// 注册 `'static` 生命周期的配置源引用，复用借用型入口。
    ///
    /// ### 设计动机（Why）
    /// - 某些内建数据源以单例形式存在（如内存快照、只读嵌入资源），调用方更倾向于共享引用；
    /// - 通过本方法可避免重复分配 `Box`，并在 API 层显式标注生命周期假设。
    ///
    /// ### 契约说明（What）
    /// - **输入**：`source` 必须在进程生命周期内有效；
    /// - **执行**：内部借助 `super::source::boxed_static_source` 适配为拥有型对象，再复用 [`Self::register_source`] 的去重与容量检查；
    /// - **后置条件**：Builder 仅持有对单例的引用包装，不负责析构。
    pub fn register_source_static(
        &mut self,
        source: &'static dyn DynConfigurationSource,
    ) -> crate::Result<(), SourceRegistrationError> {
        self.register_source(super::source::boxed_static_source(source))
    }

    /// 构建最终的 [`ConfigurationHandle`] 并产出审计友好的报告。
    ///
    /// ### 设计意图（Why）
    /// - 在统一入口完成参数校验、数据源装载、快照生成，降低调用方复用成本。
    /// - 输出 [`BuildReport`] 与 [`ConfigurationSnapshot`]，支撑审计、复现实验与回放。
    ///
    /// ### 契约说明（What）
    /// - **前置条件**：
    ///   1. 必须通过 [`ConfigurationBuilder::with_profile`] 指定 Profile；
    ///   2. 至少注册一个数据源；
    ///   3. Profile 的 `extends` 不得包含重复条目或自引用。
    /// - **后置条件**：
    ///   1. 返回 [`BuildOutcome`]，包含配置句柄、首个解析结果与脱敏快照；
    ///   2. 若失败则返回 [`BuildError`]，错误中附带已执行的校验记录。
    ///
    /// ### 执行流程（How）
    /// 1. 执行参数校验，生成 [`ValidationReport`]。
    /// 2. 依次调用每个数据源的 [`DynConfigurationSource::load_dyn`], 收集所有 Layer。
    /// 3. 按优先级排序 Layer，写入 [`LayeredConfiguration`] 并计算初始版本。
    /// 4. 基于最终结果生成脱敏快照，打包成 [`BuildOutcome`] 返回。
    ///
    /// ### 风险提示（Trade-offs）
    /// - Layer 排序后仍可能存在重复键；后写入的值会覆盖先前值，这是预期行为。
    /// - 快照始终脱敏加密字段，若业务需要调试原文，需要在可信环境额外导出。
    pub fn build(self) -> crate::Result<BuildOutcome, BuildError> {
        let ConfigurationBuilder {
            profile,
            sources,
            capacity: _,
            audit,
        } = self;
        let mut report = ValidationReport::new();

        let descriptor = match profile {
            Some(profile) => {
                report.record_pass("profile.present", "profile descriptor provided");
                profile
            }
            None => {
                report.record_fail("profile.present", "profile descriptor missing");
                return Err(BuildError::new(
                    BuildErrorKind::MissingProfile,
                    BuildErrorStage::Validation,
                    report,
                    None,
                ));
            }
        };

        // 检查 extends 不出现重复或自引用，保障层级拓扑可判定。
        let mut seen_extends = BTreeSet::new();
        let mut duplicate_found = false;
        for extend in &descriptor.extends {
            if extend == &descriptor.identifier {
                report.record_fail("profile.extends.self", "profile cannot extend itself");
                return Err(BuildError::new(
                    BuildErrorKind::ProfileExtendsSelfReference,
                    BuildErrorStage::Validation,
                    report,
                    None,
                ));
            }
            if !seen_extends.insert(extend.as_str()) {
                duplicate_found = true;
                break;
            }
        }
        if duplicate_found {
            report.record_fail(
                "profile.extends.unique",
                "profile extends list contains duplicate entries",
            );
            return Err(BuildError::new(
                BuildErrorKind::ProfileExtendsDuplicate,
                BuildErrorStage::Validation,
                report,
                None,
            ));
        } else {
            report.record_pass(
                "profile.extends.unique",
                format!("{} referenced profile(s)", descriptor.extends.len()),
            );
            report.record_pass("profile.extends.self", "profile extends list excludes self");
        }

        if sources.is_empty() {
            report.record_fail("sources.present", "no configuration sources registered");
            return Err(BuildError::new(
                BuildErrorKind::MissingSources,
                BuildErrorStage::Validation,
                report,
                None,
            ));
        } else {
            report.record_pass(
                "sources.present",
                format!("{} configuration source(s) registered", sources.len()),
            );
        }

        let mut layers = Vec::new();
        for (index, source) in sources.iter().enumerate() {
            match source.load_dyn(&descriptor.identifier) {
                Ok(mut fetched) => {
                    report.record_pass(
                        format!("source.load[{index}]"),
                        format!("loaded {} layer(s)", fetched.len()),
                    );
                    layers.append(&mut fetched);
                }
                Err(error) => {
                    report.record_fail(format!("source.load[{index}]"), "load failed");
                    return Err(BuildError::new(
                        BuildErrorKind::SourceLoadFailure { index },
                        BuildErrorStage::Load,
                        report,
                        Some(error),
                    ));
                }
            }
        }

        // 按 priority 从低到高排序，确保高优先级覆盖低优先级。
        layers.sort_by_key(|layer| layer.metadata.priority);
        report.record_pass(
            "assembly.layers.sorted",
            format!("{} layer(s) sorted by priority", layers.len()),
        );

        let snapshot_layers = layers.clone();
        let mut layered =
            LayeredConfiguration::new(descriptor.identifier.clone(), descriptor.layering);
        for layer in layers {
            layered.push_layer(layer);
        }
        report.record_pass(
            "assembly.layers.applied",
            "layers committed to layered configuration",
        );

        layered.version.store(1, Ordering::SeqCst);
        let resolved = layered.resolve();
        report.record_pass("assembly.resolve", "initial configuration resolved");

        let snapshot = ConfigurationSnapshot::capture(&descriptor, &snapshot_layers, &resolved);
        report.record_pass("snapshot.capture", "redacted snapshot generated");

        let initial_hash = AuditStateHasher::hash_configuration(resolved.values.iter());
        if let Some(pipeline) = audit.clone() {
            layered.audit = Some(pipeline);
        }
        layered.audit_chain_tip = Some(initial_hash);

        let build_report = BuildReport::new(report, snapshot);
        let handle = ConfigurationHandle {
            layered,
            descriptor,
            sources,
        };

        Ok(BuildOutcome {
            handle,
            initial: resolved,
            report: build_report,
        })
    }
}

/// 对外暴露的配置句柄。
///
/// ### 设计目的（Why）
/// - 提供配置读取与变更订阅接口，是运行时访问配置的入口。
/// - 参考 Netflix Archaius、Spring Cloud Config 的 `Environment` 概念。
pub struct ConfigurationHandle {
    pub(crate) layered: LayeredConfiguration,
    pub(crate) descriptor: ProfileDescriptor,
    pub(crate) sources: Vec<Box<dyn DynConfigurationSource>>,
}

impl ConfigurationHandle {
    /// 返回 Profile 描述。
    ///
    /// ### 契约说明（What）
    /// - **输出**：构建时传入的 [`ProfileDescriptor`] 引用，包含 layering 等信息。
    /// - 调用方可用于查询当前配置所属环境或渲染文档。
    ///
    /// ### 注意事项（Trade-offs）
    /// - 返回的是不可变引用，确保调用方无法绕过句柄直接修改 Profile。
    #[inline]
    pub fn profile(&self) -> &ProfileDescriptor {
        &self.descriptor
    }

    /// 获取最新配置快照。
    ///
    /// ### 设计意图（Why）
    /// - 允许调用方以幂等方式读取配置，而无需了解内部 Layer 结构。
    ///
    /// ### 契约说明（What）
    /// - **输出**：[`ResolvedConfiguration`]，包含合并后的映射与当前版本号。
    /// - 每次调用均重新执行合并，确保读取到最新状态；若性能敏感，可结合缓存策略使用。
    pub fn snapshot(&self) -> ResolvedConfiguration {
        self.layered.resolve()
    }

    /// 构建一个增量更新流，串联所有配置源的 `watch` 能力。
    ///
    /// ### 设计意图（Why）
    /// - 将多源增量通知统一汇聚，生成按发生顺序输出的配置快照，供运行时组件驱动热更新。
    ///
    /// ### 契约说明（What）
    /// - **返回值**：[`ConfigurationWatch`]，实现 [`Stream`]，每次轮询返回一次 [`ConfigurationUpdate`]。
    /// - 若所有数据源均不支持增量推送，返回的流会立即结束。
    /// - 错误场景（如审计哈希不一致）以 [`ConfigurationError`] 形式暴露。
    pub fn watch(&mut self) -> crate::Result<ConfigurationWatch<'_>, ConfigurationError> {
        let mut streams = Vec::with_capacity(self.sources.len());
        for source in &self.sources {
            let stream = source.watch_dyn(&self.descriptor.identifier)?;
            streams.push(stream);
        }
        Ok(ConfigurationWatch::new(&mut self.layered, streams))
    }
}

impl LayeredConfiguration {
    /// 根据最新增量通知更新内部状态，并返回差异集合。
    ///
    /// ### 使用场景（Why）
    /// - 当配置源推送 [`ChangeNotification`] 时，热更新驱动调用本方法，随后再衍生新的快照交付给业务组件。
    ///
    /// ### 契约说明（What）
    /// - **输入**：保证单调递增的序列号与按时间排序的事件列表。
    /// - **前置条件**：调用方须确保事件顺序已经在源端稳定；方法本身不会重新排序。
    /// - **后置条件**：返回的 [`ChangeSet`] 收集增量差异，同时内部版本号递增并更新审计链哈希。
    ///
    /// ### 执行逻辑（How）
    /// 1. 遍历事件，调用 `upsert_entry` 或 `remove_entry` 应用变更；
    /// 2. 收集创建、更新、删除的键值，构造 [`ChangeSet`]；
    /// 3. 若配置了审计流水线，写入事件并在失败时回滚；
    /// 4. 自增版本号并更新最新快照的哈希链表头。
    ///
    /// ### 风险提示（Trade-offs）
    /// - 若通知中出现同一键的多次修改，最终结果以最后一次事件为准，`ChangeSet` 仅记录最终值。
    pub fn apply_change(
        &mut self,
        notification: ChangeNotification,
    ) -> crate::Result<ChangeSet, ConfigurationError> {
        let before_snapshot = self.resolve();
        let state_prev_hash = AuditStateHasher::hash_configuration(before_snapshot.values.iter());
        if let Some(chain_tip) = &self.audit_chain_tip {
            if chain_tip != &state_prev_hash {
                return Err(ConfigurationError::with_context(
                    ConfigurationErrorKind::Conflict,
                    "audit hash mismatch before applying change",
                ));
            }
        } else {
            self.audit_chain_tip = Some(state_prev_hash.clone());
        }

        let previous_layers = self.layers.clone();
        let previous_chain_tip = self.audit_chain_tip.clone();
        let previous_version = self.version.load(Ordering::SeqCst);

        let mut created = Vec::new();
        let mut updated = Vec::new();
        let mut deleted = Vec::new();

        for event in &notification.events {
            match event {
                ChangeEvent::Created { key, value } => {
                    self.upsert_entry(key.clone(), value.clone(), &mut created);
                }
                ChangeEvent::Updated { key, value } => {
                    self.upsert_entry(key.clone(), value.clone(), &mut updated);
                }
                ChangeEvent::Deleted { key } => {
                    self.remove_entry(key, &mut deleted);
                }
            }
        }

        self.version.store(previous_version + 1, Ordering::SeqCst);

        let change_set = ChangeSet {
            created,
            updated,
            deleted,
        };

        let after_snapshot = self.resolve();
        let state_curr_hash = AuditStateHasher::hash_configuration(after_snapshot.values.iter());

        if let Some(pipeline) = &self.audit {
            let audit_event = AuditEventV1 {
                event_id: format!(
                    "{}:{}:{}",
                    self.profile.as_str(),
                    notification.sequence,
                    after_snapshot.version
                ),
                sequence: notification.sequence,
                entity: AuditEntityRef {
                    kind: pipeline.context.entity_kind.clone(),
                    id: self.profile.as_str().to_string(),
                    labels: pipeline.context.entity_labels.clone(),
                },
                action: pipeline.context.action.clone(),
                state_prev_hash: state_prev_hash.clone(),
                state_curr_hash: state_curr_hash.clone(),
                actor: pipeline.context.actor.clone(),
                occurred_at: notification.occurred_at,
                tsa_evidence: pipeline.context.tsa_evidence.clone(),
                changes: AuditChangeSet::from_change_set(&change_set),
            };

            if let Err(error) = pipeline.recorder.record(audit_event) {
                self.layers = previous_layers;
                self.version.store(previous_version, Ordering::SeqCst);
                self.audit_chain_tip = previous_chain_tip;
                return Err(ConfigurationError::with_context(
                    ConfigurationErrorKind::Audit,
                    format!("audit recorder failure: {}", error.message()),
                ));
            }
        }

        self.audit_chain_tip = Some(state_curr_hash);

        Ok(change_set)
    }

    /// 以新 Layer 列表替换当前配置，并返回刷新后的快照。
    ///
    /// ### 设计目的（Why）
    /// - 支持无法提供增量事件的数据源在重大变更时执行全量刷新，避免逐条计算差异的高昂成本。
    ///
    /// ### 契约说明（What）
    /// - **输入**：完整的 [`ConfigurationLayer`] 列表，按照优先级从低到高排序。
    /// - **前置条件**：调用方需确保传入的 Layer 覆盖了后续解析所需的全部键值。
    /// - **后置条件**：内部版本号自增，并将审计链表头更新为最新快照的哈希值。
    pub fn apply_refresh(
        &mut self,
        layers: Vec<ConfigurationLayer>,
    ) -> crate::Result<ResolvedConfiguration, ConfigurationError> {
        let before_snapshot = self.resolve();
        let state_prev_hash = AuditStateHasher::hash_configuration(before_snapshot.values.iter());
        if let Some(chain_tip) = &self.audit_chain_tip {
            if chain_tip != &state_prev_hash {
                return Err(ConfigurationError::with_context(
                    ConfigurationErrorKind::Conflict,
                    "audit hash mismatch before applying refresh",
                ));
            }
        } else {
            self.audit_chain_tip = Some(state_prev_hash);
        }

        self.layers = layers;
        self.version.fetch_add(1, Ordering::SeqCst);

        let snapshot = self.resolve();
        let state_curr_hash = AuditStateHasher::hash_configuration(snapshot.values.iter());
        self.audit_chain_tip = Some(state_curr_hash);

        Ok(snapshot)
    }

    /// 在现有 Layer 中插入或更新配置项。
    ///
    /// ### 契约说明（What）
    /// - **输入**：目标 `key`、`value` 以及用于收集结果的 `bucket`。
    /// - **后置条件**：若找到同名键则覆盖，否则追加到最高优先级 Layer。
    ///
    /// ### 风险提示（Trade-offs）
    /// - 若内部不存在任何 Layer，该操作将被忽略；调用方需保证至少存在一个 Layer。
    fn upsert_entry(
        &mut self,
        key: ConfigKey,
        value: ConfigValue,
        bucket: &mut Vec<(ConfigKey, ConfigValue)>,
    ) {
        for layer in &mut self.layers {
            // 简化策略：寻找第一个包含该键的层进行覆盖，否则插入优先级最高的层。
            if let Some(existing) = layer
                .entries
                .iter_mut()
                .find(|(existing_key, _)| existing_key == &key)
            {
                *existing = (key.clone(), value.clone());
                bucket.push((key, value));
                return;
            }
        }

        if let Some(last_layer) = self.layers.last_mut() {
            last_layer.entries.push((key.clone(), value.clone()));
            bucket.push((key, value));
        }
    }

    /// 从 Layer 中删除配置项。
    ///
    /// ### 契约说明（What）
    /// - **输入**：待删除的 `key` 与结果集合 `bucket`。
    /// - **后置条件**：若找到对应项则移除，并将键记录到删除列表；未找到时静默忽略。
    ///
    /// ### 风险提示（Trade-offs）
    /// - 删除操作仅影响首次匹配的 Layer；如存在多层重复键，剩余层仍保留，保证覆盖语义。
    fn remove_entry(&mut self, key: &ConfigKey, bucket: &mut Vec<ConfigKey>) {
        for layer in &mut self.layers {
            if let Some(index) = layer
                .entries
                .iter()
                .position(|(existing_key, _)| existing_key == key)
            {
                layer.entries.remove(index);
                bucket.push(key.clone());
                return;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::source::NoopConfigStream;
    use super::*;
    #[cfg(feature = "std")]
    use crate::audit::InMemoryAuditRecorder;
    use crate::audit::{AuditActor, AuditContext, AuditError, AuditPipeline, AuditRecorder};
    use crate::configuration::ConfigurationSource;
    use crate::configuration::error::ConfigurationErrorKind;
    use crate::configuration::{
        ConfigKey, ConfigMetadata, ConfigScope, ConfigValue, ProfileId, SourceMetadata,
    };
    use alloc::borrow::Cow;
    use alloc::sync::Arc;

    struct ControlledSource {
        layers: Vec<ConfigurationLayer>,
        fail: bool,
    }

    impl ControlledSource {
        fn success(layers: Vec<ConfigurationLayer>) -> Self {
            Self {
                layers,
                fail: false,
            }
        }

        fn failing() -> Self {
            Self {
                layers: Vec::new(),
                fail: true,
            }
        }
    }

    impl ConfigurationSource for ControlledSource {
        type Stream<'a>
            = NoopConfigStream
        where
            Self: 'a;

        fn load(
            &self,
            _profile: &ProfileId,
        ) -> crate::Result<Vec<ConfigurationLayer>, ConfigurationError> {
            if self.fail {
                Err(ConfigurationError::with_context(
                    ConfigurationErrorKind::Source,
                    "injected failure",
                ))
            } else {
                Ok(self.layers.clone())
            }
        }

        fn watch<'a>(
            &'a self,
            _profile: &ProfileId,
        ) -> crate::Result<Self::Stream<'a>, ConfigurationError> {
            Ok(NoopConfigStream::new())
        }
    }

    #[cfg(feature = "std")]
    #[test]
    fn apply_change_emits_audit_event() {
        let recorder = Arc::new(InMemoryAuditRecorder::new());
        let pipeline = AuditPipeline {
            recorder: recorder.clone() as Arc<dyn AuditRecorder>,
            context: AuditContext::new(
                Cow::Borrowed("configuration.profile"),
                AuditActor {
                    id: Cow::Borrowed("system"),
                    display_name: Some(Cow::Borrowed("System")),
                    tenant: None,
                },
                Cow::Borrowed("apply_changeset"),
            ),
        };

        let profile = ProfileDescriptor::new(
            ProfileId::new("test"),
            Vec::new(),
            ProfileLayering::BaseFirst,
            "unit",
        );
        let mut builder = ConfigurationBuilder::new()
            .with_profile(profile.clone())
            .with_audit_pipeline(pipeline);

        let key = ConfigKey::new("demo", "message", ConfigScope::Global, "demo message");
        let metadata = ConfigMetadata::default();
        let layer = ConfigurationLayer {
            metadata: SourceMetadata::new("inline", 0, None),
            entries: vec![(
                key.clone(),
                ConfigValue::Text(Cow::Borrowed("hello"), metadata.clone()),
            )],
        };
        builder
            .register_source(Box::new(ControlledSource::success(vec![layer])))
            .expect("register source");

        let mut handle = builder.build().expect("build").handle;
        let previous_hash = handle
            .layered
            .audit_chain_tip
            .clone()
            .expect("initial hash set");

        let notification = ChangeNotification::new(
            2,
            1_700_000_000_000,
            vec![ChangeEvent::Updated {
                key: key.clone(),
                value: ConfigValue::Text(Cow::Borrowed("world"), metadata.clone()),
            }],
        );

        let change_set = handle
            .layered
            .apply_change(notification.clone())
            .expect("apply change");

        assert_eq!(change_set.created.len(), 0);
        assert_eq!(change_set.updated.len(), 1);
        assert_eq!(change_set.deleted.len(), 0);

        let events = recorder.events();
        assert_eq!(events.len(), 1);
        let event = &events[0];
        assert_eq!(event.sequence, 2);
        assert_eq!(event.state_prev_hash, previous_hash);
        assert_eq!(event.entity.id, profile.identifier.as_str());
        assert_eq!(event.changes.updated.len(), 1);
        assert_eq!(event.changes.updated[0].key, key);
        assert_eq!(
            event.changes.updated[0].value,
            ConfigValue::Text(Cow::Borrowed("world"), metadata.clone())
        );
        assert_eq!(event.occurred_at, 1_700_000_000_000);

        let new_hash = handle
            .layered
            .audit_chain_tip
            .clone()
            .expect("hash updated");
        assert_eq!(event.state_curr_hash, new_hash);
    }

    #[test]
    fn apply_change_detects_hash_gap() {
        let profile = ProfileDescriptor::new(
            ProfileId::new("test"),
            Vec::new(),
            ProfileLayering::BaseFirst,
            "unit",
        );
        let mut builder = ConfigurationBuilder::new().with_profile(profile);
        let key = ConfigKey::new("demo", "message", ConfigScope::Global, "demo message");
        let metadata = ConfigMetadata::default();
        let layer = ConfigurationLayer {
            metadata: SourceMetadata::new("inline", 0, None),
            entries: vec![(
                key.clone(),
                ConfigValue::Text(Cow::Borrowed("hello"), metadata.clone()),
            )],
        };
        builder
            .register_source(Box::new(ControlledSource::success(vec![layer])))
            .expect("register source");

        let mut handle = builder.build().expect("build").handle;
        handle.layered.audit_chain_tip = Some("bogus".to_string());

        let notification = ChangeNotification::new(
            2,
            1_700_000_000_000,
            vec![ChangeEvent::Updated {
                key,
                value: ConfigValue::Text(Cow::Borrowed("world"), metadata),
            }],
        );

        let error = handle
            .layered
            .apply_change(notification)
            .expect_err("hash gap should fail");
        assert_eq!(error.kind(), ConfigurationErrorKind::Conflict);
    }

    struct FailingRecorder;

    impl AuditRecorder for FailingRecorder {
        fn record(&self, _event: AuditEventV1) -> crate::Result<(), AuditError> {
            Err(AuditError::new("forced failure"))
        }
    }

    #[test]
    fn apply_change_rolls_back_on_audit_failure() {
        let profile = ProfileDescriptor::new(
            ProfileId::new("test"),
            Vec::new(),
            ProfileLayering::BaseFirst,
            "unit",
        );
        let pipeline = AuditPipeline {
            recorder: Arc::new(FailingRecorder) as Arc<dyn AuditRecorder>,
            context: AuditContext::new(
                Cow::Borrowed("configuration.profile"),
                AuditActor {
                    id: Cow::Borrowed("system"),
                    display_name: None,
                    tenant: None,
                },
                Cow::Borrowed("apply_changeset"),
            ),
        };

        let mut builder = ConfigurationBuilder::new()
            .with_profile(profile)
            .with_audit_pipeline(pipeline);
        let key = ConfigKey::new("demo", "message", ConfigScope::Global, "demo message");
        let metadata = ConfigMetadata::default();
        let layer = ConfigurationLayer {
            metadata: SourceMetadata::new("inline", 0, None),
            entries: vec![(
                key.clone(),
                ConfigValue::Text(Cow::Borrowed("hello"), metadata.clone()),
            )],
        };
        builder
            .register_source(Box::new(ControlledSource::success(vec![layer])))
            .expect("register source");

        let mut handle = builder.build().expect("build").handle;
        let notification = ChangeNotification::new(
            2,
            1_700_000_000_000,
            vec![ChangeEvent::Updated {
                key: key.clone(),
                value: ConfigValue::Text(Cow::Borrowed("world"), metadata.clone()),
            }],
        );

        let error = handle
            .layered
            .apply_change(notification)
            .expect_err("audit failure should abort");
        assert_eq!(error.kind(), ConfigurationErrorKind::Audit);

        let snapshot = handle.layered.resolve();
        let value = snapshot
            .values
            .get(&key)
            .expect("value retained after rollback");
        assert_eq!(value, &ConfigValue::Text(Cow::Borrowed("hello"), metadata));
        assert_eq!(snapshot.version, 1);
    }

    #[test]
    fn build_requires_profile() {
        let mut builder = ConfigurationBuilder::new();
        builder
            .register_source(Box::new(ControlledSource::success(Vec::new())))
            .expect("register source");

        let error = builder.build().expect_err("missing profile should fail");
        assert_eq!(error.stage(), BuildErrorStage::Validation);
        assert!(matches!(error.kind(), BuildErrorKind::MissingProfile));
        assert_eq!(error.report().failed_count(), 1);
    }

    #[test]
    fn build_requires_sources() {
        let profile = ProfileDescriptor::new(
            ProfileId::new("test"),
            Vec::new(),
            ProfileLayering::BaseFirst,
            "unit",
        );
        let builder = ConfigurationBuilder::new().with_profile(profile);
        let error = builder.build().expect_err("missing sources should fail");
        assert_eq!(error.stage(), BuildErrorStage::Validation);
        assert!(matches!(error.kind(), BuildErrorKind::MissingSources));
        assert_eq!(error.report().failed_count(), 1);
    }

    #[test]
    fn build_propagates_source_failure() {
        let profile = ProfileDescriptor::new(
            ProfileId::new("test"),
            Vec::new(),
            ProfileLayering::BaseFirst,
            "unit",
        );
        let mut builder = ConfigurationBuilder::new().with_profile(profile);
        builder
            .register_source(Box::new(ControlledSource::failing()))
            .expect("register failing source");
        let error = builder
            .build()
            .expect_err("source failure should fail build");
        assert_eq!(error.stage(), BuildErrorStage::Load);
        assert!(matches!(
            error.kind(),
            BuildErrorKind::SourceLoadFailure { .. }
        ));
        assert!(error.cause().is_some());
    }

    #[test]
    fn build_generates_redacted_snapshot() {
        let profile = ProfileDescriptor::new(
            ProfileId::new("test"),
            Vec::new(),
            ProfileLayering::BaseFirst,
            "unit",
        );
        let mut builder = ConfigurationBuilder::new().with_profile(profile);

        let metadata = SourceMetadata::new("memory", 0, None);
        let entry = (
            ConfigKey::new("domain", "secret", ConfigScope::Global, "secret"),
            ConfigValue::Text(
                Cow::Borrowed("super-secret"),
                ConfigMetadata {
                    hot_reloadable: false,
                    encrypted: true,
                    experimental: false,
                    tags: Vec::new(),
                },
            ),
        );
        let layer = ConfigurationLayer {
            metadata,
            entries: vec![entry],
        };

        builder
            .register_source(Box::new(ControlledSource::success(vec![layer])))
            .expect("register source");

        let outcome = builder.build().expect("build succeeds");
        let json = outcome.report.snapshot().to_json();
        assert!(json.contains("[REDACTED]"));
        assert!(!json.contains("super-secret"));
    }
}
