//! # Contract-only Runtime Surface
//!
//! ## 契约声明
//! * **Contract-only：** 本模块仅定义运行时可供合约调用的抽象 API，约束业务侧只能依赖这些接口而非具体执行器实现，以确保在无状态执行环境、回放环境中保持一致行为。
//! * **禁止实现：** 本文件及其子模块不允许落地具体执行逻辑，实现必须由宿主运行时或测试替身在独立 crate 中提供，从而杜绝合约代码在此处混入状态机或 I/O 细节。
//! * **解耦外设：** 所有接口均以 `Send + Sync + 'static` 能力描述，对具体执行器、定时器、异步 runtime 完全解耦，便于在 wasm/no-std 等受限环境中替换宿主。
//!
//! ## 并发与错误语义
//! * **并发模型：** 默认遵循单请求上下文内的协作式并发——接口返回的 future/stream 必须可被外部执行器安全地 `await`；禁止在实现中假设特定调度器或线程池。
//! * **错误传播：** 约定使用 `SparkError` 系列枚举表达业务/系统异常；调用方必须准备处理超时、取消、幂等失败等场景，实现方不得吞掉错误或 panic。
//!
//! ## 使用前后条件
//! * **前置条件：** 合约端在调用前必须确保上下文由外部运行时注入（如 tracing span、租户信息），且所有句柄均来自这些契约。
//! * **后置条件：** 成功调用保证返回值可用于继续构造合约逻辑，但不会持有宿主资源的独占所有权，避免破坏运行时调度；任何需要长生命周期资源的对象都必须通过显式托管接口申请。
//!
//! ## 设计取舍提示
//! * **架构选择：** 通过模块化接口换取统一性，牺牲了直接操作底层 executor 的灵活性，却换来可测试性与跨平台部署能力。
//! * **边界情况：** 合约作者需关注超时、重复调度、以及宿主拒绝服务等边界；本模块接口文档会明确每个 API 的退化行为，便于上层实现补偿逻辑。
//!
use crate::{
    Error,
    configuration::{ConfigKey, ConfigScope, ConfigValue},
    contract::{BudgetKind, BudgetSnapshot},
};
use alloc::{
    borrow::Cow,
    collections::BTreeMap,
    format,
    string::{String, ToString},
    sync::Arc,
    vec::Vec,
};
use core::cmp;
use core::fmt;
use core::sync::atomic::{AtomicBool, Ordering};
use core::time::Duration;
use spin::RwLock;

/// 返回 SLO 策略映射表对应的配置键。
///
/// # 设计初衷（Why）
/// - `SLO → 策略` 的映射需要通过配置系统热更新，因此必须暴露稳定的 [`ConfigKey`]。
/// - 运行时服务与运维工具可以依赖该键做差异化处理（例如观测、审计、准入校验）。
///
/// # 契约说明（What）
/// - **域**：`"slo"`，与性能 / 稳定性治理相关配置统一归属。
/// - **名称**：`"policy_table"`，表明该键承载整张策略映射表。
/// - **作用域**：[`ConfigScope::Runtime`]，表示作用于当前进程，可随运行时热更新。
/// - **摘要**：`"SLO 策略映射表"`，供 UI / CLI 展示。
///
/// # 使用提示（How）
/// - 该函数每次调用都会构造新的 [`ConfigKey`] 实例，调用方可按需缓存。
/// - 元数据建议设置为 `hot_reloadable = true`，以满足验收标准中的“规则热更新”。
#[must_use]
pub fn slo_policy_table_key() -> ConfigKey {
    ConfigKey::new(
        "slo",
        "policy_table",
        ConfigScope::Runtime,
        "SLO 策略映射表",
    )
}

/// 描述当预算触发告警时需要执行的策略动作。
///
/// # 背景（Why）
/// - 汇总限流（Rate Limiting）、降级（Degrade）、熔断（Circuit Breaker）、重试（Retry）四类策略，
///   对齐平台对稳定性的治理手段。
/// - 采用枚举以便序列化、匹配和单元测试覆盖，避免字符串约定带来的歧义。
///
/// # 字段说明（What）
/// - `RateLimit { limit_percent }`：建议把入口速率限制在给定百分比（0-100）以内。
/// - `Degrade { feature }`：标记降级的功能特性，调用方据此关闭非关键能力。
/// - `CircuitBreak { open_for }`：建议熔断时长，调用方应在窗口结束后再尝试恢复。
/// - `Retry { max_attempts, backoff }`：推荐的重试次数与基础退避间隔。
///
/// # 设计考量（Trade-offs）
/// - 为兼顾 `no_std + alloc`，动作参数使用基本类型或 [`Duration`]；复杂策略由上层解释执行。
/// - 若未来新增策略，只需在枚举上添加变体并保持向后兼容。
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SloPolicyAction {
    RateLimit { limit_percent: u8 },
    Degrade { feature: Arc<str> },
    CircuitBreak { open_for: Duration },
    Retry { max_attempts: u8, backoff: Duration },
}

/// 控制策略触发与恢复的阈值定义，采用“激活阈值 / 恢复阈值”双门限设计。
///
/// # 背景（Why）
/// - 单一阈值容易导致抖动（thrashing），因此引入迟滞（Hysteresis）避免频繁切换。
/// - 以千分比表示剩余预算比例，可兼顾整数算术与较高精度。
///
/// # 字段说明（What）
/// - `activate_below_or_equal`：当剩余预算百分率（×10，范围 0-1000）**低于或等于**该值时触发策略。
/// - `deactivate_above_or_equal`：当剩余预算百分率（×10）**高于或等于**该值时恢复。
///
/// # 契约（Contract）
/// - **前置条件**：`activate_below_or_equal <= deactivate_above_or_equal`，否则无法形成有效迟滞区间。
/// - **后置条件**：内部逻辑根据当前激活状态与实时比例给出激活 / 恢复建议。
///
/// # 风险提示（Trade-offs）
/// - 若 `deactivate` 与 `activate` 相等，将退化为单门限行为，适合响应速度优先的场景。
/// - 千分比计算使用整数除法，可能向下取整；若需更高精度，可在未来扩展为万分比。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SloPolicyTrigger {
    activate_below_or_equal: u16,
    deactivate_above_or_equal: u16,
}

impl SloPolicyTrigger {
    /// 构造迟滞触发器。
    ///
    /// # 参数（Inputs）
    /// - `activate_below_or_equal`：激活阈值（千分比）。
    /// - `deactivate_above_or_equal`：恢复阈值（千分比）。
    ///
    /// # 前置条件（Preconditions）
    /// - `activate_below_or_equal` 与 `deactivate_above_or_equal` 均须落在 `[0, 1000]`。
    /// - `activate_below_or_equal <= deactivate_above_or_equal`。
    ///
    /// # 后置条件（Postconditions）
    /// - 返回的触发器可在 [`SloPolicyManager`] 中被多次复用，且为纯数据结构（`Clone` 安全）。
    pub fn new(activate_below_or_equal: u16, deactivate_above_or_equal: u16) -> Self {
        assert!(activate_below_or_equal <= 1000);
        assert!(deactivate_above_or_equal <= 1000);
        assert!(activate_below_or_equal <= deactivate_above_or_equal);
        Self {
            activate_below_or_equal,
            deactivate_above_or_equal,
        }
    }

    /// 根据当前剩余比例与激活状态计算是否需要切换策略。
    ///
    /// # 参数（Inputs）
    /// - `remaining_permille`：剩余预算占比（千分比）。
    /// - `currently_active`：策略当前是否已激活。
    ///
    /// # 返回值（What）
    /// - `Some(true)`：需要激活策略。
    /// - `Some(false)`：需要恢复（停用）策略。
    /// - `None`：保持原状态不变。
    #[must_use]
    pub fn evaluate(&self, remaining_permille: u16, currently_active: bool) -> Option<bool> {
        if currently_active {
            if remaining_permille >= self.deactivate_above_or_equal {
                return Some(false);
            }
        } else if remaining_permille <= self.activate_below_or_equal {
            return Some(true);
        }
        None
    }
}

/// 表示单条 SLO 策略规则，包括唯一标识、适用预算、触发条件与动作。
///
/// # 背景（Why）
/// - 将策略以规则形式表述，便于配置下发、热更新以及单元测试。
/// - `rule_id` 作为稳定主键，用于热更新时对齐旧状态（例如是否已激活）。
///
/// # 字段说明（What）
/// - `rule_id`：唯一标识，建议使用 `domain.intent` 命名。
/// - `budget_kind`：关联的预算类型，对应 [`BudgetKind`]。
/// - `trigger`：触发与恢复阈值。
/// - `action`：要执行的策略动作。
/// - `summary`：人类可读说明（可选），用于日志与可观测性输出。
///
/// # 前置条件（Preconditions）
/// - `rule_id` 必须唯一；重复规则会在热更新阶段覆盖旧项。
/// - `budget_kind` 建议与预算分配流程保持一致，避免同名不同义。
///
/// # 后置条件（Postconditions）
/// - 规则可安全克隆，用于广播与审计。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SloPolicyRule {
    rule_id: Arc<str>,
    budget_kind: BudgetKind,
    trigger: SloPolicyTrigger,
    action: SloPolicyAction,
    summary: Option<Arc<str>>,
}

impl SloPolicyRule {
    /// 构造规则。
    pub fn new(
        rule_id: Arc<str>,
        budget_kind: BudgetKind,
        trigger: SloPolicyTrigger,
        action: SloPolicyAction,
        summary: Option<Arc<str>>,
    ) -> Self {
        Self {
            rule_id,
            budget_kind,
            trigger,
            action,
            summary,
        }
    }

    /// 返回规则标识。
    #[must_use]
    pub fn rule_id(&self) -> &Arc<str> {
        &self.rule_id
    }

    /// 返回适用的预算类型。
    #[must_use]
    pub fn budget_kind(&self) -> &BudgetKind {
        &self.budget_kind
    }

    /// 返回触发器定义。
    #[must_use]
    pub fn trigger(&self) -> &SloPolicyTrigger {
        &self.trigger
    }

    /// 返回策略动作。
    #[must_use]
    pub fn action(&self) -> &SloPolicyAction {
        &self.action
    }

    /// 返回摘要说明。
    #[must_use]
    pub fn summary(&self) -> Option<&Arc<str>> {
        self.summary.as_ref()
    }
}

/// 表示策略管理器对外输出的指令，标记某条规则需要激活或恢复。
///
/// # 背景（Why）
/// - 运行时（RateLimiter、CircuitBreaker 等）只关注“执行/停止”信号，故将动作与激活态打包。
///
/// # 字段说明（What）
/// - `rule_id`：规则标识。
/// - `action`：需要执行的策略动作。
/// - `engaged`：`true` 表示激活策略，`false` 表示恢复。
/// - `summary`：可选说明，方便输出日志。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SloPolicyDirective {
    rule_id: Arc<str>,
    action: SloPolicyAction,
    engaged: bool,
    summary: Option<Arc<str>>,
}

impl SloPolicyDirective {
    /// 构造激活指令。
    fn engage(rule: &SloPolicyRule) -> Self {
        Self {
            rule_id: Arc::clone(rule.rule_id()),
            action: rule.action().clone(),
            engaged: true,
            summary: rule.summary().cloned(),
        }
    }

    /// 构造恢复指令。
    fn disengage(rule: &SloPolicyRule) -> Self {
        Self {
            rule_id: Arc::clone(rule.rule_id()),
            action: rule.action().clone(),
            engaged: false,
            summary: rule.summary().cloned(),
        }
    }

    /// 指令是否为激活。
    #[must_use]
    pub fn is_engaged(&self) -> bool {
        self.engaged
    }

    /// 返回规则标识。
    #[must_use]
    pub fn rule_id(&self) -> &Arc<str> {
        &self.rule_id
    }

    /// 返回需要执行的动作。
    #[must_use]
    pub fn action(&self) -> &SloPolicyAction {
        &self.action
    }

    /// 返回摘要说明。
    #[must_use]
    pub fn summary(&self) -> Option<&Arc<str>> {
        self.summary.as_ref()
    }
}

/// 记录策略热更新的增量情况，便于在日志与观测指标中审计。
///
/// # 字段说明（What）
/// - `added`：新增规则 ID 列表。
/// - `removed`：被移除的规则 ID 列表。
/// - `retained`：保留并复用状态的规则 ID 列表。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SloPolicyReloadReport {
    pub added: Vec<Arc<str>>,
    pub removed: Vec<Arc<str>>,
    pub retained: Vec<Arc<str>>,
}

impl SloPolicyReloadReport {
    fn empty() -> Self {
        Self {
            added: Vec::new(),
            removed: Vec::new(),
            retained: Vec::new(),
        }
    }
}

/// 策略管理器：维护规则表、提供评估与热更新能力。
///
/// # 背景（Why）
/// - 验收标准要求“混沌脚本触发 SLO 违约 → 策略按映射执行”与“规则热更新可生效”，因此需要集中式的规则管理器。
/// - 采用 `RwLock<Vec<_>>` 储存规则，读取场景多于写入，符合运行时使用特征。
///
/// # 契约说明（What）
/// - `evaluate_snapshot`：根据 [`BudgetSnapshot`] 计算待执行指令。
/// - `update_rules` / `apply_config`：更新策略表，并返回增量报告。
///
/// # 风险提示（Trade-offs）
/// - `evaluate_snapshot` 在单次评估中持有读锁，若规则很多需关注评估延迟；可通过 sharding 优化。
/// - 热更新目前基于规则 ID 匹配状态，若 ID 变更将视为新规则。
pub struct SloPolicyManager {
    table: RwLock<Vec<SloRuleSlot>>,
}

impl Default for SloPolicyManager {
    fn default() -> Self {
        Self::new()
    }
}

impl SloPolicyManager {
    /// 创建空表管理器。
    #[must_use]
    pub fn new() -> Self {
        Self {
            table: RwLock::new(Vec::new()),
        }
    }

    /// 使用初始规则构造管理器。
    #[must_use]
    pub fn with_rules(rules: Vec<SloPolicyRule>) -> Self {
        let slots = rules.into_iter().map(SloRuleSlot::new).collect();
        Self {
            table: RwLock::new(slots),
        }
    }

    /// 替换规则集合，尽可能复用已有激活状态。
    pub fn update_rules(&self, rules: Vec<SloPolicyRule>) -> SloPolicyReloadReport {
        let mut report = SloPolicyReloadReport::empty();
        let mut existing: BTreeMap<Arc<str>, bool> = BTreeMap::new();
        {
            let guard = self.table.read();
            for slot in guard.iter() {
                let id = Arc::clone(slot.rule.rule_id());
                let active = slot.active.load(Ordering::SeqCst);
                existing.insert(id, active);
            }
        }

        let mut new_slots = Vec::with_capacity(rules.len());
        for rule in rules {
            let id = Arc::clone(rule.rule_id());
            if let Some(active) = existing.remove(&id) {
                report.retained.push(Arc::clone(&id));
                new_slots.push(SloRuleSlot::with_state(rule, active));
            } else {
                report.added.push(Arc::clone(&id));
                new_slots.push(SloRuleSlot::new(rule));
            }
        }

        report.removed.extend(existing.into_keys());
        let mut guard = self.table.write();
        *guard = new_slots;
        report
    }

    /// 根据预算快照评估需要执行的策略。
    pub fn evaluate_snapshot(&self, snapshot: &BudgetSnapshot) -> Vec<SloPolicyDirective> {
        let remaining_permille = calculate_permille(snapshot.remaining(), snapshot.limit());
        let guard = self.table.read();
        let mut directives = Vec::new();
        for slot in guard.iter() {
            if slot.rule.budget_kind() != snapshot.kind() {
                continue;
            }
            if let Some(directive) = slot.evaluate(remaining_permille) {
                directives.push(directive);
            }
        }
        directives
    }

    /// 解析配置值并更新规则。
    pub fn apply_config(
        &self,
        value: &ConfigValue,
    ) -> crate::Result<SloPolicyReloadReport, SloPolicyConfigError> {
        let rules = parse_policy_table(value)?;
        Ok(self.update_rules(rules))
    }
}

struct SloRuleSlot {
    rule: SloPolicyRule,
    active: AtomicBool,
}

impl SloRuleSlot {
    fn new(rule: SloPolicyRule) -> Self {
        Self {
            rule,
            active: AtomicBool::new(false),
        }
    }

    fn with_state(rule: SloPolicyRule, active: bool) -> Self {
        Self {
            rule,
            active: AtomicBool::new(active),
        }
    }

    fn evaluate(&self, remaining_permille: u16) -> Option<SloPolicyDirective> {
        let current = self.active.load(Ordering::SeqCst);
        match self.rule.trigger().evaluate(remaining_permille, current) {
            Some(true) => self
                .active
                .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                .ok()
                .map(|_| SloPolicyDirective::engage(&self.rule)),
            Some(false) => self
                .active
                .compare_exchange(true, false, Ordering::SeqCst, Ordering::SeqCst)
                .ok()
                .map(|_| SloPolicyDirective::disengage(&self.rule)),
            None => None,
        }
    }
}

/// 配置解析错误，包含出错路径与详细信息。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SloPolicyConfigError {
    path: String,
    message: String,
}

impl SloPolicyConfigError {
    fn new(path: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            path: path.into(),
            message: message.into(),
        }
    }

    /// 返回错误路径。
    #[must_use]
    pub fn path(&self) -> &str {
        &self.path
    }

    /// 返回错误信息。
    #[must_use]
    pub fn message(&self) -> &str {
        &self.message
    }
}

impl fmt::Display for SloPolicyConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.path, self.message)
    }
}

impl Error for SloPolicyConfigError {
    #[allow(unused_parens)]
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        None
    }
}

/// 解析策略映射表配置。
fn expect_dictionary<'a>(
    value: &'a ConfigValue,
    path: &str,
    message: &str,
) -> crate::Result<&'a [(Cow<'static, str>, ConfigValue)], SloPolicyConfigError> {
    match value {
        ConfigValue::Dictionary(entries, _) => Ok(entries.as_slice()),
        _ => Err(SloPolicyConfigError::new(
            path.to_string(),
            message.to_string(),
        )),
    }
}

fn expect_list<'a>(
    value: &'a ConfigValue,
    path: &str,
    message: &str,
) -> crate::Result<&'a [ConfigValue], SloPolicyConfigError> {
    match value {
        ConfigValue::List(entries, _) => Ok(entries.as_slice()),
        _ => Err(SloPolicyConfigError::new(
            path.to_string(),
            message.to_string(),
        )),
    }
}

fn dict_get<'a>(
    dict: &'a [(Cow<'static, str>, ConfigValue)],
    prefix: &str,
    field: &str,
) -> crate::Result<&'a ConfigValue, SloPolicyConfigError> {
    dict.iter()
        .find(|(k, _)| k.as_ref() == field)
        .map(|(_, value)| value)
        .ok_or_else(|| SloPolicyConfigError::new(format!("{}.{}", prefix, field), "字段缺失"))
}

fn parse_policy_table(
    value: &ConfigValue,
) -> crate::Result<Vec<SloPolicyRule>, SloPolicyConfigError> {
    ensure_hot_reloadable("slo.policy_table", value)?;
    let entries = expect_dictionary(value, "slo.policy_table", "期望字典结构 (dictionary)")?;
    let rules_value = dict_get(entries, "slo.policy_table", "rules")?;

    ensure_hot_reloadable("slo.policy_table.rules", rules_value)?;
    let rules_list = expect_list(
        rules_value,
        "slo.policy_table.rules",
        "rules 应为列表 (list)",
    )?;

    let mut rules = Vec::with_capacity(rules_list.len());
    for (index, entry) in rules_list.iter().enumerate() {
        rules.push(parse_rule(entry, index)?);
    }
    Ok(rules)
}

fn parse_rule(
    value: &ConfigValue,
    index: usize,
) -> crate::Result<SloPolicyRule, SloPolicyConfigError> {
    let path_prefix = format!("slo.policy_table.rules[{}]", index);
    let dict = expect_dictionary(value, &path_prefix, "规则需为字典 (dictionary)")?;

    let rule_id = parse_text_field(dict, &path_prefix, "id")?;
    let budget = parse_budget_kind(dict, &path_prefix, "budget")?;
    let trigger = parse_trigger(dict, &path_prefix)?;
    let action = parse_action(dict, &path_prefix)?;
    let summary = parse_optional_text(dict, &path_prefix, "summary")?;

    Ok(SloPolicyRule::new(
        Arc::from(rule_id),
        budget,
        trigger,
        action,
        summary.map(Arc::from),
    ))
}

fn parse_budget_kind(
    dict: &[(Cow<'static, str>, ConfigValue)],
    prefix: &str,
    field: &str,
) -> crate::Result<BudgetKind, SloPolicyConfigError> {
    let raw = parse_text_field(dict, prefix, field)?;
    match raw {
        value if value.eq_ignore_ascii_case("decode") => Ok(BudgetKind::Decode),
        value if value.eq_ignore_ascii_case("flow") => Ok(BudgetKind::Flow),
        value => Ok(BudgetKind::custom(Arc::from(value))),
    }
}

fn parse_trigger(
    dict: &[(Cow<'static, str>, ConfigValue)],
    prefix: &str,
) -> crate::Result<SloPolicyTrigger, SloPolicyConfigError> {
    let trigger_value = dict_get(dict, prefix, "trigger")?;
    let trigger_dict = expect_dictionary(
        trigger_value,
        &format!("{}.trigger", prefix),
        "trigger 需为字典 (dictionary)",
    )?;

    let activate_percent = parse_percent(trigger_dict, prefix, "activate_below_percent")?;
    let deactivate_percent =
        parse_optional_percent(trigger_dict, prefix, "deactivate_above_percent")?
            .unwrap_or(cmp::max(activate_percent, 100u16));
    if deactivate_percent < activate_percent {
        return Err(SloPolicyConfigError::new(
            format!("{}.trigger.deactivate_above_percent", prefix),
            "恢复阈值必须大于等于激活阈值",
        ));
    }

    Ok(SloPolicyTrigger::new(
        percent_to_permille(activate_percent),
        percent_to_permille(deactivate_percent),
    ))
}

fn parse_action(
    dict: &[(Cow<'static, str>, ConfigValue)],
    prefix: &str,
) -> crate::Result<SloPolicyAction, SloPolicyConfigError> {
    let action_value = dict_get(dict, prefix, "action")?;
    let action_dict = expect_dictionary(
        action_value,
        &format!("{}.action", prefix),
        "action 需为字典 (dictionary)",
    )?;

    let kind = parse_text_field(action_dict, &format!("{}.action", prefix), "kind")?;
    match kind.to_ascii_lowercase().as_str() {
        "rate_limit" => {
            let percent = parse_u8(action_dict, prefix, "limit_percent", 0u8, 100u8)?;
            Ok(SloPolicyAction::RateLimit {
                limit_percent: percent,
            })
        }
        "degrade" => {
            let feature = parse_text_field(action_dict, &format!("{}.action", prefix), "feature")?;
            Ok(SloPolicyAction::Degrade {
                feature: Arc::from(feature),
            })
        }
        "circuit_break" => {
            let duration = parse_duration(action_dict, prefix, "open_for")?;
            Ok(SloPolicyAction::CircuitBreak { open_for: duration })
        }
        "retry" => {
            let attempts = parse_u8(action_dict, prefix, "max_attempts", 1u8, u8::MAX)?;
            let backoff = parse_duration(action_dict, prefix, "backoff")?;
            Ok(SloPolicyAction::Retry {
                max_attempts: attempts,
                backoff,
            })
        }
        other => Err(SloPolicyConfigError::new(
            format!("{}.action.kind", prefix),
            format!("未知策略类型: {}", other),
        )),
    }
}

fn parse_text_field(
    dict: &[(Cow<'static, str>, ConfigValue)],
    prefix: &str,
    field: &str,
) -> crate::Result<String, SloPolicyConfigError> {
    let value = dict_get(dict, prefix, field)?;
    match value {
        ConfigValue::Text(text, _) => Ok(text.to_string()),
        _ => Err(SloPolicyConfigError::new(
            format!("{}.{}", prefix, field),
            "需为字符串 (text)",
        )),
    }
}

fn parse_optional_text(
    dict: &[(Cow<'static, str>, ConfigValue)],
    prefix: &str,
    field: &str,
) -> crate::Result<Option<String>, SloPolicyConfigError> {
    match dict.iter().find(|(k, _)| k.as_ref() == field) {
        Some((_, ConfigValue::Text(text, _))) => Ok(Some(text.to_string())),
        Some((_, _)) => Err(SloPolicyConfigError::new(
            format!("{}.{}", prefix, field),
            "需为字符串 (text)",
        )),
        None => Ok(None),
    }
}

fn parse_percent(
    dict: &[(Cow<'static, str>, ConfigValue)],
    prefix: &str,
    field: &str,
) -> crate::Result<u16, SloPolicyConfigError> {
    parse_u16(dict, prefix, field, 0, 100)
}

fn parse_optional_percent(
    dict: &[(Cow<'static, str>, ConfigValue)],
    prefix: &str,
    field: &str,
) -> crate::Result<Option<u16>, SloPolicyConfigError> {
    match dict.iter().find(|(k, _)| k.as_ref() == field) {
        Some((_, ConfigValue::Integer(raw, _))) => {
            let value = *raw;
            if (0i64..=100i64).contains(&value) {
                Ok(Some(value as u16))
            } else {
                Err(SloPolicyConfigError::new(
                    format!("{}.{}", prefix, field),
                    "百分比需位于 0-100",
                ))
            }
        }
        Some((_, _)) => Err(SloPolicyConfigError::new(
            format!("{}.{}", prefix, field),
            "需为整数 (integer)",
        )),
        None => Ok(None),
    }
}

fn parse_u16(
    dict: &[(Cow<'static, str>, ConfigValue)],
    prefix: &str,
    field: &str,
    min: u16,
    max: u16,
) -> crate::Result<u16, SloPolicyConfigError> {
    let value = dict_get(dict, prefix, field)?;
    let min_i64 = i64::from(min);
    let max_i64 = i64::from(max);
    match value {
        ConfigValue::Integer(raw, _) => {
            let value = *raw;
            if (min_i64..=max_i64).contains(&value) {
                Ok(value as u16)
            } else {
                Err(SloPolicyConfigError::new(
                    format!("{}.{}", prefix, field),
                    format!("取值需位于 {}-{}", min, max),
                ))
            }
        }
        _ => Err(SloPolicyConfigError::new(
            format!("{}.{}", prefix, field),
            "需为整数 (integer)",
        )),
    }
}

fn parse_u8(
    dict: &[(Cow<'static, str>, ConfigValue)],
    prefix: &str,
    field: &str,
    min: u8,
    max: u8,
) -> crate::Result<u8, SloPolicyConfigError> {
    let value = dict_get(dict, prefix, field)?;
    let min_i64 = i64::from(min);
    let max_i64 = i64::from(max);
    match value {
        ConfigValue::Integer(raw, _) => {
            let value = *raw;
            if (min_i64..=max_i64).contains(&value) {
                Ok(value as u8)
            } else {
                Err(SloPolicyConfigError::new(
                    format!("{}.{}", prefix, field),
                    format!("取值需位于 {}-{}", min, max),
                ))
            }
        }
        _ => Err(SloPolicyConfigError::new(
            format!("{}.{}", prefix, field),
            "需为整数 (integer)",
        )),
    }
}

fn parse_duration(
    dict: &[(Cow<'static, str>, ConfigValue)],
    prefix: &str,
    field: &str,
) -> crate::Result<Duration, SloPolicyConfigError> {
    let value = dict_get(dict, prefix, field)?;
    match value {
        ConfigValue::Duration(duration, _) => Ok(*duration),
        ConfigValue::Integer(raw, _) if *raw >= 0 => Ok(Duration::from_millis(*raw as u64)),
        _ => Err(SloPolicyConfigError::new(
            format!("{}.{}", prefix, field),
            "需为 Duration 或毫秒整数",
        )),
    }
}

fn ensure_hot_reloadable(
    path: &str,
    value: &ConfigValue,
) -> crate::Result<(), SloPolicyConfigError> {
    if !value.metadata().hot_reloadable {
        return Err(SloPolicyConfigError::new(
            path,
            "该配置项未标记为 hot_reloadable，无法保证热更新",
        ));
    }
    Ok(())
}

fn percent_to_permille(percent: u16) -> u16 {
    percent.saturating_mul(10)
}

fn calculate_permille(remaining: u64, limit: u64) -> u16 {
    if limit == 0 {
        return 1000;
    }
    let ratio = remaining.saturating_mul(1000) / limit;
    cmp::min(ratio as u16, 1000)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::configuration::{ConfigMetadata, ConfigValue};
    use alloc::borrow::Cow;
    use alloc::vec;

    fn meta(hot: bool) -> ConfigMetadata {
        ConfigMetadata {
            hot_reloadable: hot,
            ..ConfigMetadata::default()
        }
    }

    #[test]
    fn trigger_evaluate_with_hysteresis() {
        let trigger = SloPolicyTrigger::new(400, 600);
        assert_eq!(trigger.evaluate(350, false), Some(true));
        assert_eq!(trigger.evaluate(500, true), None);
        assert_eq!(trigger.evaluate(650, true), Some(false));
    }

    #[test]
    fn parse_and_evaluate_rule() {
        let rule_value = ConfigValue::Dictionary(
            vec![
                (
                    Cow::Borrowed("id"),
                    ConfigValue::Text(Cow::Borrowed("demo.rule"), meta(true)),
                ),
                (
                    Cow::Borrowed("budget"),
                    ConfigValue::Text(Cow::Borrowed("flow"), meta(true)),
                ),
                (
                    Cow::Borrowed("trigger"),
                    ConfigValue::Dictionary(
                        vec![
                            (
                                Cow::Borrowed("activate_below_percent"),
                                ConfigValue::Integer(40, meta(true)),
                            ),
                            (
                                Cow::Borrowed("deactivate_above_percent"),
                                ConfigValue::Integer(60, meta(true)),
                            ),
                        ],
                        meta(true),
                    ),
                ),
                (
                    Cow::Borrowed("action"),
                    ConfigValue::Dictionary(
                        vec![
                            (
                                Cow::Borrowed("kind"),
                                ConfigValue::Text(Cow::Borrowed("rate_limit"), meta(true)),
                            ),
                            (
                                Cow::Borrowed("limit_percent"),
                                ConfigValue::Integer(50, meta(true)),
                            ),
                        ],
                        meta(true),
                    ),
                ),
            ],
            meta(true),
        );

        let rule = parse_rule(&rule_value, 0).expect("rule parsed");
        let manager = SloPolicyManager::with_rules(vec![rule]);
        let directives = manager.evaluate_snapshot(&BudgetSnapshot::new(BudgetKind::Flow, 30, 100));
        assert_eq!(directives.len(), 1);
        assert!(directives[0].is_engaged());
    }

    #[test]
    fn ensure_hot_reloadable_guard() {
        let value = ConfigValue::Dictionary(vec![], meta(false));
        let err = parse_policy_table(&value).expect_err("should fail");
        assert!(err.message().contains("hot_reloadable"));
    }
}
