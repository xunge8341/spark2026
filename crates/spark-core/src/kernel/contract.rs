use crate::{
    SparkError,
    context::Context,
    observability::TraceContext,
    runtime::AsyncRuntime,
    security::{IdentityDescriptor, SecurityPolicy},
    service::ClientFactory,
};
use alloc::sync::Arc;
use alloc::{format, string::ToString, vec, vec::Vec};
//
// 教案级说明：为了让 Loom 在模型检查阶段能够捕获原子操作的所有调度交错，
// 当启用 `--cfg loom` 时切换到它提供的原子类型；`Arc` 保持标准实现以维持
// `Eq`/`Hash` 推导能力，避免破坏 API 契约。
use core::fmt;
#[cfg(not(any(loom, spark_loom)))]
use core::sync::atomic::{AtomicBool, Ordering};
use core::time::Duration;
#[cfg(any(loom, spark_loom))]
use loom::sync::atomic::{AtomicBool, Ordering};

use crate::runtime::MonotonicTimePoint;

/// 取消原语，统一表达跨模块的可中断性契约。
///
/// # 设计背景（Why）
/// - 评审共识要求所有长时间运行的操作都能被外部主动打断，以避免雪崩扩散或无意义的资源占用。
/// - 传统 Future/任务取消机制在 `no_std` 环境下缺乏统一接口，因此通过轻量的原子位提供最小可行解。
///
/// # 逻辑解析（How）
/// - 内部使用 [`AtomicBool`] 表达取消状态，并通过 [`Arc`] 支持多方共享。
/// - `cancel` 在首次成功设置取消位时返回 `true`，后续重复调用将返回 `false` 以提示调用方避免重复执行业务兜底。
/// - `child` 生成共享同一原子位的派生实例，便于在不同子系统传播取消信号。
///
/// # 契约说明（What）
/// - **前置条件**：构造时无需额外参数，默认处于“未取消”状态。
/// - **后置条件**：一旦调用 `cancel` 成功，`is_cancelled` 必须在全局可见，后续 `CallContext` 派生出来的任务应尽快终止或回滚。
///
/// # 设计取舍与风险（Trade-offs）
/// - 未提供回调注册接口，避免在 `no_std` 下引入调度复杂度；若需要通知机制，可在上层使用轮询或自定义事件总线。
/// - 调用者需在关键热路径自行检查 `is_cancelled`，框架不会强制终止正在执行的 Future。
#[derive(Clone, Debug)]
pub struct Cancellation {
    inner: Arc<CancellationState>,
}

#[derive(Debug, Default)]
struct CancellationState {
    flag: AtomicBool,
}

impl Cancellation {
    /// 创建处于“未取消”状态的取消令牌。
    pub fn new() -> Self {
        Self {
            inner: Arc::new(CancellationState {
                flag: AtomicBool::new(false),
            }),
        }
    }

    /// 查询当前是否已被标记取消。
    pub fn is_cancelled(&self) -> bool {
        self.inner.flag.load(Ordering::Acquire)
    }

    /// 将当前令牌标记为取消。
    ///
    /// 返回值为 `true` 表示本次调用首次触发取消；返回 `false` 表示之前已被取消。
    pub fn cancel(&self) -> bool {
        self.inner
            .flag
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    /// 派生共享同一原子位的子令牌，用于跨模块传播取消语义。
    pub fn child(&self) -> Self {
        self.clone()
    }
}

impl Default for Cancellation {
    fn default() -> Self {
        Self::new()
    }
}

/// 截止原语，统一描述操作的最迟完成时间。
///
/// # 设计背景（Why）
/// - 引入 `Deadline` 可以在不依赖 `std::time::Instant` 的情况下表达单调时钟的绝对超时点。
/// - 搭配 [`Cancellation`] 使用时，可在超时到期后自动标记取消，实现统一的“超时即取消”策略。
///
/// # 契约说明（What）
/// - `Deadline` 可以为空（未设置），此时代表调用方未施加硬超时限制。
/// - `with_timeout` 以当前时间点和持续时间生成新的截止点，调用方需确保 `now` 来自同一计时源。
/// - `is_expired` 基于调用时提供的当前时间判断是否超时，避免依赖壁钟。
///
/// # 风险提示（Trade-offs）
/// - 截止时间不会自动驱动取消，调用方需在检测到超时后调用 [`Cancellation::cancel`]。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Deadline {
    instant: Option<MonotonicTimePoint>,
}

impl Deadline {
    /// 创建未设置截止时间的实例。
    pub const fn none() -> Self {
        Self { instant: None }
    }

    /// 根据绝对时间点构造截止时间。
    pub fn at(instant: MonotonicTimePoint) -> Self {
        Self {
            instant: Some(instant),
        }
    }

    /// 基于当前时间点加持续时间生成截止时间。
    pub fn with_timeout(now: MonotonicTimePoint, timeout: Duration) -> Self {
        Self::at(now.saturating_add(timeout))
    }

    /// 返回内部时间点，便于与自定义调度器协作。
    pub fn instant(&self) -> Option<MonotonicTimePoint> {
        self.instant
    }

    /// 判断是否已经超时。
    pub fn is_expired(&self, now: MonotonicTimePoint) -> bool {
        match self.instant {
            Some(deadline) => now >= deadline,
            None => false,
        }
    }
}

impl Default for Deadline {
    fn default() -> Self {
        Deadline::none()
    }
}

pub use crate::types::{Budget, BudgetDecision, BudgetKind, BudgetSnapshot, CloseReason};

/// 背压信号的统一表示，服务、Pipeline 与传输层通过它共享“是否可继续推进”的决策。
///
/// # 设计目标（Why）
/// - 以一个集中定义取代历史上散落在 `status::ready`、`transport::backpressure`、`service::metrics`
///   的多套枚举，确保“就绪/繁忙/预算耗尽/等待”四态在跨模块间具备一致含义；
/// - 将关闭语义（[`ShutdownGraceful`]、[`ShutdownImmediate`]) 纳入同一信号流，方便实现者在
///   收到强制关闭时快速短路业务逻辑；
/// - 为未来扩展（如限速建议、突发保护级别）预留空间，通过 `#[non_exhaustive]` 避免调用方匹配
///   时写死全部分支。
///
/// # 语义详解（How）
/// - `Ready`：明确表示可接受下一单位工作，常对应 `Service::poll_ready` 返回 `ReadyState::Ready`；
/// - `Busy`：通用忙碌信号，提示调用方稍后重试（无额外上下文）；
/// - `RetryAfter { delay }`：建议的退避时间，通常来自 I/O 层或队列节流策略；
/// - `BudgetExhausted { snapshot }`：预算耗尽，携带 [`BudgetSnapshot`] 用于日志或指标；
/// - `ShutdownPending`：优雅关闭已经开始，引用 [`ShutdownGraceful`] 解释原因与截止时间；
/// - `ShutdownEnforced`：立即关闭，表示后续操作应立刻终止，携带 [`ShutdownImmediate`]。
///
/// # 契约说明（What）
/// - **前置条件**：信号发布者需确保 `BudgetSnapshot`、`Shutdown*` 内的上下文字段已完成构造；
/// - **后置条件**：接收方可依据变体采取相应动作（例如停止重试、触发降级、刷新指标）；
/// - **互操作性**：该枚举不直接绑定具体通道或服务实例，可在任意 `Send + Sync` 环境中传递。
///
/// # 设计考量（Trade-offs）
/// - 并未细分“背压原因”字符串，避免恢复旧版 `BackpressureReason` 的复杂度；
/// - 关闭相关信号仅携带元信息，不驱动真正的资源释放，调用方需结合具体接口（如
///   [`Channel::close_graceful`](crate::pipeline::Channel::close_graceful)）执行动作。
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub enum BackpressureSignal {
    /// 完全就绪，可继续推进下一步工作。
    Ready,
    /// 暂时繁忙，建议稍后重新尝试。
    Busy,
    /// 提供明确退避时长的重试建议。
    RetryAfter { delay: Duration },
    /// 预算耗尽，携带统一的预算快照。
    BudgetExhausted { snapshot: BudgetSnapshot },
    /// 正在执行优雅关闭。
    ShutdownPending(ShutdownGraceful),
    /// 已触发立即关闭。
    ShutdownEnforced(ShutdownImmediate),
}

/// 立即关闭指令，强调“立刻终止，禁止等待排空”。
///
/// # 设计目标（Why）
/// - 为跨层关闭流程提供统一载体，避免传输层与服务层自行约定 `bool` 或字符串标记；
/// - 显式暴露关闭原因 [`CloseReason`]，便于日志与审计；
/// - 同步携带“是否可补救”的语义（通过与 [`ShutdownGraceful`] 区分），指导上层策略选择。
///
/// # 契约说明（What）
/// - **字段**：仅包含 `reason`，表达本次关闭的根因；
/// - **前置条件**：构造时必须提供语义化的关闭码与描述，建议遵守 `domain.category` 命名；
/// - **后置条件**：一旦进入立即关闭分支，调用方应终止 I/O、释放资源并向上报告。
///
/// # 风险提示（Trade-offs）
/// - 不提供截止时间字段，避免误解为可延迟执行；
/// - 若上层仍尝试优雅关闭，将与指令冲突，应在逻辑上显式判定并优先立即关闭。
#[derive(Clone, Debug, PartialEq)]
pub struct ShutdownImmediate {
    reason: CloseReason,
}

impl ShutdownImmediate {
    /// 构造立即关闭指令。
    pub fn new(reason: CloseReason) -> Self {
        Self { reason }
    }

    /// 读取关闭原因，便于记录或转发给外部系统。
    pub fn reason(&self) -> &CloseReason {
        &self.reason
    }
}

/// 优雅关闭计划，描述“排空 + 截止”式的慢速退出。
///
/// # 设计目标（Why）
/// - 统一服务端（Listener）、客户端（Channel）与业务 Service 的排空语义，减少重复文档；
/// - 携带 `reason` 与 `deadline`，为运维与自动化决策提供完整信息；
/// - 支持“无截止时间”场景，以兼容需要等待后台任务完成的长尾操作。
///
/// # 契约说明（What）
/// - `reason`：关闭原因，应指明触发方与影响范围；
/// - `deadline`：可选的绝对截止时间 [`Deadline`]，若为 `None` 表示仅排空不强制超时；
/// - `drain_timeout_hint`：可选的退避建议，供链路中间件调整监控或重试节奏。
///
/// # 风险提示（Trade-offs）
/// - `deadline` 未自动触发取消，调用方需结合 [`Cancellation`] 自行落地；
/// - 若 `drain_timeout_hint` 与 `deadline` 冲突（前者大于后者），以 `deadline` 为准。
#[derive(Clone, Debug, PartialEq)]
pub struct ShutdownGraceful {
    reason: CloseReason,
    deadline: Option<Deadline>,
    drain_timeout_hint: Option<Duration>,
}

impl ShutdownGraceful {
    /// 创建优雅关闭计划。
    pub fn new(
        reason: CloseReason,
        deadline: Option<Deadline>,
        drain_timeout_hint: Option<Duration>,
    ) -> Self {
        Self {
            reason,
            deadline,
            drain_timeout_hint,
        }
    }

    /// 读取关闭原因。
    pub fn reason(&self) -> &CloseReason {
        &self.reason
    }

    /// 获取截止时间（若存在）。
    pub fn deadline(&self) -> Option<&Deadline> {
        self.deadline.as_ref()
    }

    /// 获取排空建议时长，用于调节退避策略。
    pub fn drain_timeout_hint(&self) -> Option<Duration> {
        self.drain_timeout_hint
    }
}

/// 状态推进结果，配合 [`ContractStateMachine`] 描述状态转换效果。
///
/// # 设计目标（Why）
/// - 让状态机实现者在返回值中明确指示“是否发生状态跃迁”，便于上层据此触发事件或指标；
/// - 区分 `Noop` 与 `Transition`，避免上层重复记录或误判；
/// - 泛型参数 `S` 支持任何实现 `Copy + Eq` 的状态类型。
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StateAdvance<S>
where
    S: Copy + Eq,
{
    /// 状态未变化，通常表示收到重复信号或被动确认。
    Noop { state: S },
    /// 状态发生跃迁。
    Transition { from: S, to: S },
}

/// 最小状态机接口，约束 Pipeline、Transport 等组件如何驱动内部状态。
///
/// # 设计目标（Why）
/// - 提供跨子系统统一的“状态查询 + 信号驱动”模式，降低新模块接入门槛；
/// - 结合 [`BackpressureSignal`] 与关闭指令，为自动化推理（模型检查、TCK）提供稳定契约；
/// - 保持接口极简，避免强迫实现者暴露内部存储或引入多余同步原语。
///
/// # 契约说明（What）
/// - `State`：状态枚举，必须可比较且可复制，便于在日志与指标中使用；
/// - `Signal`：驱动状态的输入，可与 [`BackpressureSignal`]、协议事件等组合；
/// - `state()`：读取当前状态，应为无副作用操作；
/// - `on_signal(signal)`：根据输入信号推进状态，返回 [`StateAdvance`] 描述是否发生跃迁；
/// - **并发约束**：接口本身不规定同步策略，调用方需根据实现文档决定是否需要外部锁。
///
/// # 风险提示（Trade-offs）
/// - 若实现依赖外部锁或原子操作，应在文档中补充说明，以免上层误判线程安全；
/// - 返回 `Noop` 时务必保持状态未变，否则会破坏日志与度量的一致性。
pub trait ContractStateMachine {
    /// 状态枚举类型。
    type State: Copy + Eq;
    /// 驱动状态的信号。
    type Signal;

    /// 读取当前状态。
    fn state(&self) -> Self::State;

    /// 根据输入信号推进状态，并返回跃迁结果。
    fn on_signal(&mut self, signal: &Self::Signal) -> StateAdvance<Self::State>;
}

/// 安全上下文快照，承载调用者与对端的身份/策略元数据。
///
/// # 设计背景（Why）
/// - 符合“安全最小面”原则：接口默认启用安全校验，仅在显式调用 `allow_insecure` 后才允许降级。
/// - `Identity`、`Peer`、`Policy` 均以 [`Arc`] 包裹，确保在异步任务中复制成本可控且不会意外过期。
///
/// # 契约说明（What）
/// - `identity`：当前调用主体（如服务账户、人类用户）。
/// - `peer_identity`：通信对端身份（如下游服务、客户端证书）。
/// - `policy`：已生效的安全策略，用于审计与再评估。
/// - `allow_insecure`：显式标记可在当前调用中允许不安全模式（例如调试场景）。
#[derive(Clone, Debug, Default)]
pub struct SecurityContextSnapshot {
    identity: Option<Arc<IdentityDescriptor>>,
    peer_identity: Option<Arc<IdentityDescriptor>>,
    policy: Option<Arc<SecurityPolicy>>,
    allow_insecure: bool,
}

impl SecurityContextSnapshot {
    /// 设置当前调用主体。
    pub fn with_identity(mut self, identity: IdentityDescriptor) -> Self {
        self.identity = Some(Arc::new(identity));
        self
    }

    /// 设置通信对端身份。
    pub fn with_peer_identity(mut self, identity: IdentityDescriptor) -> Self {
        self.peer_identity = Some(Arc::new(identity));
        self
    }

    /// 设置生效策略。
    pub fn with_policy(mut self, policy: SecurityPolicy) -> Self {
        self.policy = Some(Arc::new(policy));
        self
    }

    /// 允许在当前调用中降级安全检查。
    pub fn allow_insecure(mut self) -> Self {
        self.allow_insecure = true;
        self
    }

    /// 获取主体身份。
    pub fn identity(&self) -> Option<&Arc<IdentityDescriptor>> {
        self.identity.as_ref()
    }

    /// 获取对端身份。
    pub fn peer_identity(&self) -> Option<&Arc<IdentityDescriptor>> {
        self.peer_identity.as_ref()
    }

    /// 获取当前策略。
    pub fn policy(&self) -> Option<&Arc<SecurityPolicy>> {
        self.policy.as_ref()
    }

    /// 是否允许降级为不安全模式。
    pub fn is_insecure_allowed(&self) -> bool {
        self.allow_insecure
    }

    /// 校验是否允许以安全模式继续执行。
    pub fn ensure_secure(&self) -> crate::Result<(), SparkError> {
        if self.allow_insecure {
            return Ok(());
        }
        if self.identity.is_none() {
            return Err(SparkError::new(
                crate::error::codes::APP_UNAUTHORIZED,
                "缺少调用身份，禁止在安全模式下继续执行",
            ));
        }
        if self.policy.is_none() {
            return Err(SparkError::new(
                crate::error::codes::APP_UNAUTHORIZED,
                "缺少生效安全策略，禁止在安全模式下继续执行",
            ));
        }
        Ok(())
    }
}

/// 可观测性契约，声明必须上报的指标/日志字段/追踪键。
///
/// # 设计背景（Why）
/// - 专家共识要求“可观测性即契约”，因此在核心上下文中提供稳定命名，避免实现者随意命名导致数据碎片化。
///
/// # 契约说明（What）
/// - `metric_names`：稳定的指标名称集合。
/// - `log_fields`：日志必须包含的字段键。
/// - `trace_keys`：Trace/Span 传播时需要保留的上下文字段。
/// - `audit_schema`：审计事件中要求的字段列表，建议遵循不可篡改的哈希链锚点语义。
#[derive(Clone, Debug, Default)]
pub struct ObservabilityContract {
    metric_names: &'static [&'static str],
    log_fields: &'static [&'static str],
    trace_keys: &'static [&'static str],
    audit_schema: &'static [&'static str],
}

impl ObservabilityContract {
    /// 创建契约定义。
    pub const fn new(
        metric_names: &'static [&'static str],
        log_fields: &'static [&'static str],
        trace_keys: &'static [&'static str],
        audit_schema: &'static [&'static str],
    ) -> Self {
        Self {
            metric_names,
            log_fields,
            trace_keys,
            audit_schema,
        }
    }

    /// 获取必报指标名称列表。
    pub fn metric_names(&self) -> &'static [&'static str] {
        self.metric_names
    }

    /// 获取日志字段键集合。
    pub fn log_fields(&self) -> &'static [&'static str] {
        self.log_fields
    }

    /// 获取追踪上下文键列表。
    pub fn trace_keys(&self) -> &'static [&'static str] {
        self.trace_keys
    }

    /// 获取审计事件字段 Schema。
    pub fn audit_schema(&self) -> &'static [&'static str] {
        self.audit_schema
    }
}

/// 默认可观测性契约，覆盖框架内核心数据点。
pub const DEFAULT_OBSERVABILITY_CONTRACT: ObservabilityContract = ObservabilityContract::new(
    &[
        "spark.request.total",
        "spark.request.duration",
        "spark.request.inflight",
        "spark.request.errors",
        "spark.bytes.inbound",
        "spark.bytes.outbound",
        "spark.codec.encode.duration",
        "spark.codec.decode.duration",
        "spark.codec.encode.bytes",
        "spark.codec.decode.bytes",
        "spark.codec.encode.errors",
        "spark.codec.decode.errors",
        "spark.transport.connections",
        "spark.transport.connection.attempts",
        "spark.transport.connection.failures",
        "spark.transport.handshake.duration",
        "spark.transport.bytes.inbound",
        "spark.transport.bytes.outbound",
        "spark.limits.usage",
        "spark.limits.limit",
        "spark.limits.hit",
        "spark.limits.drop",
        "spark.limits.degrade",
        "spark.limits.queue.depth",
        "spark.pipeline.epoch",
        "spark.pipeline.mutation.total",
    ],
    &[
        "request.id",
        "route.id",
        "caller.identity",
        "peer.identity",
        "budget.kind",
    ],
    &["traceparent", "tracestate", "spark-budget"],
    &[
        "event_id",
        "sequence",
        "occurred_at",
        "actor_id",
        "action",
        "entity_kind",
        "entity_id",
        "state_prev_hash",
        "state_curr_hash",
        "tsa_evidence",
    ],
);

struct CallContextInner {
    cancellation: Cancellation,
    deadline: Deadline,
    budgets: Vec<Budget>,
    security: SecurityContextSnapshot,
    observability: ObservabilityContract,
    trace_context: TraceContext,
    client_factory: Option<Arc<dyn ClientFactory>>,
    runtime: Option<Arc<dyn AsyncRuntime>>,
}

impl fmt::Debug for CallContextInner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CallContextInner")
            .field("cancellation", &self.cancellation)
            .field("deadline", &self.deadline)
            .field("budgets", &self.budgets)
            .field("security", &self.security)
            .field("observability", &self.observability)
            .field("trace_context", &self.trace_context)
            .field("client_factory", &self.client_factory.is_some())
            .field("runtime", &self.runtime.is_some())
            .finish()
    }
}

/// 调用上下文，在核心 API 之间传递取消/截止/预算三元组与安全、可观测性信息。
///
/// # 设计背景（Why）
/// - 统一承载专家共识中强调的“取消、截止、预算”三元组，确保任意公共方法都能访问这些信息。
/// - 结合安全、可观测性契约，将身份、策略、指标命名等元信息集中，避免各组件私自约定导致割裂。
///
/// # 契约说明（What）
/// - `Cancellation`：通过 [`CallContext::cancellation`] 获取，调用方在关键路径需及时检查取消标记。
/// - `Deadline`：使用 [`CallContext::deadline`] 查询绝对超时点，可结合 [`MonotonicTimePoint`] 判断是否过期。
/// - `Budgets`：通过 [`CallContext::budget`] 或 [`CallContext::budgets`] 获取，统一协调资源限额与背压语义。
/// - `SecurityContextSnapshot`：使用 [`CallContext::security`] 检查身份策略；若未显式允许不安全模式，调用前应调用 [`SecurityContextSnapshot::ensure_secure`].
/// - `ObservabilityContract`：通过 [`CallContext::observability`] 获取指标/日志/追踪键规范。
///
/// # 风险提示（Trade-offs）
/// - `CallContext` 通过 [`Arc`] 共享，克隆成本为常数，但仍需避免在热路径上不必要的 clone。
/// - 未内置自动取消逻辑，调用方需在超时或预算耗尽时主动触发取消以避免资源泄漏。
#[derive(Clone, Debug)]
pub struct CallContext {
    inner: Arc<CallContextInner>,
}

impl CallContext {
    /// 创建上下文构建器。
    pub fn builder() -> CallContextBuilder {
        CallContextBuilder::default()
    }

    /// 获取取消原语。
    pub fn cancellation(&self) -> &Cancellation {
        &self.inner.cancellation
    }

    /// 查询截止时间。
    pub fn deadline(&self) -> Deadline {
        self.inner.deadline
    }

    /// 按种类查找预算。
    pub fn budget(&self, kind: &BudgetKind) -> Option<Budget> {
        self.inner
            .budgets
            .iter()
            .find(|b| b.kind() == kind)
            .cloned()
    }

    /// 遍历所有预算。
    pub fn budgets(&self) -> impl Iterator<Item = &Budget> {
        self.inner.budgets.iter()
    }

    /// 返回调用元数据的只读视图，便于在热路径中读取取消/截止/预算/追踪/身份。
    ///
    /// # 使用场景（Why）
    /// - `Service::poll_ready`、`DynService::poll_ready_dyn` 等接口仅需读取调用状态即可决策背压或追踪采样；
    ///   通过该方法避免克隆整个 [`CallContext`]，降低热路径负担。
    ///
    /// # 契约说明（What）
    /// - **前置条件**：`self` 必须保持有效，且在返回的 [`Context`] 存活期间不得释放；
    /// - **后置条件**：返回视图仅提供只读访问；预算消费、身份变更仍需通过原始 [`CallContext`] 或安全模块处理。
    pub fn execution(&self) -> Context<'_> {
        Context::from(self)
    }

    /// 供 `Context` 生成零拷贝视图的内部辅助函数。
    pub(crate) fn budgets_slice(&self) -> &[Budget] {
        &self.inner.budgets
    }

    /// 获取安全上下文。
    pub fn security(&self) -> &SecurityContextSnapshot {
        &self.inner.security
    }

    /// 获取可观测性契约。
    pub fn observability(&self) -> &ObservabilityContract {
        &self.inner.observability
    }

    /// 获取当前追踪上下文。
    pub fn trace_context(&self) -> &TraceContext {
        &self.inner.trace_context
    }

    /// 获取可选的客户端工厂。
    ///
    /// # 教案式注释
    /// - **意图 (Why)**：允许上层在一次调用链中复用运行时注入的出站拨号能力，
    ///   例如 B2BUA 通过它建立 B-leg；
    /// - **契约 (What)**：返回 `Arc<dyn ClientFactory>` 的克隆，若未注入则为 `None`；
    /// - **前置条件**：调用方需确保 `CallContext` 仍处于有效生命周期；
    /// - **后置条件**：若返回 `Some`，则工厂实例可安全跨线程共享使用；
    /// - **风险提示 (Trade-offs)**：为避免无意义克隆，仅在确有拨号需求时调用。
    pub fn client_factory(&self) -> Option<Arc<dyn ClientFactory>> {
        self.inner.client_factory.as_ref().map(Arc::clone)
    }

    /// 获取可选的异步运行时句柄。
    ///
    /// # 教案式注释
    /// - **意图 (Why)**：业务层在提交异步任务时需要遵循治理契约，将取消、截止与追踪上下文传递给执行器；
    ///   通过显式暴露运行时句柄，调用方可以使用 [`AsyncRuntime::spawn`](crate::runtime::TaskExecutor::spawn)
    ///   在正确的执行环境中派生子任务，而无需依赖具体实现（Tokio、async-std 等）。
    /// - **契约 (What)**：
    ///   - 返回值为 `Option<&dyn AsyncRuntime>`：`Some` 表示外部已注入运行时能力，`None` 代表当前调用链
    ///     无法派生异步任务，调用方应尽早报错而非静默降级；
    ///   - 前置条件：调用方在使用前应确认返回值存在，并在提交任务时继续传递 `self` 以保持上下文；
    ///   - 后置条件：成功获取的运行时引用仅在 `CallContext` 存活期间有效，不应跨越其生命周期缓存。
    /// - **设计考量 (Trade-offs)**：
    ///   - 选择返回借用而非克隆，避免在热路径上无谓地复制执行器指针；
    ///   - 若未来需要在 `no_std` 环境以其他形式提供执行能力，可在 builder 中注入替代实现而无需改动业务代码。
    pub fn runtime(&self) -> Option<&dyn AsyncRuntime> {
        self.inner.runtime.as_deref()
    }
}

impl fmt::Display for CallContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let deadline = match self.deadline().instant() {
            Some(instant) => format!("{:?}", instant.as_duration()),
            None => "none".to_string(),
        };
        write!(
            f,
            "CallContext{{cancelled={}, deadline={}, budgets={}}}",
            self.cancellation().is_cancelled(),
            deadline,
            self.inner.budgets.len()
        )
    }
}

/// `CallContext` 构建器，确保在创建时完成参数验证。
pub struct CallContextBuilder {
    cancellation: Cancellation,
    deadline: Deadline,
    budgets: Vec<Budget>,
    security: SecurityContextSnapshot,
    observability: ObservabilityContract,
    trace_context: TraceContext,
    client_factory: Option<Arc<dyn ClientFactory>>,
    runtime: Option<Arc<dyn AsyncRuntime>>,
}

impl Default for CallContextBuilder {
    fn default() -> Self {
        Self {
            cancellation: Cancellation::new(),
            deadline: Deadline::none(),
            budgets: Vec::new(),
            security: SecurityContextSnapshot::default(),
            observability: ObservabilityContract::default(),
            trace_context: TraceContext::generate(),
            client_factory: None,
            runtime: None,
        }
    }
}

impl CallContextBuilder {
    /// 设置取消原语。
    pub fn with_cancellation(mut self, cancellation: Cancellation) -> Self {
        self.cancellation = cancellation;
        self
    }

    /// 设置截止时间。
    pub fn with_deadline(mut self, deadline: Deadline) -> Self {
        self.deadline = deadline;
        self
    }

    /// 添加预算。
    pub fn add_budget(mut self, budget: Budget) -> Self {
        self.budgets.push(budget);
        self
    }

    /// 设置安全上下文。
    pub fn with_security(mut self, security: SecurityContextSnapshot) -> Self {
        self.security = security;
        self
    }

    /// 设置可观测性契约。
    pub fn with_observability(mut self, observability: ObservabilityContract) -> Self {
        self.observability = observability;
        self
    }

    /// 设置追踪上下文。
    pub fn with_trace_context(mut self, trace_context: TraceContext) -> Self {
        self.trace_context = trace_context;
        self
    }

    /// 注入客户端工厂，实现者可据此复用运行时拨号能力。
    pub fn with_client_factory(mut self, client_factory: Arc<dyn ClientFactory>) -> Self {
        self.client_factory = Some(client_factory);
        self
    }

    /// 注入运行时能力，供业务层安全地派生异步任务。
    ///
    /// # 契约说明
    /// - **前置条件**：`runtime` 必须实现 [`AsyncRuntime`]，且生命周期覆盖 `CallContext` 的使用周期；
    /// - **后置条件**：构建出的上下文通过 [`CallContext::runtime`] 访问同一引用，调用方可据此调用 `spawn` 等接口。
    pub fn with_runtime(mut self, runtime: Arc<dyn AsyncRuntime>) -> Self {
        self.runtime = Some(runtime);
        self
    }

    /// 构建上下文，若未提供预算将默认提供“无限 Flow 预算”。
    pub fn build(self) -> CallContext {
        let budgets = if self.budgets.is_empty() {
            vec![Budget::unbounded(BudgetKind::Flow)]
        } else {
            self.budgets
        };
        CallContext {
            inner: Arc::new(CallContextInner {
                cancellation: self.cancellation,
                deadline: self.deadline,
                budgets,
                security: self.security,
                observability: self.observability,
                trace_context: self.trace_context,
                client_factory: self.client_factory,
                runtime: self.runtime,
            }),
        }
    }
}
