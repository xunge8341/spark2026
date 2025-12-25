use crate::{
    Error, TraceContext, cluster::NodeId, observability::OwnedAttributeSet, sealed::Sealed,
    security::SecurityClass, status::RetryAdvice, transport::TransportSocketAddr,
    types::BudgetKind,
};
use alloc::borrow::Cow;

pub mod category_matrix {
    //! 错误分类矩阵模块由构建脚本生成，当前文件通过 `include!` 将其纳入编译单元。
    //!
    //! # 教案式说明（Why）
    //! - 将生成文件集中置于 `error/generated/` 目录，避免手工维护静态矩阵；
    //! - 通过 `include!` 维持历史上 `spark_core::error::category_matrix` 的模块路径，确保外部调用者零感知迁移。
    //!
    //! # 契约定义（What）
    //! - 生成文件提供查询接口与数据结构，调用方依旧经由 `category_matrix::entries()` 等 API 访问；
    //! - **前置条件**：构建阶段已由 `build.rs` 写出最新矩阵；
    //! - **后置条件**：编译期包含的代码与 `contracts/error_matrix.toml` 保持一致。
    //!
    //! # 风险提示（Trade-offs & Gotchas）
    //! - 若构建脚本被禁用或生成文件缺失，编译会失败；CI 守门脚本会确保 SOT 资源同步。
    include!("error/generated/category_matrix.rs");
}
pub mod observability;
#[cfg(test)]
use alloc::format;
use alloc::{borrow::ToOwned, boxed::Box, string::String};
use core::fmt;
pub use observability::ErrorTelemetryView;

/// `CoreError` 表示 `spark-core` 跨层共享的稳定错误域，是所有可观察错误的最终形态。
///
/// # 契约维度速览
/// - **语义**：统一错误码、消息与链路上下文，形成“域层 → 核心层”唯一稳定回传格式。
/// - **错误**：自身即为错误结构；内部可携带底层 `cause`，并通过 `with_category`/`category` 映射到可重试、预算等语义。
/// - **并发**：`CoreError` 实现 `Send + Sync + 'static`，可安全在多线程间传递；内部使用 `Arc` 持有可选追踪信息。
/// - **背压**：结合 [`ErrorCategory::Retryable`] 与 [`RetryAdvice`] 转换为 [`BackpressureSignal`](crate::contract::BackpressureSignal)；错误本身不主动发出背压。
/// - **超时**：错误分类矩阵可附带 [`RetryAdvice`]，调用方可在超时场景中据此安排退避窗口。
/// - **取消**：当 `CallContext` 取消导致操作终止时，应构造 `CoreError` 并将 `code` 设置为 `contract.cancelled` 或等价语义。
/// - **观测标签**：建议统一上报 `error.code`、`error.category`、`error.retryable`、`error.node_id`，并关联 `TraceContext`。
/// - **示例(伪码)**：
///   ```text
///   let err = CoreError::new(codes::SERVICE_UNAVAILABLE, "backend drained")
///       .with_category(ErrorCategory::Retryable);
///   metrics.count("error", {"code": err.code()});
///   return Err(err);
///   ```
///
/// # 设计背景（Why）
/// - 运行时、域服务与实现细节在不同层次产生的故障需要合流为统一的错误码，以便日志、指标与告警系统能够执行精确的自动化治理。
/// - 框架仍需兼容 `no_std + alloc` 场景，因此不直接依赖 `std::error::Error`，而是复用 crate 内部定义的轻量抽象。
///
/// # 逻辑解析（How）
/// - 结构体以 Builder 风格方法叠加上下文信息（链路追踪、对端地址、节点 ID 与底层原因），并通过 `source()` 暴露完整链路。
/// - 错误码 `code` 始终为 `'static` 字符串，承载稳定语义；`message` 面向排障人员；其余字段用于补充机读上下文。
///
/// # 契约说明（What）
/// - **前置条件**：调用方必须使用 [`codes`] 模块或遵循 `<域>.<语义>` 约定的自定义码值；在构造时尚未附带域或实现层原因。
/// - **返回值**：构造函数返回拥有所有权的 `CoreError`，可安全跨线程移动（`Send + Sync + 'static`）。
/// - **后置条件**：除非显式调用 `with_*` 方法，错误不会包含额外上下文；链路上下文与节点信息需由调用方按需添加。
///
/// # 设计取舍与风险（Trade-offs）
/// - 采用 `String` 保存消息，牺牲极少量堆分配换取在日志、跨语言桥接时的灵活性。
/// - 允许附带可选的传输与链路信息，避免在轻量部署中产生不必要的依赖或复制成本。
///
/// `CoreError` 提供稳定的错误码与根因链路，是框架错误分层的最底层。
///
/// # 设计背景（Why）
/// - 错误分层共识要求“核心错误可稳定 round-trip”，因此该结构仅承载错误码、消息与底层 `source`。
/// - 通过对象安全的 [`Error`] 实现，保证在 `no_std + alloc` 环境下同样可用。
///
/// # 契约说明（What）
/// - `code`：稳定字符串，建议使用 `namespace.reason` 命名规范。
/// - `message`：人类可读描述，避免包含敏感信息。
/// - `cause`：可选底层原因；若不存在可设为 `None`。
///
/// # 风险提示（Trade-offs）
/// - 结构体仅负责承载信息，不执行任何格式化或指标上报逻辑；调用方需自行处理。
#[derive(Debug)]
pub struct CoreError {
    code: &'static str,
    message: Cow<'static, str>,
    cause: Option<ErrorCause>,
    metadata: CoreErrorMetadata,
}

impl CoreError {
    /// 构造核心错误。
    ///
    /// # 设计意图（Why）
    /// - 为框架与业务扩展提供统一的错误入口，确保所有错误码、消息在进入可观测链路前完成标准化封装。
    /// - 结合 [`crate::error::codes`] 模块的稳定枚举，帮助审计、SLO 与补救流程做精确分类。
    ///
    /// # 契约定义（What）
    /// - **输入参数**：
    ///   - `code`：遵循 `<领域>.<语义>` 约定的稳定错误码；调用方需保证其已在设计规范中备案。
    ///   - `message`：面向排障人员的自然语言描述，可为 `&'static str` 或堆分配字符串。
    /// - **前置条件**：调用场景已经根据业务上下文选定合适的错误码，且 `message` 不包含敏感信息。
    /// - **后置条件**：返回的 [`CoreError`] 拥有独立所有权，可在线程间安全传递，并准备好被进一步附加 Trace、节点信息或底层原因。
    ///
    /// # 执行步骤（How）
    /// 1. 将 `code` 与 `message` 按值存储，必要时触发一次堆分配以持有动态描述。
    /// 2. 将 `cause` 初始化为空，调用方可稍后通过 [`with_cause`](Self::with_cause) 或 [`set_cause`](Self::set_cause) 填充。
    ///
    /// # 示例（Examples）
    /// ```rust
    /// use spark_core::CoreError;
    /// use spark_core::error::codes;
    ///
    /// let err = CoreError::new(codes::APP_UNAUTHORIZED, "token expired");
    /// assert_eq!(err.code(), codes::APP_UNAUTHORIZED);
    /// assert_eq!(err.message(), "token expired");
    /// assert!(err.cause().is_none(), "初始错误默认不含底层原因");
    /// ```
    pub fn new(code: &'static str, message: impl Into<Cow<'static, str>>) -> Self {
        Self {
            code,
            message: message.into(),
            cause: None,
            metadata: CoreErrorMetadata::default(),
        }
    }

    /// 附带底层原因并返回新的核心错误。
    pub fn with_cause(mut self, cause: impl Error + Send + Sync + 'static) -> Self {
        self.cause = Some(Box::new(cause));
        self
    }

    /// 为现有错误设置底层原因。
    pub fn set_cause(&mut self, cause: impl Error + Send + Sync + 'static) {
        self.cause = Some(Box::new(cause));
    }

    /// 为错误标记结构化分类信息，驱动自动化容错策略。
    ///
    /// # 设计意图（Why）
    /// - 将“重试/预算/安全”等判定显式化，避免调用方通过解析字符串推断语义；
    /// - 允许默认 Pipeline 处理器根据分类自动转换为 `ReadyState` 或关闭策略。
    ///
    /// # 契约说明（What）
    /// - **输入**：`category` 表示错误的主要处置策略；
    /// - **前置条件**：应与错误码语义保持一致，避免将不可重试错误标记为 `Retryable`；
    /// - **后置条件**：返回新的 `CoreError`，内部分类信息被覆盖。
    pub fn with_category(mut self, category: ErrorCategory) -> Self {
        self.metadata.category = Some(category);
        self
    }

    /// 就地更新错误的分类信息。
    pub fn set_category(&mut self, category: ErrorCategory) {
        self.metadata.category = Some(category);
    }

    /// 将错误映射为标准观测属性并追加至集合。
    ///
    /// # 教案式说明
    /// - **意图（Why）**：集中处理 `error.*` 标签，避免各调用方重复拼接字符串；
    /// - **逻辑（How）**：借助 [`observability::ErrorTelemetryView`] 解析分类与补充信息，再写入 `OwnedAttributeSet`；
    /// - **契约（What）**：
    ///   - `target`：可变属性集合，方法会在末尾追加键值；
    ///   - 若错误无对应语义（如非重试错误没有等待时间），相关键将被跳过；
    ///   - 输出键遵循 `contracts/observability_keys.toml` 中的 `attributes::error` 契约。
    ///
    /// # 风险提示
    /// - 当 `RetryAdvice` 携带自定义描述或 `BudgetKind::Custom` 时需分配字符串，建议复用 `OwnedAttributeSet` 以摊销成本。
    pub fn write_observability_attributes(&self, target: &mut OwnedAttributeSet) {
        let view = observability::ErrorTelemetryView::from_error(self);
        view.apply_to(target);
    }

    /// 获取结构化错误分类。
    ///
    /// # 返回契约
    /// - 若未显式设置，默认返回 [`ErrorCategory::NonRetryable`]；
    /// - 调用方可据此驱动重试、预算耗尽或关闭策略。
    ///
    /// # 设计缘由（Why）
    /// - 运行时在 `on_exception_caught` 中需要据此触发自动化策略（退避、背压或关闭），
    ///   若每个调用点自行匹配错误码将导致语义漂移。
    /// - 通过集中式映射表（参见 `docs/error-category-matrix.md`）可保证契约机读，
    ///   也方便在文档与测试中保持同步。
    ///
    /// # 契约说明（What）
    /// - **前置条件**：错误码需遵循稳定命名并在分类矩阵中登记；若调用方显式调用
    ///   [`with_category`](Self::with_category) / [`set_category`](Self::set_category)，则以显式标记优先。
    /// - **返回值**：[`ErrorCategory`]，若未找到匹配项则回退为 `NonRetryable`，
    ///   表示默认不触发自动策略。
    /// - **后置条件**：查询不会修改内部状态，可被多次调用。
    ///
    /// # 执行逻辑（How）
    /// 1. 优先返回错误实例上显式设置的分类（用于业务覆盖默认策略）；
    /// 2. 若未设置，则按错误码查表，映射出统一的分类及默认 `RetryAdvice`/`BudgetKind`；
    /// 3. 查表失败时返回 `NonRetryable`，提醒调用方补充矩阵或手动设置分类。
    pub fn category(&self) -> ErrorCategory {
        self.metadata
            .category
            .clone()
            .or_else(|| category_matrix::matrix::lookup_default_category(self.code))
            .unwrap_or(ErrorCategory::NonRetryable)
    }

    /// 获取稳定错误码。
    pub fn code(&self) -> &'static str {
        self.code
    }

    /// 获取描述。
    pub fn message(&self) -> &str {
        &self.message
    }

    /// 获取底层原因。
    pub fn cause(&self) -> Option<&ErrorCause> {
        self.cause.as_ref()
    }

    /// 返回适合排障会议或值班新人的“人话”描述。
    ///
    /// # 设计意图（Why）
    /// - 运行日志常直接呈现技术细节（如 socket errno），对未熟悉拓扑与协议的排障人员不友好。
    /// - 通过稳定错误码映射出统一的、人类可读的摘要，可在页面、告警中复用并降低沟通成本。
    ///
    /// # 契约定义（What）
    /// - **输入前提**：调用方确保 `self.code()` 是稳定错误码；若为自定义码，需在文档中登记，否则回退到 `message()`。
    /// - **返回值**：`Cow<'static, str>`，若存在官方摘要则返回借用的静态文案；否则克隆核心消息，保证 `'static` 生命周期。
    /// - **后置条件**：不会修改内部状态，可在日志格式化、告警聚合等路径安全复用。
    ///
    /// # 执行逻辑（How）
    /// 1. 通过内部查表函数 `lookup_human_and_hint` 根据错误码检索预置的“十大常见故障”表。
    /// 2. 命中表项则直接借用其中的摘要；未命中时克隆现有 `message()`，保证依旧有可读输出。
    ///
    /// # 设计取舍与风险（Trade-offs）
    /// - 选择在运行期通过匹配表实现，避免在构造时提前分配与绑定，从而允许后续热更新表项。
    /// - 若调用方传入未备案的错误码，返回值会是原始消息：需在 SLO 报表中持续收敛自定义错误码。
    pub fn human(&self) -> Cow<'static, str> {
        lookup_human_and_hint(self.code)
            .map(|(human, _)| Cow::Borrowed(human))
            .unwrap_or_else(|| self.message.clone())
    }

    /// 返回修复建议，帮助值班人员在 30 分钟内完成处置。
    ///
    /// # 设计意图（Why）
    /// - 错误码背后常含步骤化的修复流程，若仅写在维基不利于新人查找；直接附在错误对象可在 CLI 或 Dashboard 即时展示。
    ///
    /// # 契约定义（What）
    /// - **返回值**：当错误码在官方表中登记时返回 `Some(Cow::Borrowed(hint))`；否则返回 `None`，鼓励调用方落地补充。
    /// - **前置条件**：无额外前置条件；本方法不会触发额外分配或 I/O。
    /// - **后置条件**：不影响 `CoreError` 内部 `message` 与 `cause`。
    ///
    /// # 执行逻辑（How）
    /// 1. 调用内部查表函数 `lookup_human_and_hint` 获得静态提示。
    /// 2. 将 `Option<&'static str>` 包装为 `Option<Cow<'static, str>>` 以兼容未来的动态提示实现。
    ///
    /// # 风险提示（Trade-offs）
    /// - 当前仅覆盖“十大常见错误”；若后续扩展需同步更新文档与测试。
    /// - 若错误码未覆盖，返回 `None`，调用方需在 UI 上做好兜底文案。
    pub fn hint(&self) -> Option<Cow<'static, str>> {
        lookup_human_and_hint(self.code).and_then(|(_, hint)| hint.map(Cow::Borrowed))
    }
}

impl fmt::Display for CoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[{}] {}", self.code, self.message)
    }
}

impl Error for CoreError {
    #[allow(unused_parens)]
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.cause
            .as_ref()
            .map(|boxed| boxed.as_ref() as &(dyn Error + 'static))
    }
}

/// 错误分类枚举，驱动自动化容错策略。
///
/// # 设计背景（Why）
/// - 统一表达“可重试”“预算耗尽”“安全违规”等关键信号，避免上层解析字符串；
/// - 与 Pipeline 默认处理器协作，将错误即时转换为 `ReadyState` 或关闭动作。
///
/// # 契约说明（What）
/// - `Retryable`：携带退避建议 [`RetryAdvice`]；
/// - `ResourceExhausted`：指出耗尽的 [`BudgetKind`]；
/// - `Security`：标记安全分类 [`SecurityClass`]；
/// - 其余分支对应确定性的策略，如 `Timeout`/`Cancelled` 触发取消，`ProtocolViolation` 触发关闭。
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum ErrorCategory {
    Retryable(RetryAdvice),
    NonRetryable,
    Security(SecurityClass),
    ResourceExhausted(BudgetKind),
    ProtocolViolation,
    Cancelled,
    Timeout,
}

/// 核心错误的附加属性容器。
#[derive(Clone, Debug, Default)]
struct CoreErrorMetadata {
    category: Option<ErrorCategory>,
}

/// `SparkError`（DomainError）在核心错误之上附加分布式上下文信息。
///
/// # 设计背景（Why）
/// - 结合 Trace/Peer/Node 元信息，满足跨节点调试、审计与 AIOps 需求。
/// - `source()` 始终返回内部的 [`CoreError`]，以保证错误链 `Impl → Domain → Core → Cause` 的 round-trip。
///
/// # 契约说明（What）
/// - 所有 Builder 方法返回 `Self`，保持不可变语义，避免多线程情况下出现部分更新。
/// - `with_cause` 实际调用核心错误的 `with_cause`，确保底层 cause 与核心层共享。
#[derive(Debug)]
pub struct SparkError {
    core: CoreError,
    trace_context: Option<TraceContext>,
    peer_addr: Option<TransportSocketAddr>,
    node_id: Option<NodeId>,
}

/// `ErrorCause` 封装底层原因，保持 `Send + Sync` 以方便跨线程传递。
pub type ErrorCause = Box<dyn Error + Send + Sync + 'static>;

/// `Result` 为框架统一的返回值别名，在所有层级提供稳定的错误边界。
///
/// # 设计意图（Why）
/// - 要求业务与实现层共享相同的错误封装模型，便于指标、日志与 AIOps 聚合时直接识别错误域；
/// - 避免在各处重复书写 `Result<_, CoreError>`、`Result<_, SparkError>` 等样板代码，提示开发者显式区分领域与实现层错误。
///
/// # 逻辑解析（How）
/// - 简单复用 `core::result::Result`，但通过别名集中约定默认错误类型为 [`CoreError`]；
/// - 调用方若需要返回领域或实现层错误，可在第二个泛型参数中显式指定自定义类型。
///
/// # 契约说明（What）
/// - **泛型参数**：
///   - `T`：成功路径上的返回值类型；
///   - `E`：错误类型，默认值为 [`CoreError`]；
/// - **前置条件**：调用点需确保错误类型符合框架要求（实现 `Error + Send + Sync + 'static`），以保障跨线程传递与 `source()` 链接。
/// - **后置条件**：获得的别名与标准库 `Result` 行为完全一致，可直接与 `?` 运算符、模式匹配协同工作。
///
/// # 设计取舍与风险（Trade-offs）
/// - 统一别名提升了可读性，也意味着在泛型约束或过程宏中需要显式引用 `spark_core::Result`；
/// - 若未来调整默认错误类型或增加额外上下文，仅需在此处更新别名即可全局生效，代价是所有子 crate 必须与核心保持版本一致。
pub type Result<T, E = CoreError> = core::result::Result<T, E>;

impl SparkError {
    /// 使用稳定错误码与消息创建 `SparkError`。
    pub fn new(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            core: CoreError::new(code, Cow::Owned(message.into())),
            trace_context: None,
            peer_addr: None,
            node_id: None,
        }
    }

    /// 获取稳定错误码，供日志聚合或自动化修复策略使用。
    pub fn code(&self) -> &'static str {
        self.core.code()
    }

    /// 获取人类可读的错误描述。
    pub fn message(&self) -> &str {
        self.core.message()
    }

    /// 附带一个底层原因，形成错误链。
    ///
    /// # 设计意图（Why）
    /// - 将域层或实现层错误纳入核心错误链，确保 `source()` 返回的链路可以被统一遍历。
    ///
    /// # 执行方式（How）
    /// - 使用 trait 对象将任意实现 `Error + Send + Sync + 'static` 的类型装箱。
    ///
    /// # 契约（What）
    /// - **前置条件**：`cause` 必须满足线程安全约束。
    /// - **后置条件**：返回新的 `CoreError`，其 `source()` 指向 `cause`。
    pub fn with_cause(mut self, cause: impl Error + Send + Sync + 'static) -> Self {
        self.core.set_cause(cause);
        self
    }

    /// 附带链路追踪信息，便于分布式追踪。
    pub fn with_trace(mut self, trace_context: TraceContext) -> Self {
        self.trace_context = Some(trace_context);
        self
    }

    /// 附带通信对端地址，帮助定位网络问题。
    pub fn with_peer_addr(mut self, peer: TransportSocketAddr) -> Self {
        self.peer_addr = Some(peer);
        self
    }

    /// 附带节点 ID，适配分布式部署环境。
    pub fn with_node_id(mut self, node: NodeId) -> Self {
        self.node_id = Some(node);
        self
    }

    /// 为领域错误标记结构化分类，便于上层根据分类采取措施。
    pub fn with_category(mut self, category: ErrorCategory) -> Self {
        self.core = self.core.with_category(category);
        self
    }

    /// 更新已有错误的分类信息。
    pub fn set_category(&mut self, category: ErrorCategory) {
        self.core.set_category(category);
    }

    /// 查询错误分类。
    pub fn category(&self) -> ErrorCategory {
        self.core.category()
    }

    /// 获取可选的链路追踪信息。
    pub fn trace_context(&self) -> Option<&TraceContext> {
        self.trace_context.as_ref()
    }

    /// 获取可选的对端地址。
    pub fn peer_addr(&self) -> Option<&TransportSocketAddr> {
        self.peer_addr.as_ref()
    }

    /// 获取可选的节点 ID。
    pub fn node_id(&self) -> Option<&NodeId> {
        self.node_id.as_ref()
    }

    /// 将领域错误转换为观测标签集合，委托核心错误的实现保持语义一致。
    ///
    /// # 教案式说明
    /// - **意图（Why）**：业务层面通常持有 `SparkError`，因此提供薄包装以复用核心错误的标签写入逻辑，避免重复解析。
    /// - **执行（How）**：直接调用内部 [`CoreError::write_observability_attributes`]，确保字段来源与核心一致。
    /// - **契约（What）**：
    ///   - `target`：可变属性集合；方法会追加 `error.*` 键值。
    ///   - **前置条件**：`target` 已初始化，允许被追加标签。
    ///   - **后置条件**：标签集包含与核心错误一致的错误码、分类、预算等维度。
    pub fn write_observability_attributes(&self, target: &mut OwnedAttributeSet) {
        self.core.write_observability_attributes(target);
    }

    /// 获取错误观测视图，便于测试或外部扩展直接消费标签快照。
    ///
    /// # 教案式说明
    /// - **意图（Why）**：允许上层在不持有 `OwnedAttributeSet` 的情况下，直接断言或转发结构化视图。
    /// - **执行（How）**：将内部核心错误交给 [`ErrorTelemetryView::from_error`] 构造结构体。
    /// - **契约（What）**：返回的视图包含所有契约字段；调用前无需额外初始化状态。
    pub fn observability_view(&self) -> ErrorTelemetryView {
        ErrorTelemetryView::from_error(&self.core)
    }

    /// 获取可选的底层原因。
    pub fn cause(&self) -> Option<&ErrorCause> {
        self.core.cause()
    }

    /// 访问核心错误。
    pub fn core(&self) -> &CoreError {
        &self.core
    }

    /// 返回适合业务值班理解的“人话”摘要，等价于内部核心错误的 [`CoreError::human`]。
    ///
    /// # 设计意图（Why）
    /// - 领域错误常在 GraphQL/HTTP 层直接暴露给上游；提供委托方法减少调用方拆箱 `core()` 的样板代码。
    ///
    /// # 契约（What）
    /// - **前置条件**：`self.core` 已使用稳定错误码构造。
    /// - **返回值**：`Cow<'static, str>`；成功命中官方映射时借用静态文案，否则克隆领域消息。
    /// - **后置条件**：不修改 `SparkError` 内部状态，可在 `Display` 之外的上下文重复调用。
    ///
    /// # 执行逻辑（How）
    /// - 直接委托给 [`CoreError::human`]，保持语义一致，避免重复维护。
    pub fn human(&self) -> Cow<'static, str> {
        self.core.human()
    }

    /// 返回修复建议，便于在终端或 UI 中指导排障动作。
    ///
    /// # 设计意图（Why）
    /// - 与 [`human`](Self::human) 配套，将“操作手册”直接挂在领域错误对象上，支持跨层透传。
    ///
    /// # 契约（What）
    /// - **返回值**：当核心错误码被官方文档收录时返回静态提示；否则返回 `None`，以便上游显示通用兜底信息。
    /// - **前置条件**：无额外前置条件。
    /// - **后置条件**：不影响错误链路或附加上下文。
    ///
    /// # 执行逻辑（How）
    /// - 委托 [`CoreError::hint`] 并原样返回结果。
    pub fn hint(&self) -> Option<Cow<'static, str>> {
        self.core.hint()
    }
}

impl fmt::Display for SparkError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.core)
    }
}

impl Error for SparkError {
    #[allow(unused_parens)]
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(self.core() as &dyn Error)
    }
}

/// `ImplError`（Implementation Error）包装领域错误并附加实现细节，同时保留实现侧的分类信息以供指标和降级策略使用。
///
/// # 设计背景（Why）
/// - 满足分层共识中“实现细节可扩展”的要求，便于在不同宿主/协议实现中记录私有信息。
///
/// # 契约说明（What）
/// - `detail`：实现层描述，推荐使用稳定字符串便于日志聚合。
/// - `source_cause`：实现层额外的底层错误，例如系统调用或第三方库异常。
#[derive(Debug)]
pub struct ImplError {
    kind: ImplErrorKind,
    domain: SparkError,
    detail: Option<Cow<'static, str>>,
    source_cause: Option<ErrorCause>,
}

impl ImplError {
    /// 使用实现层分类与领域错误上下文创建实现错误，确保错误链能够指明责任边界。
    ///
    /// # 契约说明
    /// - **参数**：`kind` 指向实现层发生故障的子系统；`domain` 为已封装领域语义的错误。
    /// - **前置条件**：`domain` 的 `cause` 可选；若已存在，将在链路上传递给上层。
    /// - **后置条件**：返回的实现错误默认不含额外明细，可通过 [`Self::with_detail`] 补充。
    pub fn new(kind: ImplErrorKind, domain: SparkError) -> Self {
        Self {
            kind,
            domain,
            detail: None,
            source_cause: None,
        }
    }

    /// 附加实现细节描述。
    pub fn with_detail(mut self, detail: impl Into<Cow<'static, str>>) -> Self {
        self.detail = Some(detail.into());
        self
    }

    /// 附加底层错误原因。
    pub fn with_source(mut self, cause: impl Error + Send + Sync + 'static) -> Self {
        self.source_cause = Some(Box::new(cause));
        self
    }

    /// 获取领域错误。
    pub fn domain(&self) -> &SparkError {
        &self.domain
    }

    /// 获取实现层分类，用于在日志或测试中断言具体的故障子域。
    ///
    /// # 返回值
    /// - 返回 `ImplErrorKind` 枚举，可直接用于匹配或指标打点。
    ///
    /// # 设计考量
    /// - 通过保留枚举值，而非仅输出字符串，调用方可以在编译期穷举处理逻辑，避免遗漏。
    pub fn kind(&self) -> ImplErrorKind {
        self.kind
    }

    /// 消耗自身并返回领域错误。
    pub fn into_domain(self) -> SparkError {
        self.domain
    }

    /// 获取实现细节描述。
    pub fn detail(&self) -> Option<&Cow<'static, str>> {
        self.detail.as_ref()
    }
}

impl fmt::Display for ImplError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.detail {
            Some(detail) => write!(f, "{} ({})", self.domain, detail),
            None => write!(f, "{}", self.domain),
        }
    }
}

impl Error for ImplError {
    #[allow(unused_parens)]
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        if let Some(ref cause) = self.source_cause {
            Some(cause.as_ref())
        } else {
            Some(self.domain() as &dyn Error)
        }
    }
}

/// 框架内置的错误码常量集合，确保可观测性系统具有稳定识别符。
pub mod codes {
    /// 分布式子系统的关键错误码集合。
    ///
    /// # 设计背景（Why）
    /// - 与评审阶段的共识保持一致：网络分区、领导者丢失、事件队列溢出、陈旧读取、版本冲突
    ///   是分布式系统面临的高频故障模式，必须提供标准化标识以便调用方实施兜底策略。
    /// - 错误码遵循 `<领域>.<语义>` 命名约定，方便在跨组件日志中检索与聚合。
    ///
    /// # 契约说明（What）
    /// - **使用前提**：错误码应由实现者封装进 [`CoreError`](crate::CoreError) 或下游错误类型，
    ///   并确保在链路日志、度量中携带完整上下文。
    /// - **返回承诺**：调用方收到这些错误码后，可据此触发补救措施（如退避、刷新快照、重试或
    ///   请求人工干预）。
    ///
    /// # 设计取舍与风险（Trade-offs）
    /// - 保持错误码粒度适中：既能覆盖分布式一致性与拓扑相关问题，又避免枚举过细导致实现者
    ///   难以判定场景。若后续引入更细分语义（如“复制日志滞后”），需确保与现有码值兼容。
    ///
    /// 传输层 I/O 错误。
    pub const TRANSPORT_IO: &str = "transport.io";
    /// 传输层超时。
    pub const TRANSPORT_TIMEOUT: &str = "transport.timeout";
    /// 协议解码失败。
    pub const PROTOCOL_DECODE: &str = "protocol.decode";
    /// 协议内容协商失败。
    pub const PROTOCOL_NEGOTIATION: &str = "protocol.negotiation";
    /// 协议预算超限。
    pub const PROTOCOL_BUDGET_EXCEEDED: &str = "protocol.budget_exceeded";
    /// 编解码类型不匹配。
    pub const PROTOCOL_TYPE_MISMATCH: &str = "protocol.type_mismatch";
    /// 运行时关闭。
    pub const RUNTIME_SHUTDOWN: &str = "runtime.shutdown";
    /// 集群节点不可用。
    pub const CLUSTER_NODE_UNAVAILABLE: &str = "cluster.node_unavailable";
    /// 集群未找到服务。
    pub const CLUSTER_SERVICE_NOT_FOUND: &str = "cluster.service_not_found";
    /// 集群控制面发生网络分区。
    pub const CLUSTER_NETWORK_PARTITION: &str = "cluster.network_partition";
    /// 集群当前领导者丢失（例如选举中）。
    pub const CLUSTER_LEADER_LOST: &str = "cluster.leader_lost";
    /// 集群内部事件队列溢出。
    pub const CLUSTER_QUEUE_OVERFLOW: &str = "cluster.queue_overflow";
    /// 服务发现返回陈旧数据。
    pub const DISCOVERY_STALE_READ: &str = "discovery.stale_read";
    /// 路由元数据版本冲突。
    pub const ROUTER_VERSION_CONFLICT: &str = "router.version_conflict";
    /// 应用路由失败。
    pub const APP_ROUTING_FAILED: &str = "app.routing_failed";
    /// 应用参数校验失败。
    ///
    /// - **使用场景**：配置解析、公共契约构造（如 [`crate::types::NonEmptyStr`]）发现输入为空或格式不符合约定。
    /// - **调用约定**：实现者在捕获到终端用户可修复的输入错误时，应返回该错误码，便于调用方提示或拒绝请求。
    pub const APP_INVALID_ARGUMENT: &str = "app.invalid_argument";
    /// 应用鉴权失败。
    pub const APP_UNAUTHORIZED: &str = "app.unauthorized";
    /// 应用背压施加。
    pub const APP_BACKPRESSURE_APPLIED: &str = "app.backpressure_applied";
}

/// 表征域层错误的类别，帮助评审者在文档中快速定位责任边界。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum DomainErrorKind {
    /// 传输层或底层 I/O 场景。
    Transport,
    /// 协议与编解码。
    Protocol,
    /// 运行时资源调度与生命周期管理。
    Runtime,
    /// 集群与拓扑管理。
    Cluster,
    /// 服务发现与配置。
    Discovery,
    /// 路由逻辑。
    Router,
    /// 应用或业务回调。
    Application,
    /// 缓冲区与内存池。
    Buffer,
}

/// 表征实现层（Impl）错误的精细分类，用于辅助诊断具体实现细节。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum ImplErrorKind {
    /// 对象池或缓冲区容量不足。
    BufferExhausted,
    /// 编解码注册或动态派发失败。
    CodecRegistry,
    /// 底层 I/O 或系统调用失败。
    Io,
    /// 操作超时或等待被取消。
    Timeout,
    /// 状态机违反约束或遇到非法状态。
    StateViolation,
    /// 未分类或暂未归档的实现细节错误。
    Uncategorized,
}

/// 域层错误，承载面向调用者的语义，并向核心错误映射。
///
/// # 设计背景（Why）
/// - 实现层错误语义过细，不宜直接暴露；域层需在保持语义完整的前提下，为核心错误提供稳定映射。
///
/// # 逻辑解析（How）
/// - 内部持有一个未携带底层原因的 `CoreError`，以及可选的 `ImplError` 原因；通过 [`IntoCoreError`] trait 执行最终映射。
///
/// # 契约说明（What）
/// - **构造约束**：`core` 不应预先设置 `cause`，否则将导致重复包装；调用方需确保 `core.cause()` 为空。
/// - **行为保证**：域层错误的 `source()` 指向实现层错误，保持错误链顺序。
#[derive(Debug)]
pub struct DomainError {
    kind: DomainErrorKind,
    core: CoreError,
    impl_cause: Option<Box<ImplError>>,
}

impl DomainError {
    /// 以域分类与核心错误上下文构造域层错误。
    ///
    /// # 契约说明
    /// - **参数**：`kind` 指出域责任边界；`core` 为尚未附带原因的核心错误。
    /// - **前置条件**：`core.cause()` 必须为空；若已有原因应在实现层处理后再构造域错误。
    /// - **后置条件**：`impl_cause` 默认缺省，可后续通过 [`Self::with_impl_cause`] 添补。
    pub fn new(kind: DomainErrorKind, core: CoreError) -> Self {
        debug_assert!(core.cause().is_none(), "CoreError 不应在域层前携带 cause");
        Self {
            kind,
            core,
            impl_cause: None,
        }
    }

    /// 从实现层错误直接构造域层错误，复用实现层的消息并保持错误链。
    ///
    /// # 契约
    /// - **参数**：`kind` 表示域分类；`code` 指定核心错误码；`impl_error` 为实现层错误。
    /// - **行为**：消息将沿用 `impl_error.domain().message()`，确保 Round-trip 不丢语义。
    pub fn from_impl(kind: DomainErrorKind, code: &'static str, impl_error: ImplError) -> Self {
        let message = impl_error.domain().message().to_owned();
        let core = CoreError::new(code, message);
        Self {
            kind,
            core,
            impl_cause: Some(Box::new(impl_error)),
        }
    }

    /// 附加实现层原因。
    pub fn with_impl_cause(mut self, cause: ImplError) -> Self {
        self.impl_cause = Some(Box::new(cause));
        self
    }

    /// 返回域分类。
    pub fn kind(&self) -> DomainErrorKind {
        self.kind
    }

    /// 访问域层承载的核心错误上下文。
    pub fn core(&self) -> &CoreError {
        &self.core
    }

    /// 访问实现层原因。
    pub fn impl_cause(&self) -> Option<&ImplError> {
        self.impl_cause.as_ref().map(|boxed| boxed.as_ref())
    }

    /// 返回面向值班同学的摘要描述，复用核心错误的“人话”解释。
    ///
    /// # 设计意图（Why）
    /// - 域层错误通常是业务 API 的最终返回值；直接提供摘要能避免上游服务重复拆包。
    ///
    /// # 契约（What）
    /// - **返回值**：`Cow<'static, str>`；命中常见故障表时为借用，否则克隆核心消息。
    /// - **前置条件**：域层错误已按照规范使用稳定错误码构造。
    /// - **后置条件**：不改变内部 `core` 或 `impl_cause`。
    ///
    /// # 执行逻辑（How）
    /// - 直接调用内部核心错误的 [`CoreError::human`]。
    pub fn human(&self) -> Cow<'static, str> {
        self.core.human()
    }

    /// 返回对应的修复建议（若存在），用于 CLI、工单系统等场景。
    ///
    /// # 设计意图（Why）
    /// - 在域层封装 `hint()`，可让业务同学快速找到排障手册，而无需了解框架内部结构。
    ///
    /// # 契约（What）
    /// - **返回值**：命中常见故障表时返回 `Some`，否则 `None`。
    /// - **前置条件**：无新增前置条件。
    /// - **后置条件**：保持域层错误链路不变。
    ///
    /// # 执行逻辑（How）
    /// - 复用 [`CoreError::hint`] 的实现，确保跨层输出一致。
    pub fn hint(&self) -> Option<Cow<'static, str>> {
        self.core.hint()
    }
}

impl fmt::Display for DomainError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}: {}", self.kind, self.core)
    }
}

impl Error for DomainError {
    #[allow(unused_parens)]
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.impl_cause
            .as_ref()
            .map(|boxed| boxed.as_ref() as &(dyn Error + 'static))
    }
}

/// 定义从域层映射到核心错误的统一接口，避免跳级转换。
pub trait IntoCoreError: Sealed {
    /// 将当前错误转换为核心错误，保持消息、错误码与链路来源。
    fn into_core_error(self) -> CoreError;
}

impl IntoCoreError for DomainError {
    fn into_core_error(self) -> CoreError {
        match self.impl_cause {
            Some(cause) => self.core.with_cause(*cause),
            None => self.core,
        }
    }
}

/// 定义实现层向域层上浮的映射接口。
pub trait IntoDomainError: Sealed {
    /// 将实现层错误映射为域层错误，调用者需显式提供域分类与核心错误码，避免层级混淆。
    fn into_domain_error(self, kind: DomainErrorKind, code: &'static str) -> DomainError;
}

impl IntoDomainError for ImplError {
    fn into_domain_error(self, kind: DomainErrorKind, code: &'static str) -> DomainError {
        DomainError::from_impl(kind, code, self)
    }
}

/// 根据稳定错误码查找“人话”摘要与修复建议。
///
/// # 设计背景（Why）
/// - 把常见错误与修复手册固化在代码中，可避免维基与实现脱节；通过单函数集中维护映射，便于测试与文档同步校验。
///
/// # 契约说明（What）
/// - **输入参数**：`code` 为遵循 `<领域>.<语义>` 规范的稳定错误码。
/// - **返回值**：若命中预置表，返回 `(human, hint)`；其中 `hint` 可为空表示暂未提供自动化指引。
/// - **前置条件**：调用方必须传入 `'static` 生命周期的码值，通常来自 [`codes`] 模块。
/// - **后置条件**：函数本身无副作用，纯读操作，可在 `no_std + alloc` 环境下安全复用。
///
/// # 执行逻辑（How）
/// - 通过 `match` 在编译期生成跳表，保证分支预测友好；表项严格覆盖“十大常见故障”。
///
/// # 风险提示（Trade-offs）
/// - 若新增错误码，需要同步更新此表、文档以及集成测试，否则 `hint()` 将返回 `None`。
fn lookup_human_and_hint(code: &str) -> Option<(&'static str, Option<&'static str>)> {
    use crate::error::codes;

    match code {
        codes::TRANSPORT_IO => Some((
            "传输层 I/O 故障：底层连接已断开或发生读写失败",
            Some("复查网络连通性或节点健康；必要时触发连接重建并观测是否持续报错"),
        )),
        codes::TRANSPORT_TIMEOUT => Some((
            "传输层超时：请求在约定时限内未获得响应",
            Some("确认服务端是否过载或限流；若系统正常请调高超时阈值并排查链路拥塞"),
        )),
        codes::PROTOCOL_DECODE => Some((
            "协议解码失败：收到的数据包格式不符合预期",
            Some("检查最近的协议版本变更或编解码配置；确保双方启用了兼容的 schema"),
        )),
        codes::PROTOCOL_NEGOTIATION => Some((
            "协议协商失败：双方未能达成可用的能力组合",
            Some("比对客户端与服务端支持的协议/加密选项；必要时回滚至兼容版本"),
        )),
        codes::PROTOCOL_BUDGET_EXCEEDED => Some((
            "协议预算超限：消息超过帧或速率限制",
            Some("核对调用是否突增或消息体过大；可临时提升预算并安排长期容量规划"),
        )),
        codes::CLUSTER_NODE_UNAVAILABLE => Some((
            "集群节点不可用：目标节点当前离线或失联",
            Some("通过运维面确认节点健康，必要时触发自动扩缩容或迁移流量"),
        )),
        codes::CLUSTER_LEADER_LOST => Some((
            "集群领导者丢失：当前处于选举或主节点故障",
            Some("等待选举完成并关注新 Leader 选出时间；若持续超时请检查共识组件日志"),
        )),
        codes::CLUSTER_QUEUE_OVERFLOW => Some((
            "集群事件队列溢出：内部缓冲区耗尽",
            Some("排查突发流量来源并削峰；临时提高队列容量或开启背压策略"),
        )),
        codes::DISCOVERY_STALE_READ => Some((
            "服务发现数据陈旧：拿到的拓扑已过期",
            Some("触发配置刷新或清理本地缓存；核对控制面 watch 是否正常工作"),
        )),
        codes::APP_UNAUTHORIZED => Some((
            "应用鉴权失败：凭证失效或权限不足",
            Some("重新发放凭证或调整访问策略；记录审计日志并通知调用方更新凭证"),
        )),
        _ => None,
    }
}

const _: fn() = || {
    fn assert_error_traits<T: Error + Send + Sync + 'static>() {}

    assert_error_traits::<CoreError>();
    assert_error_traits::<DomainError>();
    assert_error_traits::<ImplError>();
};

#[cfg(test)]
mod tests {
    use super::*;

    /// 验证实现层 → 域层 → 核心错误的 Round-trip，确保语义保持。
    #[test]
    fn impl_to_domain_to_core_roundtrip_preserves_message_and_cause() {
        // 准备一个实现层错误，包含详细信息与底层原因。
        let impl_cause = ImplError::new(
            ImplErrorKind::BufferExhausted,
            SparkError::new(codes::PROTOCOL_BUDGET_EXCEEDED, "buffer depleted"),
        )
        .with_detail("pool=ingress")
        .with_source(CoreError::new("inner.code", "inner message"));

        // 域层将其映射为核心错误码，并保持消息与 cause。
        let domain_error =
            impl_cause.into_domain_error(DomainErrorKind::Buffer, codes::PROTOCOL_BUDGET_EXCEEDED);

        // 提前检查：域层应能访问实现层 cause。
        let cause = domain_error.impl_cause().expect("impl cause must exist");
        assert_eq!(cause.kind(), ImplErrorKind::BufferExhausted);
        assert_eq!(cause.domain().message(), "buffer depleted");

        // 转换到核心错误后，错误码与消息保持一致，source 链仍可回溯。
        let core_error = domain_error.into_core_error();
        assert_eq!(core_error.code(), codes::PROTOCOL_BUDGET_EXCEEDED);
        assert_eq!(core_error.message(), "buffer depleted");

        let current: &dyn Error = &core_error;
        // 第一层 source：实现层错误。
        let first = current
            .source()
            .expect("core error should expose impl error");
        assert_eq!(
            format!("{}", first),
            "[protocol.budget_exceeded] buffer depleted (pool=ingress)"
        );

        // 第二层 source：实现层中的底层 cause。
        let second = first
            .source()
            .expect("impl error should expose inner cause");
        assert_eq!(format!("{}", second), "[inner.code] inner message");
    }
}
