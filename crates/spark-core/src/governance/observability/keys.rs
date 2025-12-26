// @generated 自动生成文件，请勿手工修改。
// 由 crates/spark-core/build.rs 根据 contracts/observability_keys.toml 生成。

//! 可观测性键名契约：统一指标、日志与追踪键名的单一事实来源。
//!
//! 教案式说明（Why）：数据来自 `contracts/observability_keys.toml`，构建脚本与工具据此生成代码与文档，避免多处漂移。
//! 契约定义（What）：各子模块（如 `metrics::service`）提供只读常量，供指标、日志与追踪统一引用。
//! 实现细节（How）：构建阶段展开模块树并写入稳定的 Rust 源文件，每个常量附带类型与适用范围说明。

/// attributes 键名分组（模块标识：attributes）
///
/// 该分组由 contracts/observability_keys.toml 自动生成。
pub mod attributes {

    /// 错误观测属性键
    ///
    /// 跨指标/日志/追踪复用的错误标签集合，匹配 CoreError 契约。
    pub mod error {
        #[doc = "类型：指标/日志键。"]
        #[doc = "适用范围：指标、日志、追踪。"]
        #[doc = ""]
        #[doc = "稳定错误码，遵循 <域>.<语义> 约定。"]
        pub const ATTR_CODE: &str = "error.code";

        #[doc = "类型：指标/日志键。"]
        #[doc = "适用范围：指标、日志、追踪。"]
        #[doc = ""]
        #[doc = "错误分类名称，与 ErrorCategory 变体一一对应。"]
        pub const ATTR_CATEGORY: &str = "error.category";

        #[doc = "类型：指标/日志键。"]
        #[doc = "适用范围：指标、日志、追踪。"]
        #[doc = ""]
        #[doc = "稳定错误类别标签，用于聚合错误趋势。"]
        pub const ATTR_KIND: &str = "error.kind";

        #[doc = "类型：指标/日志键。"]
        #[doc = "适用范围：指标、日志、追踪。"]
        #[doc = ""]
        #[doc = "布尔值，指示错误是否建议重试。"]
        pub const ATTR_RETRYABLE: &str = "error.retryable";

        #[doc = "类型：指标/日志键。"]
        #[doc = "适用范围：指标、日志。"]
        #[doc = ""]
        #[doc = "Retryable 错误建议等待的毫秒数。"]
        pub const ATTR_RETRY_WAIT_MS: &str = "error.retry.wait_ms";

        #[doc = "类型：指标/日志键。"]
        #[doc = "适用范围：日志。"]
        #[doc = ""]
        #[doc = "Retryable 错误附带的退避原因描述。"]
        pub const ATTR_RETRY_REASON: &str = "error.retry.reason";

        #[doc = "类型：指标/日志键。"]
        #[doc = "适用范围：指标、日志、追踪。"]
        #[doc = ""]
        #[doc = "安全相关错误所属的安全分类（authentication/authorization 等）。"]
        pub const ATTR_SECURITY_CLASS: &str = "error.security.class";

        #[doc = "类型：指标/日志键。"]
        #[doc = "适用范围：指标、日志。"]
        #[doc = ""]
        #[doc = "资源耗尽类错误关联的预算类型。"]
        pub const ATTR_BUDGET_KIND: &str = "error.budget.kind";

        #[doc = "类型：指标/日志键。"]
        #[doc = "适用范围：指标、日志。"]
        #[doc = ""]
        #[doc = "重试类错误导致的繁忙方向（上游/下游）。"]
        pub const ATTR_BUSY_DIRECTION: &str = "error.busy.direction";
    }
}

/// labels 键名分组（模块标识：labels）
///
/// 该分组由 contracts/observability_keys.toml 自动生成。
pub mod labels {

    /// 错误类别标签值
    ///
    /// ErrorCategory 到 error.kind 的标准枚举值。
    pub mod error_kind {
        #[doc = "类型：标签枚举值。"]
        #[doc = "适用范围：指标、日志、追踪。"]
        #[doc = ""]
        #[doc = "可重试错误。"]
        pub const RETRYABLE: &str = "retryable";

        #[doc = "类型：标签枚举值。"]
        #[doc = "适用范围：指标、日志、追踪。"]
        #[doc = ""]
        #[doc = "不可重试错误。"]
        pub const NON_RETRYABLE: &str = "non_retryable";

        #[doc = "类型：标签枚举值。"]
        #[doc = "适用范围：指标、日志、追踪。"]
        #[doc = ""]
        #[doc = "资源或预算耗尽。"]
        pub const RESOURCE_EXHAUSTED: &str = "resource_exhausted";

        #[doc = "类型：标签枚举值。"]
        #[doc = "适用范围：指标、日志、追踪。"]
        #[doc = ""]
        #[doc = "安全违规或合规相关错误。"]
        pub const SECURITY: &str = "security";

        #[doc = "类型：标签枚举值。"]
        #[doc = "适用范围：指标、日志、追踪。"]
        #[doc = ""]
        #[doc = "协议契约被破坏。"]
        pub const PROTOCOL_VIOLATION: &str = "protocol_violation";

        #[doc = "类型：标签枚举值。"]
        #[doc = "适用范围：指标、日志、追踪。"]
        #[doc = ""]
        #[doc = "调用超时。"]
        pub const TIMEOUT: &str = "timeout";

        #[doc = "类型：标签枚举值。"]
        #[doc = "适用范围：指标、日志、追踪。"]
        #[doc = ""]
        #[doc = "调用被主动取消。"]
        pub const CANCELLED: &str = "cancelled";

        #[doc = "类型：标签枚举值。"]
        #[doc = "适用范围：指标、日志、追踪。"]
        #[doc = ""]
        #[doc = "未归类错误的兜底标签。"]
        pub const UNKNOWN: &str = "unknown";
    }
}

/// logging 键名分组（模块标识：logging）
///
/// 该分组由 contracts/observability_keys.toml 自动生成。
pub mod logging {

    /// 审计事件日志字段
    ///
    /// AuditEvent 序列化到日志或运维事件时复用的字段，保持合规链条可追溯。
    pub mod audit {
        #[doc = "类型：日志字段。"]
        #[doc = "适用范围：日志、运维事件。"]
        #[doc = ""]
        #[doc = "审计事件的全局唯一标识。"]
        pub const FIELD_EVENT_ID: &str = "audit.event.id";

        #[doc = "类型：日志字段。"]
        #[doc = "适用范围：日志、运维事件。"]
        #[doc = ""]
        #[doc = "事件在同一实体上的顺序号，便于检测缺失。"]
        pub const FIELD_SEQUENCE: &str = "audit.sequence";

        #[doc = "类型：日志字段。"]
        #[doc = "适用范围：日志、运维事件。"]
        #[doc = ""]
        #[doc = "被操作实体的类型，例如 configuration.profile。"]
        pub const FIELD_ENTITY_KIND: &str = "audit.entity.kind";

        #[doc = "类型：日志字段。"]
        #[doc = "适用范围：日志、运维事件。"]
        #[doc = ""]
        #[doc = "被操作实体的唯一标识。"]
        pub const FIELD_ENTITY_ID: &str = "audit.entity.id";

        #[doc = "类型：日志字段。"]
        #[doc = "适用范围：日志、运维事件。"]
        #[doc = ""]
        #[doc = "执行的动作名称，例如 apply_changeset。"]
        pub const FIELD_ACTION: &str = "audit.action";

        #[doc = "类型：日志字段。"]
        #[doc = "适用范围：日志、运维事件。"]
        #[doc = ""]
        #[doc = "触发事件的操作者标识。"]
        pub const FIELD_ACTOR_ID: &str = "audit.actor.id";

        #[doc = "类型：日志字段。"]
        #[doc = "适用范围：日志、运维事件。"]
        #[doc = ""]
        #[doc = "操作者所属租户或逻辑空间，可为空。"]
        pub const FIELD_ACTOR_TENANT: &str = "audit.actor.tenant";

        #[doc = "类型：日志字段。"]
        #[doc = "适用范围：日志、运维事件。"]
        #[doc = ""]
        #[doc = "变更前状态的 SHA-256 哈希。"]
        pub const FIELD_STATE_PREV_HASH: &str = "audit.state.prev_hash";

        #[doc = "类型：日志字段。"]
        #[doc = "适用范围：日志、运维事件。"]
        #[doc = ""]
        #[doc = "变更后状态的 SHA-256 哈希。"]
        pub const FIELD_STATE_CURR_HASH: &str = "audit.state.curr_hash";

        #[doc = "类型：日志字段。"]
        #[doc = "适用范围：日志、运维事件。"]
        #[doc = ""]
        #[doc = "事件发生时间，Unix 毫秒。"]
        pub const FIELD_OCCURRED_AT: &str = "audit.occurred_at";

        #[doc = "类型：日志字段。"]
        #[doc = "适用范围：日志、运维事件。"]
        #[doc = ""]
        #[doc = "可信时间戳证书链标识，用于合规审计。"]
        pub const FIELD_TSA_CHAIN: &str = "audit.tsa.chain";

        #[doc = "类型：日志字段。"]
        #[doc = "适用范围：日志、运维事件。"]
        #[doc = ""]
        #[doc = "变更条目的数量，帮助快速估算影响范围。"]
        pub const FIELD_CHANGE_COUNT: &str = "audit.change.count";
    }

    /// 弃用公告日志字段
    ///
    /// 统一的弃用告警日志字段集合，便于告警平台解析。
    pub mod deprecation {
        #[doc = "类型：日志字段。"]
        #[doc = "适用范围：日志、运维事件。"]
        #[doc = ""]
        #[doc = "被弃用的符号或能力标识。"]
        pub const FIELD_SYMBOL: &str = "deprecation.symbol";

        #[doc = "类型：日志字段。"]
        #[doc = "适用范围：日志、运维事件。"]
        #[doc = ""]
        #[doc = "自哪个版本开始弃用。"]
        pub const FIELD_SINCE: &str = "deprecation.since";

        #[doc = "类型：日志字段。"]
        #[doc = "适用范围：日志、运维事件。"]
        #[doc = ""]
        #[doc = "计划移除的版本或时间。"]
        pub const FIELD_REMOVAL: &str = "deprecation.removal";

        #[doc = "类型：日志字段。"]
        #[doc = "适用范围：日志、运维事件。"]
        #[doc = ""]
        #[doc = "追踪链接或工单地址。"]
        pub const FIELD_TRACKING: &str = "deprecation.tracking";

        #[doc = "类型：日志字段。"]
        #[doc = "适用范围：日志、运维事件。"]
        #[doc = ""]
        #[doc = "迁移或替代方案提示。"]
        pub const FIELD_MIGRATION: &str = "deprecation.migration";
    }

    /// 关机流程日志字段
    ///
    /// Host Shutdown 生命周期的结构化日志字段。
    pub mod shutdown {
        #[doc = "类型：日志字段。"]
        #[doc = "适用范围：日志、运维事件。"]
        #[doc = ""]
        #[doc = "关机原因代码，通常来源于治理策略。"]
        pub const FIELD_REASON_CODE: &str = "shutdown.reason.code";

        #[doc = "类型：日志字段。"]
        #[doc = "适用范围：日志、运维事件。"]
        #[doc = ""]
        #[doc = "关机原因文本描述。"]
        pub const FIELD_REASON_MESSAGE: &str = "shutdown.reason.message";

        #[doc = "类型：日志字段。"]
        #[doc = "适用范围：日志、运维事件。"]
        #[doc = ""]
        #[doc = "计划关机的目标数量。"]
        pub const FIELD_TARGET_COUNT: &str = "shutdown.target.count";

        #[doc = "类型：日志字段。"]
        #[doc = "适用范围：日志、运维事件。"]
        #[doc = ""]
        #[doc = "关机截止时间与当前时刻的剩余毫秒数。"]
        pub const FIELD_DEADLINE_MS: &str = "shutdown.deadline.ms";

        #[doc = "类型：日志字段。"]
        #[doc = "适用范围：日志、运维事件。"]
        #[doc = ""]
        #[doc = "被关机目标的逻辑标识。"]
        pub const FIELD_TARGET_LABEL: &str = "shutdown.target.label";

        #[doc = "类型：日志字段。"]
        #[doc = "适用范围：日志、运维事件。"]
        #[doc = ""]
        #[doc = "关机过程出现的错误码。"]
        pub const FIELD_ERROR_CODE: &str = "shutdown.error.code";

        #[doc = "类型：日志字段。"]
        #[doc = "适用范围：日志、运维事件。"]
        #[doc = ""]
        #[doc = "关机耗时，毫秒。"]
        pub const FIELD_ELAPSED_MS: &str = "shutdown.elapsed.ms";
    }
}

/// metrics 键名分组（模块标识：metrics）
///
/// 该分组由 contracts/observability_keys.toml 自动生成。
pub mod metrics {

    /// Codec 域指标键
    ///
    /// 编解码链路的标签与枚举，辅助区分 encode/decode、错误类型与内容类型。
    pub mod codec {
        #[doc = "类型：指标/日志键。"]
        #[doc = "适用范围：指标、日志。"]
        #[doc = ""]
        #[doc = "编解码器名称，通常与协议/格式实现绑定。"]
        pub const ATTR_CODEC_NAME: &str = "codec.name";

        #[doc = "类型：指标/日志键。"]
        #[doc = "适用范围：指标。"]
        #[doc = ""]
        #[doc = "编码/解码模式标签。"]
        pub const ATTR_MODE: &str = "codec.mode";

        #[doc = "类型：指标/日志键。"]
        #[doc = "适用范围：指标、日志。"]
        #[doc = ""]
        #[doc = "内容类型或媒体类型，例如 application/grpc。"]
        pub const ATTR_CONTENT_TYPE: &str = "content.type";

        #[doc = "类型：指标/日志键。"]
        #[doc = "适用范围：指标、日志。"]
        #[doc = ""]
        #[doc = "编解码阶段的错误分类。"]
        pub const ATTR_ERROR_KIND: &str = "error.kind";

        #[doc = "类型：标签枚举值。"]
        #[doc = "适用范围：指标。"]
        #[doc = ""]
        #[doc = "Codec 模式：编码。"]
        pub const MODE_ENCODE: &str = "encode";

        #[doc = "类型：标签枚举值。"]
        #[doc = "适用范围：指标。"]
        #[doc = ""]
        #[doc = "Codec 模式：解码。"]
        pub const MODE_DECODE: &str = "decode";
    }

    /// 热更新指标键
    ///
    /// 运行时热更新流程的标签集合。
    pub mod hot_reload {
        #[doc = "类型：指标/日志键。"]
        #[doc = "适用范围：指标、日志。"]
        #[doc = ""]
        #[doc = "热更新组件名称，如 limits/timeouts。"]
        pub const ATTR_COMPONENT: &str = "hot_reload.component";
    }

    /// 限流/限额指标键
    ///
    /// 资源限额治理相关的标签集合。
    pub mod limits {
        #[doc = "类型：指标/日志键。"]
        #[doc = "适用范围：指标、日志。"]
        #[doc = ""]
        #[doc = "资源类型标签，例如并发/队列。"]
        pub const ATTR_RESOURCE: &str = "limit.resource";

        #[doc = "类型：指标/日志键。"]
        #[doc = "适用范围：指标、日志。"]
        #[doc = ""]
        #[doc = "策略动作标签，如 queue/drop/degrade。"]
        pub const ATTR_ACTION: &str = "limit.action";
    }

    /// Pipeline 域指标/日志键
    ///
    /// Pipeline 纪元、控制器与变更事件的统一标签。
    pub mod pipeline {
        #[doc = "类型：指标/日志键。"]
        #[doc = "适用范围：指标、日志。"]
        #[doc = ""]
        #[doc = "Pipeline 控制器实现标识。"]
        pub const ATTR_CONTROLLER: &str = "pipeline.controller";

        #[doc = "类型：指标/日志键。"]
        #[doc = "适用范围：指标、日志。"]
        #[doc = ""]
        #[doc = "Pipeline 唯一标识，通常映射到 Channel ID。"]
        pub const ATTR_PIPELINE_ID: &str = "pipeline.id";

        #[doc = "类型：指标/日志键。"]
        #[doc = "适用范围：指标、日志。"]
        #[doc = ""]
        #[doc = "变更操作类型。"]
        pub const ATTR_MUTATION_OP: &str = "pipeline.mutation.op";

        #[doc = "类型：指标/日志键。"]
        #[doc = "适用范围：指标、日志。"]
        #[doc = ""]
        #[doc = "Pipeline 逻辑纪元。"]
        pub const ATTR_EPOCH: &str = "pipeline.epoch";

        #[doc = "类型：标签枚举值。"]
        #[doc = "适用范围：指标、日志。"]
        #[doc = ""]
        #[doc = "变更操作：新增 Handler。"]
        pub const OP_ADD: &str = "add";

        #[doc = "类型：标签枚举值。"]
        #[doc = "适用范围：指标、日志。"]
        #[doc = ""]
        #[doc = "变更操作：移除 Handler。"]
        pub const OP_REMOVE: &str = "remove";

        #[doc = "类型：标签枚举值。"]
        #[doc = "适用范围：指标、日志。"]
        #[doc = ""]
        #[doc = "变更操作：替换 Handler。"]
        pub const OP_REPLACE: &str = "replace";

        #[doc = "类型：标签枚举值。"]
        #[doc = "适用范围：指标、日志。"]
        #[doc = ""]
        #[doc = "控制器枚举：HotSwap 控制器实现。"]
        pub const CONTROLLER_HOT_SWAP: &str = "hot_swap";
    }

    /// Service 域指标/日志键
    ///
    /// 服务调用面指标与结构化日志共享的标签与标签值，覆盖 ReadyState、Outcome 等核心维度。
    pub mod service {
        #[doc = "类型：指标/日志键。"]
        #[doc = "适用范围：指标、日志。"]
        #[doc = ""]
        #[doc = "业务服务名，建议与服务注册中心或配置中的逻辑名称保持一致。"]
        pub const ATTR_SERVICE_NAME: &str = "service.name";

        #[doc = "类型：指标/日志键。"]
        #[doc = "适用范围：指标、日志。"]
        #[doc = ""]
        #[doc = "路由或逻辑分组标识，例如 API Path、租户 ID 映射。"]
        pub const ATTR_ROUTE_ID: &str = "route.id";

        #[doc = "类型：指标/日志键。"]
        #[doc = "适用范围：指标、日志。"]
        #[doc = ""]
        #[doc = "操作/方法名，统一为低基数字符串，如 grpc 方法或 RPC 名称。"]
        pub const ATTR_OPERATION: &str = "operation";

        #[doc = "类型：指标/日志键。"]
        #[doc = "适用范围：指标、日志、追踪。"]
        #[doc = ""]
        #[doc = "入站协议标识，常见值如 grpc/http/quic。"]
        pub const ATTR_PROTOCOL: &str = "protocol";

        #[doc = "类型：指标/日志键。"]
        #[doc = "适用范围：指标、日志。"]
        #[doc = ""]
        #[doc = "HTTP/gRPC 等响应码或业务状态码。"]
        pub const ATTR_STATUS_CODE: &str = "status.code";

        #[doc = "类型：指标/日志键。"]
        #[doc = "适用范围：指标、日志。"]
        #[doc = ""]
        #[doc = "调用总体结果，限制在 success/error 等少量枚举内。"]
        pub const ATTR_OUTCOME: &str = "outcome";

        #[doc = "类型：指标/日志键。"]
        #[doc = "适用范围：指标、日志。"]
        #[doc = ""]
        #[doc = "稳定错误分类（如 timeout/internal/security），需与错误分类矩阵保持一致。"]
        pub const ATTR_ERROR_KIND: &str = "error.kind";

        #[doc = "类型：指标/日志键。"]
        #[doc = "适用范围：指标、日志。"]
        #[doc = ""]
        #[doc = "对端身份标签，例如 upstream/downstream 或租户别名，需保持低基数。"]
        pub const ATTR_PEER_IDENTITY: &str = "peer.identity";

        #[doc = "类型：指标/日志键。"]
        #[doc = "适用范围：指标、日志。"]
        #[doc = ""]
        #[doc = "ReadyState 主枚举值，用于关联治理 ReadyState 仪表盘。"]
        pub const ATTR_READY_STATE: &str = "ready.state";

        #[doc = "类型：指标/日志键。"]
        #[doc = "适用范围：指标、日志。"]
        #[doc = ""]
        #[doc = "ReadyState 细分详情，承载更具体的背压原因。"]
        pub const ATTR_READY_DETAIL: &str = "ready.detail";

        #[doc = "类型：标签枚举值。"]
        #[doc = "适用范围：指标、日志。"]
        #[doc = ""]
        #[doc = "Outcome 成功枚举值。"]
        pub const OUTCOME_SUCCESS: &str = "success";

        #[doc = "类型：标签枚举值。"]
        #[doc = "适用范围：指标、日志。"]
        #[doc = ""]
        #[doc = "Outcome 失败枚举值。"]
        pub const OUTCOME_ERROR: &str = "error";

        #[doc = "类型：标签枚举值。"]
        #[doc = "适用范围：指标、日志。"]
        #[doc = ""]
        #[doc = "ReadyState：完全就绪。"]
        pub const READY_STATE_READY: &str = "ready";

        #[doc = "类型：标签枚举值。"]
        #[doc = "适用范围：指标、日志。"]
        #[doc = ""]
        #[doc = "ReadyState：繁忙状态。"]
        pub const READY_STATE_BUSY: &str = "busy";

        #[doc = "类型：标签枚举值。"]
        #[doc = "适用范围：指标、日志。"]
        #[doc = ""]
        #[doc = "ReadyState：预算耗尽。"]
        pub const READY_STATE_BUDGET_EXHAUSTED: &str = "budget_exhausted";

        #[doc = "类型：标签枚举值。"]
        #[doc = "适用范围：指标、日志。"]
        #[doc = ""]
        #[doc = "ReadyState：处于 RetryAfter 冷却期。"]
        pub const READY_STATE_RETRY_AFTER: &str = "retry_after";

        #[doc = "类型：标签枚举值。"]
        #[doc = "适用范围：指标、日志。"]
        #[doc = ""]
        #[doc = "Ready detail 占位符，避免缺失标签导致基数膨胀。"]
        pub const READY_DETAIL_PLACEHOLDER: &str = "_";

        #[doc = "类型：标签枚举值。"]
        #[doc = "适用范围：指标、日志。"]
        #[doc = ""]
        #[doc = "Ready detail：上游繁忙。"]
        pub const READY_DETAIL_UPSTREAM: &str = "upstream";

        #[doc = "类型：标签枚举值。"]
        #[doc = "适用范围：指标、日志。"]
        #[doc = ""]
        #[doc = "Ready detail：下游繁忙。"]
        pub const READY_DETAIL_DOWNSTREAM: &str = "downstream";

        #[doc = "类型：标签枚举值。"]
        #[doc = "适用范围：指标、日志。"]
        #[doc = ""]
        #[doc = "Ready detail：内部队列溢出。"]
        pub const READY_DETAIL_QUEUE_FULL: &str = "queue_full";

        #[doc = "类型：标签枚举值。"]
        #[doc = "适用范围：指标、日志。"]
        #[doc = ""]
        #[doc = "Ready detail：自定义繁忙原因。"]
        pub const READY_DETAIL_CUSTOM: &str = "custom";

        #[doc = "类型：标签枚举值。"]
        #[doc = "适用范围：指标、日志。"]
        #[doc = ""]
        #[doc = "Ready detail：RetryAfter 相对等待。"]
        pub const READY_DETAIL_RETRY_AFTER: &str = "after";
    }

    /// Transport 域指标键
    ///
    /// 传输层连接与字节统计使用的标签与枚举。
    pub mod transport {
        #[doc = "类型：指标/日志键。"]
        #[doc = "适用范围：指标、日志。"]
        #[doc = ""]
        #[doc = "传输协议：tcp/quic/uds 等。"]
        pub const ATTR_PROTOCOL: &str = "transport.protocol";

        #[doc = "类型：指标/日志键。"]
        #[doc = "适用范围：指标、日志。"]
        #[doc = ""]
        #[doc = "监听器或连接逻辑标识，保持低基数以利聚合。"]
        pub const ATTR_LISTENER_ID: &str = "listener.id";

        #[doc = "类型：指标/日志键。"]
        #[doc = "适用范围：指标、日志。"]
        #[doc = ""]
        #[doc = "对端角色，通常为 client/server。"]
        pub const ATTR_PEER_ROLE: &str = "peer.role";

        #[doc = "类型：指标/日志键。"]
        #[doc = "适用范围：指标、日志。"]
        #[doc = ""]
        #[doc = "连接尝试结果。"]
        pub const ATTR_RESULT: &str = "result";

        #[doc = "类型：指标/日志键。"]
        #[doc = "适用范围：指标、日志。"]
        #[doc = ""]
        #[doc = "传输层错误分类。"]
        pub const ATTR_ERROR_KIND: &str = "error.kind";

        #[doc = "类型：指标/日志键。"]
        #[doc = "适用范围：指标、日志。"]
        #[doc = ""]
        #[doc = "Socket 家族：ipv4/ipv6/unix。"]
        pub const ATTR_SOCKET_FAMILY: &str = "socket.family";

        #[doc = "类型：标签枚举值。"]
        #[doc = "适用范围：指标、日志。"]
        #[doc = ""]
        #[doc = "连接结果：成功。"]
        pub const RESULT_SUCCESS: &str = "success";

        #[doc = "类型：标签枚举值。"]
        #[doc = "适用范围：指标、日志。"]
        #[doc = ""]
        #[doc = "连接结果：失败。"]
        pub const RESULT_FAILURE: &str = "failure";

        #[doc = "类型：标签枚举值。"]
        #[doc = "适用范围：指标、日志。"]
        #[doc = ""]
        #[doc = "对端角色：客户端。"]
        pub const ROLE_CLIENT: &str = "client";

        #[doc = "类型：标签枚举值。"]
        #[doc = "适用范围：指标、日志。"]
        #[doc = ""]
        #[doc = "对端角色：服务端。"]
        pub const ROLE_SERVER: &str = "server";
    }
}

/// resource 键名分组（模块标识：resource）
///
/// 该分组由 contracts/observability_keys.toml 自动生成。
pub mod resource {

    /// 核心资源属性键
    ///
    /// 服务、部署与 SDK 元数据的标准资源标签。
    pub mod core {
        #[doc = "类型：指标/日志键。"]
        #[doc = "适用范围：指标、日志、追踪。"]
        #[doc = ""]
        #[doc = "服务逻辑名称，建议与注册中心保持一致。"]
        pub const ATTR_SERVICE_NAME: &str = "service.name";

        #[doc = "类型：指标/日志键。"]
        #[doc = "适用范围：指标、日志、追踪。"]
        #[doc = ""]
        #[doc = "服务命名空间或业务域，便于分环境聚合。"]
        pub const ATTR_SERVICE_NAMESPACE: &str = "service.namespace";

        #[doc = "类型：指标/日志键。"]
        #[doc = "适用范围：指标、日志、追踪。"]
        #[doc = ""]
        #[doc = "实例唯一标识，如 Pod 名称或主机 ID。"]
        pub const ATTR_SERVICE_INSTANCE_ID: &str = "service.instance.id";

        #[doc = "类型：指标/日志键。"]
        #[doc = "适用范围：指标、日志、追踪。"]
        #[doc = ""]
        #[doc = "部署环境标签，例如 production/staging。"]
        pub const ATTR_DEPLOYMENT_ENVIRONMENT: &str = "deployment.environment";

        #[doc = "类型：指标/日志键。"]
        #[doc = "适用范围：指标、日志、追踪。"]
        #[doc = ""]
        #[doc = "可观测性 SDK 名称，默认为 spark-otel。"]
        pub const ATTR_TELEMETRY_SDK_NAME: &str = "telemetry.sdk.name";

        #[doc = "类型：指标/日志键。"]
        #[doc = "适用范围：指标、日志、追踪。"]
        #[doc = ""]
        #[doc = "可观测性 SDK 版本号，与 Cargo 包版本对齐。"]
        pub const ATTR_TELEMETRY_SDK_VERSION: &str = "telemetry.sdk.version";

        #[doc = "类型：指标/日志键。"]
        #[doc = "适用范围：指标、日志、追踪。"]
        #[doc = ""]
        #[doc = "自动化安装层版本（若使用自动注入）。"]
        pub const ATTR_TELEMETRY_AUTO_VERSION: &str = "telemetry.auto.version";
    }
}

/// tracing 键名分组（模块标识：tracing）
///
/// 该分组由 contracts/observability_keys.toml 自动生成。
pub mod tracing {

    /// Pipeline Trace 属性
    ///
    /// OpenTelemetry Handler Span 使用的标签及枚举。
    pub mod pipeline {
        #[doc = "类型：追踪字段。"]
        #[doc = "适用范围：追踪。"]
        #[doc = ""]
        #[doc = "Pipeline Handler 方向标签。"]
        pub const ATTR_DIRECTION: &str = "spark.pipeline.direction";

        #[doc = "类型：追踪字段。"]
        #[doc = "适用范围：追踪。"]
        #[doc = ""]
        #[doc = "Handler 注册时的 Label。"]
        pub const ATTR_LABEL: &str = "spark.pipeline.label";

        #[doc = "类型：追踪字段。"]
        #[doc = "适用范围：追踪。"]
        #[doc = ""]
        #[doc = "Handler 所属组件（Descriptor name）。"]
        pub const ATTR_COMPONENT: &str = "spark.pipeline.component";

        #[doc = "类型：追踪字段。"]
        #[doc = "适用范围：追踪。"]
        #[doc = ""]
        #[doc = "Handler 分类。"]
        pub const ATTR_CATEGORY: &str = "spark.pipeline.category";

        #[doc = "类型：追踪字段。"]
        #[doc = "适用范围：追踪。"]
        #[doc = ""]
        #[doc = "Handler 文本摘要。"]
        pub const ATTR_SUMMARY: &str = "spark.pipeline.summary";

        #[doc = "类型：标签枚举值。"]
        #[doc = "适用范围：追踪。"]
        #[doc = ""]
        #[doc = "Handler 方向：入站。"]
        pub const DIRECTION_INBOUND: &str = "inbound";

        #[doc = "类型：标签枚举值。"]
        #[doc = "适用范围：追踪。"]
        #[doc = ""]
        #[doc = "Handler 方向：出站。"]
        pub const DIRECTION_OUTBOUND: &str = "outbound";

        #[doc = "类型：标签枚举值。"]
        #[doc = "适用范围：追踪。"]
        #[doc = ""]
        #[doc = "Handler 方向：未指定（兜底）。"]
        pub const DIRECTION_UNSPECIFIED: &str = "unspecified";
    }
}
