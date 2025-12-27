<!-- @generated 自动生成，请勿手工编辑 -->
# 可观测性键名契约（Observability Keys Contract）

> 目标：指标/日志/追踪键名统一管理，避免代码与仪表盘漂移。
> 来源：`contracts/observability_keys.toml`（单一事实来源）。
> 产物：生成 `crates/spark-core/src/observability/keys.rs` 与本文档。

## 阅读指引

- **键类型**：区分指标/日志键、标签枚举值、日志字段与追踪字段；
- **适用范围**：标记该键应用于指标、日志、追踪或运维事件；
- **更新流程**：修改合约后同步运行生成器，并提交代码与文档。

## attributes.error — 错误观测属性键

> 跨指标/日志/追踪复用的错误标签集合，匹配 CoreError 契约。

| 常量 | 类型 | 适用范围 | 键名/取值 | 说明 |
| --- | --- | --- | --- | --- |
| `ATTR_BUDGET_KIND` | 指标/日志键 | 指标、日志 | `error.budget.kind` | 资源耗尽类错误关联的预算类型。 |
| `ATTR_BUSY_DIRECTION` | 指标/日志键 | 指标、日志 | `error.busy.direction` | 重试类错误导致的繁忙方向（上游/下游）。 |
| `ATTR_CATEGORY` | 指标/日志键 | 指标、日志、追踪 | `error.category` | 错误分类名称，与 ErrorCategory 变体一一对应。 |
| `ATTR_CODE` | 指标/日志键 | 指标、日志、追踪 | `error.code` | 稳定错误码，遵循 <域>.<语义> 约定。 |
| `ATTR_KIND` | 指标/日志键 | 指标、日志、追踪 | `error.kind` | 稳定错误类别标签，用于聚合错误趋势。 |
| `ATTR_RETRYABLE` | 指标/日志键 | 指标、日志、追踪 | `error.retryable` | 布尔值，指示错误是否建议重试。 |
| `ATTR_RETRY_REASON` | 指标/日志键 | 日志 | `error.retry.reason` | Retryable 错误附带的退避原因描述。 |
| `ATTR_RETRY_WAIT_MS` | 指标/日志键 | 指标、日志 | `error.retry.wait_ms` | Retryable 错误建议等待的毫秒数。 |
| `ATTR_SECURITY_CLASS` | 指标/日志键 | 指标、日志、追踪 | `error.security.class` | 安全相关错误所属的安全分类（authentication/authorization 等）。 |

## labels.error_kind — 错误类别标签值

> ErrorCategory 到 error.kind 的标准枚举值。

| 常量 | 类型 | 适用范围 | 键名/取值 | 说明 |
| --- | --- | --- | --- | --- |
| `CANCELLED` | 标签枚举值 | 指标、日志、追踪 | `cancelled` | 调用被主动取消。 |
| `NON_RETRYABLE` | 标签枚举值 | 指标、日志、追踪 | `non_retryable` | 不可重试错误。 |
| `PROTOCOL_VIOLATION` | 标签枚举值 | 指标、日志、追踪 | `protocol_violation` | 协议契约被破坏。 |
| `RESOURCE_EXHAUSTED` | 标签枚举值 | 指标、日志、追踪 | `resource_exhausted` | 资源或预算耗尽。 |
| `RETRYABLE` | 标签枚举值 | 指标、日志、追踪 | `retryable` | 可重试错误。 |
| `SECURITY` | 标签枚举值 | 指标、日志、追踪 | `security` | 安全违规或合规相关错误。 |
| `TIMEOUT` | 标签枚举值 | 指标、日志、追踪 | `timeout` | 调用超时。 |
| `UNKNOWN` | 标签枚举值 | 指标、日志、追踪 | `unknown` | 未归类错误的兜底标签。 |

## logging.audit — 审计事件日志字段

> AuditEvent 序列化到日志或运维事件时复用的字段，保持合规链条可追溯。

| 常量 | 类型 | 适用范围 | 键名/取值 | 说明 |
| --- | --- | --- | --- | --- |
| `FIELD_ACTION` | 日志字段 | 日志、运维事件 | `audit.action` | 执行的动作名称，例如 apply_changeset。 |
| `FIELD_ACTOR_ID` | 日志字段 | 日志、运维事件 | `audit.actor.id` | 触发事件的操作者标识。 |
| `FIELD_ACTOR_TENANT` | 日志字段 | 日志、运维事件 | `audit.actor.tenant` | 操作者所属租户或逻辑空间，可为空。 |
| `FIELD_CHANGE_COUNT` | 日志字段 | 日志、运维事件 | `audit.change.count` | 变更条目的数量，帮助快速估算影响范围。 |
| `FIELD_ENTITY_ID` | 日志字段 | 日志、运维事件 | `audit.entity.id` | 被操作实体的唯一标识。 |
| `FIELD_ENTITY_KIND` | 日志字段 | 日志、运维事件 | `audit.entity.kind` | 被操作实体的类型，例如 configuration.profile。 |
| `FIELD_EVENT_ID` | 日志字段 | 日志、运维事件 | `audit.event.id` | 审计事件的全局唯一标识。 |
| `FIELD_OCCURRED_AT` | 日志字段 | 日志、运维事件 | `audit.occurred_at` | 事件发生时间，Unix 毫秒。 |
| `FIELD_SEQUENCE` | 日志字段 | 日志、运维事件 | `audit.sequence` | 事件在同一实体上的顺序号，便于检测缺失。 |
| `FIELD_STATE_CURR_HASH` | 日志字段 | 日志、运维事件 | `audit.state.curr_hash` | 变更后状态的 SHA-256 哈希。 |
| `FIELD_STATE_PREV_HASH` | 日志字段 | 日志、运维事件 | `audit.state.prev_hash` | 变更前状态的 SHA-256 哈希。 |
| `FIELD_TSA_CHAIN` | 日志字段 | 日志、运维事件 | `audit.tsa.chain` | 可信时间戳证书链标识，用于合规审计。 |

## logging.deprecation — 弃用公告日志字段

> 统一的弃用告警日志字段集合，便于告警平台解析。

| 常量 | 类型 | 适用范围 | 键名/取值 | 说明 |
| --- | --- | --- | --- | --- |
| `FIELD_MIGRATION` | 日志字段 | 日志、运维事件 | `deprecation.migration` | 迁移或替代方案提示。 |
| `FIELD_REMOVAL` | 日志字段 | 日志、运维事件 | `deprecation.removal` | 计划移除的版本或时间。 |
| `FIELD_SINCE` | 日志字段 | 日志、运维事件 | `deprecation.since` | 自哪个版本开始弃用。 |
| `FIELD_SYMBOL` | 日志字段 | 日志、运维事件 | `deprecation.symbol` | 被弃用的符号或能力标识。 |
| `FIELD_TRACKING` | 日志字段 | 日志、运维事件 | `deprecation.tracking` | 追踪链接或工单地址。 |

## logging.shutdown — 关机流程日志字段

> Host Shutdown 生命周期的结构化日志字段。

| 常量 | 类型 | 适用范围 | 键名/取值 | 说明 |
| --- | --- | --- | --- | --- |
| `FIELD_DEADLINE_MS` | 日志字段 | 日志、运维事件 | `shutdown.deadline.ms` | 关机截止时间与当前时刻的剩余毫秒数。 |
| `FIELD_ELAPSED_MS` | 日志字段 | 日志、运维事件 | `shutdown.elapsed.ms` | 关机耗时，毫秒。 |
| `FIELD_ERROR_CODE` | 日志字段 | 日志、运维事件 | `shutdown.error.code` | 关机过程出现的错误码。 |
| `FIELD_REASON_CODE` | 日志字段 | 日志、运维事件 | `shutdown.reason.code` | 关机原因代码，通常来源于治理策略。 |
| `FIELD_REASON_MESSAGE` | 日志字段 | 日志、运维事件 | `shutdown.reason.message` | 关机原因文本描述。 |
| `FIELD_TARGET_COUNT` | 日志字段 | 日志、运维事件 | `shutdown.target.count` | 计划关机的目标数量。 |
| `FIELD_TARGET_LABEL` | 日志字段 | 日志、运维事件 | `shutdown.target.label` | 被关机目标的逻辑标识。 |

## metrics.codec — Codec 域指标键

> 编解码链路的标签与枚举，辅助区分 encode/decode、错误类型与内容类型。

| 常量 | 类型 | 适用范围 | 键名/取值 | 说明 |
| --- | --- | --- | --- | --- |
| `ATTR_CODEC_NAME` | 指标/日志键 | 指标、日志 | `codec.name` | 编解码器名称，通常与协议/格式实现绑定。 |
| `ATTR_CONTENT_TYPE` | 指标/日志键 | 指标、日志 | `content.type` | 内容类型或媒体类型，例如 application/grpc。 |
| `ATTR_ERROR_KIND` | 指标/日志键 | 指标、日志 | `error.kind` | 编解码阶段的错误分类。 |
| `ATTR_MODE` | 指标/日志键 | 指标 | `codec.mode` | 编码/解码模式标签。 |
| `MODE_DECODE` | 标签枚举值 | 指标 | `decode` | Codec 模式：解码。 |
| `MODE_ENCODE` | 标签枚举值 | 指标 | `encode` | Codec 模式：编码。 |

## metrics.hot_reload — 热更新指标键

> 运行时热更新流程的标签集合。

| 常量 | 类型 | 适用范围 | 键名/取值 | 说明 |
| --- | --- | --- | --- | --- |
| `ATTR_COMPONENT` | 指标/日志键 | 指标、日志 | `hot_reload.component` | 热更新组件名称，如 limits/timeouts。 |

## metrics.limits — 限流/限额指标键

> 资源限额治理相关的标签集合。

| 常量 | 类型 | 适用范围 | 键名/取值 | 说明 |
| --- | --- | --- | --- | --- |
| `ATTR_ACTION` | 指标/日志键 | 指标、日志 | `limit.action` | 策略动作标签，如 queue/drop/degrade。 |
| `ATTR_RESOURCE` | 指标/日志键 | 指标、日志 | `limit.resource` | 资源类型标签，例如并发/队列。 |

## metrics.pipeline — Pipeline 域指标/日志键

> Pipeline 纪元、控制器与变更事件的统一标签。

| 常量 | 类型 | 适用范围 | 键名/取值 | 说明 |
| --- | --- | --- | --- | --- |
| `ATTR_CONTROLLER` | 指标/日志键 | 指标、日志 | `pipeline.controller` | Pipeline 控制器实现标识。 |
| `ATTR_EPOCH` | 指标/日志键 | 指标、日志 | `pipeline.epoch` | Pipeline 逻辑纪元。 |
| `ATTR_MUTATION_OP` | 指标/日志键 | 指标、日志 | `pipeline.mutation.op` | 变更操作类型。 |
| `ATTR_PIPELINE_ID` | 指标/日志键 | 指标、日志 | `pipeline.id` | Pipeline 唯一标识，通常映射到 Channel ID。 |
| `CONTROLLER_HOT_SWAP` | 标签枚举值 | 指标、日志 | `hot_swap` | 控制器枚举：HotSwap 控制器实现。 |
| `OP_ADD` | 标签枚举值 | 指标、日志 | `add` | 变更操作：新增 Handler。 |
| `OP_REMOVE` | 标签枚举值 | 指标、日志 | `remove` | 变更操作：移除 Handler。 |
| `OP_REPLACE` | 标签枚举值 | 指标、日志 | `replace` | 变更操作：替换 Handler。 |

## metrics.service — Service 域指标/日志键

> 服务调用面指标与结构化日志共享的标签与标签值，覆盖 ReadyState、Outcome 等核心维度。

| 常量 | 类型 | 适用范围 | 键名/取值 | 说明 |
| --- | --- | --- | --- | --- |
| `ATTR_ERROR_KIND` | 指标/日志键 | 指标、日志 | `error.kind` | 稳定错误分类（如 timeout/internal/security），需与错误分类矩阵保持一致。 |
| `ATTR_OPERATION` | 指标/日志键 | 指标、日志 | `operation` | 操作/方法名，统一为低基数字符串，如 grpc 方法或 RPC 名称。 |
| `ATTR_OUTCOME` | 指标/日志键 | 指标、日志 | `outcome` | 调用总体结果，限制在 success/error 等少量枚举内。 |
| `ATTR_PEER_IDENTITY` | 指标/日志键 | 指标、日志 | `peer.identity` | 对端身份标签，例如 upstream/downstream 或租户别名，需保持低基数。 |
| `ATTR_PROTOCOL` | 指标/日志键 | 指标、日志、追踪 | `protocol` | 入站协议标识，常见值如 grpc/http/quic。 |
| `ATTR_READY_DETAIL` | 指标/日志键 | 指标、日志 | `ready.detail` | ReadyState 细分详情，承载更具体的背压原因。 |
| `ATTR_READY_STATE` | 指标/日志键 | 指标、日志 | `ready.state` | ReadyState 主枚举值，用于关联治理 ReadyState 仪表盘。 |
| `ATTR_ROUTE_ID` | 指标/日志键 | 指标、日志 | `route.id` | 路由或逻辑分组标识，例如 API Path、租户 ID 映射。 |
| `ATTR_SERVICE_NAME` | 指标/日志键 | 指标、日志 | `service.name` | 业务服务名，建议与服务注册中心或配置中的逻辑名称保持一致。 |
| `ATTR_STATUS_CODE` | 指标/日志键 | 指标、日志 | `status.code` | HTTP/gRPC 等响应码或业务状态码。 |
| `OUTCOME_ERROR` | 标签枚举值 | 指标、日志 | `error` | Outcome 失败枚举值。 |
| `OUTCOME_SUCCESS` | 标签枚举值 | 指标、日志 | `success` | Outcome 成功枚举值。 |
| `READY_DETAIL_CUSTOM` | 标签枚举值 | 指标、日志 | `custom` | Ready detail：自定义繁忙原因。 |
| `READY_DETAIL_DOWNSTREAM` | 标签枚举值 | 指标、日志 | `downstream` | Ready detail：下游繁忙。 |
| `READY_DETAIL_PLACEHOLDER` | 标签枚举值 | 指标、日志 | `_` | Ready detail 占位符，避免缺失标签导致基数膨胀。 |
| `READY_DETAIL_QUEUE_FULL` | 标签枚举值 | 指标、日志 | `queue_full` | Ready detail：内部队列溢出。 |
| `READY_DETAIL_RETRY_AFTER` | 标签枚举值 | 指标、日志 | `after` | Ready detail：RetryAfter 相对等待。 |
| `READY_DETAIL_UPSTREAM` | 标签枚举值 | 指标、日志 | `upstream` | Ready detail：上游繁忙。 |
| `READY_STATE_BUDGET_EXHAUSTED` | 标签枚举值 | 指标、日志 | `budget_exhausted` | ReadyState：预算耗尽。 |
| `READY_STATE_BUSY` | 标签枚举值 | 指标、日志 | `busy` | ReadyState：繁忙状态。 |
| `READY_STATE_READY` | 标签枚举值 | 指标、日志 | `ready` | ReadyState：完全就绪。 |
| `READY_STATE_RETRY_AFTER` | 标签枚举值 | 指标、日志 | `retry_after` | ReadyState：处于 RetryAfter 冷却期。 |

## metrics.transport — Transport 域指标键

> 传输层连接与字节统计使用的标签与枚举。

| 常量 | 类型 | 适用范围 | 键名/取值 | 说明 |
| --- | --- | --- | --- | --- |
| `ATTR_ERROR_KIND` | 指标/日志键 | 指标、日志 | `error.kind` | 传输层错误分类。 |
| `ATTR_LISTENER_ID` | 指标/日志键 | 指标、日志 | `listener.id` | 监听器或连接逻辑标识，保持低基数以利聚合。 |
| `ATTR_PEER_ROLE` | 指标/日志键 | 指标、日志 | `peer.role` | 对端角色，通常为 client/server。 |
| `ATTR_PROTOCOL` | 指标/日志键 | 指标、日志 | `transport.protocol` | 传输协议：tcp/quic/uds 等。 |
| `ATTR_RESULT` | 指标/日志键 | 指标、日志 | `result` | 连接尝试结果。 |
| `ATTR_SOCKET_FAMILY` | 指标/日志键 | 指标、日志 | `socket.family` | Socket 家族：ipv4/ipv6/unix。 |
| `RESULT_FAILURE` | 标签枚举值 | 指标、日志 | `failure` | 连接结果：失败。 |
| `RESULT_SUCCESS` | 标签枚举值 | 指标、日志 | `success` | 连接结果：成功。 |
| `ROLE_CLIENT` | 标签枚举值 | 指标、日志 | `client` | 对端角色：客户端。 |
| `ROLE_SERVER` | 标签枚举值 | 指标、日志 | `server` | 对端角色：服务端。 |

## resource.core — 核心资源属性键

> 服务、部署与 SDK 元数据的标准资源标签。

| 常量 | 类型 | 适用范围 | 键名/取值 | 说明 |
| --- | --- | --- | --- | --- |
| `ATTR_DEPLOYMENT_ENVIRONMENT` | 指标/日志键 | 指标、日志、追踪 | `deployment.environment` | 部署环境标签，例如 production/staging。 |
| `ATTR_SERVICE_INSTANCE_ID` | 指标/日志键 | 指标、日志、追踪 | `service.instance.id` | 实例唯一标识，如 Pod 名称或主机 ID。 |
| `ATTR_SERVICE_NAME` | 指标/日志键 | 指标、日志、追踪 | `service.name` | 服务逻辑名称，建议与注册中心保持一致。 |
| `ATTR_SERVICE_NAMESPACE` | 指标/日志键 | 指标、日志、追踪 | `service.namespace` | 服务命名空间或业务域，便于分环境聚合。 |
| `ATTR_TELEMETRY_AUTO_VERSION` | 指标/日志键 | 指标、日志、追踪 | `telemetry.auto.version` | 自动化安装层版本（若使用自动注入）。 |
| `ATTR_TELEMETRY_SDK_NAME` | 指标/日志键 | 指标、日志、追踪 | `telemetry.sdk.name` | 可观测性 SDK 名称，默认为 spark-otel。 |
| `ATTR_TELEMETRY_SDK_VERSION` | 指标/日志键 | 指标、日志、追踪 | `telemetry.sdk.version` | 可观测性 SDK 版本号，与 Cargo 包版本对齐。 |

## tracing.pipeline — Pipeline Trace 属性

> OpenTelemetry Handler Span 使用的标签及枚举。

| 常量 | 类型 | 适用范围 | 键名/取值 | 说明 |
| --- | --- | --- | --- | --- |
| `ATTR_CATEGORY` | 追踪字段 | 追踪 | `spark.pipeline.category` | Handler 分类。 |
| `ATTR_COMPONENT` | 追踪字段 | 追踪 | `spark.pipeline.component` | Handler 所属组件（Descriptor name）。 |
| `ATTR_DIRECTION` | 追踪字段 | 追踪 | `spark.pipeline.direction` | Pipeline Handler 方向标签。 |
| `ATTR_LABEL` | 追踪字段 | 追踪 | `spark.pipeline.label` | Handler 注册时的 Label。 |
| `ATTR_SUMMARY` | 追踪字段 | 追踪 | `spark.pipeline.summary` | Handler 文本摘要。 |
| `DIRECTION_INBOUND` | 标签枚举值 | 追踪 | `inbound` | Handler 方向：入站。 |
| `DIRECTION_OUTBOUND` | 标签枚举值 | 追踪 | `outbound` | Handler 方向：出站。 |
| `DIRECTION_UNSPECIFIED` | 标签枚举值 | 追踪 | `unspecified` | Handler 方向：未指定（兜底）。 |

