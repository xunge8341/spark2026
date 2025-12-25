# spark-core 错误域分层指南

> **版本说明**：本文档描述的错误分层结构自 `spark-core` vNEXT 起生效，所有实现需遵循 `CoreError ← DomainError ← ImplError` 的映射链路。

## 1. 三层错误结构概览

| 层级 | 类型 | 作用 | `source()` 语义 | 常见消费方 |
| --- | --- | --- | --- | --- |
| 核心层 | [`CoreError`](../crates/spark-core/src/error.rs) | 对外暴露的稳定错误域，携带稳定错误码与跨组件可观察上下文。 | 指向域层或实现层的原因（若存在）。 | 应用路由、Pipeline、运行时任务调度。 |
| 域层 | [`DomainError`](../crates/spark-core/src/error.rs) | 以业务域划分语义，并负责向核心层汇总消息、错误码。 | 指向实现层错误。 | 集群、路由、传输等领域契约。 |
| 实现层 | [`ImplError`](../crates/spark-core/src/error.rs) | 记录最细粒度的实现细节、底层原因与调试信息。 | 指向更底层的 `ErrorCause`。 | 具体驱动、协议适配器、缓冲池实现。 |

### 1.1 映射接口

- `ImplError::into_domain_error(kind, code)`：实现层显式选择域分类与核心错误码，返回 `DomainError`。拒绝直接跳级，以免混淆语义。
- `DomainError::into_core_error()`：域层将自身汇总为核心错误，自动保留 `message`、`code` 与整条 `source()` 链。

## 2. 错误枚举清单

### 2.1 域层分类 `DomainErrorKind`

| 枚举值 | 说明 | 推荐核心错误码前缀 |
| --- | --- | --- |
| `Transport` | 传输与底层 I/O。 | `transport.*` |
| `Protocol` | 协议编解码及协商。 | `protocol.*` |
| `Runtime` | 运行时调度、任务生命周期。 | `runtime.*` |
| `Cluster` | 集群、拓扑与一致性控制面。 | `cluster.*` |
| `Discovery` | 服务发现与配置中心。 | `discovery.*` |
| `Router` | 路由元数据、版本控制。 | `router.*` |
| `Application` | 应用或业务回调逻辑。 | `app.*` |
| `Buffer` | 缓冲池与内存租借。 | `buffer.*` 或 `protocol.budget_exceeded` |

### 2.2 实现层分类 `ImplErrorKind`

| 枚举值 | 说明 | 典型来源 |
| --- | --- | --- |
| `BufferExhausted` | 缓冲池或租借器耗尽。 | 内存池、零拷贝缓冲实现。 |
| `CodecRegistry` | 编解码注册、动态派发失败。 | `CodecRegistry`、多态工厂。 |
| `Io` | 底层 I/O 或系统调用失败。 | 文件、网络、设备驱动。 |
| `Timeout` | 操作超时或被取消。 | 传输通道、等待句柄。 |
| `StateViolation` | 状态机或协程违反契约。 | Pipeline、调度器。 |
| `Uncategorized` | 尚未归档的实现细节。 | 临时占位，需在评审中细化。 |

## 3. 核心错误码列表

`CoreError` 的 `code` 字段必须引用 [`codes`](../crates/spark-core/src/error.rs) 模块中的常量或遵循 `<域>.<语义>` 约定。常用码值如下：

- 传输：`transport.io`、`transport.timeout`
- 协议：`protocol.decode`、`protocol.negotiation`、`protocol.budget_exceeded`、`protocol.type_mismatch`
- 运行时：`runtime.shutdown`
- 集群：`cluster.node_unavailable`、`cluster.service_not_found`、`cluster.network_partition`、`cluster.leader_lost`、`cluster.queue_overflow`
- 服务发现：`discovery.stale_read`
- 路由：`router.version_conflict`
- 应用：`app.routing_failed`、`app.unauthorized`、`app.backpressure_applied`

## 4. Round-trip 契约测试摘要

参考 [`impl_to_domain_to_core_roundtrip_preserves_message_and_cause`](../crates/spark-core/src/error.rs) 单元测试：

1. 构造带有 `ImplErrorKind::BufferExhausted` 的实现层错误，并附带更底层的 `CoreError` 原因。
2. 使用 `IntoDomainError` 映射为 `DomainErrorKind::Buffer`，指定核心错误码 `protocol.budget_exceeded`。
3. 将域层错误转换为 `CoreError`，验证：
   - 错误码与消息保持一致；
   - `source()` 链依次指向实现层错误与底层原因。

通过该测试即可证明跨层映射不会丢失消息或上下文字段。

## 5. 落地建议

- **实现层**：务必在返回错误前补充详细说明（`with_detail`）与底层 `cause`，减少排障成本。
- **域层**：集中管理错误码映射，避免重复拼写字符串，可在领域模块内提供专用构造函数。
- **调用方**：若需直接返回核心错误，请先确认是否有域层枚举可复用，以保持错误语义清晰。

如需更多实践示例，请关注后续在 `docs/` 目录发布的专题指南。
