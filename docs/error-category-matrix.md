<!-- @generated 自动生成，请勿手工编辑 -->
# 错误分类矩阵（ErrorCategory Matrix）

> 目标：错误契约机器可读，默认异常处理器可据此自动触发退避、背压或关闭。
>
> 适用范围：`spark-core` 模块的稳定错误码（参见 `spark_core::error::codes`）。

## 阅读指引

- **来源**：稳定错误码，统一使用 `<域>.<语义>` 格式。
- **分类**：[`ErrorCategory`](../crates/spark-core/src/error.rs) 枚举分支，用于驱动默认策略。
- **单一事实来源**：分类矩阵由 `contracts/error_matrix.toml` 声明，经构建脚本与本工具生成代码与文档。
- **默认动作**：`ExceptionAutoResponder::on_exception_caught` 在无显式覆盖时执行的行为：
  - `Busy`：向上游广播 `ReadyState::Busy`（表示暂时不可用）；
  - `BudgetExhausted`：广播 `ReadyState::BudgetExhausted`，携带预算快照；
  - `RetryAfter`：广播 `ReadyState::RetryAfter`，附带退避建议；
  - `Close`：调用 `Context::close_graceful` 优雅关闭通道；
  - `Cancel`：标记 `CallContext::cancellation()`，终止剩余逻辑；
  - `None`：默认不产生额外动作，由业务自行处理。
- **可配置策略**：通过 `CoreError::with_category` / `set_category` 覆盖分类，或在 pipeline 中注册自定义 Handler。

## 分类明细

| 来源（错误码） | 分类 | 默认动作 | 说明 | 可配置策略 |
| --- | --- | --- | --- | --- |
| `transport.io` | Retryable（150ms，传输层 I/O 故障，等待链路恢复后重试） | RetryAfter + Busy(Downstream) | 典型 TCP/QUIC I/O 故障，建议短暂退避。 | 可根据重试策略调整等待窗口。 |
| `transport.timeout` | Timeout | Cancel | 传输层请求超时，触发调用取消。 | 若需继续等待，可显式改写分类。 |
| `protocol.decode` / `protocol.negotiation` / `protocol.type_mismatch` / `router.version_conflict` | ProtocolViolation | Close | 协议契约被破坏，必须关闭连接。 | 若协议允许纠正，可改写为 Retryable。 |
| `protocol.budget_exceeded` | ResourceExhausted(BudgetKind::Decode) | BudgetExhausted | 解码预算耗尽，触发背压。 | 可在 CallContext 注册定制预算。 |
| `runtime.shutdown` | Cancelled | Cancel | 运行时已进入关闭流程，终止后续逻辑。 | 若需等待收尾，可改写为 Retryable。 |
| `cluster.node_unavailable` / `cluster.network_partition` / `cluster.leader_lost` | Retryable（250ms，集群节点暂不可用，稍后重试） | RetryAfter + Busy(Upstream) | 集群拓扑暂时不可用，建议延后重试。 | 可根据集群健康状况调整等待。 |
| `cluster.service_not_found` / `app.routing_failed` | NonRetryable | None | 目标服务不存在或路由失败，需业务人工干预。 | 在具备兜底副本时可标记为 Retryable。 |
| `cluster.queue_overflow` | ResourceExhausted(BudgetKind::Flow) | BudgetExhausted | 集群队列满，触发速率背压。 | 可扩容队列或自定义 BudgetKind。 |
| `discovery.stale_read` | Retryable（120ms，服务发现数据陈旧，等待刷新） | RetryAfter + Busy(Upstream) | 服务发现数据陈旧，等待刷新后再试。 | 可结合监控加长等待时长。 |
| `app.unauthorized` | Security(SecurityClass::Authorization) | Close | 权限不足，记录安全事件并关闭通道。 | 可通过自定义 Handler 触发补偿。 |
| `app.backpressure_applied` | Retryable（180ms，下游正在施加背压，遵循等待窗口） | RetryAfter + Busy(Downstream) | 业务端主动背压，需遵守等待窗口。 | 可结合速率控制调整等待时间。 |

> **提示**：当新增错误码时，请执行：
> 1. 更新 `contracts/error_matrix.toml`；
> 2. 运行 `cargo run -p spark-core --bin gen_error_doc --features std,error_contract_doc` 生成文档；
> 3. 触发构建脚本（`cargo build -p spark-core`）更新 `src/error/generated/category_matrix.rs` 并补充契约测试。
