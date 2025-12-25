# spark-contract-tests

## 职责边界
- 提供传输层与宿主实现的技术合规套件，覆盖 [`docs/global-architecture.md`](../../docs/global-architecture.md) 中约定的 ReadyState、半关闭、预算与错误分类契约。
- 通过主题化入口（`graceful_shutdown`, `backpressure`, `errors`, `security` 等）复用测试脚手架，帮助 `crates/spark-transport-*` 与编解码器快速对齐协议行为。
- 作为发布门槛的一部分，在 CI 中被 `make ci-lints` 与 `make ci-bench-smoke` 调用，确保所有实现遵循统一接口。

## 公共接口入口
- [`src/lib.rs`](./src/lib.rs)：导出 Runner 与主题化测试入口，允许调用方挑选需要的测试集。
- [`src/backpressure.rs`](./src/backpressure.rs)：验证 `ReadyState`、`BudgetDecision` 与 `BusyReason` 的组合，涵盖队列、线程池、订阅预算等场景。
- [`src/graceful_shutdown.rs`](./src/graceful_shutdown.rs)：根据 [`docs/graceful-shutdown-contract.md`](../../docs/graceful-shutdown-contract.md) 检查半关闭流程。
- [`src/errors.rs`](./src/errors.rs)：断言 `ErrorCategory`/`CloseReason` 的映射符合 [`docs/error-category-matrix.md`](../../docs/error-category-matrix.md)。
- [`src/cancellation.rs`](./src/cancellation.rs)：覆盖取消语义与竞态处理，确认为超时、显式取消提供统一断言。

## 状态机与错误域
- 所有 ReadyState 断言以 [`docs/state_machines.md`](../../docs/state_machines.md) 规定的状态转移为基线；若实现发出未知状态，测试会失败并提示差异。
- 错误分类需符合 [`docs/error-hierarchy.md`](../../docs/error-hierarchy.md)，包括 `Timeout`、`SecurityViolation`、`ResourceExhausted` 等关键分支。
- 安全场景与 QUIC/TLS 相关用例参照 [`docs/transport-handshake-negotiation.md`](../../docs/transport-handshake-negotiation.md) 与 [`docs/safety-audit.md`](../../docs/safety-audit.md)。

## 关联契约与测试
- [`crates/spark-tck`](../spark-tck) 在此基础上拓展宿主行为测试（如配置热更新、任务调度），两者需保持接口兼容。
- [`crates/spark-contract-tests-macros`](../spark-contract-tests-macros) 通过编译期宏生成器补充测试数据，减少样板代码。
- 编解码器与传输实现需在自身 CI 中引用本 crate，以满足 `docs/scorecard.md` 中的合规要求。

## 集成注意事项
- Runner 默认假设被测对象实现了 `spark_core::contract::CallContext` 契约；如需自定义上下文，请实现兼容的适配层。
- 传输实现若依赖外部资源（端口、证书），应通过测试配置注入假体，确保在无网络环境下也可运行。
- 新增契约测试时务必同步更新相关文档链接与 README 索引，保证顶层导航信息一致。
