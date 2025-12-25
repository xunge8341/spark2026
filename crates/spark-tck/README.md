# spark-tck

## 职责边界
- 在真实传输栈上执行 `spark-core` 契约验证，补齐 `crates/spark-contract-tests` 在纯内存环境下无法覆盖的 IO 行为。
- 通过 `Tokio`/`quinn` 等运行时运行端到端场景，确认 [`docs/transport-handshake-negotiation.md`](../../docs/transport-handshake-negotiation.md) 与 [`docs/graceful-shutdown-contract.md`](../../docs/graceful-shutdown-contract.md) 的要求在真实网络中成立。
- 为 `crates/spark-transport-*` 与外部实现提供参考样例，作为 `make ci-bench-smoke` 的一部分评估性能与行为一致性。

## 公共接口入口
- [`src/lib.rs`](./src/lib.rs)：保留传输契约测试的占位模块，并在 `transport` 测试集中组装半关闭、背压等端到端场景。
- [`src/sip/mod.rs`](./src/sip/mod.rs)：聚焦 SIP INVITE/CANCEL 竞态，当前提供 `cancel_race` 套件覆盖多线程状态机竞争。
- [`src/sdp/mod.rs`](./src/sdp/mod.rs)：承载 SDP Offer/Answer 单元测试，确保协商逻辑在 crate 内即可快速验证。
- [`src/rtcp/mod.rs`](./src/rtcp/mod.rs)：组织 RTCP 解析与统计相关测试专题（如 `parse_compound`、`stats`）。

## 状态机与错误域
- ReadyState 与错误分类判定直接复用 `spark-contract-tests` 的断言实现；若被测对象返回未知状态，会在此 crate 中抛出带上下文的 panic。
- 安全场景需准确映射到 `ErrorCategory::SecurityViolation` 并在 `CloseReason` 中保留原因，参照 [`docs/safety-audit.md`](../../docs/safety-audit.md)。
- 对于背压与预算，测试将比较实际队列/连接池统计与 [`docs/resource-limits.md`](../../docs/resource-limits.md) 中的基准，确保实现遵守资源控制策略。

## 关联契约与测试
- 与 [`crates/spark-contract-tests`](../spark-contract-tests) 紧耦合：此 crate 的每个主题均调用核心 Runner 以共享断言逻辑。
- 默认测试拓扑由各传输实现（如 [`crates/spark-transport-tcp`](../spark-transport-tcp)、[`tls`](../spark-transport-tls)、[`quic`](../spark-transport-quic)、[`udp`](../spark-transport-udp)）提供；若替换传输实现，请同步更新 README 与索引路径。
- 性能冒烟数据会反馈到 [`docs/async-contract-performance.md`](../../docs/async-contract-performance.md)，用于追踪回归。

## 集成注意事项
- 运行测试需要可用的网络端口；CI 中使用本地回环地址，若在受限环境运行需调整 `tests/config.rs` 中的绑定策略。
- TLS/QUIC 套件依赖 `tests/certs` 下的测试证书，运行前请确保生成脚本已执行或证书未过期。
- 新增主题时需在根 README 的索引中登记，并补充相关文档链接以保持上下游同步。
