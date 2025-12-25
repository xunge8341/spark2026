# spark-codec-line

## 职责边界
- 实现基于换行符的文本编解码器，为演示自定义协议如何遵循 `spark-core::codec::Codec` 契约。
- 展示 `CallContext` 预算、取消与半关闭在纯文本场景下的处理方式，为其他编解码器提供模板。
- 在 [`docs/getting-started.md`](../../../docs/getting-started.md) 的示例中作为最小可运行扩展，帮助新成员验证开发环境。

## 公共接口入口
- [`src/lib.rs`](./src/lib.rs)：注册 `LineDelimitedCodec` 并暴露最小依赖面；同时导出兼容层入口。
- [`src/line.rs`](./src/line.rs)：包含核心状态机、错误映射与编码/解码实现，演示如何处理预算耗尽与半关闭。
- [`src/compat`](./src/compat)：提供旧版协议适配示例，展示如何在不破坏主实现的情况下扩展行为。

## 状态机与错误域
- 状态机围绕 `DecodeOutcome::{Complete, Incomplete, Invalid}`，其转移需符合 [`docs/state_machines.md`](../../../docs/state_machines.md) 对 ReadyState 的映射：
  - `Incomplete` → `ReadyState::Pending`
  - 帧超限 → `ReadyState::BudgetExhausted`
  - 格式错误 → `ReadyState::Busy(BusyReason::protocol)`
- 错误分类使用 `error::codes::PROTOCOL_*` 系列常量，并遵循 [`docs/error-category-matrix.md`](../../../docs/error-category-matrix.md) 的协议错误定义。

## 关联契约与测试
- 对应的契约验证示例位于 [`examples`](./examples) 目录，并结合 [`crates/spark-contract-tests`](../../spark-contract-tests) 扩展覆盖真实场景。
- ReadyState 映射在 [`docs/buffer-zerocopy-contract.md`](../../../docs/buffer-zerocopy-contract.md) 有详细说明，特别是缓冲租借与释放顺序。
- 与 [`crates/spark-codec-sip`](../spark-codec-sip) 共享公共测试基类，确保不同协议间的错误分类保持一致。

## 集成注意事项
- `encode`/`decode` 会检查 `CallContext::budget(BudgetKind::Codec)`，当预算耗尽时返回 `CoreError`，调用方需据此传播 `ReadyState::BudgetExhausted`。
- 接收到半关闭信号后必须停止继续读取缓冲；实现通过状态机确认 FIN 后丢弃未完成帧，符合 [`docs/graceful-shutdown-contract.md`](../../../docs/graceful-shutdown-contract.md)。
- 若需要支持不同换行约定，可扩展 `state.rs` 中的解析策略，但务必在 README 与文档中记录新增的协商参数。
