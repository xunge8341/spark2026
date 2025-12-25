# Spark Core “统一契约 15 条” 共识落地说明

> 本文档对专家共识的 15 项统一契约进行编号说明，明确其在 `spark-core` 中的文档约束与 CI 覆盖方式。
>
> - **CI 约束**：所有公共 API 变更需通过 `make ci-*` 系列任务，覆盖 `cargo fmt/clippy/build/doc/bench` 与自定义契约测试，确保契约不被破坏。
> - **文档约束**：本文件与各模块内的教案级注释共同构成可审计的契约文档，任何修改须在 PR 中同步更新。
> - **统一协议红线**：`tools/ci/check_unified_protocol_guard.sh` 会在 CI 早期检查公共协议 API 是否变更；若未同步提交 CEP（`docs/governance/CEP-*.md`），流水线将直接拒绝。

## 1. 双层 API 模型
- **落地位置**：`crates/spark-core/src/data_plane/service.rs`、`buffer/mod.rs` 等提供泛型 `Service`/`Layer` 与对象安全 `DynService`/`ServiceObject`/`BoxService`。
- **CI 约束**：`make ci-lints` 会检查对象安全实现是否编译，`cargo doc` 验证文档示例。

## 2. 取消 / 截止 / 预算统一原语
- **落地位置**：`crates/spark-core/src/kernel/contract.rs` 定义 `Cancellation`、`Deadline`、`Budget`、`CallContext`。
- **CI 约束**：`make ci-no-std-alloc` 确认新原语在 `no_std + alloc` 下可编译。

## 3. 背压语义统一
- **落地位置**：`crates/spark-core/src/kernel/status/ready.rs` 提供唯一的 `ReadyState/ReadyCheck/PollReady` 语义出口，并通过 `BusyReason` 抽象繁忙原因。
- **CI 约束**：`cargo clippy -- -D warnings` 确保所有调用方显式处理统一状态，避免忽略背压信号；`tools/ci/check_consistency.sh` 负责阻止旧的 `Backpressure` 与 `Reason` 拼接关键词再次出现。

## 4. 优雅关闭契约
- **落地位置**：`contract::CloseReason`、`pipeline::channel::Channel::close_graceful/closed`、`Context::close_graceful/closed`、`DynService::graceful_close/closed`。
- **CI 约束**：`cargo doc --workspace` 校验文档注释；`make ci-bench-smoke` 运行 smoke 测试确保接口存在。

## 5. 错误域分层
- **落地位置**：`crates/spark-core/src/error.rs` 定义 `CoreError → SparkError → ImplError` 三层模型。
- **CI 约束**：`make ci-lints` 检查未使用字段；`cargo build` 验证 API 变更。

## 6. 可观测性即契约
- **落地位置**：`contract::ObservabilityContract` 与常量 `DEFAULT_OBSERVABILITY_CONTRACT`，`CallContext` 暴露指标/日志/Trace 键。
- **CI 约束**：`cargo doc` 输出契约文档；`make ci-doc-warning` 确保文档无警告。

## 7. 安全最小面
- **落地位置**：`SecurityContextSnapshot` 记录 `identity/peer/policy`，默认拒绝不安全模式；`CallContext::security()` 提供访问。
- **CI 约束**：`make ci-lints` 强制未使用字段报错，防止实现绕过安全检查。

## 8. no_std / alloc gating
- **落地位置**：`contract.rs` 与新枚举全部使用 `alloc` / `core` 类型；`lib.rs` 维持 `#![cfg_attr(not(feature="std"), no_std)]`。
- **CI 约束**：`make ci-no-std-alloc`、`cargo build --no-default-features --features alloc`。

## 9. 对象安全 / 命名与稳定性
- **落地位置**：所有新枚举 `#[non_exhaustive]`，对象安全接口命名遵循 `Dyn*`；类型通过 `BoxService` 提供稳定封装。
- **CI 约束**：`cargo clippy` 检查枚举是否匹配完整；`cargo doc` 输出 `#[non_exhaustive]` 文档警示。

## 10. 边界同步
- **落地位置**：`Service`、`DynService`、`Context`、`Channel` 等 trait 明确 `Send + Sync + 'static` 要求；`CallContext` 采用 `Arc` 共享实现生命周期同步；`docs/send-sync-static-matrix.md` 提供矩阵化对照，覆盖借用/拥有成对入口。
- **CI 约束**：`make ci-zc-asm` 与 `cargo clippy` 检查线程安全界限。

## 11. 配置 Builder 契约
- **落地位置**：`CallContextBuilder` 提供可验证构造流程；已有 `ConfigurationBuilder` 等保持一致。
- **CI 约束**：`cargo test`（在后续契约测试 crate 中）及 `make ci-lints` 检查未使用错误。

## 12. 协议 / 编解码预算
- **落地位置**：`BudgetKind::Decode/Flow`、`Budget::try_consume()`、`ReadyState::BudgetExhausted`；`CallContext` 默认提供 `Flow` 预算。
- **CI 约束**：`make ci-bench-smoke` 检查基准是否引用预算；`cargo clippy` 防止忽略返回值。

## 13. 资源上限与配额
- **落地位置**：`BudgetSnapshot` 与 `BudgetDecision` 暴露上限/剩余，允许在路由/服务中配置策略。
- **CI 约束**：`make ci-lints` 与 `cargo clippy` 确保消费返回值被使用。

## 14. 兼容演进策略
- **落地位置**：`#[non_exhaustive]` 枚举、`CallContext` Builder 默认值、`DynService` 适配器保持向后兼容；`docs/global-architecture.md` 继续宣告 SemVer 策略。
- **CI 约束**：`cargo doc` + `make ci-doc-warning` 确保文档说明同步。

## 15. 契约测试优先
- **落地位置**：CI 必须执行 `make ci-*`（在 `README` 与主仓库 CI 配置中约定）；未来将把契约测试纳入 `spark-contract-tests`。
- **CI 约束**：本仓库的 `Makefile` 已将契约相关命令纳入 `ci-*` 任务。

---

## 统一协议红线治理

- **覆盖范围**：凡位于 `spark-core` 及兄弟 crate 中以“统一协议”为核心的公共能力（如 `Service`、`DynService`、`Channel`、`CallContext`、`Cluster Discovery` 等）被新增、重命名或删除 `pub` 接口，均被视为触碰统一协议红线。
- **流程要求**：
  - 任何公共 API 调整必须提交 CEP，记录兼容性影响、迁移计划与 Owner 审批；CI 会通过 `tools/ci/check_unified_protocol_guard.sh` 强制校验。
  - 如需紧急豁免，必须设置 `UNIFIED_PROTOCOL_OVERRIDE=allow` 并在 PR 模板的“触碰统一协议公共 API”核对框中勾选、说明原因，且事后补写 CEP。
- **配套清单**：PR 模板新增统一协议核对框；若未勾选即表示明确声明“不触碰统一协议红线”。一旦后续审计发现偏离，该声明将作为回溯依据。

> **审计提示**：若公共 API 出现新增/变更，需同步更新本文件对应条目以及相关模块的教案级注释；CI 将通过 `make ci-doc-warning` 与统一协议守卫脚本双重检查，防止遗漏文档更新与治理备案。
