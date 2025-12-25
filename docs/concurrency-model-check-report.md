# 并发内存/顺序性验证报告（Miri / Loom 抽样）

## 目标与范围
- **目标**：使用 Miri 与 Loom 对核心并发路径进行抽样验证，提前捕获潜在的未定义行为与顺序性问题。
- **抽样组件**：
  1. `Cancellation`（取消信号传播）
  2. `Budget`（额度扣减与归还）
  3. `Channel`（通道关闭状态机的竞态收敛）
- **工具链**：`cargo +nightly miri test -p spark-core --test concurrency_primitives`、`RUSTFLAGS="--cfg spark_loom" cargo test --features loom-model,std --test loom_concurrency`

## Loom 模型检查
- **测试入口**：`crates/spark-core/tests/loom_concurrency.rs`
- **覆盖要点**：
  - **取消信号可见性**：验证 `Cancellation::cancel` 的释放语义与子节点可见性，避免遗漏 `Ordering` 导致的自旋。
  - **额度竞争**：在 `Budget::try_consume` / `refund` 的交错序列下，保证剩余额度不会出现下溢或超限。
  - **通道关闭收敛**：模拟并发触发 `close_graceful` 与 `close` 的场景，确保状态最终落在 `Closed`，且优雅/强制计数各自只增长一次。
- **运行结果**：在探索深度 3～4 的 Loom 默认配置下，所有模型测试均通过，未发现死锁或竞态行为。
- **注意事项**：
  - 若需增加探索深度，可使用 `LOOM_MAX_PREEMPTIONS` 环境变量，但运行时间会显著增加。
  - 模型测试依赖 `loom-model` 特性，并需要 `--cfg spark_loom` 以启用针对 Loom 的类型别名。

## Miri 运行结论
- **命令**：`cargo +nightly miri test -p spark-core --test concurrency_primitives`
- **结果**：取消/预算/通道三类原语的 Miri 抽样测试全部通过，未发现未定义行为。
- **警告**：本轮未观察到额外的编译器警告。

## 发现与修复
- **`Arc` 与互斥实现分离**：
  - 在 `crates/spark-core/src/kernel/contract.rs` 中固定 `Arc` 的具体类型以保留 `Eq/Hash` 派生，避免 Loom 条件编译导致的 trait 派生失败。【F:crates/spark-core/src/kernel/contract.rs†L1-L16】
- **通道竞态守护**：
  - `crates/spark-core/tests/loom_concurrency.rs` 增补通道关闭场景，覆盖优雅/强制关闭的竞态收敛。【F:crates/spark-core/tests/loom_concurrency.rs†L1-L139】
- **Miri 抽样入口统一**：
  - 新增 `crates/spark-core/tests/concurrency_primitives.rs`，集中取消、预算与通道的跨线程场景，供 Miri 聚焦执行。【F:crates/spark-core/tests/concurrency_primitives.rs†L1-L208】

## 后续建议
1. **扩展预算场景**：增加失败重试、组合额度等模型测试，覆盖更多竞争模式。
2. **通道观测加强**：考虑模拟 `closed()` 等等待逻辑的 Future 语义，验证在更复杂调度下的收敛性。
3. **警告清理**：持续监控 Miri 输出，若出现新警告及时修复。

## 运行指引
```bash
# 启动 Loom 模型检查
RUSTFLAGS="--cfg spark_loom" cargo test --features loom-model,std --test loom_concurrency

# 运行 Miri（需 nightly toolchain，并提前安装 miri 与 rust-src）
cargo +nightly miri test -p spark-core --test concurrency_primitives
```
