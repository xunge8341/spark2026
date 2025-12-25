# 安全审计报告

## 概览
- `spark-core`: 通过 `#![deny(unsafe_code)]` 全局禁止 `unsafe`，当前无白名单。
- 其他 crates: 仅在契约测试中存在必要的 `unsafe`，详见下文。

## 详细白名单

### crates/spark-core/src/data_plane/pipeline/pipeline.rs
- **状态**：`HotSwapPipeline` 采用 `Arc<HotSwapPipeline>` 与 `Weak` 的组合管理上下文，不再需要任何 `unsafe` 块或 `#![allow(unsafe_code)]` 豁免。【F:crates/spark-core/src/data_plane/pipeline/pipeline.rs†L1-L121】【F:crates/spark-core/src/data_plane/pipeline/pipeline.rs†L523-L708】
- **安全性说明**：
  - 上下文通过 `Arc` 捕获控制器所有权，`Weak` 在构造阶段自动升级，避免悬垂指针；
  - 并发访问依赖 `ArcSwap`、原子计数与 `Mutex` 管理 Handler 注册表，遵循标准线程安全原语；
  - 热插拔流程使用句柄与 `ArcSwap` 原子替换，所有对控制器的访问均经过 `Arc`/`Weak` 验证，无需裸指针操作。【F:crates/spark-core/src/data_plane/pipeline/pipeline.rs†L122-L451】【F:crates/spark-core/src/data_plane/pipeline/pipeline.rs†L523-L708】
- **验证途径**：持续集成任务（`make ci-lints`、`make ci-no-std-alloc`、`make ci-bench-smoke` 等）均会构建并运行该实现，间接覆盖上下文升级与热插拔逻辑。

### crates/spark-core/src/data_plane/service/simple.rs
- **位置**：模块级 `#![allow(unsafe_code)]`、`GuardedFuture::poll` 内 `get_unchecked_mut` 与 `Pin::new_unchecked` 调用。
- **目的**：在过程宏展开的顺序服务中，以零额外分配的方式包装业务 Future，同时保证协调器守卫在完成或 Drop 时正确释放。
- **安全性说明**：
  - `GuardedFuture` 本身被 `Pin<&mut Self>` 调用，`unsafe` 块前后均确认对象未移动；
  - 创建 `Pin<&mut Fut>` 前确认 Future 未被其他引用借用，符合 `Pin` 不变式；
  - 模块注释强调仅在标准 Future 驱动下使用该类型，避免被跨线程移动。
- **验证途径**：`make ci-lints` 会执行 `cargo clippy` 与 `cargo test`（含过程宏回归测试），同时 `make ci-bench-smoke` 覆盖性能场景，确保 `GuardedFuture` 在真实流程中稳定工作。

### crates/spark-contract-tests/src/hot_reload.rs
- **位置**：`noop_waker` 函数中的 `unsafe fn` 及 `unsafe { Waker::from_raw(...) }`。
- **目的**：构造一个永远不唤醒的 `Waker`，用于在热更新契约测试中自旋轮询配置流，避免依赖外部执行器。
- **安全性说明**：
  - `clone`/`wake`/`wake_by_ref`/`drop` 均满足 `RawWakerVTable` 合约，全部为 no-op，不会触发未定义行为；
  - 使用空指针传递上下文，且所有入口均忽略该指针，保证 clone/drop 语义一致；
  - `unsafe { Waker::from_raw(...) }` 仅在测试内部使用，调用方确保不会向调度器注册，避免影响生产代码。
- **验证途径**：在持续集成中通过 `cargo test -p spark-contract-tests`（由 `make ci-lints` 与相关流水线触发），确保热更新流程及 waker 行为被覆盖。

## 后续跟踪
- 若未来在非测试 crate 中引入 `unsafe`，需在最小作用域内通过 `#![allow(unsafe_code)]` 白名单，并补充上述审计信息。
