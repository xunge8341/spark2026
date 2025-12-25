# 上下文语法糖：`spawn_in` 与 `with_ctx!`

> **目标**：在保持显式传递 [`CallContext`](../crates/spark-core/src/kernel/contract.rs) 契约的前提下，降低任务提交与上下文克隆的样板代码。

## 设计背景

- 框架要求所有异步任务必须携带调用上下文，以确保取消/截止/预算/追踪/身份五元组能够跨层传播；
- 在 Handler 或服务实现中，常见代码模式如下：
  ```rust
  let runtime = ctx.executor();
  let call_ctx = ctx.call_context().clone();
  let handle = runtime.spawn(&call_ctx, async move { /* ... */ });
  ```
  每次都需要显式提取执行器、克隆上下文、再提交任务，既冗长又容易遗漏。
- `runtime::sugar` 模块抽象“运行时能力”与“上下文视图”，让上述流程压缩为一行函数调用或一个宏展开。

## 核心组件

- [`RuntimeCaps`](../crates/spark-core/src/platform/runtime/sugar.rs)：统一描述“可提交任务的运行时能力”，默认对实现 [`TaskExecutor`](../crates/spark-core/src/platform/runtime/executor.rs) 的类型生效。
- [`CallContext`](../crates/spark-core/src/platform/runtime/sugar.rs)：语法糖视图，将 `CallContext` 引用与 `RuntimeCaps` 绑定在一起，提供 `clone_call()` 等便捷方法。
- [`spawn_in`](../crates/spark-core/src/platform/runtime/sugar.rs)：基于语法糖视图提交任务，自动完成 `spawn` 与 Join 句柄转换。
- [`with_ctx!`](../crates/spark-core/src/macros.rs)：在 Pipeline [`Context`](../crates/spark-core/src/data_plane/pipeline/context.rs) 内重绑定同名变量，直接得到语法糖视图。

## 使用示例

### 手动构造语法糖上下文

```rust
use spark_core::{contract::CallContext, runtime::{self, TaskExecutor}};

let call_ctx = CallContext::builder().build();
let executor = MyExecutor::default();
let sugar_ctx = runtime::CallContext::borrowed(&call_ctx, &executor);
let join = runtime::spawn_in(&sugar_ctx, async move {
    // 需要拥有上下文时显式克隆
    let owned_ctx = sugar_ctx.clone_call();
    do_work(owned_ctx).await
});
```

### 在 Pipeline `Context` 中配合 `with_ctx!`

```rust
use spark_core::{with_ctx, runtime};

fn on_read<C: spark_core::pipeline::Context>(ctx: &C) {
    let fut = with_ctx!(ctx, async move {
        let join = runtime::spawn_in(&ctx, async move { perform_io().await });
        join.join().await.expect("任务执行失败")
    });
    drop(fut); // 示例中忽略调度逻辑
}
```

宏在展开后仅包含显式的引用获取与结构构造，不会引入隐式线程局部或全局状态，可放心在审核严格的代码库中使用。

## 注意事项

1. `spawn_in` 不会自动克隆 `CallContext`；若异步任务需要所有权，请使用 `CallContext::clone_call()`。
2. `with_ctx!` 仅适用于实现 [`pipeline::Context`](../crates/spark-core/src/data_plane/pipeline/context.rs) 的类型；若在其它场景使用，请手动调用 `runtime::CallContext::borrowed`（或 `runtime::CallContext::new` 搭配自定义运行时能力）。
3. `RuntimeCaps` 默认返回 [`JoinHandle`](../crates/spark-core/src/platform/runtime/task.rs)；如需自定义句柄，请在自定义实现的 [`spawn_with`](../crates/spark-core/src/platform/runtime/sugar.rs) 中返回自有类型，并确保仍遵循 [`TaskHandle`](../crates/spark-core/src/platform/runtime/task.rs) 契约。
