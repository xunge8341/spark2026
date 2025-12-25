# Pipeline 热插拔切换时序与内存安全

> **适用范围**：本文档配套 `spark-core` 默认的 `HotSwapPipeline` 实现，用于解释在运行时插入、替换、移除
> Handler 时的可见性保证与调用顺序。若后续实现替换为自定义控制器，请根据相同方法补齐安全性分析。

## 1. 术语约定

- **快照（Snapshot）**：通过 `ArcSwap<Vec<Arc<HandlerEntry>>>` 暴露给读线程的 Handler 链路视图。
- **提交（Commit）**：写线程基于互斥锁构造新 `Vec` 并调用 `HandlerEpochBuffer::store` 原子替换快照。
- **栅栏（Epoch Barrier）**：`HandlerEpochBuffer::bump_epoch` 返回的逻辑时钟。读取方若观察到该值增加，即可
  推断所有伴随更新（链路快照 + 注册表快照）均已完成。

## 2. 切换时序

```
Writer Thread (热更新)                         Reader Thread (事件分发)
--------------------------------------------------------------------------------
lock(mutation)                                 load snapshot (Arc clone)
clone current Vec -> mutable Vec
apply mutation (add/remove/replace)
Arc::new(new Vec)
rebuild HandlerRegistration Vec
registry.update(new registrations)
handlers.store(new Arc)
handlers.bump_epoch()  ---- epoch ↑
unlock(mutation)                              observe epoch via Pipeline::epoch
                                              (旧快照仍有效直至 Arc 引用计数归零)
```

- **线性化点**：`handlers.store`。自此之后，新的事件读取操作将看到更新后的链路。
- **可观测一致性**：`Pipeline::epoch` 在 `bump_epoch` 后增加；管理面或测试可在调用热更新 API 后轮询该数值。
  当 `epoch` 出现自增且注册表快照中已包含目标 Handler，即可判定切换完成。

## 3. 内存安全保证

1. **旧快照延迟释放**：读线程始终持有 `Arc<Vec<Arc<HandlerEntry>>>`，即便写线程已提交新链路，旧链路的所有
   `Arc` 在最后一个读线程释放前不会销毁，避免悬垂引用。
2. **Handler 引用安全**：`HandlerEntry` 在构造时缓存 `Arc<dyn InboundHandler>`/`Arc<dyn OutboundHandler>`，
   控制器在分发事件时仅借用引用，不会跨线程移动未封装的裸指针。
3. **注册表一致性**：在调用 `handlers.bump_epoch` 前，写线程已经完成注册表快照更新。观察者如需确认更新完成，
   应同时比较 `epoch` 与 `registry().snapshot()`。

## 4. 边界条件与建议

- **空链路**：当 Pipeline 初始为空时，`epoch == 0`，读线程获取的快照长度为 0；首次插入 Handler 后 `epoch` 增加。
- **并发移除**：若在 Handler 正在处理 `on_read` 时被移除，读线程仍使用旧快照继续完成剩余调用；后续新事件
  不再进入被移除的 Handler。
- **失败回滚**：若写线程在构造新链路或注册表快照时发生错误，应在调用 `store`/`bump_epoch` 前直接返回错误，
  以免向观察者暴露不一致状态。
- **性能调优**：`bump_epoch` 使用 `SeqCst` 保证顺序一致性，若未来需要进一步降低开销，可考虑将 Handler 链路
  按方向分区，减少每次热更新的复制长度。

## 5. 验证工具

- **单元测试**：`cargo test -p spark-core -- tests::pipeline::hot_swap::*` 覆盖了常规插入场景与 Loom 并发模型。
- **代码审计**：`rg -n '\bepoch\(\)' crates/spark-core/src/pipeline` 可快速定位所有使用 epoch 的位置，核对是否遵循
  “先更新快照，再 bump” 的顺序。

以上流程确保 Pipeline 在不中断流量的前提下完成 Handler 的热插拔，并提供可复用的验证手段。
