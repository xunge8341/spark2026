# Send + Sync + 'static 契约矩阵（T07 自检）

> 本文档对公共接口的线程安全与生命周期要求做显式对照，并补充“借用/拥有”成对入口。所有条目均要求在评审时与代码注释保持同步。

## 1. 核心接口速查表

| 模块 | Trait/函数 | Send + Sync + 'static 约束 | 借用入口 | 拥有入口 | 说明 |
| --- | --- | --- | --- | --- | --- |
| `pipeline::Channel` | `Channel` | ✅ (`Send + Sync + 'static`) | N/A | `write`, `flush` | 通道封装在 `Arc<dyn Channel>` 中跨线程复用，要求 `'static`；消息体按需转移所有权。 |
| `pipeline::Context` | `Context` | ✅ (`Send + Sync`)，🚫 `'static` | `channel()`, `controller()` 返回 `'static` 对象引用 | `write`, `close_graceful` | 上下文仅在单次事件调度内存活，故不强制 `'static`。 |
| `pipeline::Pipeline` | `Pipeline` | ✅ (`Send + Sync + 'static`) | `register_inbound_handler_static`, `register_outbound_handler_static` | `register_inbound_handler`, `register_outbound_handler` | 借用入口转发至轻量代理，方便复用全局单例。 |
| `pipeline::ChainBuilder` | `ChainBuilder` | 继承实现者约束 | `register_inbound_static`, `register_outbound_static` | `register_inbound`, `register_outbound` | 面向 Middleware 声明式装配的对偶入口。 |
| `codec::CodecRegistry` | `CodecRegistry` | ✅ (`Send + Sync + 'static`) | `register_static` | `register` | 注册中心可在运行时共享，借用入口避免对 `'static` 单例重复装箱。 |
| `configuration::ConfigurationBuilder` | `register_source_static` | Builder 自身 `Send + Sync` 可选 | `register_source_static` | `register_source` | 显式标注配置源 `'static` 假设，复用公共去重逻辑。 |
| `service::DynService` | `DynService` | ✅ (`Send + Sync + 'static`) | `BoxService::new(Arc<dyn DynService>)` | `ServiceObject` 等适配器 | 对象层服务需要跨线程持久化；请求体不要求 `'static`。 |

> 记号说明：✅ 表示要求满足该约束；🚫 表示刻意不要求；`N/A` 表示语义上不存在借用/拥有双入口。

## 2. 借用/拥有入口行为说明

1. **Pipeline Handler 注册**
   - 拥有型入口继续接收 `Box<dyn Handler>`，适合动态构造并转移所有权。
   - 借用型入口接收 `&'static dyn Handler`，通过内部代理实现零拷贝转发。
   - 两者共享相同的去重与执行顺序逻辑，确保链路拓扑一致。
2. **Codec Factory 注册**
   - `register_static` 通过 `BorrowedDynCodecFactory` 代理复用已有工厂；
   - 代理自身满足 `Send + Sync + 'static`，可安全存放于注册表中。
3. **配置源注册**
   - `register_source_static` 复用 `register_source` 的容量与去重检查；
   - 使用 `boxed_static_source` 封装 `'static` 引用，避免额外分配。

## 3. 审核要点

- `rg -n "Send \+ Sync \+ 'static"` 应命中上述模块的教案级注释，确保约束文字化。
- 借用/拥有入口需保证文档与实现一致，任何新增公共注册/读写 API 必须同步补充对应的成对入口。
- 在跨线程/跨生命周期合约测试中，所有代理类型需满足 `Send + Sync + 'static`，防止因借用路径引入悬垂引用。

## 4. 维护提示

- 若未来收紧实现者集合（例如限制部分 Trait 仅供内部实现），需同步更新本矩阵并在注释中说明原因。
- 对于暂未提供借用入口的 API，应在设计评审中确认确实无法安全复用 `'static` 引用，避免接口不完整。

