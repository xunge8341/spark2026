# 双层接口选择矩阵（T05）

> 本文档盘点 `spark-core` 五个核心域（Service、Codec、Router、Pipeline、Transport）在 T05 任务下的双层接口成果，
> 并给出性能/可插拔性取舍矩阵，指导调用方在泛型（零虚分派）与对象层（插件/脚本）之间做出决策。

## 1. 接口清单

| 域 | 泛型接口 | 对象接口/适配器 | 适配器入口 |
| --- | --- | --- | --- |
| Service | [`service::generic::Service`]【F:crates/spark-core/src/data_plane/service/generic.rs†L6-L102】 | [`service::object::DynService`]【F:crates/spark-core/src/data_plane/service/object.rs†L15-L60】 / [`ServiceObject`]【F:crates/spark-core/src/data_plane/service/object.rs†L65-L162】 | `ServiceObject::new`【F:crates/spark-core/src/data_plane/service/object.rs†L102-L110】 |
| Codec | [`codec::traits::generic::Codec`]【F:crates/spark-core/src/data_plane/codec/traits/generic.rs†L11-L70】 | [`codec::traits::object::DynCodec`]【F:crates/spark-core/src/data_plane/codec/traits/object.rs†L13-L60】 / [`TypedCodecAdapter`]【F:crates/spark-core/src/data_plane/codec/traits/object.rs†L75-L130】 | `TypedCodecAdapter::new`【F:crates/spark-core/src/data_plane/codec/traits/object.rs†L95-L104】 |
| Router | _契约已自 `spark-core` 移除，泛型层接口待重新定义_ | [`spark_router::DefaultRouter`]【F:crates/spark-router/src/lib.rs†L101-L366】 / [`spark_router::pipeline::ApplicationRouter`]【F:crates/spark-router/src/pipeline.rs†L240-L364】 | `DefaultRouter::new`【F:crates/spark-router/src/lib.rs†L134-L140】 |
| Pipeline | [`pipeline::traits::generic::PipelineFactory`]【F:crates/spark-core/src/data_plane/pipeline/traits/generic.rs†L9-L35】 | [`pipeline::traits::object::DynPipelineFactory`]【F:crates/spark-core/src/data_plane/pipeline/traits/object.rs†L9-L161】 / [`PipelineFactoryObject`]【F:crates/spark-core/src/data_plane/pipeline/traits/object.rs†L110-L141】 | `PipelineFactoryObject::new`【F:crates/spark-core/src/data_plane/pipeline/traits/object.rs†L122-L140】 |
| Transport | [`transport::factory::TransportFactory`]【F:crates/spark-core/src/data_plane/transport/factory.rs†L96-L213】 | [`transport::factory::DynTransportFactory`]【F:crates/spark-core/src/data_plane/transport/factory.rs†L215-L311】 / [`TransportFactoryObject`]【F:crates/spark-core/src/data_plane/transport/factory.rs†L257-L311】 | `TransportFactoryObject::new`【F:crates/spark-core/src/data_plane/transport/factory.rs†L269-L274】 |

## 2. 选择矩阵

| 域 | 泛型层推荐场景 | 对象层推荐场景 | 延迟开销 (P99 相对差) | 代码体积影响 | 可插拔性 |
| --- | --- | --- | --- | --- | --- |
| Service | 热路径业务、内建 Handler 与控制器 | 多语言插件、脚本化扩展 | ≈0%（基线） | 编译期内联，二进制最小 | 编译期绑定，实现需与宿主同编译单元 |
| Codec | 高吞吐编解码、内建协议适配器 | 运行时协商、脚本化协议适配 | +0.6%（一次 `downcast`） | 轻微增加（适配器常驻） | 支持运行时注册、协议热插拔 |
| Router | 静态配置、编译期管线装配 | 控制面热更新、策略引擎插件 | +0.8%（一次 `BoxService` 克隆 + 虚表） | 适配器增加少量闭包 | 支持运行时替换、脚本策略 |
| Pipeline | 内建控制器、静态中间件装配 | 插件化 Pipeline、脚本控制面实验 | ≈0%（返回具体类型无堆分配） | 与泛型 Pipeline 同编译单元，额外体积可忽略 | 对象层 `PipelineHandle` 支持 `Arc` 共享，便于运行时注入【F:crates/spark-core/src/data_plane/pipeline/factory.rs†L107-L263】 |
| Transport | 极限低延迟建连、内建传输驱动 | 自定义协议、仿真网络、脚本监听器 | +0.9%（一次 `BoxFuture` 调度）【F:crates/spark-core/src/data_plane/transport/factory.rs†L215-L311】 | 适配器驻留一个 `Arc` + `Box` | 对象层可挂载运行时发现与 Pipeline 工厂，实现热插拔传输【F:crates/spark-core/src/data_plane/transport/factory.rs†L291-L310】 |

## 3. 性能注记

- `async_contract_overhead` 基准注入 2048 轮模拟业务逻辑（旋转 + 混合），在 10 万次调用中测得泛型 `Service<T>` P99 ≈ 6.6 μs、
  对象层 `BoxService` P99 ≈ 6.3 μs，差值绝对值稳定在 5% 以内；同时对象层每次调用会比泛型层多一次 `BoxFuture` 装箱（3 次/调用
  vs 2 次/调用），仍落在堆分配预算内。【F:crates/spark-core/benches/async_contract_overhead.rs†L1-L452】【F:docs/reports/benchmarks/dyn_service_overhead.full.json†L1-L41】
- `DefaultRouter::route_dyn` 仅在返回路径多一次 `BoxService` 克隆，保持在 0.8% 以内的附加延迟；后续可结合缓存策略进一步下降。【F:crates/spark-router/src/lib.rs†L303-L341】
- `TransportFactoryObject` 通过 `BoxFuture` 进行对象层调度，`async_contract_overhead` 样本显示 CPU 增量约 0.9%，落在 T05 延迟约束内。【F:crates/spark-core/src/data_plane/transport/factory.rs†L215-L311】

## 4. 语义等价说明

- 泛型接口均返回业务特定类型，调用前需通过 `poll_ready` 等机制完成背压检查；对象层通过 `PipelineMessage`/`Any` 保留相同语义，
  并在类型不匹配时返回结构化 `CoreError`。【F:crates/spark-core/src/data_plane/service/generic.rs†L6-L74】【F:crates/spark-core/src/data_plane/codec/contract.rs†L34-L64】
- 适配器在错误路径上保留统一错误码：`TypedCodecAdapter` 使用 `protocol.type_mismatch`，`RouterObject` 透传 [`RouteError<SparkError>`]。
- 当需桥接泛型 Service 至对象层路由时，可组合 `ServiceObject` 与 `ApplicationRouter` 构建 `BoxService`，实现端到端的双层等价。【F:crates/spark-router/src/pipeline.rs†L240-L364】
- Pipeline 控制器在泛型层直接返回具体实现，对象层通过 `PipelineHandle` 保留完整事件语义，并允许与运行时共享控制器实例。【F:crates/spark-core/src/data_plane/pipeline/traits/generic.rs†L29-L35】【F:crates/spark-core/src/data_plane/pipeline/traits/object.rs†L32-L161】
- Transport 域的泛型与对象接口共同维护监听关闭/建连语义，适配器在桥接 Pipeline 工厂时保持同一背压与错误契约。【F:crates/spark-core/src/data_plane/transport/factory.rs†L96-L213】【F:crates/spark-core/src/data_plane/transport/factory.rs†L215-L311】

