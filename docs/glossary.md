# Spark 契约术语表（P0-04）

> 本术语表面向架构师与实现者，旨在对齐 Spark 核心抽象的中文语义、Rustdoc 定位以及与文档/测试之间的映射关系。每个词条均给出：
> - **定位**：概念在整体架构中的职责；
> - **Rustdoc**：主要阅读入口，便于与 API 文档互跳；
> - **示例**：仓库内的参考实现或教学样例；
> - **TCK**：验证该术语相关契约的测试套件。

## Buffer（缓冲池与视图）
- **定位**：统一内存租借、回收与零拷贝视图的抽象，贯穿编解码、管线与传输层；由 `spark_core::buffer` 模块定义契约，`spark-buffer` 提供默认实现。 
- **Rustdoc**：[`spark_core::buffer`](https://docs.rs/spark-core/latest/spark_core/buffer/index.html)、[`spark_buffer`](https://docs.rs/spark-buffer/latest/spark_buffer/)。
- **示例**：[`SlabBufferPool::acquire`](../crates/spark-buffer/src/pool.rs#L122-L179) 演示如何在池中租借可写缓冲并回收指标。 
- **TCK**：[`resource_exhaustion::thread_pool_starvation_emits_busy_then_retry_after`](../crates/spark-contract-tests/src/resource_exhaustion.rs#L292-L415) 通过模拟池耗尽验证缓冲契约的背压信号。

## Frame（帧与消息封装）
- **定位**：`spark_core::protocol::Frame` 将请求 ID、序列号与负载绑定，支撑跨传输的统一分片语义。 
- **Rustdoc**：[`spark_core::protocol::Frame`](https://docs.rs/spark-core/latest/spark_core/protocol/struct.Frame.html)。
- **示例**：[`Frame::try_new`](../crates/spark-core/src/data_plane/protocol/frame.rs) 的单元测试演示帧构造约束（请求 ID、FIN 标记）。 
- **TCK**：[`spark-impl-tck::ws_sip::frame_text_binary`](../crates/spark-impl-tck/tests/ws_sip/frame_text_binary.rs#L8-L155) 验证 SIP<->WebSocket 转换对帧 FIN/掩码语义的遵守。

## Codec（编解码）
- **定位**：聚合 `Codec`/`Encoder`/`Decoder` 等 trait，定义流水线如何将用户消息与帧之间互转，并与缓冲池协作。 
- **Rustdoc**：[`spark_core::codec`](https://docs.rs/spark-core/latest/spark_core/codec/index.html)、[`spark-codecs`](https://docs.rs/spark-codecs/latest/spark_codecs/)。
- **示例**：[`LineDelimitedCodec`](../crates/spark-codec-line/src/line.rs#L170-L210) 展示 `DecodeContext`/`EncodeContext` 的最小用法。 
- **TCK**：[`slowloris::slow_reader_exceeding_budget_is_rejected`](../crates/spark-contract-tests/src/slowloris.rs#L70-L115) 使用限速读取检验编解码器的背压策略。

## Transport（传输与握手）
- **定位**：`spark_core::transport` 抽象握手、监听、双向信道构建等能力，统一 QUIC/TCP/UDP 等后端。 
- **Rustdoc**：[`spark_core::transport`](https://docs.rs/spark-core/latest/spark_core/transport/index.html)。
- **示例**：[`TransportParams`](../crates/spark-core/src/data_plane/transport/params.rs#L20-L136) 展示协商参数的序列化策略。 
- **TCK**：[`graceful_shutdown::draining_connections_respect_fin`](../crates/spark-contract-tests/src/graceful_shutdown.rs#L640-L796) 通过注入自定义传输桩验证 FIN/超时协商流程。

## Pipeline（处理链）
- **定位**：`spark_core::pipeline` 提供 Handler 链、控制器、上下文与热插拔能力，连接编解码、路由与服务执行；配套的 L2 默认实现集中在 `spark_router::pipeline` 命名空间中。
- **Rustdoc**：[`spark_core::pipeline`](https://docs.rs/spark-core/latest/spark_core/pipeline/index.html)、[`spark_router::pipeline`](https://docs.rs/spark-router/latest/spark_router/pipeline/index.html)。
- **示例**：[`hot_swap` 集成测试](../crates/spark-core/tests/pipeline/hot_swap.rs#L40-L420) 展示 `Pipeline`、`PipelineInitializer` 与 `CoreServices` 协同实现零停机热替换。 
- **TCK**：[`hot_swap::pipeline_state_transitions_are_ordered`](../crates/spark-contract-tests/src/hot_swap.rs#L430-L585) 验证 epoch 切换与事件顺序。

## PipelineInitializer（管线中间件）
- **定位**：`PipelineInitializer` 及其描述符定义在 Handler 链中插入横切逻辑（鉴权、观测、熔断）的扩展点。 
- **Rustdoc**：[`spark_core::pipeline::initializer`](https://docs.rs/spark-core/latest/spark_core/pipeline/initializer/index.html)。
- **示例**：[`ChainBuilder::register_inbound`](../crates/spark-core/src/data_plane/pipeline/initializer.rs#L52-L118) 说明如何在链路中组装中间件。 
- **TCK**：[`observability::middleware_emits_expected_attributes`](../crates/spark-contract-tests/src/observability.rs#L210-L332) 检查中间件插桩的可观测性契约。

## Service（服务接口）
- **定位**：`spark_core::service` 聚焦业务处理接口、自动对象安全桥接与层级化装饰，支撑高阶路由与管线结果返还。 
- **Rustdoc**：[`spark_core::service`](https://docs.rs/spark-core/latest/spark_core/service/index.html)、[`spark-middleware`](https://docs.rs/spark-middleware/latest/spark_middleware/)。
- **示例**：[`ServiceLogic::call`](../crates/spark-core/src/data_plane/service/simple.rs#L120-L210) 演示 `SimpleServiceFn` 将异步函数适配为服务。 
- **TCK**：[`graceful_shutdown::coordinator_waits_for_service_draining`](../crates/spark-contract-tests/src/graceful_shutdown.rs#L820-L1012) 验证服务在关闭流程中的幂等性与截止时间处理。

## Router（路由控制）
- **定位**：`spark-router` Crate 统一承载 L2 路由实现，将 `RoutingIntent`、`RouteCatalog` 与动态元数据结合后托管给对象层 Service。
- **Rustdoc**：[`spark-router`](https://docs.rs/spark-router/latest/spark_router/)。
  - **示例**：[`DefaultRouter::route_dyn`](../crates/spark-router/src/lib.rs#L303-L361) 演示对象层路由如何向下游 `Service` 转发请求。
- **TCK**：[`state_machine::context_propagates_routing_snapshot`](../crates/spark-contract-tests/src/state_machine.rs#L240-L368) 校验路由快照与 Pipeline 上下文的一致性。

## Context（调用上下文）
- **定位**：`spark_core::context` 与 `spark_core::contract::CallContext` 统一调度、取消、截止时间、缓冲等跨 Handler 信息。 
- **Rustdoc**：[`spark_core::context`](https://docs.rs/spark-core/latest/spark_core/context/index.html)、[`spark_core::contract`](https://docs.rs/spark-core/latest/spark_core/contract/index.html)。
- **示例**：[`CallContext::with_deadline`](../crates/spark-core/src/kernel/contract.rs#L120-L198) 描述截止时间传播。 
- **TCK**：[`state_machine::child_context_inherits_cancellation`](../crates/spark-contract-tests/src/state_machine.rs#L60-L214) 覆盖取消链路。

## Error（错误语义）
- **定位**：`spark_core::error` 定义 `CoreError`、`DomainError` 等抽象，并在 `category_matrix` 中给出自动响应策略。 
- **Rustdoc**：[`spark_core::error`](https://docs.rs/spark-core/latest/spark_core/error/index.html)。
- **示例**：[`CategoryMatrixEntry`](../crates/spark-core/src/error/generated/category_matrix.rs#L20-L210) 展示自动响应矩阵生成物。 
- **TCK**：[`errors::domain_error_is_mapped_to_matrix`](../crates/spark-contract-tests/src/errors.rs#L250-L360) 检查错误分类到响应的映射。

## State（状态与节流）
- **定位**：`spark_core::model::State`/`Status` 以及 `spark_core::status` 模块表示 ReadyState/Budget/节流策略，驱动背压。 
- **Rustdoc**：[`spark_core::model`](https://docs.rs/spark-core/latest/spark_core/model/index.html)、[`spark_core::status`](https://docs.rs/spark-core/latest/spark_core/status/index.html)。
- **示例**：[`RetryAdvice`](../crates/spark-core/src/kernel/status/retry.rs#L15-L140) 说明重试节奏计算。 
- **TCK**：[`backpressure::channel_queue_exhaustion_emits_busy_then_retry_after`](../crates/spark-contract-tests/src/backpressure.rs#L60-L198) 验证 ReadyState 序列。
