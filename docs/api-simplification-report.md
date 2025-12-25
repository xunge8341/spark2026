# spark-core Trait 简化评估报告

> 作者：AI 助手（根据仓库当前提交生成）

## 1. 总览
- 评估范围覆盖 `spark-core` 下的缓冲、集群、路由、运行时、可观测性、安全等子模块，共计 47 个公开 Trait。
- 目标是识别**可以合并**或**提供外观 Facade** 的接口，并指出潜在的简化收益与风险。

## 2. 可合并/重构候选

| 序号 | Trait 组合 | 建议 | 理由 | 风险 |
| ---- | ---------- | ---- | ---- | ---- |
| T1 | `runtime::AsyncRuntime` + `runtime::TaskExecutor` + `runtime::TimeDriver` | 合并为 `AsyncRuntime` 的扩展特性或提供统一 facade | 三者在多数实现（Tokio、Glommio）中由同一运行时承担；调用方常常需要三者同时注入 | 可能降低对极简执行器的灵活度，需要通过 feature gate 保留精简实现 |
| T2 | `pipeline::InboundHandler` + `pipeline::OutboundHandler` | 使用单一 `PipelineStage` Trait + 方向枚举 | 大量 Handler 同时实现两个 Trait，合并后可减少样板代码并允许共享状态机 | 合并后可能影响依赖泛型方向优化的实现，需要在新 Trait 中保留方向标识 |
| T3 | `transport::ServerChannel` + `transport::Channel` | 提供 `TransportEndpoint` Facade | 两者在 QUIC/HTTP3 等协议中共享大量代码（握手、流管理、TLS 参数）；外观可对外隐藏实现细节 | Facade 需要覆盖所有生命周期方法，若实现差异过大（如仅客户端支持 0-RTT）可能导致接口臃肿 |
| T4 | `observability::Logger` + `observability::MetricsProvider` + `observability::OpsEventBus` | 引入 `ObservabilityFacade` | 在运行时注入服务时往往需要三者组合；统一 Facade 可减少依赖注入层的 Arc 克隆，并为扩展字段提供集中配置点 | Facade 容易变成“上帝对象”，需通过 trait object 组合或 builder 避免侵入性调整 |

## 3. 简化建议

1. **Trait 聚合策略**
   - 为运行时相关 Trait 引入可选的 `RuntimeCapabilities` 结构体，封装执行器、计时器、阻塞任务能力，并提供能力探针。
   - 在管线层提供 `PipelineStage` + `StageDirection`，并为旧接口保留默认实现以平滑迁移。

2. **命名与分层**
   - 建议将 `cluster::membership::ClusterMembership` 与 `cluster::discovery::ServiceDiscovery` 的公共流控配置抽象为 `cluster::flow_control`，已在本次改动中落实，为后续合并打下基础。
   - 运行时与可观测性模块可引入 `prelude` 模块，暴露常用 Trait 组合，降低调用方的导入负担。

3. **Facade 构想**
   - `CoreServices` 结构已接近 Facade，可进一步扩展为构造器模式，允许一次性注入缓冲池、集群订阅、可观测性组件。
   - 建议新增 `ObservabilityFacade`，内部聚合指标、日志、事件策略接口，减少 Handler 直接依赖多个 `Arc`。

## 4. 后续行动建议

- 对 T1/T2 提案，可在下一轮迭代中创建实验性 feature（如 `runtime-facade`、`pipeline-unified-stage`），编写契约测试验证兼容性。
- 针对 Facade 提议，优先评估对 `spark-contract-tests` 的影响，并为主要实现（Tokio、quinn）提供概念验证。
- 与产品团队对齐 Custom 扩展命名规范，确保新增 Facade 不会与扩展点冲突。

## 5. 本轮原型输出（2024-XX-XX）

- **落地提案：T4 Observability Facade**
- 在 `spark-otel::facade` 下提供 `DefaultObservabilityFacade` 参考实现，配合 `spark-core::observability` 中的 `ObservabilityFacade` 契约聚合日志、指标、运维事件与健康探针能力。
  - `runtime::CoreServices` 新增 `observability_facade` 便捷方法，以现有字段构造外观，验证“不破坏既有字段即可增量提供 Facade”的可行性。
  - Rustdoc 明确前置/后置条件与扩展点，为后续 Feature Gate 化或替换实现奠定文档基础。
- **后续计划**
  - 评估是否为 `Context` 或其它 Handler 注入点提供 Facade 直通（可能通过扩展 `Pipeline::Context`）。
  - 在 `spark-contract-tests` 中补充 Facade 级别的契约测试，确保日志/指标/事件的绑定关系保持一致。
  - 探索与 Trace 能力的整合方式，如通过可选方法或组合 Trait 扩展 Facade 的覆盖范围。

