# Spark 全局架构蓝图（装配时 / 运行时分层版）

> 本文针对 Spark 数据面的双层分离模型提供统一视图，
> 聚焦 **装配时（L1）ServerChannel → PipelineInitializer → Pipeline** 与 **运行时（L2）Channel → Pipeline → Handler → L2 Router Handler → Service** 两条链路。
> 目标是帮助平台团队、业务团队在新模型下完成 ServerChannel 装配、请求调度与治理操作。

## 1. 愿景与设计原则
- **解耦握手与执行**：ServerChannel 在 L1 完成协议协商与 Pipeline 装配，L2 仅负责事件驱动执行，使监听器升级不再干扰业务处理链路。【F:crates/spark-core/src/data_plane/transport/server_channel.rs†L9-L122】【F:crates/spark-core/src/data_plane/pipeline/pipeline.rs†L47-L200】
- **Pipeline 契约中心化**：PipelineInitializer、Pipeline、Handler 形成声明式装配 + 运行时调度的核心骨架，所有横切能力都通过该契约注入并观测。【F:crates/spark-core/src/data_plane/pipeline/initializer.rs†L64-L133】【F:crates/spark-core/src/data_plane/pipeline/handler.rs†L10-L148】
- **服务访问收敛**：L2 Router Handler 仅负责选择 Service，最终调用继续沿用 Tower 风格 Service 契约，保持背压、取消、预算等语义一致性；标准实现由 `crates/spark-router` 提供，统一了 Handler 与 DynRouter 之间的装配语义。【F:crates/spark-router/src/pipeline.rs†L214-L376】【F:crates/spark-core/src/data_plane/service/generic.rs†L6-L74】
- **可观测性前置**：InitializerDescriptor 与 PipelineEvent 打通 L1/L2 度量链路，为握手成功率、Handler 顺序和路由决策提供统一追踪入口。【F:crates/spark-core/src/data_plane/pipeline/initializer.rs†L7-L61】【F:crates/spark-core/src/data_plane/pipeline/pipeline.rs†L56-L200】

## 2. 分层运行模型总览
Spark 数据面围绕 L1/L2 两层展开，每层既独立又协同：

### 2.1 装配时（L1）链路
```
ServerChannel (监听 / 协商)
  │  └─ set_initializer_selector(HandshakeOutcome → Result<PipelineInitializer, CoreError>)
  ▼
PipelineInitializer (声明式装配)
  │  ├─ register_inbound / register_outbound
  │  └─ descriptor() → 观测元数据
  ▼
Pipeline 控制器（生成 Handler Registry + PipelineEvent 广播）
```
- `ServerChannel::set_initializer_selector` 将握手结果映射为具体 PipelineInitializer，允许在策略缺失时返回结构化错误。【F:crates/spark-core/src/data_plane/transport/server_channel.rs†L28-L118】
- PipelineInitializer 负责注册入站/出站 Handler，并生成 Handler Registry 与描述信息；`configure` 现额外获取 [`Channel`] 引用，以便按连接特征完成装配。【F:crates/spark-core/src/data_plane/pipeline/initializer.rs†L64-L134】
- Pipeline 控制器作为 L1/L2 的交界面，持有 Handler Registry、事件广播器以及热插拔元数据，是运行时 introspection 的基石。【F:crates/spark-core/src/data_plane/pipeline/pipeline.rs†L47-L200】

### 2.2 运行时（L2）链路
```
Channel (连接级 IO)
  │  └─ read / write / flush / classify_backpressure
  ▼
Pipeline (事件循环)
  │  └─ 调度 inbound / outbound Handlers，广播 PipelineEvent
  ▼
Handlers
  │  ├─ 通用 Handler（鉴权 / 编解码 / 限流 / ...）
  │  └─ ApplicationRouter（L2 Router Handler）
  ▼
Service (Tower Service / BoxService)
```
- Channel 驱动事件循环并统一不同协议实现的读写、背压与半关闭语义，是运行时的流量入口。【F:crates/spark-core/src/data_plane/transport/channel.rs†L146-L210】
- Pipeline 根据 L1 注册的 Handler 顺序执行业务逻辑，同时向观测面广播 PipelineEvent。【F:crates/spark-core/src/data_plane/pipeline/pipeline.rs†L47-L200】
- ApplicationRouter（来自 `crates/spark-router`）作为 L2 Router Handler，读取 Pipeline 上下文并选取合适 Service，承接旧 Router 的路由职责但运行在 L2 尾部。【F:crates/spark-router/src/pipeline.rs†L214-L376】
- Service 执行最终业务调用，继续遵循 Tower 契约，支持背压与取消反馈。【F:crates/spark-core/src/data_plane/service/generic.rs†L6-L74】

## 3. 生命周期与角色分工
| 阶段 | 关键目标 | 输入 | 输出 | 主要角色 | 核心接口 |
| --- | --- | --- | --- | --- | --- |
| 需求立项 | 识别业务域、协议需求、合规限制 | 业务需求、SLO、估算流量 | 架构决策记录、初版 RoutingIntent | 架构师、产品 | `POST /v1/intents` |
| 架构设计 | 规划 L1/L2 职责，挑选 Handler/Service 契约 | 决策记录、跨域约束 | PipelineTemplate、InitializerDescriptor 草案 | 架构师、平台 | `POST /v1/pipelines/templates` |
| 开发实现 | 交付 PipelineInitializer、Handler、Service、L2 Router Handler 并自测 | 模板、Mock 数据 | 可部署工件、ServiceDescriptor | 开发、QA | `PUT /v1/services/{serviceId}` |
| 集成测试 | 验证握手、装配、运行时路由链路与可观测性 | 工件、测试计划 | 测试报告、基线指标 | QA、SRE | `POST /v1/test-suites/run` |
| 部署运营 | 将新 PipelineInitializer 与 Handler 发布到 ServerChannel，并滚动升级 | 测试报告、部署策略 | 生效监听器、Pipeline 注册表 | 平台、SRE | `POST /v1/deployments` |
| 观测优化 | 采集握手成功率、PipelineEvent、路由耗时 | 遥测数据、PipelineEvent 流 | 优化建议、异常告警 | 平台、研发 | `GET /v1/telemetry/events` |
| 弹性扩缩 | 基于 Channel 背压和 Service 就绪度调节资源 | 观测数据、弹性策略 | 调整后的资源配置 | 平台、SRE | `POST /v1/scale-plans` |
| 退役封存 | 下线 PipelineInitializer/Handler/Service 并归档路由快照 | 归档计划、依赖清单 | 退役报告、配置快照 | 架构师、SRE | `POST /v1/pipelines/{id}:decommission` |

> **说明**：控制面 API 保持统一入口，L1/L2 分层只改变监听器内部装配方式，不影响外部治理形态。【F:crates/spark-core/src/data_plane/pipeline/initializer.rs†L64-L133】【F:crates/spark-core/src/data_plane/pipeline/pipeline.rs†L47-L200】

## 4. 装配时（L1）工作流细化
1. **监听器启动**：平台通过 `TransportFactory::bind_dyn` 创建 ServerChannel，注入监听地址、握手协议与初始策略，然后调用 `set_initializer_selector` 绑定协商结果到具体 PipelineInitializer。【F:tools/baselines/spark-core.public-api.json†L500-L509】【F:crates/spark-core/src/data_plane/transport/server_channel.rs†L28-L114】
2. **协商完成**：`ServerChannel::accept` 输出 `(Channel, TransportSocketAddr)` 与 `HandshakeOutcome`，监听器依 outcome 选择 PipelineInitializer 并构造 Pipeline 控制器。【F:crates/spark-core/src/data_plane/transport/server_channel.rs†L61-L114】【F:crates/spark-core/src/data_plane/pipeline/pipeline.rs†L47-L200】
3. **Handler 编排**：`PipelineInitializer::configure` 使用 ChainBuilder 注册入站/出站 Handler，生成 Handler Registry 与 introspection 快照，供观测面查询。【F:crates/spark-core/src/data_plane/pipeline/initializer.rs†L64-L133】【F:crates/spark-core/src/data_plane/pipeline/pipeline.rs†L123-L200】
4. **部署回滚**：装配失败时，控制面依据 `CoreError` 分类回滚并广播失败事件，确保 ServerChannel 在热更新期间保持可控状态。【F:crates/spark-core/src/data_plane/pipeline/initializer.rs†L95-L133】【F:crates/spark-core/src/data_plane/pipeline/pipeline.rs†L182-L200】

## 5. 运行时（L2）工作流细化
1. **事件驱动循环**：Channel 提供读写、背压与半关闭语义，Pipeline 监听 Channel 事件并驱动入站 Handler；出站链路负责批处理与刷新写入。【F:crates/spark-core/src/data_plane/transport/channel.rs†L146-L210】【F:crates/spark-core/src/data_plane/pipeline/pipeline.rs†L47-L200】
2. **上下文扩展**：业务或平台组件可通过 `ExtensionsMap` 向 `RouterContextState` 写入意图、连接元数据与动态标签，为 L2 Router Handler 提供决策依据。【F:crates/spark-router/src/pipeline.rs†L26-L211】
3. **L2 路由决策**：ApplicationRouter 读取上下文，调用 DynRouter 判定目标 Service，再将请求托付给运行时执行器并写回响应。【F:crates/spark-router/src/pipeline.rs†L214-L376】
4. **服务调用与背压**：Service 遵循 `poll_ready` / `call` 契约执行，若返回背压信号，ApplicationRouter 与 PipelineEvent 联动触发限流或重试策略。【F:crates/spark-core/src/data_plane/service/generic.rs†L6-L74】【F:crates/spark-core/src/data_plane/pipeline/pipeline.rs†L56-L121】
5. **观测回路**：Pipeline 持续广播 `PipelineEventKind`，并暴露 Handler Registry 快照，帮助运维识别异常链路与时延热点。【F:crates/spark-core/src/data_plane/pipeline/pipeline.rs†L56-L200】

## 6. 组件角色速览
### 6.1 L1：ServerChannel、PipelineInitializer、Pipeline 控制器
- **ServerChannel**：统一监听抽象，负责握手协商、PipelineInitializer 选择与生命周期管理，是 L1 的逻辑入口。【F:crates/spark-core/src/data_plane/transport/server_channel.rs†L31-L122】
- **PipelineInitializer**：声明式配置 Handler，输出 InitializerDescriptor 并保障幂等性与线程安全，防止重复装配。【F:crates/spark-core/src/data_plane/pipeline/initializer.rs†L64-L133】
- **Pipeline 控制器**：维护 Handler Registry、事件广播和热插拔元数据，让运行时能够读取固定队列并支持观测。【F:crates/spark-core/src/data_plane/pipeline/pipeline.rs†L47-L200】

### 6.2 L2：Channel、Pipeline、Handler、ApplicationRouter、Service
- **Channel**：提供连接级 IO 与背压判定，统一协议行为，支撑运行时调度。【F:crates/spark-core/src/data_plane/transport/channel.rs†L146-L210】
- **Pipeline**：以事件驱动方式调度入站/出站 Handler，保证顺序一致与异常可控。【F:crates/spark-core/src/data_plane/pipeline/pipeline.rs†L47-L200】
- **Handler**：承担鉴权、编解码、限流等横切逻辑，与 Pipeline 上下文共享零拷贝数据。【F:crates/spark-core/src/data_plane/pipeline/handler.rs†L10-L148】
- **ApplicationRouter（L2 Router Handler）**：位于 Handler 链尾部，将请求映射到 Service，是旧 Router 职责的 L2 化实现。【F:crates/spark-router/src/pipeline.rs†L214-L376】
- **Service**：实现业务调用契约，通过 CallContext 暴露取消与背压反馈，支撑多语言运行时扩展。【F:crates/spark-core/src/data_plane/service/generic.rs†L6-L74】

## 7. API 与平台体验
### 7.1 开发体验
- **资源模型**：ServerChannel、PipelineInitializer、Handler、Service、L2 Router Handler 统一以 RESTful 资源管理，支持 CRUD 与版本控制，便于脚本化与 SDK 生成。
- **契约校验**：`POST /v1/contracts/validate` 校验 PipelineInitializer 与 Handler 组合，避免装配时破坏运行时语义。
- **仿真测试**：`POST /v1/simulations/run` 模拟握手、装配与运行时事件，提前暴露背压或路由瓶颈。
- **版本管理**：各资源暴露 `version` 与 `compat` 字段，结合 `If-Match` / `If-None-Match` 防止多人修改冲突。

### 7.2 运维体验
- **声明式部署**：部署 API 支持一次推送 ServerChannel + PipelineInitializer + Handler 版本，平台自动热更新监听器，保证 L1/L2 平滑过渡。
- **可观测性**：`GET /v1/telemetry/events` 与 `GET /v1/pipelines/{id}/registry` 展示 PipelineEvent 与 Handler Registry 快照，帮助排查 L1/L2 问题。【F:crates/spark-core/src/data_plane/pipeline/pipeline.rs†L56-L200】
- **回滚与审计**：结合 InitializerDescriptor 与审计日志快速识别变更影响范围，支撑安全回滚。【F:crates/spark-core/src/data_plane/pipeline/initializer.rs†L7-L61】
- **弹性策略**：扩缩 API 依据 Channel 背压与 Service 就绪度调整 ServerChannel 实例与执行器配额，保持服务稳定。

### 7.3 测试体验
- **环境隔离**：`X-Spark-Stage` 头与 CoreServices Profile 组合，保证沙箱 / 预发 / 生产隔离。
- **可编排测试流**：集成测试覆盖 L1 握手与 L2 请求链路，验证 PipelineInitializer 与 ApplicationRouter 组合。
- **覆盖率与指标**：测试报告输出 Handler 调用路径、路由命中率与背压统计，确保各种流量模式下稳定。

## 8. 风险治理与缓解策略
- **协商失败**：当 ServerChannel 无法根据 HandshakeOutcome 匹配 PipelineInitializer 时，应记录结构化错误并降级到安全模板，避免监听器停摆。【F:crates/spark-core/src/data_plane/transport/server_channel.rs†L28-L114】
- **装配冲突**：若 PipelineInitializer 幂等性不足导致 Handler 重复注册，可通过契约校验与回滚机制快速修复。【F:crates/spark-core/src/data_plane/pipeline/initializer.rs†L95-L133】
- **运行时背压**：Channel 的背压分类与 Service 的 `poll_ready` 信号需要纳入弹性策略，避免热点连接拖垮 ServerChannel。【F:crates/spark-core/src/data_plane/transport/channel.rs†L146-L210】【F:crates/spark-core/src/data_plane/service/generic.rs†L6-L74】
- **路由异常**：ApplicationRouter 构建上下文或调用 Service 失败时会记录错误并终止请求，需要配合观测告警与灰度策略快速响应。【F:crates/spark-router/src/pipeline.rs†L214-L376】
- **安全合规**：鉴权、审计、加密 Handler 必须在 PipelineInitializer 中显式排序，与 L2 Handler 共享最小必要信息以降低数据泄露风险。

## 9. 推进建议
1. **先行改造 L1**：优先完成 ServerChannel 与 PipelineInitializer Selector 升级，确保所有 ServerChannel 都能基于 HandshakeOutcome 挑选 Pipeline。【F:crates/spark-core/src/data_plane/transport/server_channel.rs†L28-L114】
2. **同步 L2 Router Handler**：将旧 Router 逻辑迁移到 ApplicationRouter，确保路由策略在 L2 Handler 中执行且与 Service 契约对齐。【F:crates/spark-router/src/pipeline.rs†L214-L376】
3. **平台化赋能**：提供 CLI / SDK 生成 PipelineInitializer 模板、部署策略与监控仪表板，降低团队在新架构下的接入成本。
4. **知识沉淀**：沉淀 Handler / Service / ApplicationRouter 的最佳实践与性能基准，重点关注握手成功率、路由命中率与背压处理。
5. **生态开放**：继续开放 PipelineInitializer、Handler、L2 Router Handler 扩展点，鼓励外部团队贡献协议适配器或策略插件，强化生态活力。
