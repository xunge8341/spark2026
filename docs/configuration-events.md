# 配置事件契约

- 合约版本：`1.0.0`。
- 总览：配置控制面与审计事件的统一契约：覆盖自动校准、漂移检测与一致性恢复信号。
- 单一事实来源（SOT）：`schemas/configuration-events.schema.json` 与 `schemas/configuration-events.asyncapi.json`（由 `contracts/config_events.toml` 驱动生成）。
- 生成产物：`crates/spark-core/src/governance/configuration/events.rs`、`docs/configuration-events.md`。

## 事件总览

| 事件代码 | 名称 | 家族 | 严重性 | 结构体 |
| --- | --- | --- | --- | --- |
| `configuration.calibration.requested` | 配置校准请求 | `calibration` | `info` | `ConfigurationCalibrationRequested` |
| `configuration.drift.detected` | 配置漂移检测 | `drift` | `warning` | `ConfigurationDriftDetected` |
| `configuration.consistency.restored` | 配置一致性恢复 | `consistency` | `info` | `ConfigurationConsistencyRestored` |

## 复用结构体

### `DriftNodeSnapshot`
- **Why**：描述检测到配置漂移的节点快照。
- **Rationale**：用于事件负载记录差异节点，便于审计与排障人员快速定位问题主机。
- **How**：包含节点标识、哈希值以及漂移的配置键集合，生成器会根据该结构创建 `ConfigurationDriftDetected` 事件的嵌套字段。
- **字段**：
  | 字段 | 类型 | 必填 | 说明 |
  | --- | --- | --- | --- |
  | `node_id` | `String` | `是` | 节点逻辑标识，通常对应服务拓扑中的唯一 NodeId。 |
  | `observed_hash` | `String` | `是` | 当前节点从本地状态计算出的配置哈希。 |
  | `difference_keys` | `Vec<String>` | `是` | 与期望配置相比存在差异的键集合，列表顺序在聚合器中会被排序以保证稳定 diff。 |
  | `delta_score` | `u64` | `是` | 差异数量得分，通常等于 `difference_keys` 的长度，用于在事件中按照严重程度排序节点。 |

## 事件详情

### `configuration.calibration.requested` — 配置校准请求

**Why**：控制面向运行面发布的校准请求，用于触发目标节点重新拉取或对齐配置。

**What**：当观测到集群状态可能偏离（如心跳上报的哈希与控制面不一致）时，控制面会发送校准请求，并将关键信息记录到审计事件中，确保后续回放能够追踪责任节点与触发原因。

**契约要点**：
- 结构体：`ConfigurationCalibrationRequested`。
- 审计映射：action=`configuration.calibration.request`，entity_kind=`configuration.profile`，entity_id_field=`profile_id`。
- 字段列表：
  | 字段 | 类型 | 必填 | 说明 |
  | --- | --- | --- | --- |
  | `profile_id` | `String` | `是` | 需要校准的配置 Profile 标识，对应类型 ProfileId。 |
  | `controller_id` | `String` | `是` | 发起校准的控制面节点或逻辑控制器 ID。 |
  | `expected_hash` | `String` | `是` | 控制面计算的期望配置哈希，用于对比节点上报数据。 |
  | `observed_hash` | `String` | `是` | 触发本次请求的节点哈希值，帮助快速定位偏差来源。 |
  | `reason` | `String` | `否` | 附加说明或触发原因，例如 "hash_mismatch"、"stale_revision"。 |

**演练用例**：

- 用例：单节点手动校准演练
  - 目标：验证控制面能够针对特定节点下发校准请求并在审计事件中留下完整线索。
  - 准备：
    - 部署包含 1 控制面 + 1 业务节点的最小集群
    - 通过调试接口模拟节点上报过期哈希
  - 步骤：
    1. 触发控制面下发 `configuration.calibration.requested` 事件
    1. 确认运行面重新拉取配置并回报最新哈希
  - 验收：
    - 审计系统记录到事件，`profile_id`、`controller_id` 与哈希字段完整
    - 运行面节点在校准后哈希值与控制面一致

### `configuration.drift.detected` — 配置漂移检测

**Why**：聚合多节点上报，识别集群内的配置漂移并输出结构化差异列表。

**What**：运行面每个节点周期性汇报配置哈希与差异键集合，控制面在窗口内汇总后生成漂移事件，事件负载需携带全部漂移节点及其差异详情，供审计与排障使用。

**契约要点**：
- 结构体：`ConfigurationDriftDetected`。
- 审计映射：action=`configuration.drift.detected`，entity_kind=`configuration.profile`，entity_id_field=`profile_id`。
- 字段列表：
  | 字段 | 类型 | 必填 | 说明 |
  | --- | --- | --- | --- |
  | `profile_id` | `String` | `是` | 出现漂移的配置 Profile 标识。 |
  | `expected_hash` | `String` | `是` | 控制面认为的黄金配置哈希。 |
  | `majority_hash` | `String` | `是` | 在当前窗口内占多数的哈希值，便于评估漂移面是否集中。 |
  | `drift_window_ms` | `u64` | `是` | 漂移聚合的观测窗口大小（毫秒）。 |
  | `observed_node_count` | `u64` | `是` | 在窗口内上报的节点数量，用于评估样本覆盖度。 |
  | `divergent_nodes` | `Vec<DriftNodeSnapshot>` | `是` | 漂移节点的完整快照集合，按照差异得分降序排列。 |

**演练用例**：

- 用例：十节点漂移聚合与审计演练
  - 目标：验证控制面能够在 10 个节点出现哈希分歧时正确聚合事件并生成审计记录。
  - 准备：
    - 部署 10 个业务节点并使其加载相同 Profile
    - 通过故障注入工具让其中若干节点写入不同配置键
  - 步骤：
    1. 收集所有节点的哈希与差异键集合并提交给聚合器
    1. 调用聚合 API 生成 `configuration.drift.detected` 事件
    1. 将事件传入审计系统，校验序列号、动作与实体标识
  - 验收：
    - 事件中的 `divergent_nodes` 列表长度等于实际漂移节点数，且按差异数量排序
    - 审计事件的 `action` 为 `configuration.drift.detected`，`entity.kind` 为 `configuration.profile`
    - `AuditChangeSet` 与输入的变更集合一致，确保后续回放可重现配置差异

### `configuration.consistency.restored` — 配置一致性恢复

**Why**：通知治理与审计系统集群已恢复一致状态，便于闭环漂移处理流程。

**What**：在完成自动或人工校准后，控制面会发布一致性恢复事件，记录最终稳定哈希、耗时以及关联的漂移/校准事件 ID，形成完整的闭环证据链。

**契约要点**：
- 结构体：`ConfigurationConsistencyRestored`。
- 审计映射：action=`configuration.consistency.restored`，entity_kind=`configuration.profile`，entity_id_field=`profile_id`。
- 字段列表：
  | 字段 | 类型 | 必填 | 说明 |
  | --- | --- | --- | --- |
  | `profile_id` | `String` | `是` | 恢复一致性的配置 Profile 标识。 |
  | `stable_hash` | `String` | `是` | 最终达成一致的哈希值。 |
  | `reconciled_nodes` | `Vec<String>` | `是` | 成功完成校准的节点 ID 列表。 |
  | `duration_ms` | `u64` | `是` | 从漂移检测到一致性恢复的耗时（毫秒）。 |
  | `source_event_id` | `String` | `否` | 触发本次恢复的上游事件 ID（如漂移或校准事件），便于串联审计链。 |

**演练用例**：

- 用例：漂移恢复闭环演练
  - 目标：验证从漂移检测到一致性恢复的全链路审计闭环。
  - 准备：
    - 先执行 "十节点漂移聚合与审计演练" 确保存在漂移事件
    - 完成手动或自动校准流程，使全部节点哈希一致
  - 步骤：
    1. 收集恢复后节点列表与耗时信息
    1. 生成 `configuration.consistency.restored` 事件并写入审计系统
  - 验收：
    - 事件记录 `stable_hash` 与所有节点 ID，`source_event_id` 指向之前的漂移事件
    - 审计系统能够根据恢复事件闭合哈希链

