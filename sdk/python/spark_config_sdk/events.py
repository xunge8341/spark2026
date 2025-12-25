"""
配置事件数据模型：自动生成的 dataclass 定义。

Why:
    - 为客户端与 TCK 提供强类型载体，确保字段与审计映射与 SOT 一致。
How:
    - 由生成器解析 schemas/ 下的 JSON Schema 与 AsyncAPI SOT 渲染；字段说明嵌入在 docstring 中，支持教学级自解释。
What:
    - `STRUCT_TYPES`: 复用结构体定义；
    - `EVENT_PAYLOAD_TYPES`: 事件 payload dataclass；
    - `EVENT_DESCRIPTORS`: 元数据字典，含审计映射与演练用例。

注意：文件由生成器维护，请勿手工编辑。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Type

@dataclass
class DriftNodeSnapshot:
    """
    Why:
        描述检测到配置漂移的节点快照。
    What:
        包含节点标识、哈希值以及漂移的配置键集合，生成器会根据该结构创建 `ConfigurationDriftDetected` 事件的嵌套字段。
    Rationale:
        用于事件负载记录差异节点，便于审计与排障人员快速定位问题主机。
    How:
        - 由 SOT 自动生成；使用者仅需按字段填充数据。
    Pre-conditions:
        - 调用方需保证字段遵循契约规定的类型与业务约束。
    Post-conditions:
        - 数据可直接序列化为事件 payload，供审计/演练复现。
    Trade-offs:
        - 若需新增字段，请更新 schemas/ 下的 JSON Schema/AsyncAPI 并重新生成 SDK。
    """
    node_id: str  # 节点逻辑标识，通常对应服务拓扑中的唯一 NodeId。
    observed_hash: str  # 当前节点从本地状态计算出的配置哈希。
    difference_keys: List[str] = field(default_factory=list)  # 与期望配置相比存在差异的键集合，列表顺序在聚合器中会被排序以保证稳定 diff。
    delta_score: int  # 差异数量得分，通常等于 `difference_keys` 的长度，用于在事件中按照严重程度排序节点。

@dataclass
class ConfigurationCalibrationRequested:
    """
    Why:
        控制面向运行面发布的校准请求，用于触发目标节点重新拉取或对齐配置。
    What:
        当观测到集群状态可能偏离（如心跳上报的哈希与控制面不一致）时，控制面会发送校准请求，并将关键信息记录到审计事件中，确保后续回放能够追踪责任节点与触发原因。
    How:
        - 字段顺序源自 JSON Schema 的 x-fieldOrder，便于跨语言比对 diff。
    Audit Mapping:
        - action=configuration.calibration.request
        - entity_kind=configuration.profile
        - entity_id_field=profile_id
    Trade-offs:
        - 修改字段时需同时更新 SOT（Schema/AsyncAPI）、审计、演练及 SDK。
    """
    profile_id: str  # 需要校准的配置 Profile 标识，对应类型 ProfileId。
    controller_id: str  # 发起校准的控制面节点或逻辑控制器 ID。
    expected_hash: str  # 控制面计算的期望配置哈希，用于对比节点上报数据。
    observed_hash: str  # 触发本次请求的节点哈希值，帮助快速定位偏差来源。
    reason: Optional[str] = None  # 附加说明或触发原因，例如 "hash_mismatch"、"stale_revision"。

@dataclass
class ConfigurationDriftDetected:
    """
    Why:
        聚合多节点上报，识别集群内的配置漂移并输出结构化差异列表。
    What:
        运行面每个节点周期性汇报配置哈希与差异键集合，控制面在窗口内汇总后生成漂移事件，事件负载需携带全部漂移节点及其差异详情，供审计与排障使用。
    How:
        - 字段顺序源自 JSON Schema 的 x-fieldOrder，便于跨语言比对 diff。
    Audit Mapping:
        - action=configuration.drift.detected
        - entity_kind=configuration.profile
        - entity_id_field=profile_id
    Trade-offs:
        - 修改字段时需同时更新 SOT（Schema/AsyncAPI）、审计、演练及 SDK。
    """
    profile_id: str  # 出现漂移的配置 Profile 标识。
    expected_hash: str  # 控制面认为的黄金配置哈希。
    majority_hash: str  # 在当前窗口内占多数的哈希值，便于评估漂移面是否集中。
    drift_window_ms: int  # 漂移聚合的观测窗口大小（毫秒）。
    observed_node_count: int  # 在窗口内上报的节点数量，用于评估样本覆盖度。
    divergent_nodes: List[DriftNodeSnapshot] = field(default_factory=list)  # 漂移节点的完整快照集合，按照差异得分降序排列。

@dataclass
class ConfigurationConsistencyRestored:
    """
    Why:
        通知治理与审计系统集群已恢复一致状态，便于闭环漂移处理流程。
    What:
        在完成自动或人工校准后，控制面会发布一致性恢复事件，记录最终稳定哈希、耗时以及关联的漂移/校准事件 ID，形成完整的闭环证据链。
    How:
        - 字段顺序源自 JSON Schema 的 x-fieldOrder，便于跨语言比对 diff。
    Audit Mapping:
        - action=configuration.consistency.restored
        - entity_kind=configuration.profile
        - entity_id_field=profile_id
    Trade-offs:
        - 修改字段时需同时更新 SOT（Schema/AsyncAPI）、审计、演练及 SDK。
    """
    profile_id: str  # 恢复一致性的配置 Profile 标识。
    stable_hash: str  # 最终达成一致的哈希值。
    reconciled_nodes: List[str] = field(default_factory=list)  # 成功完成校准的节点 ID 列表。
    duration_ms: int  # 从漂移检测到一致性恢复的耗时（毫秒）。
    source_event_id: Optional[str] = None  # 触发本次恢复的上游事件 ID（如漂移或校准事件），便于串联审计链。

STRUCT_TYPES: Dict[str, Type[object]] = {
    "DriftNodeSnapshot": DriftNodeSnapshot,
}

EVENT_PAYLOAD_TYPES: Dict[str, Type[object]] = {
    "ConfigurationCalibrationRequested": ConfigurationCalibrationRequested,
    "ConfigurationDriftDetected": ConfigurationDriftDetected,
    "ConfigurationConsistencyRestored": ConfigurationConsistencyRestored,
}

EVENT_DESCRIPTORS: Dict[str, Dict[str, object]] = {
    "configuration.calibration.requested": {
        "ident": "ConfigurationCalibrationRequested",
        "family": "calibration",
        "severity": "info",
        "name": "配置校准请求",
        "summary": "控制面向运行面发布的校准请求，用于触发目标节点重新拉取或对齐配置。",
        "description": "当观测到集群状态可能偏离（如心跳上报的哈希与控制面不一致）时，控制面会发送校准请求，并将关键信息记录到审计事件中，确保后续回放能够追踪责任节点与触发原因。",
        "audit": {
            "action": "configuration.calibration.request",
            "entity_kind": "configuration.profile",
            "entity_id_field": "profile_id"
        },
        "drills": [{"expectations":["审计系统记录到事件，`profile_id`、`controller_id` 与哈希字段完整","运行面节点在校准后哈希值与控制面一致"],"goal":"验证控制面能够针对特定节点下发校准请求并在审计事件中留下完整线索。","setup":["部署包含 1 控制面 + 1 业务节点的最小集群","通过调试接口模拟节点上报过期哈希"],"steps":["触发控制面下发 `configuration.calibration.requested` 事件","确认运行面重新拉取配置并回报最新哈希"],"title":"单节点手动校准演练"}]
    },
    "configuration.drift.detected": {
        "ident": "ConfigurationDriftDetected",
        "family": "drift",
        "severity": "warning",
        "name": "配置漂移检测",
        "summary": "聚合多节点上报，识别集群内的配置漂移并输出结构化差异列表。",
        "description": "运行面每个节点周期性汇报配置哈希与差异键集合，控制面在窗口内汇总后生成漂移事件，事件负载需携带全部漂移节点及其差异详情，供审计与排障使用。",
        "audit": {
            "action": "configuration.drift.detected",
            "entity_kind": "configuration.profile",
            "entity_id_field": "profile_id"
        },
        "drills": [{"expectations":["事件中的 `divergent_nodes` 列表长度等于实际漂移节点数，且按差异数量排序","审计事件的 `action` 为 `configuration.drift.detected`，`entity.kind` 为 `configuration.profile`","`AuditChangeSet` 与输入的变更集合一致，确保后续回放可重现配置差异"],"goal":"验证控制面能够在 10 个节点出现哈希分歧时正确聚合事件并生成审计记录。","setup":["部署 10 个业务节点并使其加载相同 Profile","通过故障注入工具让其中若干节点写入不同配置键"],"steps":["收集所有节点的哈希与差异键集合并提交给聚合器","调用聚合 API 生成 `configuration.drift.detected` 事件","将事件传入审计系统，校验序列号、动作与实体标识"],"title":"十节点漂移聚合与审计演练"}]
    },
    "configuration.consistency.restored": {
        "ident": "ConfigurationConsistencyRestored",
        "family": "consistency",
        "severity": "info",
        "name": "配置一致性恢复",
        "summary": "通知治理与审计系统集群已恢复一致状态，便于闭环漂移处理流程。",
        "description": "在完成自动或人工校准后，控制面会发布一致性恢复事件，记录最终稳定哈希、耗时以及关联的漂移/校准事件 ID，形成完整的闭环证据链。",
        "audit": {
            "action": "configuration.consistency.restored",
            "entity_kind": "configuration.profile",
            "entity_id_field": "profile_id"
        },
        "drills": [{"expectations":["事件记录 `stable_hash` 与所有节点 ID，`source_event_id` 指向之前的漂移事件","审计系统能够根据恢复事件闭合哈希链"],"goal":"验证从漂移检测到一致性恢复的全链路审计闭环。","setup":["先执行 \"十节点漂移聚合与审计演练\" 确保存在漂移事件","完成手动或自动校准流程，使全部节点哈希一致"],"steps":["收集恢复后节点列表与耗时信息","生成 `configuration.consistency.restored` 事件并写入审计系统"],"title":"漂移恢复闭环演练"}]
    },
}
