// @generated 自动生成文件，请勿手工修改。
// 由 crates/spark-core/build.rs 根据 contracts/config_events.toml 生成。

#[doc = "配置事件契约：由 contracts/config_events.toml 自动生成，统一控制面、运行面与审计面共享的事件语义。"]
#[doc = ""]
#[doc = "合约版本（version）：1.0.0。"]
#[doc = "总体说明（summary）：配置控制面与审计事件的统一契约：覆盖自动校准、漂移检测与一致性恢复信号。。"]
#[doc = "本模块与 docs/configuration-events.md 同步生成，如需调整字段请修改合约后重新生成。"]
use alloc::{string::String, vec::Vec};

#[doc = "配置事件严重性等级，映射合约中的 `severity` 字段，供告警与审计标签复用。"]
#[doc = ""]
#[doc = "# 教案式说明（Why）"]
#[doc = "- 统一控制面与审计面对于事件严重性的理解，避免多处硬编码。"]
#[doc = "# 契约定义（What）"]
#[doc = "- 取值范围：info/warning/critical；"]
#[doc = "# 实现提示（How）"]
#[doc = "- 生成代码中提供 `as_str` 帮助转换为稳定字符串标签。"]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EventSeverity {
    Info,
    Warning,
    Critical,
}

#[doc = "严重性到字符串的稳定映射，用于事件标签与审计实体标签。"]
#[doc = ""]
#[doc = "# 合同说明（What）"]
#[doc = "- 输入：严重性枚举；输出：`info`/`warning`/`critical`。"]
#[doc = "# 风险提示（Trade-offs）"]
#[doc = "- 若合约新增等级需同步扩展匹配分支，否则构建时会 panic。"]
impl EventSeverity {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Info => "info",
            Self::Warning => "warning",
            Self::Critical => "critical",
        }
    }
}

#[doc = "事件元数据描述符，集中存放代码、名称、审计映射与演练用例。"]
#[doc = ""]
#[doc = "# Why"]
#[doc = "- 运行面与工具链可以读取该描述符生成文档或校验事件负载。"]
#[doc = "# What"]
#[doc = "- 包含事件识别码、所属家族、严重性、摘要、详细说明、审计映射、字段列表与演练用例。"]
#[doc = "# How"]
#[doc = "- 构建脚本直接写出 `'static` 常量，确保运行期零成本访问。"]
#[derive(Clone, Debug, PartialEq)]
pub struct ConfigurationEventDescriptor {
    pub ident: &'static str,
    pub code: &'static str,
    pub family: &'static str,
    pub name: &'static str,
    pub severity: EventSeverity,
    pub summary: &'static str,
    pub description: &'static str,
    pub audit: AuditDescriptor,
    pub fields: &'static [EventFieldDescriptor],
    pub drills: &'static [DrillDescriptor],
}

#[doc = "描述事件与审计系统的映射关系，包括动作与实体标识。"]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AuditDescriptor {
    pub action: &'static str,
    pub entity_kind: &'static str,
    pub entity_id_field: &'static str,
}

#[doc = "字段描述符，记录字段类型、是否必填以及教案级说明。"]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EventFieldDescriptor {
    pub name: &'static str,
    pub type_name: &'static str,
    pub required: bool,
    pub doc: &'static str,
}

#[doc = "演练用例描述符，记录目标、准备步骤、执行步骤与验收标准。"]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DrillDescriptor {
    pub title: &'static str,
    pub goal: &'static str,
    pub setup: &'static [&'static str],
    pub steps: &'static [&'static str],
    pub expectations: &'static [&'static str],
}

#[doc = "结构体 `DriftNodeSnapshot`：描述检测到配置漂移的节点快照。。"]
#[doc = ""]
#[doc = "# 教案式说明（Why）"]
#[doc = "- 用于事件负载记录差异节点，便于审计与排障人员快速定位问题主机。"]
#[doc = "# 契约定义（What）"]
#[doc = "- 字段映射遵循合约声明顺序，所有字段均参与事件序列化。"]
#[doc = "# 实现提示（How）"]
#[doc = "- 包含节点标识、哈希值以及漂移的配置键集合，生成器会根据该结构创建 `ConfigurationDriftDetected` 事件的嵌套字段。"]
#[doc = "# 风险与注意事项（Trade-offs）"]
#[doc = "- 修改字段时务必同步更新合约与演练文档，避免 SOT 漂移。"]
#[derive(Clone, Debug, PartialEq)]
pub struct DriftNodeSnapshot {
    #[doc = "字段 `node_id`（Why/What/How）：节点逻辑标识，通常对应服务拓扑中的唯一 NodeId。"]
    #[doc = "- 类型：`String`。"]
    #[doc = "- 前置条件：调用方需提供符合命名规范的数据；"]
    #[doc = "- 后置条件：事件序列化后保持原样，供审计回放。"]
    pub node_id: String,

    #[doc = "字段 `observed_hash`（Why/What/How）：当前节点从本地状态计算出的配置哈希。"]
    #[doc = "- 类型：`String`。"]
    #[doc = "- 前置条件：调用方需提供符合命名规范的数据；"]
    #[doc = "- 后置条件：事件序列化后保持原样，供审计回放。"]
    pub observed_hash: String,

    #[doc = "字段 `difference_keys`（Why/What/How）：与期望配置相比存在差异的键集合，列表顺序在聚合器中会被排序以保证稳定 diff。"]
    #[doc = "- 类型：`Vec<String>`。"]
    #[doc = "- 前置条件：调用方需提供符合命名规范的数据；"]
    #[doc = "- 后置条件：事件序列化后保持原样，供审计回放。"]
    pub difference_keys: Vec<String>,

    #[doc = "字段 `delta_score`（Why/What/How）：差异数量得分，通常等于 `difference_keys` 的长度，用于在事件中按照严重程度排序节点。"]
    #[doc = "- 类型：`u64`。"]
    #[doc = "- 前置条件：调用方需提供符合命名规范的数据；"]
    #[doc = "- 后置条件：事件序列化后保持原样，供审计回放。"]
    pub delta_score: u64,

}

#[doc = "事件 `配置校准请求`（代码：`configuration.calibration.requested`，家族：`calibration`）。"]
#[doc = ""]
#[doc = "# 教案式说明（Why）"]
#[doc = "- 控制面向运行面发布的校准请求，用于触发目标节点重新拉取或对齐配置。"]
#[doc = "# 契约定义（What）"]
#[doc = "- 当观测到集群状态可能偏离（如心跳上报的哈希与控制面不一致）时，控制面会发送校准请求，并将关键信息记录到审计事件中，确保后续回放能够追踪责任节点与触发原因。"]
#[doc = "- 审计映射：action=`configuration.calibration.request`，entity_kind=`configuration.profile`，entity_id_field=`profile_id`。"]
#[doc = "# 实现提示（How）"]
#[doc = "- 该结构体字段顺序与合约保持一致，便于聚合器直接构造；"]
#[doc = "- 由生成器保证字段文档嵌入，提醒调用者关注前置/后置条件。"]
#[doc = "# 风险与注意事项（Trade-offs）"]
#[doc = "- 修改字段前需评估审计与演练文档影响，避免链路断裂。"]
#[derive(Clone, Debug, PartialEq)]
pub struct ConfigurationCalibrationRequested {
    #[doc = "字段 `profile_id`（Why/What/How）：需要校准的配置 Profile 标识，对应类型 ProfileId。"]
    #[doc = "- 类型：`String`。"]
    #[doc = "- 前置条件：调用方必须提供该字段；"]
    #[doc = "- 后置条件：事件序列化后用于审计与演练校验。"]
    pub profile_id: String,

    #[doc = "字段 `controller_id`（Why/What/How）：发起校准的控制面节点或逻辑控制器 ID。"]
    #[doc = "- 类型：`String`。"]
    #[doc = "- 前置条件：调用方必须提供该字段；"]
    #[doc = "- 后置条件：事件序列化后用于审计与演练校验。"]
    pub controller_id: String,

    #[doc = "字段 `expected_hash`（Why/What/How）：控制面计算的期望配置哈希，用于对比节点上报数据。"]
    #[doc = "- 类型：`String`。"]
    #[doc = "- 前置条件：调用方必须提供该字段；"]
    #[doc = "- 后置条件：事件序列化后用于审计与演练校验。"]
    pub expected_hash: String,

    #[doc = "字段 `observed_hash`（Why/What/How）：触发本次请求的节点哈希值，帮助快速定位偏差来源。"]
    #[doc = "- 类型：`String`。"]
    #[doc = "- 前置条件：调用方必须提供该字段；"]
    #[doc = "- 后置条件：事件序列化后用于审计与演练校验。"]
    pub observed_hash: String,

    #[doc = "字段 `reason`（Why/What/How）：附加说明或触发原因，例如 \\\"hash_mismatch\\\"、\\\"stale_revision\\\"。"]
    #[doc = "- 类型：`Option<String>`。"]
    #[doc = "- 前置条件：可选字段，缺省表示信息未知；"]
    #[doc = "- 后置条件：事件序列化后用于审计与演练校验。"]
    pub reason: Option<String>,

}

#[doc = "事件 `配置漂移检测`（代码：`configuration.drift.detected`，家族：`drift`）。"]
#[doc = ""]
#[doc = "# 教案式说明（Why）"]
#[doc = "- 聚合多节点上报，识别集群内的配置漂移并输出结构化差异列表。"]
#[doc = "# 契约定义（What）"]
#[doc = "- 运行面每个节点周期性汇报配置哈希与差异键集合，控制面在窗口内汇总后生成漂移事件，事件负载需携带全部漂移节点及其差异详情，供审计与排障使用。"]
#[doc = "- 审计映射：action=`configuration.drift.detected`，entity_kind=`configuration.profile`，entity_id_field=`profile_id`。"]
#[doc = "# 实现提示（How）"]
#[doc = "- 该结构体字段顺序与合约保持一致，便于聚合器直接构造；"]
#[doc = "- 由生成器保证字段文档嵌入，提醒调用者关注前置/后置条件。"]
#[doc = "# 风险与注意事项（Trade-offs）"]
#[doc = "- 修改字段前需评估审计与演练文档影响，避免链路断裂。"]
#[derive(Clone, Debug, PartialEq)]
pub struct ConfigurationDriftDetected {
    #[doc = "字段 `profile_id`（Why/What/How）：出现漂移的配置 Profile 标识。"]
    #[doc = "- 类型：`String`。"]
    #[doc = "- 前置条件：调用方必须提供该字段；"]
    #[doc = "- 后置条件：事件序列化后用于审计与演练校验。"]
    pub profile_id: String,

    #[doc = "字段 `expected_hash`（Why/What/How）：控制面认为的黄金配置哈希。"]
    #[doc = "- 类型：`String`。"]
    #[doc = "- 前置条件：调用方必须提供该字段；"]
    #[doc = "- 后置条件：事件序列化后用于审计与演练校验。"]
    pub expected_hash: String,

    #[doc = "字段 `majority_hash`（Why/What/How）：在当前窗口内占多数的哈希值，便于评估漂移面是否集中。"]
    #[doc = "- 类型：`String`。"]
    #[doc = "- 前置条件：调用方必须提供该字段；"]
    #[doc = "- 后置条件：事件序列化后用于审计与演练校验。"]
    pub majority_hash: String,

    #[doc = "字段 `drift_window_ms`（Why/What/How）：漂移聚合的观测窗口大小（毫秒）。"]
    #[doc = "- 类型：`u64`。"]
    #[doc = "- 前置条件：调用方必须提供该字段；"]
    #[doc = "- 后置条件：事件序列化后用于审计与演练校验。"]
    pub drift_window_ms: u64,

    #[doc = "字段 `observed_node_count`（Why/What/How）：在窗口内上报的节点数量，用于评估样本覆盖度。"]
    #[doc = "- 类型：`u64`。"]
    #[doc = "- 前置条件：调用方必须提供该字段；"]
    #[doc = "- 后置条件：事件序列化后用于审计与演练校验。"]
    pub observed_node_count: u64,

    #[doc = "字段 `divergent_nodes`（Why/What/How）：漂移节点的完整快照集合，按照差异得分降序排列。"]
    #[doc = "- 类型：`Vec<DriftNodeSnapshot>`。"]
    #[doc = "- 前置条件：调用方必须提供该字段；"]
    #[doc = "- 后置条件：事件序列化后用于审计与演练校验。"]
    pub divergent_nodes: Vec<DriftNodeSnapshot>,

}

#[doc = "事件 `配置一致性恢复`（代码：`configuration.consistency.restored`，家族：`consistency`）。"]
#[doc = ""]
#[doc = "# 教案式说明（Why）"]
#[doc = "- 通知治理与审计系统集群已恢复一致状态，便于闭环漂移处理流程。"]
#[doc = "# 契约定义（What）"]
#[doc = "- 在完成自动或人工校准后，控制面会发布一致性恢复事件，记录最终稳定哈希、耗时以及关联的漂移/校准事件 ID，形成完整的闭环证据链。"]
#[doc = "- 审计映射：action=`configuration.consistency.restored`，entity_kind=`configuration.profile`，entity_id_field=`profile_id`。"]
#[doc = "# 实现提示（How）"]
#[doc = "- 该结构体字段顺序与合约保持一致，便于聚合器直接构造；"]
#[doc = "- 由生成器保证字段文档嵌入，提醒调用者关注前置/后置条件。"]
#[doc = "# 风险与注意事项（Trade-offs）"]
#[doc = "- 修改字段前需评估审计与演练文档影响，避免链路断裂。"]
#[derive(Clone, Debug, PartialEq)]
pub struct ConfigurationConsistencyRestored {
    #[doc = "字段 `profile_id`（Why/What/How）：恢复一致性的配置 Profile 标识。"]
    #[doc = "- 类型：`String`。"]
    #[doc = "- 前置条件：调用方必须提供该字段；"]
    #[doc = "- 后置条件：事件序列化后用于审计与演练校验。"]
    pub profile_id: String,

    #[doc = "字段 `stable_hash`（Why/What/How）：最终达成一致的哈希值。"]
    #[doc = "- 类型：`String`。"]
    #[doc = "- 前置条件：调用方必须提供该字段；"]
    #[doc = "- 后置条件：事件序列化后用于审计与演练校验。"]
    pub stable_hash: String,

    #[doc = "字段 `reconciled_nodes`（Why/What/How）：成功完成校准的节点 ID 列表。"]
    #[doc = "- 类型：`Vec<String>`。"]
    #[doc = "- 前置条件：调用方必须提供该字段；"]
    #[doc = "- 后置条件：事件序列化后用于审计与演练校验。"]
    pub reconciled_nodes: Vec<String>,

    #[doc = "字段 `duration_ms`（Why/What/How）：从漂移检测到一致性恢复的耗时（毫秒）。"]
    #[doc = "- 类型：`u64`。"]
    #[doc = "- 前置条件：调用方必须提供该字段；"]
    #[doc = "- 后置条件：事件序列化后用于审计与演练校验。"]
    pub duration_ms: u64,

    #[doc = "字段 `source_event_id`（Why/What/How）：触发本次恢复的上游事件 ID（如漂移或校准事件），便于串联审计链。"]
    #[doc = "- 类型：`Option<String>`。"]
    #[doc = "- 前置条件：可选字段，缺省表示信息未知；"]
    #[doc = "- 后置条件：事件序列化后用于审计与演练校验。"]
    pub source_event_id: Option<String>,

}

#[doc = "配置事件合约版本常量，用于运行时快速校验生成产物。"]
pub const CONFIGURATION_EVENTS_VERSION: &str = "1.0.0";

#[doc = "配置事件合约摘要，便于 UI 或 CLI 展示总体说明。"]
pub const CONFIGURATION_EVENTS_SUMMARY: &str = "配置控制面与审计事件的统一契约：覆盖自动校准、漂移检测与一致性恢复信号。";

pub const CONFIGURATION_CALIBRATION_REQUESTED_FIELDS: &[EventFieldDescriptor] = &[
    EventFieldDescriptor {
        name: "profile_id",
        type_name: "String",
        required: true,
        doc: "需要校准的配置 Profile 标识，对应类型 ProfileId。",
    },
    EventFieldDescriptor {
        name: "controller_id",
        type_name: "String",
        required: true,
        doc: "发起校准的控制面节点或逻辑控制器 ID。",
    },
    EventFieldDescriptor {
        name: "expected_hash",
        type_name: "String",
        required: true,
        doc: "控制面计算的期望配置哈希，用于对比节点上报数据。",
    },
    EventFieldDescriptor {
        name: "observed_hash",
        type_name: "String",
        required: true,
        doc: "触发本次请求的节点哈希值，帮助快速定位偏差来源。",
    },
    EventFieldDescriptor {
        name: "reason",
        type_name: "Option<String>",
        required: false,
        doc: "附加说明或触发原因，例如 \"hash_mismatch\"、\"stale_revision\"。",
    },
];

pub const CONFIGURATION_CALIBRATION_REQUESTED_DRILLS: &[DrillDescriptor] = &[
    DrillDescriptor {
        title: "单节点手动校准演练",
        goal: "验证控制面能够针对特定节点下发校准请求并在审计事件中留下完整线索。",
        setup: &["部署包含 1 控制面 + 1 业务节点的最小集群", "通过调试接口模拟节点上报过期哈希"],
        steps: &["触发控制面下发 `configuration.calibration.requested` 事件", "确认运行面重新拉取配置并回报最新哈希"],
        expectations: &["审计系统记录到事件，`profile_id`、`controller_id` 与哈希字段完整", "运行面节点在校准后哈希值与控制面一致"],
    },
];

pub const CONFIGURATION_CALIBRATION_REQUESTED: ConfigurationEventDescriptor = ConfigurationEventDescriptor {
    ident: "ConfigurationCalibrationRequested",
    code: "configuration.calibration.requested",
    family: "calibration",
    name: "配置校准请求",
    severity: EventSeverity::Info,
    summary: "控制面向运行面发布的校准请求，用于触发目标节点重新拉取或对齐配置。",
    description: "当观测到集群状态可能偏离（如心跳上报的哈希与控制面不一致）时，控制面会发送校准请求，并将关键信息记录到审计事件中，确保后续回放能够追踪责任节点与触发原因。",
    audit: AuditDescriptor {
        action: "configuration.calibration.request",
        entity_kind: "configuration.profile",
        entity_id_field: "profile_id",
    },
    fields: CONFIGURATION_CALIBRATION_REQUESTED_FIELDS,
    drills: CONFIGURATION_CALIBRATION_REQUESTED_DRILLS,
};

pub const CONFIGURATION_DRIFT_DETECTED_FIELDS: &[EventFieldDescriptor] = &[
    EventFieldDescriptor {
        name: "profile_id",
        type_name: "String",
        required: true,
        doc: "出现漂移的配置 Profile 标识。",
    },
    EventFieldDescriptor {
        name: "expected_hash",
        type_name: "String",
        required: true,
        doc: "控制面认为的黄金配置哈希。",
    },
    EventFieldDescriptor {
        name: "majority_hash",
        type_name: "String",
        required: true,
        doc: "在当前窗口内占多数的哈希值，便于评估漂移面是否集中。",
    },
    EventFieldDescriptor {
        name: "drift_window_ms",
        type_name: "u64",
        required: true,
        doc: "漂移聚合的观测窗口大小（毫秒）。",
    },
    EventFieldDescriptor {
        name: "observed_node_count",
        type_name: "u64",
        required: true,
        doc: "在窗口内上报的节点数量，用于评估样本覆盖度。",
    },
    EventFieldDescriptor {
        name: "divergent_nodes",
        type_name: "Vec<DriftNodeSnapshot>",
        required: true,
        doc: "漂移节点的完整快照集合，按照差异得分降序排列。",
    },
];

pub const CONFIGURATION_DRIFT_DETECTED_DRILLS: &[DrillDescriptor] = &[
    DrillDescriptor {
        title: "十节点漂移聚合与审计演练",
        goal: "验证控制面能够在 10 个节点出现哈希分歧时正确聚合事件并生成审计记录。",
        setup: &["部署 10 个业务节点并使其加载相同 Profile", "通过故障注入工具让其中若干节点写入不同配置键"],
        steps: &["收集所有节点的哈希与差异键集合并提交给聚合器", "调用聚合 API 生成 `configuration.drift.detected` 事件", "将事件传入审计系统，校验序列号、动作与实体标识"],
        expectations: &["事件中的 `divergent_nodes` 列表长度等于实际漂移节点数，且按差异数量排序", "审计事件的 `action` 为 `configuration.drift.detected`，`entity.kind` 为 `configuration.profile`", "`AuditChangeSet` 与输入的变更集合一致，确保后续回放可重现配置差异"],
    },
];

pub const CONFIGURATION_DRIFT_DETECTED: ConfigurationEventDescriptor = ConfigurationEventDescriptor {
    ident: "ConfigurationDriftDetected",
    code: "configuration.drift.detected",
    family: "drift",
    name: "配置漂移检测",
    severity: EventSeverity::Warning,
    summary: "聚合多节点上报，识别集群内的配置漂移并输出结构化差异列表。",
    description: "运行面每个节点周期性汇报配置哈希与差异键集合，控制面在窗口内汇总后生成漂移事件，事件负载需携带全部漂移节点及其差异详情，供审计与排障使用。",
    audit: AuditDescriptor {
        action: "configuration.drift.detected",
        entity_kind: "configuration.profile",
        entity_id_field: "profile_id",
    },
    fields: CONFIGURATION_DRIFT_DETECTED_FIELDS,
    drills: CONFIGURATION_DRIFT_DETECTED_DRILLS,
};

pub const CONFIGURATION_CONSISTENCY_RESTORED_FIELDS: &[EventFieldDescriptor] = &[
    EventFieldDescriptor {
        name: "profile_id",
        type_name: "String",
        required: true,
        doc: "恢复一致性的配置 Profile 标识。",
    },
    EventFieldDescriptor {
        name: "stable_hash",
        type_name: "String",
        required: true,
        doc: "最终达成一致的哈希值。",
    },
    EventFieldDescriptor {
        name: "reconciled_nodes",
        type_name: "Vec<String>",
        required: true,
        doc: "成功完成校准的节点 ID 列表。",
    },
    EventFieldDescriptor {
        name: "duration_ms",
        type_name: "u64",
        required: true,
        doc: "从漂移检测到一致性恢复的耗时（毫秒）。",
    },
    EventFieldDescriptor {
        name: "source_event_id",
        type_name: "Option<String>",
        required: false,
        doc: "触发本次恢复的上游事件 ID（如漂移或校准事件），便于串联审计链。",
    },
];

pub const CONFIGURATION_CONSISTENCY_RESTORED_DRILLS: &[DrillDescriptor] = &[
    DrillDescriptor {
        title: "漂移恢复闭环演练",
        goal: "验证从漂移检测到一致性恢复的全链路审计闭环。",
        setup: &["先执行 \"十节点漂移聚合与审计演练\" 确保存在漂移事件", "完成手动或自动校准流程，使全部节点哈希一致"],
        steps: &["收集恢复后节点列表与耗时信息", "生成 `configuration.consistency.restored` 事件并写入审计系统"],
        expectations: &["事件记录 `stable_hash` 与所有节点 ID，`source_event_id` 指向之前的漂移事件", "审计系统能够根据恢复事件闭合哈希链"],
    },
];

pub const CONFIGURATION_CONSISTENCY_RESTORED: ConfigurationEventDescriptor = ConfigurationEventDescriptor {
    ident: "ConfigurationConsistencyRestored",
    code: "configuration.consistency.restored",
    family: "consistency",
    name: "配置一致性恢复",
    severity: EventSeverity::Info,
    summary: "通知治理与审计系统集群已恢复一致状态，便于闭环漂移处理流程。",
    description: "在完成自动或人工校准后，控制面会发布一致性恢复事件，记录最终稳定哈希、耗时以及关联的漂移/校准事件 ID，形成完整的闭环证据链。",
    audit: AuditDescriptor {
        action: "configuration.consistency.restored",
        entity_kind: "configuration.profile",
        entity_id_field: "profile_id",
    },
    fields: CONFIGURATION_CONSISTENCY_RESTORED_FIELDS,
    drills: CONFIGURATION_CONSISTENCY_RESTORED_DRILLS,
};

pub const CONFIGURATION_EVENTS: &[ConfigurationEventDescriptor] = &[
    CONFIGURATION_CALIBRATION_REQUESTED,
    CONFIGURATION_DRIFT_DETECTED,
    CONFIGURATION_CONSISTENCY_RESTORED,
];
