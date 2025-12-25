use crate::host::capability::CapabilityDescriptor;
use alloc::collections::BTreeMap;
use alloc::string::String;
use alloc::vec::Vec;

/// 宿主环境的静态元数据。
///
/// # 背景（Why）
/// - 结合 Kubernetes Node 信息、Dapr Runtime Metadata 以及 Wasmtime Host 信息模型，统一描述宿主身份、版本与部署区域。
/// - 元数据在组件之间共享，避免每个模块重复解析环境变量或配置文件。
///
/// # 字段说明（What）
/// - `name`：宿主实例的逻辑名称，通常对应 Pod、进程或节点标识。
/// - `version`：宿主框架版本，便于在 A/B 或金丝雀部署时做差异化处理。
/// - `vendor`：宿主提供方或发行版，用于兼容性判定。
/// - `region`/`zone`：多活或边缘部署时的地理信息。
/// - `labels`：按照 Kubernetes/Service Mesh 惯例提供的键值标签，实现策略化路由。
///
/// # 契约（Contract）
/// - **前置条件**：宿主应确保 `name` 与 `version` 非空。
/// - **后置条件**：`labels` 键值需遵循 UTF-8，调用方读取时不得假定排序；`BTreeMap` 仅用于 deterministic 输出。
///
/// # BTreeMap 取舍说明
/// - `labels` 选用 [`BTreeMap`]，确保遍历与序列化顺序稳定，利于生成签名与做配置 diff。
/// - 与 `HashMap` 相比，`BTreeMap` 写入复杂度为 `O(log n)`，在高频更新场景可能略慢；因此推荐在热路径内部先使用 `HashMap` 聚合，最终导出
///   到 `HostMetadata` 时再排序成 `BTreeMap` 以兼顾确定性。
/// - 若未来出现无序访问需求，可考虑新增 `labels_as_hash_map` 辅助方法或引入 feature flag 暴露零拷贝视图。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HostMetadata {
    /// 宿主名称。
    pub name: String,
    /// 宿主版本。
    pub version: String,
    /// 宿主提供方。
    pub vendor: Option<String>,
    /// 部署区域。
    pub region: Option<String>,
    /// 可用区或边缘节点。
    pub zone: Option<String>,
    /// 自定义标签。
    pub labels: BTreeMap<String, String>,
}

impl HostMetadata {
    /// 构造基础元数据。
    ///
    /// # 设计说明
    /// - 保留最核心的三要素（名称、版本、标签），其余可选信息交由宿主按需填写。
    pub fn new(name: String, version: String, labels: BTreeMap<String, String>) -> Self {
        Self {
            name,
            version,
            vendor: None,
            region: None,
            zone: None,
            labels,
        }
    }
}

/// 宿主运行时画像，描述硬件与软件环境。
///
/// # 背景（Why）
/// - 借鉴 CNCF CloudEvents `Context` 与 HashiCorp Nomad Device 描述，将 CPU、OS、加速器信息统一结构化。
///
/// # 字段说明（What）
/// - `operating_system`：运行时 OS 名称。
/// - `architecture`：CPU 架构，如 `x86_64`、`aarch64`。
/// - `cpu_cores`：逻辑核心数，影响并发调度策略。
/// - `memory_bytes`：可用内存，用于决定批处理与缓冲策略。
/// - `accelerators`：GPU、TPU 等异构加速能力列表。
///
/// # 前后置条件（Contract）
/// - **前置条件**：宿主在启动阶段应尽量填充所有字段，无法探测的字段可设为 `None` 或空集合。
/// - **后置条件**：结构体为不可变快照，组件不得依赖其实时更新。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HostRuntimeProfile {
    /// 操作系统名称。
    pub operating_system: String,
    /// CPU 架构。
    pub architecture: String,
    /// 逻辑核心数。
    pub cpu_cores: Option<u16>,
    /// 可用内存（字节）。
    pub memory_bytes: Option<u64>,
    /// 异构加速器列表。
    pub accelerators: Vec<String>,
}

impl HostRuntimeProfile {
    /// 快速构造仅包含 OS 与架构的信息。
    ///
    /// # 逻辑拆解
    /// - 其余资源信息默认为空，以支持极简嵌入式宿主。
    pub fn minimal(operating_system: String, architecture: String) -> Self {
        Self {
            operating_system,
            architecture,
            cpu_cores: None,
            memory_bytes: None,
            accelerators: Vec::new(),
        }
    }
}

/// 宿主上下文，贯穿组件生命周期的关键输入。
///
/// # 背景（Why）
/// - 综合 Dapr `Context`, Envoy Filter Context 与 Kubernetes AdmissionReview 的经验，将宿主能力、元数据、运行时状态一次性传递。
/// - 减少组件通过全局变量、环境变量读取的耦合点，提升可测试性。
///
/// # 字段说明（What）
/// - `metadata`：静态身份信息。
/// - `runtime`：硬件/软件画像。
/// - `capabilities`：宿主能力快照，通常来源于协商阶段。
/// - `extensions`：动态扩展键值，便于宿主注入临时上下文（如 Trace、租户信息）。
///
/// # 契约（Contract）
/// - **前置条件**：宿主必须保证 `metadata` 和 `runtime` 已经填充最小必需信息。
/// - **后置条件**：上下文应视为只读，任何修改应通过配置或生命周期回调完成。
///
/// # 与执行上下文协作（Context）
/// - `HostContext` 关注宿主静态与运行时画像，与 [`crate::context::Context`] 携带的取消/截止/预算三元组形成互补；
///   组件在初始化阶段通常同时接收两类上下文，以便在调用期既能读取宿主能力，也能感知调用约束；
/// - 在跨线程传播时，应参照 `Context` 的契约提前克隆所需字段（如 `Arc`、`BTreeMap`），避免在任务执行过程中产生悬垂引用。
///
/// # 风险提示（Trade-offs）
/// - 使用 `BTreeMap` 作为扩展容器以保持序列化稳定性；其插入成本为 `O(log n)`，写入密集型负载下会略逊于 `HashMap` 的平均 `O(1)` 性能。
/// - 若扩展字段需要频繁突变，可在宿主内部维护可变 `HashMap` 并在交付给组件前转换为 `BTreeMap`，以平衡性能与确定性，或将热点字段缓
///   存到 `HashMap`/`SmallVec` 后按需同步回扩展表。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HostContext {
    /// 静态元数据。
    pub metadata: HostMetadata,
    /// 运行时画像。
    pub runtime: HostRuntimeProfile,
    /// 能力描述。
    pub capabilities: CapabilityDescriptor,
    /// 扩展字段。
    pub extensions: BTreeMap<String, String>,
}

impl HostContext {
    /// 根据核心结构体拼装上下文。
    ///
    /// # 设计意图
    /// - 提供一个显式的构造函数，确保宿主在提供 `CapabilityDescriptor` 时与元数据保持一致。
    pub fn new(
        metadata: HostMetadata,
        runtime: HostRuntimeProfile,
        capabilities: CapabilityDescriptor,
        extensions: BTreeMap<String, String>,
    ) -> Self {
        Self {
            metadata,
            runtime,
            capabilities,
            extensions,
        }
    }
}
