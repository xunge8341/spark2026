use alloc::vec::Vec;

use super::{ConfigKey, ConfigValue};

/// 配置变更通知中包含的单条事件。
///
/// ### 设计目的（Why）
/// - 对齐 Envoy Delta xDS、NATS JetStream Push 机制提供的增量事件语义。
/// - 帮助运行时细粒度地感知新增、更新、删除，避免全量刷新导致的性能抖动。
///
/// ### 逻辑说明（How）
/// - `Created`：表示此前不存在的配置键被创建。
/// - `Updated`：已存在的配置值发生变化，携带新值。
/// - `Deleted`：配置项被移除。
///
/// ### 契约定义（What）
/// - 事件序列由 [`ChangeNotification`] 按顺序携带。
/// - `Updated` 与 `Created` 均返回最新值；删除事件不包含旧值，避免泄漏敏感数据。
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub enum ChangeEvent {
    Created { key: ConfigKey, value: ConfigValue },
    Updated { key: ConfigKey, value: ConfigValue },
    Deleted { key: ConfigKey },
}

/// 封装一次批量变更。
///
/// ### 设计目的（Why）
/// - 借鉴 Consul、etcd Watch API，将多个事件在单一通知中发送，减少网络往返。
/// - 保持事件顺序性，使上层可以幂等地重放。
///
/// ### 契约说明（What）
/// - **前置条件**：事件列表需按照发生顺序排列；Builder 在发送前需保证这一点。
/// - **后置条件**：`sequence` 单调递增，可用于断点续传；`occurred_at` 记录 Unix 毫秒时间戳便于审计。
#[derive(Clone, Debug, PartialEq)]
pub struct ChangeNotification {
    pub sequence: u64,
    pub occurred_at: u64,
    pub events: Vec<ChangeEvent>,
}

impl ChangeNotification {
    /// 创建新的增量通知。
    pub fn new(sequence: u64, occurred_at: u64, events: Vec<ChangeEvent>) -> Self {
        Self {
            sequence,
            occurred_at,
            events,
        }
    }
}

/// 同步变更用的最小集合。
///
/// ### 设计目的（Why）
/// - 为“拉模式”场景（pull-based reconciliation）提供一次性差异列表。
/// - 相比 `ChangeNotification` 省略序号，更适合离线比较或快照校验。
#[derive(Clone, Debug, PartialEq)]
pub struct ChangeSet {
    pub created: Vec<(ConfigKey, ConfigValue)>,
    pub updated: Vec<(ConfigKey, ConfigValue)>,
    pub deleted: Vec<ConfigKey>,
}
