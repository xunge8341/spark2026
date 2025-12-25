use alloc::{collections::BTreeMap, string::String};
use core::time::Duration;

/// `TransportParams` 表达 Endpoint 或连接意图附带的键值参数。
///
/// # 设计背景（Why）
/// - **生产级启发**：借鉴 Envoy Cluster、gRPC Channel Arguments、Kafka Client Config，将可选项集中在统一的键值表，避免接口碎片化。
/// - **前沿探索**：允许在学术实验中注入例如能耗模型、网络切片等参数，通过键值解耦具体算法实现。
///
/// # 契约说明（What）
/// - 所有键与值均为 UTF-8 字符串，键名建议使用 `snake_case`，便于与配置中心/注册中心统一。
/// - 提供若干类型安全的访问器，帮助调用方减少样板解析代码。
/// - **前置条件**：调用前应约定好关键键名及值范围；本类型不强制验证。
/// - **后置条件**：解析成功时返回 `Some(value)`，失败时返回 `None` 并保持原值不变。
///
/// # 设计取舍与风险（Trade-offs）
/// - 使用 [`BTreeMap`] 保证遍历顺序稳定，利于配置 diff 与 deterministic 序列化；与 `HashMap` 相比写入为 `O(log n)`，若频繁更新需留意
///   延迟。
/// - 未内建 schema 校验，保持轻量；若需强约束，可结合外部验证器。
///
/// # BTreeMap 访问策略
/// - 性能敏感路径可通过 `TransportParams::as_map` 拿到有序视图，再 `cloned().collect::<HashMap<_, _>>()` 获得无序版本缓存，避免在热路
///   径多次承受 `O(log n)` 插入开销。
/// - 后续若出现广泛的 `HashMap` 需求，可新增 `into_inner` 或 `into_hash_map` 辅助函数；当前契约保持最小化以兼顾稳定排序与 API 简洁。
/// - 由于 `BTreeMap` 提供对称的 `range`、`split_off` 能力，实现者也可利用其特性构建前缀查询或分页逻辑。
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct TransportParams(BTreeMap<String, String>);

impl TransportParams {
    /// 创建空参数表。
    pub fn new() -> Self {
        Self::default()
    }

    /// 插入或覆盖键值。
    pub fn insert(&mut self, key: String, value: String) {
        self.0.insert(key, value);
    }

    /// 合并另一个参数表，右侧优先。
    ///
    /// # 逻辑解析（How）
    /// - 逐条遍历 `other`，调用 `insert` 覆盖同名键。
    /// - 返回 `&mut Self` 便于链式调用。
    pub fn merge(&mut self, other: &TransportParams) -> &mut Self {
        for (key, value) in &other.0 {
            self.0.insert(key.clone(), value.clone());
        }
        self
    }

    /// 以不可变引用形式暴露内部映射，供调试或一次性遍历。
    pub fn as_map(&self) -> &BTreeMap<String, String> {
        &self.0
    }

    /// 读取字符串值。
    pub fn get_str(&self, key: &str) -> Option<&str> {
        self.0.get(key).map(|value| value.as_str())
    }

    /// 解析布尔值，接受 `true/false`（大小写敏感）。
    pub fn get_bool(&self, key: &str) -> Option<bool> {
        self.get_str(key)
            .and_then(|value| value.parse::<bool>().ok())
    }

    /// 解析无符号整数，常用于端口、并发上限。
    pub fn get_u64(&self, key: &str) -> Option<u64> {
        self.get_str(key)
            .and_then(|value| value.parse::<u64>().ok())
    }

    /// 按毫秒解析持续时间。
    pub fn get_duration(&self, key: &str) -> Option<Duration> {
        self.get_u64(key).map(Duration::from_millis)
    }

    /// 按字节解析大小，遵循十进制。
    pub fn get_size_bytes(&self, key: &str) -> Option<u64> {
        self.get_u64(key)
    }
}
