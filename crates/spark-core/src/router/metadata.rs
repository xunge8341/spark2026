use alloc::borrow::Cow;
use alloc::collections::{BTreeMap, btree_map};

/// 路由元数据键，使用 `Cow<'static, str>` 兼顾静态与动态标签。
///
/// # 教案级说明
/// - **意图 (Why)**：统一存储控制面与运行时附加的键名，确保可比较且具备稳定排序；
/// - **契约 (What)**：键名必须是非空字符串，调用方负责保证语义唯一性；
/// - **设计 (How)**：内部持有 `Cow`，允许零拷贝复用静态切片，也支持在运行时分配新字符串；
/// - **权衡 (Trade-offs)**：使用 `BTreeMap` 的排序依赖键实现 `Ord`；因此键值保持最小封装避免额外开销。
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct MetadataKey(Cow<'static, str>);

impl MetadataKey {
    /// 基于任意可转换为 `Cow` 的输入创建键名。
    pub fn new<S>(key: S) -> Self
    where
        S: Into<Cow<'static, str>>,
    {
        Self(key.into())
    }

    /// 读取底层字符串切片。
    pub fn as_str(&self) -> &str {
        self.0.as_ref()
    }
}

/// 路由元数据值，预留扩展空间。
///
/// # 教案级说明
/// - **意图 (Why)**：以最小枚举支撑当前用例（文本标签），同时便于未来扩展数值/结构化类型；
/// - **契约 (What)**：调用方负责确保值的语义与键匹配；
/// - **风险 (Trade-offs)**：枚举非穷尽，新增变体不会破坏现有匹配；但序列化/日志需自行处理新分支。
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
#[non_exhaustive]
pub enum MetadataValue {
    /// 文本标签，适合租户、Trace ID 等描述性字段。
    Text(Cow<'static, str>),
}

/// 路由元数据表，使用有序映射以便稳定迭代。
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RouteMetadata(BTreeMap<MetadataKey, MetadataValue>);

impl RouteMetadata {
    /// 创建空的元数据映射。
    pub fn new() -> Self {
        Self(BTreeMap::new())
    }

    /// 插入或覆盖键值对。
    pub fn insert(&mut self, key: MetadataKey, value: MetadataValue) {
        self.0.insert(key, value);
    }

    /// 以只读方式遍历键值对，遵循有序顺序。
    pub fn iter(&self) -> btree_map::Iter<'_, MetadataKey, MetadataValue> {
        self.0.iter()
    }

    /// 检查是否为空。
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}
